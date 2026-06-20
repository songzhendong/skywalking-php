// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Batched meter reporter using `MeterReportServiceClient::collect_batch`.
//!
//! SkyWalking OAP applies meter aggregation when a gRPC stream completes. The
//! streaming `collect` RPC keeps the stream open while the agent sends points,
//! so OAP does not finalize those meters until disconnect. PHP emits metrics on
//! a periodic tick rather than a long-lived stream, which would delay or lose
//! data if we used `collect` alone. `collectBatch` accepts a short stream of
//! `MeterDataCollection` messages and processes each batch when that stream
//! ends, which matches our flush-every-interval reporting model.
//!
//! On `collectBatch` failure the batch is kept and retried every 5 seconds
//! (same backoff as Go). Retries stop after [`MAX_BATCH_RETRY_ATTEMPTS`]
//! failures or [`MAX_BATCH_RETENTION`], whichever comes first; the batch is
//! then dropped.

use skywalking::proto::v3::{
    MeterData, MeterDataCollection, meter_report_service_client::MeterReportServiceClient,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    select,
    sync::mpsc,
    time::{self, MissedTickBehavior},
};
use tokio_stream::{self as stream};
use tonic::{
    Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::Channel,
};
use tracing::warn;

const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Same backoff as Go agent meter stream reopen (`time.Sleep(5 *
/// time.Second)`).
const BATCH_RETRY_INTERVAL: Duration = Duration::from_secs(5);
/// Stop retrying a stuck batch after this many consecutive send failures.
const MAX_BATCH_RETRY_ATTEMPTS: u32 = 12;
/// Wall-clock cap on holding a failed batch (5 minutes).
const MAX_BATCH_RETENTION: Duration = Duration::from_secs(300);

#[derive(Clone, Default)]
struct AuthInterceptor {
    authentication: Option<Arc<String>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(authentication) = &self.authentication {
            if let Ok(authentication) = authentication.parse::<MetadataValue<Ascii>>() {
                request
                    .metadata_mut()
                    .insert("authentication", authentication);
            }
        }
        Ok(request)
    }
}

type MeterClient = MeterReportServiceClient<InterceptedService<Channel, AuthInterceptor>>;

pub async fn run_meter_batch_reporter(
    channel: Channel, authentication: String, mut meter_rx: mpsc::Receiver<MeterData>,
) -> anyhow::Result<()> {
    let authentication = if authentication.is_empty() {
        None
    } else {
        Some(Arc::new(authentication))
    };

    let mut client =
        MeterReportServiceClient::with_interceptor(channel, AuthInterceptor { authentication });

    let mut buffer = Vec::new();
    let mut interval = time::interval(FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        select! {
            item = meter_rx.recv() => {
                match item {
                    Some(meter) => buffer.push(meter),
                    None => {
                        flush_batch(&mut client, &mut buffer).await;
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                flush_batch(&mut client, &mut buffer).await;
            }
        }
    }

    Ok(())
}

async fn send_meter_batch(
    client: &mut MeterClient, meter_data: Vec<MeterData>,
) -> Result<(), Status> {
    let collection = MeterDataCollection { meter_data };
    client.collect_batch(stream::iter([collection])).await?;
    Ok(())
}

async fn flush_batch(client: &mut MeterClient, buffer: &mut Vec<MeterData>) {
    if buffer.is_empty() {
        return;
    }

    let pending = std::mem::take(buffer);
    let count = pending.len();
    let retention_deadline = time::Instant::now() + MAX_BATCH_RETENTION;
    let mut failures = 0u32;

    loop {
        match send_meter_batch(client, pending.clone()).await {
            Ok(()) => return,
            Err(status) => {
                failures += 1;
                if failures >= MAX_BATCH_RETRY_ATTEMPTS
                    || time::Instant::now() >= retention_deadline
                {
                    warn!(
                        ?status,
                        count,
                        failures,
                        max_failures = MAX_BATCH_RETRY_ATTEMPTS,
                        max_retention_secs = MAX_BATCH_RETENTION.as_secs(),
                        "Dropping meter batch after collectBatch retry limits exceeded"
                    );
                    return;
                }
                warn!(
                    ?status,
                    count,
                    failures,
                    max_failures = MAX_BATCH_RETRY_ATTEMPTS,
                    max_retention_secs = MAX_BATCH_RETENTION.as_secs(),
                    "Collect meter data by collectBatch failed, retry after {:?}",
                    BATCH_RETRY_INTERVAL
                );
                time::sleep(BATCH_RETRY_INTERVAL).await;
            }
        }
    }
}
