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

use skywalking::{
    proto::v3::MeterData,
    reporter::{CollectItem, CollectItemConsume, ConsumeResult},
};
use tokio::sync::mpsc;
use tonic::async_trait;
use tracing::warn;

pub struct MeterFilteringConsumer<C> {
    inner: C,
    meter_tx: mpsc::Sender<MeterData>,
}

impl<C> MeterFilteringConsumer<C> {
    pub fn new(inner: C, meter_tx: mpsc::Sender<MeterData>) -> Self {
        Self { inner, meter_tx }
    }

    fn forward_meter(&self, meter: MeterData) -> ConsumeResult {
        if let Err(err) = self.meter_tx.try_send(meter) {
            warn!(?err, "Failed to enqueue meter data for batch reporter");
        }
        Ok(None)
    }
}

#[async_trait]
impl<C: CollectItemConsume> CollectItemConsume for MeterFilteringConsumer<C> {
    async fn consume(&mut self) -> ConsumeResult {
        loop {
            match self.inner.consume().await? {
                None => return Ok(None),
                Some(CollectItem::Meter(meter)) => {
                    self.forward_meter(*meter)?;
                }
                Some(item) => return Ok(Some(item)),
            }
        }
    }

    async fn try_consume(&mut self) -> ConsumeResult {
        loop {
            match self.inner.try_consume().await? {
                None => return Ok(None),
                Some(CollectItem::Meter(meter)) => {
                    self.forward_meter(*meter)?;
                }
                Some(item) => return Ok(Some(item)),
            }
        }
    }
}
