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

//! Periodic PHM meter collection in the reporter worker, similar to Java
//! `JVMService` / Go runtime meter collector. Samples the parent PHP-FPM worker
//! process via `/proc` so meters are reported without waiting for HTTP traffic.

use crate::channel::TxReporter;
use skywalking::{
    proto::v3::{MeterData, MeterSingleValue, meter_data::Metric},
    reporter::{CollectItem, Report},
};
use std::{
    fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{debug, trace, warn};

const METRIC_PROCESS_CPU: &str = "instance_php_process_cpu_utilization";
const DEFAULT_CLK_TCK: f64 = 100.0;
const METRIC_MEMORY_USED_MB: &str = "instance_php_memory_used_mb";
const METRIC_MEMORY_PEAK_MB: &str = "instance_php_memory_peak_mb";
const METRIC_THREAD_COUNT: &str = "instance_php_thread_count";
const METRIC_VIRTUAL_MEMORY_MB: &str = "instance_php_virtual_memory_mb";
const METRIC_OPEN_FD_COUNT: &str = "instance_php_open_fd_count";

pub struct PhmConfiguration {
    pub service_name: String,
    pub service_instance: String,
    pub report_period_secs: i64,
    /// Parent PHP-FPM worker PID (the process that forked this worker).
    pub php_process_pid: i32,
}

struct CpuStatSample {
    utime: u64,
    stime: u64,
    wall_ms: u128,
}

pub fn run_phm_collector(config: PhmConfiguration, reporter: TxReporter) {
    tokio::spawn(async move {
        let period = Duration::from_secs(config.report_period_secs.max(1) as u64);
        let mut cpu_sample: Option<CpuStatSample> = None;
        loop {
            let pid = resolve_php_process_pid(config.php_process_pid);
            if !process_alive(pid) {
                warn!(pid, "PHM target PHP process is gone, stop collector");
                break;
            }

            if let Some(mb) = read_status_kib(pid, "VmRSS") {
                report_meter(
                    &reporter,
                    &config.service_name,
                    &config.service_instance,
                    METRIC_MEMORY_USED_MB,
                    mb,
                );
            }
            if let Some(mb) = read_status_kib(pid, "VmHWM") {
                report_meter(
                    &reporter,
                    &config.service_name,
                    &config.service_instance,
                    METRIC_MEMORY_PEAK_MB,
                    mb,
                );
            }
            if let Some(mb) = read_status_kib(pid, "VmSize") {
                report_meter(
                    &reporter,
                    &config.service_name,
                    &config.service_instance,
                    METRIC_VIRTUAL_MEMORY_MB,
                    mb,
                );
            }
            if let Some(count) = read_status_count(pid, "Threads") {
                report_meter(
                    &reporter,
                    &config.service_name,
                    &config.service_instance,
                    METRIC_THREAD_COUNT,
                    count as f64,
                );
            }
            if let Some(count) = read_open_fd_count(pid) {
                report_meter(
                    &reporter,
                    &config.service_name,
                    &config.service_instance,
                    METRIC_OPEN_FD_COUNT,
                    count,
                );
            }
            if let Some((utime, stime)) = read_proc_stat_cpu(pid) {
                let now_ms = current_time_millis();
                let cpu = match &mut cpu_sample {
                    None => {
                        cpu_sample = Some(CpuStatSample {
                            utime,
                            stime,
                            wall_ms: now_ms,
                        });
                        None
                    }
                    Some(sample) => {
                        let delta_jiffies =
                            utime.saturating_sub(sample.utime) + stime.saturating_sub(sample.stime);
                        let delta_wall_ms = now_ms.saturating_sub(sample.wall_ms);
                        sample.utime = utime;
                        sample.stime = stime;
                        sample.wall_ms = now_ms;
                        Some(cpu_percent(delta_jiffies, delta_wall_ms))
                    }
                };
                if let Some(cpu) = cpu {
                    trace!(pid, cpu, "report PHM process CPU from worker collector");
                    report_meter(
                        &reporter,
                        &config.service_name,
                        &config.service_instance,
                        METRIC_PROCESS_CPU,
                        cpu,
                    );
                }
            } else {
                warn!(pid, "failed to read /proc stat for PHM CPU sampling");
            }
            debug!(pid, "PHM proc meters reported from worker collector");
            tokio::time::sleep(period).await;
        }
    });
}

fn resolve_php_process_pid(fallback: i32) -> i32 {
    let ppid = unsafe { libc::getppid() as i32 };
    if ppid > 1 { ppid } else { fallback }
}

fn process_alive(pid: i32) -> bool {
    fs::metadata(format!("/proc/{pid}")).is_ok()
}

fn read_status_count(pid: i32, key: &str) -> Option<u64> {
    let content = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let prefix = format!("{key}:");
    for line in content.lines() {
        if line.starts_with(&prefix) {
            return line.split_whitespace().nth(1)?.parse().ok();
        }
    }
    None
}

fn read_open_fd_count(pid: i32) -> Option<f64> {
    let count = fs::read_dir(format!("/proc/{pid}/fd"))
        .ok()?
        .filter_map(|entry| entry.ok())
        .count();
    Some(count as f64)
}

fn read_status_kib(pid: i32, key: &str) -> Option<f64> {
    let content = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let prefix = format!("{key}:");
    for line in content.lines() {
        if line.starts_with(&prefix) {
            let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
            return Some(kb / 1024.0);
        }
    }
    None
}

fn read_proc_stat_cpu(pid: i32) -> Option<(u64, u64)> {
    let content = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rparen = content.rfind(')')?;
    let fields: Vec<&str> = content[rparen + 2..].split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some((utime, stime))
}

fn cpu_percent(delta_jiffies: u64, delta_wall_ms: u128) -> f64 {
    if delta_wall_ms == 0 {
        return 0.0;
    }
    let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let clk_tck = if clk_tck > 0 {
        clk_tck as f64
    } else {
        warn!(
            clk_tck,
            "sysconf(_SC_CLK_TCK) unavailable, using default {DEFAULT_CLK_TCK}"
        );
        DEFAULT_CLK_TCK
    };
    let cpu_sec = delta_jiffies as f64 / clk_tck;
    let wall_sec = delta_wall_ms as f64 / 1000.0;
    cpu_sec / wall_sec * 100.0
}

fn report_meter(reporter: &TxReporter, service: &str, instance: &str, name: &str, value: f64) {
    reporter.report(CollectItem::Meter(Box::new(MeterData {
        service: service.to_owned(),
        service_instance: instance.to_owned(),
        timestamp: current_time_millis() as i64,
        metric: Some(Metric::SingleValue(MeterSingleValue {
            name: name.to_owned(),
            labels: vec![],
            value,
        })),
    })));
}

fn current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}
