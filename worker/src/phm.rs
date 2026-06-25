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

//! Periodic PHM meter collection in the forked reporter worker. Samples the
//! parent PHP process via `/proc` and reports through `skywalking::metrics`
//! `Metricer`, booted from `start_worker` alongside heartbeat reporting.

use crate::channel::TxReporter;
use skywalking::metrics::{
    meter::Gauge,
    metricer::{Booting, Metricer},
};
use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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

#[derive(Clone)]
pub struct PhmConfiguration {
    pub service_name: String,
    pub service_instance: String,
    pub report_period_secs: i64,
}

#[derive(Clone)]
struct PhmCollectorConfiguration {
    report_period_secs: i64,
}

#[derive(Clone, Default)]
pub struct PhmSamples {
    memory_used_mb: Arc<AtomicU64>,
    memory_peak_mb: Arc<AtomicU64>,
    virtual_memory_mb: Arc<AtomicU64>,
    thread_count: Arc<AtomicU64>,
    open_fd_count: Arc<AtomicU64>,
    process_cpu: Arc<AtomicU64>,
    extended: extended::Samples,
}

impl PhmSamples {
    fn store(cell: &AtomicU64, value: f64) {
        cell.store(value.to_bits(), Ordering::Relaxed);
    }

    fn gauge(cell: Arc<AtomicU64>) -> impl Fn() -> f64 + Send + Sync + 'static {
        move || f64::from_bits(cell.load(Ordering::Relaxed))
    }
}

pub fn register_gauges(metricer: &mut Metricer, samples: PhmSamples) {
    metricer.register(Gauge::new(
        METRIC_MEMORY_USED_MB,
        PhmSamples::gauge(samples.memory_used_mb.clone()),
    ));
    metricer.register(Gauge::new(
        METRIC_MEMORY_PEAK_MB,
        PhmSamples::gauge(samples.memory_peak_mb.clone()),
    ));
    metricer.register(Gauge::new(
        METRIC_VIRTUAL_MEMORY_MB,
        PhmSamples::gauge(samples.virtual_memory_mb.clone()),
    ));
    metricer.register(Gauge::new(
        METRIC_THREAD_COUNT,
        PhmSamples::gauge(samples.thread_count.clone()),
    ));
    metricer.register(Gauge::new(
        METRIC_OPEN_FD_COUNT,
        PhmSamples::gauge(samples.open_fd_count.clone()),
    ));
    metricer.register(Gauge::new(
        METRIC_PROCESS_CPU,
        PhmSamples::gauge(samples.process_cpu.clone()),
    ));
    extended::register_gauges(metricer, &samples.extended);
}

struct CpuStatSample {
    utime: u64,
    stime: u64,
    wall_ms: u128,
}

fn update_samples(samples: &PhmSamples, cpu_sample: &mut Option<CpuStatSample>) -> Option<i32> {
    let pid = unsafe { libc::getppid() as i32 };
    if !process_alive(pid) {
        warn!(pid, "PHM target PHP process is gone, skip sample");
        return None;
    }

    let snapshot = extended::ProcSnapshot::read(pid);
    let now_ms = current_time_millis();

    if let Some(mb) = snapshot.vm_rss_mb {
        PhmSamples::store(&samples.memory_used_mb, mb);
    }
    if let Some(mb) = snapshot.vm_hwm_mb {
        PhmSamples::store(&samples.memory_peak_mb, mb);
    }
    if let Some(mb) = snapshot.vm_size_mb {
        PhmSamples::store(&samples.virtual_memory_mb, mb);
    }
    if let Some(count) = snapshot.threads {
        PhmSamples::store(&samples.thread_count, count as f64);
    }
    if let Some(count) = snapshot.open_fd_count {
        PhmSamples::store(&samples.open_fd_count, count);
    }
    if let (Some(utime), Some(stime)) = (snapshot.utime, snapshot.stime) {
        let cpu = match cpu_sample {
            None => {
                *cpu_sample = Some(CpuStatSample {
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
            trace!(pid, cpu, "update PHM process CPU sample");
            PhmSamples::store(&samples.process_cpu, cpu);
        }
    } else {
        warn!(pid, "failed to read /proc stat for PHM CPU sampling");
    }
    extended::update_samples(&samples.extended, &snapshot, now_ms);
    debug!(pid, "PHM proc samples updated");
    Some(pid)
}

/// Populate gauges once before `Metricer::boot()` so the first report is not
/// all zeros.
pub fn warmup_samples(samples: &PhmSamples) {
    let mut cpu_sample = None;
    update_samples(samples, &mut cpu_sample);
}

pub fn boot_phm_metrics(config: PhmConfiguration, reporter: TxReporter) -> Booting {
    let samples = PhmSamples::default();
    let report_period = Duration::from_secs(config.report_period_secs.max(1) as u64);
    let collector_config = PhmCollectorConfiguration {
        report_period_secs: config.report_period_secs,
    };
    warmup_samples(&samples);
    run_phm_collector(collector_config, samples.clone());
    let mut metricer = Metricer::new(config.service_name, config.service_instance, reporter);
    metricer.set_report_interval(report_period);
    register_gauges(&mut metricer, samples);
    metricer.boot()
}

fn run_phm_collector(config: PhmCollectorConfiguration, samples: PhmSamples) {
    tokio::spawn(async move {
        let period = Duration::from_secs(config.report_period_secs.max(1) as u64);
        let mut cpu_sample: Option<CpuStatSample> = None;
        loop {
            if update_samples(&samples, &mut cpu_sample).is_none() {
                break;
            }
            tokio::time::sleep(period).await;
        }
    });
}

fn process_alive(pid: i32) -> bool {
    fs::metadata(format!("/proc/{pid}")).is_ok()
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

fn current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}

// =============================================================================
// Extended PHM — six additional instance meters (Linux /proc only)
// =============================================================================

mod extended {
    use skywalking::metrics::{meter::Gauge, metricer::Metricer};
    use std::{
        cell::RefCell,
        fs,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };
    use tracing::warn;

    const METRIC_SWAP_USED_MB: &str = "instance_php_swap_used_mb";
    const METRIC_FD_UTILIZATION: &str = "instance_php_fd_utilization";
    const METRIC_PROCESS_MEM_UTILIZATION: &str = "instance_php_process_mem_utilization";
    const METRIC_IO_READ_KB_PER_SEC: &str = "instance_php_io_read_kb_per_sec";
    const METRIC_MAJOR_PAGE_FAULTS_PER_SEC: &str = "instance_php_major_page_faults_per_sec";
    const METRIC_PROCESS_UPTIME_SEC: &str = "instance_php_process_uptime_sec";
    const DEFAULT_CLK_TCK: f64 = 100.0;
    const FD_SOFT_LIMIT_UNLIMITED: u64 = 1_000_000_000;

    /// One `/proc` read per source file per sample tick (shared by base +
    /// extended PHM).
    pub struct ProcSnapshot {
        pub vm_rss_mb: Option<f64>,
        pub vm_hwm_mb: Option<f64>,
        pub vm_size_mb: Option<f64>,
        pub vm_swap_mb: Option<f64>,
        pub threads: Option<u64>,
        pub open_fd_count: Option<f64>,
        pub fd_soft_limit: Option<u64>,
        pub utime: Option<u64>,
        pub stime: Option<u64>,
        pub majflt: Option<u64>,
        pub starttime: Option<u64>,
        pub io_read_bytes: Option<u64>,
        pub mem_total_kb: Option<u64>,
        pub system_uptime_sec: Option<f64>,
    }

    impl ProcSnapshot {
        pub fn read(pid: i32) -> Self {
            let mut snap = ProcSnapshot {
                vm_rss_mb: None,
                vm_hwm_mb: None,
                vm_size_mb: None,
                vm_swap_mb: None,
                threads: None,
                open_fd_count: None,
                fd_soft_limit: None,
                utime: None,
                stime: None,
                majflt: None,
                starttime: None,
                io_read_bytes: None,
                mem_total_kb: None,
                system_uptime_sec: None,
            };
            snap.read_status(pid);
            snap.open_fd_count = read_open_fd_count(pid);
            snap.read_limits(pid);
            snap.read_stat(pid);
            snap.io_read_bytes = read_io_read_bytes(pid);
            snap.mem_total_kb = read_mem_total_kb();
            snap.system_uptime_sec = read_system_uptime_sec();
            snap
        }

        fn read_status(&mut self, pid: i32) {
            let Ok(content) = fs::read_to_string(format!("/proc/{pid}/status")) else {
                return;
            };
            for line in content.lines() {
                if self.vm_rss_mb.is_none() {
                    self.vm_rss_mb = parse_status_kib_line(line, "VmRSS");
                }
                if self.vm_hwm_mb.is_none() {
                    self.vm_hwm_mb = parse_status_kib_line(line, "VmHWM");
                }
                if self.vm_size_mb.is_none() {
                    self.vm_size_mb = parse_status_kib_line(line, "VmSize");
                }
                if self.vm_swap_mb.is_none() {
                    self.vm_swap_mb = parse_status_kib_line(line, "VmSwap");
                }
                if self.threads.is_none() {
                    self.threads = parse_status_count_line(line, "Threads");
                }
            }
        }

        fn read_limits(&mut self, pid: i32) {
            let Ok(content) = fs::read_to_string(format!("/proc/{pid}/limits")) else {
                return;
            };
            for line in content.lines() {
                if line.starts_with("Max open files") {
                    if let Ok(soft) = line.split_whitespace().nth(3).unwrap_or("0").parse::<u64>() {
                        if soft < FD_SOFT_LIMIT_UNLIMITED {
                            self.fd_soft_limit = Some(soft);
                        }
                    }
                    break;
                }
            }
        }

        fn read_stat(&mut self, pid: i32) {
            let Ok(content) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
                return;
            };
            let Some(rparen) = content.rfind(')') else {
                return;
            };
            let fields: Vec<&str> = content[rparen + 2..].split_whitespace().collect();
            self.majflt = fields.get(9).and_then(|v| v.parse().ok());
            self.utime = fields.get(11).and_then(|v| v.parse().ok());
            self.stime = fields.get(12).and_then(|v| v.parse().ok());
            self.starttime = fields.get(19).and_then(|v| v.parse().ok());
        }
    }

    #[derive(Clone, Default)]
    pub struct Samples {
        swap_used_mb: Arc<AtomicU64>,
        fd_utilization: Arc<AtomicU64>,
        process_mem_utilization: Arc<AtomicU64>,
        io_read_kb_per_sec: Arc<AtomicU64>,
        major_page_faults_per_sec: Arc<AtomicU64>,
        process_uptime_sec: Arc<AtomicU64>,
    }

    struct CounterSample {
        value: u64,
        wall_ms: u128,
    }

    #[derive(Default)]
    struct CollectorState {
        io_sample: Option<CounterSample>,
        majflt_sample: Option<CounterSample>,
    }

    thread_local! {
        static COLLECTOR_STATE: RefCell<CollectorState> = RefCell::new(CollectorState::default());
    }

    pub fn register_gauges(metricer: &mut Metricer, samples: &Samples) {
        metricer.register(Gauge::new(
            METRIC_SWAP_USED_MB,
            gauge(samples.swap_used_mb.clone()),
        ));
        metricer.register(Gauge::new(
            METRIC_FD_UTILIZATION,
            gauge(samples.fd_utilization.clone()),
        ));
        metricer.register(Gauge::new(
            METRIC_PROCESS_MEM_UTILIZATION,
            gauge(samples.process_mem_utilization.clone()),
        ));
        metricer.register(Gauge::new(
            METRIC_IO_READ_KB_PER_SEC,
            gauge(samples.io_read_kb_per_sec.clone()),
        ));
        metricer.register(Gauge::new(
            METRIC_MAJOR_PAGE_FAULTS_PER_SEC,
            gauge(samples.major_page_faults_per_sec.clone()),
        ));
        metricer.register(Gauge::new(
            METRIC_PROCESS_UPTIME_SEC,
            gauge(samples.process_uptime_sec.clone()),
        ));
    }

    pub fn update_samples(samples: &Samples, snap: &ProcSnapshot, now_ms: u128) {
        COLLECTOR_STATE.with_borrow_mut(|state| {
            if let (Some(mb), Some(mem_total_kb)) = (snap.vm_rss_mb, snap.mem_total_kb) {
                store(
                    &samples.process_mem_utilization,
                    mb * 1024.0 / mem_total_kb as f64 * 100.0,
                );
            }
            if let Some(mb) = snap.vm_swap_mb {
                store(&samples.swap_used_mb, mb);
            }
            if let (Some(count), Some(limit)) = (snap.open_fd_count, snap.fd_soft_limit) {
                store(&samples.fd_utilization, count / limit as f64 * 100.0);
            }
            if let Some(read_bytes) = snap.io_read_bytes {
                if let Some(bytes_per_sec) = rate_per_sec(read_bytes, now_ms, &mut state.io_sample)
                {
                    store(&samples.io_read_kb_per_sec, bytes_per_sec / 1024.0);
                }
            }
            if let (Some(majflt), Some(starttime), Some(uptime_sec)) =
                (snap.majflt, snap.starttime, snap.system_uptime_sec)
            {
                let start_sec = starttime as f64 / clk_tck();
                store(
                    &samples.process_uptime_sec,
                    (uptime_sec - start_sec).max(0.0),
                );
                if let Some(rate) = rate_per_sec(majflt, now_ms, &mut state.majflt_sample) {
                    store(&samples.major_page_faults_per_sec, rate);
                }
            }
        });
    }

    fn store(cell: &AtomicU64, value: f64) {
        cell.store(value.to_bits(), Ordering::Relaxed);
    }

    fn gauge(cell: Arc<AtomicU64>) -> impl Fn() -> f64 + Send + Sync + 'static {
        move || f64::from_bits(cell.load(Ordering::Relaxed))
    }

    fn parse_status_kib_line(line: &str, key: &str) -> Option<f64> {
        let prefix = format!("{key}:");
        if !line.starts_with(&prefix) {
            return None;
        }
        let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
        Some(kb / 1024.0)
    }

    fn parse_status_count_line(line: &str, key: &str) -> Option<u64> {
        let prefix = format!("{key}:");
        if !line.starts_with(&prefix) {
            return None;
        }
        line.split_whitespace().nth(1)?.parse().ok()
    }

    fn read_open_fd_count(pid: i32) -> Option<f64> {
        let count = fs::read_dir(format!("/proc/{pid}/fd"))
            .ok()?
            .filter_map(|entry| entry.ok())
            .count();
        Some(count as f64)
    }

    fn read_io_read_bytes(pid: i32) -> Option<u64> {
        let content = fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
        for line in content.lines() {
            if line.starts_with("read_bytes:") {
                return line.split_whitespace().nth(1)?.parse().ok();
            }
        }
        None
    }

    fn read_mem_total_kb() -> Option<u64> {
        let content = fs::read_to_string("/proc/meminfo").ok()?;
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                return line.split_whitespace().nth(1)?.parse().ok();
            }
        }
        None
    }

    fn read_system_uptime_sec() -> Option<f64> {
        fs::read_to_string("/proc/uptime")
            .ok()?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }

    fn rate_per_sec(value: u64, now_ms: u128, sample: &mut Option<CounterSample>) -> Option<f64> {
        match sample {
            None => {
                *sample = Some(CounterSample {
                    value,
                    wall_ms: now_ms,
                });
                None
            }
            Some(prev) => {
                let delta = value.saturating_sub(prev.value);
                let delta_wall_ms = now_ms.saturating_sub(prev.wall_ms);
                prev.value = value;
                prev.wall_ms = now_ms;
                if delta_wall_ms == 0 {
                    return Some(0.0);
                }
                Some(delta as f64 / (delta_wall_ms as f64 / 1000.0))
            }
        }
    }

    fn clk_tck() -> f64 {
        let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if clk_tck > 0 {
            clk_tck as f64
        } else {
            warn!(
                clk_tck,
                "sysconf(_SC_CLK_TCK) unavailable, using default {DEFAULT_CLK_TCK}"
            );
            DEFAULT_CLK_TCK
        }
    }
}
