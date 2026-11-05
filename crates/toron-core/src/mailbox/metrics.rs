// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Mailbox metrics + robot-mode state core (bd-led.8.4, G10).
//!
//! The AM-Rust parity contract requires **structured metrics** — call counts,
//! errors, latency histograms — observable to agents via `--json`. The
//! instrumentation lives here (toron-core::mailbox) so the CLI (`toron metrics`,
//! `toron bench`) and any embedding (flywheel) share one source of truth.
//!
//! Robot mode (ADR-0003 CLI design): `--robot-snapshot` / `--robot-status` /
//! `--robot-capabilities` emit JSON state snapshots an agent driver can poll.
//! The snapshot builders produce the JSON shapes; the CLI (oc-c lane) wires the
//! flags.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;
use serde::Serialize;

/// Latency histogram buckets (µs) — AM-Rust parity spirit: coarse buckets an
/// agent can plot without a time-series backend.
pub const LATENCY_BUCKETS_US: [u64; 8] =
    [100, 500, 1_000, 5_000, 10_000, 50_000, 200_000, u64::MAX];

/// One tool's call/error/latency counters.
#[derive(Debug, Default)]
pub struct ToolMetrics {
    /// Successful call count.
    pub calls: AtomicU64,
    /// Failed call count.
    pub errors: AtomicU64,
    /// Latency histogram (bucket index → count). Buckets in
    /// [`LATENCY_BUCKETS_US`]; the last bucket is `>200ms`.
    pub latency_hist: Mutex<[u64; LATENCY_BUCKETS_US.len()]>,
}

impl ToolMetrics {
    /// Record a completed call with its latency. A call is counted regardless
    /// of outcome; `ok` only toggles the error tally.
    pub fn record(&self, ok: bool, elapsed_us: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        let bucket = LATENCY_BUCKETS_US
            .iter()
            .position(|b| elapsed_us <= *b)
            .unwrap_or(LATENCY_BUCKETS_US.len() - 1);
        self.latency_hist.lock()[bucket] += 1;
    }
}

/// Aggregate mailbox metrics: per-tool + totals.
#[derive(Debug, Default)]
pub struct MailboxMetrics {
    /// Per-tool metrics keyed by tool name.
    pub per_tool: Mutex<std::collections::HashMap<String, ToolMetrics>>,
}

/// A serializable snapshot of the counters.
#[derive(Debug, Default, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub tools: std::collections::BTreeMap<String, ToolCounters>,
    pub total_calls: u64,
    pub total_errors: u64,
}

/// Per-tool counters in the snapshot.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ToolCounters {
    pub calls: u64,
    pub errors: u64,
    pub latency_hist: Vec<u64>,
}

impl MailboxMetrics {
    /// New empty metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a tool call outcome.
    pub fn record(&self, tool: &str, ok: bool, elapsed_us: u64) {
        self.per_tool
            .lock()
            .entry(tool.to_string())
            .or_default()
            .record(ok, elapsed_us);
    }

    /// Time a closure and record its outcome under `tool` (imperative-shell
    /// wrapper; the functional core stays in the closure).
    pub fn time_call<T, F: FnOnce() -> Result<T, anyhow::Error>>(
        &self,
        tool: &str,
        f: F,
    ) -> Result<T, anyhow::Error> {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed().as_micros() as u64;
        self.record(tool, result.is_ok(), elapsed);
        result
    }

    /// Serializable snapshot (the `metrics` / `--robot-status` payload).
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut out = MetricsSnapshot {
            total_calls: 0,
            total_errors: 0,
            tools: std::collections::BTreeMap::new(),
        };
        {
            let tools = self.per_tool.lock();
            for (name, m) in tools.iter() {
                out.tools.insert(
                    name.clone(),
                    ToolCounters {
                        calls: m.calls.load(Ordering::Relaxed),
                        errors: m.errors.load(Ordering::Relaxed),
                        latency_hist: m.latency_hist.lock().to_vec(),
                    },
                );
                out.total_calls += m.calls.load(Ordering::Relaxed);
                out.total_errors += m.errors.load(Ordering::Relaxed);
            }
        }
        out
    }

    /// `--robot-status`: minimal status (compact).
    pub fn robot_status(&self) -> serde_json::Value {
        let snap = self.snapshot();
        serde_json::json!({
            "ok": snap.total_errors == 0,
            "tools": snap.tools.len(),
            "calls": snap.total_calls,
            "errors": snap.total_errors,
        })
    }

    /// `--robot-capabilities`: capability/role map.
    ///
    /// ADR-0020 Spec 3: the CLI plane nouns (`channel`, `job`, `react`,
    /// `presence`, `watch`) are shell commands, NOT MCP tools — the frozen
    /// 39-tool cluster never grows. They are listed under a separate `cli`
    /// cluster so a driver knows the Nostr two-plane surface exists.
    pub fn robot_capabilities(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": "am-rust-parity",
            "tool_clusters": {
                "infrastructure": ["health_check", "ensure_project", "install_precommit_guard", "uninstall_precommit_guard"],
                "identity": ["create_agent_identity", "register_agent", "whois", "list_agents", "resolve_pane_identity", "cleanup_pane_identities"],
                "messaging": ["send_message", "reply_message", "fetch_inbox", "acknowledge_message", "mark_message_read"],
                "contacts": ["request_contact", "respond_contact", "set_contact_policy", "list_contacts"],
                "reservations": ["file_reservation_paths", "renew_file_reservations", "release_file_reservations", "check_file_reservation_conflicts", "force_release_file_reservation"],
                "search": ["search_messages", "summarize_thread"],
                "macros": ["macro_start_session", "macro_prepare_thread", "macro_contact_handshake", "macro_file_reservation_cycle"],
                "product_bus": ["ensure_product", "products_link", "search_messages_product", "fetch_inbox_product", "summarize_thread_product"],
                "build_slots": ["acquire_build_slot", "renew_build_slot", "release_build_slot"],
            },
            "cli": {
                "plane_s": ["channel", "react"],
                "plane_j": ["job"],
                "plane_n": ["presence"],
                "plane_w": ["watch"],
            },
            "json": true,
            "stdin_dash": true,
        })
    }

    /// `--robot-snapshot`: full state snapshot (metrics + capabilities +
    /// envelope TTL + runtime identity).
    pub fn robot_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "status": self.robot_status(),
            "capabilities": self.robot_capabilities(),
            "metrics": self.snapshot(),
            "runtime": {
                "envelope_ttl_days": (crate::bus::DEFAULT_ENVELOPE_TTL_SECS / 86_400),
                "latency_buckets_us": LATENCY_BUCKETS_US,
            },
        })
    }
}

/// A bench scenario timer: measures one operation's latency + records it.
#[derive(Debug)]
pub struct BenchTimer {
    tool: String,
    start: Instant,
}

impl BenchTimer {
    /// Start a bench scenario timing.
    pub fn start(tool: impl Into<String>) -> Self {
        Self {
            tool: tool.into(),
            start: Instant::now(),
        }
    }

    /// Finish the timing and record into `metrics`.
    pub fn finish(self, metrics: &MailboxMetrics, ok: bool) {
        metrics.record(&self.tool, ok, self.start.elapsed().as_micros() as u64);
    }
}

/// Bench scenario results (AM-Rust parity: startup/inbox/send/search/threads).
#[derive(Debug, Default, Serialize)]
pub struct BenchReport {
    pub scenarios: Vec<BenchCase>,
}

/// One bench case result.
#[derive(Debug, Default, Serialize)]
pub struct BenchCase {
    pub name: String,
    pub iterations: u64,
    pub total_ms: u64,
    pub avg_us: u64,
    pub p50_us: u64,
    pub p95_us: u64,
}

impl BenchReport {
    /// Build a report from raw per-iteration latencies (µs).
    pub fn case(name: &str, latencies: &[u64]) -> BenchCase {
        let n = latencies.len().max(1) as u64;
        let total_us: u64 = latencies.iter().sum();
        let mut sorted = latencies.to_vec();
        sorted.sort_unstable();
        let pct =
            |p: f64| sorted[((sorted.len() as f64) * p).min(sorted.len() as f64 - 1.0) as usize];
        BenchCase {
            name: name.to_string(),
            iterations: n,
            total_ms: total_us / 1_000,
            avg_us: total_us / n,
            p50_us: pct(0.50),
            p95_us: pct(0.95),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_record_increments() {
        let m = MailboxMetrics::new();
        m.record("health_check", true, 150);
        m.record("health_check", true, 2_000);
        m.record("send_message", false, 900);

        let snap = m.snapshot();
        assert_eq!(snap.tools["health_check"].calls, 2);
        assert_eq!(snap.tools["health_check"].errors, 0);
        assert_eq!(snap.tools["send_message"].errors, 1);
        assert_eq!(snap.total_calls, 3);
        assert_eq!(snap.total_errors, 1);
    }

    #[test]
    fn latency_histogram_buckets() {
        let m = MailboxMetrics::new();
        m.record("x", true, 50);
        m.record("x", true, 250);
        m.record("x", true, 12_000);
        m.record("x", true, 999_999);
        let snap = m.snapshot();
        let hist = &snap.tools["x"].latency_hist;
        assert_eq!(hist[0], 1);
        assert_eq!(hist[1], 1);
        assert_eq!(hist[5], 1);
        assert_eq!(hist[7], 1);
    }

    #[test]
    fn time_call_records_success_and_failure() {
        let m = MailboxMetrics::new();
        let ok = m.time_call("whois", || Ok::<_, anyhow::Error>(42)).unwrap();
        assert_eq!(ok, 42);
        let err = m.time_call("whois", || Err::<(), _>(anyhow::anyhow!("boom")));
        assert!(err.is_err());
        let snap = m.snapshot();
        assert_eq!(snap.tools["whois"].calls, 2, "both calls counted");
        assert_eq!(snap.tools["whois"].errors, 1);
    }

    #[test]
    fn robot_json_shapes_valid() {
        let m = MailboxMetrics::new();
        m.record("health_check", true, 100);
        let status = m.robot_status();
        let caps = m.robot_capabilities();
        let snap = m.robot_snapshot();
        assert_eq!(status["ok"], true);
        assert_eq!(status["calls"], 1);
        assert!(caps["tool_clusters"]["messaging"].is_array());
        assert_eq!(snap["status"]["calls"], 1);
        assert!(snap["capabilities"]["json"].as_bool().unwrap());
    }

    #[test]
    fn bench_report_percentiles() {
        let lat: Vec<u64> = (0..100).map(|i| i * 10).collect();
        let case = BenchReport::case("inbox", &lat);
        assert_eq!(case.iterations, 100);
        assert_eq!(case.p50_us, 500);
        assert_eq!(case.p95_us, 950);
        assert_eq!(case.avg_us, 495);
    }

    /// F4.LOCK-ORDER pin (bd-p1-lock-ordering-deadlock-71po): the ONLY nested
    /// multi-lock hold in the tree is `snapshot()` (per_tool → latency_hist).
    /// Hammer it concurrently with `record()` (latency_hist alone) under a
    /// bounded wall-clock — a lock-order inversion (a future change acquiring
    /// latency_hist → per_tool) would deadlock and trip the bound.
    #[test]
    fn snapshot_concurrent_with_record_no_deadlock() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let m = Arc::new(MailboxMetrics::new());
        let done = Arc::new(AtomicBool::new(false));
        let done_work = done.clone();
        let worker = std::thread::spawn(move || {
            let mut handles = Vec::new();
            for i in 0..8 {
                let m = m.clone();
                handles.push(std::thread::spawn(move || {
                    for k in 0..500 {
                        m.record(&format!("tool-{i}"), true, k);
                        let _ = m.snapshot();
                    }
                }));
            }
            for h in handles {
                h.join().expect("metrics worker no panic");
            }
            done_work.store(true, Ordering::Relaxed);
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !done.load(Ordering::Relaxed) {
            assert!(
                std::time::Instant::now() < deadline,
                "metrics snapshot/record deadlock (F4.LOCK-ORDER per_tool -> latency_hist)"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        worker.join().expect("metrics stress thread no panic");
    }
}
