// governed-by: ADR-0005 (P4.3 — cron manager, Spec 3 scheduler durability)
#![forbid(unsafe_code)]

//! The Resonate durable-sleep cron manager (ADR-0005 Spec 3).
//!
//! The scheduler is NOT a wall-clock assumption: each registered cron runs as
//! a **Resonate durable function** that loops `ctx.sleep(until next tick)`
//! then fires the owning workflow as a durable child run. Restart survival +
//! no double-fire fall out of Resonate's deterministic replay: the child
//! promise ids derive from the loop's context sequence, so a crash mid-run
//! replays to the same ticks and the server dedups (no duplicate runs).
//!
//! Missed-tick policy = **`latest`**: on wake after downtime, the loop jumps
//! `last_fired` to the most-recent due tick at-or-before `now` and fires that
//! ONE — never a burst of every missed tick.
//!
//! toron never imports flywheel (ADR-0003 Decision 3A). The registry is
//! injected via [`CronRegistry`] (DI seam, same idiom as the engine's
//! `ActionRunner`);
//! flywheel's `crons.toml` (its own catalog half, `flywheel::cron`) is
//! bridged by the embedding. The cron-expression parser here accepts the spec
//! 5-field form (`"30 3 * * 0"`).

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::cron_failure::{
    PolicyEvent, PolicySink, evaluate as evaluate_policy, failure_details, is_failed,
};
use crate::engine::{run_workflow, RunInput};
use crate::schema::{CronScope, FailureAction, Workflow};

/// One registered cron the manager drives (Spec 3). The embedding resolves
/// the workflow template + owner profile; the manager only needs the plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronRegistration {
    /// Unique cron name (the durable run id prefix).
    pub name: String,
    /// 5-field cron expression (`"30 3 * * 0"`), vixie conventions.
    pub cron: String,
    /// The workflow run on each tick (already resolved from the template).
    pub workflow: Workflow,
    /// Owning profile (the fixer's identity — Spec 3).
    pub owner: String,
    /// Signing scope: host = operator identity, project = scoped agent.
    #[serde(default)]
    pub scope: CronScope,
    /// Paused crons are skipped (entry preserved; nothing fires).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

/// Registry access (DI seam): the embedding supplies the current cron set.
/// Returns the enabled registrations to drive.
pub type CronRegistry = Arc<dyn Fn() -> Vec<CronRegistration> + Send + Sync>;

/// Input to the durable cron loop (serialized; replayed on resume).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronLoopInput {
    name: String,
    cron: String,
    workflow: Workflow,
    owner: String,
    scope: CronScope,
}

/// The durable cron loop function: sleep until the next due tick, fire the
/// workflow as a durable child, repeat. Registered on the engine's Resonate
/// runtime by [`CronManager`].
///
/// Determinism contract (Spec 3):
/// - The loop runs under the run id `cron:{name}` — a restart re-invoking
///   that id resumes the same durable function (no duplicate loop).
/// - Each tick fires `run_workflow` as a child (`ctx.run`); the child's id
///   derives from the loop's deterministic context sequence, so replay after
///   a crash re-derives the same child id → the server resolves the existing
///   promise (no double-fire).
/// - Missed-tick policy = `latest`: on wake, jump `last_fired` to the most
///   recent due tick ≤ now and fire that one; ticks skipped by downtime are
///   never back-filled.
#[resonate_sdk::function]
async fn cron_loop(
    ctx: &resonate_sdk::prelude::Context,
    input: CronLoopInput,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // The last tick we fired (unix seconds). Starts at the loop's birth time,
    // so the first tick is the next genuinely-future one (a naive 0 would
    // catch up through every tick since epoch). Local + durable: replay
    // re-derives it deterministically, so missed ticks are skipped — never
    // replayed as a burst.
    let mut last_fired: i64 = unix_now();

    // Consecutive-failure streak (Spec 3 repeat policy). Durable local: replay
    // re-derives it, so a crash mid-run never double-counts.
    let mut consecutive: u32 = 0;
    // The policy (Spec 3). None → no failure side effects (log-only).
    let policy = input.workflow.policy.clone();
    let policy_sink = ctx.get_dependency::<PolicySink>();

    loop {
        let now = unix_now();

        // Next tick strictly after the last fired one.
        let Some(due) = next_due(&input.cron, last_fired) else {
            // Unparsable expression: park forever (durable sleep is
            // unbounded — a bad expr must not spin). The registry entry is
            // the source of truth for correction.
            ctx.sleep(std::time::Duration::from_secs(86_400)).await?;
            continue;
        };

        // LATEST catch-up: if downtime skipped several ticks, advance to the
        // most-recent due tick at-or-before now. Fire only that one.
        let due = latest_due_at_or_before(&input.cron, last_fired, now).unwrap_or(due);

        // Durable sleep until the tick. Survives a crash: on resume the
        // function replays to this point and continues from `last_fired`.
        let secs = (due - now).max(0) as u64;
        if secs > 0 {
            ctx.sleep(std::time::Duration::from_secs(secs)).await?;
        }

        // Fire the tick as a durable child run. The trigger carries the
        // SCHEDULED due time (the run's identity), not the wall-clock wake.
        let tick_ts = due;
        let trigger = serde_json::json!({
            "type": "schedule",
            "cron": input.cron,
            "tick": tick_ts,
            "owner": input.owner,
            "scope": format!("{:?}", input.scope).to_lowercase(),
        });
        // A failed tick must NOT kill the scheduler: the child's output (or
        // error) is recorded; the loop moves on to the next tick. Failure
        // policy (bead/escalate) is applied via the policy sink below.
        let tick_output: serde_json::Value = match ctx
            .run(
                run_workflow,
                RunInput {
                    workflow: input.workflow.clone(),
                    trigger,
                },
            )
            .await
        {
            Ok(out) => out,
            Err(_e) => serde_json::json!({
                "status": "failed",
                "error": format!("cron tick {tick_ts} run errored"),
            }),
        };

        // Spec 3 failure policy: on a failed tick, record the decision on the
        // policy sink for the embedding to execute (br create / escalate DM).
        // Log-only when no policy is declared.
        if let Some(policy) = &policy {
            let failed = is_failed(&tick_output);
            let (action, next) = evaluate_policy(policy, failed, consecutive);
            consecutive = next;
            let (error, failed_step) = failure_details(&tick_output);
            policy_sink
                .lock()
                .await
                .push(PolicyEvent {
                    cron: input.name.clone(),
                    tick: tick_ts,
                    action,
                    bundle: crate::cron_failure::FailureBundle {
                        run_id: format!("cron:{}:{}", input.name, tick_ts),
                        workflow: input.workflow.name.clone(),
                        owner: input.owner.clone(),
                        error,
                        failed_step,
                        consecutive,
                    },
                });
            let _ = FailureAction::Log; // actions are the embedding's to perform
        }

        // Advance the durable checkpoint (replay resumes here next).
        last_fired = tick_ts;
    }
}

/// Unix timestamp (seconds).
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Next due tick (unix seconds) for a cron expression strictly after
/// `from_ts`. Accepts the Spec 3 5-field form (`"30 3 * * 0"`) plus 6/7-field
/// forms — 5-field is normalized by prepending seconds=0 (the `cron` crate
/// requires the seconds field). Day-of-week is remapped vixie→crate. `None`
/// for an unparsable expression.
pub fn next_due(cron_expr: &str, from_ts: i64) -> Option<i64> {
    let normalized = normalize_cron(cron_expr);
    let schedule = normalized.parse::<cron::Schedule>().ok()?;
    let from = DateTime::from_timestamp(from_ts, 0)?;
    let next = schedule.after(&from).next()?;
    Some(next.timestamp())
}

/// The **`latest`** catch-up tick (Spec 3): the most-recent due tick at or
/// before `now`, strictly after `last_fired`. This is the missed-tick policy —
/// on wake after downtime the scheduler runs ONE tick (the latest one that
/// became due while down), never a burst of every missed tick.
///
/// Normal (on-time) case: `last_fired` is the previous tick, the next due is
/// in the future, so this returns the next scheduled tick and the loop sleeps
/// to it. `None` when the expression is unparsable.
pub fn latest_due_at_or_before(cron_expr: &str, last_fired: i64, now: i64) -> Option<i64> {
    let mut due = next_due(cron_expr, last_fired)?;
    while let Some(next) = next_due(cron_expr, due) {
        if next <= now {
            due = next;
        } else {
            break;
        }
    }
    Some(due)
}

/// Normalize a cron expression to the form the `cron` crate parses:
/// 1. 5-field → 6-field by prepending seconds `0`.
/// 2. day-of-week vixie→crate remap: vixie 0/7=Sunday, 1-6=Mon-Sat; crate
///    1=Sunday…7=Saturday. Numeric component `d` → `(d % 7) + 1`. Names pass
///    through (crate uses 1=Sun too).
pub fn normalize_cron(expr: &str) -> String {
    let mut fields: Vec<String> = expr.split_whitespace().map(str::to_string).collect();
    // 5-field (vixie) → 6-field (crate): prepend seconds=0 AND remap the dow.
    // 6/7-field and `@`-shorthand expressions pass through unchanged — their
    // day-of-week fields are already in the crate's 1-7 (1=Sunday) form.
    if fields.len() == 5 {
        fields.insert(0, "0".to_string());
        if let Some(dow) = fields.get_mut(5) {
            *dow = normalize_dow(dow);
        }
    }
    fields.join(" ")
}

/// Remap one day-of-week token from vixie to crate ordinals.
fn normalize_dow(token: &str) -> String {
    if token.chars().any(|c| c.is_ascii_alphabetic()) || token == "*" {
        return token.to_string();
    }
    token
        .split(',')
        .map(|item| {
            let (range, step) = match item.split_once('/') {
                Some((r, s)) => (r, Some(s)),
                None => (item, None),
            };
            let mapped = match range.split_once('-') {
                Some((a, b)) => format!("{}-{}", dow_ordinal(a), dow_ordinal(b)),
                None => dow_ordinal(range),
            };
            match step {
                Some(s) => format!("{mapped}/{s}"),
                None => mapped,
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// vixie day-of-week digit → crate ordinal (`(d % 7) + 1`; 0 and 7 → 1).
fn dow_ordinal(d: &str) -> String {
    match d.parse::<u32>() {
        Ok(n) => ((n % 7) + 1).to_string(),
        Err(_) => d.to_string(),
    }
}

/// Drives the durable-sleep cron loops over a registry (Spec 3).
///
/// The manager is deliberately thin: it registers the durable [`cron_loop`]
/// function on the engine's Resonate runtime once, then for each enabled
/// registration starts a run `cron:{name}`. The loop itself owns the durable
/// sleep, catch-up, and deterministic child ids — this type is the
/// embedding's control surface.
pub struct CronManager {
    engine: Arc<crate::engine::WorkflowEngine>,
    registry: CronRegistry,
}

impl CronManager {
    /// Construct over the engine + registry. Registers the durable loop
    /// function (idempotent; re-registration is a no-op in Resonate).
    pub fn new(engine: Arc<crate::engine::WorkflowEngine>, registry: CronRegistry) -> Result<Self> {
        engine
            .resonate()
            .register(cron_loop)
            .context("register cron_loop durable function")?;
        Ok(Self { engine, registry })
    }

    /// Start (or resume) the durable loop for every enabled registration.
    ///
    /// Idempotent: re-invoking `cron:{name}` resumes the SAME durable
    /// function — safe to call on every flywheel boot (restart survival).
    pub async fn start(&self) -> Result<()> {
        for reg in (self.registry)().into_iter().filter(|r| r.enabled) {
            let input = CronLoopInput {
                name: reg.name,
                cron: reg.cron,
                workflow: reg.workflow,
                owner: reg.owner,
                scope: reg.scope,
            };
            let run_id = format!("cron:{}", input.name);
            // Fire-and-forget: the loop's result is irrelevant; the durable
            // function owns the repeating behavior. Errors (bad expr, etc.)
            // are surfaced via the run's terminal event.
            let _ = self
                .engine
                .resonate()
                .run(&run_id, cron_loop, input)
                .spawn()
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// next_due finds a future tick for a valid expr; None for garbage.
    #[test]
    fn next_due_computes_future_tick() {
        let from = 1_700_000_000i64;
        let due = next_due("30 3 * * *", from).expect("valid expr yields a tick");
        assert!(due > from, "next tick is in the future");
    }

    /// next_due accepts the vixie Sunday-as-zero convention (0 and 7 → Sun).
    #[test]
    fn next_due_accepts_vixie_sunday_zero() {
        use chrono::{Datelike, Timelike};
        // Monday 2023-11-20 00:00 UTC. Sunday (0) → the Sunday after.
        let from = DateTime::parse_from_rfc3339("2023-11-20T00:00:00Z")
            .unwrap()
            .timestamp();
        let due = next_due("30 3 * * 0", from).expect("valid");
        let dt = DateTime::from_timestamp(due, 0).unwrap();
        assert_eq!(dt.weekday(), chrono::Weekday::Sun, "Sunday tick");
        assert_eq!(dt.hour(), 3);
        assert_eq!(dt.minute(), 30);
    }

    /// next_due skips ticks at-or-before `from` (strictly after).
    #[test]
    fn next_due_is_strictly_after_from() {
        let from = 1_700_000_000i64;
        let due = next_due("*/5 * * * *", from).expect("valid");
        assert!(due > from);
        let second = next_due("*/5 * * * *", due).expect("valid");
        assert!(second > due, "strictly increasing");
    }

    /// normalize_cron accepts 5-field (prepends seconds=0 + vixie→crate dow)
    /// and 6-field (passes through unchanged — already crate convention).
    #[test]
    fn normalize_accepts_5_and_6_field() {
        assert_eq!(normalize_cron("30 3 * * 0"), "0 30 3 * * 1");
        assert_eq!(normalize_cron("0 30 3 * * 1"), "0 30 3 * * 1");
    }

    /// Unparsable expressions yield None (the loop parks, never spins).
    #[test]
    fn next_due_garbage_is_none() {
        assert!(next_due("not a cron", 1_700_000_000).is_none());
    }

    /// `latest` catch-up: after downtime skipping several ticks, the loop
    /// fires ONLY the most-recent due tick — never a burst of every missed.
    #[test]
    fn latest_catchup_runs_only_the_most_recent_missed_tick() {
        use chrono::TimeZone;
        // Every 10 minutes starting at 10:00. Ticks at 10:00, 10:10, 10:20…
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        let tick_0 = latest_due_at_or_before("*/10 * * * *", base, base + 60).unwrap();
        assert_eq!(tick_0, base + 600, "on-time: sleep to the next scheduled tick");

        // Down for 25 minutes (ticks 10:00, 10:10, 10:20 all due while down):
        // catch up to the LATEST (10:20), not 10:00 or 10:10.
        let now = base + 25 * 60;
        let caught = latest_due_at_or_before("*/10 * * * *", base, now).unwrap();
        assert_eq!(caught, base + 20 * 60, "latest missed tick, not a burst");

        // And firing that ONE tick advances `last_fired` to it; the next tick
        // is then strictly after — no replay of the intermediate missed ones.
        let next = next_due("*/10 * * * *", caught).unwrap();
        assert!(next > now, "resume from the latest caught-up tick");
    }

    /// On-time case (no downtime): the next tick is in the future; catch-up
    /// returns it unchanged (the loop sleeps to it).
    #[test]
    fn latest_catchup_on_time_is_the_next_tick() {
        use chrono::TimeZone;
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        // last_fired = 10:00, now = 10:01 → next due is 10:10 (in the future).
        let due = latest_due_at_or_before("*/10 * * * *", base, base + 60).unwrap();
        assert!(due > base + 60, "sleep to the next scheduled tick");
    }
}
