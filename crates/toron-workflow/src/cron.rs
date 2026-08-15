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
//! **Replay determinism (bd-25g7.2):** the loop's per-iteration decisions are
//! recorded as durable promises, so a restart mid-sleep resumes EXACTLY where
//! the original execution left off:
//! - The catch-up decision (which tick to fire) runs as the durable
//!   [`decide_tick`] child — replay returns the RECORDED decision, never a
//!   re-derived wall-clock one (a phantom policy event / early-fire would
//!   otherwise follow a reboot).
//! - The tick body + failure-policy side effects run as the durable
//!   [`run_cron_tick`] child — replay resolves the recorded child, so the
//!   policy event is pushed exactly once per real tick.
//! - Every iteration makes the SAME terminal-op calls in the same order
//!   (decide → sleep → tick; the sleep is issued even for a zero-duration
//!   catch-up so the call pattern cannot diverge across replay).
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
use crate::engine::{RunInput, run_workflow};
use crate::schema::{
    CronCatchup, CronPolicy, CronScope, CronSpec, CronSpecPolicy, FailureAction, Workflow,
};

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

/// One ADR-0012 spec-driven cron the manager drives (P12.2, bd-25g7.2): the
/// schedule + owner + policy + steps body come from the [`CronSpec`] itself —
/// the spec IS the registration. Runs under the `cron-spec:{name}` run-id
/// namespace (no collision with legacy `cron:{name}` loops).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronSpecRegistration {
    /// The cron definition (its `name` is the identity + run-id suffix).
    pub spec: CronSpec,
    /// Paused crons are skipped (entry preserved; nothing fires).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Test/embedding hook: the loop's initial `last_fired` (unix seconds).
    /// Default = the loop's birth time (first tick is the next genuinely
    /// future one). Deterministic — stored in the loop input, replayed
    /// verbatim on resume.
    #[serde(default)]
    pub start_ts: Option<i64>,
}

/// Registry access for spec-driven crons (ADR-0012 DI seam, mirrors
/// [`CronRegistry`]).
pub type CronSpecRegistry = Arc<dyn Fn() -> Vec<CronSpecRegistration> + Send + Sync>;

/// Input to the durable cron loop (serialized; replayed on resume).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronLoopInput {
    name: String,
    cron: String,
    /// The tick body: the bound workflow (legacy) or the spec-derived
    /// workflow (ADR-0012 — `spec_tick_workflow`). The loop itself is
    /// schedule-only; the body runs via the shared engine.
    workflow: Workflow,
    owner: String,
    scope: CronScope,
    /// Effective missed-tick policy (spec mode; legacy = `Latest` — the
    /// pre-ADR-0012 behavior).
    #[serde(default)]
    catchup: CronCatchup,
    /// Failure-bundle run-id prefix (`cron` / `cron-spec` — matches the
    /// loop's run-id namespace).
    #[serde(default = "default_run_prefix")]
    run_prefix: String,
    /// The loop's initial `last_fired` (see [`CronSpecRegistration::start_ts`]).
    #[serde(default)]
    start_ts: Option<i64>,
}

fn default_run_prefix() -> String {
    "cron".to_string()
}

/// Input to the durable catch-up decision ([`decide_tick`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecideInput {
    cron: String,
    last_fired: i64,
    now: i64,
    catchup: CronCatchup,
}

/// The durable catch-up decision: which tick to fire next, under the
/// effective catch-up policy. Run as a child of the loop so the decision is
/// RECORDED — a replay returns the original decision instead of re-deriving
/// one from the (changed) wall clock after a restart.
#[resonate_sdk::function]
async fn decide_tick(
    _ctx: &resonate_sdk::prelude::Context,
    input: DecideInput,
) -> resonate_sdk::Result<Option<i64>> {
    Ok(due_tick(
        &input.cron,
        input.last_fired,
        input.now,
        input.catchup,
    ))
}

/// Input to the durable tick body ([`run_cron_tick`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronTickInput {
    name: String,
    cron: String,
    /// The tick body workflow (bound or spec-derived) run through the shared
    /// engine. Its `policy` drives the failure-policy side effects.
    workflow: Workflow,
    owner: String,
    scope: CronScope,
    /// The SCHEDULED tick this run fires for (the run's identity).
    tick: i64,
    /// Failure-bundle run-id prefix (`cron` / `cron-spec`).
    run_prefix: String,
    /// The consecutive-failure streak entering this tick (Spec 3 repeat
    /// policy; the loop carries it across ticks via the recorded result).
    consecutive: u32,
}

/// The tick child's recorded result (replayed verbatim on resume).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronTickResult {
    /// The tick that fired (advances the loop's `last_fired`).
    tick: i64,
    /// The consecutive-failure streak AFTER this tick (Spec 3).
    consecutive: u32,
    /// The tick body's output (the policy event rides along with it).
    output: serde_json::Value,
}

/// The durable tick body: runs the tick workflow through the SHARED engine
/// and applies the failure-policy side effects (PolicySink push). A child of
/// the loop — replay resolves the recorded child, so the workflow runs and
/// the policy event is pushed exactly once per real tick (never re-played
/// into a phantom event after a restart).
#[resonate_sdk::function]
async fn run_cron_tick(
    ctx: &resonate_sdk::prelude::Context,
    input: CronTickInput,
) -> resonate_sdk::Result<CronTickResult> {
    let trigger = serde_json::json!({
        "type": "schedule",
        "cron": input.cron,
        "tick": input.tick,
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
            "error": format!("cron tick {} run errored", input.tick),
        }),
    };

    // Spec 3 failure policy: on a failed tick, record the decision on the
    // policy sink for the embedding to execute (br create / escalate DM).
    // Log-only when no policy is declared.
    let mut consecutive = input.consecutive;
    if let Some(policy) = &input.workflow.policy {
        let failed = is_failed(&tick_output);
        let (action, next) = evaluate_policy(policy, failed, consecutive);
        consecutive = next;
        let (error, failed_step) = failure_details(&tick_output);
        ctx.get_dependency::<PolicySink>()
            .lock()
            .await
            .push(PolicyEvent {
                cron: input.name.clone(),
                tick: input.tick,
                action,
                bundle: crate::cron_failure::FailureBundle {
                    run_id: format!("{}:{}:{}", input.run_prefix, input.name, input.tick),
                    workflow: input.workflow.name.clone(),
                    owner: input.owner.clone(),
                    error,
                    failed_step,
                    consecutive,
                },
                // bd-m7ac: the full tick output (the oracle step's
                // synthesis for weekly-review) rides along so the
                // embedding can perform success side-effects.
                output: Some(tick_output.clone()),
            });
        let _ = FailureAction::Log; // actions are the embedding's to perform
    }

    Ok(CronTickResult {
        tick: input.tick,
        consecutive,
        output: tick_output,
    })
}

/// The tick body for a spec-driven loop: a [`Workflow`] built from the
/// CronSpec's steps/graph/policy — the SAME shape the shared engine runs
/// (one engine, two input types; `call_workflow` composes workflows).
/// No trigger: the loop synthesizes the schedule trigger per tick. Public so
/// the embedding (flywheel `cron run`/lint — P12.3) drives the same tick body
/// the loop fires.
pub fn spec_tick_workflow(spec: &CronSpec) -> Workflow {
    Workflow {
        name: spec.name.clone(),
        description: spec.description.clone(),
        trigger: None,
        policy: spec.policy.as_ref().map(spec_policy_to_cron_policy),
        guard: None,
        steps: spec.steps.clone(),
        graph: spec.graph.clone(),
        inputs: Vec::new(),
        crew: None,
    }
}

/// Map the cron-specific policy (which carries `catchup`, ADR-0012) to the
/// workflow policy the loop's failure-policy machinery consumes.
fn spec_policy_to_cron_policy(p: &CronSpecPolicy) -> CronPolicy {
    CronPolicy {
        on_failure: p.on_failure,
        on_repeat_failure: p.on_repeat_failure,
        repeat_threshold: p.repeat_threshold,
        on_success: p.on_success,
        max_retries: p.max_retries,
        no_claim_timeout: p.no_claim_timeout,
    }
}

/// The effective missed-tick policy for a spec (default `Latest`).
fn spec_catchup(spec: &CronSpec) -> CronCatchup {
    spec.policy
        .as_ref()
        .map(|p| p.catchup)
        .unwrap_or(CronCatchup::Latest)
}

/// The next tick to FIRE under the effective catch-up policy (pure):
/// - `Latest` (ADR-0005 Spec 3): the most-recent due tick at-or-before `now`
///   strictly after `last_fired` — after downtime, ONE tick (never a burst);
///   on-time, the next scheduled tick in the future.
/// - `Skip` (ADR-0012): never fire a missed tick — the first due tick
///   strictly after `last_fired` when it is still in the future, otherwise
///   the first tick after `now` (downtime ticks are dropped).
///
/// `None` for an unparsable expression (the loop parks, never spins).
fn due_tick(cron: &str, last_fired: i64, now: i64, catchup: CronCatchup) -> Option<i64> {
    match catchup {
        CronCatchup::Latest => {
            latest_due_at_or_before(cron, last_fired, now).or_else(|| next_due(cron, last_fired))
        }
        CronCatchup::Skip => {
            let due = next_due(cron, last_fired)?;
            if due <= now {
                next_due(cron, now)
            } else {
                Some(due)
            }
        }
    }
}

/// The durable cron loop function: sleep until the next due tick, fire the
/// tick body as a durable child, repeat. Registered on the engine's Resonate
/// runtime by [`CronManager`]. ONE loop, TWO input shapes (legacy
/// workflow-binding + ADR-0012 CronSpec) — the shared engine runs both.
///
/// Determinism contract (Spec 3 + bd-25g7.2):
/// - The loop runs under the run id `cron:{name}` / `cron-spec:{name}` — a
///   restart re-invoking that id resumes the same durable function (no
///   duplicate loop).
/// - Every iteration makes the SAME terminal-op calls in the SAME order:
///   `ctx.run(decide_tick)` → `ctx.sleep(...)` → `ctx.run(run_cron_tick)`.
///   Replay resolves each call at its recorded promise position, so a
///   restart mid-sleep resumes at the recorded timer, the recorded decision,
///   and the recorded tick result — never a re-derived wall-clock one.
/// - The catch-up decision (which tick to fire) is a RECORDED child result:
///   on wake after downtime, replay returns the ORIGINAL decision (the
///   most-recent due tick at-or-before the original wake), so the loop fires
///   exactly ONE tick — never a burst of every missed tick.
/// - The tick body + policy side effects live in the [`run_cron_tick`] child:
///   replay resolves the recorded child, so the workflow runs and the policy
///   event is pushed exactly once per real tick (no phantom events on
///   resume).
#[resonate_sdk::function]
async fn cron_loop(
    ctx: &resonate_sdk::prelude::Context,
    input: CronLoopInput,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // The last tick we fired (unix seconds). Starts at the loop's birth time
    // (or the input's `start_ts` hook), so the first tick is the next
    // genuinely-future one (a naive 0 would catch up through every tick
    // since epoch). Local + durable: replay re-derives it from the recorded
    // tick results, so missed ticks are skipped — never replayed as a burst.
    let mut last_fired: i64 = input.start_ts.unwrap_or_else(unix_now);

    // Consecutive-failure streak (Spec 3 repeat policy). Durable local:
    // replay re-derives it from the recorded tick results, so a crash
    // mid-run never double-counts.
    let mut consecutive: u32 = 0;

    loop {
        let now = unix_now();

        // The next tick to FIRE under the effective catch-up policy — a
        // RECORDED decision (replay returns the original). An unparsable
        // expression → park forever (durable sleep is unbounded — a bad
        // expr must not spin).
        let due = ctx
            .run(
                decide_tick,
                DecideInput {
                    cron: input.cron.clone(),
                    last_fired,
                    now,
                    catchup: input.catchup,
                },
            )
            .await?;
        let Some(due) = due else {
            ctx.sleep(std::time::Duration::from_secs(86_400)).await?;
            continue;
        };

        // Durable sleep until the tick. Issued EVERY iteration (even a
        // zero-duration catch-up fire) so the terminal-op call pattern is
        // identical across replay — an existing timer record wins on resume
        // (idempotent create), so the ORIGINAL timer governs.
        let secs = (due - now).max(0) as u64;
        ctx.sleep(std::time::Duration::from_secs(secs)).await?;

        // Fire the tick body (workflow run + policy side effects) as a
        // durable child. On replay the RECORDED child resolves — the tick is
        // never re-executed and its policy event never re-pushed.
        let result = match ctx
            .run(
                run_cron_tick,
                CronTickInput {
                    name: input.name.clone(),
                    cron: input.cron.clone(),
                    workflow: input.workflow.clone(),
                    owner: input.owner.clone(),
                    scope: input.scope,
                    tick: due,
                    run_prefix: input.run_prefix.clone(),
                    consecutive,
                },
            )
            .await
        {
            Ok(r) => r,
            // Function-level failure (a rejected promise — only reachable via
            // a panic inside the tick child). Keep the scheduler alive and
            // advance past the tick; the failure bundle is lost (the policy
            // side effect lives in the child's first execution only).
            Err(_e) => CronTickResult {
                tick: due,
                consecutive,
                output: serde_json::json!({
                    "status": "failed",
                    "error": format!("cron tick {due} function errored"),
                }),
            },
        };

        // Advance the durable checkpoint (replay resumes here next).
        last_fired = result.tick;
        consecutive = result.consecutive;
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
/// registration starts a run `cron:{name}` (legacy) / `cron-spec:{name}`
/// (ADR-0012 CronSpec). The loop itself owns the durable sleep, catch-up, and
/// deterministic child ids — this type is the embedding's control surface.
pub struct CronManager {
    engine: Arc<crate::engine::WorkflowEngine>,
    registry: CronRegistry,
    spec_registry: Option<CronSpecRegistry>,
}

impl CronManager {
    /// Construct over the engine + registry. Registers the durable loop
    /// function (idempotent; re-registration is a no-op in Resonate).
    pub fn new(engine: Arc<crate::engine::WorkflowEngine>, registry: CronRegistry) -> Result<Self> {
        Self::with_spec_registry(engine, registry, None)
    }

    /// Like [`Self::new`] with an ADR-0012 spec registry: the manager also
    /// drives `CronSpec`-based loops (`cron-spec:{name}` run ids, no
    /// collision with the legacy `cron:{name}` namespace).
    pub fn with_spec_registry(
        engine: Arc<crate::engine::WorkflowEngine>,
        registry: CronRegistry,
        spec_registry: Option<CronSpecRegistry>,
    ) -> Result<Self> {
        engine
            .resonate()
            .register(cron_loop)
            .context("register cron_loop durable function")?;
        engine
            .resonate()
            .register(decide_tick)
            .context("register decide_tick durable function")?;
        engine
            .resonate()
            .register(run_cron_tick)
            .context("register run_cron_tick durable function")?;
        Ok(Self {
            engine,
            registry,
            spec_registry,
        })
    }

    /// Start (or resume) the durable loop for every enabled registration —
    /// legacy AND spec-driven (when a spec registry is wired).
    ///
    /// Idempotent: re-invoking `cron:{name}` / `cron-spec:{name}` resumes the
    /// SAME durable function — safe to call on every flywheel boot (restart
    /// survival).
    pub async fn start(&self) -> Result<()> {
        for reg in (self.registry)().into_iter().filter(|r| r.enabled) {
            let input = CronLoopInput {
                name: reg.name,
                cron: reg.cron,
                workflow: reg.workflow,
                owner: reg.owner,
                scope: reg.scope,
                catchup: CronCatchup::Latest,
                run_prefix: "cron".to_string(),
                start_ts: None,
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
        if let Some(specs) = &self.spec_registry {
            for reg in specs().into_iter().filter(|r| r.enabled) {
                let spec = reg.spec;
                let name = spec.name.clone();
                let input = CronLoopInput {
                    name: name.clone(),
                    cron: spec.schedule.clone(),
                    workflow: spec_tick_workflow(&spec),
                    owner: spec.owner.clone(),
                    scope: spec.scope,
                    catchup: spec_catchup(&spec),
                    run_prefix: "cron-spec".to_string(),
                    start_ts: reg.start_ts,
                };
                let run_id = format!("cron-spec:{}", name);
                let _ = self
                    .engine
                    .resonate()
                    .run(&run_id, cron_loop, input)
                    .spawn()
                    .await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Action, parse_cron};

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
        assert_eq!(
            tick_0,
            base + 600,
            "on-time: sleep to the next scheduled tick"
        );

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

    // ---- ADR-0012 / bd-25g7.2: CronSpec-driven loops ----

    /// The spec's tick body maps to a workflow the SHARED engine runs:
    /// steps + graph cloned, policy mapped (catchup lives on the loop, not
    /// the workflow), NO trigger (the loop synthesizes the schedule trigger).
    #[test]
    fn spec_tick_workflow_maps_steps_graph_and_policy() {
        let spec = parse_cron(
            "name: w\nschedule: '* * * * *'\nowner: o\npolicy:\n  on_failure: bead\n  catchup: latest\nsteps:\n  - { name: a, action: call_workflow, workflow: hc }\n",
        )
        .expect("spec");
        let wf = spec_tick_workflow(&spec);
        assert_eq!(wf.name, "w");
        assert!(
            wf.trigger.is_none(),
            "no workflow trigger — the loop owns the schedule"
        );
        assert_eq!(wf.steps.len(), 1);
        assert_eq!(wf.steps[0].name.as_deref(), Some("a"));
        let p = wf.policy.as_ref().expect("policy mapped");
        assert_eq!(p.on_failure, FailureAction::Bead);
        assert_eq!(p.no_claim_timeout, 600, "policy defaults survive the map");
    }

    /// Missing policy → None (log-only loop) + catchup defaults to `latest`.
    #[test]
    fn spec_policy_absent_maps_to_none_and_latest() {
        let spec =
            parse_cron("name: w\nschedule: '* * * * *'\nowner: o\nsteps: []\n").expect("spec");
        let wf = spec_tick_workflow(&spec);
        assert!(wf.policy.is_none(), "no policy -> log-only");
        assert_eq!(spec_catchup(&spec), CronCatchup::Latest);
    }

    /// `catchup: skip` is honored; `catchup: latest` is the default.
    #[test]
    fn spec_catchup_reads_policy() {
        let spec = parse_cron(
            "name: w\nschedule: '* * * * *'\nowner: o\npolicy:\n  catchup: skip\nsteps: []\n",
        )
        .expect("spec");
        assert_eq!(spec_catchup(&spec), CronCatchup::Skip);
    }

    /// `due_tick(latest)`: downtime skipping several ticks fires ONLY the
    /// most-recent due tick at-or-before now (never a burst).
    #[test]
    fn due_tick_latest_catches_up_to_most_recent() {
        use chrono::TimeZone;
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        // Down 25 minutes: ticks 10:00/10:10/10:20 all due while down.
        let now = base + 25 * 60;
        let caught = due_tick("*/10 * * * *", base, now, CronCatchup::Latest).unwrap();
        assert_eq!(caught, base + 20 * 60, "latest missed tick only");
    }

    /// `due_tick(latest)` on-time: the next scheduled tick in the future.
    #[test]
    fn due_tick_latest_on_time_is_next_tick() {
        use chrono::TimeZone;
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        let due = due_tick("*/10 * * * *", base, base + 60, CronCatchup::Latest).unwrap();
        assert_eq!(due, base + 600, "sleep to the next scheduled tick");
    }

    /// `due_tick(skip)`: missed ticks are NEVER fired — after downtime the
    /// next tick is the first one after `now`.
    #[test]
    fn due_tick_skip_never_fires_missed() {
        use chrono::TimeZone;
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        let now = base + 25 * 60;
        let due = due_tick("*/10 * * * *", base, now, CronCatchup::Skip).unwrap();
        assert!(due > now, "skips the 3 missed ticks, next is in the future");
        assert_eq!(due, base + 30 * 60);
    }

    /// `due_tick(skip)` on-time: unchanged (next scheduled tick).
    #[test]
    fn due_tick_skip_on_time_is_next_tick() {
        use chrono::TimeZone;
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        let due = due_tick("*/10 * * * *", base, base + 60, CronCatchup::Skip).unwrap();
        assert_eq!(due, base + 600);
    }

    /// The spec registration round-trips; `enabled`/`start_ts` default.
    #[test]
    fn cron_spec_registration_roundtrip() {
        let reg: CronSpecRegistration =
            serde_yaml::from_str("spec:\n  name: w\n  schedule: '* * * * *'\n  owner: o\n")
                .expect("reg");
        assert!(reg.enabled, "enabled defaults true");
        assert!(reg.start_ts.is_none());
        let out = serde_yaml::to_string(&reg).expect("serialize");
        assert!(!out.contains("cron-spec"));
        assert!(out.contains("name: w"));
    }

    // ---- live: real resonate server + remote engine (the P12.2 gate) ----

    /// Boot a real `resonate serve` (sqlite storage in `dir`) + a remote
    /// engine whose `call_workflow` resolves tiny child workflows. Returns
    /// (server, engine); the caller keeps the server alive for the test's
    /// duration.
    async fn spec_engine(
        dir: &std::path::Path,
    ) -> (
        std::process::Child,
        std::sync::Arc<crate::engine::WorkflowEngine>,
    ) {
        use std::process::{Command, Stdio};
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let url = format!("http://127.0.0.1:{port}");
        let server = Command::new("resonate")
            .args([
                "serve",
                "--storage-type",
                "sqlite",
                "--server-port",
                &port.to_string(),
                "--server-host",
                "127.0.0.1",
            ])
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn resonate server");
        for _ in 0..200 {
            if std::net::TcpStream::connect_timeout(
                &format!("127.0.0.1:{port}").parse().expect("addr"),
                std::time::Duration::from_millis(100),
            )
            .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let runner: crate::engine::ActionRunner =
            Arc::new(|a: &Action| Ok(serde_json::json!({ "ran": a.type_name() })));
        let workflow_resolver: crate::engine::WorkflowResolver = Arc::new(|name: &str| {
            let yaml =
                format!("name: {name}\nsteps:\n  - action: send_dm\n    to: t\n    body: ok\n");
            crate::schema::parse_workflow(&yaml).ok()
        });
        let engine = crate::engine::WorkflowEngine::remote_with_resolvers(
            url,
            runner,
            Arc::new(|_name: &str| None),
            workflow_resolver,
        )
        .expect("engine");
        (server, std::sync::Arc::new(engine))
    }

    async fn wait_for_event(sink: &PolicySink, timeout_secs: u64) -> Option<PolicyEvent> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            {
                let guard = sink.lock().await;
                if let Some(e) = guard.first() {
                    return Some(e.clone());
                }
            }
            if std::time::Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    async fn count_events(sink: &PolicySink) -> usize {
        sink.lock().await.len()
    }

    /// The P12.2 gate: a CronSpec runs as a Resonate durable function —
    /// schedule via ctx.sleep, body via the SHARED engine (`call_workflow`
    /// children execute), catchup `latest` on wake after downtime, and
    /// resume-dedup (re-starting the loop never double-fires a tick).
    #[tokio::test]
    async fn cron_spec_loop_fires_tick_with_catchup_and_resume_dedup() {
        use std::time::Duration;
        let dir = std::env::temp_dir().join(format!("cron-spec-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let (mut server, engine) = spec_engine(&dir).await;

        let spec = parse_cron(
            r##"
name: weekly-upgrade
schedule: "* * * * *"
owner: housekeeper
scope: host
policy:
  on_success: log
  max_retries: 0
  no_claim_timeout: 600
  catchup: latest
steps:
  - { name: probe,  action: call_workflow, workflow: health-check }
  - { name: upgrade, action: call_workflow, workflow: docker-upgrade }
  - { name: verify, action: call_workflow, workflow: health-check }
"##,
        )
        .expect("spec parses");
        let start_ts = unix_now() - 90;

        let spec1 = spec.clone();
        let spec_registry: CronSpecRegistry = Arc::new(move || {
            vec![CronSpecRegistration {
                spec: spec1.clone(),
                enabled: true,
                start_ts: Some(start_ts),
            }]
        });
        let empty: CronRegistry = Arc::new(Vec::new);
        let manager = CronManager::with_spec_registry(engine.clone(), empty, Some(spec_registry))
            .expect("manager");
        let sink = engine.policy_sink().clone();

        // Start → the loop catches up to the most-recent due tick (inside the
        // 90s downtime window) and runs the body through the shared engine.
        manager.start().await.expect("start");
        let event = wait_for_event(&sink, 30)
            .await
            .expect("tick event within 30s");
        let t1 = event.tick;
        assert!(
            t1 <= unix_now() && t1 >= start_ts,
            "catchup latest fires the most-recent due tick in the window (t1={t1}, start={start_ts})"
        );
        let out = event.output.as_ref().expect("tick output rides along");
        assert!(
            out.get("probe").is_some()
                && out.get("upgrade").is_some()
                && out.get("verify").is_some(),
            "call_workflow body executed via the shared engine: {out:?}"
        );

        // Re-start (same run id) → resume, NO duplicate dispatch for the tick.
        manager.start().await.expect("resume");
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            count_events(&sink).await,
            1,
            "no duplicate tick after resume"
        );

        // Downtime: kill the server, wait past the NEXT tick boundary, reboot
        // (same sqlite dir) + new engine → the loop resumes and catchup
        // `latest` fires the single most-recent missed tick (never a burst).
        let next_tick = t1 + 60;
        let _ = server.kill();
        let _ = server.wait();
        while unix_now() < next_tick + 2 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let (server2, engine2) = spec_engine(&dir).await;
        server = server2;
        let spec2 = spec.clone();
        let spec_registry2: CronSpecRegistry = Arc::new(move || {
            vec![CronSpecRegistration {
                spec: spec2.clone(),
                enabled: true,
                start_ts: Some(start_ts),
            }]
        });
        let empty2: CronRegistry = Arc::new(Vec::new);
        let manager2 =
            CronManager::with_spec_registry(engine2.clone(), empty2, Some(spec_registry2))
                .expect("manager2");
        manager2.start().await.expect("resume after restart");
        let sink2 = engine2.policy_sink().clone();
        let event2 = wait_for_event(&sink2, 30)
            .await
            .expect("catchup event after restart within 30s");
        assert_eq!(
            event2.tick, next_tick,
            "most-recent missed tick, not a burst (got {})",
            event2.tick
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
        {
            let guard = sink2.lock().await;
            let ticks: Vec<(i64, i64)> = guard.iter().map(|e| (e.tick, unix_now())).collect();
            assert_eq!(
                guard.len(),
                1,
                "exactly one catch-up tick (events: {ticks:?}, next_tick={next_tick}, now={})",
                unix_now()
            );
        }

        let _ = server.kill();
        let _ = server.wait();
    }
}
