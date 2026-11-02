// governed-by: ADR-0005 (P4.3 — cron manager, Spec 3 scheduler durability)
#![forbid(unsafe_code)]

//! Cron specs and pure tick timing.
//!
//! This module owns the cron DATA and arithmetic: `CronSpec` parsing, the
//! 5-field next-tick (`due_tick`), catch-up policy, and the tick body
//! (`spec_tick_workflow`) the host runs on the durable engine. It is not a
//! scheduler: the host drives ticks (`ElmCron` in `cron_elm.rs`) and persists
//! the checkpoint; the engine owns the run. There is no wall-clock sleep and
//! no vendor replay here — `due_tick` is a pure function of the expression and
//! two timestamps.
use chrono::DateTime;

use crate::schema::{CronCatchup, CronPolicy, CronSpec, CronSpecPolicy, Workflow};

/// One registered cron the host drives (Spec 3). The embedding resolves
/// the workflow template + owner profile; the host only needs the plan.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    pub scope: crate::schema::CronScope,
    /// Paused crons are skipped (entry preserved; nothing fires).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

const fn default_enabled() -> bool {
    true
}



/// The tick body for a spec-driven loop: a [`Workflow`] built from the
/// CronSpec's steps/graph/policy — the SAME shape the shared engine runs
/// (one engine, two input types; `call_workflow` composes workflows).
/// No trigger: the loop synthesizes the schedule trigger per tick. Public so
/// the embedding (flywheel `cron run`/lint — P12.3) drives the same tick body
/// the loop fires.
pub fn spec_tick_workflow(spec: &CronSpec) -> Workflow {
    Workflow {
        // The tick body is a WRAPPER around the spec, not the spec's name:
        // a cron may `call_workflow` a catalog workflow with the same name,
        // and the recursion guard compares names. `-tick` keeps the two
        // distinct without changing the cron's identity or run id.
        name: format!("{}-tick", spec.name),
        description: spec.description.clone(),
        trigger: None,
        policy: spec.policy.as_ref().map(spec_policy_to_cron_policy),
        guard: None,
        steps: spec.steps.clone(),
        graph: spec.graph.clone(),
        inputs: Vec::new(),
        crew: None,
        principles: Vec::new(),
    }
}

/// Map the cron-specific policy (which carries `catchup`, ADR-0012) to the
/// workflow policy the loop's failure-policy machinery consumes.
const fn spec_policy_to_cron_policy(p: &CronSpecPolicy) -> CronPolicy {
    CronPolicy {
        on_failure: p.on_failure,
        on_repeat_failure: p.on_repeat_failure,
        repeat_threshold: p.repeat_threshold,
        on_success: p.on_success,
        max_retries: p.max_retries,
        no_claim_timeout: p.no_claim_timeout,
    }
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
    d.parse::<u32>()
        .map_or_else(|_| d.to_string(), |n| ((n % 7) + 1).to_string())
}

/// The edge's bounded candidate set for one decision (bd-j32w): the
/// latest due tick (if any) plus the next ticks from `now` and from
/// `last_fired` — O(1) schedule reads, no gap enumeration. Shared by the
/// durable child and the conformance battery below.
#[cfg(test)]
fn decide_candidates(cron: &str, last_fired: i64, now: i64) -> Vec<i64> {
    let mut candidates = Vec::new();
    for t in [
        latest_due_at_or_before(cron, last_fired, now),
        next_due(cron, now),
        next_due(cron, last_fired),
    ]
    .into_iter()
    .flatten()
    {
        if !candidates.contains(&t) {
            candidates.push(t);
        }
    }
    candidates
}

/// The effective missed-tick policy for a spec (default `Latest`).
#[cfg(test)]
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
///
/// Legacy oracle for the kernel conformance tests below: production calls
/// go through `cron_core`, tests pin this math. Only referenced under
/// `cfg(test)`, hence the allow.
#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FailureAction, parse_cron};

    /// next_due matrix: future tick, vixie Sunday-zero, strictly-after-from,
    /// garbage-is-none. One test (was 4).
    #[test]
    fn next_due_matrix() {
        let from = 1_700_000_000i64;
        let due = next_due("30 3 * * *", from).expect("valid expr yields a tick");
        assert!(due > from, "next tick is in the future");
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
        let from = 1_700_000_000i64;
        let due = next_due("*/5 * * * *", from).expect("valid");
        assert!(due > from);
        let second = next_due("*/5 * * * *", due).expect("valid");
        assert!(second > due, "strictly increasing");
        assert!(next_due("not a cron", 1_700_000_000).is_none());
    }

    /// normalize_cron accepts 5-field (prepends seconds=0 + vixie→crate dow)
    /// and 6-field (passes through unchanged — already crate convention).
    #[test]
    fn normalize_accepts_5_and_6_field() {
        assert_eq!(normalize_cron("30 3 * * 0"), "0 30 3 * * 1");
        assert_eq!(normalize_cron("0 30 3 * * 1"), "0 30 3 * * 1");
    }

    /// bd-j32w conformance: the kernel selection agrees with the legacy
    /// `due_tick` schedule math across catch-up modes, gaps, and edges —
    /// one implementation going forward, machine-checked here.
    #[test]
    fn kernel_selection_agrees_with_legacy_due_tick() {
        use crate::cron_core::{Catchup, next_future, select_fireable};
        use crate::schema::CronCatchup;
        let schedules = [
            "*/10 * * * *",   // every 10 minutes
            "30 3 * * *",     // daily
            "0 * * * *",      // hourly
            "*/1 * * * * *",  // every second (6-field)
            "not a schedule", // unparsable
        ];
        let base = 1_700_000_000i64;
        let mut cases = 0usize;
        for cron in schedules {
            for last_fired in [base - 3600, base - 61, base - 1, base, base + 30] {
                for now in [base, base + 1, base + 599, base + 3600] {
                    for (legacy_catchup, kernel_catchup) in [
                        (CronCatchup::Latest, Catchup::Latest),
                        (CronCatchup::Skip, Catchup::Skip),
                    ] {
                        let legacy = due_tick(cron, last_fired, now, legacy_catchup);
                        let candidates = decide_candidates(cron, last_fired, now);
                        let kernel_fire =
                            select_fireable(kernel_catchup, Some(last_fired), now, &candidates);
                        let kernel_sleep = next_future(Some(last_fired), now, &candidates);
                        // Full-decision agreement: the legacy Option carries
                        // fire-or-sleep-target in one value; the kernel
                        // splits them (Fire vs SleepUntil) and the durable
                        // child recombines identically (fire wins).
                        assert_eq!(
                            kernel_fire.or(kernel_sleep),
                            legacy,
                            "kernel vs legacy decision: {cron} last={last_fired} now={now} {legacy_catchup:?}"
                        );
                        if kernel_catchup == Catchup::Skip {
                            assert_eq!(
                                kernel_fire, None,
                                "skip fires nothing overdue: {cron} last={last_fired} now={now}"
                            );
                        }
                        if legacy.is_none() && candidates.is_empty() {
                            assert!(
                                kernel_sleep.is_none(),
                                "unparsable parks on both sides: {cron}"
                            );
                        }
                        cases += 1;
                    }
                }
            }
        }
        assert!(cases > 100, "battery covered: {cases} cases");
    }

    /// bd-j32w conformance: the kernel policy table agrees with the legacy
    /// `cron_failure::evaluate` across the streak space.
    #[test]
    fn kernel_policy_table_agrees_with_legacy_evaluate() {
        use crate::cron_core::{CronPolicyData, PolicyActionKind, select_policy_action};
        use crate::cron_failure::evaluate as legacy_evaluate;
        use crate::schema::{CronPolicy, FailureAction};
        let legacy_policy = CronPolicy {
            on_failure: FailureAction::Bead,
            on_repeat_failure: FailureAction::Escalate,
            repeat_threshold: 3,
            on_success: FailureAction::Log,
            max_retries: 2,
            no_claim_timeout: 600,
        };
        let kernel_policy = CronPolicyData {
            on_failure: PolicyActionKind::Bead,
            on_repeat_failure: PolicyActionKind::Escalate,
            repeat_threshold: 3,
            on_success: PolicyActionKind::Log,
        };
        for consecutive in 0..6u32 {
            for failed in [false, true] {
                let (legacy_action, legacy_next) =
                    legacy_evaluate(&legacy_policy, failed, consecutive);
                let (kernel_action, kernel_next) =
                    select_policy_action(&kernel_policy, failed, consecutive);
                let map = |a: PolicyActionKind| match a {
                    PolicyActionKind::Log => FailureAction::Log,
                    PolicyActionKind::Bead => FailureAction::Bead,
                    PolicyActionKind::Escalate => FailureAction::Escalate,
                };
                assert_eq!(
                    map(kernel_action),
                    legacy_action,
                    "policy agree: failed={failed} streak={consecutive}"
                );
                assert_eq!(
                    kernel_next, legacy_next,
                    "streak agree: failed={failed} streak={consecutive}"
                );
            }
        }
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
        assert_eq!(wf.name, "w-tick");
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
    /// `due_tick` catch-up matrix (latest vs skip, missed vs on-time).
    /// One test (was 4).
    #[test]
    fn due_tick_catchup_matrix() {
        use chrono::TimeZone;
        let base = chrono::Utc
            .with_ymd_and_hms(2023, 11, 20, 10, 0, 0)
            .unwrap()
            .timestamp();
        // Down 25 minutes: ticks 10:00/10:10/10:20 all due while down.
        let now = base + 25 * 60;
        let caught = due_tick("*/10 * * * *", base, now, CronCatchup::Latest).unwrap();
        assert_eq!(caught, base + 20 * 60, "latest missed tick only");
        let due = due_tick("*/10 * * * *", base, base + 60, CronCatchup::Latest).unwrap();
        assert_eq!(due, base + 600, "sleep to the next scheduled tick");
        let due = due_tick("*/10 * * * *", base, now, CronCatchup::Skip).unwrap();
        assert!(due > now, "skips the 3 missed ticks, next is in the future");
        assert_eq!(due, base + 30 * 60);
        let due = due_tick("*/10 * * * *", base, base + 60, CronCatchup::Skip).unwrap();
        assert_eq!(due, base + 600);
    }

}
