// governed-by: ADR-0035
//! Pure cron scheduler kernel (ADR-0035 Spec 1, bead bd-j32w).
//!
//! Catchup, missed-fires, overlap, and backoff are STATE PATHS here —
//! [`tick`] over ([`CronModel`], [`CronMsg`]) — not scattered unit tests.
//! Verbatim shape of `dispatch_core.rs` / `receipts_core.rs`: **std +
//! serde only** — no tokio, no Nostr types, no sqlx, no clock, no env, no
//! schedule parsing (fenced by test below).
//!
//! ## Law
//!
//! One `Model`, one `Msg` union, one [`tick`]: `tick(&Model, &Msg) ->
//! (Model, Vec<Effect>)`. Wall-clock never read in core: timers arrive as
//! [`CronMsg::Tick`] payloads (`now_secs` + precomputed `candidates`);
//! the host's durable sleep stays the interpreter of [`CronEffect::SleepUntil`]
//! and [`CronEffect::Park`].
//!
//! ## Edge contract (the sensor side, in `cron.rs`)
//!
//! The edge parses the schedule (cron crate) and materializes a BOUNDED
//! candidate window per decision: the latest due tick at-or-before `now`
//! (if any) plus the next few future ticks. The kernel only compares
//! integers — it never parses, never reads time, never sleeps.
//!
//! ## Backoff rule
//!
//! A failing streak gates OVERDUE fires only: selecting a tick at-or-before
//! `now` while `consecutive > 0` sleeps until `now + backoff(streak)`
//! instead of firing immediately (storm spacing for tight schedules under
//! persistent failure). Future ticks are unaffected — the schedule never
//! shifts — and the gate is capped, named, and visible in the output
//! effects. First failure is free of extra delay only when its tick was
//! already future-scheduled; an overdue fire after any failure waits out
//! the backoff.

use serde::{Deserialize, Serialize};

/// Missed-tick catch-up policy, mirrored from the schema so the core stays
/// dependency-free (the host maps across; a mismatch is a compile error at
/// the mapping site, never a silent drift).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Catchup {
    /// Run the most-recent due tick on wake (never a burst).
    Latest,
    /// Skip missed ticks entirely.
    Skip,
}

/// Failure action decided by the kernel, mirrored from the schema
/// (`FailureAction`) for the same dependency-free reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PolicyActionKind {
    Log,
    Bead,
    Escalate,
}

/// Failure policy as DATA (bead rule): thresholds and actions travel in the
/// model, never as code paths in the tick arms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronPolicyData {
    /// First-failure action.
    pub on_failure: PolicyActionKind,
    /// After `repeat_threshold` consecutive failures.
    pub on_repeat_failure: PolicyActionKind,
    /// Consecutive failures before `on_repeat_failure` triggers.
    pub repeat_threshold: u32,
    /// Success action.
    pub on_success: PolicyActionKind,
}

impl Default for CronPolicyData {
    fn default() -> Self {
        Self {
            on_failure: PolicyActionKind::Log,
            on_repeat_failure: PolicyActionKind::Log,
            repeat_threshold: 3,
            on_success: PolicyActionKind::Log,
        }
    }
}

/// Model: the whole scheduler state. Clone-and-return keeps [`tick`] pure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronModel {
    /// Effective missed-tick policy.
    pub catchup: Catchup,
    /// Failure/backoff policy as data.
    pub policy: CronPolicyData,
    /// Last fired tick (checkpoint). `None` = never fired.
    pub last_fired: Option<i64>,
    /// Consecutive-failure streak (resets on success).
    pub consecutive: u32,
    /// Tick currently executing, if any (overlap tracking). The sequential
    /// durable loop guarantees `None` at every decision; the path exists
    /// for concurrent interpreters and is covered by paths tests.
    pub in_flight: Option<i64>,
    /// A fire held back by the failure backoff (bd-cron-double-emit). A
    /// gated decision emits ONLY `SleepUntil`; the deferred tick fires on
    /// the first `Tick` at or after `at_secs`, never alongside the sleep.
    #[serde(default)]
    pub deferred: Option<DeferredTick>,
}

/// One fire held back by the failure backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredTick {
    /// The due tick waiting for the gate.
    pub tick: i64,
    /// Wall time the gate lifts.
    pub at_secs: i64,
}

impl CronModel {
    /// New model: nothing fired, no streak, nothing in flight.
    pub const fn new(catchup: Catchup, policy: CronPolicyData) -> Self {
        Self {
            catchup,
            policy,
            last_fired: None,
            consecutive: 0,
            in_flight: None,
            deferred: None,
        }
    }

    /// Test constructor free of pools/clocks (ADR-0031 A8).
    pub fn test_new() -> Self {
        Self {
            catchup: Catchup::Latest,
            policy: CronPolicyData::default(),
            last_fired: None,
            consecutive: 0,
            in_flight: None,
            deferred: None,
        }
    }
}

/// All state changes enter as messages. Every time-bearing variant carries
/// `now_secs` from the host clock — the core never reads time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronMsg {
    /// The timer fired: decide what to do. `candidates` is the edge's
    /// bounded window (latest due at-or-before `now`, if any, plus upcoming
    /// ticks, all strictly after `last_fired`); `schedule_ok` is false when
    /// the expression did not parse.
    Tick {
        now_secs: i64,
        candidates: Vec<i64>,
        schedule_ok: bool,
    },
    /// The interpreter began executing a tick.
    TickStarted { tick: i64 },
    /// The interpreter finished a tick successfully.
    TickSucceeded { tick: i64 },
    /// The interpreter finished a tick failed.
    TickFailed { tick: i64 },
}

/// Intents the core journals; the host executes. No I/O in [`tick`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CronEffect {
    /// Sleep until this wall time (interpreter: the host's durable sleep).
    SleepUntil { at_secs: i64 },
    /// Execute this tick now (interpreter: durable child).
    FireTick { tick: i64 },
    /// An overlapping tick was skipped: another tick is still in flight.
    /// Journaled and visible — a skip is a decision, never silence.
    SkipOverlap { tick: i64 },
    /// The schedule is unparsable: park (the interpreter sleeps long and
    /// never spins).
    Park { reason: &'static str },
    /// The policy action this terminal decided (embedding performs
    /// bead/escalate/log).
    PolicyAction {
        action: PolicyActionKind,
        consecutive: u32,
    },
}

/// Backoff base: first gated refire waits this long.
pub const BACKOFF_BASE_SECS: i64 = 60;
/// Backoff cap: the gate never exceeds this, however long the streak.
pub const BACKOFF_CAP_SECS: i64 = 3600;
/// Idle re-ask when the edge supplied no usable candidate: the interpreter
/// re-fires a fresh decision after this long (bounded liveness, never spin).
pub const IDLE_REASK_SECS: i64 = 60;

/// Backoff for a failure streak: exponential from the base, capped.
/// Pure function of the streak — deterministic given the Msg log.
pub fn backoff_for_streak(consecutive: u32) -> i64 {
    let shift = consecutive.saturating_sub(1).min(6);
    let backoff = BACKOFF_BASE_SECS << shift;
    if backoff > BACKOFF_CAP_SECS {
        BACKOFF_CAP_SECS
    } else {
        backoff
    }
}

/// Named pure predicates only — no inline boolean soup in [`tick`].
const fn schedule_usable(schedule_ok: bool) -> bool {
    schedule_ok
}

/// No tick may start while another is in flight.
const fn no_in_flight(in_flight: Option<i64>) -> bool {
    in_flight.is_none()
}

/// Select the tick to fire under the catch-up policy, over candidates
/// strictly after `last_fired` (bd-j32w: the edge parses, the kernel
/// decides — the durable `decide_tick` child routes through here so there
/// is exactly one selection implementation). `Latest` takes the most-recent due tick
/// at-or-before `now` (ONE tick after downtime, never a burst);
/// `Skip` takes nothing overdue (missed ticks are dropped).
pub fn select_fireable(
    catchup: Catchup,
    last_fired: Option<i64>,
    now_secs: i64,
    candidates: &[i64],
) -> Option<i64> {
    let after_last = |t: &i64| last_fired.is_none_or(|last| *t > last);
    match catchup {
        Catchup::Latest => candidates
            .iter()
            .filter(|t| after_last(t) && **t <= now_secs)
            .max()
            .copied(),
        Catchup::Skip => None,
    }
}

/// Earliest future candidate strictly after both `last_fired` and `now`
/// (bd-j32w: the sleep target when nothing is fireable).
pub fn next_future(last_fired: Option<i64>, now_secs: i64, candidates: &[i64]) -> Option<i64> {
    candidates
        .iter()
        .filter(|t| last_fired.is_none_or(|last| **t > last) && **t > now_secs)
        .min()
        .copied()
}

/// Policy action selection (moved from `cron_failure::evaluate`, same
/// truth table): success resets to `on_success`; the N-th consecutive
/// failure (N >= threshold, minimum 1) escalates to `on_repeat_failure`.
pub const fn select_policy_action(
    policy: &CronPolicyData,
    failed: bool,
    consecutive: u32,
) -> (PolicyActionKind, u32) {
    if !failed {
        return (policy.on_success, 0);
    }
    let next = consecutive.saturating_add(1);
    let threshold = if policy.repeat_threshold < 1 {
        1
    } else {
        policy.repeat_threshold
    };
    let action = if next >= threshold {
        policy.on_repeat_failure
    } else {
        policy.on_failure
    };
    (action, next)
}

/// Machine config JSON for the visualizer (dispatch_core pattern).
pub fn machine_config() -> serde_json::Value {
    serde_json::json!({
        "machine": "cron_core",
        "states": ["idle", "in_flight", "backing_off", "parked"],
        "transitions": [
            {"on": "Tick", "guard": "schedule_usable+no_in_flight", "effects": ["FireTick", "SleepUntil", "SkipOverlap", "Park"]},
            {"on": "TickStarted", "guard": null, "effects": []},
            {"on": "TickSucceeded", "guard": null, "effects": ["PolicyAction"]},
            {"on": "TickFailed", "guard": null, "effects": ["PolicyAction"]},
        ],
        "guards": ["schedule_usable", "no_in_flight"],
        "consts": {"BACKOFF_BASE_SECS": BACKOFF_BASE_SECS, "BACKOFF_CAP_SECS": BACKOFF_CAP_SECS, "IDLE_REASK_SECS": IDLE_REASK_SECS},
    })
}

/// Pure transition: one model in, new model + journaled effects out.
pub fn tick(model: &CronModel, msg: &CronMsg) -> (CronModel, Vec<CronEffect>) {
    let mut next = model.clone();
    match msg {
        CronMsg::Tick {
            now_secs,
            candidates,
            schedule_ok,
        } => {
            if !schedule_usable(*schedule_ok) {
                return (
                    next,
                    vec![CronEffect::Park {
                        reason: "unparsable",
                    }],
                );
            }
            if !no_in_flight(next.in_flight) {
                let skipped =
                    select_fireable(model.catchup, model.last_fired, *now_secs, candidates);
                let wake = next_future(model.last_fired, *now_secs, candidates)
                    .unwrap_or(now_secs.saturating_add(IDLE_REASK_SECS));
                let mut effects = vec![CronEffect::SleepUntil { at_secs: wake }];
                if let Some(tick) = skipped {
                    effects.insert(0, CronEffect::SkipOverlap { tick });
                }
                return (next, effects);
            }
            if let Some(deferred) = next.deferred {
                if *now_secs < deferred.at_secs {
                    return (
                        next,
                        vec![CronEffect::SleepUntil {
                            at_secs: deferred.at_secs,
                        }],
                    );
                }
                next.deferred = None;
                if model.last_fired.is_none_or(|last| deferred.tick > last) {
                    next.in_flight = Some(deferred.tick);
                    return (
                        next,
                        vec![CronEffect::FireTick {
                            tick: deferred.tick,
                        }],
                    );
                }
            }
            match select_fireable(model.catchup, model.last_fired, *now_secs, candidates) {
                Some(tick) if next.consecutive > 0 => {
                    let at_secs = now_secs.saturating_add(backoff_for_streak(next.consecutive));
                    next.deferred = Some(DeferredTick { tick, at_secs });
                    (next, vec![CronEffect::SleepUntil { at_secs }])
                }
                Some(tick) => {
                    next.in_flight = Some(tick);
                    (next, vec![CronEffect::FireTick { tick }])
                }
                None => {
                    let wake = next_future(model.last_fired, *now_secs, candidates)
                        .unwrap_or(now_secs.saturating_add(IDLE_REASK_SECS));
                    (next, vec![CronEffect::SleepUntil { at_secs: wake }])
                }
            }
        }
        CronMsg::TickStarted { tick } => {
            next.deferred = None;
            next.in_flight = Some(*tick);
            (next, vec![])
        }
        CronMsg::TickSucceeded { tick } => {
            next.deferred = None;
            if next.in_flight == Some(*tick) {
                next.in_flight = None;
            }
            next.consecutive = 0;
            if next.last_fired.is_none_or(|l| *tick > l) {
                next.last_fired = Some(*tick);
            }
            let (action, consecutive) = select_policy_action(&next.policy, false, next.consecutive);
            next.consecutive = consecutive;
            (
                next,
                vec![CronEffect::PolicyAction {
                    action,
                    consecutive,
                }],
            )
        }
        CronMsg::TickFailed { tick, .. } => {
            next.deferred = None;
            if next.in_flight == Some(*tick) {
                next.in_flight = None;
            }
            if next.last_fired.is_none_or(|l| *tick > l) {
                next.last_fired = Some(*tick);
            }
            let (action, consecutive) = select_policy_action(&next.policy, true, next.consecutive);
            next.consecutive = consecutive;
            (
                next,
                vec![CronEffect::PolicyAction {
                    action,
                    consecutive,
                }],
            )
        }
    }
}

#[cfg(test)]
mod kernel_paths_tests {
    use super::*;

    /// Import fence (bd-j32w VERIFY): the shipped file must never name a
    /// runtime path — neither in `use` lines nor qualified inline.
    #[test]
    fn import_fence_has_no_runtime_paths() {
        let src = include_str!("cron_core.rs");
        let shipped = src
            .split("#[cfg(test)]")
            .next()
            .expect("test module marker");
        let imports: Vec<&str> = shipped
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("use "))
            .collect();
        assert!(!imports.is_empty(), "fence must see the use lines");
        for banned in [
            "tokio",
            "nostr",
            "sqlx",
            "chrono",
            "parking_lot",
            "axum",
            "rmcp",
            "flywheel",
            "doorbell",
            "herdr",
            "resonate",
            "std::time",
            "std::env",
            "std::fs",
            "std::net",
            "std::process",
            "std::thread",
            "std::sync::Mutex",
            "std::sync::RwLock",
            "crate::cron",
            "crate::engine",
            "crate::mailbox",
            "super::",
        ] {
            for line in &imports {
                assert!(
                    !line.contains(banned),
                    "cron_core.rs imports banned `{banned}`: {line}"
                );
            }
        }
        for path in [
            "tokio::",
            "std::time::",
            "std::env::",
            "std::fs::",
            "std::net::",
            "std::process::",
            "std::thread::",
            "Utc::now",
            "SystemTime::now",
        ] {
            assert!(
                !shipped.contains(path),
                "cron_core.rs names runtime path `{path}` outside imports"
            );
        }
    }

    fn tick_msg(now_secs: i64, candidates: Vec<i64>) -> CronMsg {
        CronMsg::Tick {
            now_secs,
            candidates,
            schedule_ok: true,
        }
    }

    /// Determinism: the same Msg log replays byte-identical models+effects.
    #[test]
    fn same_msg_log_replays_identically() {
        let log = vec![
            tick_msg(1000, vec![1000]),
            CronMsg::TickStarted { tick: 1000 },
            CronMsg::TickSucceeded { tick: 1000 },
            tick_msg(1060, vec![1060]),
            CronMsg::TickStarted { tick: 1060 },
            CronMsg::TickFailed { tick: 1060 },
            tick_msg(1120, vec![1060, 1120]),
        ];
        let run = || {
            let mut model = CronModel::test_new();
            let mut states = Vec::new();
            let mut all_fx = Vec::new();
            for msg in &log {
                let (next, fx) = tick(&model, msg);
                model = next;
                states.push(model.clone());
                all_fx.push(fx);
            }
            (states, all_fx)
        };
        assert_eq!(run(), run(), "replay is deterministic");
    }

    /// BFS over reachable (model × msg) space: every reachable row obeys
    /// the kernel invariants — catchup takes at most one overdue tick,
    /// overlap never fires, backoff never shifts the schedule.
    #[test]
    fn bfs_reachable_paths_obey_kernel_invariants() {
        use std::collections::{HashSet, VecDeque};
        let ticks: Vec<i64> = vec![100, 110, 120, 130];
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(CronModel, usize)> = VecDeque::new();
        queue.push_back((CronModel::test_new(), 0));
        let mut parked_model = CronModel::test_new();
        parked_model.catchup = Catchup::Skip;
        queue.push_back((parked_model, 0));
        let mut transitions = 0usize;
        while let Some((model, depth)) = queue.pop_front() {
            let key = format!("{:?}", (&model, depth.min(9)));
            if !visited.insert(key) {
                continue;
            }
            let mut msgs = Vec::new();
            for now in [105, 115, 125, 135] {
                msgs.push(CronMsg::Tick {
                    now_secs: now,
                    candidates: ticks.clone(),
                    schedule_ok: true,
                });
            }
            msgs.push(CronMsg::Tick {
                now_secs: 115,
                candidates: ticks.clone(),
                schedule_ok: false,
            });
            for t in &ticks {
                msgs.push(CronMsg::TickStarted { tick: *t });
                msgs.push(CronMsg::TickSucceeded { tick: *t });
                msgs.push(CronMsg::TickFailed { tick: *t });
            }
            for msg in &msgs {
                let (next, fx) = tick(&model, msg);
                transitions += 1;
                if let (Some(a), Some(b)) = (model.last_fired, next.last_fired) {
                    assert!(b >= a, "checkpoint monotonic: {a} -> {b}");
                }
                for e in &fx {
                    match e {
                        CronEffect::FireTick { tick } => {
                            assert!(
                                model.last_fired.is_none_or(|l| *tick > l),
                                "never refire {tick} at/before checkpoint"
                            );
                            assert!(
                                model.in_flight.is_none(),
                                "never fire while another tick is in flight"
                            );
                        }
                        CronEffect::SkipOverlap { .. } => {
                            assert!(
                                model.in_flight.is_some(),
                                "overlap skips only name a live in-flight tick"
                            );
                        }
                        CronEffect::Park { .. } => {
                            assert!(
                                matches!(
                                    msg,
                                    CronMsg::Tick {
                                        schedule_ok: false,
                                        ..
                                    }
                                ),
                                "park only on unparsable schedules"
                            );
                        }
                        CronEffect::SleepUntil { at_secs } => {
                            if let CronMsg::Tick { now_secs, .. } = msg {
                                assert!(
                                    at_secs >= now_secs,
                                    "sleeps never go backward: {at_secs} < {now_secs}"
                                );
                            }
                        }
                        CronEffect::PolicyAction { .. } => {}
                    }
                }
                let fires = fx
                    .iter()
                    .filter(|e| matches!(e, CronEffect::FireTick { .. }))
                    .count();
                let sleeps = fx
                    .iter()
                    .filter(|e| matches!(e, CronEffect::SleepUntil { .. }))
                    .count();
                assert!(
                    fires == 0 || sleeps == 0,
                    "a fire and a sleep never travel together: {fx:?}"
                );
                if let CronMsg::TickSucceeded { tick } | CronMsg::TickFailed { tick } = msg
                    && model.in_flight == Some(*tick)
                {
                    assert_eq!(
                        next.in_flight, None,
                        "terminal report clears its own in-flight"
                    );
                }
                if depth < 3 {
                    queue.push_back((next, depth + 1));
                }
            }
        }
        assert!(transitions > 200, "BFS explored: {transitions} transitions");
        assert!(!visited.is_empty());
    }

    /// DFS: one deep catch-up storm path (downtime + persistent failure)
    /// replays deterministically, fires exactly one catch-up tick, and the
    /// backoff gate spaces refires without shifting the schedule.
    #[test]
    fn dfs_catchup_storm_path_replays_and_spaces_refires() {
        let downtime: Vec<i64> = vec![1060, 1120, 1180, 1240, 1300, 1360];
        let run_once = || {
            let mut model = CronModel::test_new();
            let mut fired = Vec::new();
            let mut fx_log = Vec::new();
            let (m, fx) = tick(
                &model,
                &CronMsg::Tick {
                    now_secs: 1360,
                    candidates: downtime.clone(),
                    schedule_ok: true,
                },
            );
            model = m;
            fx_log.push(fx.clone());
            let fire: Vec<i64> = fx
                .iter()
                .filter_map(|e| match e {
                    CronEffect::FireTick { tick } => Some(*tick),
                    _ => None,
                })
                .collect();
            assert_eq!(fire, vec![1360], "one catch-up tick, the latest");
            fired.push(1360);
            model = tick(&model, &CronMsg::TickStarted { tick: 1360 }).0;
            for _ in 0..3 {
                let (m, fx) = tick(&model, &CronMsg::TickFailed { tick: 1360 });
                model = m;
                fx_log.push(fx);
            }
            assert_eq!(model.consecutive, 3, "streak counted");
            (model, fired, fx_log)
        };
        let (m1, f1, _) = run_once();
        let (m2, f2, _) = run_once();
        assert_eq!((m1.clone(), f1), (m2, f2), "storm replays");
        let (m3, fx3) = tick(
            &m1,
            &CronMsg::Tick {
                now_secs: 1420,
                candidates: vec![1360, 1420],
                schedule_ok: true,
            },
        );
        let gate = 1420 + backoff_for_streak(m1.consecutive);
        assert_eq!(
            fx3,
            vec![CronEffect::SleepUntil { at_secs: gate }],
            "a gated tick sleeps without firing alongside: {fx3:?}"
        );
        assert_eq!(
            m3.deferred,
            Some(DeferredTick {
                tick: 1420,
                at_secs: gate
            }),
            "the due tick is carried across the gate, never dropped"
        );

        let (m4, fx4) = tick(
            &m3,
            &CronMsg::Tick {
                now_secs: 1500,
                candidates: vec![1360, 1420],
                schedule_ok: true,
            },
        );
        assert_eq!(
            fx4,
            vec![CronEffect::SleepUntil { at_secs: gate }],
            "the gate holds until its deadline"
        );
        assert_eq!(m4.deferred, m3.deferred);

        let (m5, fx5) = tick(
            &m4,
            &CronMsg::Tick {
                now_secs: gate,
                candidates: vec![1360, 1420, 1720],
                schedule_ok: true,
            },
        );
        assert_eq!(
            fx5,
            vec![CronEffect::FireTick { tick: 1420 }],
            "the carried tick fires exactly once at the gate"
        );
        assert_eq!(m5.in_flight, Some(1420));
        assert!(m5.deferred.is_none(), "the gate resolves on fire");
    }

    /// Policy table pinned in the kernel (moved semantics from
    /// `cron_failure::evaluate`): success resets, N-th failure escalates.
    #[test]
    fn policy_action_table_matches_legacy_evaluate() {
        let policy = CronPolicyData {
            on_failure: PolicyActionKind::Bead,
            on_repeat_failure: PolicyActionKind::Escalate,
            repeat_threshold: 3,
            on_success: PolicyActionKind::Log,
        };
        assert_eq!(
            select_policy_action(&policy, false, 2),
            (PolicyActionKind::Log, 0)
        );
        assert_eq!(
            select_policy_action(&policy, true, 0),
            (PolicyActionKind::Bead, 1)
        );
        assert_eq!(
            select_policy_action(&policy, true, 2),
            (PolicyActionKind::Escalate, 3)
        );
    }

    #[test]
    fn machine_config_serializes() {
        let cfg = machine_config();
        assert_eq!(cfg["machine"], "cron_core");
        assert!(cfg["transitions"].as_array().is_some());
    }
}
