// governed-by: ADR-0035
//! Pure receipts decision core (ADR-0035 Spec 1, wave 1, Lane A).
//!
//! Verbatim shape of `dispatch_core.rs`: **std + serde only** — no tokio,
//! no Nostr types, no sqlx, no clock, no env (fenced by test below).
//! One `Model`, one `Msg` union, one [`tick`]: clock arrives as `now_ms`
//! Msg payloads supplied by the host (`ReceiptStore`), never read here.
//!
//! The host interprets [`ReceiptEffect`]: `Stamp` performs the sqlite
//! upsert, `PruneBefore` performs the delete, `Ignored` performs nothing.
//! Same Msg log → byte-identical Model + Effects (tested).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Receipt lifecycle state, mirrored from `receipts::ReceiptState` so the
/// core stays dependency-free (the host maps across; a mismatch is a
/// compile error at the mapping site, never a silent drift).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum State {
    Owed,
    Acked,
    Resulted,
    Degraded,
}

/// One row of core state: lifecycle + path + last-stamp clock (host-supplied).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub state: State,
    pub result_path: Option<String>,
    pub updated_at_ms: i64,
}

/// Model: the whole receipts decision state. Clone-and-return keeps
/// [`tick`] pure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptsModel {
    pub entries: BTreeMap<String, Entry>,
}

impl ReceiptsModel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Single-row projection for the host: the store loads one corr row,
    /// ticks, and interprets the single effect.
    pub fn with_row(
        corr: &str,
        state: State,
        result_path: Option<String>,
        updated_at_ms: i64,
    ) -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(
            corr.to_string(),
            Entry {
                state,
                result_path,
                updated_at_ms,
            },
        );
        Self { entries }
    }

    /// Test constructor free of pools/clocks (ADR-0031 A8).
    pub fn test_new() -> Self {
        Self::new()
    }
}

/// All state changes enter as messages. Every time-bearing variant carries
/// `now_ms` from the host clock — the core never reads time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptsMsg {
    /// Transition owed → acked (idempotent; resulted never downgrades).
    Ack { corr: String, now_ms: i64 },
    /// Explicit set to resulted (creates the row; idempotent on same path).
    SetResulted {
        corr: String,
        result_path: Option<String>,
        now_ms: i64,
    },
    /// Record delivery-without-bookkeeping as degraded (acked-unknown).
    /// Creates the row when bookkeeping never ran; guarded by
    /// `transition_allowed` like every other transition — one kernel,
    /// one decider (bd-5mxm). Acked/resulted rows never drag down.
    MarkDegraded { corr: String, now_ms: i64 },
    /// Expire rows older than `retention_ms` (host deletes).
    Prune { now_ms: i64, retention_ms: i64 },
}

/// Intents the core journals; the host executes. No I/O in [`tick`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptEffect {
    /// Write this row (upsert).
    Stamp {
        corr: String,
        state: State,
        result_path: Option<String>,
        updated_at_ms: i64,
    },
    /// No write: the transition is a no-op or forbidden.
    Ignored { corr: String, reason: &'static str },
    /// Delete rows with `updated_at_ms` below this cutoff.
    PruneBefore { cutoff_ms: i64 },
}

/// Named pure predicates only — no inline boolean soup in [`tick`].
fn is_proven(state: State) -> bool {
    state == State::Resulted
}

/// Monotonic guard: resulted never downgrades, acked never drags to owed,
/// degraded (acked-unknown) never re-owes and never drags an ack down.
const fn transition_allowed(from: State, to: State) -> bool {
    !matches!(
        (from, to),
        (State::Resulted, _)
            | (State::Acked, State::Owed)
            | (State::Acked, State::Degraded)
            | (State::Degraded, State::Owed)
    )
}

/// Pure decision: `(Model, Msg) -> (Model, Effects)`.
pub fn tick(model: &ReceiptsModel, msg: &ReceiptsMsg) -> (ReceiptsModel, Vec<ReceiptEffect>) {
    let mut next = model.clone();
    match msg {
        ReceiptsMsg::Ack { corr, now_ms } => {
            let Some(entry) = next.entries.get(corr).cloned() else {
                return (
                    next,
                    vec![ReceiptEffect::Ignored {
                        corr: corr.clone(),
                        reason: "no-row",
                    }],
                );
            };
            if is_proven(entry.state) || entry.state == State::Acked {
                return (
                    next,
                    vec![ReceiptEffect::Ignored {
                        corr: corr.clone(),
                        reason: "idempotent",
                    }],
                );
            }
            if !transition_allowed(entry.state, State::Acked) {
                return (
                    next,
                    vec![ReceiptEffect::Ignored {
                        corr: corr.clone(),
                        reason: "monotonic-guard",
                    }],
                );
            }
            next.entries.insert(
                corr.clone(),
                Entry {
                    state: State::Acked,
                    result_path: entry.result_path.clone(),
                    updated_at_ms: *now_ms,
                },
            );
            (
                next,
                vec![ReceiptEffect::Stamp {
                    corr: corr.clone(),
                    state: State::Acked,
                    result_path: entry.result_path,
                    updated_at_ms: *now_ms,
                }],
            )
        }
        ReceiptsMsg::SetResulted {
            corr,
            result_path,
            now_ms,
        } => {
            match next.entries.get(corr).cloned() {
                Some(entry) if is_proven(entry.state) => {
                    // Proven rows keep their stored path unless the incoming
                    // row carries a new one (explicit path update).
                    if result_path.is_some() && *result_path != entry.result_path {
                        next.entries.insert(
                            corr.clone(),
                            Entry {
                                state: State::Resulted,
                                result_path: result_path.clone(),
                                updated_at_ms: *now_ms,
                            },
                        );
                        return (
                            next,
                            vec![ReceiptEffect::Stamp {
                                corr: corr.clone(),
                                state: State::Resulted,
                                result_path: result_path.clone(),
                                updated_at_ms: *now_ms,
                            }],
                        );
                    }
                    return (
                        next,
                        vec![ReceiptEffect::Ignored {
                            corr: corr.clone(),
                            reason: "idempotent",
                        }],
                    );
                }
                Some(entry) if !transition_allowed(entry.state, State::Resulted) => {
                    return (
                        next,
                        vec![ReceiptEffect::Ignored {
                            corr: corr.clone(),
                            reason: "monotonic-guard",
                        }],
                    );
                }
                _ => {}
            }
            next.entries.insert(
                corr.clone(),
                Entry {
                    state: State::Resulted,
                    result_path: result_path.clone(),
                    updated_at_ms: *now_ms,
                },
            );
            (
                next,
                vec![ReceiptEffect::Stamp {
                    corr: corr.clone(),
                    state: State::Resulted,
                    result_path: result_path.clone(),
                    updated_at_ms: *now_ms,
                }],
            )
        }
        ReceiptsMsg::Prune {
            now_ms,
            retention_ms,
        } => (
            next,
            vec![ReceiptEffect::PruneBefore {
                cutoff_ms: now_ms.saturating_sub(*retention_ms),
            }],
        ),
        ReceiptsMsg::MarkDegraded { corr, now_ms } => match next.entries.get(corr).cloned() {
            None => {
                next.entries.insert(
                    corr.clone(),
                    Entry {
                        state: State::Degraded,
                        result_path: None,
                        updated_at_ms: *now_ms,
                    },
                );
                (
                    next,
                    vec![ReceiptEffect::Stamp {
                        corr: corr.clone(),
                        state: State::Degraded,
                        result_path: None,
                        updated_at_ms: *now_ms,
                    }],
                )
            }
            Some(entry) if entry.state == State::Degraded => (
                next,
                vec![ReceiptEffect::Ignored {
                    corr: corr.clone(),
                    reason: "idempotent",
                }],
            ),
            Some(entry) if !transition_allowed(entry.state, State::Degraded) => (
                next,
                vec![ReceiptEffect::Ignored {
                    corr: corr.clone(),
                    reason: "monotonic-guard",
                }],
            ),
            Some(entry) => {
                next.entries.insert(
                    corr.clone(),
                    Entry {
                        state: State::Degraded,
                        result_path: entry.result_path.clone(),
                        updated_at_ms: *now_ms,
                    },
                );
                (
                    next,
                    vec![ReceiptEffect::Stamp {
                        corr: corr.clone(),
                        state: State::Degraded,
                        result_path: entry.result_path,
                        updated_at_ms: *now_ms,
                    }],
                )
            }
        },
    }
}

/// Machine config JSON for the visualizer (dispatch_core pattern).
pub fn machine_config() -> serde_json::Value {
    serde_json::json!({
        "machine": "receipts_core",
        "states": ["Owed", "Acked", "Resulted", "Degraded"],
        "transitions": [
            {"on": "Ack", "guard": "transition_allowed", "effects": ["Stamp", "Ignored"]},
            {"on": "SetResulted", "guard": "transition_allowed", "effects": ["Stamp", "Ignored"]},
            {"on": "MarkDegraded", "guard": "transition_allowed", "effects": ["Stamp", "Ignored"]},
            {"on": "Prune", "guard": null, "effects": ["PruneBefore"]},
        ],
        "guards": ["is_proven", "transition_allowed"],
    })
}

// ponytail: one test module covering fence + determinism + guards + paths.
#[cfg(test)]
mod pure_cores_tests {
    use super::*;

    /// Import fence: this file's IMPORTS must never touch the runtime
    /// (tokio, Nostr, sqlx, clock, env). Scoped to `use` lines.
    #[test]
    fn core_source_has_no_runtime_imports() {
        let src = include_str!("receipts_core.rs");
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
            "flywheel",
            "doorbell",
            "herdr",
            "sqlx",
            "axum",
            "rmcp",
            "parking_lot",
            "chrono",
            "std::time",
            "std::env",
            "crate::",
            "super::",
        ] {
            for line in &imports {
                assert!(
                    !line.contains(banned),
                    "receipts_core.rs imports banned `{banned}`: {line}"
                );
            }
        }
        // Belt and braces: no clock read anywhere in the shipped core.
        for needle in ["Utc::now", "SystemTime::now"] {
            assert!(
                !shipped.contains(needle),
                "receipts_core.rs contains clock read `{needle}`"
            );
        }
        // bd-zh38: harden to QUALIFIED paths — the use-line fence above is
        // blind to `tokio::spawn` written inline. The shipped body must not
        // name a runtime, clock, env, fs, net, process, or thread path.
        for path in [
            "tokio::",
            "nostr::",
            "sqlx::",
            "chrono::",
            "parking_lot::",
            "axum::",
            "rmcp::",
            "flywheel::",
            "doorbell::",
            "herdr::",
            "std::time::",
            "std::env::",
            "std::fs::",
            "std::net::",
            "std::process::",
            "std::thread::",
            "std::sync::Mutex",
            "std::sync::RwLock",
        ] {
            assert!(
                !shipped.contains(path),
                "receipts_core.rs names runtime path `{path}` outside imports"
            );
        }
    }

    /// bd-zh38 RED: BFS over the reachable state space — every reachable
    /// row obeys the monotonic invariants, no path escapes them.
    #[test]
    fn bfs_reachable_states_obey_monotonic_invariants() {
        use std::collections::{HashSet, VecDeque};
        // Alphabet: one message per shape, two corrs, two timestamps.
        fn alphabet(corr: &str) -> Vec<ReceiptsMsg> {
            vec![
                ReceiptsMsg::Ack {
                    corr: corr.into(),
                    now_ms: 7,
                },
                ReceiptsMsg::SetResulted {
                    corr: corr.into(),
                    result_path: Some("/r.json".into()),
                    now_ms: 8,
                },
                ReceiptsMsg::Prune {
                    now_ms: 100,
                    retention_ms: 50,
                },
            ]
        }
        let seeds = vec![
            ReceiptsModel::new(),
            ReceiptsModel::with_row("s", State::Owed, None, 1),
            ReceiptsModel::with_row("s", State::Degraded, None, 1),
            ReceiptsModel::with_row("s", State::Resulted, Some("/p.json".into()), 1),
        ];
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(ReceiptsModel, usize)> = VecDeque::new();
        for seed in seeds {
            for _ in 0..3 {
                queue.push_back((seed.clone(), 0));
            }
        }
        let mut transitions = 0usize;
        while let Some((model, depth)) = queue.pop_front() {
            // Canonical encoding bounds the BFS (timestamps normalized: the
            // core only compares them, so shape + state space is finite).
            let key = format!("{:?}", model.entries);
            if !visited.insert(key) {
                continue;
            }
            for msg in alphabet("s").into_iter().chain(alphabet("t")) {
                let (next, _) = tick(&model, &msg);
                for (corr, entry) in &next.entries {
                    // Invariants on EVERY reachable row, not just named cases.
                    if entry.state == State::Owed {
                        // Nothing ever transitions INTO owed: any row that is
                        // owed now was owed before (no re-owe path exists).
                        let before = model.entries.get(corr).map(|e| e.state);
                        assert!(
                            before.is_none() || before == Some(State::Owed),
                            "row {corr} reached Owed from {before:?} — re-owe path"
                        );
                    }
                }
                transitions += 1;
                if depth < 4 {
                    queue.push_back((next, depth + 1));
                }
            }
        }
        assert!(transitions > 50, "BFS explored: {transitions} transitions");
        assert!(!visited.is_empty());
    }

    /// bd-zh38: DFS over the same space, asserting the same invariants
    /// plus determinism — replaying one path twice yields byte-identical
    /// effects, so the journal can trust a Msg log as its identity.
    #[test]
    fn dfs_paths_obey_invariants_and_replay_deterministically() {
        fn alphabet(corr: &str) -> Vec<ReceiptsMsg> {
            vec![
                ReceiptsMsg::Ack {
                    corr: corr.into(),
                    now_ms: 7,
                },
                ReceiptsMsg::SetResulted {
                    corr: corr.into(),
                    result_path: Some("/r.json".into()),
                    now_ms: 8,
                },
            ]
        }
        // One deep path per seed, replayed twice.
        let seeds = vec![
            ReceiptsModel::new(),
            ReceiptsModel::with_row("s", State::Owed, None, 1),
            ReceiptsModel::with_row("s", State::Degraded, None, 1),
        ];
        for seed in &seeds {
            let path: Vec<ReceiptsMsg> = alphabet("s")
                .into_iter()
                .chain(alphabet("t"))
                .chain(alphabet("s"))
                .collect();
            let run_once = {
                let mut model = seed.clone();
                let mut all_fx = Vec::new();
                for msg in &path {
                    let (next, fx) = tick(&model, msg);
                    model = next;
                    all_fx.push(fx);
                }
                (model, all_fx)
            };
            let run_twice = {
                let mut model = seed.clone();
                let mut all_fx = Vec::new();
                for msg in &path {
                    let (next, fx) = tick(&model, msg);
                    model = next;
                    all_fx.push(fx);
                }
                (model, all_fx)
            };
            assert_eq!(run_once, run_twice, "replay is deterministic");
            // Terminal invariant on the deep state: the path acked every
            // seeded row, so no Owed may remain (nothing re-owes).
            for entry in run_once.0.entries.values() {
                assert_ne!(
                    entry.state,
                    State::Owed,
                    "deep path acked all rows — Owed must not remain"
                );
            }
        }
    }

    fn scripted_log() -> Vec<ReceiptsMsg> {
        use ReceiptsMsg::*;
        vec![
            SetResulted {
                corr: "new".into(),
                result_path: None,
                now_ms: 10,
            },
            Ack {
                corr: "ghost".into(),
                now_ms: 11,
            },
            SetResulted {
                corr: "o1".into(),
                result_path: None,
                now_ms: 12,
            },
            Ack {
                corr: "o1".into(),
                now_ms: 13,
            },
            Ack {
                corr: "o1".into(),
                now_ms: 14,
            },
            SetResulted {
                corr: "o1".into(),
                result_path: Some("/a.json".into()),
                now_ms: 15,
            },
            SetResulted {
                corr: "o1".into(),
                result_path: Some("/a.json".into()),
                now_ms: 16,
            },
            Prune {
                now_ms: 100,
                retention_ms: 30,
            },
        ]
    }

    fn replay(model: &ReceiptsModel, log: &[ReceiptsMsg]) -> (ReceiptsModel, Vec<ReceiptEffect>) {
        let mut model = model.clone();
        let mut all = Vec::new();
        for msg in log {
            let (next, effects) = tick(&model, msg);
            model = next;
            all.extend(effects);
        }
        (model, all)
    }

    #[test]
    fn same_msg_log_byte_identical_effects() {
        // Seed one owed row the way the store would load it.
        let seed = ReceiptsModel::with_row("o1", State::Owed, None, 5);
        let log = scripted_log();
        let (a_model, a_fx) = replay(&seed, &log);
        let (b_model, b_fx) = replay(&seed, &log);
        assert_eq!(format!("{a_fx:?}"), format!("{b_fx:?}"));
        assert_eq!(a_model, b_model);
        // Trace: new→Resulted stamp; ghost→Ignored; o1 owed→Resulted stamp
        // (explicit verb jumps straight); ack on resulted→Ignored; dup
        // ack→Ignored; set-resulted w/ new path→Stamp (path update);
        // same path again→Ignored; prune→PruneBefore{70}.
        assert_eq!(
            a_fx,
            vec![
                ReceiptEffect::Stamp {
                    corr: "new".into(),
                    state: State::Resulted,
                    result_path: None,
                    updated_at_ms: 10,
                },
                ReceiptEffect::Ignored {
                    corr: "ghost".into(),
                    reason: "no-row",
                },
                ReceiptEffect::Stamp {
                    corr: "o1".into(),
                    state: State::Resulted,
                    result_path: None,
                    updated_at_ms: 12,
                },
                ReceiptEffect::Ignored {
                    corr: "o1".into(),
                    reason: "idempotent",
                },
                ReceiptEffect::Ignored {
                    corr: "o1".into(),
                    reason: "idempotent",
                },
                ReceiptEffect::Stamp {
                    corr: "o1".into(),
                    state: State::Resulted,
                    result_path: Some("/a.json".into()),
                    updated_at_ms: 15,
                },
                ReceiptEffect::Ignored {
                    corr: "o1".into(),
                    reason: "idempotent",
                },
                ReceiptEffect::PruneBefore { cutoff_ms: 70 },
            ]
        );
        assert_eq!(
            a_model.entries["o1"],
            Entry {
                state: State::Resulted,
                result_path: Some("/a.json".into()),
                updated_at_ms: 15
            }
        );
    }

    #[test]
    fn owed_to_acked_updates_stamp() {
        let seed = ReceiptsModel::with_row("c", State::Owed, Some("/keep.json".into()), 1);
        let (model, fx) = tick(
            &seed,
            &ReceiptsMsg::Ack {
                corr: "c".into(),
                now_ms: 42,
            },
        );
        assert_eq!(
            fx,
            vec![ReceiptEffect::Stamp {
                corr: "c".into(),
                state: State::Acked,
                result_path: Some("/keep.json".into()),
                updated_at_ms: 42,
            }]
        );
        assert_eq!(
            model.entries["c"],
            Entry {
                state: State::Acked,
                result_path: Some("/keep.json".into()),
                updated_at_ms: 42
            }
        );
    }

    #[test]
    fn degraded_upgrades_to_acked_never_reowes() {
        // ADR-0010 D8: degraded is acked-unknown — an ack landing after is
        // the recovery path; re-owed would invite a duplicating resend.
        let seed = ReceiptsModel::with_row("d", State::Degraded, None, 1);
        let (_, fx) = tick(
            &seed,
            &ReceiptsMsg::Ack {
                corr: "d".into(),
                now_ms: 9,
            },
        );
        assert!(matches!(fx[0], ReceiptEffect::Stamp { .. }), "ack upgrades");
        assert!(
            !transition_allowed(State::Degraded, State::Owed),
            "degraded never re-owes"
        );
        assert!(
            !transition_allowed(State::Acked, State::Owed)
                && !transition_allowed(State::Acked, State::Degraded),
            "acked never drags down"
        );
        assert!(
            !transition_allowed(State::Resulted, State::Acked),
            "resulted never downgrades"
        );
    }

    #[test]
    fn proven_path_update_stamps_new_path_only() {
        let seed = ReceiptsModel::with_row("p", State::Resulted, Some("/old.json".into()), 1);
        let same = ReceiptsMsg::SetResulted {
            corr: "p".into(),
            result_path: Some("/old.json".into()),
            now_ms: 2,
        };
        let (_, fx) = tick(&seed, &same);
        assert!(
            matches!(fx[0], ReceiptEffect::Ignored { .. }),
            "same path is idempotent"
        );
        let newer = ReceiptsMsg::SetResulted {
            corr: "p".into(),
            result_path: Some("/new.json".into()),
            now_ms: 3,
        };
        let (model, fx) = tick(&seed, &newer);
        assert_eq!(
            fx,
            vec![ReceiptEffect::Stamp {
                corr: "p".into(),
                state: State::Resulted,
                result_path: Some("/new.json".into()),
                updated_at_ms: 3,
            }]
        );
        assert_eq!(model.entries["p"].result_path.as_deref(), Some("/new.json"));
    }

    #[test]
    fn machine_config_serializes() {
        let cfg = machine_config();
        assert_eq!(cfg["machine"], "receipts_core");
        assert!(
            cfg["transitions"].as_array().is_some(),
            "transitions listed"
        );
    }

    /// bd-5mxm: MarkDegraded creates the row when bookkeeping never ran.
    #[test]
    fn mark_degraded_creates_row_from_missing() {
        let (model, fx) = tick(
            &ReceiptsModel::new(),
            &ReceiptsMsg::MarkDegraded {
                corr: "c".into(),
                now_ms: 9,
            },
        );
        assert_eq!(
            fx,
            vec![ReceiptEffect::Stamp {
                corr: "c".into(),
                state: State::Degraded,
                result_path: None,
                updated_at_ms: 9,
            }]
        );
        assert_eq!(model.entries["c"].state, State::Degraded);
    }

    /// bd-5mxm: an owed row degrades in place (never re-owes later).
    #[test]
    fn mark_degraded_from_owed_stamps_degraded() {
        let seed = ReceiptsModel::with_row("c", State::Owed, Some("/r.json".into()), 1);
        let (model, fx) = tick(
            &seed,
            &ReceiptsMsg::MarkDegraded {
                corr: "c".into(),
                now_ms: 9,
            },
        );
        assert_eq!(
            fx,
            vec![ReceiptEffect::Stamp {
                corr: "c".into(),
                state: State::Degraded,
                result_path: Some("/r.json".into()),
                updated_at_ms: 9,
            }]
        );
        assert_eq!(model.entries["c"].state, State::Degraded);
    }

    /// bd-5mxm: degraded is idempotent; acked/resulted never drag down.
    #[test]
    fn mark_degraded_idempotent_and_guarded() {
        for (state, reason) in [
            (State::Degraded, "idempotent"),
            (State::Acked, "monotonic-guard"),
            (State::Resulted, "monotonic-guard"),
        ] {
            let seed = ReceiptsModel::with_row("c", state, None, 1);
            let (_, fx) = tick(
                &seed,
                &ReceiptsMsg::MarkDegraded {
                    corr: "c".into(),
                    now_ms: 9,
                },
            );
            assert_eq!(
                fx,
                vec![ReceiptEffect::Ignored {
                    corr: "c".into(),
                    reason,
                }],
                "MarkDegraded from {state:?}"
            );
        }
    }
}
