// governed-by: ADR-0035
//! Pure reservations decision core (ADR-0035 Spec 1, bd-ati6).
//!
//! Verbatim shape of `dispatch_core.rs`: **std + serde only** — no tokio,
//! no Nostr types, no sqlx, no clock, no env, no statics (fenced by test
//! below). One `Model`, one `Msg` union, one [`tick`]: `tick(&Model, &Msg)
//! -> (Model, Vec<Effect>)`. Effects are data; the thin host (today the
//! bus/event projection in `reservations.rs`) executes them. Same Msg log
//! → byte-identical Effects (tested).
//!
//! The core decides on projected rows only: lease/intent rows live in the
//! bus kind-30031 store (lossless-projection rule), so [`LeaseEntry`]
//! carries holder + glob + TTL + epoch and nothing else. Atomic claim is a
//! host property (one transaction per tick-batch); the core never
//! check-then-acts across awaits because it never awaits.
//!
//! Glob matching is a verbatim copy of `guard.rs` (`pattern_matches` +
//! helpers) and `overlaps` from `reservations.rs` — one semantics, two
//! homes, fenced by the copy note (a drift test lives with the host).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Intent TTL seconds (mirror of `reservations::INTENT_TTL_SECS` — an
/// intent auto-expires this long after declaration unless converted).
pub const INTENT_TTL_SECS: i64 = 5 * 60;

/// Entry lifecycle (subset of `ReservationStatus` the kernel decides on).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntryState {
    Held,
    Intent,
}

/// One projected lease/intent row. Plain data: no handles, no locks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseEntry {
    pub id: String,
    pub holder: String,
    pub path_pattern: String,
    pub exclusive: bool,
    /// NIP-40 expiry (µs); `None` = no expiry.
    pub expires_us: Option<i64>,
    /// Declaration time (unix seconds; bounds intent TTL).
    pub created_at_secs: i64,
    /// Fencing epoch (monotonic per coordinate; carried, not decided here).
    pub epoch: u64,
    pub state: EntryState,
    /// Liveness-oracle voucher (bd-lease-liveness-bo0g): an expired TTL
    /// still blocks while the holder's session is live.
    pub liveness_extended: bool,
}

/// Model: the whole reservation table. Clone-and-return keeps `tick` pure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelModel {
    pub leases: BTreeMap<String, LeaseEntry>,
}

impl KernelModel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test constructor free of bus/clock/pools (ADR-0031 A8).
    pub fn test_new() -> Self {
        Self::new()
    }
}

/// All state changes enter as messages. Clocks arrive as fields
/// (`now_us`, i64-µs per ADR G7) — the core never reads time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KernelMsg {
    /// Request a lease (or declare an intent via `state`).
    Acquire {
        id: String,
        holder: String,
        path_pattern: String,
        exclusive: bool,
        expires_us: Option<i64>,
        created_at_secs: i64,
        epoch: u64,
        state: EntryState,
        now_us: i64,
    },
    /// Drop a lease by id.
    Release { id: String },
    /// Extend one lease's expiry (holder must match).
    Renew {
        id: String,
        holder: String,
        expires_us: Option<i64>,
    },
    /// Sweep expired TTLs and stale intents at host time `now_us`.
    ExpireTick { now_us: i64 },
}

/// Intents the core journals; the host executes. No I/O in [`tick`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KernelEffect {
    Granted { id: String },
    Denied { id: String, reason: DenyReason },
    Released { id: String },
    Renewed { id: String },
    Expired { ids: Vec<String> },
}

/// Why an acquire was denied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DenyReason {
    /// Id already present (replay-safe: never double-grant).
    DuplicateId,
    /// Another holder's live exclusive lease overlaps.
    HardConflict { blocking_id: String },
}

/// Named pure predicates only — no inline boolean soup in [`tick`].
const fn ttl_live(entry: &LeaseEntry, now_us: i64) -> bool {
    match entry.expires_us {
        None => true,
        Some(expires) => expires > now_us,
    }
}

const fn is_live(entry: &LeaseEntry, now_us: i64) -> bool {
    // Oracle-vouched rows outlive their TTL (host sets the flag from the
    // liveness oracle + SQL mirror; the kernel only honors it).
    if ttl_live(entry, now_us) {
        return true;
    }
    entry.liveness_extended
}

const fn intent_live(entry: &LeaseEntry, now_us: i64) -> bool {
    entry.created_at_secs.saturating_add(INTENT_TTL_SECS) * 1_000_000 >= now_us
}

fn entry_blocks(entry: &LeaseEntry, requester: &str, path: &str, now_us: i64) -> bool {
    entry.holder != requester
        && entry.exclusive
        && is_live(entry, now_us)
        && match entry.state {
            EntryState::Held => true,
            EntryState::Intent => intent_live(entry, now_us),
        }
        && overlaps(&entry.path_pattern, path)
}

/// bd-4dl4: a live SHARED lease by another agent, overlapping `path`.
///
/// NOT a blocker. A shared lease is a declaration, not a fence: it says
/// "someone is working here", and the whole point of the shared tier is that
/// it does not stop anyone. What it must never be is INVISIBLE - before this,
/// a held shared lease matched no branch, so a path someone had declared was
/// reported in `clear_paths` with nothing else set, and an operator reading
/// `conflict_free: true` concluded nobody was there.
///
/// This is the same reason `entry_blocks` needs `exclusive`: the two
/// functions answer different questions about the same overlap. One decides
/// WHO GETS DENIED; this one decides WHO GETS NAMED.
pub fn entry_notices(entry: &LeaseEntry, requester: &str, path: &str, now_us: i64) -> bool {
    entry.holder != requester
        && !entry.exclusive
        && is_live(entry, now_us)
        && overlaps(&entry.path_pattern, path)
}

/// Every lease in `model` that a reader must be told about for `path`,
/// regardless of whether it blocks. Pure; the advisory twin of the blocking
/// scan, and the reason `clear_paths` can be trusted to mean "nobody is here".
pub fn advisory_viewers(
    model: &KernelModel,
    requester: &str,
    path: &str,
    now_us: i64,
) -> Vec<String> {
    let mut out: Vec<String> = model
        .leases
        .values()
        .filter(|e| {
            entry_blocks(e, requester, path, now_us) || entry_notices(e, requester, path, now_us)
        })
        .map(|e| e.id.clone())
        .collect();
    out.sort();
    out
}

/// Pure decision: `(Model, Msg) -> (Model, Effects)`.
pub fn tick(model: &KernelModel, msg: &KernelMsg) -> (KernelModel, Vec<KernelEffect>) {
    let mut next = model.clone();
    match msg {
        KernelMsg::Acquire {
            id,
            holder,
            path_pattern,
            exclusive,
            expires_us,
            created_at_secs,
            epoch,
            state,
            now_us,
        } => {
            if next.leases.contains_key(id) {
                return (
                    next,
                    vec![KernelEffect::Denied {
                        id: id.clone(),
                        reason: DenyReason::DuplicateId,
                    }],
                );
            }
            // Every request respects other holders' live exclusive
            // overlapping rows (conflict_view parity); shared leases
            // themselves never block.
            let blocker = next
                .leases
                .values()
                .find(|e| entry_blocks(e, holder, path_pattern, *now_us))
                .map(|e| e.id.clone());
            if let Some(blocking_id) = blocker {
                return (
                    next,
                    vec![KernelEffect::Denied {
                        id: id.clone(),
                        reason: DenyReason::HardConflict { blocking_id },
                    }],
                );
            }
            next.leases.insert(
                id.clone(),
                LeaseEntry {
                    id: id.clone(),
                    holder: holder.clone(),
                    path_pattern: path_pattern.clone(),
                    exclusive: *exclusive,
                    expires_us: *expires_us,
                    created_at_secs: *created_at_secs,
                    epoch: *epoch,
                    state: *state,
                    // Fresh grants are TTL-live by admission; the oracle
                    // flag arrives only via projected mirror rows.
                    liveness_extended: false,
                },
            );
            (next, vec![KernelEffect::Granted { id: id.clone() }])
        }
        KernelMsg::Release { id } => {
            if next.leases.remove(id).is_some() {
                (next, vec![KernelEffect::Released { id: id.clone() }])
            } else {
                (next, Vec::new())
            }
        }
        KernelMsg::Renew {
            id,
            holder,
            expires_us,
        } => match next.leases.get_mut(id) {
            Some(entry) if entry.holder == *holder => {
                entry.expires_us = *expires_us;
                (next, vec![KernelEffect::Renewed { id: id.clone() }])
            }
            _ => (next, Vec::new()),
        },
        KernelMsg::ExpireTick { now_us } => {
            let mut dead: Vec<String> = next
                .leases
                .iter()
                .filter(|(_, e)| {
                    !is_live(e, *now_us)
                        || (e.state == EntryState::Intent && !intent_live(e, *now_us))
                })
                .map(|(id, _)| id.clone())
                .collect();
            dead.sort();
            for id in &dead {
                next.leases.remove(id);
            }
            if dead.is_empty() {
                (next, Vec::new())
            } else {
                (next, vec![KernelEffect::Expired { ids: dead }])
            }
        }
    }
}

/// Machine config JSON for the visualizer (dispatch_core pattern).
pub fn machine_config() -> serde_json::Value {
    serde_json::json!({
        "machine": "reservations_core",
        "states": ["Held", "Intent"],
        "transitions": [
            {"on": "Acquire", "guard": "no_live_exclusive_conflict", "effects": ["Granted", "Denied"]},
            {"on": "Release", "guard": null, "effects": ["Released"]},
            {"on": "Renew", "guard": "holder_matches", "effects": ["Renewed"]},
            {"on": "ExpireTick", "guard": null, "effects": ["Expired"]},
        ],
        "guards": ["ttl_live", "is_live", "intent_live", "entry_blocks"],
    })
}

// ── Glob matcher: verbatim copy of guard.rs ──────────────────────────────
// (`pattern_matches` + `match_impl` + `parse_class` + `class_contains`,
// plus `overlaps` from reservations.rs. One semantics; drift guarded by
// the host-side copy test.)

/// Symmetric-glob conflict: either side's glob matches the other.
pub fn overlaps(a: &str, b: &str) -> bool {
    pattern_matches(a, b) || pattern_matches(b, a)
}

pub fn pattern_matches(pattern: &str, path: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = path.chars().collect();
    match_impl(&pat, &text, 0, 0)
}

fn match_impl(pat: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    if pi == pat.len() {
        return ti == text.len();
    }
    match pat[pi] {
        '*' => {
            let mut next = pi;
            while next < pat.len() && pat[next] == '*' {
                next += 1;
            }
            if next == pat.len() {
                return true;
            }
            for k in ti..=text.len() {
                if match_impl(pat, text, next, k) {
                    return true;
                }
            }
            false
        }
        '?' => ti < text.len() && match_impl(pat, text, pi + 1, ti + 1),
        '[' => {
            if ti >= text.len() {
                return false;
            }
            match parse_class(pat, pi) {
                Some((neg, end)) => {
                    let contained = class_contains(pat, pi + 1, end - 1, text[ti]);
                    (if neg { !contained } else { contained }) && match_impl(pat, text, end, ti + 1)
                }
                None => text[ti] == '[' && match_impl(pat, text, pi + 1, ti + 1),
            }
        }
        c => ti < text.len() && text[ti] == c && match_impl(pat, text, pi + 1, ti + 1),
    }
}

const fn parse_class(pat: &[char], start: usize) -> Option<(bool, usize)> {
    let mut i = start + 1;
    if i >= pat.len() {
        return None;
    }
    let neg = pat[i] == '!' || pat[i] == '^';
    if neg {
        i += 1;
    }
    if i >= pat.len() || pat[i] == ']' {
        return None;
    }
    while i < pat.len() && pat[i] != ']' {
        i += 1;
    }
    if i >= pat.len() {
        None
    } else {
        Some((neg, i + 1))
    }
}

fn class_contains(pat: &[char], lo: usize, hi: usize, c: char) -> bool {
    let mut i = lo;
    while i <= hi {
        if i + 2 <= hi && pat[i + 1] == '-' {
            let (a, b) = (pat[i], pat[i + 2]);
            if (a..=b).contains(&c) {
                return true;
            }
            i += 3;
        } else {
            if pat[i] == c {
                return true;
            }
            i += 1;
        }
    }
    false
}

// ponytail: one test module covering fence + determinism + model paths.
// New transitions bring their own cases here.
#[cfg(test)]
mod kernel_tests {
    use super::*;

    /// Import fence: this file's IMPORTS must never touch the runtime, and
    /// the shipped body must name no clock, env, statics, or threads.
    /// Scoped to `use` lines + body scan (doc prose may name the banned).
    #[test]
    fn core_source_has_no_runtime_imports() {
        let src = include_str!("reservations_core.rs");
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
            "uuid",
            "crate::",
            "super::",
        ] {
            for line in &imports {
                assert!(
                    !line.contains(banned),
                    "reservations_core.rs imports banned `{banned}`: {line}"
                );
            }
        }
        for needle in [
            "Utc::now",
            "SystemTime::now",
            "std::env",
            "std::time",
            "static ",
            "Mutex",
            "std::thread",
            "tokio::",
        ] {
            assert!(
                !shipped.contains(needle),
                "reservations_core.rs body names banned `{needle}`"
            );
        }
    }

    fn acquire(id: &str, holder: &str, path: &str, exclusive: bool, now_us: i64) -> KernelMsg {
        KernelMsg::Acquire {
            id: id.into(),
            holder: holder.into(),
            path_pattern: path.into(),
            exclusive,
            expires_us: Some(now_us + 3_600_000_000),
            created_at_secs: 1_700_000_000,
            epoch: 0,
            state: EntryState::Held,
            now_us,
        }
    }

    fn replay(model: &KernelModel, log: &[KernelMsg]) -> (KernelModel, Vec<KernelEffect>) {
        let mut model = model.clone();
        let mut all = Vec::new();
        for msg in log {
            let (next, effects) = tick(&model, msg);
            model = next;
            all.extend(effects);
        }
        (model, all)
    }

    /// Determinism: one scripted log replays to byte-identical Models and
    /// Effects, twice.
    #[test]
    fn same_msg_log_byte_identical_effects() {
        let log = vec![
            acquire("a", "alice", "src/**", true, 1_000),
            acquire("b", "bob", "src/x.rs", true, 2_000),
            acquire("c", "bob", "docs/**", false, 3_000),
            KernelMsg::Renew {
                id: "a".into(),
                holder: "alice".into(),
                expires_us: Some(9_999_999_999),
            },
            KernelMsg::Release { id: "c".into() },
            KernelMsg::ExpireTick {
                now_us: 99_999_999_999,
            },
        ];
        let (ma, fa) = replay(&KernelModel::new(), &log);
        let (mb, fb) = replay(&KernelModel::new(), &log);
        assert_eq!(format!("{fa:?}"), format!("{fb:?}"));
        assert_eq!(ma, mb);
        // Trace: a granted; b denied (alice's exclusive src/** blocks);
        // c granted (shared, but bob's docs/** touches nothing exclusive...
        // shared still respects others' exclusive leases: docs/** is free);
        // renew ok; release ok; expire sweeps a (TTL out).
        assert_eq!(
            fa,
            vec![
                KernelEffect::Granted { id: "a".into() },
                KernelEffect::Denied {
                    id: "b".into(),
                    reason: DenyReason::HardConflict {
                        blocking_id: "a".into()
                    },
                },
                KernelEffect::Granted { id: "c".into() },
                KernelEffect::Renewed { id: "a".into() },
                KernelEffect::Released { id: "c".into() },
                KernelEffect::Expired {
                    ids: vec!["a".into()]
                },
            ]
        );
    }

    /// Model-paths sweep: BFS over a small op alphabet from the empty
    /// model; every visited state upholds the kernel invariants.
    #[test]
    fn bfs_model_paths_uphold_invariants() {
        // Alphabet: two agents × two paths × {exclusive acquire, shared
        // acquire, release, expire-tick at two clocks}.
        let holders = ["alice", "bob"];
        let paths = ["src/**", "docs/**"];
        let mut seq = 0u64;
        let mut next_id = || {
            seq += 1;
            format!("op-{seq}")
        };
        let mut ops: Vec<KernelMsg> = Vec::new();
        for h in holders {
            for p in paths {
                ops.push(acquire(&next_id(), h, p, true, 1_000));
                ops.push(acquire(&next_id(), h, p, false, 1_000));
            }
        }
        ops.push(KernelMsg::Release { id: "op-1".into() });
        ops.push(KernelMsg::ExpireTick { now_us: 1_000 });
        ops.push(KernelMsg::ExpireTick {
            now_us: 99_999_999_999,
        });

        // BFS to depth 4 over op indices (order matters; cap breadth).
        let mut visited = 0u32;
        let mut stack = vec![(KernelModel::new(), Vec::<usize>::new())];
        while let Some((model, path)) = stack.pop() {
            assert_model_invariants(&model);
            visited += 1;
            if path.len() >= 4 {
                continue;
            }
            for (i, op) in ops.iter().enumerate() {
                // Bound the fan-out: full breadth for the first two
                // levels, then a deterministic diagonal (still covers
                // grant/deny/release/expire shapes across agents/paths).
                if path.len() >= 2 && i != (path[0] + path.len()) % ops.len() {
                    continue;
                }
                let (next, _) = tick(&model, op);
                let mut path2 = path.clone();
                path2.push(i);
                stack.push((next, path2));
            }
        }
        assert!(visited > 50, "paths swept, got {visited}");
    }

    /// Invariants checked at every BFS state.
    fn assert_model_invariants(model: &KernelModel) {
        // No two live exclusive leases from different holders overlap.
        let live: Vec<&LeaseEntry> = model.leases.values().collect();
        for (i, a) in live.iter().enumerate() {
            for b in &live[i + 1..] {
                if a.exclusive && b.exclusive && a.holder != b.holder {
                    assert!(
                        !overlaps(&a.path_pattern, &b.path_pattern),
                        "exclusive overlap: {} vs {}",
                        a.id,
                        b.id
                    );
                }
            }
        }
        // Ids are unique by construction (BTreeMap keys).
        assert_eq!(model.leases.len(), live.len());
    }

    #[test]
    fn own_lease_never_conflicts() {
        // Same holder re-acquiring an overlapping path always grants.
        let (m1, _) = tick(
            &KernelModel::new(),
            &acquire("a", "alice", "src/**", true, 1_000),
        );
        let (_, fx) = tick(&m1, &acquire("b", "alice", "src/x.rs", true, 2_000));
        assert_eq!(fx, vec![KernelEffect::Granted { id: "b".into() }]);
    }

    #[test]
    fn duplicate_id_denied_not_double_granted() {
        let (m1, _) = tick(
            &KernelModel::new(),
            &acquire("a", "alice", "src/**", true, 1_000),
        );
        let (m2, fx) = tick(&m1, &acquire("a", "bob", "other/**", true, 2_000));
        assert!(matches!(
            fx[0],
            KernelEffect::Denied {
                reason: DenyReason::DuplicateId,
                ..
            }
        ));
        assert_eq!(m2.leases.len(), 1);
    }

    #[test]
    fn renew_rejects_foreign_holder() {
        let (m1, _) = tick(
            &KernelModel::new(),
            &acquire("a", "alice", "src/**", true, 1_000),
        );
        let (_, fx) = tick(
            &m1,
            &KernelMsg::Renew {
                id: "a".into(),
                holder: "bob".into(),
                expires_us: None,
            },
        );
        assert!(fx.is_empty(), "foreign renew emits nothing");
    }

    #[test]
    fn expired_vouched_lease_still_blocks() {
        // bd-lease-liveness-bo0g: TTL past but oracle-vouched → blocks.
        let mut model = KernelModel::new();
        model.leases.insert(
            "v".into(),
            LeaseEntry {
                id: "v".into(),
                holder: "alice".into(),
                path_pattern: "src/**".into(),
                exclusive: true,
                expires_us: Some(500),
                created_at_secs: 1,
                epoch: 0,
                state: EntryState::Held,
                liveness_extended: true,
            },
        );
        let (_, fx) = tick(&model, &acquire("b", "bob", "src/x.rs", true, 1_000));
        assert!(
            matches!(
                &fx[0],
                KernelEffect::Denied {
                    reason: DenyReason::HardConflict { .. },
                    ..
                }
            ),
            "vouched expiry blocks: {fx:?}"
        );
        // Same row unvouched: expiry frees the path.
        let mut model2 = model;
        model2.leases.get_mut("v").unwrap().liveness_extended = false;
        let (_, fx2) = tick(&model2, &acquire("b", "bob", "src/x.rs", true, 1_000));
        assert_eq!(fx2, vec![KernelEffect::Granted { id: "b".into() }]);
    }

    #[test]
    fn machine_config_serializes() {
        let cfg = machine_config();
        assert_eq!(cfg["machine"], "reservations_core");
        assert!(cfg["transitions"].as_array().is_some());
    }
}
