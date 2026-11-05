// governed-by: ADR-0035
//! Run-state MUTATION check (bd-4dpq, spec `docs/notes/2026-09-26-run-state-checkpoint-design.md` §9.3).
//!
//! ## Why this file exists
//!
//! `tests/run_state_conformance.rs` proves the store behaves. This file proves
//! the BEHAVIOUR IS PINNED — that the conformance suite would go red if an
//! invariant broke. A green suite on its own is not evidence that the suite has
//! teeth; that is the one thing a test suite cannot tell you about itself.
//!
//! ## How it works
//!
//! [`violations`] is a pure function from source text to the invariants that
//! source text breaks. It is called twice per invariant:
//!
//! * once on the REAL source, which must produce nothing, and
//! * once on a DELIBERATELY MUTATED copy, which must produce that invariant.
//!
//! The second call is the mutation check proper. Each mutation below is a
//! plausible refactor accident, not a strawman: they are the edits a careful
//! engineer makes while tidying SQL, and each one is a real bug the conformance
//! suite is supposed to catch. A detector that cannot reject its own mutation
//! is decoration, so a mutation that survives fails the build.
//!
//! ## Honest limit
//!
//! This pins the store's SQL and authority SHAPE, not its runtime behaviour.
//! The behavioural half is `run_state_conformance.rs` (and, for the read side,
//! `run_state_read_side.rs`), and those are the tests that would catch a wrong
//! value. This file exists so the wiring cannot silently drift away from the
//! behaviour those tests describe — the same division of labour as
//! `flywheel/tests/journal_mode_invariant.rs`.

use std::sync::OnceLock;

const SOURCE: &str = include_str!("../src/index/run_state.rs");

/// The invariants, as detectors. Each returns a human-readable reason.
type Detector = (&'static str, fn(&str) -> Option<String>);

/// The byte range of one item's body, brace-matched from the start of `sig`.
///
/// Brace matching rather than a line window, because a body can be long and a
/// line window would let a neighbouring item satisfy the detector.
fn body_range(source: &str, sig: &str) -> Option<(usize, usize)> {
    let start = source.find(sig)?;
    let open = source[start..].find('{')? + start;
    let mut depth = 0i32;
    for (i, b) in source[open..].bytes().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some((start, open + i + 1));
                }
            }
            _ => {}
        }
    }
    None
}

fn body<'a>(source: &'a str, sig: &str) -> Option<&'a str> {
    let (a, b) = body_range(source, sig)?;
    Some(&source[a..b])
}

/// Apply a mutation INSIDE one item's body.
///
/// Scoping matters, and the first version of this file got it wrong: `seq <
/// ?2` appears in both `history` and `prune_before`, so a whole-file replace
/// mutated the wrong function and the check reported a MUTATION SURVIVED for a
/// change it had never actually made. A mutation that does not land where it
/// claims to is worse than no mutation, because it reports the guard as broken.
fn mutate(source: &str, anchor: &str, find: &str, replace: &str) -> Option<String> {
    let (start, end) = body_range(source, anchor)?;
    let rel = source[start..end].find(find)?;
    let at = start + rel;
    let mut out = String::with_capacity(source.len());
    out.push_str(&source[..at]);
    out.push_str(replace);
    out.push_str(&source[at + find.len()..]);
    Some(out)
}

fn detect_latest_orders_by_seq(source: &str) -> Option<String> {
    let b = body(source, "pub async fn latest(")?;
    if b.contains("ORDER BY seq DESC") {
        return None;
    }
    Some(
        "latest must ORDER BY seq DESC: seq is the fold's own counter and does not \
         depend on the clock, so ordering by created_at or checkpoint_id lets a \
         backwards clock step return an OLDER boundary than one already handed out"
            .to_string(),
    )
}

fn detect_history_is_bounded_and_ordered(source: &str) -> Option<String> {
    let b = body(source, "pub async fn history(")?;
    if !b.contains("limit > 0") {
        return Some(
            "history must refuse a non-positive limit: an unbounded read of a \
             long-lived thread is a memory bug, and there is deliberately no \
             '0 means everything' escape"
                .to_string(),
        );
    }
    if !b.contains("ORDER BY seq DESC") {
        return Some(
            "history must ORDER BY seq DESC so a walk resumes from the newest \
             boundary below its cursor"
                .to_string(),
        );
    }
    None
}

fn detect_prune_is_strictly_below_the_floor(source: &str) -> Option<String> {
    let b = body(source, "pub async fn prune_before(")?;
    if b.contains("seq <= ?2") {
        return Some(
            "prune must delete `seq < ?2`, never `seq <= ?2`: the floor is the \
             boundary a live resume still needs, and an inclusive delete takes \
             the resume point with it"
                .to_string(),
        );
    }
    if !b.contains("seq < ?2") {
        return Some("prune_before lost its `seq < floor` predicate".to_string());
    }
    None
}

fn detect_resume_falls_back_at_or_before(source: &str) -> Option<String> {
    let b = body(source, "pub async fn load_for_resume(")?;
    if b.contains("seq <= ?2") {
        return None;
    }
    Some(
        "load_for_resume must fall back to `seq <= ?2`. The exact lookup \
         covers the common case; the fallback is what makes a resume that asks \
         for a boundary which was pruned resume from the last real one instead \
         of refusing outright"
            .to_string(),
    )
}

fn detect_append_refuses_a_divergent_rewrite(source: &str) -> Option<String> {
    let b = body(source, "pub async fn append(")?;
    if b.contains("run-state conflict at") {
        return None;
    }
    Some(
        "append must refuse a replay that carries DIFFERENT content for an \
         existing (thread_id, seq). The log is authoritative, so silently \
         keeping either version makes the surviving model depend on write \
         order"
            .to_string(),
    )
}

fn detect_replay_is_exact_not_the_resume_seed(source: &str) -> Option<String> {
    let b = body(source, "pub async fn replay(")?;
    if b.contains("load_for_resume") {
        return Some(
            "replay must read the EXACT boundary via `get`, never through \
             load_for_resume. Falling back would show a reader a step nobody \
             asked for, and the operator would believe it is step N"
                .to_string(),
        );
    }
    if !b.contains("self.get(") {
        return Some("replay lost its exact `self.get` read".to_string());
    }
    None
}

fn detect_install_refuses(source: &str) -> Option<String> {
    let b = body(source, "pub fn install(")?;
    if b.contains("Err(InstallRefused") {
        return None;
    }
    Some(
        "RunStateReplay::install must return Err(InstallRefused) on every \
         path. It exists to be the loud answer at a call site that reaches for \
         it, and a body that can return Ok is a second seeding path beside the \
         live host's (spec S3)"
            .to_string(),
    )
}

fn detect_one_index_one_schema(source: &str) -> Option<String> {
    if source.contains("SqlitePool::connect") || source.contains("SqliteConnectOptions") {
        return Some(
            "the store must ride the EXISTING index pool, never open a \
             database of its own. One index, one schema is the same rule \
             0010_workflow_effects.sql states, and a second SQLite file would \
             be a second store"
                .to_string(),
        );
    }
    None
}

fn detectors() -> &'static [Detector] {
    static D: OnceLock<Vec<Detector>> = OnceLock::new();
    D.get_or_init(|| {
        vec![
            ("latest_orders_by_seq", detect_latest_orders_by_seq),
            (
                "history_is_bounded_and_ordered",
                detect_history_is_bounded_and_ordered,
            ),
            (
                "prune_is_strictly_below_the_floor",
                detect_prune_is_strictly_below_the_floor,
            ),
            (
                "resume_falls_back_at_or_before",
                detect_resume_falls_back_at_or_before,
            ),
            (
                "append_refuses_a_divergent_rewrite",
                detect_append_refuses_a_divergent_rewrite,
            ),
            (
                "replay_is_exact_not_the_resume_seed",
                detect_replay_is_exact_not_the_resume_seed,
            ),
            ("install_refuses", detect_install_refuses),
            ("one_index_one_schema", detect_one_index_one_schema),
        ]
    })
}

/// Every invariant the given source text breaks. Pure; the whole check is two
/// calls to this.
fn violations(source: &str) -> Vec<String> {
    detectors()
        .iter()
        .filter_map(|(name, detect)| detect(source).map(|why| format!("{name}: {why}")))
        .collect()
}

#[test]
fn the_store_breaks_no_invariant_this_file_pins() {
    assert_eq!(
        violations(SOURCE),
        Vec::<String>::new(),
        "a real invariant broke in run_state.rs — if the change was intended, \
         the pin is what has to change with it, not the other way round"
    );
}

#[test]
fn every_detector_still_finds_what_it_claims_to_check() {
    for (sig, name) in [
        ("pub async fn latest(", "latest_orders_by_seq"),
        ("pub async fn history(", "history_is_bounded_and_ordered"),
        (
            "pub async fn prune_before(",
            "prune_is_strictly_below_the_floor",
        ),
        (
            "pub async fn load_for_resume(",
            "resume_falls_back_at_or_before",
        ),
        ("pub async fn append(", "append_refuses_a_divergent_rewrite"),
        (
            "pub async fn replay(",
            "replay_is_exact_not_the_resume_seed",
        ),
        ("pub fn install(", "install_refuses"),
    ] {
        assert!(
            body(SOURCE, sig).is_some(),
            "{name}: `{sig}` is gone from run_state.rs, so its detector can no \
             longer see anything and the pin it guards is gone with it. Add a \
             detector for the new shape rather than deleting the pin."
        );
    }
}

/// A mutation: the anchor it lands inside, the invariant it must break, the
/// text it replaces, and what the change would mean if it shipped.
///
/// Each is a plausible accident, not a strawman: the tidy-ups a careful
/// refactor makes to SQL that quietly change what the store means.
fn mutations() -> Vec<(
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
)> {
    vec![
        (
            "pub async fn latest(",
            "latest_orders_by_seq",
            "ORDER BY seq DESC",
            "ORDER BY created_at DESC",
            "ordering latest by the clock instead of the fold's counter",
        ),
        (
            "pub async fn latest(",
            "latest_orders_by_seq",
            "ORDER BY seq DESC",
            "ORDER BY checkpoint_id DESC",
            "ordering latest by an id that embeds created_at",
        ),
        (
            "pub async fn history(",
            "history_is_bounded_and_ordered",
            "limit > 0",
            "limit >= 0",
            "letting limit=0 mean unbounded again",
        ),
        (
            "pub async fn history(",
            "history_is_bounded_and_ordered",
            "ORDER BY seq DESC LIMIT ?3",
            "ORDER BY seq ASC LIMIT ?3",
            "returning history oldest-first, which breaks a walk's cursor",
        ),
        (
            "pub async fn prune_before(",
            "prune_is_strictly_below_the_floor",
            "seq < ?2",
            "seq <= ?2",
            "an inclusive prune that takes the resume point with it",
        ),
        (
            "pub async fn load_for_resume(",
            "resume_falls_back_at_or_before",
            "seq <= ?2",
            "seq = ?2",
            "a resume that refuses whenever the exact boundary is gone",
        ),
        (
            "pub async fn append(",
            "append_refuses_a_divergent_rewrite",
            "run-state conflict at",
            "rewrote the same boundary",
            "last-write-wins, so the log stops being authoritative",
        ),
        (
            "pub async fn replay(",
            "replay_is_exact_not_the_resume_seed",
            "self.get(thread_id, seq)",
            "self.load_for_resume(thread_id, seq)",
            "a replay that silently shows the nearest boundary instead",
        ),
        (
            "pub fn install(",
            "install_refuses",
            "Err(InstallRefused {",
            "Ok(RunStateCheckpoint {",
            "an installer growing on the read side",
        ),
        (
            "pub const fn new(pool: SqlitePool) -> Self {",
            "one_index_one_schema",
            "Self { pool }",
            "Self { pool: SqlitePool::connect(\"second.sqlite\").await.unwrap() }",
            "the store opening a second SQLite file of its own",
        ),
    ]
}

#[test]
fn every_mutation_is_caught_by_the_detector_that_guards_it() {
    for (anchor, name, find, replace, what) in mutations() {
        let mutated = mutate(SOURCE, anchor, find, replace).unwrap_or_else(|| {
            panic!(
                "mutation `{what}` cannot be applied: `{find}` is not inside `{anchor}` in \
                 run_state.rs. The mutation drifted from the code, which means the check is \
                 no longer proving anything — update the mutation."
            )
        });

        let broken = violations(&mutated);
        assert!(
            broken.iter().any(|v| v.starts_with(&format!("{name}: "))),
            "MUTATION SURVIVED: {what}\n  replaced `{find}` with `{replace}` inside \
             `{anchor}`\n  detected: {broken:?}\n  expected: {name} to fire. A detector \
             that cannot reject its own mutation is decoration, not a check."
        );
    }
}

#[test]
fn a_mutation_to_one_invariant_does_not_trip_the_others() {
    for (anchor, name, find, replace, what) in mutations() {
        let mutated = mutate(SOURCE, anchor, find, replace).expect("mutation applies");
        let broken = violations(&mutated);
        assert_eq!(
            broken.len(),
            1,
            "mutation `{what}` tripped {} pins ({broken:?}), expected only {name}. A \
             detector that fires on unrelated changes cannot be read as evidence that it \
             caught the thing it was aimed at.",
            broken.len()
        );
    }
}
