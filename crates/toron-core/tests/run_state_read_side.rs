// governed-by: ADR-0035
//! Run-state READ side (bd-7n8e): replay, dry fork, refusal to install.
//!
//! Spec: `docs/notes/2026-09-26-run-state-checkpoint-design.md` §3, §6, §8.
//!
//! EFFECT RULES (binding), ADR-0035: the only effect in this file is one SQL
//! read against a temp-file sqlite on the PRODUCTION code path
//! (`schema::connect` + `schema::migrate`). No network. No wall clock: `seq`
//! and `created_at_us` are passed in and pinned, which is the whole reason the
//! store takes them as parameters.
//!
//! What these pin, and why each is a contract rather than a restatement:
//!
//! 1. `replay` is EXACT — a missing boundary is a typed failure naming the
//!    boundary, never the nearest row below it. A reader that fell back would
//!    show a step nobody asked for, and the operator would believe it is step
//!    N. This is the difference from `load_for_resume`, which MUST fall back.
//! 2. the pending-write set renders ALONGSIDE the model, so a reader can see
//!    which effects a boundary owned before deciding to re-enter it
//! 3. `NULL` effect_keys is a boundary that owned nothing — a fact, not a
//!    missing read — while a MALFORMED one fails closed rather than reading as
//!    empty (an empty set tells a reader re-entry is safe)
//! 4. `plan_fork` is PURE: one boundary + one intent ⇒ one plan, every time
//! 5. fork is DRY by default, and `Perform` cannot be reached without naming
//!    the hazard it accepts
//! 6. NO code path installs: `install` refuses and says who owns the call

use std::path::PathBuf;

use toron_core::index::run_state::{
    AcknowledgeDispatchedEffects, ForkIntent, NewRunStateCheckpoint, RunStateReplay, RunStateStore,
    plan_fork,
};
use toron_core::index::schema;

const T: &str = "thread-read-side";

async fn store(tag: &str) -> (RunStateStore, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "toron-run-state-read-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(path.to_str().expect("utf-8 path"))
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    (RunStateStore::new(pool), path)
}

/// A boundary that owned `effect_keys` (None = the row carried no set).
fn boundary(seq: i64, effect_keys: Option<&str>) -> NewRunStateCheckpoint {
    NewRunStateCheckpoint {
        thread_id: T.to_string(),
        seq,
        parent_checkpoint_id: None,
        model_json: format!(r#"{{"step":{seq}}}"#),
        msg_kind: Some("WatchNdjson".to_string()),
        effect_keys: effect_keys.map(str::to_string),
        created_at: seq * 1_000,
    }
}

// --- 1. replay is exact ---------------------------------------------------

#[tokio::test]
async fn replay_returns_the_model_and_the_pending_write_set_at_that_boundary() {
    let (s, _p) = store("replay-hit").await;
    s.append(boundary(1, None)).await.expect("append 1");
    s.append(boundary(
        2,
        Some(r#"["prompt:pane-1","fetch-inbox:toron"]"#),
    ))
    .await
    .expect("append 2");

    let r = s.replay(T, 2).await.expect("boundary 2 replays");

    // The model, not a re-derivation of it: the row is the authority.
    assert_eq!(r.model["step"], 2, "the model is the stored one");
    assert_eq!(r.seq, 2);
    // The half a bare model_json cannot give a reader. Without these keys a
    // reader cannot tell whether re-entering this boundary repeats a
    // dispatch that already left the machine.
    assert_eq!(
        r.effect_keys,
        vec!["prompt:pane-1".to_string(), "fetch-inbox:toron".to_string()],
        "the pending-write set renders next to the model"
    );
    assert_eq!(r.msg_kind.as_deref(), Some("WatchNdjson"));
    assert!(!r.checkpoint_id.is_empty(), "the boundary names itself");
}

#[tokio::test]
async fn replay_of_a_missing_boundary_fails_loud_instead_of_showing_a_neighbour() {
    let (s, _p) = store("replay-miss").await;
    for seq in 1..=3 {
        s.append(boundary(seq, None)).await.expect("append");
    }

    // TYPED FAILURE, not a defect and not a hang: the caller named a boundary
    // and got an answer saying there is not one. The message has to name it,
    // because the whole refusal is "this is not the step you asked for".
    let err = s.replay(T, 9).await.expect_err("boundary 9 does not exist");
    let text = err.to_string();
    assert!(
        text.contains(&format!("{T}/9")),
        "the error names the boundary asked for, so the operator knows which \
         step is missing: {text}"
    );

    // The contrast that makes the contract legible: the resume seed is the one
    // read allowed to fall back, and it still finds 3. If `replay` also fell
    // back it would be indistinguishable from a resume, which is exactly the
    // confusion it must not create.
    assert_eq!(
        s.load_for_resume(T, 9)
            .await
            .expect("resume")
            .expect("row")
            .seq,
        3,
        "resume falls back at-or-before; replay does not. One finds the row \
         below, the other refuses, and the difference is the contract."
    );
}

// --- 2. the pending-write set is never silently empty ---------------------

#[tokio::test]
async fn a_null_pending_write_set_is_a_boundary_that_owned_nothing() {
    let (s, _p) = store("null-keys").await;
    s.append(boundary(1, None)).await.expect("append");
    let r = s.replay(T, 1).await.expect("replay");
    assert!(
        r.effect_keys.is_empty(),
        "NULL is a real state — a boundary that owned no effects — and reads \
         as an empty set rather than as a failed read"
    );
}

#[tokio::test]
async fn an_unreadable_pending_write_set_fails_closed_instead_of_reading_as_empty() {
    let (s, _p) = store("bad-keys").await;
    s.append(boundary(1, Some(r#"["prompt:pane-1""#)))
        .await
        .expect("append");

    // The dangerous reading: an empty set tells a reader that re-entering this
    // boundary is safe. A corrupt set that reads as empty would say exactly
    // that about a boundary whose effects nobody can name. Fail closed.
    let err = s
        .replay(T, 1)
        .await
        .expect_err("a corrupt pending-write set must not read as empty");
    let text = format!("{err:#}");
    assert!(
        text.contains("pending-write set") && text.contains(&format!("{T}/1")),
        "the error says WHICH boundary and WHAT was unreadable, so the fix is \
         to inspect that row: {text}"
    );

    // The row is still visible, so an operator can see the boundary exists
    // and is unreadable rather than concluding the thread ends at nothing.
    assert!(
        s.get(T, 1).await.expect("get").is_some(),
        "the boundary remains visible even though its reader refuses"
    );
}

// --- 3. plan_fork is a pure fold -----------------------------------------

fn replay_with_keys(keys: &[&str]) -> RunStateReplay {
    RunStateReplay {
        thread_id: T.to_string(),
        seq: 4,
        checkpoint_id: "0000000000001000-000000000004-0123456789abcdef".to_string(),
        parent_checkpoint_id: None,
        msg_kind: Some("WatchNdjson".to_string()),
        created_at: 4_000,
        model: serde_json::json!({ "step": 4 }),
        effect_keys: keys.iter().map(|k| (*k).to_string()).collect(),
    }
}

#[test]
fn one_boundary_and_one_intent_produce_one_plan_every_time() {
    // DETERMINISM. The fold is pure, so the same two inputs cannot produce two
    // answers. Run it enough times that a hidden clock or a HashMap iteration
    // order would show up as a difference.
    let b = replay_with_keys(&["prompt:pane-1"]);
    let first = plan_fork(&b, &ForkIntent::Dry);
    for _ in 0..64 {
        assert_eq!(
            plan_fork(&b, &ForkIntent::Dry),
            first,
            "the same boundary and intent must give the same plan; a \
             difference means something non-pure leaked into the decision"
        );
    }
}

#[test]
fn the_default_intent_is_dry_so_dry_is_what_a_caller_gets_for_free() {
    // DRY BY DEFAULT is a property of the TYPE, not of a caller remembering.
    // `ForkIntent::default()` is what an unconfigured caller reaches, and the
    // only way to perform is to name the variant explicitly.
    assert_eq!(ForkIntent::default(), ForkIntent::Dry);
    let plan = plan_fork(
        &replay_with_keys(&["prompt:pane-1"]),
        &ForkIntent::default(),
    );
    assert!(!plan.would_perform, "the default intent dispatches nothing");
}

// --- 4. the fork hazard is visible, not optional --------------------------

#[test]
fn a_dry_fork_from_a_boundary_that_owned_effects_names_those_effects() {
    let plan = plan_fork(
        &replay_with_keys(&["prompt:pane-1", "fetch-inbox:toron"]),
        &ForkIntent::Dry,
    );

    assert_eq!(
        plan.inherited_effect_keys,
        vec!["prompt:pane-1".to_string(), "fetch-inbox:toron".to_string()],
        "the plan carries the set a reader must reason about"
    );
    // The refusal that BLOCKS reads first, because it is the one that decides
    // whether performing is even legal.
    let first = plan.refusals.first().expect("at least one refusal");
    assert!(
        first.contains("prompt:pane-1") && first.contains("already reached a provider"),
        "the blocking refusal names the effects and the hazard: {first}"
    );
    assert!(
        plan.refusals.iter().any(|r| r.contains("no installer")),
        "and the plan says it cannot install, as data an operator can read"
    );
}

#[test]
fn performing_still_cannot_install_and_still_reports_the_hazard_first() {
    // ACKNOWLEDGEMENT IS REQUIRED, and even once given it buys nothing here:
    // the acknowledgement waives the HAZARD, not the absence of an installer.
    let plan = plan_fork(
        &replay_with_keys(&["prompt:pane-1"]),
        &ForkIntent::Perform(AcknowledgeDispatchedEffects::accept()),
    );
    assert!(
        plan.would_perform,
        "an acknowledged Perform intent is a performing plan"
    );
    assert!(
        !plan.installable,
        "and it is still not installable: seeding a live model is the live \
         host's authority call, and no acknowledgement changes whose call it is"
    );
    assert!(
        plan.refusals
            .first()
            .is_some_and(|r| r.contains("already reached a provider")),
        "the hazard still leads the refusals after the acknowledgement, \
         because it is still true"
    );
}

#[test]
fn a_fork_from_a_boundary_that_owned_nothing_still_refuses_to_install() {
    // The absence of effects is not a licence: it removes ONE refusal and the
    // installer refusal stands, so a plan can never read as "safe to resume".
    let plan = plan_fork(&replay_with_keys(&[]), &ForkIntent::Dry);
    assert!(
        plan.refusals.iter().all(|r| !r.contains("already reached")),
        "no effects owned means no dispatch hazard to report"
    );
    assert!(
        plan.refusals.iter().any(|r| r.contains("no installer")),
        "but the plan still refuses to install"
    );
    assert!(!plan.installable);
}

// --- 5. no code path installs --------------------------------------------

#[test]
fn install_refuses_and_says_whose_call_the_authority_is() {
    let err = replay_with_keys(&["prompt:pane-1"])
        .install()
        .expect_err("install must always refuse");
    let text = err.to_string();
    assert!(
        text.contains(&format!("{T}/4")),
        "the refusal names the boundary that was not installed: {text}"
    );
    assert!(
        text.contains("authority") && text.contains("second seeding path"),
        "and names the rule and the hazard it prevents, so a caller who \
         reaches for it learns why at the call site: {text}"
    );

    // A refusal that only ever fires once is not a rule. Drive it on a
    // boundary that owned nothing, on one with no parent, and on the oldest
    // shape the store can hold: the answer does not vary with the input.
    for keys in [&[][..], &["a"][..], &["a", "b", "c"][..]] {
        let mut b = replay_with_keys(keys);
        b.parent_checkpoint_id = None;
        assert!(
            b.install().is_err(),
            "install refuses whatever the boundary looks like"
        );
    }
}

#[tokio::test]
async fn the_read_path_adds_nothing_to_the_log() {
    // The structural claim, checked the only way it can be: run a real replay
    // against a real store, then prove the store holds exactly the boundaries
    // that were appended and not one more. The read side hands out a value
    // with no store, no pool, and no `append` on it, so there is no path from
    // a reader back into the log — this is the evidence for that, rather than
    // a restatement of the type's shape.
    let (s, _p) = store("read-is-read-only").await;
    s.append(boundary(1, None)).await.expect("append 1");
    s.append(boundary(2, Some(r#"["prompt:pane-1"]"#)))
        .await
        .expect("append 2");

    let before = s.history(T, None, 64).await.expect("history before");
    let replayed = s.replay(T, 2).await.expect("replay");
    let plan = plan_fork(
        &replayed,
        &ForkIntent::Perform(AcknowledgeDispatchedEffects::accept()),
    );
    assert!(plan.would_perform && !plan.installable);
    let after = s.history(T, None, 64).await.expect("history after");

    assert_eq!(
        before, after,
        "replay and fork planning leave the log byte-identical: a read that \
         could write would make the log's authority depend on who asked"
    );
    assert_eq!(after.len(), 2, "and it holds only what was appended");
}
