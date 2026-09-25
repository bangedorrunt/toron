// governed-by: ADR-0035
//! Run-state checkpoint conformance (flywheel B2/B3, bd-4dpq).
//!
//! Spec: `docs/notes/2026-09-26-run-state-checkpoint-design.md` §9.
//!
//! EFFECT RULES (binding), ADR-0035: substitution only. Temp-file sqlite on the
//! PRODUCTION code path (`schema::connect` + `schema::migrate`, the same pair
//! `mail_shim` uses — stricter than in-memory). No network. No wall clock: the
//! store takes `created_at_us` as a parameter precisely so a test can pin it,
//! and every case below passes explicit values.
//!
//! What these pin, and why each is a real contract rather than a restatement
//! of the code:
//!
//! 1. round trip — append then get returns the same boundary
//! 2. idempotent append — a durable replay re-delivers a boundary
//! 3. divergent rewrite is REFUSED — the log is authoritative, so write order
//!    must not decide which version of a boundary survives
//! 4. `latest` is the newest by `seq` and is CLOCK-INDEPENDENT — a backwards
//!    clock step must not hand out an older boundary
//! 5. history is newest-first and honours `before` / `limit`
//! 6. get-by-id is an equality lookup that resolves the exact row
//! 7. prune honours the protected floor and deletes strictly below it
//! 8. resume falls back to the boundary at-or-before the request
//! 9. a corrupt model payload fails closed rather than reading as empty state

use std::path::PathBuf;

use toron_core::index::run_state::{NewRunStateCheckpoint, RunStateStore, parse_model};
use toron_core::index::schema;

const T: &str = "thread-conformance";

async fn store(tag: &str) -> (RunStateStore, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "toron-run-state-conf-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(path.to_str().expect("utf-8 path"))
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    (RunStateStore::new(pool), path)
}

fn boundary(seq: i64, created_at: i64, model: &str) -> NewRunStateCheckpoint {
    NewRunStateCheckpoint {
        thread_id: T.to_string(),
        seq,
        parent_checkpoint_id: None,
        model_json: model.to_string(),
        msg_kind: Some("WatchNdjson".to_string()),
        effect_keys: None,
        created_at,
    }
}

// --- 1. round trip -------------------------------------------------------

#[tokio::test]
async fn append_then_get_returns_the_same_boundary() {
    let (s, _p) = store("roundtrip").await;
    let (_, created) = s
        .append(boundary(1, 1_000, r#"{"seq":1}"#))
        .await
        .expect("append");
    assert!(created, "first append creates");

    let got = s.get(T, 1).await.expect("get").expect("row exists");
    assert_eq!(got.seq, 1);
    assert_eq!(got.model_json, r#"{"seq":1}"#);
    assert_eq!(got.msg_kind.as_deref(), Some("WatchNdjson"));
    assert_eq!(
        parse_model(&got).expect("payload parses")["seq"],
        1,
        "the stored model round-trips as the value that was appended"
    );
}

// --- 2. idempotent append ------------------------------------------------

#[tokio::test]
async fn append_is_idempotent_for_a_replayed_boundary() {
    let (s, _p) = store("idempotent").await;
    let (first, created_first) = s
        .append(boundary(7, 5_000, r#"{"n":7}"#))
        .await
        .expect("append");
    let (second, created_second) = s
        .append(boundary(7, 5_000, r#"{"n":7}"#))
        .await
        .expect("replay");

    assert!(created_first, "first append creates");
    assert!(
        !created_second,
        "a durable replay must not create a second row"
    );
    assert_eq!(first, second, "the replay attaches to the same row");
}

// --- 3. divergent rewrite refused ---------------------------------------

#[tokio::test]
async fn divergent_rewrite_of_one_boundary_is_refused_and_keeps_the_original() {
    let (s, _p) = store("diverge").await;
    s.append(boundary(3, 9_000, r#"{"v":"first"}"#))
        .await
        .expect("append");

    let err = s
        .append(boundary(3, 9_000, r#"{"v":"second"}"#))
        .await
        .expect_err("a divergent rewrite must be refused");
    assert!(
        err.to_string().contains("contract violation"),
        "the refusal must name the real cause: {err}"
    );

    let kept = s.get(T, 3).await.expect("get").expect("row survives");
    assert_eq!(
        kept.model_json, r#"{"v":"first"}"#,
        "a refused rewrite must leave the original boundary byte-identical, not a partial row"
    );
}

// --- 4. latest is clock-independent --------------------------------------

#[tokio::test]
async fn latest_is_the_newest_seq_even_when_the_clock_steps_backwards() {
    let (s, _p) = store("clock").await;
    s.append(boundary(1, 10_000, r#"{"n":1}"#))
        .await
        .expect("append 1");
    // A backwards clock step: created_at goes DOWN while seq goes UP.
    s.append(boundary(2, 5_000, r#"{"n":2}"#))
        .await
        .expect("append 2");

    let latest = s
        .latest(T)
        .await
        .expect("latest")
        .expect("a boundary exists");
    assert_eq!(
        latest.seq, 2,
        "ordering must follow the fold's counter, not the clock: a backwards \
         clock step must not make latest() hand back boundary 1"
    );
}

// --- 5. history ordering and bounds --------------------------------------

#[tokio::test]
async fn history_is_newest_first_and_honours_before_and_limit() {
    let (s, _p) = store("history").await;
    for seq in 1..=5 {
        s.append(boundary(seq, seq * 1_000, &format!(r#"{{"n":{seq}}}"#)))
            .await
            .expect("append");
    }

    let all = s.history(T, None, 0).await.expect("history");
    assert_eq!(
        all.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![5, 4, 3, 2, 1],
        "history is newest-first"
    );

    let bounded = s.history(T, Some(3), 0).await.expect("history before");
    assert_eq!(
        bounded.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![2, 1],
        "`before` is exclusive: a walk resumes without re-reading its cursor"
    );

    let limited = s.history(T, None, 2).await.expect("history limited");
    assert_eq!(
        limited.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![5, 4],
        "limit bounds the read"
    );
}

// --- 6. get by durable id ------------------------------------------------

#[tokio::test]
async fn get_by_checkpoint_id_resolves_the_exact_row() {
    let (s, _p) = store("byid").await;
    let (row, _) = s
        .append(boundary(11, 77_000, r#"{"n":11}"#))
        .await
        .expect("append");

    let found = s
        .get_by_checkpoint_id(&row.checkpoint_id)
        .await
        .expect("get by id")
        .expect("row resolves by its evidence id");
    assert_eq!(found, row, "an id lookup must return the identical row");
    assert_eq!(found.seq, 11);

    assert!(
        s.get_by_checkpoint_id("0000000000000000-000000000000-0000000000000000")
            .await
            .expect("unknown id is a miss, not an error")
            .is_none(),
        "an unknown id is None, never a wrong row"
    );
}

// --- 7. prune honours the protected floor --------------------------------

#[tokio::test]
async fn prune_keeps_the_protected_boundary_and_deletes_strictly_below_it() {
    let (s, _p) = store("prune").await;
    for seq in 1..=5 {
        s.append(boundary(seq, seq * 1_000, &format!(r#"{{"n":{seq}}}"#)))
            .await
            .expect("append");
    }

    let counts = s.prune_before(T, 4).await.expect("prune");
    assert_eq!(
        counts.checkpoints, 3,
        "seq 1..=3 are strictly below the floor"
    );

    let left = s.history(T, None, 0).await.expect("history");
    assert_eq!(
        left.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![5, 4],
        "the protected floor and everything above it survive"
    );
    assert!(
        s.get(T, 4).await.expect("get").is_some(),
        "prune must never drop the boundary a live resume is standing on"
    );
}

// --- 8. resume falls back at-or-before -----------------------------------

#[tokio::test]
async fn resume_uses_the_exact_boundary_then_falls_back_at_or_before() {
    let (s, _p) = store("resume").await;
    for seq in 1..=3 {
        s.append(boundary(seq, seq * 1_000, &format!(r#"{{"n":{seq}}}"#)))
            .await
            .expect("append");
    }

    let exact = s
        .load_for_resume(T, 2)
        .await
        .expect("resume")
        .expect("a boundary exists");
    assert_eq!(exact.seq, 2, "an exact hit is used as-is");

    let fallback = s
        .load_for_resume(T, 99)
        .await
        .expect("resume")
        .expect("falls back rather than reporting nothing");
    assert_eq!(
        fallback.seq, 3,
        "a request past the end resumes from the newest boundary at or before it, \
         so the caller learns the true state it got instead of seeing a gap"
    );

    assert!(
        s.load_for_resume("thread-never-written", 1)
            .await
            .expect("unknown thread is a miss, not an error")
            .is_none(),
        "an unwritten thread has no resume seed"
    );
}

// --- 9. a corrupt payload fails closed -----------------------------------

#[tokio::test]
async fn corrupt_model_payload_fails_closed_instead_of_reading_as_empty_state() {
    let (s, _p) = store("corrupt").await;
    s.append(boundary(1, 1_000, "{not json"))
        .await
        .expect("append");

    let row = s.get(T, 1).await.expect("get").expect("row exists");
    let err = parse_model(&row).expect_err("a corrupt payload must not parse");
    // Assert on the diagnostic carrying a position rather than serde's exact
    // wording: the load-bearing property is that it fails WITH a located parse
    // error instead of degrading to empty state.
    assert!(
        err.to_string().contains("line 1"),
        "the parse error is located, so an operator can see which boundary is \
         unreadable: {err}"
    );
    // The load-bearing part: the row is still there, so a caller can see the
    // boundary EXISTS and is unreadable, rather than resuming from nothing.
    assert!(
        row.seq == 1,
        "the boundary is visible even though its payload is not"
    );
}
