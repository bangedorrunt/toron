// governed-by: ADR-0035, ADR-0003
//! `toron run-state replay|fork` end-to-end (bd-7n8e).
//!
//! Shells out to the built binary against a real temp index, so this covers
//! the part a core test cannot: that the verb is reachable, that `--thread`
//! and `--seq` are REQUIRED rather than defaulted, and that the thing it
//! prints says what the core says it says.
//!
//! Hermetic: `TORON_DATA_DIR` is pinned per-command to a temp root, and
//! `TORON_MCP_URL` is removed so an ambient daemon cannot be reached instead.

use std::path::PathBuf;
use std::process::Command;

use serde_json::Value;
use toron_core::index::run_state::{NewRunStateCheckpoint, RunStateStore};
use toron_core::index::schema;

const T: &str = "cli-run-state";

/// A data root with a migrated index holding two boundaries on one thread.
async fn seeded_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "toron-cli-run-state-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&root).expect("data root");
    let pool = schema::connect(root.join("index.sqlite").to_str().expect("utf-8"))
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let store = RunStateStore::new(pool);
    store
        .append(NewRunStateCheckpoint {
            thread_id: T.to_string(),
            seq: 1,
            parent_checkpoint_id: None,
            model_json: r#"{"step":1}"#.to_string(),
            msg_kind: Some("WatchNdjson".to_string()),
            effect_keys: None,
            created_at: 1_000,
        })
        .await
        .expect("append 1");
    store
        .append(NewRunStateCheckpoint {
            thread_id: T.to_string(),
            seq: 2,
            parent_checkpoint_id: None,
            model_json: r#"{"step":2}"#.to_string(),
            msg_kind: Some("WatchNdjson".to_string()),
            effect_keys: Some(r#"["prompt:pane-1","fetch-inbox:toron"]"#.to_string()),
            created_at: 2_000,
        })
        .await
        .expect("append 2");
    root
}

fn toron(root: &PathBuf) -> Command {
    let mut c = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    c.env_remove("TORON_MCP_URL").env("TORON_DATA_DIR", root);
    c
}

fn run(root: &PathBuf, args: &[&str]) -> (bool, String, String) {
    let out = toron(root).args(args).output().expect("toron runs");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[tokio::test]
async fn replay_prints_the_model_and_the_pending_write_set() {
    let root = seeded_root().await;
    let (ok, stdout, stderr) = run(
        &root,
        &["run-state", "replay", "--thread", T, "--seq", "2", "--json"],
    );
    assert!(ok, "replay exits 0: {stderr}");

    let v: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    assert_eq!(v["thread"], T);
    assert_eq!(v["seq"], 2);
    assert_eq!(
        v["model"]["step"], 2,
        "the stored model, not a re-derivation"
    );
    // The load-bearing half: a reader cannot decide whether re-entering this
    // boundary repeats a dispatch unless the verbs prints which effects it
    // owned.
    assert_eq!(
        v["effect_keys"],
        serde_json::json!(["prompt:pane-1", "fetch-inbox:toron"])
    );

    // A boundary that owned nothing reports an empty set rather than a
    // missing key, so a caller does not have to distinguish the two.
    let (ok2, stdout2, _) = run(
        &root,
        &["run-state", "replay", "--thread", T, "--seq", "1", "--json"],
    );
    assert!(ok2, "replay of boundary 1 exits 0");
    let v2: Value = serde_json::from_str(&stdout2).expect("stdout is JSON");
    assert_eq!(v2["effect_keys"], serde_json::json!([]));
}

#[tokio::test]
async fn fork_is_dry_and_refuses_to_install() {
    let root = seeded_root().await;
    let (ok, stdout, stderr) = run(
        &root,
        &["run-state", "fork", "--thread", T, "--seq", "2", "--json"],
    );
    assert!(ok, "fork exits 0: {stderr}");

    let v: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    assert_eq!(
        v["would_perform"], false,
        "a plain `fork` dispatches nothing, and there is no flag that could \
         make it: this build has no installer, so an opt-in flag would lie"
    );
    assert_eq!(
        v["installable"], false,
        "and nothing seeds a live model from the log (spec S3)"
    );
    assert_eq!(
        v["inherited_effect_keys"],
        serde_json::json!(["prompt:pane-1", "fetch-inbox:toron"])
    );

    let refusals = v["refusals"].as_array().expect("refusals array");
    let first = refusals[0].as_str().expect("refusal text");
    assert!(
        first.contains("prompt:pane-1") && first.contains("already reached a provider"),
        "the blocking refusal leads and names the hazard: {first}"
    );
    assert!(
        refusals
            .iter()
            .any(|r| r.as_str().is_some_and(|s| s.contains("no installer"))),
        "and the plan states it cannot install, as data the operator reads"
    );
}

#[tokio::test]
async fn a_missing_boundary_is_a_named_failure_not_a_nearby_row() {
    let root = seeded_root().await;
    // Exit 1 (runtime failure, not 2 — the usage was legal, the boundary is
    // not there), and the message names the boundary asked for.
    let (ok, _, stderr) = run(
        &root,
        &["run-state", "replay", "--thread", T, "--seq", "9", "--json"],
    );
    assert!(!ok, "a missing boundary is a failure, not a silent success");
    assert!(
        stderr.contains(&format!("{T}/9")),
        "the error names the boundary, because the whole refusal is 'this is \
         not the step you asked for': {stderr}"
    );
}

#[tokio::test]
async fn thread_and_seq_are_required_rather_than_defaulted() {
    let root = seeded_root().await;
    // clap owns exit 2 for usage errors. A default would let a typo replay a
    // boundary nobody asked for, which is the confusion the exact read exists
    // to prevent.
    for args in [
        vec!["run-state", "replay", "--seq", "1"],
        vec!["run-state", "replay", "--thread", T],
        vec!["run-state", "fork", "--seq", "1"],
        vec!["run-state", "fork", "--thread", T],
    ] {
        let (ok, _, stderr) = run(&root, &args);
        assert!(!ok, "`toron {}` must refuse", args.join(" "));
        assert!(
            stderr.contains("--thread") || stderr.contains("--seq"),
            "the usage error names the missing required flag for `toron {}`",
            args.join(" ")
        );
    }
}

#[tokio::test]
async fn an_absent_index_is_an_answer_not_a_panic() {
    // A run with no run-state recording has no index at all. That is a real
    // state (the recorder is opt-in), so the verb has to say so rather than
    // crash on a path that does not exist.
    let empty = std::env::temp_dir().join(format!(
        "toron-cli-run-state-empty-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&empty).expect("empty root");
    let (ok, _, stderr) = run(
        &empty,
        &["run-state", "replay", "--thread", T, "--seq", "1"],
    );
    assert!(!ok, "no index is a failure");
    assert!(
        stderr.contains("no run-state index"),
        "and it says which precondition is missing, so the operator knows to \
         turn recording on rather than to debug SQL: {stderr}"
    );
}
