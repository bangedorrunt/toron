// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! bd-6setu.6 (V10 at the CLAIM) — a review job refuses its author's claim,
//! end to end through the real CLI.
//!
//! Plane-S jobs are claimed by the agent in its own pane, so flywheel's
//! dispatch-side binding cannot see them. The guard lives in `toron job claim`:
//! a request whose spec carries `exclude_claimants_of` (the review step's
//! stage-contract reads) refuses a claimant that already claimed one of those
//! nodes in the SAME run, BEFORE publishing — the job must stay open for a
//! peer. This drives that path with the shipped binary over one bus:
//!
//!   1. the author claims the `execute` node,
//!   2. the author's claim on the `review` node exits 1 with the typed
//!      `v10-author-conflict` token and publishes nothing,
//!   3. another pin claims the same job and wins (proof nothing was published).
//!
//! Ungated: it needs no daemon, only a temp data dir and the CLI.

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use nostr::prelude::{Keys, Tag, Timestamp};
use serde_json::Value;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;
use toron_workflow::jobs::{JobRequest, build_job_request_with_tags};

mod common;

const PROJECT: &str = "crew-v10";
const AUTHOR: &str = "flywheel-toron-v10-author";
const REVIEWER: &str = "flywheel-toron-v10-reviewer";
const RUN: &str = "run-v10";

fn toron_bin() -> PathBuf {
    common::toron_bin()
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    toron_core::testkit::toron_command(toron_bin())
        .args(args)
        .arg("--json")
        .env("TORON_MCP_DATA_DIR", data_dir)
        .env_remove("TORON_MCP_URL")
        .output()
        .expect("spawn toron")
}

fn exit_code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

fn stdout_json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout is not JSON (exit {}): {} | stderr: {}",
            exit_code(out),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn claim(data_dir: &Path, job_id: &str, who: &str) -> Output {
    run(
        data_dir,
        &["job", "claim", job_id, "--project", PROJECT, "--as", who],
    )
}

/// Register the two pins and publish the two tagged requests of ONE run.
async fn setup(data_dir: &Path) -> (String, String) {
    std::fs::create_dir_all(data_dir).expect("create data dir");
    let db = data_dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let operator = Keys::generate();
    let mailbox = Mailbox::new(
        Arc::new(bus),
        data_dir.to_path_buf(),
        Some(operator.clone()),
    )
    .with_index_pool(pool);
    for name in [AUTHOR, REVIEWER] {
        mailbox
            .register_agent(PROJECT, &AgentProfile::new(name))
            .await
            .expect("register agent");
    }

    let tag = |k: &str, v: &str| Tag::parse([k, v]).expect("tag parses");
    let execute = build_job_request_with_tags(
        &operator,
        None,
        &JobRequest::new("execute node", serde_json::json!({"task": "do the work"})),
        Timestamp::now(),
        vec![tag("run", RUN), tag("node", "execute")],
    )
    .expect("execute request");
    let review = build_job_request_with_tags(
        &operator,
        None,
        &JobRequest::new(
            "review node",
            serde_json::json!({
                "task": "review the work",
                "exclude_claimants_of": ["execute"],
            }),
        ),
        Timestamp::now(),
        vec![tag("run", RUN), tag("node", "review")],
    )
    .expect("review request");
    mailbox
        .bus()
        .publish(execute.clone())
        .await
        .expect("publish");
    mailbox
        .bus()
        .publish(review.clone())
        .await
        .expect("publish");
    (execute.id.to_hex(), review.id.to_hex())
}

#[tokio::test]
async fn a_review_claim_refuses_its_author_end_to_end() {
    let dir = std::env::temp_dir().join(format!("toron-v10-claim-{}", uuid::Uuid::new_v4()));
    let (execute_id, review_id) = setup(&dir).await;

    // 1. The author takes the execute node — a normal, winning claim.
    let out = claim(&dir, &execute_id, AUTHOR);
    assert_eq!(
        exit_code(&out),
        0,
        "the author claims its own node: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(stdout_json(&out)["won"], Value::Bool(true));

    // 2. The author cannot claim the review job of the same run: typed refusal,
    //    exit 1, and NOTHING published.
    let out = claim(&dir, &review_id, AUTHOR);
    assert_eq!(
        exit_code(&out),
        1,
        "an author's review claim is refused: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("v10-author-conflict"),
        "the refusal carries its stable token: {stderr}"
    );
    assert!(
        stderr.contains("execute") && stderr.contains(RUN),
        "the refusal names the node and the run: {stderr}"
    );

    // 3. Another pin takes the same job. This only succeeds if the refused
    //    claim published nothing — a published claim would have won the race.
    let out = claim(&dir, &review_id, REVIEWER);
    assert_eq!(
        exit_code(&out),
        0,
        "a different pin is a legitimate reviewer: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stdout_json(&out)["won"],
        Value::Bool(true),
        "the refused claim left the job open for a peer"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
