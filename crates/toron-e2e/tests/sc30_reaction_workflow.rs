// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-30 — Reaction triggers a workflow once (ADR-0020 §6.2).
//!
//! YAML: `trigger: reaction_added`, action `send_message` to operator (a test
//! sink). B `toron react --to <post> --emoji :+1:`. Assert: the workflow emits
//! exactly ONE stamped action, and a second identical react does not
//! double-fire (idempotent trigger filter).
//!
//! Mechanism: `toron react` (CLI subprocess) publishes a kind-7 reaction to
//! the shared bus store. The test hosts the reaction workflow over the SAME
//! shared store (poll/replay — the daemon trigger loop's equivalent for a
//! cross-process e2e), matching kind-7 reactions against `reaction_added`
//! and firing the engine exactly once per unique (target, emoji, author).
//! The runner stamps every emitted event (ADR-0007 F6) and records the fired
//! action; the assert is the engine fired exactly once across two identical
//! CLI reacts.
//!
//! RED contract: `toron react` / `toron channel` do not exist until the
//! wave-1 CLI lands, so this fails at the first command with exit 2.
//!
//! Un-gated (ADR-0020 §6.2).

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::Value;
use sqlx::SqlitePool;
use toron_core::index::schema;
use toron_core::kernel::OurEngine;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;
use toron_workflow::engine::durable::RunEffect;
use toron_workflow::engine::durable_host::DurableRun;
use toron_workflow::schema::{Workflow, parse_workflow};
use toron_workflow::stamp_workflow_emitted;

const PROJECT: &str = "crew-demo";
const CHANNEL: &str = "steer-sc30";
const A: &str = "flywheel-toron-coder";
const B: &str = "flywheel-toron-coder2";
const OPERATOR: &str = "operator";

mod common;

fn toron_bin() -> PathBuf {
    common::toron_bin()
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    toron_core::testkit::toron_command(toron_bin())
        .args(args)
        .arg("--json")
        .env("TORON_MCP_DATA_DIR", data_dir)
        // An ambient TORON_MCP_URL outranks the data-dir pin
        // (`mailbox_source::decide_source` checks the daemon URL first),
        // so the CLI would hit a live daemon instead of this scenario's
        // store and reject the locally registered recipient (bd-5z0l).
        .env_remove("TORON_MCP_URL")
        .output()
        .expect("spawn toron")
}

fn exit_code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

fn stderr_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

async fn setup(data_dir: &Path, project: &str, names: &[&str]) -> SqlitePool {
    std::fs::create_dir_all(data_dir).expect("create data dir");
    let db = data_dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = toron_core::bus::in_process::InProcessBus::open(
        pool.clone(),
        toron_core::bus::in_process::InProcessBusConfig::default(),
    )
    .await
    .expect("bus");
    let mailbox =
        Mailbox::new(Arc::new(bus), data_dir.to_path_buf(), None).with_index_pool(pool.clone());
    for name in names {
        mailbox
            .register_agent(project, &AgentProfile::new(*name))
            .await
            .expect("register agent");
    }
    pool
}

/// Drive one reaction workflow to terminal on the durable host (bd-i84w).
async fn drive_reaction_workflow(
    pool: &SqlitePool,
    workflow: &Workflow,
    trigger: serde_json::Value,
    seams: &toron_workflow::engine::leaf::LeafSeams,
) {
    let run_id = format!("sc30-{}", uuid::Uuid::new_v4());
    let digest = common::durable::digest(workflow);
    let (engine_store, journal, state) = common::durable::handles(pool);
    let (wf, trig_for_open) = (workflow.clone(), trigger.clone());
    let (mut run, mut effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableRun::open(
            engine,
            state,
            run_id,
            wf,
            trig_for_open,
            common::durable::now_ms(),
        )
    })
    .await
    .expect("open task")
    .expect("open reaction run");
    loop {
        if effects.is_empty() {
            panic!("reaction drive ran out of effects while non-terminal");
        }
        let mut follow = Vec::new();
        let mut done = false;
        for effect in std::mem::take(&mut effects) {
            match effect {
                RunEffect::ExecuteStep { index, step } => {
                    let mut outputs: serde_json::Map<String, serde_json::Value> =
                        run.model().outputs.clone().into_iter().collect();
                    let value = common::durable::run_step(
                        "sc30-react",
                        &workflow.name,
                        &digest,
                        &step,
                        index,
                        &trigger,
                        &mut outputs,
                        seams,
                    )
                    .await;
                    let (back, mut eff) = tokio::task::spawn_blocking(move || {
                        run.step_done(value, common::durable::now_ms())
                            .map(|eff| (run, eff))
                    })
                    .await
                    .expect("step feed task")
                    .unwrap_or_else(|e| panic!("step feed failed: {e}"));
                    run = back;
                    follow.append(&mut eff);
                }
                RunEffect::Complete { .. } => {
                    done = true;
                    break;
                }
                other => panic!("unexpected reaction effect: {other:?}"),
            }
        }
        if done {
            break;
        }
        effects = follow;
    }
}

/// Poll the shared bus store for kind-7 reactions matching the trigger, and
/// run the workflow once per UNIQUE (target, emoji, author) — the idempotent
/// trigger filter. Returns how many workflow actions fired.
async fn drain_reactions(
    pool: &SqlitePool,
    workflow: &Workflow,
    fired: &AtomicUsize,
    seen: &mut std::collections::HashSet<String>,
) {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_id, raw_json FROM bus_events WHERE kind = 7 ORDER BY created_at, event_id",
    )
    .fetch_all(pool)
    .await
    .expect("reactions");
    for (id, raw) in rows {
        if !seen.insert(id.clone()) {
            continue;
        }
        let Ok(ev) = toron_core::nostr::Event::from_json(&raw) else {
            continue;
        };
        // reaction_added: kind 7, e-tag = target, content = emoji, h = channel.
        let target = ev
            .tags
            .iter()
            .find(|t| t.as_slice().first().map(String::as_str) == Some("e"))
            .and_then(|t| t.as_slice().get(1).cloned())
            .unwrap_or_default();
        let emoji = ev.content.clone();
        let author = ev.pubkey.to_hex();
        let key = format!("{target}|{emoji}|{author}");
        if !seen.insert(format!("key|{key}")) {
            continue; // identical react → idempotent filter, no double-fire
        }
        // Fire the reaction workflow on the durable host (stamped async
        // runner, temp-pool stores): open → one step → terminal. No server.
        let seams = common::durable::seams(Arc::new(
            |inv: &toron_workflow::engine::ActionInvocation| {
                let _ = stamp_workflow_emitted(vec![]);
                let _ = &inv.action;
                Box::pin(async move { Ok(serde_json::json!({ "ok": true, "stamped": true })) })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = anyhow::Result<serde_json::Value>>
                                + Send,
                        >,
                    >
            },
        ));
        let payload = serde_json::json!({
            "kind": 7,
            "id": id,
            "pubkey": author,
            "content": emoji,
        });
        drive_reaction_workflow(pool, workflow, payload, &seams).await;
        fired.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn sc30_reaction_fires_workflow_once() {
    let dir = std::env::temp_dir().join(format!("toron-sc30-{}", uuid::Uuid::new_v4()));
    let pool = setup(&dir, PROJECT, &[A, B, OPERATOR]).await;

    // Channel + post (kind 9) — the reaction target.
    let out = run(
        &dir,
        &[
            "channel",
            "create",
            CHANNEL,
            "--name",
            "bead sc30",
            "--private",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(exit_code(&out), 0, "channel create: {}", stderr_text(&out));
    let out = run(
        &dir,
        &["channel", "join", CHANNEL, "--project", PROJECT, "--as", B],
    );
    assert_eq!(exit_code(&out), 0, "channel join: {}", stderr_text(&out));
    let out = run(
        &dir,
        &[
            "channel",
            "post",
            CHANNEL,
            "--body",
            "react target",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(exit_code(&out), 0, "channel post: {}", stderr_text(&out));
    let post: Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("post JSON: {e}: {}", String::from_utf8_lossy(&out.stdout)));
    let post_id = post["event_id"]
        .as_str()
        .expect("post event_id")
        .to_string();

    // Reaction workflow: reaction_added → stamped send_message to operator.
    let wf_yaml = format!(
        "name: sc30-react\n\
         trigger:\n  type: reaction_added\n  channel: {CHANNEL}\n  emoji: \":+1:\"\n\
         steps:\n  - name: notify\n    action: send_message\n    to: [\"{OPERATOR}\"]\n    subject: react-fired\n    body: exactly-once\n    project: \"{PROJECT}\"\n"
    );
    let workflow: Workflow = parse_workflow(&wf_yaml).expect("parse workflow YAML");

    let fired = Arc::new(AtomicUsize::new(0));
    let mut seen = std::collections::HashSet::new();

    // First react → exactly one fired action.
    let out = run(
        &dir,
        &[
            "react",
            "--to",
            &post_id,
            "--emoji",
            ":+1:",
            "--project",
            PROJECT,
            "--as",
            B,
            "--channel",
            CHANNEL,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "react must exit 0: {}",
        stderr_text(&out)
    );
    // Give the CLI subprocess time to persist the kind-7 to the shared store.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while fired.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        drain_reactions(&pool, &workflow, &fired, &mut seen).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    drain_reactions(&pool, &workflow, &fired, &mut seen).await;
    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "exactly one stamped action after first react"
    );

    // Second IDENTICAL react → no double-fire.
    let out = run(
        &dir,
        &[
            "react",
            "--to",
            &post_id,
            "--emoji",
            ":+1:",
            "--project",
            PROJECT,
            "--as",
            B,
            "--channel",
            CHANNEL,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "second react must exit 0: {}",
        stderr_text(&out)
    );
    tokio::time::sleep(Duration::from_millis(1500)).await;
    drain_reactions(&pool, &workflow, &fired, &mut seen).await;
    assert_eq!(
        fired.load(Ordering::SeqCst),
        1,
        "second identical react must not double-fire (idempotent trigger filter)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
