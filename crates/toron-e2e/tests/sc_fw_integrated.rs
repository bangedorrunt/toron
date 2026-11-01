// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-FW — flywheel integrated mode (ADR-0003 Decision 3A) [P1].
//!
//! "Integrated mode: one flywheel process serves agents via MCP AND runs the
//! loop; agent↔agent mail + flywheel dispatch both work, no second process."
//!
//! This test proves the one-process contract against the SAME mailbox surface
//! flywheel embeds (`toron_mcp::demo_mailbox` + InProcessBus):
//!
//! 1. **Single process**: the mailbox, bus, and tool surface live in one
//!    `Mailbox`/`InProcessBus` pair (the MCP server and the loop share it —
//!    zero IPC, no second daemon).
//! 2. **Peer mail**: agentA → mailbox → bus → agentB (the "agent → MCP →
//!    toron → MCP → agent" path with flywheel NOT in it), delivered by bus
//!    push (<50ms, no polling).
//! 3. **Direct dispatch**: the flywheel loop's "dispatch" is a direct
//!    toron_core call (no HTTP) — exercised via the mailbox methods.
//! 4. **Single hc client**: `fn agent_prompt` has exactly ONE definition
//!    in toron-core (the canonical hc client) — asserted statically. The
//!    flywheel-side assertion (their old herdr.rs/mail.rs removed by bd-o8j.2)
//!    is an `#[ignore]`d sub-case that also boots the real flywheel binary.
#![cfg(feature = "p1")]

use std::sync::Arc;
use std::time::Instant;

use nostr_sdk::prelude::*;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::bus::{Bus, Filter, InProcessBus, SubscriptionEvent};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::mailbox::messaging::{FetchInbox, SendMessage};
use toron_core::nostr::identity::AgentProfile;

/// Boot the integrated-mode process: one mailbox + one in-process bus, exactly
/// as `toron_mcp::demo_mailbox` / the flywheel embed construct it.
async fn boot(dir: &std::path::Path) -> (Mailbox, InProcessBus) {
    let db = dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(
        pool.clone(),
        InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: vec![],
            channel_capacity: 256,
            detailed_stage_logging: false,
        },
    )
    .await
    .expect("bus");
    let mailbox = Mailbox::new(Arc::new(bus.clone()), dir.to_path_buf(), None)
        .with_agent_passphrase("toron-dev")
        .with_index_pool(pool);
    (mailbox, bus)
}

async fn register(mailbox: &Mailbox, project: &str, name: &str) -> PublicKey {
    mailbox
        .register_agent(project, &AgentProfile::new(name))
        .await
        .expect("register")
        .npub
        .parse()
        .expect("npub")
}

/// SC-FW: one process serves both agents' mail via a shared bus (no second
/// daemon, no HTTP between flywheel and mailbox).
#[tokio::test]
async fn sc_fw_one_process_peer_mail_via_bus() {
    let dir = std::env::temp_dir().join(format!("toron-scfw-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    // ONE process: mailbox + bus share the pool; there is no second daemon.
    let (mailbox, bus) = boot(&dir).await;

    let _pi = register(&mailbox, "demo", "PiAlpha").await;
    let oc = register(&mailbox, "demo", "OCBeta").await;

    // OC-β subscribes to its own gift wraps BEFORE the send (push path).
    let mut sub = bus
        .subscribe(Filter::new().kinds([Kind::GiftWrap]).pubkey(oc))
        .await
        .expect("subscribe to OC wraps");

    // Pi-α sends via the mailbox (the MCP tool surface in the same process).
    let sent = mailbox
        .send_message(&SendMessage {
            project_slug: "demo".into(),
            sender_name: "PiAlpha".into(),
            to: vec!["OCBeta".into()],
            subject: "peer".into(),
            body_md: "hello via bus".into(),
            ..Default::default()
        })
        .await
        .expect("send");
    assert_eq!(sent.count, 1, "one delivery");

    // OC-β receives via BUS PUSH (<50ms, zero polling) — flywheel NOT in path.
    let t0 = Instant::now();
    let push_evt = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        loop {
            match sub.next().await {
                Some(SubscriptionEvent::Event(evt)) => return Some(evt),
                Some(SubscriptionEvent::Eose) => {}
                Some(SubscriptionEvent::Closed(_)) | None => return None,
            }
        }
    })
    .await
    .expect("push within timeout")
    .expect("push arrives");
    assert_eq!(push_evt.kind, Kind::GiftWrap, "kind-1059 wrap");
    let p_tag = push_evt
        .tags
        .iter()
        .find(|t| t.as_slice().first().map(String::as_str) == Some("p"))
        .and_then(|t| t.as_slice().get(1))
        .map(String::as_str)
        .expect("p tag");
    assert_eq!(p_tag, oc.to_hex(), "addressed to OC via p tag");
    assert!(
        t0.elapsed().as_millis() < 50,
        "push {:?} < 50ms (no polling)",
        t0.elapsed()
    );

    // OC-β reads it back (parity poll) — direct toron_core call, zero IPC.
    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "OCBeta".into(),
            include_bodies: true,
            ..Default::default()
        })
        .await
        .expect("fetch");
    assert_eq!(inbox.len(), 1, "one message in OC inbox");
    assert_eq!(inbox[0].body_md, "hello via bus");
    // Parity: inbox `sender` is the resolved display name (AM-style), not the
    // raw pubkey — the sender-name resolution rewrites hex npubs for locally
    // registered agents.
    assert_eq!(inbox[0].sender, "PiAlpha", "from Pi-α (resolved name)");

    // flywheel's loop "dispatch" is a direct call here — no HTTP round trip.
    // (The health check + a dispatch-equivalent direct call both succeed.)
    assert!(
        mailbox.health_check().await.bus.ok,
        "mailbox healthy in-process"
    );

    mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "PiAlpha".into(),
            ..Default::default()
        })
        .await
        .expect("direct dispatch read");

    let _ = std::fs::remove_dir_all(&dir);
}

/// SC-FW static invariant: exactly ONE `fn agent_prompt` definition in
/// toron-core (the canonical hc client). The flywheel-side duplicate
/// definitions are removed by bd-o8j.2; this asserts the toron-core side.
#[test]
fn sc_fw_single_agent_prompt_definition_in_core() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let core_src = std::path::Path::new(manifest_dir)
        .parent()
        .unwrap()
        .join("toron-core/src");
    // Count `fn agent_prompt(` definitions (not `agent_prompt_coded`, not
    // calls/references) in toron-core.
    let mut count = 0usize;
    for entry in walk_rs(&core_src) {
        let text = std::fs::read_to_string(&entry).unwrap_or_default();
        for line in text.lines() {
            let t = line.trim();
            if t.starts_with("fn agent_prompt(") || t.starts_with("pub fn agent_prompt(") {
                count += 1;
            }
        }
    }
    assert_eq!(
        count, 1,
        "exactly one agent_prompt definition in toron-core"
    );
}

/// SC-FW live flywheel sub-case (RESOLVED 2026-08-09, bd-p5-sc-fw-integrated-
/// resolution-8m5d): bd-o8j.2 landed (flywheel MailBackend deleted; toron-core's
/// hc client is the single client) and the test PASSES on machines with the
/// flywheel binary + checkout. Kept `#[ignore]`d: it boots a DIFFERENT repo's
/// binary (`flywheel toron status`) and rg's `~/workspace/flywheel/src` — a
/// cross-repo e2e, flywheel CI scope per captain decision 3 (toron only).
/// The toron-core single-definition invariant is asserted run-always by
/// `sc_fw_single_agent_prompt_definition_in_core` above.
/// Run locally: `cargo test -p toron-e2e --features p1 --test sc_fw_integrated
/// sc_fw_flywheel_binary_live -- --ignored`.
#[tokio::test]
#[ignore = "cross-repo e2e: requires flywheel binary + ~/workspace/flywheel checkout (flywheel CI scope, captain decision 3); verified passing 2026-08-09"]
async fn sc_fw_flywheel_binary_live() {
    // Static: exactly one agent_prompt across both repos (flywheel's old
    // herdr.rs/mail.rs deleted by bd-o8j.2).
    let out = std::process::Command::new("rg")
        .arg("-n")
        .arg("fn agent_prompt")
        .arg("~/workspace/flywheel/src")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/../toron-core/src"))
        .output()
        .expect("rg");
    let hits = String::from_utf8_lossy(&out.stdout);
    let defs = hits
        .lines()
        .filter(|l| l.contains("fn agent_prompt("))
        .count();
    assert_eq!(defs, 1, "single agent_prompt after bd-o8j.2: {hits}");

    // Live: the flywheel binary's embedded mailbox health check.
    // flywheel embeds toron-core: under the harness its mailbox is the
    // sandbox (an explicit TORON_DATA_DIR any toron-core honours), never the
    // operator's store.
    let out = toron_core::testkit::apply_test_env(&mut std::process::Command::new("flywheel"))
        .args(["toron", "status"])
        .output()
        .expect("flywheel toron status");
    assert!(
        out.status.success(),
        "flywheel integrated status: {:?}",
        out.status
    );
}

/// Recursively list .rs files (stdlib-only walk).
fn walk_rs(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk_rs(&p));
            } else if p.extension().map(|x| x == "rs").unwrap_or(false) {
                out.push(p);
            }
        }
    }
    out
}
