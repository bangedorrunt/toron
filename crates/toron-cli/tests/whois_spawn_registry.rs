// governed-by: ADR-0025
//! bd-whois-spawn-registry-pane-binding-zoir: `whois` divergence via the
//! flywheel spawn registry.
//!
//! Measured on elm-effect-rs 2026-10-05: pane w3S:p1 stamps `SlateHerder`
//! while flywheel's spawn registry binds `SilverPond` to the same pane. The
//! direct pane sweep has no entry for SilverPond, so its `whois` row carried
//! an empty pane pair and the divergence stayed invisible. `whois` must fall
//! back to the registry binding, attach the pane's stamped npub, and flag
//! `identity_divergent`.
//!
//! The world is substituted (`whois_with_panes`): fixture panes + registry
//! bindings instead of live herdr and the operator's state home, so the test
//! is hermetic on any machine.

use std::path::PathBuf;
use std::sync::Arc;

use toron_core::backend::{AgentInfo, PaneInfo};
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::mailbox::identity::spawn_registry_bindings;
use toron_core::nostr::identity::AgentProfile;

async fn test_mailbox(tag: &str) -> (Mailbox, PathBuf, PathBuf) {
    let db_path = std::env::temp_dir().join(format!(
        "toron-whois-registry-{tag}-{}-{}.sqlite",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(db_path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let data_dir = std::env::temp_dir().join(format!(
        "toron-whois-registry-data-{tag}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&data_dir).unwrap();
    let mailbox = Mailbox::new(
        Arc::new(bus),
        data_dir.clone(),
        Some(toron_core::keys::generate()),
    )
    .with_agent_passphrase("x")
    .with_index_pool(pool);
    (mailbox, db_path, data_dir)
}

fn profile(name: &str) -> AgentProfile {
    AgentProfile {
        name: name.into(),
        program: Some("pi".into()),
        model: None,
        persona: None,
        task_description: None,
    }
}

/// A pane fixture carrying the mail-identity stamp `agent bootstrap` writes.
fn pane_with_stamp(pane_id: &str, pin: &str, npub: &str) -> PaneInfo {
    serde_json::from_value(serde_json::json!({
        "pane_id": pane_id,
        "terminal_id": "term_whois",
        "workspace_id": "w3S",
        "tab_id": "w3S:t1",
        "focused": false,
        "agent_status": "working",
        "revision": 1,
        "tokens": {
            "toron_pin": pin,
            "toron_project": "/Users/x/workspace/elm-effect-rs",
            "toron_npub": npub,
        },
    }))
    .expect("pane fixture parses")
}

#[tokio::test]
async fn whois_divergence_via_spawn_registry() {
    let (mailbox, db_path, data_dir) = test_mailbox("whois-registry").await;
    mailbox
        .register_agent("elm-effect-rs", &profile("SilverPond"))
        .await
        .expect("register SilverPond");
    let slate = mailbox
        .register_agent("elm-effect-rs", &profile("SlateHerder"))
        .await
        .expect("register SlateHerder")
        .npub;

    let panes = vec![pane_with_stamp("w3S:p1", "SlateHerder", &slate)];
    let agents: Vec<AgentInfo> = Vec::new();
    let without_registry = mailbox
        .whois_with_panes("elm-effect-rs", "SilverPond", &panes, &agents, &[])
        .await
        .expect("whois without registry rows");
    assert!(
        without_registry.pane_npub.is_none(),
        "no registry row means no pane npub (the pre-fix shape)"
    );
    assert!(
        !without_registry.identity_divergent,
        "no registry row means nothing to flag"
    );

    let bindings = spawn_registry_bindings(
        r#"[
            {"pane_id": "w3S:p1", "name": "crew-elme1003214316c7-coder", "profile": "coder",
             "harness": "pi", "model_pin": "opencode-go/muse-spark-1.3-contributor",
             "skills_digest": "skills-v1:7a6d9a8305e4:n=5", "mail_identity": "SilverPond",
             "skills_injected": true},
            {"pane_id": "w3S:p1", "name": "SlateHerder", "mail_identity": "SlateHerder"}
        ]"#,
    );
    assert_eq!(bindings.len(), 2, "both mail_identity rows parse");
    let who = mailbox
        .whois_with_panes("elm-effect-rs", "SilverPond", &panes, &agents, &bindings)
        .await
        .expect("whois with registry rows");
    assert_eq!(who.name, "SilverPond");
    assert_eq!(
        who.pane_id.as_deref(),
        Some("w3S:p1"),
        "the registry-resolved pane is attached"
    );
    assert_eq!(
        who.pane_npub.as_deref(),
        Some(slate.as_str()),
        "the pane's stamped npub is attached"
    );
    assert!(
        who.identity_divergent,
        "stamped npub != registered npub must flag identity_divergent"
    );

    let slate_row = mailbox
        .whois_with_panes("elm-effect-rs", "SlateHerder", &panes, &agents, &bindings)
        .await
        .expect("whois SlateHerder");
    assert_eq!(slate_row.pane_id.as_deref(), Some("w3S:p1"));
    assert!(
        !slate_row.identity_divergent,
        "a pane whose stamp matches the registered npub is not divergent"
    );

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_dir_all(&data_dir);
}
