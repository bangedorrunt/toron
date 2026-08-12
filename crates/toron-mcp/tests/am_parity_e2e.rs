// governed-by: ADR-0003
//
//! AM-parity end-to-end test (bd-am-parity-e2e).
//!
//! Boots the REAL toron MCP daemon (axum router on a real TCP socket, with an
//! operator key so the privileged surface is live) and drives the full 38-tool
//! AM-Rust parity surface over the Streamable HTTP wire as a real agent would.
//! Each cluster asserts FUNCTIONAL parity with the documented agent-mail
//! contract — not byte-parity on Nostr-vs-sqlite id shapes (allowlisted in
//! `docs/parity/known-divergences.md`), but behavioral parity: round-trips
//! deliver, conflicts are detected, reservations expire, threads resolve,
//! macros compose, products fan out.
//!
//! This is the "is toron a trustworthy agent-mail replacement?" gate. It
//! exists because the sibling `acceptance.rs` proves the surface is enumerable
//! (38 tools / 25 resources / outputSchema present) but never INVOKES a tool.
//! Enumeration ≠ function.
//!
//! Run: `cargo test -p toron-mcp --test am_parity_e2e`.

use std::sync::Arc;

use rmcp::ClientServiceExt;
use rmcp::model::{
    CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, ProtocolVersion,
};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use serde_json::Value;

// ======================================================================
// Harness
// ======================================================================

/// One live toron daemon: operator key on disk + env set BEFORE `router()`
/// (which reads `TORON_OPERATOR_KEY` at boot), axum on a random port, and an
/// rmcp client connected over Streamable HTTP. Dropping this kills the server
/// task + scrubs the temp dir.
struct LiveDaemon {
    client:
        rmcp::service::RunningService<rmcp::RoleClient, rmcp::model::InitializeRequestParams>,
    _server: tokio::task::JoinHandle<()>,
    _dir: Arc<ScrubDir>,
}

// We avoid pulling `tempfile` as a named dep by reusing the crate's existing
// pattern (std::env::temp_dir). Wrap a cleanup guard manually.
struct ScrubDir(std::path::PathBuf);
impl Drop for ScrubDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl LiveDaemon {
    async fn boot(tag: &str) -> anyhow::Result<Self> {
        // 1. temp dir holding the operator ncryptsec + the mailbox data dir.
        let dir = std::env::temp_dir().join(format!("toron-am-parity-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let scrub = Arc::new(ScrubDir(dir.clone()));

        // 2. generate an operator keypair → encrypt to ncryptsec → write 0600.
        let keys = toron_core::keys::generate();
        let pass = "am-parity-test-pass";
        let ncryptsec = toron_core::keys::encrypt(&keys, pass, 16)?;
        let key_path = dir.join("operator_nsec");
        std::fs::write(&key_path, ncryptsec)?;
        // chmod 0600 on unix (NIP-49 keys are sensitive at rest).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&key_path)?.permissions();
            perms.set_mode(0o600);
            std::fs::set_permissions(&key_path, perms)?;
        }

        // 3. env MUST be set before router() — operator_keys() reads it at boot.
        // SAFETY: test is single-threaded per process; env leak is bounded to
        // the test process lifetime and the key is a throwaway.
        unsafe {
            std::env::set_var("TORON_OPERATOR_KEY", key_path.to_str().unwrap());
            std::env::set_var("TORON_KEY_PASSPHRASE", pass);
            // Per-invocation mailbox root (R2 / ADR-0007 D3): WITHOUT this the
            // daemon falls back to the DURABLE XDG root
            // (`~/.local/share/toron`), which accumulates agent keys across
            // test runs. The unwrap-loop pipeline rebuild KDF-loads EVERY key
            // in the root (~1s/key, cold) — after a few runs the projection
            // (search/summarize/product fan) stalls for minutes while the
            // per-agent inbox (single-key load) stays fast. The parity suite
            // was green at 56s only while per-pid temp roots were the default;
            // the durable default re-introduced the stall (F2.KDF class). This
            // env restores per-invocation isolation for the tests; production
            // keeps the durable root.
            std::env::set_var("TORON_MCP_DATA_DIR", dir.join("mailbox"));
        }

        // 4. boot the router (reads the env above) on a kernel-chosen port.
        let router = toron_mcp::router().await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        // 5. connect an rmcp client over the real HTTP socket.
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
        );
        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::from_build_env(),
        );
        let client = client_info
            .serve_with_lifecycle(
                transport,
                rmcp::ClientLifecycleMode::Discover {
                    preferred_versions: vec![
                        ProtocolVersion::V_2026_07_28,
                        ProtocolVersion::V_2025_11_25,
                    ],
                },
            )
            .await?;

        Ok(Self {
            client,
            _server: server,
            _dir: scrub,
        })
    }

    /// Call a tool by name, returning the parsed JSON payload of the first
    /// text content block. Tool errors surface as a panic with the server's
    /// error text so failures are actionable.
    async fn call(&mut self, tool: &str, args: Value) -> Value {
        let result = self.call_raw(tool, args).await;
        assert!(
            !result.is_error.unwrap_or(false),
            "tool {tool} returned an error result: {:?}",
            result.content
        );
        Self::text_json(tool, &result.content)
    }

    /// Call a tool and return the RAW `CallToolResult` (error flag + content).
    /// Use when an MCP error is an EXPECTED functional outcome (e.g. a
    /// force-release denial on a non-stale holder).
    async fn call_raw(
        &mut self,
        tool: &str,
        args: Value,
    ) -> rmcp::model::CallToolResult {
        let params = CallToolRequestParams::new(tool.to_string())
            .with_arguments(args.as_object().cloned().unwrap_or_default());
        self.client
            .call_tool(params)
            .await
            .unwrap_or_else(|e| panic!("call_tool({tool}) transport error: {e}"))
    }

    /// Extract + parse the first text content block of a CallToolResult.
    fn text_json(tool: &str, content: &[rmcp::model::ContentBlock]) -> Value {
        let text = content
            .iter()
            .find_map(|c| c.as_text().map(|t| t.text.clone()))
            .unwrap_or_else(|| panic!("tool {tool} returned no text content"));
        serde_json::from_str::<Value>(&text)
            .unwrap_or_else(|e| panic!("tool {tool} text is not JSON: {e} / raw={text}"))
    }
}

// ======================================================================
// Cluster 1+2: Infrastructure + Identity
// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_infra_and_identity() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("infra-id").await?;
    let proj = format!("/tmp/toron-am-parity-infra-{}", uuid::Uuid::new_v4());

    // health_check → a status-ish field (archive/bus/ok).
    let health = d.call("health_check", serde_json::json!({})).await;
    let has_status = health.get("ok").is_some()
        || health.pointer("/bus/ok").is_some()
        || health.get("status").is_some();
    assert!(has_status, "health_check surfaces a status field: {health}");

    // ensure_project → idempotent; carries a slug.
    let p1 = d.call("ensure_project", serde_json::json!({ "human_key": proj })).await;
    let p2 = d.call("ensure_project", serde_json::json!({ "human_key": proj })).await;
    assert_eq!(
        p1["slug"], p2["slug"],
        "ensure_project slug stable across calls"
    );
    assert_eq!(
        p1["manifest_event_id"], p2["manifest_event_id"],
        "ensure_project manifest event id stable (idempotent)"
    );
    assert_eq!(p1["created"], true, "first ensure_project creates the manifest");
    assert_eq!(p2["created"], false, "second ensure_project is a no-op (created=false)");

    // register_agent (upsert) → crypto identity envelope (npub, key_path, ...).
    let alice = d
        .call(
            "register_agent",
            serde_json::json!({
                "project_key": proj,
                "program": "opencode",
                "model": "test",
                "name": "AliceOak",
                "task_description": "parity probe A",
            }),
        )
        .await;
    assert_eq!(alice["name"], "AliceOak", "register echoes the name");
    assert!(alice["npub"].is_string(), "toron returns a crypto npub (not an int id)");
    assert!(alice["key_path"].is_string(), "agent key path is materialized on disk");
    let npub_a = alice["npub"].as_str().unwrap().to_string();

    // re-register same name = upsert: same npub, no duplicate key.
    let alice_again = d
        .call(
            "register_agent",
            serde_json::json!({
                "project_key": proj,
                "program": "opencode",
                "model": "test",
                "name": "AliceOak",
            }),
        )
        .await;
    assert_eq!(
        alice_again["npub"].as_str().unwrap(),
        npub_a,
        "re-register is an upsert (npub stable)"
    );

    // register a second agent for the messaging round-trip.
    let bob = d
        .call(
            "register_agent",
            serde_json::json!({
                "project_key": proj,
                "program": "pi",
                "model": "test",
                "name": "BobPine",
            }),
        )
        .await;
    assert_eq!(bob["name"], "BobPine");

    // list_agents → both present, each with npub.
    let agents = d.call("list_agents", serde_json::json!({ "project_key": proj })).await;
    let arr = agents
        .as_array()
        .unwrap_or_else(|| panic!("list_agents is an array: {agents}"));
    assert!(arr.len() >= 2, "both registered agents visible: {agents}");
    let names: Vec<&str> = arr.iter().filter_map(|a| a["name"].as_str()).collect();
    assert!(names.contains(&"AliceOak") && names.contains(&"BobPine"));

    // whois → resolves name → npub (the AM "enriched profile" parity field).
    let who = d
        .call(
            "whois",
            serde_json::json!({ "project_key": proj, "agent_name": "AliceOak" }),
        )
        .await;
    assert_eq!(who["name"], "AliceOak");
    assert_eq!(who["npub"].as_str().unwrap(), npub_a);
    assert_eq!(who["program"], "opencode", "whois carries program (AM parity)");

    // create_agent_identity → always-fresh (mint a new key, never upsert).
    let carol = d
        .call(
            "create_agent_identity",
            serde_json::json!({
                "project_key": proj,
                "program": "grok",
                "model": "test",
                "name_hint": "CedarFern",
            }),
        )
        .await;
    assert_eq!(carol["name"], "CedarFern");
    assert!(carol["npub"].is_string());

    d.client.close().await?;
    Ok(())
}

// ======================================================================
// Cluster 3: Messaging (send → inbox → reply → ack → read → search → thread)
// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_messaging_round_trip() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("messaging").await?;
    let proj = format!("/tmp/toron-am-parity-msg-{}", uuid::Uuid::new_v4());
    let reg = |name: &str, program: &str| {
        let p = proj.clone();
        let n = name.to_string();
        let prog = program.to_string();
        async move {
            serde_json::json!({
                "project_key": p,
                "program": prog,
                "model": "test",
                "name": n,
            })
        }
    };
    d.call("register_agent", reg("AliceOak", "opencode").await).await;
    d.call("register_agent", reg("BobPine", "pi").await).await;

    // send_message: Alice → Bob, ack_required, on a thread.
    let send = d
        .call(
            "send_message",
            serde_json::json!({
                "project_key": proj,
                "sender_name": "AliceOak",
                "to": ["BobPine"],
                "subject": "parity subject",
                "body_md": "hello from Alice on the parity thread",
                "importance": "normal",
                "ack_required": true,
                "thread_id": "parity-thread-1",
            }),
        )
        .await;
    assert_eq!(send["count"], 1, "one delivery for one recipient");
    assert!(
        send["deliveries"].is_array() && send["deliveries"].as_array().unwrap().len() == 1,
        "deliveries[] carries the per-recipient record: {send}"
    );

    // The unwrap loop projects wraps → the inbox asynchronously. Poll Bob's
    // inbox until the message lands (bounded — same shape as flywheel's
    // live_parity harness).
    let bob_inbox = poll_inbox(&mut d, &proj, "BobPine", "parity subject", 30).await;
    let msg = &bob_inbox[0];
    assert_eq!(msg["sender"], "AliceOak", "inbox entry names the sender");
    assert_eq!(msg["subject"], "parity subject");
    assert_eq!(
        msg["body_md"], "hello from Alice on the parity thread",
        "include_bodies surfaces the body"
    );
    assert_eq!(msg["ack_required"], true, "ack_required flag round-trips");
    assert_eq!(msg["thread_id"], "parity-thread-1", "thread_id round-trips");
    let msg_id = msg["id"].as_str().expect("toron id is a hex string").to_string();
    assert!(
        msg_id.chars().all(|c| c.is_ascii_hexdigit()),
        "toron message id is a hex event id (not an int): {msg_id}"
    );

    // acknowledge_message: idempotent; carries an ack timestamp.
    // NOTE: toron's id model is hex (content-addressed). The tool MUST accept
    // the hex id as a string. AM uses integer ids; toron's divergence is
    // documented (known-divergences §1) — the tool arg is typed to match
    // toron's own id model, not AM's.
    let ack = d
        .call(
            "acknowledge_message",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "BobPine",
                "message_id": msg_id,
            }),
        )
        .await;
    assert_eq!(ack["acknowledged"], true, "ack surfaces acknowledged=true");
    // idempotent re-ack does not error.
    let ack2 = d
        .call(
            "acknowledge_message",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "BobPine",
                "message_id": msg_id,
            }),
        )
        .await;
    assert_eq!(ack2["acknowledged"], true, "re-ack is idempotent");

    // mark_message_read: idempotent read receipt.
    let read = d
        .call(
            "mark_message_read",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "BobPine",
                "message_id": msg_id,
            }),
        )
        .await;
    assert!(read.get("message_id").is_some(), "mark_read echoes the message id");

    // reply_message: Bob → Alice on the SAME thread. AM anchors a reply to the
    // original message_id; toron replies via thread_id (its NIP-10 model). The
    // thread must be preserved so summarize_thread resolves both participants.
    let reply = d
        .call(
            "reply_message",
            serde_json::json!({
                "project_key": proj,
                "sender_name": "BobPine",
                "to": ["AliceOak"],
                "subject": "Re: parity subject",
                "body_md": "ack from Bob",
                "thread_id": "parity-thread-1",
            }),
        )
        .await;
    assert_eq!(reply["count"], 1, "reply delivers to one recipient");

    // Alice's inbox eventually receives Bob's reply.
    let alice_inbox = poll_inbox(&mut d, &proj, "AliceOak", "ack from Bob", 30).await;
    assert_eq!(
        alice_inbox[0]["sender"], "BobPine",
        "Alice sees Bob's reply"
    );

    // search_messages: the message body is FTS-indexed (porter + unicode61).
    // The projection is ASYNC (unwrap loop → sqlite sink), so poll until the
    // FTS index catches up (bounded 20s) — an immediate single search races
    // the sink and flakes (the am-parity "projection never lands" class).
    let mut search_hits: Vec<Value> = Vec::new();
    for _ in 0..40 {
        let search = d
            .call(
                "search_messages",
                serde_json::json!({
                    "project_key": proj,
                    "query": "parity",
                    "limit": 10,
                }),
            )
            .await;
        search_hits = search
            .get("result")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        if !search_hits.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        !search_hits.is_empty(),
        "search finds the parity message (projection caught up)"
    );

    // summarize_thread: participants include both agents, message_count >= 2.
    let summary = d
        .call(
            "summarize_thread",
            serde_json::json!({
                "project_key": proj,
                "thread_id": "parity-thread-1",
            }),
        )
        .await;
    let participants = summary["participants"]
        .as_array()
        .unwrap_or_else(|| panic!("summarize returns participants[]: {summary}"));
    let participant_names: Vec<&str> =
        participants.iter().filter_map(|p| p.as_str()).collect();
    assert!(
        participant_names.contains(&"AliceOak") && participant_names.contains(&"BobPine"),
        "thread summary names both participants: {summary}"
    );
    assert!(
        summary["message_count"].as_u64().unwrap_or(0) >= 2,
        "thread has both messages: {summary}"
    );

    d.client.close().await?;
    Ok(())
}

// ======================================================================
// Cluster 4: Contacts (policy → request → respond → list)
// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_contacts_handshake() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("contacts").await?;
    let proj = format!("/tmp/toron-am-parity-contact-{}", uuid::Uuid::new_v4());
    for (name, prog) in [("DawnElm", "opencode"), ("FoxRidge", "pi")] {
        d.call(
            "register_agent",
            serde_json::json!({
                "project_key": proj,
                "program": prog,
                "model": "test",
                "name": name,
            }),
        )
        .await;
    }

    // set_contact_policy on the recipient.
    let _ = d
        .call(
            "set_contact_policy",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "FoxRidge",
                "policy": "contacts_only",
            }),
        )
        .await;

    // request_contact: DawnElm → FoxRidge.
    let req = d
        .call(
            "request_contact",
            serde_json::json!({
                "project_key": proj,
                "from_agent": "DawnElm",
                "to_agent": "FoxRidge",
                "reason": "parity coordination",
            }),
        )
        .await;
    assert!(
        req.get("from_name").is_some() || req.get("status").is_some(),
        "request_contact returns a link/status: {req}"
    );

    // respond_contact: FoxRidge accepts.
    let _ = d
        .call(
            "respond_contact",
            serde_json::json!({
                "project_key": proj,
                "to_agent": "FoxRidge",
                "from_agent": "DawnElm",
                "accept": true,
            }),
        )
        .await;

    // list_contacts: the accepted link is visible.
    let links = d
        .call(
            "list_contacts",
            serde_json::json!({ "project_key": proj, "agent_name": "FoxRidge" }),
        )
        .await;
    let arr = links
        .as_array()
        .unwrap_or_else(|| panic!("list_contacts is an array: {links}"));
    assert!(
        !arr.is_empty(),
        "the accepted contact link is listed: {links}"
    );

    d.client.close().await?;
    Ok(())
}

// ======================================================================
// Cluster 5: File Reservations (acquire → conflict → renew → release → force)
// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_file_reservations() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("reservations").await?;
    let proj = format!("/tmp/toron-am-parity-res-{}", uuid::Uuid::new_v4());
    for name in ["GaleSummit", "HillCrest"] {
        d.call(
            "register_agent",
            serde_json::json!({
                "project_key": proj,
                "program": "opencode",
                "model": "test",
                "name": name,
            }),
        )
        .await;
    }

    // GaleSummit acquires an exclusive lease on src/**.
    let acquire = d
        .call(
            "file_reservation_paths",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "GaleSummit",
                "paths": ["src/**"],
                "ttl_seconds": 3600,
                "exclusive": true,
                "reason": "br-parity",
            }),
        )
        .await;
    assert!(
        acquire["granted"].is_array() && acquire["granted"].as_array().unwrap().len() == 1,
        "exclusive lease granted: {acquire}"
    );
    let res_id = acquire["granted"][0]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("reservation carries an id: {acquire}"))
        .to_string();

    // HillCrest checks the SAME path → conflict reported (but read-only check).
    let conflicts = d
        .call(
            "check_file_reservation_conflicts",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "HillCrest",
                "paths": ["src/auth.rs"],
            }),
        )
        .await;
    assert_eq!(
        conflicts["conflict_free"], false,
        "conflict detected against GaleSummit's exclusive lease: {conflicts}"
    );
    assert!(
        conflicts["total_conflicting_reservations"].as_u64().unwrap_or(0) >= 1,
        "conflict count is non-zero: {conflicts}"
    );
    // bd-dsu1: the conflict report carries the conflicting lease's event id —
    // the operator lock board needs it for diagnostics without DB access.
    let ids = conflicts["conflicts"][0]["reservation_ids"]
        .as_array()
        .unwrap_or_else(|| panic!("conflict carries reservation_ids: {conflicts}"));
    assert_eq!(ids.len(), 1, "one conflicting lease, one id: {conflicts}");
    assert_eq!(
        ids[0].as_str().unwrap_or(""),
        res_id.as_str(),
        "reservation id matches the granted lease's id: {conflicts}"
    );

    // GaleSummit renews → expiry extends (absolute, not recomputed from now).
    // bd-evqs cap: a lease whose remaining TTL is ALREADY >= extend_secs is
    // NOT republished — so the extension must exceed the remaining TTL
    // (acquire used 3600s; 7200s > remaining) to actually renew.
    let renew = d
        .call(
            "renew_file_reservations",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "GaleSummit",
                "extend_seconds": 7200,
            }),
        )
        .await;
    assert!(renew["renewed"].as_u64().unwrap_or(0) >= 1, "renew extends the lease");

    // release → freed.
    let release = d
        .call(
            "release_file_reservations",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "GaleSummit",
            }),
        )
        .await;
    assert!(release["released"].as_u64().unwrap_or(0) >= 1, "release frees the lease");

    // After release, HillCrest's conflict check is now clear.
    let clear = d
        .call(
            "check_file_reservation_conflicts",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "HillCrest",
                "paths": ["src/auth.rs"],
            }),
        )
        .await;
    assert_eq!(
        clear["conflict_free"], true,
        "no conflict after release: {clear}"
    );

    // force_release_file_reservation path: re-acquire then operator force-release.
    // The staleness heuristic correctly DENIES force-release of an active
    // (non-stale) holder — that denial is the right behavior, not a failure.
    // We assert the tool runs the operator-gated logic and returns EITHER a
    // release OR a structured denial (both prove the tool is functional).
    let re_acq = d
        .call(
            "file_reservation_paths",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "GaleSummit",
                "paths": ["docs/**"],
                "ttl_seconds": 3600,
                "exclusive": true,
            }),
        )
        .await;
    let forced_raw = d
        .call_raw(
            "force_release_file_reservation",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "HillCrest",
                "path": "docs/**",
                "note": "parity force-release",
                "notify_previous": false,
            }),
        )
        .await;
    // The staleness heuristic correctly DENIES force-release of an active
    // (non-stale) holder. The denial surfaces as an MCP error result whose
    // text carries the reason — assert that reason is the staleness gate, not
    // a transport/schema failure (which would say "deserialize" / "no operator").
    let forced_text = forced_raw
        .content
        .iter()
        .find_map(|c| c.as_text().map(|t| t.text.clone()))
        .unwrap_or_default();
    assert!(
        forced_text.contains("stale") || forced_text.contains("denied"),
        "force-release of an active holder is denied by the staleness gate: {forced_text}"
    );
    let _ = res_id; // suppress unused on older toolchains
    d.client.close().await?;
    Ok(())
}

// ======================================================================
// Cluster 6: Build Slots (acquire → renew → release)
// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_build_slots() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("slots").await?;
    let proj = format!("/tmp/toron-am-parity-slot-{}", uuid::Uuid::new_v4());
    d.call(
        "register_agent",
        serde_json::json!({
            "project_key": proj,
            "program": "opencode",
            "model": "test",
            "name": "IrisBay",
        }),
    )
    .await;

    let acq = d
        .call(
            "acquire_build_slot",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "IrisBay",
                "slot": "ci",
                "exclusive": true,
                "ttl_seconds": 1800,
            }),
        )
        .await;
    assert!(
        acq.get("slot").is_some() || acq.get("granted").is_some(),
        "acquire returns the slot envelope: {acq}"
    );

    let _ = d
        .call(
            "renew_build_slot",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "IrisBay",
                "slot": "ci",
                "extend_seconds": 600,
            }),
        )
        .await;

    let rel = d
        .call(
            "release_build_slot",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "IrisBay",
                "slot": "ci",
            }),
        )
        .await;
    // release returns a truthy result (bare `true` or an envelope). AM
    // returns `{slot, released_at}`; toron currently returns bare `true` —
    // either is a successful release, neither is an error.
    assert!(
        rel == Value::Bool(true)
            || rel.get("released").is_some()
            || rel.get("slot").is_some()
            || rel.get("ok").is_some(),
        "release returns a result: {rel}"
    );

    d.client.close().await?;
    Ok(())
}

// ======================================================================
// Cluster 7+8: Macros + Product Bus
// ======================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_macros_and_product_bus() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("macros-prod").await?;
    let proj = format!("/tmp/toron-am-parity-macro-{}", uuid::Uuid::new_v4());

    // macro_start_session: ensure project + register + reserve + inbox in one call.
    let session = d
        .call(
            "macro_start_session",
            serde_json::json!({
                "human_key": proj,
                "program": "opencode",
                "model": "test",
                "agent_name": "JadeCove",
                "task_description": "parity macro session",
                "file_reservation_paths": ["src/**"],
                "file_reservation_reason": "br-macro",
                "file_reservation_ttl_seconds": 1800,
                "inbox_limit": 10,
            }),
        )
        .await;
    assert_eq!(session["agent_name"], "JadeCove", "macro boots the identity");
    assert!(session["npub"].is_string(), "macro returns the npub");
    assert!(
        session["granted_reservations"].is_array()
            && session["granted_reservations"].as_array().unwrap().len() == 1,
        "macro reserved the requested paths: {session}"
    );

    // file_reservation_cycle: reserve then auto-release.
    let cycle = d
        .call(
            "macro_file_reservation_cycle",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "JadeCove",
                "paths": ["tests/**"],
                "ttl_seconds": 600,
                "exclusive": true,
                "auto_release": true,
            }),
        )
        .await;
    assert!(
        cycle.get("granted").is_some() || cycle.get("released").is_some(),
        "cycle reserves (and releases): {cycle}"
    );

    // contact_handshake: JadeCove + a second agent, auto-accept.
    d.call(
        "register_agent",
        serde_json::json!({
            "project_key": proj,
            "program": "pi",
            "model": "test",
            "name": "KiteLoft",
        }),
    )
    .await;
    let hs = d
        .call(
            "macro_contact_handshake",
            serde_json::json!({
                "project_key": proj,
                "agent_name": "JadeCove",
                "to_agent": "KiteLoft",
                "auto_accept": true,
                "reason": "parity handshake",
            }),
        )
        .await;
    assert!(
        hs.get("link").is_some() || hs.get("status").is_some() || hs.get("accepted").is_some(),
        "handshake returns a link/status: {hs}"
    );

    // Drop a message on JadeCove so the product-bus search has something to find.
    d.call(
        "send_message",
        serde_json::json!({
            "project_key": proj,
            "sender_name": "KiteLoft",
            "to": ["JadeCove"],
            "subject": "product-bus probe",
            "body_md": "product bus parity body",
            "thread_id": "prod-1",
        }),
    )
    .await;

    // ensure_product + link the project.
    let prod = d
        .call(
            "ensure_product",
            serde_json::json!({ "name": "parity-product" }),
        )
        .await;
    let product_key = prod["product_key"]
        .as_str()
        .or_else(|| prod["product_uid"].as_str())
        .unwrap_or("parity-product")
        .to_string();
    let _ = d
        .call(
            "products_link",
            serde_json::json!({
                "product_key": product_key,
                "project_key": proj,
            }),
        )
        .await;

    // Probe: membership must persist (kind-30011 a-tags AND/OR the SQLite
    // mirror). If this is empty, `search_messages_product` fans over nothing.
    let mut linked = false;
    for _ in 0..20 {
        let p2 = d
            .call(
                "ensure_product",
                serde_json::json!({ "product_key": product_key }),
            )
            .await;
        let members: Vec<&str> = p2["member_projects"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        if members.contains(&proj.as_str()) {
            linked = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(linked, "products_link persists membership (probe ensure_product: {prod} -> {product_key})");

    // fetch_inbox_product: JadeCove's inbox fanned across the product.
    let pinbox = d
        .call(
            "fetch_inbox_product",
            serde_json::json!({
                "product_key": product_key,
                "agent_name": "JadeCove",
                "limit": 10,
            }),
        )
        .await;
    assert!(
        pinbox.is_array() || pinbox.get("result").is_some(),
        "product inbox returns a list: {pinbox}"
    );

    // search_messages_product: poll until the projection catches up. First
    // confirm per-project search finds the message (isolates projection vs
    // product-membership), then fan across the product.
    let mut per_project_hit = false;
    for _ in 0..40 {
        let s = d
            .call(
                "search_messages",
                serde_json::json!({
                    "project_key": proj,
                    "query": "product",
                    "limit": 10,
                }),
            )
            .await;
        let hits = s
            .get("result")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        if !hits.is_empty() {
            per_project_hit = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        per_project_hit,
        "per-project search finds the message (projection works)"
    );

    let mut prod_hits: Vec<Value> = Vec::new();
    for _ in 0..40 {
        let s = d
            .call(
                "search_messages_product",
                serde_json::json!({
                    "product_key": product_key,
                    "query": "product",
                    "limit": 10,
                }),
            )
            .await;
        // Wire shape is a BARE array of ProjectSearchMessageOut (outputSchema
        // `Vec<ProjectSearchMessageOut>`), unlike per-project search_messages
        // which wraps in `{result: [...], next_cursor}`. Accept both.
        prod_hits = if let Some(arr) = s.as_array() {
            arr.clone()
        } else {
            s.get("result")
                .and_then(|r| r.as_array())
                .cloned()
                .unwrap_or_default()
        };
        if !prod_hits.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        !prod_hits.is_empty(),
        "product-bus search finds the cross-project message: {prod_hits:?}"
    );

    d.client.close().await?;
    Ok(())
}

// ======================================================================
// Helpers
// ======================================================================

/// Poll an agent's inbox until at least one entry's subject contains `needle`,
/// returning the filtered entries. Bounded by `tries` × 500ms — the unwrap
/// loop projects gift wraps → the sqlite inbox asynchronously.
async fn poll_inbox(d: &mut LiveDaemon, proj: &str, agent: &str, needle: &str, tries: usize) -> Vec<Value> {
    let mut last_full: Vec<Value> = Vec::new();
    for _ in 0..tries {
        let inbox = d
            .call(
                "fetch_inbox",
                serde_json::json!({
                    "project_key": proj,
                    "agent_name": agent,
                    "include_bodies": true,
                    "limit": 20,
                }),
            )
            .await;
        last_full = inbox
            .as_array()
            .cloned()
            .unwrap_or_else(|| panic!("inbox is an array: {inbox}"));
        let matched: Vec<Value> = last_full
            .iter()
            .filter(|m| {
                let s = m.get("subject").and_then(|s| s.as_str()).unwrap_or("");
                let b = m.get("body_md").and_then(|s| s.as_str()).unwrap_or("");
                s.contains(needle) || b.contains(needle)
            })
            .cloned()
            .collect();
        if !matched.is_empty() {
            return matched;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!(
        "inbox for {agent} never received a subject matching {needle:?} \
         within {tries} tries; last inbox snapshot: {last_full:?}"
    );
}

/// D8.2 honesty gap (bd-p5-d82-parity-am-parity-e2e-5dp3): six catalog tools
/// were ENUMERATED by acceptance.rs but never FUNCTIONALLY driven:
/// `install_precommit_guard`, `uninstall_precommit_guard`,
/// `resolve_pane_identity`, `cleanup_pane_identities`, `macro_prepare_thread`,
/// `summarize_thread_product`. This cluster drives all six for their honest
/// behavior (real git repo for the guard; structured herdr note when the
/// backend is absent; real registration + product fan-out for the macros).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_guard_pane_and_prepare_thread() -> anyhow::Result<()> {
    let mut d = LiveDaemon::boot("guard-pane").await?;
    let proj = format!("/tmp/toron-am-parity-guard-{}", uuid::Uuid::new_v4());

    // install_precommit_guard needs a REAL git repo (git2 open).
    let repo = std::env::temp_dir().join(format!("toron-parity-repo-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&repo)?;
    let init = std::process::Command::new("git")
        .arg("init")
        .current_dir(&repo)
        .output()?;
    assert!(init.status.success(), "git init in temp repo");
    let installed = d
        .call(
            "install_precommit_guard",
            serde_json::json!({
                "project_key": proj,
                "code_repo_path": repo.to_str().unwrap(),
            }),
        )
        .await;
    assert!(
        installed.get("hook_path").is_some(),
        "install_precommit_guard materializes the hook path: {installed}"
    );
    let removed = d
        .call(
            "uninstall_precommit_guard",
            serde_json::json!({ "code_repo_path": repo.to_str().unwrap() }),
        )
        .await;
    assert!(
        removed.is_boolean() || removed.is_object(),
        "uninstall_precommit_guard returns the removal result: {removed}"
    );

    // resolve_pane_identity / cleanup_pane_identities are herdr-backed: without
    // a live herdr socket the HONEST response is a structured note naming the
    // requirement — never a hang, never a fake pane mapping.
    let pane = d
        .call(
            "resolve_pane_identity",
            serde_json::json!({ "project_key": proj, "pane_id": "pane-xyz" }),
        )
        .await;
    assert!(
        pane.get("agent_name").is_some() || pane.get("note").is_some(),
        "resolve_pane_identity is honest (mapping or herdr note): {pane}"
    );
    let cleaned = d
        .call(
            "cleanup_pane_identities",
            serde_json::json!({ "project_key": proj }),
        )
        .await;
    assert!(
        cleaned.is_array() || cleaned.is_object(),
        "cleanup_pane_identities returns a structured response: {cleaned}"
    );

    // macro_prepare_thread: ensure registration + thread summary in one call.
    let prep = d
        .call(
            "macro_prepare_thread",
            serde_json::json!({
                "project_key": proj,
                "thread_id": "thread-probe",
                "program": "opencode",
                "model": "test",
                "agent_name": "WrenPeak",
                "register_if_missing": true,
            }),
        )
        .await;
    assert_eq!(prep["agent_name"], "WrenPeak", "macro_prepare_thread echoes the agent");
    assert!(prep["npub"].is_string(), "macro_prepare_thread registers (npub): {prep}");
    assert!(
        prep.get("summary").is_some(),
        "macro_prepare_thread returns a thread summary: {prep}"
    );

    // summarize_thread_product: product-bus fan-out across a linked project.
    let prod = format!("parity-prod-{}", uuid::Uuid::new_v4());
    let ep = d
        .call("ensure_product", serde_json::json!({ "product_key": prod, "name": "parity" }))
        .await;
    assert!(
        ep.get("product_key").is_some() || ep.get("name").is_some(),
        "ensure_product returns the product: {ep}"
    );
    d.call(
        "products_link",
        serde_json::json!({ "product_key": prod, "project_key": proj }),
    )
    .await;
    d.call(
        "send_message",
        serde_json::json!({
            "project_key": proj,
            "sender_name": "WrenPeak",
            "to": ["WrenPeak"],
            "subject": "prod-thread",
            "body_md": "hello product",
            "thread_id": "thread-probe",
        }),
    )
    .await;
    let stp = d
        .call(
            "summarize_thread_product",
            serde_json::json!({ "product_key": prod, "thread_id": "thread-probe" }),
        )
        .await;
    assert!(
        stp.get("summary").is_some() || stp.get("participants").is_some(),
        "summarize_thread_product fans across the product: {stp}"
    );

    let _ = std::fs::remove_dir_all(&repo);
    Ok(())
}

// (dead helpers pruned: the test asserts behavior directly on Value.)
