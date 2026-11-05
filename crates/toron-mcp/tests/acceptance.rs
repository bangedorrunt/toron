// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-led.7.1 acceptance: an rmcp client completes `initialize` over both
//! transports and the negotiated protocol is 2026-07-28.

use rmcp::ClientServiceExt;
use rmcp::ServiceExt;
use rmcp::model::{
    ClientCapabilities, ClientConfig, Implementation, ProtocolVersion, ReadResourceRequestParams,
};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};

/// The 38 AM-Rust parity tool names (bd-led.7.2).
const AM_RUST_TOOLS: [&str; 38] = [
    "health_check",
    "ensure_project",
    "install_precommit_guard",
    "uninstall_precommit_guard",
    "register_agent",
    "create_agent_identity",
    "whois",
    "resolve_pane_identity",
    "cleanup_pane_identities",
    "list_agents",
    "send_message",
    "reply_message",
    "fetch_inbox",
    "acknowledge_message",
    "mark_message_read",
    "request_contact",
    "respond_contact",
    "list_contacts",
    "set_contact_policy",
    "check_file_reservation_conflicts",
    "file_reservation_paths",
    "renew_file_reservations",
    "release_file_reservations",
    "force_release_file_reservation",
    "search_messages",
    "summarize_thread",
    "macro_start_session",
    "macro_prepare_thread",
    "macro_contact_handshake",
    "macro_file_reservation_cycle",
    "ensure_product",
    "products_link",
    "search_messages_product",
    "fetch_inbox_product",
    "summarize_thread_product",
    "acquire_build_slot",
    "renew_build_slot",
    "release_build_slot",
];

/// Isolated router per test (GS-018): each test gets its own data dir +
/// operator key, so parallel tests (same binary or sibling binaries in one
/// `cargo test --workspace` run) never share a sqlite file. Sharing the
/// durable root caused migration races (`UNIQUE ... _sqlx_migrations`) and
/// write-lock convoys (`database is locked`) on loaded runners. Uses the
/// bd-6wfv explicit-paths API — no process-global env reads.
async fn isolated_router() -> anyhow::Result<axum::Router> {
    let dir = std::env::temp_dir().join(format!("toron-accept-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let keys = toron_core::keys::generate();
    let key_path = dir.join("op.ncryptsec");
    toron_core::keys::write_encrypted_key(
        &key_path,
        &keys,
        "test-pass",
        toron_core::keys::TEST_LOG_N,
    )?;
    toron_mcp::router_with_isolated(dir, key_path, "test-pass").await
}

#[tokio::test]
async fn http_resources_list_and_read() -> anyhow::Result<()> {
    let router = isolated_router().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );
    let client_info = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    );
    let mut client = client_info
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

    let resources = client.list_all_resources().await?;
    assert_eq!(
        resources.len(),
        7,
        "only concrete resources belong in resources/list"
    );
    assert!(
        resources.iter().all(|r| !r.uri.contains('{')),
        "resources/list must not contain URI templates"
    );

    let templates = client.list_resource_templates(None).await?;
    assert_eq!(templates.resource_templates.len(), 18);
    assert!(
        templates
            .resource_templates
            .iter()
            .all(|t| t.uri_template.contains('{')),
        "resources/templates/list must contain URI templates"
    );

    for resource in &resources {
        let read = client
            .read_resource(ReadResourceRequestParams::new(resource.uri.clone()))
            .await?;
        assert_eq!(
            read.contents.len(),
            1,
            "listed resource is readable: {}",
            resource.uri
        );
    }

    let env = client
        .read_resource(ReadResourceRequestParams::new("toron://config/environment"))
        .await?;
    assert_eq!(env.contents.len(), 1);

    let missing = client
        .read_resource(ReadResourceRequestParams::new(
            "toron://nope/does-not-exist",
        ))
        .await;
    assert!(missing.is_err(), "unknown resource must be rejected");

    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn http_initialize_negotiates_2026_07_28() -> anyhow::Result<()> {
    let router = isolated_router().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );

    let client_info = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    );
    let mut client = client_info
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

    let peer = client.peer_info().expect("peer info after initialize");
    assert_eq!(
        peer.protocol_version,
        ProtocolVersion::V_2026_07_28,
        "negotiated protocol must be 2026-07-28"
    );
    assert!(
        peer.server_info.is_some(),
        "server identity must be present"
    );

    let tools = client.list_tools(None).await?;
    assert_eq!(
        tools.tools.len(),
        41,
        "38 AM-Rust parity tools + release_file_reservations_by_reason (bd-h3oj) \
         + list_file_reservations (bd-locks-list-daemon-xetz) \
         + ask_judgment (bd-ncpt, the frozen41-surface)"
    );

    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn http_lists_exactly_38_am_rust_tools() -> anyhow::Result<()> {
    let router = isolated_router().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );
    let client_info = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    );
    let mut client = client_info
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

    let tools = client.list_tools(None).await?;
    assert_eq!(
        tools.tools.len(),
        AM_RUST_TOOLS.len() + 3,
        "38 parity tools + release_file_reservations_by_reason (bd-h3oj) \
         + list_file_reservations (bd-locks-list-daemon-xetz) \
         + ask_judgment (bd-ncpt), got {}",
        tools.tools.len()
    );

    let names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_ref()).collect();
    for expected in AM_RUST_TOOLS {
        assert!(names.contains(&expected), "missing tool: {expected}");
    }
    assert!(
        names.contains(&"release_file_reservations_by_reason"),
        "additive release-on-bead-close tool missing"
    );

    assert!(
        names.contains(&"list_file_reservations"),
        "additive read-only lease board missing"
    );

    assert!(
        names.contains(&"ask_judgment"),
        "additive judgment bridge missing (bd-ncpt)"
    );
    let by_name = |n: &str| {
        tools
            .tools
            .iter()
            .find(|t| t.name == n)
            .unwrap_or_else(|| panic!("tool {n}"))
    };
    assert_eq!(by_name("send_message").name, "send_message");
    assert_eq!(by_name("fetch_inbox").name, "fetch_inbox");
    assert_eq!(
        by_name("file_reservation_paths").name,
        "file_reservation_paths"
    );

    let ro = |n: &str| by_name(n).annotations.as_ref();
    assert_eq!(
        ro("health_check").and_then(|a| a.read_only_hint),
        Some(true),
        "health_check read-only"
    );
    assert_eq!(
        ro("search_messages").and_then(|a| a.read_only_hint),
        Some(true),
        "search_messages read-only"
    );
    assert_eq!(
        ro("force_release_file_reservation").and_then(|a| a.destructive_hint),
        Some(true),
        "force_release destructive"
    );
    assert_eq!(
        ro("install_precommit_guard").and_then(|a| a.destructive_hint),
        Some(true),
        "install_precommit_guard destructive"
    );

    let no_output: Vec<&str> = tools
        .tools
        .iter()
        .filter(|t| t.output_schema.is_none())
        .map(|t| t.name.as_ref())
        .collect();
    assert!(
        no_output.is_empty(),
        "tools missing outputSchema on the wire ({}): {}",
        no_output.len(),
        no_output.join(", ")
    );

    client.close().await?;
    server.abort();
    Ok(())
}

/// 2026-07-28 retired the `initialize` handshake: a client that names
/// V_2026_07_28 over `initialize` is answered with the server's newest
/// legacy version (V_2025_11_25). The future-proof path is `server/discover`
/// (`ClientLifecycleMode::Discover`), exercised here over stdio.
#[tokio::test]
async fn stdio_discover_negotiates_2026_07_28() -> anyhow::Result<()> {
    let (server_io, client_io) = tokio::io::duplex(4096);

    let server_task = tokio::spawn(async move {
        let server = toron_mcp::ToronServer::new();
        let running = server.serve(server_io).await;
        match running {
            Ok(running) => {
                let _ = running.waiting().await;
            }
            Err(e) => eprintln!("server failed: {e}"),
        }
    });

    let client_info = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    )
    .with_protocol_version(ProtocolVersion::V_2026_07_28);
    let client = client_info
        .serve_with_lifecycle(
            client_io,
            rmcp::ClientLifecycleMode::Discover {
                preferred_versions: vec![
                    ProtocolVersion::V_2026_07_28,
                    ProtocolVersion::V_2025_11_25,
                ],
            },
        )
        .await;

    server_task.abort();
    match client {
        Ok(mut running) => {
            let peer = running.peer_info().expect("peer info");
            assert_eq!(peer.protocol_version, ProtocolVersion::V_2026_07_28);
            running.close().await?;
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("stdio initialize failed: {e}")),
    }
}

/// ADR-0006 (bd-esh.5): the daemon router serves the Blossom surface at
/// `/.blossom` alongside the `/mcp` MCP transport — a PUT upload then a GET
/// retrieval round-trip over live HTTP.
#[tokio::test]
async fn blossom_route_serves_upload_and_retrieval() -> anyhow::Result<()> {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let router = isolated_router().await?;
    let payload = b"blossom round-trip payload";

    let put = router
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/.blossom/upload")
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(payload.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        put.status() == StatusCode::CREATED || put.status() == StatusCode::OK,
        "upload accepted (BUD-02: 201 new or 200 existing), got {}",
        put.status()
    );
    let descriptor: serde_json::Value =
        serde_json::from_slice(&put.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let hash = descriptor["sha256"].as_str().unwrap().to_string();

    let get = router
        .oneshot(
            Request::builder()
                .uri(format!("/.blossom/{hash}.txt"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK, "retrieval ok");
    assert_eq!(
        get.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        "*",
        "BUD-01 CORS"
    );
    let got = get.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(got.as_ref(), payload, "retrieved bytes match");
    Ok(())
}
