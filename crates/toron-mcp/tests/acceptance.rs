// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-led.7.1 acceptance: an rmcp client completes `initialize` over both
//! transports and the negotiated protocol is 2026-07-28.

use rmcp::model::{
    ClientCapabilities, ClientInfo, Implementation, ProtocolVersion, ReadResourceRequestParams,
};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::ClientServiceExt;
use rmcp::ServiceExt;

/// The 38 AM-Rust parity tool names (bd-led.7.2).
const AM_RUST_TOOLS: [&str; 38] = [
    // Infrastructure (4)
    "health_check",
    "ensure_project",
    "install_precommit_guard",
    "uninstall_precommit_guard",
    // Identity (6)
    "register_agent",
    "create_agent_identity",
    "whois",
    "resolve_pane_identity",
    "cleanup_pane_identities",
    "list_agents",
    // Messaging (5)
    "send_message",
    "reply_message",
    "fetch_inbox",
    "acknowledge_message",
    "mark_message_read",
    // Contacts (4)
    "request_contact",
    "respond_contact",
    "list_contacts",
    "set_contact_policy",
    // File reservations (5)
    "check_file_reservation_conflicts",
    "file_reservation_paths",
    "renew_file_reservations",
    "release_file_reservations",
    "force_release_file_reservation",
    // Search (2)
    "search_messages",
    "summarize_thread",
    // Macros (4)
    "macro_start_session",
    "macro_prepare_thread",
    "macro_contact_handshake",
    "macro_file_reservation_cycle",
    // Product bus (5)
    "ensure_product",
    "products_link",
    "search_messages_product",
    "fetch_inbox_product",
    "summarize_thread_product",
    // Build slots (3)
    "acquire_build_slot",
    "renew_build_slot",
    "release_build_slot",
];

#[tokio::test]
async fn http_resources_list_and_read() -> anyhow::Result<()> {
    let router = toron_mcp::router().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );
    let client_info = ClientInfo::new(
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

    // 25 parity resources listed.
    let resources = client.list_all_resources().await?;
    assert_eq!(resources.len(), 25, "AM-Rust parity requires 25 resources");

    // Templates expand (any resource with a {param}).
    let templates = client.list_resource_templates(None).await?;
    assert!(!templates.resource_templates.is_empty());

    // Static resource reads succeed.
    let env = client
        .read_resource(ReadResourceRequestParams::new("toron://config/environment"))
        .await?;
    assert_eq!(env.contents.len(), 1);

    // Unknown resource is rejected with invalid params.
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
    let router = toron_mcp::router().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );

    let client_info = ClientInfo::new(
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
        38,
        "AM-Rust parity requires exactly 38 tools"
    );

    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn http_lists_exactly_38_am_rust_tools() -> anyhow::Result<()> {
    let router = toron_mcp::router().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );
    let client_info = ClientInfo::new(
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
        AM_RUST_TOOLS.len(),
        "exactly 38 tools, got {}",
        tools.tools.len()
    );

    let names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_ref()).collect();
    for expected in AM_RUST_TOOLS {
        assert!(names.contains(&expected), "missing tool: {expected}");
    }

    // Exact name matches on representative tools.
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

    // Annotation parity: read-only + destructive hints on the wire.
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

    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_initialize_completes() -> anyhow::Result<()> {
    // Drive the server over an in-process duplex pair instead of the real
    // process stdin/stdout so the test needs no child binary. The wire frames
    // are identical to a real stdio connection (newline-delimited JSON-RPC).
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

    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    )
    .with_protocol_version(ProtocolVersion::V_2026_07_28);
    let client = client_info.serve(client_io).await;

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
