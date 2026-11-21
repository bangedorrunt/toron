// governed-by: ADR-0039
//! bd-9te8e: the server negotiates down for older MCP clients instead of
//! refusing them, while 2026-07-28 keeps its stricter metadata contract.

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use rmcp::ClientServiceExt;
use rmcp::model::{ClientCapabilities, ClientConfig, Implementation};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use toron_mcp::server::{EVENTS_NEED_NEW_PROTOCOL, SUPPORTED_PROTOCOL_VERSIONS};
use tower::ServiceExt;

async fn isolated_router() -> anyhow::Result<axum::Router> {
    let dir = std::env::temp_dir().join(format!("toron-negotiate-{}", uuid::Uuid::new_v4()));
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

async fn raw_events_list(
    router: axum::Router,
    version: &str,
    meta: bool,
) -> anyhow::Result<String> {
    let params = if meta {
        serde_json::json!({"_meta": {
            "io.modelcontextprotocol/protocolVersion": version,
            "io.modelcontextprotocol/clientInfo": {"name": "negotiation-test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {}
        }})
    } else {
        serde_json::json!({})
    };
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "events/list", "params": params
    });
    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::HOST, "localhost")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header("mcp-protocol-version", version)
                .header("mcp-method", "events/list")
                .body(Body::from(body.to_string()))?,
        )
        .await?;
    assert_eq!(resp.status(), StatusCode::OK, "handler errors stay in band");
    Ok(String::from_utf8(
        resp.into_body().collect().await?.to_bytes().to_vec(),
    )?)
}

#[tokio::test]
async fn every_advertised_version_can_initialize_and_list_tools() -> anyhow::Result<()> {
    for version in SUPPORTED_PROTOCOL_VERSIONS {
        let router = isolated_router().await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
        );
        let config = ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::from_build_env(),
        );
        let mut client = config
            .serve_with_lifecycle(
                transport,
                rmcp::ClientLifecycleMode::Discover {
                    preferred_versions: vec![version.clone()],
                },
            )
            .await?;
        let peer = client.peer_info().expect("peer info after initialize");
        assert_eq!(peer.protocol_version, *version, "negotiated version");
        let tools = client.list_tools(None).await?;
        assert_eq!(tools.tools.len(), 41, "old clients keep the tool surface");
        client.close().await?;
        server.abort();
    }
    Ok(())
}

#[tokio::test]
async fn events_answer_a_typed_fallback_on_old_sessions() -> anyhow::Result<()> {
    let text = raw_events_list(isolated_router().await?, "2025-11-25", false).await?;
    assert!(
        text.contains(EVENTS_NEED_NEW_PROTOCOL),
        "older sessions get the doorbell-fallback pointer: {text}"
    );

    let no_meta = raw_events_list(isolated_router().await?, "2026-07-28", false).await?;
    assert!(
        no_meta.contains("-32602") && no_meta.contains("missing required protocol metadata"),
        "the draft version still requires per-request metadata: {no_meta}"
    );

    let with_meta = raw_events_list(isolated_router().await?, "2026-07-28", true).await?;
    assert!(
        !with_meta.contains(EVENTS_NEED_NEW_PROTOCOL)
            && !with_meta.contains("missing required protocol metadata"),
        "a 2026-07-28 request with metadata reaches events/list: {with_meta}"
    );
    Ok(())
}
