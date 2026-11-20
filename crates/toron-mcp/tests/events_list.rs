// governed-by: ADR-0039
//! U1 conformance: SEP-3415 wire shape, AE1's in-band -32602 vs HTTP 400, the
//! frozen 41-tool surface, and the regression that legacy clients keep working.
//! No live daemon, no operator key material (throwaway keys in temp).

use rmcp::ClientServiceExt;
use rmcp::model::{ClientCapabilities, ClientConfig, Implementation, ProtocolVersion};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use toron_mcp::events::{EVENTS_EXTENSION, route};
use toron_mcp::server::ToronServer;

#[test]
fn events_list_uses_sep3415_wire_keys() {
    let result = route("events/list", None).expect("routed").expect("ok");
    let rows = result.0["events"].as_array().expect("events array");
    assert_eq!(rows.len(), 6);
    for row in rows {
        let mut keys: Vec<&str> = row
            .as_object()
            .expect("row object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "delivery",
                "description",
                "inputSchema",
                "name",
                "payloadSchema"
            ],
            "exact SEP-3415 row keys"
        );
        assert!(row["name"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(row["delivery"], serde_json::json!(["poll", "push"]));
        assert_eq!(row["inputSchema"]["properties"]["name"]["type"], "string");
        assert_eq!(row["payloadSchema"]["type"], "object");
        let props = row["payloadSchema"]["properties"].as_object().unwrap();
        assert!(props.contains_key("event_id"));
        assert!(!props.contains_key("body"), "hint-only payload (R5)");
    }
}

#[test]
fn capabilities_advertise_the_events_extension_only_under_extensions() {
    let caps = ToronServer::capabilities();
    let ext = caps.extensions.as_ref().expect("extensions");
    assert_eq!(ext.len(), 1, "U1 advertises only the events extension");
    let value = &ext[EVENTS_EXTENSION];
    assert_eq!(value["listChanged"], false, "SEP-3415 capability value");
    assert_eq!(value.len(), 1, "no version key");
    assert!(caps.tools.is_some(), "tools capability unchanged");
}

#[test]
fn ae1_unknown_event_name_is_in_band_invalid_params() {
    let params = serde_json::json!({"name": "not.an.event"});
    let err = route("events/subscribe", Some(&params))
        .expect("routed")
        .expect_err("unknown name");
    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    assert_eq!(err.code.0, -32602);
}

#[test]
fn known_names_on_unimplemented_methods_are_errors_never_ok() {
    let params = serde_json::json!({"name": "mail.addressed", "arguments": {}});
    for method in ["events/subscribe", "events/poll", "events/stream"] {
        assert!(
            route(method, Some(&params)).expect("routed").is_err(),
            "{method} must not fake success"
        );
    }
}

async fn isolated_router() -> anyhow::Result<axum::Router> {
    let dir = std::env::temp_dir().join(format!("toron-events-{}", uuid::Uuid::new_v4()));
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

/// AE1 at the HTTP boundary: an unknown event name is an in-band -32602 with
/// HTTP 200; a request missing the mandatory `_meta` is HTTP 400. The 400 is
/// now method-scoped (events/* only), not a global transport option.
#[tokio::test]
async fn ae1_http_contract() -> anyhow::Result<()> {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let router = isolated_router().await?;
    let meta = serde_json::json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "events-test", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {}
    });
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "events/subscribe",
        "params": {"name": "not.an.event", "_meta": meta}
    });
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::HOST, "localhost")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header("mcp-protocol-version", "2026-07-28")
                .header("mcp-method", "events/subscribe")
                .body(Body::from(body.to_string()))?,
        )
        .await?;
    let status = resp.status();
    let text = String::from_utf8(resp.into_body().collect().await?.to_bytes().to_vec())?;
    assert_eq!(
        status,
        StatusCode::OK,
        "handler error stays in-band: {text}"
    );
    assert!(text.contains("-32602"), "error code in body: {text}");

    let no_meta = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "events/list",
        "params": {}
    });
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::HOST, "localhost")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header("mcp-protocol-version", "2026-07-28")
                .header("mcp-method", "events/list")
                .body(Body::from(no_meta.to_string()))?,
        )
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "events/* missing _meta is a 400"
    );

    // The stricter stateless contract rejects a request carrying neither the
    // MCP-Protocol-Version header nor per-request `_meta` (legacy fallback).
    let no_header = serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/list",
        "params": {}
    });
    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::HOST, "localhost")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .body(Body::from(no_header.to_string()))?,
        )
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "stateless 2026-07-28 requires the MCP-Protocol-Version header"
    );
    Ok(())
}

/// R13 regression: 2026-07-28 is the only supported version, and its clients
/// send per-request `_meta`. A tools/call with that metadata must succeed
/// through the transport, and tools/list must still be 41 — U1 added no tools.
#[tokio::test]
async fn modern_2026_client_calls_tools_with_meta() -> anyhow::Result<()> {
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
    let tools = client.list_all_tools().await?;
    assert_eq!(tools.len(), 41, "frozen tool surface (R13)");
    let health = client
        .call_tool(rmcp::model::CallToolRequestParams::new(
            "health_check".to_string(),
        ))
        .await?;
    assert!(
        !health.is_error.unwrap_or(false),
        "2026-07-28 tools/call with _meta succeeds: {health:?}"
    );
    client.close().await?;
    server.abort();
    Ok(())
}
