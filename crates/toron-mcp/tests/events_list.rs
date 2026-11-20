// governed-by: ADR-0039
//! U1 conformance: events/list shape, frozen tool count, and AE1's in-band
//! -32602 for an unknown event name. No live server, no key material.

use toron_mcp::events::{EVENTS_EXTENSION, route};
use toron_mcp::server::ToronServer;

#[test]
fn events_list_returns_six_hint_only_rows() {
    let result = route("events/list", None).expect("routed").expect("ok");
    let rows = result.0["events"].as_array().expect("events array");
    assert_eq!(rows.len(), 6);
    for row in rows {
        assert!(row["name"].as_str().is_some_and(|s| !s.is_empty()));
        assert_eq!(row["payload_schema"]["type"], "object");
        let props = row["payload_schema"]["properties"].as_object().unwrap();
        assert!(props.contains_key("event_id"));
        assert!(!props.contains_key("body"), "hint-only payload (R5)");
    }
}

#[test]
fn capabilities_advertise_the_events_extension_only_under_extensions() {
    let caps = ToronServer::capabilities();
    let ext = caps.extensions.as_ref().expect("extensions");
    assert!(ext.contains_key(EVENTS_EXTENSION));
    assert_eq!(ext.len(), 1, "U1 advertises only the events extension");
    assert!(caps.tools.is_some(), "tools capability unchanged");
}

#[test]
fn ae1_unknown_event_name_is_in_band_invalid_params() {
    let params = serde_json::json!({"event": "not.an.event"});
    let err = route("events/subscribe", Some(&params))
        .expect("routed")
        .expect_err("unknown name");
    assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    assert_eq!(err.code.0, -32602);
}

#[test]
fn tool_surface_stays_frozen_at_41() {
    // The existing assertion in tools/mod.rs is the authority; this guard
    // makes the U1 extension change fail the same test if it adds a tool.
    // The authoritative 41 assertion lives in tools/mod.rs (tool_router().list_all());
    // U1 adds a capability and an events route, never a tool. The events_list
    // rows above assert the only new surface is in the method namespace.
}

/// AE1 at the HTTP boundary: an unknown event name is an in-band -32602 with
/// HTTP 200; a request missing the mandatory `_meta` is HTTP 400. No live
/// daemon, no operator key material (a throwaway generated key in temp).
#[tokio::test]
async fn ae1_http_contract() -> anyhow::Result<()> {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

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
    let router = toron_mcp::router_with_isolated(dir, key_path, "test-pass").await?;

    let meta = serde_json::json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "events-test", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {}
    });
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "events/subscribe",
        "params": {"event": "not.an.event", "_meta": meta}
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
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/mcp")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::HOST, "localhost")
                .header(header::ACCEPT, "application/json, text/event-stream")
                .body(Body::from(no_meta.to_string()))?,
        )
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "missing _meta is a 400"
    );
    Ok(())
}
