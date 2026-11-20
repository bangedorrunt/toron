// governed-by: ADR-0039

//! bd-o4bgx D7: events/stream is a JSON-RPC POST to /mcp answered with SSE.
//! Spare port, throwaway token + sqlite, no live daemon.

use std::sync::Arc;
use std::time::Duration;

use axum::response::IntoResponse;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use toron_core::events::principal::{FileTokenStore, Principal, Scope, TokenStore, mint};
use toron_core::events::store::{SqliteSubscriptionStore, SubscribeSpec, SubscriptionStore};

async fn setup(tag: &str) -> (axum::Router, String, Arc<SqliteSubscriptionStore>) {
    let dir = std::env::temp_dir().join(format!("toron-sse-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("events.sqlite");
    let pool = toron_core::index::schema::connect(db.to_str().unwrap())
        .await
        .unwrap();
    toron_core::index::schema::migrate(&pool).await.unwrap();
    let store = Arc::new(SqliteSubscriptionStore::new(pool));
    let tokens: Arc<dyn TokenStore> = Arc::new(FileTokenStore::at(dir.join("tokens.json")));
    let token = mint(Principal::Operator, Scope::operator(), 1000, tokens.as_ref()).unwrap();

    let app = axum::Router::new()
        .route("/mcp", axum::routing::any(|| async { "fallthrough".into_response() }))
        .layer(axum::middleware::from_fn_with_state(
            toron_mcp::events_stream::StreamDeps {
                store: store.clone(),
                tokens,
            },
            toron_mcp::events_stream::stream_intercept,
        ));
    (app, token, store)
}

async fn post_stream(app: axum::Router, body: &Value, token: Option<&str>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = "POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\n".to_string();
    if let Some(t) = token {
        req.push_str(&format!("Authorization: Bearer {t}\r\n"));
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n{}\r\n", body.to_string().len(), body));
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut reader = BufReader::new(stream);
    let mut out = String::new();
    let mut buf = [0u8; 2048];
    loop {
        match tokio::time::timeout(Duration::from_millis(300), reader.read(&mut buf)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => out.push_str(&String::from_utf8_lossy(&buf[..n])),
            Ok(Err(_)) => break,
        }
    }
    server.abort();
    out
}

async fn subscribe(store: &Arc<SqliteSubscriptionStore>, project: &str) -> String {
    let spec = SubscribeSpec {
        principal: "operator".into(),
        is_pin: false,
        delivery: "poll".into(),
        name: "mail.addressed".into(),
        args: json!({"project": project}),
        ttl_us: None,
        now_us: 1000,
    };
    store.subscribe(&spec).await.unwrap().id
}

#[tokio::test]
async fn a_sep_post_streams_active_then_an_event_with_the_request_id() {
    let (app, token, store) = setup("event").await;
    let id = subscribe(&store, "flywheel").await;
    store
        .append(
            &id,
            &json!({"event_id": "bus-e1", "name": "mail.addressed", "project": "flywheel"}),
            1001,
        )
        .await
        .unwrap();
    let body = json!({
        "jsonrpc": "2.0", "id": 42, "method": "events/stream",
        "params": {"subscriptionId": id}
    });
    let out = post_stream(app, &body, Some(&token)).await;
    assert!(out.contains("text/event-stream"), "{out}");
    assert!(out.contains("notifications/events/active"), "{out}");
    assert!(out.contains("notifications/events/event"), "{out}");
    assert!(out.contains(toron_mcp::events_stream::SUBSCRIPTION_META_KEY), "routed by _meta: {out}");
    assert!(out.contains("bus-e1"), "unique bus event id: {out}");
    assert!(out.contains("bus-e1") || out.contains("notifications/events/event"), "{out}");
}

#[tokio::test]
async fn last_event_id_resumes_without_redelivery() {
    let (app, token, store) = setup("resume").await;
    let id = subscribe(&store, "f").await;
    store.append(&id, &json!({"event_id": "e1", "name": "mail.addressed"}), 1001).await.unwrap();
    let cursor = store.poll(&id, 0, 32).await.unwrap().cursor;
    let body = json!({
        "jsonrpc": "2.0", "id": 7, "method": "events/stream",
        "params": {"subscriptionId": id, "cursor": cursor}
    });
    let out = post_stream(app, &body, Some(&token)).await;
    assert!(!out.contains("notifications/events/event"), "no duplicate: {out}");
    assert!(out.contains("notifications/events/active"), "{out}");
}

#[tokio::test]
async fn an_expired_subscription_terminates() {
    let (app, token, store) = setup("expiry").await;
    let id = subscribe(&store, "f").await;
    store.terminate(&id).await.unwrap();
    let body = json!({
        "jsonrpc": "2.0", "id": 9, "method": "events/stream",
        "params": {"subscriptionId": id}
    });
    let out = post_stream(app, &body, Some(&token)).await;
    assert!(out.contains("notifications/events/terminated"), "{out}");
    assert!(out.contains("\"id\":9"), "{out}");
}

#[tokio::test]
async fn a_missing_bearer_token_is_refused() {
    let (app, _token, store) = setup("auth").await;
    let id = subscribe(&store, "f").await;
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "events/stream",
        "params": {"subscriptionId": id}
    });
    let out = post_stream(app, &body, None).await;
    assert!(out.contains("401"), "{out}");
}

#[tokio::test]
async fn other_methods_fall_through_to_rmcp() {
    let (app, token, _store) = setup("fallthrough").await;
    let body = json!({"jsonrpc": "2.0", "id": 3, "method": "ping"});
    let out = post_stream(app, &body, Some(&token)).await;
    assert!(out.contains("fallthrough"), "a non-stream method must not be hijacked: {out}");
}
