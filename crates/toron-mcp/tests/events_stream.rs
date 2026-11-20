// governed-by: ADR-0039

//! bd-o4bgx D7: events/stream SSE shares the poll cursor and bounded table.
//! Spare port (127.0.0.1:0), throwaway token + sqlite, no live daemon.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
    let token_path = dir.join("tokens.json");
    let tokens: Arc<dyn TokenStore> = Arc::new(FileTokenStore::at(token_path));
    let token = mint(Principal::Operator, Scope::operator(), 1000, tokens.as_ref()).unwrap();

    let app = axum::Router::new()
        .route(
            "/events/stream",
            axum::routing::get(toron_mcp::events_stream::events_stream),
        )
        .with_state((
            store.clone() as Arc<dyn SubscriptionStore>,
            tokens,
        ));
    (app, token, store)
}

async fn read_frames(app: axum::Router, query: &str, token: &str, last_id: Option<&str>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut req = format!(
        "GET /events/stream?{query} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nAccept: text/event-stream\r\n"
    );
    if let Some(id) = last_id {
        req.push_str(&format!("Last-Event-ID: {id}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();

    let mut reader = BufReader::new(stream);
    let mut out = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(4000);
    while tokio::time::Instant::now() < deadline {
        let mut line = String::new();
        let read = tokio::time::timeout(Duration::from_millis(1000), reader.read_line(&mut line)).await;
        match read {
            Ok(Ok(0)) => break,
            Ok(Ok(_)) => {
                out.push_str(&line);
                if out.contains("event: event") || out.contains("event: terminated") {
                    // read a few more lines to capture the data line
                    for _ in 0..3 {
                        let mut extra = String::new();
                        let _ = tokio::time::timeout(Duration::from_millis(100), reader.read_line(&mut extra)).await;
                        out.push_str(&extra);
                    }
                    break;
                }
            }
            _ => break,
        }
    }
    server.abort();
    out
}

#[tokio::test]
async fn an_addressed_mail_streams_one_event_frame_with_a_unique_id() {
    let (app, token, store) = setup("event").await;
    let spec = SubscribeSpec {
        principal: "operator".into(),
        is_pin: false,
        delivery: "poll".into(),
        name: "mail.addressed".into(),
        args: json!({"project": "flywheel"}),
        ttl_us: None,
        now_us: 1000,
    };
    let sub = store.subscribe(&spec).await.unwrap();
    store
        .append(
            &sub.id,
            &json!({"event_id": "bus-e1", "name": "mail.addressed", "project": "flywheel"}),
            1001,
        )
        .await
        .unwrap();

    let body = read_frames(app, &format!("subscriptionId={}", sub.id), &token, None).await;
    assert!(body.contains("event: active"), "{body}");
    assert!(body.contains("event: event"), "{body}");
    assert!(body.contains("bus-e1"), "the bus event's unique id (D8 dedupe): {body}");
    assert!(body.contains("mail.addressed"), "{body}");
}

#[tokio::test]
async fn reconnect_with_last_event_id_does_not_redeliver() {
    let (app, token, store) = setup("resume").await;
    let spec = SubscribeSpec {
        principal: "operator".into(),
        is_pin: false,
        delivery: "poll".into(),
        name: "mail.addressed".into(),
        args: json!({"project": "f"}),
        ttl_us: None,
        now_us: 1000,
    };
    let sub = store.subscribe(&spec).await.unwrap();
    store
        .append(&sub.id, &json!({"event_id": "e1", "name": "mail.addressed"}), 1001)
        .await
        .unwrap();
    let page = store.poll(&sub.id, 0, 32).await.unwrap();
    let cursor = page.cursor;

    let body = read_frames(
        app,
        &format!("subscriptionId={}", sub.id),
        &token,
        Some(&cursor.to_string()),
    )
    .await;
    assert!(!body.contains("event: event"), "no duplicate after resume: {body}");
    assert!(body.contains("event: active"), "{body}");
}

#[tokio::test]
async fn an_expired_subscription_terminates_the_stream() {
    let (app, token, store) = setup("expiry").await;
    let spec = SubscribeSpec {
        principal: "operator".into(),
        is_pin: false,
        delivery: "poll".into(),
        name: "mail.addressed".into(),
        args: json!({"project": "f"}),
        ttl_us: Some(100),
        now_us: 1000,
    };
    let sub = store.subscribe(&spec).await.unwrap();
    store.terminate(&sub.id).await.unwrap();

    let body = read_frames(app, &format!("subscriptionId={}", sub.id), &token, None).await;
    assert!(body.contains("event: terminated"), "{body}");
}

#[tokio::test]
async fn a_missing_bearer_token_is_refused() {
    let (app, _token, _store) = setup("auth").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /events/stream?subscriptionId=x HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut first = String::new();
    tokio::time::timeout(Duration::from_millis(1000), reader.read_line(&mut first))
        .await
        .unwrap()
        .unwrap();
    assert!(first.contains("401"), "unauthorized: {first}");
    server.abort();
}
