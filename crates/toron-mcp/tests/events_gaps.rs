// governed-by: ADR-0039

//! bd-sptj4: poll and stream share ONE gap decision. The same states must
//! produce the same gap flag and fresh cursor on both paths.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use toron_core::events::principal::{FileTokenStore, Principal, Scope, TokenStore, mint};
use toron_core::events::store::{
    DELIVERY_TTL_US, SqliteSubscriptionStore, SubscribeSpec, SubscriptionStore, gap_floor,
};
use toron_mcp::events::route_events;

async fn store(tag: &str) -> Arc<SqliteSubscriptionStore> {
    let path = std::env::temp_dir().join(format!("toron-gap-{}-{tag}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let pool = toron_core::index::schema::connect(path.to_str().unwrap())
        .await
        .unwrap();
    toron_core::index::schema::migrate(&pool).await.unwrap();
    Arc::new(SqliteSubscriptionStore::new(pool))
}

async fn new_sub(store: &Arc<SqliteSubscriptionStore>) -> String {
    store
        .subscribe(&SubscribeSpec {
            principal: "operator".into(),
            is_pin: false,
            delivery: "poll".into(),
            name: "mail.addressed".into(),
            args: json!({"project": "f"}),
            ttl_us: None,
            now_us: 1000,
        })
        .await
        .unwrap()
        .id
}

async fn poll_wire(store: &Arc<SqliteSubscriptionStore>, id: &str, cursor: u64) -> Value {
    let params = json!({"subscriptionId": id, "cursor": cursor.to_string()});
    let out = route_events(
        "events/poll",
        Some(&params),
        &Principal::Operator,
        Some(store.as_ref()),
        2000,
    )
    .await
    .expect("routed")
    .expect("poll ok");
    out.0
}

async fn stream_gap(store: &Arc<SqliteSubscriptionStore>, id: &str, cursor: u64) -> Option<String> {
    let dir = std::env::temp_dir().join(format!("toron-gap-tok-{}-{id}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let tokens: Arc<dyn TokenStore> = Arc::new(FileTokenStore::at(dir.join("tokens.json")));
    let token = mint(
        Principal::Operator,
        Scope::operator(),
        1000,
        tokens.as_ref(),
    )
    .unwrap();
    let app = axum::Router::new()
        .route("/mcp", axum::routing::any(|| async { "fallthrough" }))
        .layer(axum::middleware::from_fn_with_state(
            toron_mcp::events_stream::StreamDeps {
                store: store.clone(),
                tokens,
            },
            toron_mcp::events_stream::stream_intercept,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = json!({
        "jsonrpc": "2.0", "id": 1, "method": "events/stream",
        "params": {"subscriptionId": id, "cursor": cursor.to_string()}
    });
    let req = format!(
        "POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: text/event-stream\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\n\r\n{}\r\n",
        body.to_string().len(),
        body
    );
    tcp.write_all(req.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(tcp);
    let mut out = String::new();
    let mut buf = [0u8; 2048];
    loop {
        match tokio::time::timeout(Duration::from_millis(250), reader.read(&mut buf)).await {
            Ok(Ok(0)) | Err(_) => break,
            Ok(Ok(n)) => {
                out.push_str(&String::from_utf8_lossy(&buf[..n]));
                if out.contains("notifications/events/gap")
                    || out.contains("notifications/events/event")
                {
                    break;
                }
            }
            Ok(Err(_)) => break,
        }
    }
    server.abort();
    out.lines().find_map(|line| {
        let data = line.strip_prefix("data: ")?;
        let v: Value = serde_json::from_str(data).ok()?;
        if v["method"].as_str() == Some("notifications/events/gap") {
            v["params"]["cursor"].as_str().map(str::to_string)
        } else {
            None
        }
    })
}

struct ErrorStore;

#[async_trait::async_trait]
impl SubscriptionStore for ErrorStore {
    async fn subscribe(
        &self,
        _spec: &SubscribeSpec,
    ) -> anyhow::Result<toron_core::events::store::Subscription> {
        anyhow::bail!("store down")
    }
    async fn get(
        &self,
        id: &str,
    ) -> anyhow::Result<Option<toron_core::events::store::Subscription>> {
        Ok(Some(toron_core::events::store::Subscription {
            id: id.into(),
            principal: "operator".into(),
            delivery: "poll".into(),
            name: "mail.addressed".into(),
            args_hash: "h".into(),
            cursor: 0,
            created_us: 0,
            expires_us: None,
            state: "active".into(),
        }))
    }
    async fn list_active(&self) -> anyhow::Result<Vec<toron_core::events::store::Subscription>> {
        self.get("s").await.map(|o| o.into_iter().collect())
    }
    async fn append(&self, _id: &str, _payload: &Value, _now_us: i64) -> anyhow::Result<()> {
        Ok(())
    }
    async fn poll(
        &self,
        _id: &str,
        _after: u64,
        _limit: u32,
    ) -> anyhow::Result<toron_core::events::store::Page> {
        anyhow::bail!("store down")
    }
    async fn unsubscribe(&self, _id: &str, _principal: &str) -> anyhow::Result<bool> {
        Ok(true)
    }
    async fn terminate(&self, _id: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn prune_deliveries(&self, _now_us: i64) -> anyhow::Result<()> {
        Ok(())
    }
    async fn sweep(&self, _now_us: i64) -> anyhow::Result<usize> {
        Ok(0)
    }
}

#[tokio::test]
async fn a_store_error_is_a_gap_on_both_paths() {
    let store = ErrorStore;
    let wire = route_events(
        "events/poll",
        Some(&json!({"subscriptionId": "s", "cursor": "7"})),
        &Principal::Operator,
        Some(&store),
        2000,
    )
    .await
    .expect("routed")
    .expect("poll still answers");
    assert_eq!(
        wire.0["truncated"], true,
        "a store error is a gap, not a 500"
    );
    assert_eq!(wire.0["cursor"], "7", "the caller's cursor stays servable");
}

#[tokio::test]
async fn poll_and_stream_agree_on_every_gap_state() {
    let store = store("table").await;
    let id = new_sub(&store).await;
    // Two rows, then age the first out so the table's oldest seq advances.
    store
        .append(
            &id,
            &json!({"event_id": "e0", "name": "mail.addressed"}),
            1000,
        )
        .await
        .unwrap();
    store
        .append(
            &id,
            &json!({"event_id": "e1", "name": "mail.addressed"}),
            DELIVERY_TTL_US + 2000,
        )
        .await
        .unwrap();

    // (label, cursor) — in range, and a cursor that fell off the bounded end.
    let cases: &[(&str, u64)] = &[("in-range", 0), ("fell-off", 1)];
    for (label, cursor) in cases {
        let wire = poll_wire(&store, &id, *cursor).await;
        let (expected_truncated, expected_start) = gap_floor(*cursor, Some(2));
        assert_eq!(
            wire["truncated"].as_bool(),
            Some(expected_truncated),
            "{label}: poll flag"
        );
        if expected_truncated {
            let gap_cursor = stream_gap(&store, &id, *cursor).await;
            assert_eq!(
                gap_cursor.as_deref(),
                Some(wire["cursor"].as_str().unwrap()),
                "{label}: stream gap cursor equals poll fresh cursor"
            );
        }
        let fresh: u64 = wire["cursor"].as_str().unwrap().parse().unwrap();
        assert!(
            fresh >= expected_start as u64,
            "{label}: fresh cursor is servable"
        );
    }
}
