// governed-by: ADR-0039

//! bd-6bgrd U2: the single events/* auth seam (D6 matrix, R13 tools token-less).
//! No live daemon, no operator key, hermetic in-memory token store.

use toron_core::events::principal::{
    FileTokenStore, Principal, Scope, TokenRecord, TokenStore, hash_token,
};

static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn store_with(record: TokenRecord) -> FileTokenStore {
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "toron-events-auth-{}-{}.json",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_file(&path);
    let store = FileTokenStore::at(path);
    store.insert(&record).expect("insert token");
    store
}

fn record(principal: Principal, scope: Scope, revoked: bool) -> TokenRecord {
    TokenRecord {
        hash: hash_token("a-valid-token"),
        principal,
        scope,
        created_us: 1000,
        revoked,
    }
}

#[test]
fn a_valid_pin_token_authorizes() {
    let store = store_with(record(
        Principal::Pin {
            pin: "AmberProbe".into(),
            project: "flywheel".into(),
        },
        Scope::for_pin("AmberProbe", "flywheel"),
        false,
    ));
    assert_eq!(
        toron_mcp::events::authorize_with_store(Some("Bearer a-valid-token"), 1000, &store)
            .unwrap(),
        Principal::Pin {
            pin: "AmberProbe".into(),
            project: "flywheel".into()
        }
    );
}

#[test]
fn an_operator_token_authorizes() {
    let store = store_with(record(Principal::Operator, Scope::operator(), false));
    assert_eq!(
        toron_mcp::events::authorize_with_store(Some("Bearer a-valid-token"), 1000, &store)
            .unwrap(),
        Principal::Operator
    );
}

#[test]
fn a_pin_token_cannot_reach_operator_only_verbs_but_operator_can() {
    let pin = Principal::Pin {
        pin: "P".into(),
        project: "f".into(),
    };
    for params in [
        serde_json::json!({"name": "mail.addressed", "arguments": {"project": "other"}}),
        serde_json::json!({"name": "mail.addressed", "arguments": {"full_payload": true}}),
    ] {
        let err = toron_mcp::events::route("events/subscribe", Some(&params), &pin)
            .expect("routed")
            .expect_err("a pin token cannot escalate scope");
        assert_eq!(err.code, rmcp::model::ErrorCode(-32024));
        assert!(err.message.contains("operator"), "{err:?}");
        // The operator gets the normal not-implemented error, not an auth one.
        let out = toron_mcp::events::route("events/subscribe", Some(&params), &Principal::Operator)
            .expect("routed");
        assert!(out.is_err());
        assert!(!out.unwrap_err().message.contains("operator"));
    }
}

#[test]
fn missing_unknown_and_revoked_tokens_are_in_band_errors() {
    let store = store_with(record(Principal::Operator, Scope::operator(), true));
    for header in [
        None,
        Some("Bearer nope"),
        Some("Basic xyz"),
        Some("Bearer a-valid-token"),
    ] {
        let err = toron_mcp::events::authorize_with_store(header, 1000, &store)
            .expect_err("must refuse a missing/unknown/revoked token");
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        assert_eq!(err.code.0, -32602, "in-band error, never an HTTP 401");
        assert!(err.message.contains("unauthorized"), "{err:?}");
    }
}

#[tokio::test]
async fn subscribe_append_then_poll_returns_the_hint_after_the_cursor() {
    use toron_core::events::store::{SqliteSubscriptionStore, SubscriptionStore};
    use toron_mcp::events::route_events;

    let path = std::env::temp_dir().join(format!(
        "toron-events-poll-{}-{}.sqlite",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_file(&path);
    let pool = toron_core::index::schema::connect(path.to_str().unwrap())
        .await
        .unwrap();
    toron_core::index::schema::migrate(&pool).await.unwrap();
    let store = SqliteSubscriptionStore::new(pool);
    let principal = Principal::Operator;
    let params = serde_json::json!({
        "name": "mail.addressed",
        "arguments": {"project": "flywheel"}
    });

    let sub = route_events(
        "events/subscribe",
        Some(&params),
        &principal,
        Some(&store),
        1000,
    )
    .await
    .expect("routed")
    .expect("subscribed");
    let id = sub.0["id"].as_str().expect("subscription id").to_string();

    store
        .append(
            &id,
            &serde_json::json!({"event_id": "e1", "name": "mail.addressed", "project": "flywheel"}),
            1001,
        )
        .await
        .unwrap();
    let poll_params = serde_json::json!({"subscriptionId": id, "cursor": "0"});
    let page = route_events(
        "events/poll",
        Some(&poll_params),
        &principal,
        Some(&store),
        1001,
    )
    .await
    .expect("routed")
    .expect("poll ok");
    let events = page.0["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1, "one hint on poll");
    assert_eq!(events[0]["name"], "mail.addressed");
    assert!(events[0].get("body").is_none(), "hint-only payload (R5)");
    assert_eq!(page.0["nextPollMs"], 5000);
    assert_eq!(page.0["hasMore"], false);
    assert_eq!(page.0["truncated"], false);

    let next = serde_json::json!({"subscriptionId": id, "cursor": page.0["cursor"]});
    let again = route_events("events/poll", Some(&next), &principal, Some(&store), 1002)
        .await
        .expect("routed")
        .expect("poll ok");
    assert_eq!(
        again.0["events"].as_array().unwrap().len(),
        0,
        "cursor advanced"
    );
}

#[test]
fn tools_stay_token_less_r13() {
    // The auth seam is events-only: `route` still declines tools/list, and the
    // tool handlers never pass through authorize.
    assert!(toron_mcp::events::route("tools/list", None, &Principal::Operator).is_none());
    assert!(toron_mcp::events::route("tools/call", None, &Principal::Operator).is_none());
}
