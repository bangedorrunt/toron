// governed-by: ADR-0039
//! bd-2ghz8: `GET /signal` — count-only view for one participant. The secret
//! arrives ONLY in `Authorization: Bearer`; it is never a URL path/query
//! segment and never logged. The response carries counts and expiry — no ids,
//! no bodies, no hints.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use toron_core::events::signal::{
    SignalStore, hash_secret, mint_signal, resolve_signal, revoke_signal,
};
use toron_core::events::store::SubscriptionStore;

/// toron's own extension key for the count-only signal methods (bd-2ghz8,
/// bd-6zo01 G12/#1446). Deliberately NOT under the MCP Events key — signal/*
/// is not part of that extension and rmcp 3.11 rejects unadvertised methods.
pub const SIGNAL_EXTENSION: &str = "dev.toron/signal";

#[derive(Clone)]
pub struct SignalDeps {
    pub links: Arc<dyn SignalStore>,
    pub events: Arc<dyn SubscriptionStore>,
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

pub async fn signal_get(State(deps): State<SignalDeps>, headers: HeaderMap) -> Response {
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let now = now_us();
    let Some(participant) = resolve_signal(auth, now, deps.links.as_ref()) else {
        return (StatusCode::UNAUTHORIZED, "invalid or expired signal link").into_response();
    };
    let counts = match deps.events.counts_for(&participant, now).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("signal counts failed: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "counts unavailable").into_response();
        }
    };
    // The link's expiry is the signal's own TTL; the store has no ids/bodies.
    let expires_at = deps
        .links
        .get(&{
            let raw = auth.unwrap_or("").strip_prefix("Bearer ").unwrap_or("");
            hash_secret(raw.trim())
        })
        .ok()
        .flatten()
        .map(|l| l.expires_us)
        .unwrap_or(0);
    // Wire key is `expiresAt` (SEP-3415 / events source convention), not the
    // Rust field name. Never echo the secret or any event identity.
    Json(json!({
        "pending": counts.pending,
        "urgent": counts.urgent,
        "waking": counts.waking,
        "cursor": counts.cursor,
        "expiresAt": expires_at,
    }))
    .into_response()
}

/// Route a `signal/*` custom request (bd-2ghz8). The caller is already
/// authenticated by its events token; the participant comes from that
/// principal, never from client-supplied arguments. The secret is returned
/// ONCE and never logged.
pub fn route_signal(
    method: &str,
    participant: &str,
    store: &dyn SignalStore,
    now_us: i64,
) -> Option<Result<rmcp::model::CustomResult, rmcp::ErrorData>> {
    let err = |msg: &str| {
        Some(Err(rmcp::ErrorData::new(
            rmcp::model::ErrorCode::INVALID_PARAMS,
            msg.to_string(),
            None,
        )))
    };
    match method {
        "signal/create" => match mint_signal(participant, now_us, store) {
            Ok(secret) => Some(Ok(rmcp::model::CustomResult(json!({
                "secret": secret,
                "expiresAt": now_us.saturating_add(
                    toron_core::events::signal::SIGNAL_TTL_US
                ),
            })))),
            Err(e) => err(&format!("signal create failed: {e}")),
        },
        "signal/revoke" => match revoke_signal(participant, store) {
            Ok(n) => Some(Ok(rmcp::model::CustomResult(json!({ "revoked": n })))),
            Err(e) => err(&format!("signal revoke failed: {e}")),
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use toron_core::events::signal::{SignalLink, mint_signal};
    use toron_core::events::store::{Delivery, Page, SubscribeSpec, Subscription};
    use tower::ServiceExt;

    #[derive(Default)]
    struct FakeLinks {
        rows: Mutex<Vec<SignalLink>>,
    }
    impl SignalStore for FakeLinks {
        fn get(&self, hash: &str) -> anyhow::Result<Option<SignalLink>> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .iter()
                .find(|r| r.hash == hash)
                .cloned())
        }
        fn rotate(&self, participant: &str, link: &SignalLink) -> anyhow::Result<()> {
            let mut rows = self.rows.lock().unwrap();
            for r in rows.iter_mut().filter(|r| r.participant == participant) {
                r.revoked = true;
            }
            rows.push(link.clone());
            Ok(())
        }
        fn revoke(&self, participant: &str) -> anyhow::Result<usize> {
            let mut rows = self.rows.lock().unwrap();
            let mut n = 0;
            for r in rows
                .iter_mut()
                .filter(|r| r.participant == participant && !r.revoked)
            {
                r.revoked = true;
                n += 1;
            }
            Ok(n)
        }
    }

    #[derive(Default)]
    struct FakeEvents {
        subs: Mutex<Vec<(String, Subscription)>>,
    }
    #[async_trait::async_trait]
    impl SubscriptionStore for FakeEvents {
        async fn subscribe(&self, spec: &SubscribeSpec) -> anyhow::Result<Subscription> {
            let sub = Subscription {
                id: format!("s{}", self.subs.lock().unwrap().len()),
                principal: spec.principal.clone(),
                delivery: spec.delivery.clone(),
                name: spec.name.clone(),
                args_hash: String::new(),
                cursor: 0,
                created_us: spec.now_us,
                expires_us: None,
                state: "active".into(),
            };
            self.subs
                .lock()
                .unwrap()
                .push((sub.id.clone(), sub.clone()));
            Ok(sub)
        }
        async fn get(&self, id: &str) -> anyhow::Result<Option<Subscription>> {
            Ok(self
                .subs
                .lock()
                .unwrap()
                .iter()
                .find(|(k, _)| k == id)
                .map(|(_, s)| s.clone()))
        }
        async fn list_active(&self) -> anyhow::Result<Vec<Subscription>> {
            Ok(self
                .subs
                .lock()
                .unwrap()
                .iter()
                .map(|(_, s)| s.clone())
                .collect())
        }
        async fn append(
            &self,
            _id: &str,
            _payload: &serde_json::Value,
            _now_us: i64,
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn poll(&self, _id: &str, _after: u64, _limit: u32) -> anyhow::Result<Page> {
            Ok(Page {
                events: vec![Delivery {
                    seq: 1,
                    payload: "secret".into(),
                    created_us: 1_000,
                }],
                cursor: 1,
                has_more: false,
                truncated: false,
            })
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

    fn router(links: Arc<dyn SignalStore>, events: Arc<dyn SubscriptionStore>) -> axum::Router {
        axum::Router::new()
            .route("/signal", axum::routing::get(signal_get))
            .with_state(SignalDeps { links, events })
    }

    #[tokio::test]
    async fn a_signal_link_returns_counts_only_never_ids_or_bodies() {
        let links = Arc::new(FakeLinks::default());
        let secret = mint_signal("SlateHarbor", now_us(), links.as_ref()).unwrap();
        let events = Arc::new(FakeEvents::default());
        // The store's counts_for is the default method; feed it via poll/sub.
        events
            .subscribe(&SubscribeSpec {
                principal: "SlateHarbor".into(),
                is_pin: true,
                delivery: "poll".into(),
                name: "mail.addressed".into(),
                args: json!({}),
                ttl_us: None,
                now_us: 1_000,
            })
            .await
            .unwrap();
        let app = router(links, events);
        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/signal")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {secret}"),
                    )
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("pending").is_some() && v.get("urgent").is_some());
        assert!(
            v.get("waking").is_some() && v.get("cursor").is_some() && v.get("expiresAt").is_some()
        );
        assert!(v.get("event_id").is_none() && v.get("body").is_none() && v.get("id").is_none());
        assert!(
            !String::from_utf8_lossy(&body).contains("secret"),
            "the secret is never echoed"
        );
    }

    #[tokio::test]
    async fn missing_or_expired_or_revoked_link_refuses() {
        let links = Arc::new(FakeLinks::default());
        let events = Arc::new(FakeEvents::default());
        let app = router(links.clone(), events);
        let req = |auth: Option<&str>| {
            let mut b = axum::http::Request::builder().uri("/signal");
            if let Some(a) = auth {
                b = b.header(axum::http::header::AUTHORIZATION, a);
            }
            b.body(axum::body::Body::empty()).unwrap()
        };
        assert_eq!(
            app.clone().oneshot(req(None)).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            app.clone()
                .oneshot(req(Some("Bearer nope")))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let secret = mint_signal("SlateHarbor", now_us(), links.as_ref()).unwrap();
        assert_eq!(
            app.clone()
                .oneshot(req(Some(&format!("Bearer {secret}"))))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        toron_core::events::signal::revoke_signal("SlateHarbor", links.as_ref()).unwrap();
        assert_eq!(
            app.oneshot(req(Some(&format!("Bearer {secret}"))))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn signal_create_returns_the_secret_once_and_revoke_ends_it() {
        let links = FakeLinks::default();
        let now = now_us();
        let created = route_signal("signal/create", "SlateHarbor", &links, now)
            .unwrap()
            .unwrap();
        let v: serde_json::Value = created.0;
        let secret = v["secret"].as_str().expect("secret shown once").to_string();
        assert_eq!(
            v["expiresAt"].as_i64(),
            Some(now + toron_core::events::signal::SIGNAL_TTL_US)
        );
        assert_eq!(
            resolve_signal(Some(&format!("Bearer {secret}")), now + 1, &links).as_deref(),
            Some("SlateHarbor")
        );
        let revoked = route_signal("signal/revoke", "SlateHarbor", &links, now)
            .unwrap()
            .unwrap();
        let rv: serde_json::Value = revoked.0;
        assert_eq!(rv["revoked"], 1);
        assert_eq!(
            resolve_signal(Some(&format!("Bearer {secret}")), now + 1, &links),
            None
        );
        assert!(route_signal("signal/unknown", "SlateHarbor", &links, now).is_none());
    }
}
