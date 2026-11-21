// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! ADR-0039 D7 (bd-o4bgx): `events/stream` is a SEP-3415 JSON-RPC method. The
//! axum layer in front of rmcp answers the POST with text/event-stream:
//! notifications/events/* routed by `_meta.subscriptionId`, then the JSON-RPC
//! result echoing the request id. Shares the poll cursor and bounded table.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::{
    IntoResponse, Response,
    sse::{Event, Sse},
};
use futures_util::stream::Stream;
use serde_json::{Value, json};
use toron_core::events::principal::TokenStore;
use toron_core::events::store::SubscriptionStore;
use toron_core::events::stream::{StreamModel, StreamMsg, StreamState, journal_then_run};

pub const SUBSCRIPTION_META_KEY: &str = "io.modelcontextprotocol/subscriptionId";
/// Shared with the /mcp router's DefaultBodyLimit: one limit for the layer and
/// for rmcp, so the layer never rejects a request rmcp would have accepted.
pub const MCP_BODY_LIMIT: usize = 2 * 1024 * 1024;

const POLL_MS: u64 = 200;
const HEARTBEAT_MS: u64 = 5000;

#[derive(Clone)]
pub struct StreamDeps {
    pub store: Arc<dyn SubscriptionStore>,
    pub tokens: Arc<dyn TokenStore>,
}

struct StreamHost {
    store: Arc<dyn SubscriptionStore>,
    id: String,
    cursor: u64,
    model: StreamModel,
    pending: VecDeque<Event>,
    last_heartbeat_ms: u64,
    active_sent: bool,
    done: bool,
    result_id: Value,
    /// Granted heartbeat interval, suggested by the client in heartbeatMs.
    heartbeat_ms: u64,
}

impl StreamHost {
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn notification(&self, method: &str, params: Value) -> Event {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        Event::default().event("message").data(body.to_string())
    }

    fn meta(&self) -> Value {
        json!({"_meta": {SUBSCRIPTION_META_KEY: self.id}})
    }

    async fn step(&mut self) {
        let now_ms = Self::now_ms();
        let now_us = now_ms as i64 * 1000;
        let next = journal_then_run(
            &self.model.clone(),
            &StreamMsg::Clock { now_us },
            |effect| {
                tracing::info!("events stream effect journal sub={} {effect:?}", self.id);
            },
        );
        self.model = next;
        if self.model.state == StreamState::Terminated {
            self.terminate();
            return;
        }
        let sub = match self.store.get(&self.id).await {
            Ok(Some(sub)) => sub,
            Ok(None) => {
                self.terminate();
                return;
            }
            Err(e) => {
                self.gap(e);
                return;
            }
        };
        self.model.expires_us = sub.expires_us;
        if sub.state == "terminated" {
            self.terminate();
            return;
        }
        if now_ms >= self.last_heartbeat_ms + self.heartbeat_ms {
            let next = journal_then_run(
                &self.model.clone(),
                &StreamMsg::Heartbeat { now_us },
                |_| {},
            );
            self.model = next;
            self.last_heartbeat_ms = now_ms;
            let mut params = self.meta();
            params["now"] = json!(now_us);
            self.pending
                .push_back(self.notification("notifications/events/heartbeat", params));
        }
        match self.store.poll(&self.id, self.cursor, 32).await {
            Err(e) => self.gap(e),
            Ok(page) => {
                for delivery in page.events {
                    let event_id = delivery.seq.to_string();
                    let next = journal_then_run(
                        &self.model.clone(),
                        &StreamMsg::Fact {
                            event_id: event_id.clone(),
                            payload: delivery.payload.clone(),
                        },
                        |_| {},
                    );
                    self.model = next;
                    self.cursor = delivery.seq;
                    // bd-6zo01 G1: the occurrence's fields are the params
                    // themselves, not nested under "event" (SEP-3415 Final).
                    let occ = toron_core::events::occurrence::occurrence(
                        &delivery.payload,
                        delivery.seq,
                        now_us,
                    );
                    let mut params = self.meta();
                    if let Some(obj) = occ.as_object() {
                        for (k, v) in obj {
                            params[k.clone()] = v.clone();
                        }
                    }
                    self.pending
                        .push_back(self.notification("notifications/events/event", params));
                }
                if page.truncated {
                    // G4: a gap is a fresh active with truncated:true, never a
                    // a spec-compliant active notification.
                    let mut params = self.meta();
                    params["cursor"] = json!(page.cursor.to_string());
                    params["truncated"] = json!(true);
                    params["heartbeatMs"] = json!(self.heartbeat_ms);
                    self.pending
                        .push_back(self.notification("notifications/events/active", params));
                    self.cursor = page.cursor;
                }
            }
        }
    }

    fn gap(&mut self, error: anyhow::Error) {
        // G4: transient per-occurrence failures are a spec
        // notifications/events/error; the stream stays open.
        tracing::warn!("events stream store error: {error}");
        let mut params = self.meta();
        params["error"] = json!({
            "code": -32603,
            "message": "UpstreamError",
            "data": {"reason": "store_unavailable"},
        });
        self.pending
            .push_back(self.notification("notifications/events/error", params));
    }

    fn terminate(&mut self) {
        self.pending
            .push_back(self.notification("notifications/events/terminated", self.meta()));
        let result = json!({
            "jsonrpc": "2.0",
            "id": self.result_id,
            "result": {"resultType": "complete", "_meta": {
                SUBSCRIPTION_META_KEY: self.result_id,
            }},
        });
        self.pending
            .push_back(Event::default().event("message").data(result.to_string()));
        self.done = true;
    }
}

/// Front-door layer on `/mcp`: a POST whose JSON-RPC method is `events/stream`
/// is answered with SSE; every other request passes through to rmcp unchanged.
pub async fn stream_intercept(
    State(state): State<StreamDeps>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MCP_BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    let request_value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return next
                .run(Request::from_parts(parts, axum::body::Body::from(bytes)))
                .await;
        }
    };
    if request_value["method"].as_str() != Some("events/stream") {
        return next
            .run(Request::from_parts(parts, axum::body::Body::from(bytes)))
            .await;
    }
    let authorization = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if crate::events::authorize_with_store(
        authorization,
        StreamHost::now_ms() as i64 * 1000,
        state.tokens.as_ref(),
    )
    .is_err()
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let params = &request_value["params"];
    let subscription_id = params["subscriptionId"].as_str().unwrap_or("").to_string();
    if subscription_id.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let last_event_id = parts
        .headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let cursor = last_event_id
        .or_else(|| {
            params["cursor"]
                .as_str()
                .and_then(|v| v.parse::<u64>().ok())
        })
        .or_else(|| params["cursor"].as_u64())
        .unwrap_or(0);
    let result_id = request_value["id"].clone();
    let heartbeat_ms = params["heartbeatMs"]
        .as_u64()
        .map(|v| v.clamp(1000, 60_000))
        .unwrap_or(HEARTBEAT_MS);
    stream_sse(state, subscription_id, cursor, result_id, heartbeat_ms).into_response()
}

/// Build the SSE response for an authorized `events/stream` JSON-RPC request.
pub fn stream_sse(
    state: StreamDeps,
    subscription_id: String,
    cursor: u64,
    result_id: Value,
    heartbeat_ms: u64,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let host = StreamHost {
        store: state.store,
        id: subscription_id,
        cursor,
        model: StreamModel::new(None),
        pending: VecDeque::new(),
        last_heartbeat_ms: StreamHost::now_ms(),
        active_sent: false,
        done: false,
        result_id,
        heartbeat_ms,
    };
    let stream = futures_util::stream::unfold(host, |mut host| async move {
        loop {
            if let Some(event) = host.pending.pop_front() {
                return Some((Ok(event), host));
            }
            if host.done {
                return None;
            }
            if !host.active_sent {
                host.active_sent = true;
                let mut params = host.meta();
                params["cursor"] = json!(host.cursor.to_string());
                params["truncated"] = json!(false);
                params["heartbeatMs"] = json!(host.heartbeat_ms);
                host.pending
                    .push_back(host.notification("notifications/events/active", params));
                continue;
            }
            tokio::time::sleep(Duration::from_millis(POLL_MS)).await;
            host.step().await;
        }
    });
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}
