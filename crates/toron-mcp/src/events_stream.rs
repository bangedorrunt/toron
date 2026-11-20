// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! ADR-0039 D7 (bd-o4bgx): `events/stream` as one long-lived SSE request per
//! subscription. Shares the poll cursor, the bounded table and its gap rule.
//! Frames: active, event, heartbeat, terminated.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, Sse};
use futures_util::stream::Stream;
use serde::Deserialize;
use toron_core::events::store::SubscriptionStore;
use toron_core::events::stream::{StreamMsg, StreamModel, StreamState, journal_then_run};

const POLL_MS: u64 = 200;
const HEARTBEAT_MS: u64 = 5000;

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    #[serde(rename = "subscriptionId")]
    pub subscription_id: String,
    pub cursor: Option<String>,
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
}

impl StreamHost {
    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn step(&mut self) {
        let now_us = Self::now_ms() as i64 * 1000;
        let model = self.model.clone();
        let next = journal_then_run(&model, &StreamMsg::Clock { now_us }, |effect| {
            tracing::info!("events stream effect journal sub={} {effect:?}", self.id);
        });
        self.model = next;
        if self.model.state == StreamState::Terminated {
            self.pending.push_back(
                Event::default()
                    .event("terminated")
                    .data(r#"{"terminated":true}"#),
            );
            self.done = true;
            return;
        }
        let Ok(Some(sub)) = self.store.get(&self.id).await else {
            self.pending.push_back(
                Event::default()
                    .event("terminated")
                    .data(r#"{"terminated":true}"#),
            );
            self.done = true;
            return;
        };
        self.model.expires_us = sub.expires_us;
        if sub.state == "terminated" {
            self.pending.push_back(
                Event::default()
                    .event("terminated")
                    .data(r#"{"terminated":true}"#),
            );
            self.done = true;
            return;
        }
        if now_us as u64 >= self.last_heartbeat_ms + HEARTBEAT_MS {
            let next = journal_then_run(&self.model.clone(), &StreamMsg::Heartbeat { now_us }, |_| {});
            self.model = next;
            self.last_heartbeat_ms = now_us as u64;
            self.pending.push_back(
                Event::default()
                    .event("heartbeat")
                    .data(format!(r#"{{"now":{now_us}}}"#)),
            );
        }
        if let Ok(page) = self.store.poll(&self.id, self.cursor, 32).await {
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
                self.pending.push_back(
                    Event::default()
                        .event("event")
                        .id(event_id)
                        .data(delivery.payload),
                );
            }
            if page.truncated {
                self.pending.push_back(
                    Event::default()
                        .event("gap")
                        .data(format!(r#"{{"cursor":{}}}"#, page.cursor)),
                );
                self.cursor = page.cursor;
            }
        }
    }
}

/// GET /events/stream?subscriptionId=…&cursor=… with optional Last-Event-ID.
pub async fn events_stream(
    State((store, tokens)): State<(Arc<dyn SubscriptionStore>, Arc<dyn toron_core::events::principal::TokenStore>)>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if crate::events::authorize_with_store(authorization, StreamHost::now_ms() as i64 * 1000, tokens.as_ref()).is_err() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    let cursor = last_event_id
        .or_else(|| query.cursor.as_deref().and_then(|v| v.parse::<u64>().ok()))
        .unwrap_or(0);
    let host = StreamHost {
        store,
        id: query.subscription_id,
        cursor,
        model: StreamModel::new(None),
        pending: VecDeque::new(),
        last_heartbeat_ms: StreamHost::now_ms(),
        active_sent: false,
        done: false,
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
                host.pending.push_back(
                    Event::default()
                        .event("active")
                        .data(r#"{"active":true}"#),
                );
                continue;
            }
            tokio::time::sleep(Duration::from_millis(POLL_MS)).await;
            host.step().await;
        }
    });
    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}
