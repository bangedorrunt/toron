// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Minimal in-process NIP-01 relay used ONLY by tests (`#[cfg(test)]`).
//!
//! Speaks the real WS frame protocol over `tokio-tungstenite`: accepts a
//! connection, handles `EVENT` (verify + store + reply `OK`), `REQ` (replay
//! matching stored events, send `EOSE`, then forward live events to ALL
//! connections' matching subscriptions via a shared broadcast). Enough to
//! integration-test `RelayBus` end-to-end without external network.

#![cfg(test)]

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::WebSocketStream;

use futures_util::{SinkExt, StreamExt};

use nostr_sdk::nostr::message::client::ClientMessage;
use nostr_sdk::nostr::message::relay::RelayMessage;
use nostr_sdk::nostr::{Event, Filter, JsonUtil, SubscriptionId};

/// In-memory relay state shared across connections.
#[derive(Default)]
struct RelayState {
    events: Vec<Event>,
}

/// A mock relay listening on a random loopback port.
pub struct MockRelay {
    state: Arc<Mutex<RelayState>>,
    url: String,
    handle: tokio::task::JoinHandle<()>,
}

impl MockRelay {
    /// Start the relay; returns a handle bound to `127.0.0.1:<random>`.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let url = format!("ws://{addr}");
        let state: Arc<Mutex<RelayState>> = Arc::new(Mutex::new(RelayState::default()));
        let (live_tx, _) = tokio::sync::broadcast::channel(256);

        let handle = tokio::spawn({
            let state = state.clone();
            let live_tx = live_tx.clone();
            async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let state = state.clone();
                    let live_tx = live_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_conn(stream, state, live_tx).await {
                            eprintln!("mock relay conn error: {e}");
                        }
                    });
                }
            }
        });

        Self { state, url, handle }
    }

    /// The relay's WS URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Number of stored events (diagnostics).
    pub fn stored_count(&self) -> usize {
        self.state.lock().events.len()
    }

    /// Stop the relay task.
    pub fn shutdown(self) {
        self.handle.abort();
    }
}

async fn handle_conn(
    stream: TcpStream,
    state: Arc<Mutex<RelayState>>,
    live_tx: tokio::sync::broadcast::Sender<Event>,
) -> anyhow::Result<()> {
    let ws: WebSocketStream<TcpStream> = tokio_tungstenite::accept_async(stream).await?;
    let (mut sink, mut stream) = ws.split();
    // (subscription_id, filter) active on THIS connection.
    let mut local_subs: Vec<(SubscriptionId, Filter)> = Vec::new();
    let mut live_rx = live_tx.subscribe();

    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(msg) = msg else { break };
                let msg = msg?;
                let WsMessage::Text(text) = msg else { continue };
                let parsed: ClientMessage = match ClientMessage::from_json(&text) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                match parsed {
                    ClientMessage::Event(event) => {
                        let event: Event = event.into_owned();
                        // Verify + dedup by id. Lock only briefly.
                        let accepted = {
                            let mut st = state.lock();
                            if event.verify().is_err() || st.events.iter().any(|e| e.id == event.id) {
                                false
                            } else {
                                st.events.push(event.clone());
                                true
                            }
                        };
                        let ok = RelayMessage::ok(
                            event.id,
                            accepted,
                            if accepted { "stored" } else { "duplicate" },
                        );
                        sink.send(WsMessage::Text(ok.as_json())).await?;
                        if accepted {
                            // Broadcast live to every connection's subscriptions.
                            let _ = live_tx.send(event);
                        }
                    }
                    ClientMessage::Req { subscription_id, filters } => {
                        let sub_id = subscription_id.into_owned();
                        let filter = filters
                            .into_iter()
                            .next()
                            .map(|c| c.into_owned())
                            .unwrap_or_default();
                        // Replay stored matches (clone under lock, no guard across await).
                        let stored: Vec<Event> = state.lock().events.clone();
                        for ev in stored {
                            if filter.match_event(&ev, Default::default()) {
                                let msg = RelayMessage::event(sub_id.clone(), ev);
                                sink.send(WsMessage::Text(msg.as_json())).await?;
                            }
                        }
                        // EOSE.
                        let eose = RelayMessage::eose(sub_id.clone());
                        sink.send(WsMessage::Text(eose.as_json())).await?;
                        local_subs.push((sub_id, filter));
                    }
                    ClientMessage::Close(subscription_id) => {
                        let sub_id = subscription_id.into_owned();
                        local_subs.retain(|(id, _)| id != &sub_id);
                    }
                    _ => {}
                }
            }
            live = live_rx.recv() => {
                match live {
                    Ok(event) => {
                        // Forward to this connection's own matching subscriptions.
                        let subs = local_subs.clone();
                        for (sub_id, filter) in subs {
                            if filter.match_event(&event, Default::default()) {
                                let msg = RelayMessage::event(sub_id, event.clone());
                                sink.send(WsMessage::Text(msg.as_json())).await?;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    }
    Ok(())
}
