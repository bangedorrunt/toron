use anyhow::Result;
use asupersync::Outcome;
use asupersync::net::TcpListener;
use asupersync::net::websocket::{WebSocketAcceptor, Message as WsMessage};
use asupersync_macros::scope;
use std::sync::Arc;
use crate::protocol::Message as ToronMessage;
use resonate_sdk::prelude::Resonate;
use async_trait::async_trait;

/// A generic interface for a transport mechanism (e.g., WebSocket, QUIC).
#[async_trait]
pub trait Transport: Send + Sync {
    /// Starts the transport listener.
    async fn start(&self, cx: &asupersync::Cx) -> Outcome<(), anyhow::Error>;
    
    /// Stops the transport.
    fn stop(&self) -> Result<()>;
}

/// Shared state for the WebSocket transport.
#[derive(Clone)]
pub struct WsState {
    pub resonate: Arc<Resonate>,
    pub psk: String,
    pub hub_tx: asupersync::channel::Sender<ToronMessage>,
    pub sessions: Arc<parking_lot::RwLock<std::collections::HashMap<uuid::Uuid, asupersync::channel::Sender<ToronMessage>>>>,
}

/// WebSocket transport implementation.
pub struct WsTransport {
    port: u16,
    state: WsState,
}

impl WsTransport {
    pub fn new(
        port: u16, 
        psk: &str, 
        resonate: Arc<Resonate>, 
        hub_tx: asupersync::channel::Sender<ToronMessage>,
        sessions: Arc<parking_lot::RwLock<std::collections::HashMap<uuid::Uuid, asupersync::channel::Sender<ToronMessage>>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            port,
            state: WsState { 
                resonate,
                psk: psk.to_string(),
                hub_tx,
                sessions,
            },
        })
    }
}

#[async_trait]
impl Transport for WsTransport {
    async fn start(&self, cx: &asupersync::Cx) -> Outcome<(), anyhow::Error> {
        log::info!("Starting WebSocket transport on port {}", self.port);
        
        let ws_state = self.state.clone();
        let port = self.port;

        let mut rt_state = asupersync::runtime::RuntimeState::default();
        
        // Use scope! to bind rt_state to __state and cx to __cx for spawn!
        let res: Outcome<(), anyhow::Error> = scope!(cx, state: &mut rt_state, {
            match TcpListener::bind(format!("0.0.0.0:{}", port)).await {
                Ok(listener) => {
                    log::info!("WebSocket listener bound to 0.0.0.0:{}", port);
                    
                    loop {
                        // Check for cancellation
                        let _ = __cx.checkpoint();

                        match listener.accept().await {
                            Ok((mut stream, addr)) => {
                                log::info!("New connection from {}", addr);
                                let res_sdk = ws_state.resonate.clone();
                                let expected_psk = ws_state.psk.clone();
                                let hub_tx = ws_state.hub_tx.clone();
                                let sessions = ws_state.sessions.clone();
                                
                                // Spawn handler task in the scope
                                let _ = asupersync_macros::spawn!(move |session_cx| async move {
                                    log::debug!("Handling agent session for {}", addr);
                                    
                                    let mut buf = [0u8; 4096];
                                    use asupersync::io::AsyncReadExt;
                                    
                                    // 1. Read HTTP handshake
                                    let n = match stream.read(&mut buf).await {
                                        Ok(n) if n > 0 => n,
                                        _ => return,
                                    };

                                    // Verify PSK in handshake headers
                                    let handshake_str = String::from_utf8_lossy(&buf[..n]);
                                    let mut psk_valid = false;
                                    for line in handshake_str.lines() {
                                        if line.to_lowercase().starts_with("x-toron-psk:") {
                                            if let Some(provided_psk) = line.split(':').nth(1) {
                                                if provided_psk.trim() == expected_psk {
                                                    psk_valid = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    if !psk_valid {
                                        log::warn!("Unauthorized connection attempt from {} (invalid PSK)", addr);
                                        return;
                                    }

                                    // 2. Upgrade to WebSocket
                                    let acceptor = WebSocketAcceptor::new();
                                    match acceptor.accept(&session_cx, &buf[..n], stream).await {
                                        Ok(mut ws) => {
                                            log::info!("Agent {} upgraded to WebSocket", addr);

                                            let session_id = uuid::Uuid::new_v4();
                                            let (tx, mut rx) = asupersync::channel::channel(1024);
                                            
                                            // Register session
                                            sessions.write().insert(session_id, tx);
                                            
                                            let mut rt_state = asupersync::runtime::RuntimeState::default();
                                            let _: Outcome<(), anyhow::Error> = scope!(&session_cx, state: &mut rt_state, {
                                                let hub_tx_c = hub_tx.clone();
                                                let res_sdk_c = res_sdk.clone();
                                                
                                                let _ = asupersync_macros::spawn!(move |c_cx| async move {
                                                    loop {
                                                        let _ = c_cx.checkpoint();
                                                        
                                                        // 1. Hub -> Agent
                                                        if let Ok(msg) = rx.try_recv() {
                                                            if let Ok(json) = serde_json::to_string(&msg) {
                                                                let _ = ws.send(&c_cx, WsMessage::Text(json)).await;
                                                            }
                                                        }
                                                        
                                                        // 2. Agent -> Hub
                                                        match ws.recv(&c_cx).await {
                                                            Ok(Some(ws_msg)) => {
                                                                match ws_msg {
                                                                    WsMessage::Text(text) => {
                                                                        if let Ok(toron_msg) = serde_json::from_str::<ToronMessage>(&text) {
                                                                            let _ = res_sdk_c.run("process-msg", crate::durability::workflow::process_topic_message, toron_msg.clone()).spawn().await;
                                                                            let _ = hub_tx_c.send(&c_cx, toron_msg).await;
                                                                        }
                                                                    }
                                                                    WsMessage::Binary(bin) => {
                                                                        if let Ok(toron_msg) = serde_json::from_slice::<ToronMessage>(&bin) {
                                                                            let _ = res_sdk_c.run("process-msg", crate::durability::workflow::process_topic_message, toron_msg.clone()).spawn().await;
                                                                            let _ = hub_tx_c.send(&c_cx, toron_msg).await;
                                                                        }
                                                                    }
                                                                    WsMessage::Close(_) => break,
                                                                    _ => {}
                                                                }
                                                            }
                                                            Ok(None) => break,
                                                            Err(_) => break,
                                                        }
                                                    }
                                                });
                                                Outcome::ok(())
                                            });

                                            // Unregister session
                                            sessions.write().remove(&session_id);
                                            log::info!("Agent {} (session {}) disconnected", addr, session_id);
                                        }
                                        Err(e) => {
                                            log::error!("WS handshake failed for {}: {:?}", addr, e);
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                log::error!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to bind WS port: {}", e);
                    Outcome::err(anyhow::anyhow!("Bind failed: {}", e))
                }
            }
        });
        
        res
    }

    fn stop(&self) -> Result<()> {
        log::info!("Stopping WebSocket transport.");
        Ok(())
    }
}

/// QUIC transport implementation.
pub struct QuicTransport {
    #[allow(dead_code)]
    port: u16,
    #[allow(dead_code)]
    state: WsState,
}

impl QuicTransport {
    pub fn new(port: u16, psk: &str, resonate: Arc<Resonate>, hub_tx: asupersync::channel::Sender<ToronMessage>, sessions: Arc<parking_lot::RwLock<std::collections::HashMap<uuid::Uuid, asupersync::channel::Sender<ToronMessage>>>>) -> Arc<Self> {
        Arc::new(Self { 
            port,
            state: WsState {
                resonate,
                psk: psk.to_string(),
                hub_tx,
                sessions,
            },
        })
    }
}

#[async_trait]
impl Transport for QuicTransport {
    async fn start(&self, cx: &asupersync::Cx) -> Outcome<(), anyhow::Error> {
        log::info!("Starting QUIC transport on port {} (Skeleton)", self.port);
        // Skeleton for QUIC - asupersync 0.3.1 net::quic is low-level state machine based
        loop {
            let _ = cx.checkpoint();
            asupersync::time::sleep(cx.now(), std::time::Duration::from_secs(3600)).await;
        }
    }

    fn stop(&self) -> Result<()> {
        log::info!("Stopping QUIC transport.");
        Ok(())
    }
}
// governed-by: ADR-0001
