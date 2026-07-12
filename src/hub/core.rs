use crate::hub::discovery::DiscoveryManager;
use crate::hub::transport::{Transport, WsTransport, QuicTransport};
use anyhow::Result;
use std::sync::Arc;
use resonate_sdk::prelude::Resonate;
use parking_lot::RwLock;
use asupersync::Outcome;
use asupersync_macros::scope;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HubRole {
    Active,
    Passive,
}

pub struct HubCore {
    discovery: Arc<DiscoveryManager>,
    ws_transport: Arc<WsTransport>,
    quic_transport: Arc<QuicTransport>,
    #[allow(dead_code)]
    resonate: Arc<Resonate>,
    psk: String,
    role: Arc<RwLock<HubRole>>,
    #[allow(dead_code)]
    broadcast_tx: asupersync::channel::Sender<crate::protocol::Message>,
    broadcast_rx: Arc<parking_lot::Mutex<Option<asupersync::channel::Receiver<crate::protocol::Message>>>>,
    sessions: Arc<RwLock<std::collections::HashMap<uuid::Uuid, asupersync::channel::Sender<crate::protocol::Message>>>>,
}

impl HubCore {
    pub fn new(instance_name: &str, psk: &str, resonate: Arc<Resonate>) -> Result<Arc<Self>> {
        let discovery = Arc::new(DiscoveryManager::new(instance_name)?);
        let (tx, rx) = asupersync::channel::channel(1024);
        let sessions = Arc::new(RwLock::new(std::collections::HashMap::new()));
        
        let ws_transport = WsTransport::new(8080, psk, resonate.clone(), tx.clone(), sessions.clone());
        let quic_transport = QuicTransport::new(8081, psk, resonate.clone(), tx.clone(), sessions.clone());
        
        Ok(Arc::new(Self {
            discovery,
            ws_transport,
            quic_transport,
            resonate,
            psk: psk.to_string(),
            role: Arc::new(RwLock::new(HubRole::Passive)),
            broadcast_tx: tx,
            broadcast_rx: Arc::new(parking_lot::Mutex::new(Some(rx))),
            sessions,
        }))
    }

    pub fn role(&self) -> HubRole {
        *self.role.read()
    }

    pub async fn start(&self, port: u16, cx: &mut asupersync::Cx) -> Result<()> {
        log::info!("Starting Toron Hub Core...");
        
        // Setup mDNS watch loop
        let events = self.discovery.watch()?;

        // Initial role evaluation: start as passive
        self.discovery.advertise(port, &self.psk, "passive")?;
        
        log::info!("Monitoring for other hubs to determine role...");
        
        let mut rt_state = asupersync::runtime::RuntimeState::default();

        let res: Outcome<(), anyhow::Error> = scope!(cx, state: &mut rt_state, {
            // Background task to monitor mDNS events and manage role
            let role_manager = self.role.clone();
            let discovery = self.discovery.clone();
            let psk = self.psk.clone();
            
            let ws_transport = self.ws_transport.clone();
            let quic_transport = self.quic_transport.clone();

            let role_manager_for_monitor = role_manager.clone();
            let discovery_for_monitor = discovery.clone();
            let mut monitor = asupersync_macros::spawn!(move |monitor_cx| async move {
                let mut other_active_found = false;
                
                // Phase 1: Short monitoring period to see if someone is already active
                log::info!("Phase 1: Detecting existing active hubs...");
                let start_time = std::time::Instant::now();
                while start_time.elapsed() < std::time::Duration::from_secs(2) {
                    let _ = monitor_cx.checkpoint();
                    
                    // Non-blocking check for events
                    while let Ok(event) = events.try_recv() {
                        if let mdns_sd::ServiceEvent::ServiceResolved(info) = event {
                            if let Some(role) = info.get_property_val_str("role") {
                                if role == "active" && !info.get_fullname().contains(discovery_for_monitor.instance_name()) {
                                    log::info!("Found active hub: {}", info.get_fullname());
                                    other_active_found = true;
                                }
                            }
                        }
                    }
                    asupersync::time::sleep(monitor_cx.now(), std::time::Duration::from_millis(100)).await;
                }

                loop {
                    let _ = monitor_cx.checkpoint();
                    
                    let current_role = *role_manager_for_monitor.read();
                    
                    if current_role == HubRole::Passive && !other_active_found {
                        log::info!("No other active hubs found. Promoting to ACTIVE role...");
                        *role_manager_for_monitor.write() = HubRole::Active;
                        
                        // Re-advertise as active
                        let _ = discovery_for_monitor.advertise(port, &psk, "active");
                    }
                    
                    // Drain events to keep track of other hubs
                    while let Ok(event) = events.try_recv() {
                        if let mdns_sd::ServiceEvent::ServiceResolved(info) = event {
                            if let Some(role) = info.get_property_val_str("role") {
                                if role == "active" && !info.get_fullname().contains(discovery_for_monitor.instance_name()) {
                                    if current_role == HubRole::Active {
                                        // Deterministic tie-break: lower instance name wins
                                        if info.get_fullname() < discovery_for_monitor.instance_name() {
                                            log::warn!("Conflict! Found higher priority active hub: {}. Yielding role.", info.get_fullname());
                                            *role_manager_for_monitor.write() = HubRole::Passive;
                                            let _ = discovery_for_monitor.advertise(port, &psk, "passive");
                                        } else {
                                            log::info!("Conflict! Found lower priority active hub: {}. Maintaining leadership.", info.get_fullname());
                                        }
                                    } else {
                                        other_active_found = true;
                                    }
                                }
                            }
                        } else if let mdns_sd::ServiceEvent::ServiceRemoved(_type, name) = event {
                             log::info!("Hub removed: {}. Will re-evaluate role.", name);
                             other_active_found = false;
                        }
                    }
                    
                    asupersync::time::sleep(monitor_cx.now(), std::time::Duration::from_secs(1)).await;
                }
            });

            // Hub bus fanning task
            let hub_rx_opt = self.broadcast_rx.lock().take();
            let hub_sessions = self.sessions.clone();
            let mut bus_task = asupersync_macros::spawn!(move |bus_cx| async move {
                if let Some(mut hub_rx) = hub_rx_opt {
                    loop {
                        let _ = bus_cx.checkpoint();
                        match hub_rx.recv(&bus_cx).await {
                            Ok(msg) => {
                                log::debug!("Fanning out message {} to sessions", msg.id);
                                let senders: Vec<_> = hub_sessions.read().values().cloned().collect();
                                for tx in senders {
                                    let _ = tx.send(&bus_cx, msg.clone()).await;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            });

            // Main loop that runs transports ONLY when active
            loop {
                let _ = __cx.checkpoint();
                
                let is_active = *role_manager.read() == HubRole::Active;
                
                if is_active {
                    log::info!("Toron Hub is ACTIVE, starting transports...");
                    
                    let ws = ws_transport.clone();
                    let quic = quic_transport.clone();

                    // Spawn transports concurrently
                    let mut h1 = asupersync_macros::spawn!(move |child_cx| async move {
                        ws.start(&child_cx).await
                    });

                    let mut h2 = asupersync_macros::spawn!(move |child_cx| async move {
                        quic.start(&child_cx).await
                    });

                    // Wait for either to fail or for role to change
                    let outcomes = asupersync_macros::join!(h1.join(__cx), h2.join(__cx));
                    log::warn!("Transports stopped: {:?}", outcomes);
                    break;
                } else {
                    log::debug!("Hub is in PASSIVE mode, waiting...");
                    asupersync::time::sleep(__cx.now(), std::time::Duration::from_secs(1)).await;
                }
            }
            
            let _ = asupersync_macros::join!(monitor.join(__cx), bus_task.join(__cx));
            Outcome::ok(())
        });

        match res {
            Outcome::Ok(_) => Ok(()),
            Outcome::Err(e) => Err(e),
            Outcome::Cancelled(r) => Err(anyhow::anyhow!("Runtime cancelled: {:?}", r)),
            Outcome::Panicked(p) => Err(anyhow::anyhow!("Runtime panicked: {:?}", p)),
        }
    }

    pub fn stop(&self) -> Result<()> {
        self.discovery.shutdown()?;
        let _ = self.ws_transport.stop();
        let _ = self.quic_transport.stop();
        Ok(())
    }
}
// governed-by: ADR-0001
