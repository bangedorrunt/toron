use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

const SERVICE_TYPE: &str = "_toron-hub._udp.local.";

/// Information about a discovered Toron Hub.
#[derive(Debug, Clone)]
pub struct HubInfo {
    pub instance_name: String,
    pub hostname: String,
    pub port: u16,
    pub addresses: Vec<String>,
}

/// Manages mDNS discovery for Toron Hubs.
pub struct DiscoveryManager {
    daemon: ServiceDaemon,
    #[allow(dead_code)]
    known_hubs: Arc<RwLock<HashMap<String, HubInfo>>>,
    instance_name: String,
}

impl DiscoveryManager {
    /// Creates a new discovery manager and starts browsing.
    pub fn new(instance_name: impl Into<String>) -> Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let known_hubs = Arc::new(RwLock::new(HashMap::new()));
        
        Ok(Self {
            daemon,
            known_hubs,
            instance_name: instance_name.into(),
        })
    }

    pub fn instance_name(&self) -> &str {
        &self.instance_name
    }

    /// Starts advertising this hub to the local network.
    pub fn advertise(&self, port: u16, psk: &str, role: &str) -> Result<()> {
        // Typically we'd use local IP, assuming 0.0.0.0 for now but mdns-sd handles interfaces
        let ip = "0.0.0.0";
        let host_name = format!("{}.local.", self.instance_name);

        let mut properties = HashMap::new();
        properties.insert("psk_id".to_string(), psk.to_string());
        properties.insert("role".to_string(), role.to_string());

        let my_service = ServiceInfo::new(
            SERVICE_TYPE,
            &self.instance_name,
            &host_name,
            ip,
            port,
            Some(properties),
        )?; // mdns-sd will auto-populate with local IPs if none given

        self.daemon.register(my_service)?;
        log::info!(
            "Advertising Toron Hub '{}' on port {} (Role: {}, PSK secured)", 
            self.instance_name, 
            port,
            role
        );
        Ok(())
    }

    /// Starts watching for other Toron Hubs. Note: in a real async environment,
    /// we should spawn a background task to process events. For now we expose a blocking receiver.
    pub fn watch(&self) -> Result<mdns_sd::Receiver<ServiceEvent>> {
        let receiver = self.daemon.browse(SERVICE_TYPE)?;
        Ok(receiver)
    }

    /// Gracefully shutdown the discovery service.
    pub fn shutdown(&self) -> Result<()> {
        log::info!("Shutting down mDNS discovery...");
        self.daemon.shutdown()?;
        Ok(())
    }
}
// governed-by: ADR-0001
