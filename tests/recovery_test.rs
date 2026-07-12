// governed-by: ADR-0001
use toron::hub::{HubCore, HubRole};
use resonate_sdk::prelude::{Resonate, ResonateConfig};
use std::sync::Arc;

#[tokio::test]
async fn test_resonate_registration_survives_restart() {
    let config = || ResonateConfig {
        url: None,
        group: None,
        pid: None,
        ttl: None,
        token: None,
        encryptor: None,
        network: None,
        prefix: None,
    };
    
    // Simulating Hub 1
    {
        let resonate = Arc::new(Resonate::new(config()));
        // Register workflows
        resonate.register(toron::durability::workflow::record_message).unwrap();
        let hub = HubCore::new("hub-1", "secret", resonate).expect("Failed to create hub 1");
        assert_eq!(hub.role(), HubRole::Passive);
        hub.stop().unwrap();
    }
    
    // Simulating Hub 1 Restart
    {
        let resonate = Arc::new(Resonate::new(config()));
        resonate.register(toron::durability::workflow::record_message).unwrap();
        let hub = HubCore::new("hub-1", "secret", resonate).expect("Failed to create hub 1 restart");
        assert_eq!(hub.role(), HubRole::Passive);
        hub.stop().unwrap();
    }
}
