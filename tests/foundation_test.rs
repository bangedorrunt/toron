// governed-by: ADR-0001
use toron::hub::{HubCore, HubRole};
use resonate_sdk::prelude::{Resonate, ResonateConfig};
use std::sync::Arc;
use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome, Error};
use asupersync_macros::scope;

#[tokio::test]
async fn test_hub_initialization() {
    let rt = RuntimeBuilder::current_thread().build().expect("Failed to build runtime");
    
    rt.block_on(async {
        let mut cx = Cx::for_request();
        let _: Outcome<(), Error> = scope!(&mut cx, {
            let config = ResonateConfig {
                url: None,
                group: None,
                pid: None,
                ttl: None,
                token: None,
                encryptor: None,
                network: None,
                prefix: None,
            };
            let resonate = Arc::new(Resonate::new(config));
            let hub = HubCore::new("test-hub", "secret", resonate).expect("Failed to create hub");
            
            // Check initial state
            assert_eq!(hub.role(), HubRole::Passive);
            
            assert_eq!(hub.stop().is_ok(), true);
            Outcome::ok(())
        });
    });
}
