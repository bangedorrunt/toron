use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome, Error};
use asupersync_macros::scope;
use resonate_sdk::prelude::{Resonate, ResonateConfig};
use toron::hub::HubCore;
use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    // Initialize asupersync runtime
    let rt = RuntimeBuilder::current_thread().build()
        .map_err(|e| anyhow::anyhow!("Failed to build runtime: {:?}", e))?;

    rt.block_on(async {
        let mut cx = Cx::for_request();
        
        let outcome: Outcome<(), Error> = scope!(&mut cx, {
            log::info!("Initializing Toron Hub...");

            // Initialize Resonate durable execution
            let config = ResonateConfig {
                url: Some("http://localhost:8001".to_string()),
                group: None,
                pid: None,
                ttl: None,
                token: None,
                encryptor: None,
                network: None,
                prefix: None,
            };

            let resonate = Arc::new(Resonate::new(config));
            
            // Register workflows
            let _ = resonate.register(toron::durability::workflow::record_message);
            let _ = resonate.register(toron::durability::workflow::broadcast_message);
            let _ = resonate.register(toron::durability::workflow::process_topic_message);
            
            log::info!("Resonate services registered.");

            let hub = HubCore::new("primary-hub", "toron-secret-psk", resonate.clone()).unwrap();
            
            // Start the hub
            if let Err(e) = hub.start(8080, &mut cx).await {
                log::error!("Hub failed to start: {:?}", e);
            }
            
            Outcome::ok(())
        });

        if let Outcome::Err(e) = outcome {
            log::error!("Runtime error: {:?}", e);
        }
    });

    Ok(())
}

// governed-by: ADR-0001
