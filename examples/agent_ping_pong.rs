// governed-by: ADR-0001
use asupersync::runtime::RuntimeBuilder;
use asupersync::{Cx, Outcome, Error};
use asupersync_macros::scope;
use toron::protocol::{Message, Intent};
use std::sync::Arc;
use asupersync::net::TcpStream;
use asupersync::net::websocket::{WebSocketAcceptor, Message as WsMessage};

fn main() -> anyhow::Result<()> {
    env_logger::init();
    
    let rt = RuntimeBuilder::current_thread().build()
        .map_err(|e| anyhow::anyhow!("Failed to build runtime: {:?}", e))?;

    rt.block_on(async {
        let mut cx = Cx::for_request();
        
        let _: Outcome<(), Error> = scope!(&mut cx, {
            // This example assumes a Hub is running on localhost:8080
            // and uses the default PSK "toron-secret-psk"
            
            let addr = "127.0.0.1:8080";
            log::info!("Agent 1 connecting to Hub at {}...", addr);
            
            match TcpStream::connect(addr).await {
                Ok(mut stream) => {
                    // 1. Handshake with PSK
                    let handshake = format!(
                        "GET / HTTP/1.1\r\n\
                         Host: {}\r\n\
                         Upgrade: websocket\r\n\
                         Connection: Upgrade\r\n\
                         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                         Sec-WebSocket-Version: 13\r\n\
                         X-Toron-PSK: toron-secret-psk\r\n\r\n",
                        addr
                    );
                    
                    use asupersync::io::AsyncWriteExt;
                    stream.write_all(handshake.as_bytes()).await.unwrap();
                    
                    // 2. Wrap in WebSocket (simplified, in real life use a client library)
                    // For this example, we'll just show the structure of communication
                    
                    log::info!("Agent 1 connected. Sending 'Ping' message...");
                    
                    let ping = Message::new(
                        "general",
                        "agent-1",
                        Intent::Propose,
                        serde_json::json!({"text": "Ping"})
                    );
                    
                    // In a real agent, you'd use a higher-level SDK
                    log::info!("Message sent: {:?}", ping);
                    
                    log::info!("Agents communicate by connecting to the Hub, joining a topic, and sending Messages.");
                    log::info!("The Hub fans out messages to all agents joined to the same topic.");
                }
                Err(e) => {
                    log::error!("Failed to connect: {}. Is the Hub running?", e);
                }
            }
            
            Outcome::ok(())
        });
    });

    Ok(())
}