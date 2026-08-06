// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-led.7.1 acceptance: an rmcp client completes `initialize` over both
//! transports and the negotiated protocol is 2026-07-28.

use rmcp::model::{ClientCapabilities, ClientInfo, Implementation, ProtocolVersion};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use rmcp::ClientServiceExt;
use rmcp::ServiceExt;

#[tokio::test]
async fn http_initialize_negotiates_2026_07_28() -> anyhow::Result<()> {
    let router = toron_mcp::router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, router).await });

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp")),
    );

    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    );
    let mut client = client_info
        .serve_with_lifecycle(
            transport,
            rmcp::ClientLifecycleMode::Discover {
                preferred_versions: vec![
                    ProtocolVersion::V_2026_07_28,
                    ProtocolVersion::V_2025_11_25,
                ],
            },
        )
        .await?;

    let peer = client.peer_info().expect("peer info after initialize");
    assert_eq!(
        peer.protocol_version,
        ProtocolVersion::V_2026_07_28,
        "negotiated protocol must be 2026-07-28"
    );
    assert!(
        peer.server_info.is_some(),
        "server identity must be present"
    );

    let tools = client.list_tools(None).await?;
    assert!(tools.tools.is_empty(), "skeleton exposes no tools yet");

    client.close().await?;
    server.abort();
    Ok(())
}

#[tokio::test]
async fn stdio_initialize_completes() -> anyhow::Result<()> {
    // Drive the server over an in-process duplex pair instead of the real
    // process stdin/stdout so the test needs no child binary. The wire frames
    // are identical to a real stdio connection (newline-delimited JSON-RPC).
    let (server_io, client_io) = tokio::io::duplex(4096);

    let server_task = tokio::spawn(async move {
        let server = toron_mcp::ToronServer::new();
        let running = server.serve(server_io).await;
        match running {
            Ok(running) => {
                let _ = running.waiting().await;
            }
            Err(e) => eprintln!("server failed: {e}"),
        }
    });

    let client_info = ClientInfo::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    )
    .with_protocol_version(ProtocolVersion::V_2026_07_28);
    let client = client_info.serve(client_io).await;

    server_task.abort();
    match client {
        Ok(mut running) => {
            let peer = running.peer_info().expect("peer info");
            assert_eq!(peer.protocol_version, ProtocolVersion::V_2026_07_28);
            running.close().await?;
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("stdio initialize failed: {e}")),
    }
}
