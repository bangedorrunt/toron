// governed-by: ADR-0003
#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};

/// toron: a Nostr-native agent mailbox.
#[derive(Parser)]
#[command(name = "toron", version, about = "Nostr-native agent mailbox")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the toron daemon (mailbox + MCP server).
    Serve(ServeArgs),
}

#[derive(clap::Args)]
struct ServeArgs {
    /// Serve the MCP endpoint over stdio (child-process / embedded mode).
    #[arg(long, conflicts_with = "mcp_http")]
    mcp_stdio: bool,

    /// Serve the MCP endpoint over Streamable HTTP at this address (e.g. 127.0.0.1:8000).
    #[arg(long)]
    mcp_http: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "toron=info".into()),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => {
            if args.mcp_stdio {
                tracing::info!("toron MCP server on stdio");
                toron_mcp::serve_stdio().await
            } else if let Some(addr) = args.mcp_http {
                toron_mcp::serve_http(&addr).await
            } else {
                anyhow::bail!("serve requires --mcp-stdio or --mcp-http <addr>")
            }
        }
    }
}
