// governed-by: ADR-0003
#![forbid(unsafe_code)]

mod cli;
mod config;
mod output;

use std::process::ExitCode;

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::config::Config;
use crate::output::{render_json, stdout_is_tty, ColorChoice, OutputFormat};

fn main() -> ExitCode {
    // Exit codes (clig.dev, ADR-0003): 0 ok · 1 runtime · 2 usage (clap handles 2).
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("toron: error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> anyhow::Result<()> {
    // Config precedence: --config flag > project ./.toron.toml > user config > system.
    // Env vars layer above files (see Config::load).
    let cfg = Config::load(cli.globals.config.as_deref());
    let format = OutputFormat::from_globals(&cli.globals);
    let color = ColorChoice::from_globals(&cli.globals, stdout_is_tty());

    match &cli.command {
        Command::Serve(args) => run_serve(args, format, color),
        Command::Status => run_status(&cfg, format),
    }
}

#[tokio::main]
async fn run_serve(
    args: &cli::ServeArgs,
    _format: OutputFormat,
    _color: ColorChoice,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "toron=info".into()),
        )
        .init();

    if args.mcp_stdio {
        tracing::info!("toron MCP server on stdio");
        toron_mcp::serve_stdio().await
    } else if let Some(addr) = &args.mcp_http {
        toron_mcp::serve_http(addr).await
    } else {
        anyhow::bail!("serve requires --mcp-stdio or --mcp-http <addr>")
    }
}

fn run_status(cfg: &Config, format: OutputFormat) -> anyhow::Result<()> {
    let value = serde_json::json!({
        "data_dir": cfg.data_dir.as_ref().map(|p| p.display().to_string()),
        "backend": cfg.backend,
        "operator_key": cfg.operator_key.as_ref().map(|p| p.display().to_string()),
        "relays": cfg.relays,
        "envelope_ttl_days": cfg.envelope_ttl_days.unwrap_or(90),
    });
    match format {
        OutputFormat::Json => render_json(&value, format),
        OutputFormat::Plain => {
            for (k, v) in value.as_object().unwrap() {
                println!("{k}: {v}");
            }
        }
        OutputFormat::Human => {
            println!("toron status");
            for (k, v) in value.as_object().unwrap() {
                println!("  {k}: {v}");
            }
        }
    }
    Ok(())
}
