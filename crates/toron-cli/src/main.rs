// governed-by: ADR-0003
#![forbid(unsafe_code)]

mod cli;
mod config;
mod output;
mod robot;

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
        Command::Config(args) => run_config(&cfg, args, format),
        Command::Mail(_)
        | Command::Reserve(_)
        | Command::Slot(_)
        | Command::Contact(_)
        | Command::Guard(_) => run_robot(&cli.command),
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

/// Robot parity surface (bd-led.8.3): build the demo mailbox ONCE (the same
/// one the MCP server uses), call the matching mailbox method, and print its
/// `ToJson` shape. Robot commands always emit JSON on stdout.
#[tokio::main]
async fn run_robot(command: &Command) -> anyhow::Result<()> {
    let mailbox = toron_mcp::demo_mailbox().await?;
    match command {
        Command::Mail(cmd) => robot::run_mail(&mailbox, cmd).await,
        Command::Reserve(cmd) => robot::run_reserve(&mailbox, cmd).await,
        Command::Slot(cmd) => robot::run_slot(&mailbox, cmd).await,
        Command::Contact(cmd) => robot::run_contact(&mailbox, cmd).await,
        Command::Guard(cmd) => robot::run_guard(&mailbox, cmd).await,
        _ => anyhow::bail!("not a robot command"),
    }
}

fn run_status(cfg: &Config, format: OutputFormat) -> anyhow::Result<()> {
    let value = serde_json::json!({
        "data_dir": cfg.data_dir.as_ref().map(|p| p.display().to_string()),
        "backend": cfg.effective_backend(),
        "operator_key": cfg.operator_key.as_ref().map(|p| p.display().to_string()),
        "relays": cfg.relays,
        "envelope_ttl_days": cfg.envelope_ttl_days(None),
        "proof_gate_enabled": cfg.proof_gate_enabled(),
        "proof_scheme": cfg.effective_proof_scheme(),
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

fn run_config(cfg: &Config, args: &cli::ConfigArgs, format: OutputFormat) -> anyhow::Result<()> {
    if args.sample {
        // stdout = data: the annotated sample config (see examples/toron.sample.toml).
        println!("{}", include_str!("../examples/toron.sample.toml"));
        return Ok(());
    }
    let value = serde_json::json!({
        "data_dir": cfg.data_dir.as_ref().map(|p| p.display().to_string()),
        "backend": cfg.effective_backend(),
        "operator_key": cfg.operator_key.as_ref().map(|p| p.display().to_string()),
        "relays": cfg.relays,
        "envelope_ttl_days": cfg.envelope_ttl_days(None),
        "project_ttl_days": cfg.project_ttl_days.iter().map(|pt| serde_json::json!({
            "project": pt.project,
            "days": pt.days,
        })).collect::<Vec<_>>(),
        "proof_gate_enabled": cfg.proof_gate_enabled(),
        "proof_trusted_keys": cfg.proof_trusted_keys,
        "proof_scheme": cfg.effective_proof_scheme(),
        "git_binary": cfg.git_binary.as_ref().map(|p| p.display().to_string()),
        "federation_auth": cfg.federation_auth.unwrap_or(false),
        "federation_pow": cfg.federation_pow.unwrap_or(0),
        "federation_allowlist": cfg.federation_allowlist,
    });
    match format {
        OutputFormat::Json | OutputFormat::Plain => render_json(&value, format),
        OutputFormat::Human => {
            println!("toron config (effective)");
            for (k, v) in value.as_object().unwrap() {
                println!("  {k}: {v}");
            }
        }
    }
    Ok(())
}
