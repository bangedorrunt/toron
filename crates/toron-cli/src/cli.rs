// governed-by: ADR-0003

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// toron: a Nostr-native agent mailbox.
///
/// Two personas from one surface:
///   - Operators: daemon lifecycle, keys, projects, relays, backend, doctor.
///   - Agents/scripts (robot mode): the full 38-tool AM-Rust parity surface.
///
/// Examples
/// --------
///   toron serve --mcp-stdio
///   toron serve --mcp-http 127.0.0.1:8000
///   toron status
///   toron status --json
#[derive(Debug, Parser)]
#[command(name = "toron", version, about, arg_required_else_help = true)]
pub struct Cli {
    #[command(flatten)]
    pub globals: Globals,

    #[command(subcommand)]
    pub command: Command,
}

/// Global flags (clig.dev: flags > env > project config > user config > system).
#[derive(Debug, Args, Clone, Default)]
pub struct Globals {
    /// Structured JSON output (all read commands).
    #[arg(long, global = true)]
    pub json: bool,

    /// Flat tabular text for grep/awk.
    #[arg(long, global = true)]
    pub plain: bool,

    /// Disable color (also: NO_COLOR, TERM=dumb, piped).
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Less output.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Debug/verbose to stderr.
    #[arg(short, long, global = true)]
    pub debug: bool,

    /// Show changes without running.
    #[arg(short = 'n', long, global = true)]
    pub dry_run: bool,

    /// Never prompt; fail with instruction if input is needed.
    #[arg(long, global = true)]
    pub no_input: bool,

    /// Override config file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Output to file (commands that write artifacts).
    #[arg(short, long, global = true)]
    pub output: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the toron daemon (mailbox + MCP server).
    Serve(ServeArgs),

    /// Quick health/config overview (human by default, --json for machines).
    Status,

    /// Show the effective configuration (flags > env > project > user > system).
    Config(ConfigArgs),
}

#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Print the annotated sample configuration to stdout.
    #[arg(long)]
    pub sample: bool,
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Serve the MCP endpoint over stdio (child-process / embedded mode).
    #[arg(long, conflicts_with = "mcp_http")]
    pub mcp_stdio: bool,

    /// Serve the MCP endpoint over Streamable HTTP at this address (e.g. 127.0.0.1:8000).
    #[arg(long)]
    pub mcp_http: Option<String>,
}
