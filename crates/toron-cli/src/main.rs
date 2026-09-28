// governed-by: ADR-0003
#![forbid(unsafe_code)]

mod catalog;
mod cli;
mod config;
mod daemon;
mod mailbox_source;
mod operator;
mod output;
mod plane;
mod platform;
mod robot;
mod theme;
mod tui;
mod wake_sock;
mod workflow;

use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use toron_core::index::run_state::{ForkIntent, RunStateStore, plan_fork};

use clap::Parser;

use crate::cli::{Cli, Command};
use crate::config::Config;
use crate::output::{ColorChoice, OutputFormat, render_json, stdout_is_tty};

fn main() -> ExitCode {
    // Exit codes (clig.dev, ADR-0003): 0 ok · 1 runtime · 2 usage (clap
    // handles 2) · 3 doctor --check fails-closed.
    let cli = Cli::parse();
    match run(&cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("toron: error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> anyhow::Result<ExitCode> {
    // Config precedence: --config flag > project ./.toron.toml > user config > system.
    // Env vars layer above files (see Config::load).
    let cfg = Config::load(cli.globals.config.as_deref());
    let format = OutputFormat::from_globals(&cli.globals);
    let color = ColorChoice::from_globals(&cli.globals, stdout_is_tty());

    // Robot mode global flags (bd-led.8.4): emit a JSON snapshot and exit.
    // These short-circuit before the command dispatch so an agent driver can
    // poll state without knowing the subcommand surface (ADR-0003).
    if cli.globals.robot_snapshot || cli.globals.robot_status || cli.globals.robot_capabilities {
        return run_robot_flag(&cli.globals, format);
    }

    // A subcommand is required unless a robot flag short-circuited above.
    let Some(command) = &cli.command else {
        let mut cmd = <cli::Cli as clap::CommandFactory>::command();
        let _ = cmd.print_help();
        return Ok(ExitCode::from(2));
    };

    let code = match command {
        Command::Serve(args) => {
            run_serve(args, &cfg, format, color)?;
            ExitCode::SUCCESS
        }
        Command::Status => {
            run_status(&cfg, format)?;
            ExitCode::SUCCESS
        }
        Command::Catalog => {
            run_catalog(cli.globals.output.clone())?;
            ExitCode::SUCCESS
        }
        Command::Config(args) => {
            run_config(&cfg, args, format)?;
            ExitCode::SUCCESS
        }
        Command::Daemon(args) => {
            daemon::run_dispatch(args)?;
            ExitCode::SUCCESS
        }
        Command::Doctor(args) => operator::run_doctor(&cfg, args, format)?,
        Command::Metrics => {
            operator::run_metrics()?;
            ExitCode::SUCCESS
        }
        Command::Key(cmd) => {
            operator::run_key(&cfg, cmd)?;
            ExitCode::SUCCESS
        }
        Command::Project(cmd) => {
            operator::run_project(cmd)?;
            ExitCode::SUCCESS
        }
        Command::Relay(cmd) => {
            operator::run_relay(&cfg, cmd)?;
            ExitCode::SUCCESS
        }
        Command::Backend(cmd) => {
            operator::run_backend(&cfg, cmd)?;
            ExitCode::SUCCESS
        }
        Command::Bench(args) => {
            run_bench(args, format)?;
            ExitCode::SUCCESS
        }
        Command::Tui(args) => {
            tui::run_tui(args)?;
            ExitCode::SUCCESS
        }
        Command::Workflow(cmd) => {
            use crate::cli::WorkflowCommand;
            let code = match cmd {
                WorkflowCommand::Lint { file } => workflow::lint(file)?,
                WorkflowCommand::Validate { file } => workflow::lint(file)?,
                WorkflowCommand::Run { file, trigger } => workflow::run(file, trigger)?,
                WorkflowCommand::Invoke { file, inputs } => workflow::invoke(file, inputs)?,
                WorkflowCommand::DryRun { file, trigger } => workflow::dry_run(file, trigger)?,
            };
            ExitCode::from(code as u8)
        }
        Command::Receipt(cmd) => {
            run_receipt(cmd, format)?;
            ExitCode::SUCCESS
        }
        Command::Role(cmd) => {
            run_role(cmd, format)?;
            ExitCode::SUCCESS
        }
        Command::RunState(cmd) => {
            run_run_state(cmd, &cfg, format)?;
            ExitCode::SUCCESS
        }
        Command::Mail(_)
        | Command::AckSla(_)
        | Command::Reserve(_)
        | Command::Slot(_)
        | Command::Contact(_)
        | Command::Agent(_)
        | Command::Guard(_) => {
            run_robot(command, cli.globals.dry_run)?;
            ExitCode::SUCCESS
        }
        Command::Channel(_)
        | Command::Job(_)
        | Command::React(_)
        | Command::Presence(_)
        | Command::Watch(_)
        | Command::Wait(_) => {
            // Two-plane surface (ADR-0020 Spec 2): same in-process mailbox as
            // the MCP server, but the Nostr plane as shell commands. Usage
            // errors (unknown --as, --once without --timeout-ms) exit 2.
            match plane::run_plane_command(command) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) if e.downcast_ref::<plane::UsageError>().is_some() => {
                    eprintln!("toron: error: {e:#}");
                    ExitCode::from(2)
                }
                // Idle exit → 4 (documented extension): watch --live
                // --idle-exit fired; a supervisor treats it as Died→respawn.
                Err(e) if e.downcast_ref::<plane::IdleExit>().is_some() => {
                    eprintln!("toron: {e}");
                    ExitCode::from(4)
                }
                Err(e) => return Err(e),
            }
        }
    };
    Ok(code)
}

/// Robot-mode global flag dispatch (bd-led.8.4): snapshot / status /
/// capabilities as JSON on stdout. Backed by the shared
/// [`toron_core::mailbox::MailboxMetrics`] core.
fn run_robot_flag(globals: &cli::Globals, format: OutputFormat) -> anyhow::Result<ExitCode> {
    let metrics = toron_core::mailbox::MailboxMetrics::new();
    let value = if globals.robot_capabilities {
        metrics.robot_capabilities()
    } else if globals.robot_status {
        metrics.robot_status()
    } else {
        metrics.robot_snapshot()
    };
    match format {
        OutputFormat::Json | OutputFormat::Plain => render_json(&value, format),
        OutputFormat::Human => println!("{value}"),
    }
    Ok(ExitCode::SUCCESS)
}

/// `toron bench <case>` (bd-led.8.4, AM-Rust parity): measure a mailbox
/// scenario's latency against a live in-process bus and emit a percentile
/// report (startup/inbox/send/search/threads).
#[tokio::main]
async fn run_bench(args: &cli::BenchArgs, format: OutputFormat) -> anyhow::Result<ExitCode> {
    use toron_core::mailbox::{BenchReport, FetchInbox, SearchMessages, SendMessage};

    const ITERATIONS: u64 = 20;
    let mut latencies = Vec::with_capacity(ITERATIONS as usize);

    // Non-startup cases measure the mailbox PRIMITIVE, not mailbox
    // construction — build once, then time 20 iterations against the same
    // instance (construction cost would otherwise dominate and skew the
    // percentile report). `startup` measures construction itself, so it keeps
    // a fresh demo_mailbox() per iteration.
    let mb = if args.case == cli::BenchCaseArg::Startup {
        None
    } else {
        Some(toron_mcp::demo_mailbox().await?)
    };

    for _ in 0..ITERATIONS {
        let start = std::time::Instant::now();
        // Each case measures the mailbox primitive's latency. Success/failure
        // is irrelevant to the timing benchmark (a missing agent key or empty
        // index still exercises the full dispatch path), so errors are
        // swallowed — the report reflects the operation's cost.
        let _ = match args.case {
            cli::BenchCaseArg::Startup => {
                // Mailbox construction (bus + SQLite) + health probe.
                let _mb = toron_mcp::demo_mailbox().await;
                Ok(())
            }
            _ => {
                let mb = mb.as_ref().expect("prebuilt mailbox for non-startup");
                match args.case {
                    cli::BenchCaseArg::Inbox => mb
                        .fetch_inbox(&FetchInbox {
                            project_slug: "demo".into(),
                            agent_name: "bench".into(),
                            ..Default::default()
                        })
                        .await
                        .map(|_| ()),
                    cli::BenchCaseArg::Send => mb
                        .send_message(&SendMessage {
                            project_slug: "demo".into(),
                            sender_name: "bench".into(),
                            subject: "s".into(),
                            body_md: "b".into(),
                            ..Default::default()
                        })
                        .await
                        .map(|_| ()),
                    cli::BenchCaseArg::Search => mb
                        .search_messages(&SearchMessages {
                            project_key: "demo".into(),
                            query: "bench".into(),
                            ..Default::default()
                        })
                        .await
                        .map(|_| ()),
                    cli::BenchCaseArg::Threads => mb
                        .summarize_thread("demo", "bench-thread", false, false, None)
                        .await
                        .map(|_| ()),
                    cli::BenchCaseArg::Startup => unreachable!("handled above"),
                }
            }
        };
        latencies.push(start.elapsed().as_micros() as u64);
    }

    let report = BenchReport {
        scenarios: vec![BenchReport::case(args.case.as_str(), &latencies)],
    };
    match format {
        OutputFormat::Json | OutputFormat::Plain => {
            render_json(&serde_json::to_value(&report)?, format);
        }
        OutputFormat::Human => println!(
            "bench {}: {} iterations, avg {}us, p50 {}us, p95 {}us",
            args.case.as_str(),
            report.scenarios[0].iterations,
            report.scenarios[0].avg_us,
            report.scenarios[0].p50_us,
            report.scenarios[0].p95_us,
        ),
    }
    Ok(ExitCode::SUCCESS)
}

#[tokio::main]
async fn run_serve(
    args: &cli::ServeArgs,
    cfg: &Config,
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
        toron_mcp::serve_http(addr, &cfg.mcp_allowed_hosts).await
    } else {
        anyhow::bail!("serve requires --mcp-stdio or --mcp-http <addr>")
    }
}

/// Robot parity surface (bd-led.8.3): build the demo mailbox ONCE (the same
/// one the MCP server uses), call the matching mailbox method, and print its
/// `ToJson` shape. Robot commands always emit JSON on stdout. Each dispatch is
/// instrumented into the operator metrics (bd-led.8.2): tool name + duration on
/// success, an error counter on failure.
#[tokio::main]
async fn run_robot(command: &Command, dry_run: bool) -> anyhow::Result<()> {
    // bd-pin-public-plane-s-easy.14: the robot path uses the SAME mailbox
    // selector as flywheel — daemon when configured/serving, else the
    // durable XDG root. No second in-process mailbox when the daemon serves.
    let source = crate::mailbox_source::MailboxSource::resolve().await?;
    let tool_name = operator::robot_tool_name(command);
    let start = std::time::Instant::now();
    let result = match (&source, command) {
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::Mail(cmd)) => {
            robot::run_mail_daemon(dc, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::Reserve(cmd)) => {
            robot::run_reserve_daemon(dc, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::Slot(cmd)) => {
            robot::run_slot_daemon(dc, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::Contact(cmd)) => {
            robot::run_contact_daemon(dc, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::Guard(cmd)) => {
            robot::run_guard_daemon(dc, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::Agent(cmd)) => {
            robot::run_agent_daemon(dc, cmd, dry_run).await
        }
        (crate::mailbox_source::MailboxSource::Daemon(dc), Command::AckSla(cmd)) => {
            robot::run_ack_sla_daemon(dc, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::Mail(cmd)) => {
            robot::run_mail(mb, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::Reserve(cmd)) => {
            robot::run_reserve(mb, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::Slot(cmd)) => {
            robot::run_slot(mb, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::Contact(cmd)) => {
            robot::run_contact(mb, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::Guard(cmd)) => {
            robot::run_guard(mb, cmd).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::Agent(cmd)) => {
            robot::run_agent(mb, cmd, dry_run).await
        }
        (crate::mailbox_source::MailboxSource::Embedded(mb), Command::AckSla(cmd)) => {
            robot::run_ack_sla(mb, cmd).await
        }
        _ => anyhow::bail!("not a robot command"),
    };
    match result {
        Ok(()) => {
            operator::record_tool_call(tool_name, start.elapsed());
            Ok(())
        }
        Err(e) => {
            operator::record_tool_error(tool_name, start.elapsed());
            Err(e)
        }
    }
}

/// `toron catalog` — the MCP surface catalog (ADR-0002 D3): 38 tools, 25
/// resources, CLI commands. `--output <path>` writes the committed
/// `docs/catalog/toron-mcp.json`; otherwise the catalog prints to stdout.
fn run_catalog(output: Option<PathBuf>) -> anyhow::Result<()> {
    let catalog = catalog::catalog_now()?;
    match output {
        Some(path) => catalog::write_catalog(&path, &catalog),
        None => {
            println!("{}", serde_json::to_string_pretty(&catalog)?);
            Ok(())
        }
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

/// `toron run-state replay|fork` (ADR-0035, spec S8).
///
/// The read half of the checkpoint store. Both arms are READS: `replay` renders
/// one boundary, `fork` renders what a fork from it would do. Neither installs
/// anything, and `fork` is dry by default, so this whole function is incapable
/// of seeding a live model from the log (spec S3) — the one arm that could is
/// the live host's, beside its own resume path.
///
/// The pool is opened read-only in the only sense that matters here (nothing
/// but SELECT runs), and the store is the SAME one the recorder appends to: one
/// index, one schema.
fn run_run_state(
    cmd: &cli::RunStateCommand,
    cfg: &Config,
    format: OutputFormat,
) -> anyhow::Result<()> {
    use crate::cli::RunStateCommand;

    let data_dir = resolve_data_dir(cfg);
    let db_path = data_dir.join("index.sqlite");
    if !db_path.exists() {
        anyhow::bail!(
            "no run-state index at {}. Checkpoints are recorded only when the host runs \
             with run-state logging on, so an absent index is a real answer, not a \
             failed read.",
            db_path.display()
        );
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (thread, seq) = match cmd {
        RunStateCommand::Replay(a) | RunStateCommand::Fork(a) => (a.thread.clone(), a.seq),
    };

    let value = rt.block_on(async {
        let pool = toron_core::index::schema::connect(db_path.to_str().unwrap()).await?;
        let store = RunStateStore::new(pool);
        let boundary = store.replay(&thread, seq).await?;
        // The fork arm is dry by construction. There is no --perform flag
        // here: an opt-in that this build cannot honour would be a flag that
        // lies. The acknowledged `Perform` intent exists in the core for the
        // host that owns the installer, and is reachable by naming the hazard.
        let value = match cmd {
            RunStateCommand::Replay(_) => serde_json::json!({
                "thread": boundary.thread_id,
                "seq": boundary.seq,
                "checkpoint_id": boundary.checkpoint_id,
                "parent_checkpoint_id": boundary.parent_checkpoint_id,
                "msg_kind": boundary.msg_kind,
                "created_at": boundary.created_at,
                "model": boundary.model,
                "effect_keys": boundary.effect_keys,
            }),
            RunStateCommand::Fork(_) => {
                serde_json::json!(plan_fork(&boundary, &ForkIntent::Dry))
            }
        };
        Ok::<serde_json::Value, anyhow::Error>(value)
    })?;

    match format {
        OutputFormat::Json | OutputFormat::Plain => render_json(&value, format),
        OutputFormat::Human => println!("{}", render_run_state_human(&value)),
    }
    Ok(())
}

/// Human rendering for a replay or a fork plan.
///
/// Kept as its own function so the JSON path and the text path cannot drift on
/// what they claim: a boundary printed as text and the same boundary printed as
/// JSON must say the same thing about installability.
fn render_run_state_human(value: &serde_json::Value) -> String {
    use std::fmt::Write as _;

    let field = |k: &str| {
        value
            .get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_string()
    };
    let mut out = format!(
        "boundary  {}/{}\ncheckpoint {}\nmsg        {}",
        field("thread"),
        value
            .get("seq")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(-1),
        field("checkpoint_id"),
        field("msg_kind"),
    );
    if let Some(keys) = value
        .get("effect_keys")
        .and_then(serde_json::Value::as_array)
    {
        let _ = writeln!(out, "\nowned      {} effect(s)", keys.len());
        for k in keys {
            let _ = writeln!(out, "           {k}");
        }
    }
    if let Some(refusals) = value.get("refusals").and_then(serde_json::Value::as_array) {
        for r in refusals {
            if let Some(s) = r.as_str() {
                let _ = writeln!(out, "refused    {s}");
            }
        }
    }
    if let Some(model) = value.get("model") {
        let _ = writeln!(
            out,
            "model      {}",
            serde_json::to_string(model).unwrap_or_else(|e| format!("<unrenderable: {e}>"))
        );
    }
    out.trim_end().to_string()
}

/// The durable data dir this process reads. Same ladder the other one-store
/// commands use, so `run-state` reads the same index they write.
fn resolve_data_dir(cfg: &Config) -> PathBuf {
    std::env::var_os("TORON_DATA_DIR")
        .map(PathBuf::from)
        .or_else(|| cfg.data_dir.clone())
        .unwrap_or_else(toron_core::config::durable_data_dir)
}

#[tokio::main]
async fn run_receipt(cmd: &cli::ReceiptCommand, format: OutputFormat) -> anyhow::Result<()> {
    use crate::cli::ReceiptCommand;
    use toron_core::mailbox::ReceiptState;
    let mailbox = toron_mcp::demo_mailbox().await?;
    let store = mailbox.receipt_store()?;
    match cmd {
        ReceiptCommand::Get(args) => {
            let receipt = store.get(&args.corr).await?;
            match receipt {
                Some(r) => {
                    let value = serde_json::json!({
                        "corr": r.corr,
                        "state": r.state.as_str(),
                        "wake": r.wake.map(toron_core::mailbox::send_tags::Wake::as_str),
                        "due_at": r.due_at,
                        "result_path": r.result_path,
                        "updated_at": r.updated_at,
                    });
                    render_json(&value, format);
                }
                None => {
                    anyhow::bail!("receipt not found for corr {}", args.corr);
                }
            }
        }
        ReceiptCommand::List(args) => {
            let state = match &args.state {
                Some(s) => Some(ReceiptState::parse(s).map_err(|e| anyhow::anyhow!(e))?),
                None => None,
            };
            let receipts = store.list(state).await?;
            let value: Vec<serde_json::Value> = receipts
                .into_iter()
                .map(|r| {
                    serde_json::json!({
                        "corr": r.corr,
                        "state": r.state.as_str(),
                        "wake": r.wake.map(toron_core::mailbox::send_tags::Wake::as_str),
                        "due_at": r.due_at,
                        "result_path": r.result_path,
                        "updated_at": r.updated_at,
                    })
                })
                .collect();
            render_json(&serde_json::Value::Array(value), format);
        }
        ReceiptCommand::Set(args) => {
            let state = ReceiptState::parse(&args.state).map_err(|e| anyhow::anyhow!(e))?;
            if state != ReceiptState::Resulted {
                anyhow::bail!(
                    "receipt set only supports --state resulted (got {})",
                    args.state
                );
            }
            // Boundary parse (ADR-0035 Spec 6): an empty corr fails loud.
            let corr = toron_core::mailbox::Corr::from_str(&args.corr)
                .map_err(|e| anyhow::anyhow!("invalid --corr: {e}"))?;
            // Host clock (ADR-0035 Spec 1): the store takes host time.
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let receipt = store
                .set_resulted(&corr, args.result_path.clone(), now_ms)
                .await?;
            let value = serde_json::json!({
                "corr": receipt.corr,
                "state": receipt.state.as_str(),
                "wake": receipt.wake.map(toron_core::mailbox::send_tags::Wake::as_str),
                "due_at": receipt.due_at,
                "result_path": receipt.result_path,
                "updated_at": receipt.updated_at,
            });
            render_json(&value, format);
        }
    }
    Ok(())
}

#[tokio::main]
async fn run_role(cmd: &cli::RoleCommand, format: OutputFormat) -> anyhow::Result<()> {
    use crate::cli::RoleCommand;
    let mailbox = toron_mcp::demo_mailbox().await?;
    match cmd {
        RoleCommand::Pin(args) => {
            let pin = mailbox
                .set_role_pin(&args.project, &args.role, &args.pin)
                .await?;
            let value = serde_json::json!({
                "project": args.project,
                "role": args.role,
                "pin": pin,
            });
            render_json(&value, format);
        }
        RoleCommand::List(args) => {
            let roles = mailbox.list_role_pins(&args.project).await?;
            let value: Vec<serde_json::Value> = roles
                .into_iter()
                .map(|(role, pin)| serde_json::json!({"role": role, "pin": pin}))
                .collect();
            render_json(&serde_json::Value::Array(value), format);
        }
        RoleCommand::Clear(args) => {
            let cleared = mailbox.clear_role_pin(&args.project, &args.role).await?;
            let value = serde_json::json!({
                "project": args.project,
                "role": args.role,
                "cleared": cleared.is_some(),
                "previous": cleared,
            });
            render_json(&value, format);
        }
    }
    Ok(())
}
