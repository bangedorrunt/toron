// governed-by: ADR-0003
// governed-by: ADR-0004 (MailCommand::Overseer — Ship 5)

//! Robot parity surface (bd-led.8.3): the MCP tools as shell-out commands.
//!
//! Parity-by-construction: each command builds a [`Mailbox`] via
//! [`toron_mcp::demo_mailbox`] (the exact same in-process bus + SQLite index
//! the MCP server uses), calls the SAME mailbox method the MCP tool calls, and
//! prints `result.to_json()` through the SAME [`toron_mcp::ToJson`] impl.
//! Byte-identical JSON to the MCP tool.

use std::path::Path;

use anyhow::Context;
use serde_json::json;
use toron_core::mailbox::{
    ContactPolicy, FetchInbox, Mailbox, ReleaseAsk, ReleaseDecision, ReleaseResponse,
    RequestContact, RequestRelease, RespondContact, SearchMessages, SendMessage,
};
use toron_mcp::ToJson;

use crate::cli::{
    ContactCommand, GuardCommand, MailCommand, PolicyArg, ReserveCommand, SlotCommand,
};
use crate::mailbox_source::DaemonClient;

/// Call a daemon MCP tool and print its text content (the parity JSON).
async fn daemon_tool(dc: &DaemonClient, tool: &str, args: serde_json::Value) -> anyhow::Result<()> {
    let out = dc.call_tool(tool, args).await?;
    print_json(&out)
}

/// Print a result's MCP-parity JSON shape to stdout (always pretty JSON).
fn emit<T: ToJson>(result: T) -> anyhow::Result<()> {
    print_json(&result.to_json())
}

/// Pretty-print a raw JSON value (hand-built parity shapes).
fn print_json(value: &serde_json::Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub async fn run_mail(mailbox: &Mailbox, cmd: &MailCommand) -> anyhow::Result<()> {
    match cmd {
        MailCommand::Send(args) => {
            let msg = SendMessage {
                project_slug: args.project.clone(),
                sender_name: args.from.clone(),
                to: args.to.clone(),
                subject: args.subject.clone(),
                body_md: args.body.clone(),
                cc: args.cc.clone(),
                bcc: args.bcc.clone(),
                importance: args.importance.clone(),
                ack_required: args.ack,
                thread_id: args.thread.clone(),
                sender_token: args.sender_token.clone(),
                overseer: false,
                extra_tags: Vec::new(),
            };
            emit(mailbox.send_message(&msg).await?)
        }
        MailCommand::Overseer(args) => {
            // ADR-0004 Ship 5: operator Overseer override. Signs with the
            // operator key (sender_name is ignored), importance=high, tagged
            // toron:overseer, body carries the override preamble.
            let msg = SendMessage {
                project_slug: args.project.clone(),
                sender_name: String::new(), // overseer signs with the operator key
                to: args.to.clone(),
                subject: args
                    .subject
                    .clone()
                    .unwrap_or_else(|| "Operator override".to_string()),
                body_md: format!(
                    "Operator override. Pause current work, handle this, then resume.\n\n{}",
                    args.body
                ),
                cc: vec![],
                bcc: vec![],
                importance: Some("high".to_string()),
                ack_required: false,
                thread_id: None,
                sender_token: None,
                overseer: true,
                extra_tags: Vec::new(),
            };
            emit(mailbox.send_message(&msg).await?)
        }
        MailCommand::Reply(args) => {
            let msg = SendMessage {
                project_slug: args.project.clone(),
                sender_name: args.from.clone(),
                to: args.to.clone(),
                subject: args.subject.clone(),
                body_md: args.body.clone(),
                cc: args.cc.clone(),
                bcc: args.bcc.clone(),
                importance: args.importance.clone(),
                ack_required: args.ack,
                thread_id: Some(args.thread.clone()),
                sender_token: args.sender_token.clone(),
                overseer: false,
                extra_tags: Vec::new(),
            };
            emit(mailbox.reply_message(&msg).await?)
        }
        MailCommand::Inbox(args) => {
            let fi = FetchInbox {
                project_slug: args.project.clone(),
                agent_name: args.agent.clone(),
                unread_only: args.unread,
                limit: args.limit,
                include_bodies: args.bodies,
                urgent_only: false,
                since_ts: args.since.clone(),
                thread_id: args.thread_id.clone(),
            };
            emit(mailbox.fetch_inbox(&fi).await?)
        }
        MailCommand::Ack(args) => emit(
            mailbox
                .acknowledge_message(&args.project, &args.agent, &args.id)
                .await?,
        ),
        MailCommand::Read(args) => emit(
            mailbox
                .mark_message_read(&args.project, &args.agent, &args.id)
                .await?,
        ),
        MailCommand::Search(args) => {
            let sm = SearchMessages {
                project_key: args.project.clone(),
                query: args.query.clone(),
                limit: args.limit,
                ranking: args.ranking.clone(),
                sender_name: args.sender.clone(),
                importance: args.importance.clone(),
                thread_id: args.thread.clone(),
                since_us: args.since,
                until_us: args.until,
                include_body_md: args.bodies,
            };
            emit(mailbox.search_messages(&sm).await?)
        }
        MailCommand::ThreadSummarize(args) => emit(
            mailbox
                .summarize_thread(
                    &args.project,
                    &args.thread,
                    args.examples,
                    args.llm_mode,
                    args.llm_model.as_deref(),
                )
                .await?,
        ),
    }
}

/// Daemon arm of `run_mail` (bd-pin-public-plane-s-easy.14): call the daemon's
/// MCP tool, print the parity JSON. CLI-only commands with no daemon tool
/// (overseer) fail loud rather than silently using a second mailbox.
pub async fn run_mail_daemon(dc: &DaemonClient, cmd: &MailCommand) -> anyhow::Result<()> {
    match cmd {
        MailCommand::Send(args) => {
            daemon_tool(
                dc,
                "send_message",
                json!({
                    "project_key": args.project,
                    "sender_name": args.from,
                    "to": args.to,
                    "subject": args.subject,
                    "body_md": args.body,
                    "cc": args.cc,
                    "bcc": args.bcc,
                    "importance": args.importance,
                    "ack_required": args.ack,
                    "thread_id": args.thread,
                    "sender_token": args.sender_token,
                }),
            )
            .await
        }
        MailCommand::Overseer(_) => anyhow::bail!(
            "mail overseer is a local-operator command (ADR-0004 Ship 5) — the daemon has no \
             overseer tool; run it with the daemon URL unset (embedded mode)"
        ),
        MailCommand::Reply(args) => {
            daemon_tool(
                dc,
                "reply_message",
                json!({
                    "project_key": args.project,
                    "sender_name": args.from,
                    "to": args.to,
                    "subject": args.subject,
                    "body_md": args.body,
                    "cc": args.cc,
                    "bcc": args.bcc,
                    "importance": args.importance,
                    "ack_required": args.ack,
                    "thread_id": args.thread,
                    "sender_token": args.sender_token,
                }),
            )
            .await
        }
        MailCommand::Inbox(args) => {
            daemon_tool(
                dc,
                "fetch_inbox",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "unread_only": args.unread,
                    "limit": args.limit,
                    "include_bodies": args.bodies,
                    "urgent_only": false,
                    "since_ts": args.since,
                    "thread_id": args.thread_id,
                }),
            )
            .await
        }
        MailCommand::Ack(args) => {
            daemon_tool(
                dc,
                "acknowledge_message",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "message_id": args.id,
                }),
            )
            .await
        }
        MailCommand::Read(args) => {
            daemon_tool(
                dc,
                "mark_message_read",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "message_id": args.id,
                }),
            )
            .await
        }
        MailCommand::Search(args) => {
            daemon_tool(
                dc,
                "search_messages",
                json!({
                    "project_key": args.project,
                    "query": args.query,
                    "limit": args.limit,
                    "ranking": args.ranking,
                    "sender": args.sender,
                    "importance": args.importance,
                    "thread_id": args.thread,
                    "since": args.since,
                    "until": args.until,
                    "include_body_md": args.bodies,
                }),
            )
            .await
        }
        MailCommand::ThreadSummarize(args) => {
            daemon_tool(
                dc,
                "summarize_thread",
                json!({
                    "project_key": args.project,
                    "thread_id": args.thread,
                    "include_examples": args.examples,
                    "llm_mode": args.llm_mode,
                    "llm_model": args.llm_model,
                }),
            )
            .await
        }
    }
}

pub async fn run_reserve(mailbox: &Mailbox, cmd: &ReserveCommand) -> anyhow::Result<()> {
    match cmd {
        ReserveCommand::Acquire(args) => emit(
            mailbox
                .file_reservation_paths(
                    &args.project,
                    &args.agent,
                    &args.paths,
                    args.ttl_secs,
                    args.exclusive,
                    args.reason.as_deref(),
                )
                .await?,
        ),
        ReserveCommand::Conflicts(args) => emit(
            mailbox
                .check_file_reservation_conflicts(&args.project, &args.agent, &args.paths)
                .await?,
        ),
        ReserveCommand::Renew(args) => emit(
            mailbox
                .renew_file_reservations(&args.project, &args.agent, &args.paths, args.extend_secs)
                .await?,
        ),
        ReserveCommand::Release(args) => emit(
            mailbox
                .release_file_reservations(&args.project, &args.agent, &args.paths)
                .await?,
        ),
        ReserveCommand::ForceRelease(args) => {
            mailbox
                .force_release_file_reservation(
                    &args.project,
                    &args.agent,
                    &args.path,
                    args.holder.as_deref(),
                    args.note.as_deref(),
                    args.notify_previous,
                )
                .await?;
            print_json(&json!({ "ok": true }))
        }
        ReserveCommand::RequestRelease(args) => emit(
            mailbox
                .request_release(&RequestRelease {
                    project_slug: args.project.clone(),
                    sender_name: args.agent.clone(),
                    target_agent: args.target.clone(),
                    reservation_id: args.reservation_id.clone(),
                    ask: parse_release_ask(&args.ask)?,
                    note: args.note.clone(),
                })
                .await?,
        ),
        ReserveCommand::Respond(args) => emit(
            mailbox
                .respond_request_release(&ReleaseResponse {
                    project_slug: args.project.clone(),
                    sender_name: args.agent.clone(),
                    request_message_id: args.request_id.clone(),
                    decision: parse_release_decision(&args.decision)?,
                    note: args.note.clone(),
                })
                .await?,
        ),
    }
}

/// Parse the CLI `--ask` value: `release` | `defer:<secs>`.
fn parse_release_ask(s: &str) -> anyhow::Result<ReleaseAsk> {
    match s {
        "release" => Ok(ReleaseAsk::Release),
        _ => s
            .strip_prefix("defer:")
            .and_then(|secs| secs.parse::<u64>().ok())
            .map(|defer_secs| ReleaseAsk::Defer { defer_secs })
            .ok_or_else(|| anyhow::anyhow!("invalid --ask {s:?} — use release or defer:<secs>")),
    }
}

/// Parse the CLI `--decision` value: `agreed` | `denied` | `deferred:<secs>`.
fn parse_release_decision(s: &str) -> anyhow::Result<ReleaseDecision> {
    match s {
        "agreed" => Ok(ReleaseDecision::Agreed),
        "denied" => Ok(ReleaseDecision::Denied),
        _ => s
            .strip_prefix("deferred:")
            .and_then(|secs| secs.parse::<u64>().ok())
            .map(|defer_secs| ReleaseDecision::Deferred { defer_secs })
            .ok_or_else(|| {
                anyhow::anyhow!("invalid --decision {s:?} — use agreed, denied, or deferred:<secs>")
            }),
    }
}

/// Daemon arm of `run_reserve` (bd-pin-public-plane-s-easy.14).
pub async fn run_reserve_daemon(dc: &DaemonClient, cmd: &ReserveCommand) -> anyhow::Result<()> {
    match cmd {
        ReserveCommand::Acquire(args) => {
            daemon_tool(
                dc,
                "file_reservation_paths",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "paths": args.paths,
                    "ttl_seconds": args.ttl_secs,
                    "exclusive": args.exclusive,
                    "reason": args.reason,
                }),
            )
            .await
        }
        ReserveCommand::Conflicts(args) => {
            daemon_tool(
                dc,
                "check_file_reservation_conflicts",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "paths": args.paths,
                }),
            )
            .await
        }
        ReserveCommand::Renew(args) => {
            daemon_tool(
                dc,
                "renew_file_reservations",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "extend_seconds": args.extend_secs,
                    "paths": args.paths,
                }),
            )
            .await
        }
        ReserveCommand::Release(args) => {
            daemon_tool(
                dc,
                "release_file_reservations",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "paths": args.paths,
                }),
            )
            .await
        }
        ReserveCommand::ForceRelease(args) => {
            daemon_tool(
                dc,
                "force_release_file_reservation",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "path": args.path,
                    "holder": args.holder,
                    "note": args.note,
                    "notify_previous": args.notify_previous,
                }),
            )
            .await
        }
        ReserveCommand::RequestRelease(_) | ReserveCommand::Respond(_) => anyhow::bail!(
            "reserve request-release/respond are CLI-only negotiation (no daemon tool) — run \
             with the daemon URL unset (embedded mode)"
        ),
    }
}

/// Daemon arm of `run_slot` (bd-pin-public-plane-s-easy.14).
pub async fn run_slot_daemon(dc: &DaemonClient, cmd: &SlotCommand) -> anyhow::Result<()> {
    match cmd {
        SlotCommand::Acquire(args) => {
            daemon_tool(
                dc,
                "acquire_build_slot",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "slot": args.slot,
                    "ttl_seconds": args.ttl_secs,
                    "exclusive": args.exclusive,
                }),
            )
            .await
        }
        SlotCommand::Renew(args) => {
            daemon_tool(
                dc,
                "renew_build_slot",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "slot": args.slot,
                    "extend_seconds": args.extend_secs,
                }),
            )
            .await
        }
        SlotCommand::Release(args) => {
            daemon_tool(
                dc,
                "release_build_slot",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "slot": args.slot,
                }),
            )
            .await
        }
    }
}

/// Daemon arm of `run_contact` (bd-pin-public-plane-s-easy.14).
pub async fn run_contact_daemon(dc: &DaemonClient, cmd: &ContactCommand) -> anyhow::Result<()> {
    match cmd {
        ContactCommand::Request(args) => {
            daemon_tool(
                dc,
                "request_contact",
                json!({
                    "project_key": args.project,
                    "from_agent": args.from,
                    "to_agent": args.to,
                    "to_project": args.to_project,
                    "reason": args.reason,
                }),
            )
            .await
        }
        ContactCommand::Respond(args) => {
            daemon_tool(
                dc,
                "respond_contact",
                json!({
                    "project_key": args.project,
                    "to_agent": args.to,
                    "from_agent": args.from,
                    "from_project": args.from_project,
                    "accept": args.accept,
                }),
            )
            .await
        }
        ContactCommand::List(args) => {
            daemon_tool(
                dc,
                "list_contacts",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                }),
            )
            .await
        }
        ContactCommand::Policy(args) => {
            let policy = match args.policy {
                PolicyArg::Open => "open",
                PolicyArg::Auto => "auto",
                PolicyArg::ContactsOnly => "contacts_only",
                PolicyArg::BlockAll => "block_all",
            };
            daemon_tool(
                dc,
                "set_contact_policy",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "policy": policy,
                }),
            )
            .await
        }
    }
}

pub async fn run_slot(mailbox: &Mailbox, cmd: &SlotCommand) -> anyhow::Result<()> {
    match cmd {
        SlotCommand::Acquire(args) => emit(
            mailbox
                .acquire_build_slot(
                    &args.project,
                    &args.agent,
                    &args.slot,
                    args.ttl_secs,
                    args.exclusive,
                )
                .await?,
        ),
        SlotCommand::Renew(args) => emit(
            mailbox
                .renew_build_slot(&args.project, &args.agent, &args.slot, args.extend_secs)
                .await?,
        ),
        SlotCommand::Release(args) => emit(
            mailbox
                .release_build_slot(&args.project, &args.agent, &args.slot)
                .await?,
        ),
    }
}

pub async fn run_contact(mailbox: &Mailbox, cmd: &ContactCommand) -> anyhow::Result<()> {
    match cmd {
        ContactCommand::Request(args) => {
            let rc = RequestContact {
                project_slug: args.project.clone(),
                from_agent: args.from.clone(),
                to_agent: args.to.clone(),
                to_project: args.to_project.clone(),
                reason: args.reason.clone(),
            };
            emit(mailbox.request_contact(&rc).await?)
        }
        ContactCommand::Respond(args) => {
            let rc = RespondContact {
                project_slug: args.project.clone(),
                to_agent: args.to.clone(),
                from_agent: args.from.clone(),
                accept: args.accept,
                from_project: args.from_project.clone(),
            };
            emit(mailbox.respond_contact(&rc).await?)
        }
        ContactCommand::List(args) => {
            emit(mailbox.list_contacts(&args.project, &args.agent).await?)
        }
        ContactCommand::Policy(args) => {
            let policy = match args.policy {
                PolicyArg::Open => ContactPolicy::Open,
                PolicyArg::Auto => ContactPolicy::Auto,
                PolicyArg::ContactsOnly => ContactPolicy::ContactsOnly,
                PolicyArg::BlockAll => ContactPolicy::BlockAll,
            };
            emit(
                mailbox
                    .set_contact_policy(&args.project, &args.agent, policy)
                    .await?,
            )
        }
    }
}

pub async fn run_guard(mailbox: &Mailbox, cmd: &GuardCommand) -> anyhow::Result<()> {
    match cmd {
        GuardCommand::Install(args) => {
            let hook_path =
                mailbox.install_precommit_guard(Path::new(&args.repo), &args.project)?;
            print_json(&json!({ "hook_path": hook_path.display().to_string() }))
        }
        GuardCommand::Uninstall(args) => {
            emit(mailbox.uninstall_precommit_guard(Path::new(&args.repo))?)
        }
        GuardCommand::Check(args) => {
            let agent = guard_agent(args);
            let staged = toron_core::mailbox::guard::staged_paths_in(&args.repo);
            let reservations = mailbox
                .active_reservations_all_aliases(&args.project)
                .await?
                .into_iter()
                .map(|r| toron_core::mailbox::guard::Reservation {
                    holder: r.agent_name,
                    pattern: r.path_pattern,
                    exclusive: r.exclusive,
                })
                .collect::<Vec<_>>();
            finish_guard_check(args, agent.as_deref(), &staged, &reservations)
        }
    }
}

/// Resolve the committing agent from `--as` / env (shared embedded+daemon).
fn guard_agent(args: &crate::cli::GuardCheckArgs) -> Option<String> {
    args.as_agent
        .clone()
        .or_else(|| std::env::var("TORON_AGENT").ok())
        .or_else(|| std::env::var("FLYWHEEL_MAIL_AS").ok())
        // bd-2dox: AGENT_NAME is the mcp-agent-mail convention the
        // chain-runner plugin enforces; the strict guard's fail-closed
        // message points at it, so it must resolve here too.
        .or_else(|| std::env::var("AGENT_NAME").ok())
}

/// Shared tail of `guard check` (strict + legacy paths, exit codes).
fn finish_guard_check(
    args: &crate::cli::GuardCheckArgs,
    agent: Option<&str>,
    staged: &[String],
    reservations: &[toron_core::mailbox::guard::Reservation],
) -> anyhow::Result<()> {
    // bd-2dox strict sweep-guard: --strict or TORON_GUARD_STRICT
    // non-"0" enables; TORON_GUARD_STRICT=0 is the operator escape
    // hatch (logged, wins over the flag).
    let strict = match std::env::var("TORON_GUARD_STRICT") {
        Ok(v) if v == "0" => {
            eprintln!(
                "toron: TORON_GUARD_STRICT=0 — strict guard disabled for this session (operator escape hatch)"
            );
            false
        }
        Ok(_) => true,
        Err(_) => args.strict,
    };
    if strict {
        use toron_core::mailbox::guard::{StrictBlockKind, guard_check_strict};
        let blocked = guard_check_strict(
            agent,
            staged,
            reservations,
            toron_core::mailbox::guard::SHARED_SAFE_PATHS,
        );
        if !blocked.is_empty() {
            let summary: Vec<String> = blocked
                .iter()
                .map(|b| match &b.kind {
                    StrictBlockKind::HeldBy(h) => format!("{} (held by {h})", b.path),
                    StrictBlockKind::Unreserved => {
                        format!("{} (no active reservation)", b.path)
                    }
                    StrictBlockKind::Unattributed => b.path.clone(),
                })
                .collect();
            if agent.is_none() {
                eprintln!(
                    "toron: strict guard: unattributed commit blocked — set AGENT_NAME=<pin> (or TORON_AGENT / FLYWHEEL_MAIL_AS)"
                );
            } else {
                eprintln!(
                    "toron: strict guard blocked staged paths: {}",
                    summary.join(", ")
                );
            }
        }
        let out = json!({
            "strict": true,
            "blocked": blocked.iter().map(|b| json!({
                "path": b.path,
                "reason": match &b.kind {
                    StrictBlockKind::Unattributed => "unattributed",
                    StrictBlockKind::HeldBy(_) => "held-by",
                    StrictBlockKind::Unreserved => "unreserved",
                },
                "holder": match &b.kind {
                    StrictBlockKind::HeldBy(h) => Some(h),
                    _ => None,
                },
            })).collect::<Vec<_>>(),
            "agent": agent,
            "project": args.project,
        });
        print_json(&out)?;
        // Explicit exit both ways: the demo mailbox keeps a
        // background thread alive after the tokio runtime shuts down,
        // which would HANG a pre-commit hook.
        std::process::exit(if blocked.is_empty() { 0 } else { 1 });
    }
    let blocked = toron_core::mailbox::guard::guard_check_blocked(agent, staged, reservations);
    let out = json!({
        "blocked": blocked,
        "agent": agent,
        "project": args.project,
    });
    print_json(&out)?;
    // Explicit exit both ways: the demo mailbox keeps a background
    // thread alive after the tokio runtime shuts down, which would
    // HANG a pre-commit hook. A hook must terminate with the exit
    // code of the check, never dangle.
    std::process::exit(
        if out["blocked"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true)
        {
            0
        } else {
            1
        },
    )
}

/// Daemon arm of `run_guard` (bd-pin-public-plane-s-easy.14).
///
/// Install/uninstall are LOCAL git-hook operations (no mailbox store access
/// — they only write `.git/hooks/pre-commit`); they work identically in
/// daemon mode. `check` reads the reservation roster from the daemon's
/// `toron://reservations/{project}` + `toron://agents/{project}` resources
/// (the writer's view — never a second in-process mailbox), then runs the
/// guard logic locally (staged paths are local by nature).
pub async fn run_guard_daemon(dc: &DaemonClient, cmd: &GuardCommand) -> anyhow::Result<()> {
    match cmd {
        GuardCommand::Install(args) => {
            let hook_path = toron_core::mailbox::guard::install_precommit_guard(
                Path::new(&args.repo),
                &args.project,
            )?;
            print_json(&json!({ "hook_path": hook_path.display().to_string() }))
        }
        GuardCommand::Uninstall(args) => {
            let removed =
                toron_core::mailbox::guard::uninstall_precommit_guard(Path::new(&args.repo))?;
            print_json(&json!({ "removed": removed }))
        }
        GuardCommand::Check(args) => {
            let agent = guard_agent(args);
            let staged = toron_core::mailbox::guard::staged_paths_in(&args.repo);
            let reservations = daemon_guard_roster(dc, &args.project)
                .await
                .with_context(|| {
                    format!(
                        "guard check: cannot list reservations for project {} from the daemon \
                         (is `toron serve --mcp-http` running?)",
                        args.project
                    )
                })?;
            finish_guard_check(args, agent.as_deref(), &staged, &reservations)
        }
    }
}

/// Build the guard reservation roster from the daemon's reservations +
/// agents resources (bd-pin-public-plane-s-easy.14).
///
/// The reservations resource returns raw kind-30031 events; the agents
/// resource maps npub → name. Held leases only: intent/released/revoked and
/// expired events are dropped, mirroring the server's held view.
async fn daemon_guard_roster(
    dc: &DaemonClient,
    project: &str,
) -> anyhow::Result<Vec<toron_core::mailbox::guard::Reservation>> {
    let res = dc
        .read_resource(&format!("toron://reservations/{project}"))
        .await?;
    let agents = dc
        .read_resource(&format!("toron://agents/{project}"))
        .await?;
    // npub → agent name (agents resource: [{name, npub, ...}]).
    let name_by_npub: std::collections::HashMap<String, String> = agents
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| {
            Some((
                a.get("npub")?.as_str()?.to_string(),
                a.get("name")?.as_str()?.to_string(),
            ))
        })
        .collect();
    let now_secs = chrono::Utc::now().timestamp();
    let mut out = Vec::new();
    let events = res.get("events").and_then(|v| v.as_array());
    let Some(events) = events else {
        return Ok(out);
    };
    for ev in events {
        // Skip intent/released/revoked (server lease_status semantics).
        let content = ev.get("content").and_then(|v| v.as_str()).unwrap_or("");
        if content.contains("intent")
            || content.contains("status:released")
            || content.contains("\"status\":\"released\"")
            || content.contains("status:revoked")
            || content.contains("\"status\":\"revoked\"")
        {
            continue;
        }
        // Skip expired (NIP-40 expiration tag < now).
        if let Some(exp) = ev.get("tags").and_then(|v| v.as_array()).and_then(|tags| {
            tags.iter().find_map(|t| {
                let a = t.as_array()?;
                (a.first().and_then(|s| s.as_str()) == Some("expiration"))
                    .then(|| {
                        a.get(1)
                            .and_then(|s| s.as_str())
                            .and_then(|s| s.parse::<i64>().ok())
                    })
                    .flatten()
            })
        }) && exp < now_secs
        {
            continue;
        }
        // Path from the d-tag (`{project}:{path}` — server split_once ':').
        let path = ev
            .get("tags")
            .and_then(|v| v.as_array())
            .and_then(|tags| {
                tags.iter().find_map(|t| {
                    let a = t.as_array()?;
                    (a.first().and_then(|s| s.as_str()) == Some("d"))
                        .then(|| a.get(1).and_then(|s| s.as_str()).map(|s| s.to_string()))
                        .flatten()
                })
            })
            .and_then(|d| d.split_once(':').map(|(_, p)| p.to_string()).or(Some(d)))
            .unwrap_or_default();
        let exclusive =
            content.contains("\"exclusive\":true") || !content.contains("\"exclusive\":false");
        let npub = ev.get("pubkey").and_then(|v| v.as_str()).unwrap_or("");
        let holder = name_by_npub
            .get(npub)
            .cloned()
            .unwrap_or_else(|| npub.chars().take(8).collect());
        out.push(toron_core::mailbox::guard::Reservation {
            holder,
            pattern: path,
            exclusive,
        });
    }
    Ok(out)
}
