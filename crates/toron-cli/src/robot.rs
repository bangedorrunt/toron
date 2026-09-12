// governed-by: ADR-0025
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
use serde_json::{Value, json};
use toron_core::mailbox::{
    ContactPolicy, FetchInbox, Mailbox, ReleaseAsk, ReleaseDecision, ReleaseResponse,
    RequestContact, RequestRelease, RespondContact, SearchMessages, SendMessage,
};
use toron_mcp::ToJson;

use crate::cli::{
    AckSlaArgs, AgentCommand, ContactCommand, GuardCommand, MailCommand, PolicyArg, ReserveCommand,
    SlotCommand,
};
use crate::mailbox_source::DaemonClient;

/// Call a daemon MCP tool and print its text content (the parity JSON).
async fn daemon_tool(dc: &DaemonClient, tool: &str, args: serde_json::Value) -> anyhow::Result<()> {
    let out = dc.call_tool(tool, args).await?;
    print_json(&out)
}

/// [`daemon_tool`] with an explicit deadline (bd-gtqe): interactive robot
/// paths like `reserve respond` must complete or fail loudly inside seconds,
/// never park silently on an unresponsive daemon.
async fn daemon_tool_within(
    dc: &DaemonClient,
    tool: &str,
    args: serde_json::Value,
    deadline: std::time::Duration,
) -> anyhow::Result<()> {
    let out = dc.call_tool_within(tool, args, deadline).await?;
    print_json(&out)
}

/// Deadline for `reserve respond` (bd-gtqe VERIFY): completes or errors <5s.
const RESPOND_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// Print a result's MCP-parity JSON shape to stdout (always pretty JSON).
fn emit<T: ToJson>(result: T) -> anyhow::Result<()> {
    print_json(&result.to_json())
}

/// Pretty-print a raw JSON value (hand-built parity shapes).
fn print_json(value: &serde_json::Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn build_send_tags(
    corr: Option<String>,
    wake: Option<String>,
    priority: Option<String>,
    audience: Option<String>,
    result_path: Option<String>,
    verdict: Option<String>,
) -> anyhow::Result<Vec<toron_core::nostr::Tag>> {
    use std::str::FromStr;
    use toron_core::mailbox::send_tags::{Audience, Priority, SendTags, Verdict, Wake};
    let wake_typed = wake
        .map(|s| Wake::from_str(&s).map_err(|e| anyhow::anyhow!(e.message)))
        .transpose()?;
    let priority_typed = priority
        .map(|s| Priority::from_str(&s).map_err(|e| anyhow::anyhow!(e.message)))
        .transpose()?;
    let audience_typed = audience
        .map(|s| Audience::from_str(&s).map_err(|e| anyhow::anyhow!(e.message)))
        .transpose()?;
    let verdict_typed = verdict
        .map(|s| Verdict::from_str(&s).map_err(|e| anyhow::anyhow!(e.message)))
        .transpose()?;
    let tags = SendTags::from_fields(
        corr,
        wake_typed,
        priority_typed,
        audience_typed,
        result_path,
        verdict_typed,
    )
    .map_err(|e| anyhow::anyhow!(e.message))?;
    Ok(tags.to_tags())
}

pub async fn run_mail(mailbox: &Mailbox, cmd: &MailCommand) -> anyhow::Result<()> {
    match cmd {
        MailCommand::Send(args) => {
            let mut extra_tags = build_send_tags(
                args.corr.clone(),
                args.wake.clone(),
                args.priority.clone(),
                args.audience.clone(),
                args.result_path.clone(),
                args.verdict.clone(),
            )?;
            // ADR-0010 D9 (bd-l0fz): --resolve-bead stamps the structural
            // done-hold tag. SendTags::parse validates it at the send boundary
            // (non-empty); the guard matches the TAG, not the subject.
            if let Some(bd) = args.resolve_bead.as_deref() {
                extra_tags.push(
                    toron_core::nostr::Tag::parse(["bead", bd.trim()])
                        .map_err(|e| anyhow::anyhow!("bead tag: {e}"))?,
                );
            }
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
                extra_tags,
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
            let extra_tags = build_send_tags(
                args.corr.clone(),
                args.wake.clone(),
                args.priority.clone(),
                args.audience.clone(),
                args.result_path.clone(),
                None,
            )?;
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
                extra_tags,
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
        MailCommand::Whois(args) => emit(mailbox.whois(&args.project, &args.name).await?),
    }
}

/// Daemon arm of `run_mail` (bd-pin-public-plane-s-easy.14): call the daemon's
/// MCP tool, print the parity JSON. CLI-only commands with no daemon tool
/// (overseer) fail loud rather than silently using a second mailbox.
/// `reserve request-release`/`respond`/`transfer` compose existing tools
/// (`send_message` / `reply_message` / `file_reservation_paths`).
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
                    "corr": args.corr,
                    "wake": args.wake,
                    "priority": args.priority,
                    "audience": args.audience,
                    "result_path": args.result_path,
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
                    "corr": args.corr,
                    "wake": args.wake,
                    "priority": args.priority,
                    "audience": args.audience,
                    "result_path": args.result_path,
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
        MailCommand::Whois(args) => {
            daemon_tool(
                dc,
                "whois",
                json!({
                    "project_key": args.project,
                    "agent_name": args.name,
                }),
            )
            .await
        }
    }
}

/// Pane mail-identity stamp shared by both bootstrap arms (bd-6f57 direct,
/// bd-8xwk daemon): display-only `pane.report_metadata` (pin -> pane).
/// Best-effort — never fails bootstrap: herdr may be down or the pane flag
/// absent (detached/CI). Explicit `--pane-id` wins, else `$HERDR_PANE_ID`
/// injected by herdr into managed panes. Returns the stamped pane id.
fn stamp_bootstrap_pane(
    pane_flag: Option<String>,
    pin: &str,
    project: &str,
    npub: &str,
) -> Option<String> {
    let pane_id = pane_flag
        .or_else(|| std::env::var("HERDR_PANE_ID").ok())
        .filter(|p| !p.is_empty())?;
    if npub.is_empty() {
        return None;
    }
    match toron_core::backend::herdr::HerdrBackend::connect(None) {
        Ok(backend) => match backend.client().pane_report_metadata(&pane_id, pin, project, npub) {
            Ok(_) => Some(pane_id),
            Err(e) => {
                eprintln!("toron: pane stamp skipped ({e})");
                None
            }
        },
        Err(e) => {
            eprintln!("toron: pane stamp skipped ({e})");
            None
        }
    }
}

/// One-step session bootstrap: idempotent identity upsert, then an optional
/// reserve in the same call. CLI-only composition over existing mailbox
/// methods (mirrors macro_start_session minus ensure/inbox); no new MCP tool.
pub async fn run_agent(mailbox: &Mailbox, cmd: &AgentCommand) -> anyhow::Result<()> {
    match cmd {
        AgentCommand::Bootstrap(args) => {
            let profile = toron_core::nostr::identity::AgentProfile {
                name: args.agent.clone(),
                program: args.program.clone(),
                model: args.model.clone(),
                task_description: args.task.clone(),
                persona: None,
            };
            let agent = mailbox.register_agent(&args.project, &profile).await?;
            let stamped = stamp_bootstrap_pane(
                args.pane_id.clone(),
                &agent.name,
                &agent.project_slug,
                &agent.npub,
            );
            let reservation = if args.paths.is_empty() {
                serde_json::Value::Null
            } else {
                let res = mailbox
                    .file_reservation_paths(
                        &args.project,
                        &args.agent,
                        &args.paths,
                        args.ttl_secs,
                        args.exclusive,
                        args.reason.as_deref(),
                    )
                    .await?;
                serde_json::to_value(&res)?
            };
            print_json(&json!({
                "agent": agent.name,
                "project": agent.project_slug,
                "npub": agent.npub,
                "key_path": agent.key_path,
                "created": agent.created,
                "pane_id": stamped,
                "reservation": reservation,
                "next": "export AGENT_NAME=<pin> (or pass --as on every call); reserve before edit — the strict guard blocks unattributed or unreserved commits",
            }))
        }
    }
}

/// Daemon arm of `run_agent`: same composition over MCP tools.
pub async fn run_agent_daemon(dc: &DaemonClient, cmd: &AgentCommand) -> anyhow::Result<()> {
    match cmd {
        AgentCommand::Bootstrap(args) => {
            let registration = dc
                .call_tool(
                    "register_agent",
                    json!({
                        "project_key": args.project,
                        "name": args.agent,
                        "program": args.program,
                        "model": args.model,
                        "task_description": args.task,
                    }),
                )
                .await?;
            let reservation = if args.paths.is_empty() {
                serde_json::Value::Null
            } else {
                dc.call_tool(
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
                .await?
            };
            // bd-8xwk: client-side pane stamp — the daemon has no pane
            // context (and the MCP register_agent schema is frozen), so the
            // CLI stamps from its own pane env, same best-effort rule as the
            // direct arm above.
            let daemon_stamped = stamp_bootstrap_pane(
                args.pane_id.clone(),
                &args.agent,
                &args.project,
                registration.get("npub").and_then(Value::as_str).unwrap_or_default(),
            );
            print_json(&json!({
                "agent": args.agent,
                "project": args.project,
                "registration": registration,
                "reservation": reservation,
                "pane_id": daemon_stamped,
                "next": "export AGENT_NAME=<pin> (or pass --as on every call); reserve before edit — the strict guard blocks unattributed or unreserved commits",
            }))
        }
    }
}

/// Ack-SLA scan+publish (bd-0zcr.2 G1): the CLI verb over the mailbox
/// primitive — one `ack-sla-expired` wake per overdue ack-required post.
/// Prints the expired post ids so the doorbell daemon can close its G1 gap.
pub async fn run_ack_sla(mailbox: &Mailbox, args: &AckSlaArgs) -> anyhow::Result<()> {
    let expired = mailbox
        .check_ack_sla_expired(&args.project, args.sla_secs)
        .await?;
    print_json(&json!({
        "project": args.project,
        "sla_secs": args.sla_secs.unwrap_or(30 * 60),
        "expired": expired,
        "count": expired.len(),
    }))
}

/// Daemon arm of `run_ack_sla`: no daemon MCP tool exists (ADR-0020 wave 1
/// freeze: CLI-only nouns stay CLI-only), so the scan runs against the
/// shared durable XDG root the daemon itself uses — the same store, never
/// a second one. SLA truth is sqlite (`message_reads` / `receipts`), never
/// process-local state, so the cross-process read is exact.
pub async fn run_ack_sla_daemon(_dc: &DaemonClient, args: &AckSlaArgs) -> anyhow::Result<()> {
    eprintln!("toron ack-sla: no daemon tool; scanning the shared durable root directly");
    let mb = toron_mcp::demo_mailbox().await?;
    run_ack_sla(&mb, args).await
}

/// bd-zugo / F13: a lease binds the resolved actor, but the ROSTER is what
/// `whois` and the strict commit guard attribute through. When the acting
/// name has no durable registration (`agents` row) in the project, the lease
/// lands on a ghost holder — it can hold, but transfer fails, no
/// name-routed surface reaches it, and the guard cannot map the commit back
/// to a pin. Warn loud, name the remedy; NEVER block (a key-file holder is
/// legal and this is advisory).
async fn warn_if_unregistered(mailbox: &Mailbox, project: &str, agent: &str) {
    if mailbox.agent_registered(project, agent).await {
        return;
    }
    eprintln!(
        "toron reserve: warning — `{agent}` has no registered identity in project `{project}`. \
         This lease binds a key-file holder the roster cannot resolve, so whois/guard attribution \
         will fail and a transfer cannot reach it. Register it first: \
         `toron agent bootstrap --project {project} --as {agent}` (bd-zugo)."
    );
}

pub async fn run_reserve(mailbox: &Mailbox, cmd: &ReserveCommand) -> anyhow::Result<()> {
    match cmd {
        ReserveCommand::Acquire(args) => {
            warn_if_unregistered(mailbox, &args.project, &args.agent).await;
            emit(
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
            )
        }
        ReserveCommand::Conflicts(args) => emit(
            mailbox
                .check_file_reservation_conflicts(&args.project, &args.agent, &args.paths)
                .await?,
        ),
        ReserveCommand::List(args) => {
            let rows = mailbox
                .list_file_reservations(&args.project, args.holder.as_deref(), args.path.as_deref())
                .await?;
            emit(toron_mcp::tools::reservation::LeaseBoardOut {
                project_key: args.project.clone(),
                count: rows.len() as i64,
                reservations: rows
                    .into_iter()
                    .map(|r| toron_mcp::tools::reservation::LeaseRowOut {
                        id: r.id,
                        path: r.path_pattern,
                        holder: r.agent_name,
                        exclusive: r.exclusive,
                        reason: r.reason,
                        expires_ts_us: r.expires_ts_us,
                    })
                    .collect(),
            })
        }
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
        ReserveCommand::ReleaseByReason(args) => emit(
            mailbox
                .release_by_reason(&args.project, &args.agent, &args.bead_id, !args.no_notify)
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
        ReserveCommand::Transfer(args) => emit(
            mailbox
                .transfer_reservation(
                    &args.project,
                    &args.from,
                    &args.to,
                    &args.path,
                    args.reason.as_deref(),
                )
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
            // bd-zugo: same ghost-holder warning on the daemon arm — the
            // shared durable root is readable from here (`demo_mailbox`).
            if let Ok(mb) = toron_mcp::demo_mailbox().await {
                warn_if_unregistered(&mb, &args.project, &args.agent).await;
            }
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
        ReserveCommand::List(args) => {
            daemon_tool(
                dc,
                "list_file_reservations",
                json!({
                    "project_key": args.project,
                    "holder": args.holder,
                    "path": args.path,
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
        ReserveCommand::ReleaseByReason(args) => {
            daemon_tool(
                dc,
                "release_file_reservations_by_reason",
                json!({
                    "project_key": args.project,
                    "agent_name": args.agent,
                    "bead_id": args.bead_id,
                    "notify_previous": !args.no_notify,
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
        ReserveCommand::RequestRelease(args) => {
            daemon_tool(
                dc,
                "send_message",
                json!({
                    "project_key": args.project,
                    "sender_name": args.agent,
                    "to": [args.target.clone()],
                    "subject": "[request-release]",
                    "body_md": args.note.clone().unwrap_or_default(),
                    "reservation_id": args.reservation_id,
                    "release_ask": args.ask,
                    "reservation_note": args.note,
                }),
            )
            .await
        }
        ReserveCommand::Respond(args) => {
            daemon_tool_within(
                dc,
                "reply_message",
                json!({
                    "project_key": args.project,
                    "sender_name": args.agent,
                    "to": [args.agent.clone()],
                    "subject": "[request-release-response]",
                    "body_md": args.note.clone().unwrap_or_default(),
                    "request_message_id": args.request_id,
                    "release_decision": args.decision,
                    "reservation_note": args.note,
                }),
                RESPOND_DEADLINE,
            )
            .await
        }
        ReserveCommand::Transfer(args) => {
            daemon_tool(
                dc,
                "file_reservation_paths",
                json!({
                    "project_key": args.project,
                    "agent_name": args.to,
                    "paths": [args.path.clone()],
                    "transfer_from": args.from,
                    "reason": args.reason,
                }),
            )
            .await
        }
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
        ContactCommand::Captain(args) => {
            // bd-auqj: clear the captain alias for the project.
            if args.clear {
                let previous = toron_core::mailbox::identity::clear_captain_alias_in_db(
                    &toron_core::config::ToronPaths::resolve()
                        .index_path()
                        .display()
                        .to_string(),
                    &args.project,
                )
                .await?;
                print_json(&json!({
                    "project": args.project,
                    "cleared": previous.is_some(),
                    "previous": previous,
                }))?;
                return Ok(());
            }
            // bd-captain-alias-0g8a: validate the pin against the WRITER
            // (the daemon's whois tool), then write the plain-state UPSERT
            // into the durable root's index DB directly — no second
            // in-process mailbox (guard precedent). Reads of `captain` then
            // resolve through the ONE resolver everywhere.
            let db_path = toron_core::config::ToronPaths::resolve().index_path();
            let db_path_s = db_path.display().to_string();
            // bd-lsgm: the captain fold is consulted ONLY in swarm identity
            // mode — writing it in open mode silently creates a fold that
            // can never route. Fail loud with the remedy; --force writes
            // anyway, audited to stderr.
            let mode = toron_core::mailbox::identity::project_identity_mode_in_db(
                &db_path_s,
                &args.project,
            )
            .await?;
            match &args.pin {
                Some(pin) => {
                    captain_write_gate(&mode, args.force, &args.project)?;
                    if args.force {
                        eprintln!(
                            "{}",
                            json!({
                                "audit": "captain fold written in open identity mode",
                                "project": args.project,
                                "pin": pin,
                                "routes": false,
                            })
                        );
                    }
                    let out = dc
                        .call_tool(
                            "whois",
                            json!({ "project_key": args.project, "agent_name": pin }),
                        )
                        .await?;
                    let canonical = out
                        .get("name")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "captain must be a REGISTERED adjective+noun pin — whois did \
                                 not resolve {pin:?}"
                            )
                        })?
                        .to_string();
                    toron_core::mailbox::identity::set_captain_alias_in_db(
                        &db_path.display().to_string(),
                        &args.project,
                        &canonical,
                    )
                    .await?;
                    print_json(&json!({
                        "project": args.project,
                        "captain": canonical,
                        "resolved_via": "captain",
                        "store": db_path.display().to_string(),
                    }))
                }
                None => {
                    let current = toron_core::mailbox::identity::captain_alias_in_db(
                        &db_path.display().to_string(),
                        &args.project,
                    )
                    .await?;
                    print_json(&json!({ "project": args.project, "captain": current }))
                }
            }
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
        ContactCommand::Captain(args) => {
            if args.clear {
                let previous = mailbox.clear_project_captain(&args.project).await?;
                print_json(&json!({
                    "project": args.project,
                    "cleared": previous.is_some(),
                    "previous": previous,
                }))?;
                return Ok(());
            }
            match &args.pin {
                Some(pin) => {
                    let captain = mailbox.set_project_captain(&args.project, pin).await?;
                    // Prove the fold end-to-end: the alias itself must resolve.
                    let w = mailbox.whois(&args.project, "captain").await?;
                    print_json(&json!({
                        "project": args.project,
                        "captain": captain,
                        "npub": w.npub,
                        "resolved_via": "captain",
                    }))
                }
                None => {
                    let current = mailbox.agent_name_alias(&args.project, "captain").await;
                    print_json(&json!({ "project": args.project, "captain": current }))
                }
            }
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
            let mut reservations = mailbox
                .active_reservations_all_aliases(&args.project)
                .await?
                .into_iter()
                .map(|r| toron_core::mailbox::guard::Reservation {
                    holder: r.agent_name,
                    pattern: r.path_pattern,
                    exclusive: r.exclusive,
                })
                .collect::<Vec<_>>();
            // bd-guard-crossproject-lease-er38: one lease covers every
            // spelling. Merge other-project HELD leases that escape into
            // this repo (`../<repo>/...`), translated to repo-relative
            // globs, and normalize the same spelling on own-project leases.
            if let Some(repo_name) = std::path::Path::new(&args.repo)
                .file_name()
                .and_then(|n| n.to_str())
            {
                let foreign = mailbox
                    .active_reservations_escaping_into(&args.project, repo_name)
                    .await?
                    .into_iter()
                    .map(|r| toron_core::mailbox::guard::Reservation {
                        holder: r.agent_name,
                        pattern: r.path_pattern,
                        exclusive: r.exclusive,
                    })
                    .collect::<Vec<_>>();
                merge_cross_project_leases(&mut reservations, repo_name, foreign);
            }
            finish_guard_check(args, agent.as_deref(), &staged, &reservations)
        }
        GuardCommand::Record(args) => run_guard_record(args),
    }
}

/// bd-jhes: post-commit GitFact producer — best-effort, NEVER blocks commit.
fn run_guard_record(args: &crate::cli::GuardRecordArgs) -> anyhow::Result<()> {
    let agent = guard_record_agent(args);
    let journal = args
        .journal
        .clone()
        .unwrap_or_else(toron_core::mailbox::guard::git_fact_journal_path);
    if let Some(pin) = agent.as_deref() {
        toron_core::mailbox::guard::record_git_fact_for_head_with_journal(
            Path::new(&args.repo),
            pin,
            &journal,
        );
    }
    // Best-effort: always exit 0, even when journal unwritable or repo unreadable.
    print_json(&json!({
        "recorded": agent.is_some(),
        "agent": agent,
        "repo": args.repo.display().to_string(),
        "journal": journal.display().to_string(),
        "project": args.project,
    }))?;
    std::process::exit(0);
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

fn guard_record_agent(args: &crate::cli::GuardRecordArgs) -> Option<String> {
    args.as_agent
        .clone()
        .or_else(|| std::env::var("TORON_AGENT").ok())
        .or_else(|| std::env::var("FLYWHEEL_MAIL_AS").ok())
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
    // bd-sdzw: the hatch disables the WHOLE sweep, not just strict mode —
    // a disabled guard must exit 0 with an empty report (the operator chose
    // to bypass; the legacy non-strict path must not still block after the
    // hatch). Without this, `TORON_GUARD_STRICT=0` printed the escape-hatch
    // notice then STILL exited 1 on any held-by-other path, making commits
    // fall back to --no-verify (the hatch was unusable).
    if !strict {
        print_json(&json!({
            "strict": false,
            "hatched": true,
            "blocked": [],
            "agent": agent,
            "project": args.project,
        }))?;
        std::process::exit(0);
    }
    if strict {
        use toron_core::mailbox::guard::{StrictBlockKind, guard_check_strict};
        let blocked = guard_check_strict(
            agent,
            staged,
            reservations,
            toron_core::mailbox::guard::SHARED_SAFE_PATHS,
        );
        // bd-9mb8 bead-hunk attribution: bd-* tokens ADDED by the staged
        // diff must appear in the commit message (--msg-file from the
        // commit-msg hook). Skipped when no message file was supplied.
        let bead_blocks = match &args.msg_file {
            Some(mf) => {
                let msg = std::fs::read_to_string(mf).unwrap_or_default();
                let added = toron_core::mailbox::guard::staged_added_lines_in(
                    &args.repo,
                    toron_core::mailbox::guard::SHARED_SAFE_PATHS,
                );
                toron_core::mailbox::guard::bead_hunk_violations(&added, &msg)
            }
            None => Vec::new(),
        };
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
        if !bead_blocks.is_empty() {
            let toks: Vec<String> = bead_blocks.iter().map(|b| b.token.clone()).collect();
            eprintln!(
                "toron: strict guard: unattributed bead-tagged hunks — {toks:?} appear in the staged diff but not in the commit message; add them to the message (escapes: TORON_GUARD_STRICT=0 or --no-verify)"
            );
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
            "unattributed_bead_tokens": bead_blocks
                .iter()
                .map(|b| b.token.clone())
                .collect::<Vec<_>>(),
            "agent": agent,
            "project": args.project,
        });
        print_json(&out)?;
        // Explicit exit both ways: the demo mailbox keeps a
        // background thread alive after the tokio runtime shuts down,
        // which would HANG a pre-commit hook.
        std::process::exit(if blocked.is_empty() && bead_blocks.is_empty() {
            0
        } else {
            1
        });
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
            let mut reservations =
                daemon_guard_roster(dc, &args.project)
                    .await
                    .with_context(|| {
                        format!(
                            "guard check: cannot list reservations for project {} from the daemon \
                         (is `toron serve --mcp-http` running?)",
                            args.project
                        )
                    })?;
            // bd-guard-crossproject-lease-er38: the roster resource is
            // project-scoped, so foreign-project leases escape it. Augment
            // from the durable root (the same store the daemon serves);
            // best-effort — on failure the guard degrades to the old
            // project-scoped behavior, never blocks harder.
            if let Some(repo_name) = std::path::Path::new(&args.repo)
                .file_name()
                .and_then(|n| n.to_str())
                && let Ok(mb) = toron_mcp::demo_mailbox().await
                && let Ok(foreign) = mb
                    .active_reservations_escaping_into(&args.project, repo_name)
                    .await
            {
                merge_cross_project_leases(
                    &mut reservations,
                    repo_name,
                    foreign
                        .into_iter()
                        .map(|r| toron_core::mailbox::guard::Reservation {
                            holder: r.agent_name,
                            pattern: r.path_pattern,
                            exclusive: r.exclusive,
                        }),
                );
            }
            finish_guard_check(args, agent.as_deref(), &staged, &reservations)
        }
        GuardCommand::Record(args) => run_guard_record(args),
    }
}

/// bd-guard-crossproject-lease-er38: one lease covers every spelling.
/// Append foreign-project leases whose `../<repo>/…` pattern escapes into
/// the guarded `repo_name` (translated to a repo-relative glob), then
/// normalize the same spelling on entries already present so a
/// workspace-root-relative lease covers its own repo too.
fn merge_cross_project_leases(
    reservations: &mut Vec<toron_core::mailbox::guard::Reservation>,
    repo_name: &str,
    foreign: impl IntoIterator<Item = toron_core::mailbox::guard::Reservation>,
) {
    for r in foreign {
        let Some(pattern) =
            toron_core::mailbox::guard::cross_project_pattern(&r.pattern, repo_name)
        else {
            continue;
        };
        reservations.push(toron_core::mailbox::guard::Reservation {
            holder: r.holder,
            pattern,
            exclusive: r.exclusive,
        });
    }
    for r in reservations.iter_mut() {
        if let Some(pattern) =
            toron_core::mailbox::guard::cross_project_pattern(&r.pattern, repo_name)
        {
            r.pattern = pattern;
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
    let now_secs = chrono::Utc::now().timestamp();
    Ok(resolve_daemon_roster(&res, &agents, now_secs))
}

/// Build the guard reservation roster from raw daemon resource JSON (pure,
/// testable). `res` is the `toron://reservations/{project}` document
/// (`{events: [...]}`), `agents` the `toron://agents/{project}` document.
///
/// Held leases only: intent/released/revoked and expired events are dropped,
/// mirroring the server's held view.
///
/// bd-0pjf tombstone ordering (mirrors embedded `resolve_lease_state`,
/// bd-reacquire): a revocation/release marker covers only events at the same
/// path OLDER than itself — a lease re-acquired after a force-release is
/// live again on the very next check. Without any marker the coordinate
/// resolves exactly as before (all live lease events surface).
fn resolve_daemon_roster(
    res: &serde_json::Value,
    agents: &serde_json::Value,
    now_secs: i64,
) -> Vec<toron_core::mailbox::guard::Reservation> {
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
    let events = res.get("events").and_then(|v| v.as_array());
    let Some(events) = events else {
        return Vec::new();
    };
    fn event_created_at(ev: &serde_json::Value) -> i64 {
        ev.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0)
    }
    fn event_content(ev: &serde_json::Value) -> &str {
        ev.get("content").and_then(|v| v.as_str()).unwrap_or("")
    }
    fn is_marker(content: &str) -> bool {
        content.contains("status:revoked")
            || content.contains("\"status\":\"revoked\"")
            || content.contains("status:released")
            || content.contains("\"status\":\"released\"")
    }
    fn event_path(ev: &serde_json::Value) -> String {
        // Path from the d-tag (`{project}:{path}` — server split_once ':').
        ev.get("tags")
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
            .unwrap_or_default()
    }
    // PASS 1 — tombstone cutoff per path: the newest revocation/release
    // marker's timestamp. Older events at that path are dead; newer ones
    // (re-acquired after the release) stay live.
    let mut cutoff: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for ev in events {
        if !is_marker(event_content(ev)) {
            continue;
        }
        let path = event_path(ev);
        let at = event_created_at(ev);
        cutoff
            .entry(path)
            .and_modify(|c| *c = (*c).max(at))
            .or_insert(at);
    }
    // PASS 2 — resolve live leases (markers, intents, expired, and
    // tombstoned-older events are dropped).
    let mut out = Vec::new();
    for ev in events {
        let content = event_content(ev);
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
        let path = event_path(ev);
        // bd-0pjf: tombstoned-older events stay dead even though the marker
        // itself is skipped above.
        if cutoff
            .get(&path)
            .is_some_and(|c| event_created_at(ev) <= *c)
        {
            continue;
        }
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
    out
}

/// bd-lsgm: the captain fold routes only in swarm identity mode. An open-mode
/// write is refused unless explicitly forced (the call site audits it to
/// stderr) — otherwise `contact captain` silently writes a fold that can
/// never route (ADR-0018).
pub(crate) fn captain_write_gate(
    mode: &toron_core::mailbox::naming::IdentityMode,
    force: bool,
    project: &str,
) -> anyhow::Result<()> {
    if matches!(mode, toron_core::mailbox::naming::IdentityMode::Open) && !force {
        anyhow::bail!(
            "captain fold is swarm-only (ADR-0018): project {project} runs identity_mode=open, \
             where the fold can never route — flip via ensure_project identity_mode=swarm, or \
             address the pin directly; --force overrides (audited)",
            project = project
        );
    }
    Ok(())
}

#[cfg(test)]
mod captain_open_mode_tests {
    use super::*;

    #[test]
    fn captain_open_mode_refuses_without_force() {
        let err = captain_write_gate(
            &toron_core::mailbox::naming::IdentityMode::Open,
            false,
            "flywheel",
        )
        .expect_err("open mode must refuse the fold write");
        let msg = err.to_string();
        assert!(msg.contains("swarm-only"), "{msg}");
        assert!(msg.contains("identity_mode=swarm"), "{msg}");
    }

    #[test]
    fn captain_open_mode_force_overrides() {
        captain_write_gate(
            &toron_core::mailbox::naming::IdentityMode::Open,
            true,
            "flywheel",
        )
        .expect("force overrides the open-mode refusal");
    }

    #[test]
    fn captain_swarm_mode_writes_without_force() {
        captain_write_gate(
            &toron_core::mailbox::naming::IdentityMode::Swarm,
            false,
            "toron",
        )
        .expect("swarm mode routes without force");
    }

    /// The reader defaults a fresh (migrated) index DB to Open — the mode the
    /// gate refuses by default.
    #[tokio::test]
    async fn captain_open_mode_reader_defaults_open() {
        let dir = std::env::temp_dir().join(format!("bd-lsgm-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let db = dir.join("index.sqlite").display().to_string();
        let mode = toron_core::mailbox::identity::project_identity_mode_in_db(&db, "flywheel")
            .await
            .expect("read mode");
        assert!(matches!(
            mode,
            toron_core::mailbox::naming::IdentityMode::Open
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod daemon_roster_revocation_tests {
    use super::*;
use serde_json::{Value, json};

    const HOLDER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OPERATOR: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn roster_event(
        pubkey: &str,
        created_at: i64,
        d: &str,
        content: &str,
        extra_tags: serde_json::Value,
    ) -> serde_json::Value {
        let mut tags = vec![json!(["d", d]), json!(["expiration", "9999999999"])];
        if let Some(arr) = extra_tags.as_array() {
            for t in arr {
                tags.push(t.clone());
            }
        }
        json!({
            "id": "00",
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": 30031,
            "tags": tags,
            "content": content,
            "sig": "00"
        })
    }

    fn held_lease(pubkey: &str, created_at: i64) -> serde_json::Value {
        roster_event(
            pubkey,
            created_at,
            "toron:scratch-guard.txt",
            r#"{"exclusive":true,"reason":"bd-0pjf"}"#,
            json!([]),
        )
    }

    fn force_revoke(created_at: i64) -> serde_json::Value {
        roster_event(
            OPERATOR,
            created_at,
            "toron:scratch-guard.txt",
            r#"{"status":"revoked","heuristic":"stale-holder"}"#,
            json!([["status", "revoked"]]),
        )
    }

    fn agents() -> serde_json::Value {
        json!([{ "name": "RoseDune", "npub": HOLDER }])
    }

    fn resolve(events: Vec<serde_json::Value>) -> Vec<toron_core::mailbox::guard::Reservation> {
        let res = json!({ "events": events });
        resolve_daemon_roster(&res, &agents(), 500)
    }

    /// bd-0pjf core: force_release revocation hides the lease on the very
    /// next guard check (pins the 7c98dcf behavior).
    #[test]
    fn revoke_hides_lease_on_next_check() {
        let roster = resolve(vec![held_lease(HOLDER, 100), force_revoke(200)]);
        assert!(
            roster.is_empty(),
            "revoked lease must not reach the guard: {roster:?}"
        );
    }

    /// bd-0pjf tombstone ordering (mirrors embedded `resolve_lease_state`,
    /// bd-reacquire): a revocation marker covers only OLDER events — a lease
    /// re-acquired after the force-release is live again.
    #[test]
    fn reacquire_after_revoke_is_live() {
        let roster = resolve(vec![
            held_lease(HOLDER, 100),
            force_revoke(200),
            held_lease(HOLDER, 300),
        ]);
        assert_eq!(roster.len(), 1, "re-acquired lease is live: {roster:?}");
        assert_eq!(roster[0].pattern, "scratch-guard.txt");
    }

    /// A stale revocation never blankets a newer lease at the same path.
    #[test]
    fn stale_revoke_does_not_blanket_newer_lease() {
        let roster = resolve(vec![force_revoke(100), held_lease(HOLDER, 200)]);
        assert_eq!(
            roster.len(),
            1,
            "newer lease survives stale marker: {roster:?}"
        );
    }
}
