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

use serde_json::json;
use toron_core::mailbox::{
    ContactPolicy, FetchInbox, Mailbox, RequestContact, RespondContact, SearchMessages, SendMessage,
};
use toron_mcp::ToJson;

use crate::cli::{
    ContactCommand, GuardCommand, MailCommand, PolicyArg, ReserveCommand, SlotCommand,
};

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
            let agent = args
                .as_agent
                .clone()
                .or_else(|| std::env::var("TORON_AGENT").ok())
                .or_else(|| std::env::var("FLYWHEEL_MAIL_AS").ok())
                // bd-2dox: AGENT_NAME is the mcp-agent-mail convention the
                // chain-runner plugin enforces; the strict guard's fail-closed
                // message points at it, so it must resolve here too.
                .or_else(|| std::env::var("AGENT_NAME").ok());
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
                    agent.as_deref(),
                    &staged,
                    &reservations,
                    toron_core::mailbox::guard::SHARED_SAFE_PATHS,
                );
                if !blocked.is_empty() {
                    let summary: Vec<String> = blocked
                        .iter()
                        .map(|b| match &b.kind {
                            StrictBlockKind::HeldBy(h) => {
                                format!("{} (held by {h})", b.path)
                            }
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
            let blocked = toron_core::mailbox::guard::guard_check_blocked(
                agent.as_deref(),
                &staged,
                &reservations,
            );
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
    }
}
