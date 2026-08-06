// governed-by: ADR-0003

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
                    &args.id,
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
            let hook_path = mailbox.install_precommit_guard(Path::new(&args.repo))?;
            print_json(&json!({ "hook_path": hook_path.display().to_string() }))
        }
        GuardCommand::Uninstall(args) => {
            emit(mailbox.uninstall_precommit_guard(Path::new(&args.repo))?)
        }
    }
}
