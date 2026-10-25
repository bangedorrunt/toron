// governed-by: ADR-0025, ADR-0037
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

/// bd-reserve-yield-taqf: the yield board. CLI-only noun (no MCP tool, same
/// discipline as `ack-sla`), so this shape is hand-built and shared by both
/// arms — the markers plus the beads a resume must skip right now.
fn yield_board_json(
    project_key: &str,
    views: Vec<toron_core::mailbox::yields::YieldView>,
    suppressed: Vec<String>,
) -> Value {
    let yields: Vec<Value> = views
        .into_iter()
        .map(|v| {
            json!({
                "bead_id": v.bead_id,
                "holder": v.holder,
                "yielder": v.yielder,
                "note": v.note,
                "created_ts_us": v.created_ts_us,
                "holder_live": v.holder_live,
            })
        })
        .collect();
    json!({
        "project": project_key,
        "count": yields.len(),
        "yields": yields,
        "suppressed": suppressed,
    })
}

/// bd-close-releases-leases-qkbo: mark board rows whose reason bead is CLOSED
/// (CLI-only additive fields; the MCP `LeaseBoardOut` shape is untouched).
/// Best-effort: no tracker file means no annotation, never an error —
/// `reserve list` must work in any directory.
fn annotate_stale_rows(
    board: &mut Value,
    closed: &std::collections::BTreeSet<String>,
    project: &str,
) {
    let Some(rows) = board.get_mut("reservations").and_then(Value::as_array_mut) else {
        return;
    };
    for row in rows {
        let reason = row.get("reason").and_then(Value::as_str).unwrap_or("");
        if reason.is_empty() || !closed.contains(reason) {
            continue;
        }
        if let Some(obj) = row.as_object_mut() {
            obj.insert("stale".into(), Value::Bool(true));
            obj.insert(
                "stale_hint".into(),
                Value::String(format!(
                    "reason bead is closed; sweep with: toron reserve sweep --closed-beads --project {project}"
                )),
            );
        }
    }
}

/// Resolve a sweep's repo root + closed-bead set. `--closed-beads` is required
/// (it is the only sweep source) and a missing tracker fails loud: the flag
/// asked for closed-bead discovery and there is nothing to discover from.
fn sweep_closed_set(
    args: &crate::cli::ReserveSweepArgs,
) -> anyhow::Result<(std::path::PathBuf, std::collections::BTreeSet<String>)> {
    if !args.closed_beads {
        anyhow::bail!(
            "`reserve sweep` needs --closed-beads (the only sweep source: <repo>/.beads/issues.jsonl)"
        );
    }
    let repo = args
        .repo
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let issues = crate::tracker::issues_path(&repo);
    if !issues.exists() {
        anyhow::bail!(
            "no tracker at {} — run from the repo root or pass --repo",
            issues.display()
        );
    }
    let closed = crate::tracker::closed_bead_ids(&repo);
    Ok((repo, closed))
}

/// Group stale rows `(path, holder, reason)` by reason for the sweep report.
/// Shared by the embedded and daemon arms so both report the same leases.
fn group_stale_by_reason(
    rows: &[(String, String, String)],
    closed: &std::collections::BTreeSet<String>,
) -> std::collections::BTreeMap<String, Vec<(String, String)>> {
    let mut by_reason: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for (path, holder, reason) in rows {
        if reason.is_empty() || !closed.contains(reason) {
            continue;
        }
        by_reason
            .entry(reason.clone())
            .or_default()
            .push((path.clone(), holder.clone()));
    }
    by_reason
}

/// The shared sweep report shape (embedded + daemon arms).
fn print_sweep_report(
    project: &str,
    repo: &std::path::Path,
    closed_count: usize,
    by_reason: &std::collections::BTreeMap<String, Vec<(String, String)>>,
    released: u64,
) -> anyhow::Result<()> {
    let leases: Vec<Value> = by_reason
        .iter()
        .flat_map(|(reason, leases)| {
            leases.iter().map(
                move |(path, holder)| json!({"path": path, "holder": holder, "reason": reason}),
            )
        })
        .collect();
    print_json(&json!({
        "project_key": project,
        "repo": repo.display().to_string(),
        "closed_beads": closed_count,
        "stale_reasons": by_reason.keys().collect::<Vec<_>>(),
        "released": released,
        "leases": leases,
    }))
}

/// Encode the ADR-0027 E2 assignment vocabulary into rumor tags.
///
/// `pub(crate)` so the operator console's send form goes through the SAME
/// encoder as the CLI verb: a second copy is how the two surfaces end up
/// disagreeing about what `tasked` means on the wire.
pub(crate) fn build_send_tags(
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
                ack_overdue_only: false,
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
        Ok(backend) => {
            let stamped = backend
                .client()
                .pane_report_metadata(&pane_id, pin, project, npub);
            match stamped {
                Ok(_) => Some(pane_id),
                Err(e) => {
                    eprintln!("toron: pane stamp skipped ({e})");
                    None
                }
            }
        }
        Err(e) => {
            eprintln!("toron: pane stamp skipped ({e})");
            None
        }
    }
}

/// One-step session bootstrap: idempotent identity upsert, then an optional
/// reserve in the same call. CLI-only composition over existing mailbox
/// methods (mirrors macro_start_session minus ensure/inbox); no new MCP tool.
pub async fn run_agent(mailbox: &Mailbox, cmd: &AgentCommand, dry_run: bool) -> anyhow::Result<()> {
    match cmd {
        AgentCommand::Bootstrap(args) => {
            // bd-pinrule: the pin is a NAMESPACE, not a label, so the shape is
            // enforced at the door where an operator types it. Identity is
            // per-PROJECT, so one agent holds a different key in each project
            // and a name two agents can pick independently WILL collide: proven
            // live, `Buffy` is registered in two projects and resolves to two
            // different npubs, so mail sent to that name from one project is
            // invisible to the agent reading under the other (bd-xroute).
            toron_core::nostr::identity::validate_agent_pin(&args.agent)
                .map_err(anyhow::Error::msg)?;
            let profile = toron_core::nostr::identity::AgentProfile {
                name: args.agent.clone(),
                program: args.program.clone(),
                model: args.model.clone(),
                task_description: args.task.clone(),
                persona: None,
            };
            // bd-toron-bootstrap-dryrun-mutates-1-we4c: `-n/--dry-run` is a
            // GLOBAL flag whose help promises "show changes without running".
            // Minting key material under it is the one class of side effect
            // callers cannot retry away — the next real run can no longer show
            // what it would have done. Preview, never mutate.
            if dry_run {
                let plan = mailbox
                    .plan_agent_registration(&args.project, &profile)
                    .await?;
                print_json(&json!({
                    "agent": plan.name,
                    "project": plan.project_slug,
                    "key_path": plan.key_path,
                    "created": plan.created,
                    "would_reuse_key_from": mailbox
                        .pin_reuse_source(&args.project, &args.agent, args.key_from.as_deref())
                        .await?,
                    "dry_run": true,
                    "would_reserve": args.paths,
                    "next": "re-run without --dry-run to register; nothing was written",
                }))?;
                return Ok(());
            }
            let reused_from = reuse_pin_key(mailbox, args, &profile).await?;
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
                "reused_key_from": reused_from,
                "pane_id": stamped,
                "reservation": reservation,
                "next": "export AGENT_NAME=<pin> (or pass --as on every call); reserve before edit — the strict guard blocks unattributed or unreserved commits",
            }))
        }
        AgentCommand::Prune(args) => {
            // bd-4a0u: `-n/--dry-run` is a GLOBAL flag promising "show changes
            // without running" — so preview the rows doctor names and absorb
            // nothing (the mutation could not be retried away otherwise).
            if dry_run {
                let leftovers = mailbox.folded_slug_leftovers_for(&args.project).await?;
                print_json(&json!({
                    "project": args.project,
                    "as": args.agent,
                    "would_absorb": leftovers,
                    "count": leftovers.len(),
                    "dry_run": true,
                    "next": "re-run without --dry-run to absorb the listed rows; nothing was written",
                }))?;
                return Ok(());
            }
            let report = mailbox.prune_folded_slugs(&args.project).await?;
            // bd-0tbg: a held row is NOT a tidy-up. It is an identity still
            // holding reservations, and absorbing it would delete the roster
            // row those reservations are addressed by — so it is reported in
            // its own bucket with the paths, and the operator is told the
            // release comes first. Silently folding it into `kept` would tell
            // them to go register a pin, which fixes nothing.
            let held_count = report.held.len();
            print_json(&json!({
                "project": args.project,
                "as": args.agent,
                "removed": report.removed,
                "kept": report.kept,
                "held": report.held,
                "removed_count": report.removed.len(),
                "kept_count": report.kept.len(),
                "held_count": held_count,
                "next": if held_count > 0 {
                    "HELD rows still hold file reservations — release or force-release them \
                     (`toron reserve release-by-reason <bead>` or `toron reserve force-release`), \
                     then re-run `toron agent prune`. Their roster rows were kept on purpose: \
                     deleting a holder's row makes its live reservations unaddressable."
                } else if report.kept.is_empty() {
                    "no folded-slug leftovers remain — doctor's legacy_slug_identities is clean"
                } else {
                    "kept rows have NO registered canonical pin; register the pin, then re-run `toron agent prune`"
                },
            }))
        }
        AgentCommand::Rename(args) => {
            // A rename MOVES an identity; it never mints a second key for the
            // same words (bd-0tbg). The npub is unchanged, so anything the
            // identity received stays reachable — only the name it answers to
            // changes.
            let report = mailbox
                .rename_agent(&args.project, &args.old_name, &args.new_name)
                .await?;
            print_json(&json!({
                "project": report.project_slug,
                "renamed": { "from": report.old_name, "to": report.new_name },
                "npub": report.pubkey,
                "leases_repointed": report.leases_repointed,
                "next": "the npub is unchanged — mail sent to the old name is no longer \
                          routable, and the old name's leases now answer to the new one",
            }))
        }
    }
}

/// ADR-0037 W2: when the pin already holds a key in another project, prove
/// possession with that key file and register it here with the SAME key.
/// Returns the source project, or `None` when there was nothing to reuse.
async fn reuse_pin_key(
    mailbox: &Mailbox,
    args: &crate::cli::AgentBootstrapArgs,
    profile: &toron_core::nostr::identity::AgentProfile,
) -> anyhow::Result<Option<String>> {
    let Some(from) = mailbox
        .pin_reuse_source(&args.project, &args.agent, args.key_from.as_deref())
        .await?
    else {
        return Ok(None);
    };
    let proof = mailbox
        .prove_pin_possession(&from, &args.agent, &args.project)
        .await?;
    mailbox
        .register_agent_reusing(&args.project, profile, &from, &proof)
        .await?;
    Ok(Some(from))
}

/// Daemon arm of `run_agent`: same composition over MCP tools.
pub async fn run_agent_daemon(
    dc: &DaemonClient,
    cmd: &AgentCommand,
    dry_run: bool,
) -> anyhow::Result<()> {
    match cmd {
        AgentCommand::Bootstrap(args) => {
            // bd-pinrule: same gate on the daemon arm, or the rule holds only
            // when the CLI happens to run embedded.
            toron_core::nostr::identity::validate_agent_pin(&args.agent)
                .map_err(anyhow::Error::msg)?;
            // bd-toron-bootstrap-dryrun-mutates-1-we4c: the daemon arm must not
            // mutate under `--dry-run` either. The daemon holds the mailbox, so
            // the CLI cannot canonicalize the slug or read the key path without
            // a new MCP verb (MCP freeze: same-verb-as-CLI only) — so it
            // reports the request it would send and leaves the outcome
            // explicitly unknown rather than guessing a `created` value.
            if dry_run {
                print_json(&json!({
                    "dry_run": true,
                    "would_call": "register_agent",
                    "project": args.project,
                    "agent": args.agent,
                    "created": Value::Null,
                    "would_reserve": args.paths,
                    "note": "the daemon path cannot preview the key mint (it holds the mailbox); set TORON_MCP_DATA_DIR to run against an embedded mailbox and preview, or re-run without --dry-run to register",
                    "next": "re-run without --dry-run to register; nothing was written",
                }))?;
                return Ok(());
            }
            // ADR-0037 W2: the MCP surface is frozen and refuses a second key
            // for a known pin, so the reuse proof runs host-local against the
            // shared durable root (the `agent prune` precedent). Afterwards the
            // daemon registration is a plain idempotent upsert.
            let local = toron_mcp::demo_mailbox().await?;
            let profile = toron_core::nostr::identity::AgentProfile {
                name: args.agent.clone(),
                program: args.program.clone(),
                model: args.model.clone(),
                task_description: args.task.clone(),
                persona: None,
            };
            let reused_from = reuse_pin_key(&local, args, &profile).await?;
            drop(local);
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
                registration
                    .get("npub")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            print_json(&json!({
                "agent": args.agent,
                "project": args.project,
                "registration": registration,
                "reservation": reservation,
                "reused_key_from": reused_from,
                "pane_id": daemon_stamped,
                "next": "export AGENT_NAME=<pin> (or pass --as on every call); reserve before edit — the strict guard blocks unattributed or unreserved commits",
            }))
        }
        AgentCommand::Prune(_) => {
            // bd-4a0u: no daemon MCP tool exists (ADR-0020 wave 1 freeze:
            // CLI-only nouns stay CLI-only), so the sweep runs against the
            // shared durable XDG root the daemon itself uses — the same
            // store, never a second one. Roster state is sqlite, so the
            // cross-process read is exact.
            eprintln!(
                "toron agent prune: no daemon tool; sweeping the shared durable root directly"
            );
            let mb = toron_mcp::demo_mailbox().await?;
            run_agent(&mb, cmd, dry_run).await
        }
        AgentCommand::Rename(_) => {
            // A rename MOVES an identity: its key directory, its roster row,
            // its kind-30013 profile and its live leases. Like the prune, it
            // runs against the shared durable XDG root the daemon itself
            // uses — the same store, never a second one.
            eprintln!(
                "toron agent rename: no daemon tool; renaming in the shared durable root directly"
            );
            let mb = toron_mcp::demo_mailbox().await?;
            run_agent(&mb, cmd, dry_run).await
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
pub(crate) async fn warn_if_unregistered(mailbox: &Mailbox, project: &str, agent: &str) {
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

/// bd-release-dry-run-mutates-ri7o: the strict dry-run gate for the whole
/// mutating reserve surface. `Some(preview)` for every mutating command;
/// `None` for the read-only ones (`list` / `conflicts`), which run normally
/// under `-n`.
///
/// The preview calls NOTHING — mailbox and daemon both return here before the
/// match. A preview that mutates is worse than no preview at all: measured
/// live, `reserve release -n` reported `released: N`, released for real, and
/// the next real release reported `released: 0`.
fn reserve_dry_run_preview(cmd: &ReserveCommand) -> Option<Value> {
    let (tool, args, note) = match cmd {
        ReserveCommand::Acquire(a) => (
            "file_reservation_paths",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "paths": a.paths,
                "ttl_seconds": a.ttl_secs,
                "exclusive": a.exclusive,
                "reason": a.reason,
            }),
            "nothing was leased",
        ),
        ReserveCommand::Renew(a) => (
            "renew_file_reservations",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "extend_seconds": a.extend_secs,
                "paths": a.paths,
            }),
            "nothing was renewed",
        ),
        ReserveCommand::Release(a) => (
            "release_file_reservations",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "paths": a.paths,
            }),
            "nothing was released",
        ),
        ReserveCommand::ReleaseByReason(a) => (
            "release_file_reservations_by_reason",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "bead_id": a.bead_id,
                "notify_previous": !a.no_notify,
            }),
            "nothing was released",
        ),
        ReserveCommand::Sweep(a) => (
            "release_file_reservations_by_reason",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "repo": a.repo.as_ref().map(|p| p.display().to_string()),
                "closed_beads": a.closed_beads,
            }),
            "nothing was released; the preview reads no board or tracker — run without -n \
             to discover the closed-reason leases and release them",
        ),
        ReserveCommand::Yield(a) => (
            "reserve_yield",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "bead_id": a.bead_id,
                "holder": a.holder,
                "note": a.note,
            }),
            "nothing was yielded",
        ),
        ReserveCommand::ForceRelease(a) => (
            "force_release_file_reservation",
            json!({
                "project_key": a.project,
                "agent_name": a.agent,
                "path": a.path,
                "holder": a.holder,
                "note": a.note,
                "notify_previous": a.notify_previous,
            }),
            "nothing was revoked",
        ),
        ReserveCommand::RequestRelease(a) => (
            "send_message",
            json!({
                "project_key": a.project,
                "sender_name": a.agent,
                "target_agent": a.target,
                "reservation_id": a.reservation_id,
                "ask": a.ask,
                "note": a.note,
                "subject": "[request-release]",
            }),
            "no request was sent",
        ),
        ReserveCommand::Respond(a) => (
            "reply_message",
            json!({
                "project_key": a.project,
                "sender_name": a.agent,
                "request_id": a.request_id,
                "decision": a.decision,
                "note": a.note,
                "subject": "[request-release-response]",
            }),
            "no response was sent",
        ),
        ReserveCommand::Transfer(a) => (
            "file_reservation_paths",
            json!({
                "project_key": a.project,
                "agent_name": a.to,
                "paths": [a.path.clone()],
                "transfer_from": a.from,
                "reason": a.reason,
            }),
            "nothing was transferred",
        ),
        // Read-only commands run normally under -n; with-lease is dispatched
        // in `main` and owns its own dry-run (it prints the plan and leases
        // nothing).
        ReserveCommand::Conflicts(_)
        | ReserveCommand::List(_)
        | ReserveCommand::WithLease(_)
        | ReserveCommand::Yields(_) => {
            return None;
        }
    };
    Some(json!({
        "dry_run": true,
        "would_call": tool,
        "args": args,
        "note": note,
    }))
}

pub async fn run_reserve(
    mailbox: &Mailbox,
    cmd: &ReserveCommand,
    dry_run: bool,
) -> anyhow::Result<()> {
    if dry_run && let Some(preview) = reserve_dry_run_preview(cmd) {
        return print_json(&preview);
    }
    match cmd {
        ReserveCommand::Acquire(args) => {
            warn_if_unregistered(mailbox, &args.project, &args.agent).await;
            // bd-v354: loud zero-file-match warning (stderr, warning only).
            warn_acquire_paths(&args.paths);
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
            let mut board = toron_mcp::tools::reservation::LeaseBoardOut {
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
            }
            .to_json();
            // bd-close-releases-leases-qkbo: stale audit rides on top of the
            // shared board (default repo = cwd).
            let closed =
                crate::tracker::closed_bead_ids(args.repo.as_deref().unwrap_or(Path::new(".")));
            annotate_stale_rows(&mut board, &closed, &args.project);
            print_json(&board)
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
        ReserveCommand::Sweep(args) => {
            let (repo, closed) = sweep_closed_set(args)?;
            let board = mailbox
                .list_file_reservations(&args.project, None, None)
                .await?;
            let rows: Vec<(String, String, String)> = board
                .iter()
                .map(|r| {
                    (
                        r.path_pattern.clone(),
                        r.agent_name.clone(),
                        r.reason.clone(),
                    )
                })
                .collect();
            let by_reason = group_stale_by_reason(&rows, &closed);
            let mut released = 0u64;
            for bead_id in by_reason.keys() {
                let result = mailbox
                    .release_by_reason(&args.project, &args.agent, bead_id, !args.no_notify)
                    .await?;
                released += result.released as u64;
            }
            print_sweep_report(&args.project, &repo, closed.len(), &by_reason, released)
        }
        ReserveCommand::Yield(args) => {
            mailbox
                .record_reservation_yield(
                    &args.project,
                    &args.agent,
                    &args.bead_id,
                    &args.holder,
                    args.note.as_deref().unwrap_or(""),
                )
                .await?;
            print_json(&json!({
                "ok": true,
                "project": args.project,
                "bead_id": args.bead_id,
                "holder": args.holder,
            }))
        }
        ReserveCommand::Yields(args) => {
            let (views, suppressed) = mailbox.reservation_yield_board(&args.project).await?;
            print_json(&yield_board_json(&args.project, views, suppressed))
        }
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
        ReserveCommand::WithLease(_) => anyhow::bail!(
            "reserve with-lease is dispatched in main before the mailbox arm (its child runs in \
             the CLI's process); this arm is a tripwire, not a path"
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

/// bd-v354: warn loudly when a reserve glob matches zero files under the
/// repo (warning only — the lease is still granted, fail-closed unchanged).
/// Stderr so the parity JSON on stdout stays parseable. Silent when the
/// file list cannot be built (non-repo cwd) — never blocks an acquire.
/// Pre-flight warnings for an acquire's `--paths`, both stderr-only so the
/// lease behaviour is unchanged: a list that used the wrong separator (which
/// silently becomes one useless glob), and a glob that matches no file.
fn warn_acquire_paths(paths: &[String]) {
    for p in paths {
        if let Some(w) = toron_core::mailbox::guard::whitespace_path_warning(p) {
            eprintln!("{w}");
        }
    }
    warn_zero_file_matches(paths);
}

fn warn_zero_file_matches(paths: &[String]) {
    let files = repo_relative_files();
    if files.is_empty() {
        return;
    }
    for p in paths {
        let n = toron_core::mailbox::guard::count_pattern_matches(p, &files);
        if let Some(w) = toron_core::mailbox::guard::zero_file_match_warning(p, n) {
            eprintln!("{w}");
        }
    }
}

/// Best-effort repo file list: recursive walk of the cwd, slash-joined
/// repo-relative paths. Skips `.git` and `target`; capped so a huge tree
/// cannot stall an acquire. Public within the crate: `reserve with-lease`
/// refuses zero-file-match globs on the same list.
pub(crate) fn repo_relative_files() -> Vec<String> {
    const CAP: usize = 100_000;
    let mut out = Vec::new();
    let mut stack = vec![std::path::PathBuf::from(".")];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name == ".git" || name == "target" {
                    continue;
                }
                stack.push(path);
            } else if path.is_file() {
                let rel = path
                    .strip_prefix(".")
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .trim_start_matches('/')
                    .trim_start_matches("./")
                    .replace('\\', "/");
                out.push(rel);
                if out.len() >= CAP {
                    return out;
                }
            }
        }
    }
    out
}

/// Daemon arm of `run_reserve` (bd-pin-public-plane-s-easy.14).
pub async fn run_reserve_daemon(
    dc: &DaemonClient,
    cmd: &ReserveCommand,
    dry_run: bool,
) -> anyhow::Result<()> {
    if dry_run && let Some(preview) = reserve_dry_run_preview(cmd) {
        return print_json(&preview);
    }
    match cmd {
        ReserveCommand::Acquire(args) => {
            // bd-zugo: same ghost-holder warning on the daemon arm — the
            // shared durable root is readable from here (`demo_mailbox`).
            if let Ok(mb) = toron_mcp::demo_mailbox().await {
                warn_if_unregistered(&mb, &args.project, &args.agent).await;
            }
            // bd-v354: client-side zero-file-match warning (cwd is the repo;
            // the daemon never sees it). Stderr, warning only.
            warn_acquire_paths(&args.paths);
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
            let mut board = dc
                .call_tool(
                    "list_file_reservations",
                    json!({
                        "project_key": args.project,
                        "holder": args.holder,
                        "path": args.path,
                    }),
                )
                .await?;
            // bd-close-releases-leases-qkbo: the tracked repo is the client's
            // cwd — the daemon never sees it — so annotate here.
            let closed =
                crate::tracker::closed_bead_ids(args.repo.as_deref().unwrap_or(Path::new(".")));
            annotate_stale_rows(&mut board, &closed, &args.project);
            print_json(&board)
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
        ReserveCommand::Sweep(args) => {
            let (repo, closed) = sweep_closed_set(args)?;
            let board = dc
                .call_tool(
                    "list_file_reservations",
                    json!({
                        "project_key": args.project,
                        "holder": null,
                        "path": null,
                    }),
                )
                .await?;
            let rows: Vec<(String, String, String)> = board
                .get("reservations")
                .and_then(Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .map(|row| {
                            (
                                row.get("path")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string(),
                                row.get("holder")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string(),
                                row.get("reason")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            let by_reason = group_stale_by_reason(&rows, &closed);
            let mut released = 0u64;
            for bead_id in by_reason.keys() {
                let result = dc
                    .call_tool(
                        "release_file_reservations_by_reason",
                        json!({
                            "project_key": args.project,
                            "agent_name": args.agent,
                            "bead_id": bead_id,
                            "notify_previous": !args.no_notify,
                        }),
                    )
                    .await?;
                released += result.get("released").and_then(Value::as_u64).unwrap_or(0);
            }
            print_sweep_report(&args.project, &repo, closed.len(), &by_reason, released)
        }
        // bd-reserve-yield-taqf: CLI-only noun, NO daemon tool (the frozen 41
        // stays frozen) — scan the shared durable root directly, the same
        // pattern `ack-sla` uses. Loud on stderr so a divergence between the
        // daemon's root and this process's root is never silent.
        ReserveCommand::Yield(_) | ReserveCommand::Yields(_) => {
            eprintln!(
                "toron reserve yield/yields: no daemon tool; scanning the shared durable root directly"
            );
            let mb = toron_mcp::demo_mailbox().await?;
            run_reserve(&mb, cmd, dry_run).await
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
        ReserveCommand::WithLease(_) => anyhow::bail!(
            "reserve with-lease is dispatched in main before the mailbox arm (its child runs in \
             the CLI's process); this arm is a tripwire, not a path"
        ),
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
            let rows = mailbox
                .active_reservations_all_aliases(&args.project)
                .await?;
            // adr-0018 D1a ("ONE resolver") + bd-guard-holder-identity-fold-dqog:
            // every lease row carries the holder's AUTHORITATIVE hex pubkey
            // (`agent_pubkey`) beside its possibly-rendered name. Seed the fold
            // from the row's own pubkey FIRST — that evidence needs no roster
            // read, so a lease held under an `npub[..8]` rendering still folds
            // to the same identity as the name. The roster/`whois` lookup below
            // is an ADDITIONAL source for spellings the rows cannot attribute
            // (aliases, slugs), never the primary one.
            let mut holder_fold = HolderFold::default();
            for r in &rows {
                holder_fold.record(&r.agent_pubkey, &r.agent_pubkey);
                holder_fold.record(&r.agent_name, &r.agent_pubkey);
            }
            // The committer resolves through the SAME name→pubkey path the
            // acquire stamped the lease with (local key material, never the
            // roster): acting as your own pin matches your own lease even when
            // the roster cannot name either spelling.
            if let Some(name) = agent.as_deref()
                && let Some(pk) = mailbox.agent_pubkey_hex(&args.project, name).await
            {
                holder_fold.record(name, &pk);
            }
            let mut reservations = rows
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
                    .await?;
                for r in &foreign {
                    holder_fold.record(&r.agent_pubkey, &r.agent_pubkey);
                    holder_fold.record(&r.agent_name, &r.agent_pubkey);
                }
                let foreign = foreign
                    .into_iter()
                    .map(|r| toron_core::mailbox::guard::Reservation {
                        holder: r.agent_name,
                        pattern: r.path_pattern,
                        exclusive: r.exclusive,
                    })
                    .collect::<Vec<_>>();
                merge_cross_project_leases(&mut reservations, repo_name, foreign);
            }
            // Additional evidence only: the project roster (the writer's view)
            // plus a best-effort `whois` for spellings it does not name (the
            // alias table — the pin a role/slug folds to).
            let spellings = reservations
                .iter()
                .map(|r| r.holder.clone())
                .chain(agent.clone());
            holder_fold.merge(embedded_roster_fold(mailbox, &args.project, spellings).await);
            finish_guard_check(args, agent.as_deref(), &staged, &reservations, &holder_fold)
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

/// ADR-0018 D1a ("ONE resolver") applied to the sweep-guard: fold every
/// spelling of an agent — pin, slug, folded alias, npub, and the daemon's
/// `npub[..8]` holder fallback — onto ONE canonical identity (the agent's
/// full pubkey hex), so a lease held under one spelling covers the same
/// agent committing under another.
///
/// Why this exists (observed live): `reserve acquire --as <name>` stores the
/// lease under the resolved agent, while `guard check` compares the lease
/// HOLDER against the committer NAME **raw**. When the roster could not name
/// the holder, the daemon rendered it as an 8-char npub prefix — a string no
/// agent name can ever equal — so the owner's own lease blocked its own
/// commit until the committer passed the literal prefix.
///
/// Fail-closed by construction: a spelling we cannot attribute folds to
/// ITSELF, never to a wildcard, so the fold cannot turn a foreign lease into
/// an own one. An ambiguous npub prefix also stays itself (two candidates are
/// not an attribution).
#[derive(Debug, Default)]
struct HolderFold {
    /// spelling (name · alias · npub · prefix) → canonical pubkey hex.
    by_spelling: std::collections::HashMap<String, String>,
    /// Every canonical pubkey (the npub-prefix fallback looks here).
    npubs: std::collections::BTreeSet<String>,
}

impl HolderFold {
    /// `npub` is the canonical for both spellings.
    fn record(&mut self, spelling: &str, npub: &str) {
        let canonical = npub.trim().to_ascii_lowercase();
        let spelling = spelling.trim();
        if spelling.is_empty() || canonical.is_empty() {
            return;
        }
        self.by_spelling
            .insert(spelling.to_string(), canonical.clone());
        self.by_spelling
            .insert(canonical.clone(), canonical.clone());
        self.npubs.insert(canonical);
    }

    /// True when this spelling is already attributable (so callers can skip
    /// an expensive roster lookup).
    fn knows(&self, spelling: &str) -> bool {
        self.by_spelling.contains_key(spelling.trim())
    }

    /// Union another fold in, first-wins per spelling (so the daemon's roster
    /// — the writer's view — outranks a later local lookup).
    fn merge(&mut self, other: HolderFold) {
        for (spelling, canonical) in other.by_spelling {
            self.by_spelling.entry(spelling).or_insert(canonical);
        }
        self.npubs.extend(other.npubs);
    }

    fn resolve(&self, spelling: &str) -> String {
        if let Some(canonical) = self.by_spelling.get(spelling) {
            return canonical.clone();
        }
        let lower = spelling.trim().to_ascii_lowercase();
        if (8..64).contains(&lower.len()) && lower.bytes().all(|b| b.is_ascii_hexdigit()) {
            let hits: Vec<&String> = self
                .npubs
                .iter()
                .filter(|n| n.starts_with(&lower))
                .collect();
            if let [only] = hits.as_slice() {
                return (*only).clone();
            }
        }
        spelling.to_string()
    }
}

/// Additional fold evidence from the project roster plus a best-effort
/// `whois` for any spelling the roster does not name — `whois` resolves the
/// alias table (the pin a role/slug folds to), which is precisely the case a
/// raw name compare got wrong. Best-effort throughout: an unusable roster
/// leaves this contribution empty and the caller keeps the lease-row evidence
/// it already has, which degrades to the previous raw compare instead of
/// blocking harder.
async fn embedded_roster_fold(
    mailbox: &Mailbox,
    project: &str,
    spellings: impl IntoIterator<Item = String>,
) -> HolderFold {
    let mut fold = HolderFold::default();
    if let Ok(roster) = mailbox
        .list_agents(project, toron_core::mailbox::ListAgentsOptions::default())
        .await
    {
        for who in &roster {
            fold.record(&who.name, &who.npub);
        }
    }
    let mut wanted: Vec<String> = spellings.into_iter().collect();
    wanted.sort_unstable();
    wanted.dedup();
    for spelling in wanted {
        if spelling.trim().is_empty() || fold.knows(&spelling) {
            continue;
        }
        if let Ok(who) = mailbox.whois(project, &spelling).await {
            fold.record(&spelling, &who.npub);
            fold.record(&who.name, &who.npub);
        }
    }
    fold
}

/// Fold from the daemon's `toron://agents/{project}` document (pure).
fn fold_from_agents_resource(agents: &serde_json::Value) -> HolderFold {
    let mut fold = HolderFold::default();
    for entry in agents.as_array().into_iter().flatten() {
        if let (Some(name), Some(npub)) = (
            entry.get("name").and_then(serde_json::Value::as_str),
            entry.get("npub").and_then(serde_json::Value::as_str),
        ) {
            fold.record(name, npub);
        }
    }
    fold
}

/// Resolve the committing agent from `--as` / env (shared embedded+daemon).
fn pin_from_flag_or_agent_name(flag: Option<String>) -> Option<String> {
    flag.or_else(|| std::env::var("AGENT_NAME").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn guard_agent(args: &crate::cli::GuardCheckArgs) -> Option<String> {
    pin_from_flag_or_agent_name(args.as_agent.clone())
}

fn guard_record_agent(args: &crate::cli::GuardRecordArgs) -> Option<String> {
    pin_from_flag_or_agent_name(args.as_agent.clone())
}

/// Shared tail of `guard check` (strict + legacy paths, exit codes).
/// The guard's verdict on one commit, as DATA.
///
/// bd-lyih: the decision and the effect edge were fused — `finish_guard_check`
/// printed, wrote to stderr and called `std::process::exit` inline, so the
/// one thing that most needs proving (that a declaration the guard cannot
/// honour actually BLOCKS, rather than degrading into a shrug) could only be
/// reached through a live daemon, a git hook and a subprocess. The decision
/// is pure; the edge prints and exits.
pub struct GuardVerdict {
    /// What the commit-msg hook parses.
    pub report: serde_json::Value,
    /// 0 allows the commit, 1 blocks it. The hook gates on this alone.
    pub exit_code: i32,
    /// Human-facing stderr lines, in the order they should be read.
    pub notices: Vec<String>,
}

/// Pure decision core for `toron guard check`. Assumes strict mode; the
/// operator hatch is handled by the caller, which must also print its own
/// escape notice before exiting 0.
pub fn guard_check_verdict(
    repo: &std::path::Path,
    project: &str,
    agent: Option<&str>,
    staged: &[String],
    reservations: &[toron_core::mailbox::guard::Reservation],
    resolve: &dyn Fn(&str) -> String,
    message: Option<&str>,
) -> GuardVerdict {
    use toron_core::mailbox::guard::{
        ProjectedDeclFault, StrictBlockKind, guard_check_strict_resolved,
    };
    let blocked = guard_check_strict_resolved(
        agent,
        staged,
        reservations,
        toron_core::mailbox::guard::SHARED_SAFE_PATHS,
        resolve,
    );
    // bd-9mb8 bead-hunk attribution: bd-* tokens ADDED by the staged diff must
    // appear in the commit message (--msg-file from the commit-msg hook).
    // Skipped when no message was supplied.
    let bead_blocks = message.map_or_else(Vec::new, |msg| {
        let added = toron_core::mailbox::guard::added_lines_for_hunk_attribution(repo);
        toron_core::mailbox::guard::bead_hunk_violations(&added, msg)
    });
    // bd-lyih: a repository that VENDORS prose from another repo declares
    // those paths in .gitattributes, and the bead ids inside them belong to
    // the SOURCE ledger. A declaration we cannot honour is NOT ignored —
    // honouring a bad one hides real work, and ignoring a good one blocks a
    // legitimate commit with no explanation. Both fail closed, loudly.
    let projected_paths = toron_core::mailbox::guard::projected_paths_in(repo);
    let projected_fault = toron_core::mailbox::guard::projected_decl_fault(repo, &projected_paths);

    // bd-26yr build gate: the staged paths' packages must build. Runs LAST,
    // because a build over a broken reservation is not the reservation's fault
    // and the reservation message is the one the agent needs first.
    let mut blocked = blocked;
    blocked.extend(toron_core::mailbox::guard::build_gate_blocks(repo, staged));

    let mut notices: Vec<String> = Vec::new();
    if let Some(fault) = &projected_fault {
        notices.push(format!(
            "toron: strict guard: projected-path declaration REFUSED — {}",
            match fault {
                ProjectedDeclFault::CoversBuildSource { pattern, probe } => format!(
                    "`{pattern}` matches compiled source ({probe}); a projection may not exempt \
                     files this repository builds. Narrow the pattern, or drop the declaration \
                     and attribute the ids."
                ),
                ProjectedDeclFault::Malformed { line_no, line } => format!(
                    ".gitattributes:{line_no} (`{line}`) has no attribute; a projected path is \
                     declared with `{}=true`.",
                    toron_core::mailbox::guard::PROJECTED_ATTR
                ),
                ProjectedDeclFault::AnchoredPattern { line_no, pattern } => format!(
                    ".gitattributes:{line_no} declares the anchored pattern `{pattern}`, which \
                     matches nothing: git anchors a leading `/`, this matcher does not. Drop the \
                     leading slash (the pattern is already repo-relative)."
                ),
            }
        ));
    }
    if !blocked.is_empty() {
        let summary: Vec<String> = blocked
            .iter()
            .map(|b| match &b.kind {
                StrictBlockKind::HeldBy(h) => format!("{} (held by {h})", b.path),
                StrictBlockKind::Unreserved => format!("{} (no active reservation)", b.path),
                StrictBlockKind::Unattributed => b.path.clone(),
                StrictBlockKind::BuildFailed(first) => {
                    format!("BUILD FAILED: `{}` — {}", b.path, first)
                }
            })
            .collect();
        notices.push(if agent.is_none() {
            "toron: strict guard: unattributed commit blocked — set AGENT_NAME=<pin> or pass --as"
                .to_string()
        } else {
            format!(
                "toron: strict guard blocked staged paths: {}",
                summary.join(", ")
            )
        });
    }
    if !bead_blocks.is_empty() {
        let toks: Vec<String> = bead_blocks.iter().map(|b| b.token.clone()).collect();
        notices.push(format!(
            "toron: strict guard: unattributed bead-tagged hunks — {toks:?} appear in the staged \
             diff but not in the commit message; add them to the message (escapes: \
             TORON_GUARD_STRICT=0 or --no-verify)"
        ));
    }
    let report = json!({
        "strict": true,
        "blocked": blocked.iter().map(|b| json!({
            "path": b.path,
            "reason": match &b.kind {
                StrictBlockKind::Unattributed => "unattributed".to_string(),
                StrictBlockKind::HeldBy(_) => "held-by".to_string(),
                StrictBlockKind::Unreserved => "unreserved".to_string(),
                StrictBlockKind::BuildFailed(first) => format!("build-failed: {first}"),
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
        "projected_paths": projected_paths,
        "projected_decl_fault": projected_fault.as_ref().map(|f| match f {
            ProjectedDeclFault::CoversBuildSource { pattern, probe, .. } => {
                format!("covers-build-source: {pattern} matches {probe}")
            }
            ProjectedDeclFault::Malformed { line_no, line } => {
                format!("malformed: .gitattributes:{line_no} `{line}`")
            }
            ProjectedDeclFault::AnchoredPattern { line_no, pattern } => {
                format!("anchored-pattern: .gitattributes:{line_no} `{pattern}` matches nothing")
            }
        }),
        "agent": agent,
        "project": project,
    });
    let clean = blocked.is_empty() && bead_blocks.is_empty() && projected_fault.is_none();
    GuardVerdict {
        report,
        exit_code: i32::from(!clean),
        notices,
    }
}

fn finish_guard_check(
    args: &crate::cli::GuardCheckArgs,
    agent: Option<&str>,
    staged: &[String],
    reservations: &[toron_core::mailbox::guard::Reservation],
    fold: &HolderFold,
) -> anyhow::Result<()> {
    // Both comparison surfaces go through the ONE identity fold: a lease's
    // holder and the committer's name are the same person when they fold to
    // the same pubkey, whatever spelling either side used.
    let resolve = |n: &str| fold.resolve(n);
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
        // NO `--msg-file` means the pre-commit hook, which runs before the
        // editor opens and therefore has no message to judge. That must stay
        // DISTINCT from an empty one: collapsing the two made pre-commit
        // charge every bead-tagged hunk against an empty message, so any
        // commit carrying a `bd-*` line was blocked twice over.
        let message = args
            .msg_file
            .as_ref()
            .map(|mf| std::fs::read_to_string(mf).unwrap_or_default());
        let verdict = guard_check_verdict(
            &args.repo,
            &args.project,
            agent,
            staged,
            reservations,
            &resolve,
            message.as_deref(),
        );
        for line in &verdict.notices {
            eprintln!("{line}");
        }
        print_json(&verdict.report)?;
        // Explicit exit both ways: the demo mailbox keeps a
        // background thread alive after the tokio runtime shuts down,
        // which would HANG a pre-commit hook.
        std::process::exit(verdict.exit_code);
    }
    // Legacy (non-strict) surface: unchanged fail-open for an unattributed
    // commit, but the same identity fold for everything attributed.
    let blocked = agent.map_or_else(Vec::new, |agent| {
        toron_core::mailbox::guard::guard_blocked_paths_resolved(
            agent,
            staged,
            reservations,
            &resolve,
        )
    });
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
        if out["blocked"].as_array().map(Vec::is_empty).unwrap_or(true) {
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
            let (mut reservations, mut fold) = daemon_guard_roster(dc, &args.project)
                .await
                .with_context(|| {
                    format!(
                        "guard check: cannot list reservations for project {} from the daemon \
                         (is `toron serve --mcp-http` running?)",
                        args.project
                    )
                })?;
            // adr-0018 D1a: the daemon's roster is a RENDERING — when it
            // cannot name a holder it emits `npub[..8]`, a string no agent
            // name can equal, so the owner's own shared lease reads as
            // "unreserved". Identity is LOCAL truth (the key store the lease
            // was acquired from), so fold the committer and any unnamed
            // holder through one local mailbox lookup. Best-effort: no local
            // store leaves today's raw compare, never a harder block.
            let local = toron_mcp::demo_mailbox().await.ok();
            if let Some(mb) = local.as_ref() {
                // The committer resolves through the SAME local name→pubkey
                // path `reserve acquire` stamped the lease with, so acting as
                // one's own pin matches one's own lease even when the roster
                // cannot name either spelling.
                if let Some(name) = agent.as_deref()
                    && let Some(pk) = mb.agent_pubkey_hex(&args.project, name).await
                {
                    fold.record(name, &pk);
                }
                let spellings = reservations
                    .iter()
                    .map(|r| r.holder.clone())
                    .chain(agent.clone());
                fold.merge(embedded_roster_fold(mb, &args.project, spellings).await);
            }
            // bd-guard-crossproject-lease-er38: the roster resource is
            // project-scoped, so foreign-project leases escape it. Augment
            // from the durable root (the same store the daemon serves);
            // best-effort — on failure the guard degrades to the old
            // project-scoped behavior, never blocks harder.
            if let Some(repo_name) = std::path::Path::new(&args.repo)
                .file_name()
                .and_then(|n| n.to_str())
                && let Some(mb) = local.as_ref()
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
            finish_guard_check(args, agent.as_deref(), &staged, &reservations, &fold)
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
) -> anyhow::Result<(Vec<toron_core::mailbox::guard::Reservation>, HolderFold)> {
    let res = dc
        .read_resource(&format!("toron://reservations/{project}"))
        .await?;
    let agents = dc
        .read_resource(&format!("toron://agents/{project}"))
        .await?;
    let now_secs = chrono::Utc::now().timestamp();
    // adr-0018 D1a: seed the fold from the lease events' own author pubkeys
    // FIRST (roster-independent — it holds even when the agents document is
    // empty), then let the agents document name any remaining spellings.
    let mut fold = fold_from_daemon_leases(&res, &agents);
    fold.merge(fold_from_agents_resource(&agents));
    Ok((resolve_daemon_roster(&res, &agents, now_secs), fold))
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
    let name_by_npub = name_by_npub(agents);
    let events = res.get("events").and_then(|v| v.as_array());
    let Some(events) = events else {
        return Vec::new();
    };
    fn event_created_at(ev: &serde_json::Value) -> i64 {
        ev.get("created_at")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
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
                        .then(|| a.get(1).and_then(|s| s.as_str()).map(ToString::to_string))
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

/// npub → agent name from the daemon's `toron://agents/{project}` document
/// (empty when the roster read returns nothing).
fn name_by_npub(agents: &serde_json::Value) -> std::collections::HashMap<String, String> {
    agents
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|a| {
            Some((
                a.get("npub")?.as_str()?.to_string(),
                a.get("name")?.as_str()?.to_string(),
            ))
        })
        .collect()
}

/// Seed the identity fold from the lease events' OWN author pubkeys (pure,
/// bd-guard-holder-identity-fold-dqog).
///
/// Each kind-30031 event carries the holder's canonical hex `pubkey`; the
/// holder NAME the roster builds is a rendering that falls back to
/// `npub[..8]` when the roster cannot name the author. Recording both makes
/// every rendered spelling fold back to the event's real identity — with no
/// roster read. This is the evidence a roster-blind guard needs: it works
/// even when `toron://agents/{project}` returns `[]`.
fn fold_from_daemon_leases(res: &serde_json::Value, agents: &serde_json::Value) -> HolderFold {
    let names = name_by_npub(agents);
    let mut fold = HolderFold::default();
    for ev in res
        .get("events")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
    {
        let Some(pk) = ev
            .get("pubkey")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        else {
            continue;
        };
        fold.record(pk, pk);
        let holder = names
            .get(pk)
            .cloned()
            .unwrap_or_else(|| pk.chars().take(8).collect());
        fold.record(&holder, pk);
    }
    fold
}

/// bd-ow6c: the captain fold routes in BOTH identity modes.
///
/// The old gate refused an open-mode write with "where the fold can never
/// route" — FALSE. The alias table is authoritative in Open mode too
/// (ADR-0018 D1a: ONE resolver), the read path applied no such gate, and
/// `toron mail whois flywheel captain` resolved in the open-mode flywheel
/// project while this gate refused to let that fold be written. The pin is
/// validated against the daemon's whois tool on the write path, so the fold
/// routes.
///
/// What the gate still does: an open-mode write is AUDITED (the `--force`
/// flag asks for the record), because a captain fold is an operational
/// pointer an operator should be able to see was set.
pub(crate) fn captain_write_gate(
    mode: &toron_core::mailbox::naming::IdentityMode,
    force: bool,
    project: &str,
) -> anyhow::Result<()> {
    if matches!(mode, toron_core::mailbox::naming::IdentityMode::Open) && force {
        eprintln!(
            "{}",
            serde_json::json!({
                "audit": "captain fold written in open identity mode",
                "project": project,
                "routes": true,
            })
        );
    }
    Ok(())
}

#[cfg(test)]
mod captain_open_mode_tests {
    use super::*;

    /// bd-ow6c: the captain fold routes in BOTH identity modes, so an
    /// open-mode write is NOT refused. The old matrix pinned the opposite
    /// ("open refuses: swarm-only"), which was false: the alias table is
    /// authoritative in Open mode, the read path applied no such gate, and
    /// `toron mail whois flywheel captain` resolved in the open-mode flywheel
    /// project while this gate refused to let that fold be written.
    #[test]
    fn captain_write_gate_does_not_refuse_a_fold_that_routes() {
        captain_write_gate(
            &toron_core::mailbox::naming::IdentityMode::Open,
            false,
            "flywheel",
        )
        .expect("an open-mode captain fold routes; it is not refused");
        captain_write_gate(
            &toron_core::mailbox::naming::IdentityMode::Open,
            true,
            "flywheel",
        )
        .expect("force only asks for the audit");
        captain_write_gate(
            &toron_core::mailbox::naming::IdentityMode::Swarm,
            false,
            "toron",
        )
        .expect("swarm mode routes without force");
    }

    /// The reader defaults a fresh (migrated) index DB to Open — the mode a
    /// captain fold is written in by default.
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
    use serde_json::json;

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

    /// Revocation tombstone ordering (mirrors the embedded resolver):
    /// `resolve_lease_state`): a revoked lease stays hidden, a lease
    /// re-acquired after the marker is live, and a stale marker never
    /// blankets a newer lease. One table (was 3 tests).
    #[test]
    fn revoke_tombstone_ordering() {
        let roster = resolve(vec![held_lease(HOLDER, 100), force_revoke(200)]);
        assert!(
            roster.is_empty(),
            "revoked lease must not reach the guard: {roster:?}"
        );
        let roster = resolve(vec![
            held_lease(HOLDER, 100),
            force_revoke(200),
            held_lease(HOLDER, 300),
        ]);
        assert_eq!(roster.len(), 1, "re-acquired lease is live: {roster:?}");
        assert_eq!(roster[0].pattern, "scratch-guard.txt");
        let roster = resolve(vec![force_revoke(100), held_lease(HOLDER, 200)]);
        assert_eq!(
            roster.len(),
            1,
            "newer lease survives stale marker: {roster:?}"
        );
    }
}

#[cfg(test)]
mod holder_fold_tests {
    use super::*;
    use serde_json::json;

    const ALICE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const BOB: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    /// A minimal held kind-30031 lease event, as `toron://reservations/{p}`
    /// renders it (the author pubkey is the authoritative field).
    fn lease_event(pubkey: &str) -> serde_json::Value {
        json!({
            "id": "00",
            "pubkey": pubkey,
            "created_at": 100,
            "kind": 30031,
            "tags": [["d", "toron:src/effect_journal.rs"]],
            "content": r#"{"exclusive":false,"reason":"bd-guard-holder-identity-fold-dqog"}"#,
            "sig": "00"
        })
    }

    fn fold_with(names: &[(&str, &str)]) -> HolderFold {
        let agents = serde_json::Value::Array(
            names
                .iter()
                .map(|(name, npub)| json!({ "name": name, "npub": npub }))
                .collect(),
        );
        fold_from_agents_resource(&agents)
    }

    /// Holder-fold spelling matrix (fail-closed throughout): name/npub/prefix
    /// fold to one identity; unknown and ambiguous spellings stay themselves
    /// (never a wildcard); empty spellings are never recorded. One table
    /// (was 4 tests).
    #[test]
    fn holder_fold_spelling_matrix() {
        let fold = fold_with(&[("RoseDune", ALICE)]);
        assert_eq!(fold.resolve("RoseDune"), ALICE);
        assert_eq!(fold.resolve(ALICE), ALICE);
        assert_eq!(
            fold.resolve(&ALICE[..8]),
            ALICE,
            "the daemon's holder rendering must fold"
        );
        assert_eq!(fold.resolve("Ghost"), "Ghost");
        assert_ne!(fold.resolve("Ghost"), fold.resolve("RoseDune"));
        let twin = format!(
            "aaaaaaaa{}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            &BOB[8..16]
        );
        let fold = fold_with(&[("RoseDune", ALICE), ("BlueLake", &twin)]);
        assert_eq!(fold.resolve(&ALICE[..8]), ALICE[..8]);
        let mut fold = HolderFold::default();
        fold.record("", ALICE);
        fold.record("  ", ALICE);
        fold.record("RoseDune", "");
        assert!(!fold.knows(""));
        assert_eq!(fold.resolve(""), "");
        assert_eq!(fold.resolve("RoseDune"), "RoseDune");
    }

    /// The live bug, end to end at the guard seam: a lease whose holder
    /// degraded to the `npub[..8]` rendering must still count as the OWNER's
    /// coverage, so a shared lease stops reading as "unreserved".
    #[test]
    fn own_shared_lease_covers_the_owner_despite_the_prefix_holder() {
        let fold = fold_with(&[("RoseDune", ALICE)]);
        let reservations = vec![toron_core::mailbox::guard::Reservation {
            holder: ALICE[..8].to_string(),
            pattern: "src/**".to_string(),
            exclusive: false,
        }];
        let staged = vec!["src/effect_journal.rs".to_string()];
        let resolve = |n: &str| fold.resolve(n);

        let blocked = toron_core::mailbox::guard::guard_check_strict_resolved(
            Some("RoseDune"),
            &staged,
            &reservations,
            toron_core::mailbox::guard::SHARED_SAFE_PATHS,
            &resolve,
        );
        assert!(
            blocked.is_empty(),
            "the owner's own shared lease must cover its commit: {blocked:?}"
        );

        // And a DIFFERENT agent is still blocked — the fold must not open the
        // guard for everyone.
        let other = toron_core::mailbox::guard::guard_check_strict_resolved(
            Some("BlueLake"),
            &staged,
            &reservations,
            toron_core::mailbox::guard::SHARED_SAFE_PATHS,
            &resolve,
        );
        assert_eq!(
            other.len(),
            1,
            "an unrelated agent has no coverage: {other:?}"
        );
    }

    /// bd-guard-holder-identity-fold-dqog: the lease events' own author
    /// pubkeys seed the fold with NO roster at all. This is the case that
    /// matters live — `toron://agents/{project}` returned `[]`, so any
    /// roster-built fold was empty and the owner's own lease read as
    /// "unreserved".
    #[test]
    fn lease_events_seed_the_fold_without_any_roster() {
        let res = json!({ "events": [lease_event(ALICE)] });
        let fold = fold_from_daemon_leases(&res, &json!([]));
        assert_eq!(fold.resolve(ALICE), ALICE);
        assert_eq!(
            fold.resolve(&ALICE[..8]),
            ALICE,
            "the rendered holder spelling must fold to the event author"
        );
        assert_eq!(
            fold.resolve("RoseDune"),
            "RoseDune",
            "with no roster a name the events do not carry stays itself"
        );
    }

    /// The full roster-blind attribution: a fold seeded only from the lease
    /// event plus the committer's locally-resolved pubkey (what
    /// `Mailbox::agent_pubkey_hex` supplies) covers the owner's own commit and
    /// still blocks a foreign one.
    #[test]
    fn roster_blind_fold_covers_the_owner_and_blocks_foreign_pins() {
        let res = json!({ "events": [lease_event(ALICE)] });
        let mut fold = fold_from_daemon_leases(&res, &json!([]));
        // The committer resolves through local key material, exactly as the
        // guard seeds it (`agent_pubkey_hex`).
        fold.record("RoseDune", ALICE);
        let reservations = vec![toron_core::mailbox::guard::Reservation {
            holder: ALICE[..8].to_string(),
            pattern: "src/**".to_string(),
            exclusive: false,
        }];
        let staged = vec!["src/effect_journal.rs".to_string()];
        let resolve = |n: &str| fold.resolve(n);

        let own = toron_core::mailbox::guard::guard_check_strict_resolved(
            Some("RoseDune"),
            &staged,
            &reservations,
            toron_core::mailbox::guard::SHARED_SAFE_PATHS,
            &resolve,
        );
        assert!(
            own.is_empty(),
            "the owner's own lease must cover its commit with no roster: {own:?}"
        );

        let foreign = toron_core::mailbox::guard::guard_check_strict_resolved(
            Some("GhostPin"),
            &staged,
            &reservations,
            toron_core::mailbox::guard::SHARED_SAFE_PATHS,
            &resolve,
        );
        assert_eq!(
            foreign.len(),
            1,
            "an unattributable pin must stay blocked: {foreign:?}"
        );
    }
}

#[cfg(test)]
mod guard_projection_tests {
    //! bd-lyih: the declared-projection exemption as the guard REPORTS it.
    //!
    //! The decision is proved in `toron-core`'s `mailbox::guard` tests. What
    //! is proved HERE is the part where it actually bites: the JSON the
    //! commit-msg hook parses, and the exit code it gates on. A declaration
    //! the guard cannot honour has to BLOCK, loudly — not degrade into a
    //! shrug that looks like success.
    //!
    //! These drive [`guard_check_verdict`] rather than the binary, because
    //! the binary reaches this decision through a live daemon's reservation
    //! roster. A test that needs a daemon is a test that fails in CI for
    //! reasons unrelated to the rule.

    use super::*;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    /// A repo with one real crate (so there IS compiled source to protect), a
    /// guide carrying a foreign bead id, and a source file carrying its own.
    /// Built with the git CLI: toron-cli does not depend on libgit2, and this
    /// is the index state the guard reads, not the library that reads it.
    fn repo(attrs: &str) -> PathBuf {
        // A per-process COUNTER, not a clock. `SystemTime::now()` is coarse
        // enough on macOS that two test threads reading it in the same
        // instant get the same value, so two fixtures landed in one
        // directory and raced on `git init` — a flake that only shows up
        // under the parallel runner.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("toron-guard-projected-{}-{n}", std::process::id()));
        std::fs::create_dir_all(root.join("docs/guides")).expect("guides");
        std::fs::create_dir_all(root.join("crates/demo/src")).expect("crate src");
        std::fs::write(root.join("crates/demo/Cargo.toml"), "[package]\n").expect("toml");
        std::fs::write(root.join(".gitattributes"), attrs).expect("attrs");
        std::fs::write(
            root.join("docs/guides/mail.md"),
            "toron mail send --thread bd-foreign1\n",
        )
        .expect("guide");
        std::fs::write(
            root.join("crates/demo/src/lib.rs"),
            "// governed-by: bd-source01\n",
        )
        .expect("src");
        let git = |args: &[&str]| {
            let out = Command::new("git")
                .args(args)
                .current_dir(&root)
                .output()
                .expect("run git");
            assert!(
                out.status.success(),
                "git {args:?} failed in {}: {}",
                root.display(),
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git(&["init", "-q"]);
        git(&["add", "docs/guides/mail.md", "crates/demo/src/lib.rs"]);
        root
    }

    const STAGED: [&str; 2] = ["docs/guides/mail.md", "crates/demo/src/lib.rs"];

    /// The verdict the way the hook sees it: every staged path covered by the
    /// committer's own lease, so the staged-path check is satisfied and the
    /// hunk rule is the ONLY thing under test.
    fn verdict(root: &Path, message: &str) -> GuardVerdict {
        let staged: Vec<String> = STAGED.iter().map(|s| (*s).to_string()).collect();
        let own = vec![toron_core::mailbox::guard::Reservation {
            holder: "IvoryRidge".to_string(),
            pattern: "**".to_string(),
            exclusive: true,
        }];
        let resolve = |n: &str| n.to_string();
        guard_check_verdict(
            root,
            "demo",
            Some("IvoryRidge"),
            &staged,
            &own,
            &resolve,
            Some(message),
        )
    }

    #[test]
    fn a_declared_projection_is_reported_and_not_charged_attribution() {
        let root = repo("docs/guides/** linguist-generated=true\n");
        let v = verdict(&root, "docs: refresh [bd-source01]\n");

        assert_eq!(
            v.report["projected_paths"],
            json!(["docs/guides/**"]),
            "the declaration is surfaced, not applied silently: {}",
            v.report
        );
        assert!(
            v.report["projected_decl_fault"].is_null(),
            "a sound declaration reports no fault: {}",
            v.report
        );
        assert_eq!(
            v.report["unattributed_bead_tokens"],
            json!([]),
            "the guide's foreign id is exempt and the source's is named: {}",
            v.report
        );
        assert_eq!(v.exit_code, 0, "a sound declaration must not block");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn the_same_commit_without_the_declaration_is_blocked() {
        // The negative control. Same repo, same message, same staged files —
        // only the .gitattributes line removed. Without this the test above
        // could be green because the guard is not scanning the guide at all.
        let root = repo("# no declaration here\n");
        let v = verdict(&root, "docs: refresh [bd-source01]\n");

        assert_eq!(
            v.report["unattributed_bead_tokens"],
            json!(["bd-foreign1"]),
            "an undeclared foreign id is still a violation: {}",
            v.report
        );
        assert_eq!(v.exit_code, 1, "and it blocks");
        assert!(
            v.notices
                .iter()
                .any(|n| n.contains("bd-foreign1") && n.contains("bead-tagged")),
            "the operator is told which token to attribute: {:?}",
            v.notices
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_declaration_reaching_compiled_source_fails_closed() {
        // The dangerous direction: `**` would switch the rule off for every
        // source file in the repo. It must BLOCK and name itself.
        let root = repo("** linguist-generated=true\n");
        let v = verdict(&root, "docs: [bd-source01] [bd-foreign1]\n");

        let fault = v.report["projected_decl_fault"]
            .as_str()
            .unwrap_or_else(|| panic!("a blanket declaration must be refused: {}", v.report));
        assert!(
            fault.starts_with("covers-build-source: ** matches "),
            "the fault names the pattern and the source it reaches: {fault}"
        );
        assert_eq!(v.exit_code, 1, "an unhonourable declaration blocks");
        assert!(
            v.notices.iter().any(|n| n.contains("REFUSED")),
            "and it says so on stderr: {:?}",
            v.notices
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_malformed_declaration_line_fails_closed() {
        let root = repo("docs/guides/**\n");
        let v = verdict(&root, "docs: [bd-source01] [bd-foreign1]\n");

        let fault = v.report["projected_decl_fault"]
            .as_str()
            .expect("an attribute line with no attribute is refused");
        assert!(
            fault.starts_with("malformed: .gitattributes:1"),
            "the fault names the line: {fault}"
        );
        assert_eq!(v.exit_code, 1, "and it blocks");
        let _ = std::fs::remove_dir_all(root);
    }

    /// bd-lyih regression: NO message is not the same as an EMPTY message.
    ///
    /// `guard check` runs from TWO hooks. pre-commit passes no `--msg-file`
    /// because it fires before the editor opens; commit-msg passes one. A
    /// refactor that read the absent file into an empty string and passed
    /// `Some("")` made pre-commit judge every bead-tagged hunk against an
    /// empty message, so any commit containing a `bd-*` line was blocked at
    /// pre-commit as well as at commit-msg — the message could not possibly
    /// satisfy it, because pre-commit never sees one.
    #[test]
    fn no_message_file_skips_attribution_rather_than_emptying_it() {
        let root = repo("# no declaration here\n");
        let staged: Vec<String> = STAGED.iter().map(|s| (*s).to_string()).collect();
        let own = vec![toron_core::mailbox::guard::Reservation {
            holder: "IvoryRidge".to_string(),
            pattern: "**".to_string(),
            exclusive: true,
        }];
        let resolve = |n: &str| n.to_string();

        // pre-commit's shape: no message at all.
        let v = guard_check_verdict(
            &root,
            "demo",
            Some("IvoryRidge"),
            &staged,
            &own,
            &resolve,
            None,
        );
        assert_eq!(
            v.report["unattributed_bead_tokens"],
            json!([]),
            "with no message there is nothing to charge: {}",
            v.report
        );
        assert_eq!(
            v.exit_code, 0,
            "pre-commit must not block on a rule it cannot evaluate: {:?}",
            v.notices
        );

        // And the contrast: an empty MESSAGE is a real, unsatisfied message.
        let w = guard_check_verdict(
            &root,
            "demo",
            Some("IvoryRidge"),
            &staged,
            &own,
            &resolve,
            Some(""),
        );
        assert_eq!(
            w.report["unattributed_bead_tokens"],
            json!(["bd-source01", "bd-foreign1"]),
            "an empty message attributes nothing: {}",
            w.report
        );
        assert_eq!(w.exit_code, 1, "and it blocks");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_projected_path_still_needs_its_own_reservation() {
        // The exemption is for the HUNK rule only. A projected path with no
        // lease must still be Unreserved, or the projection is an
        // unaudited write lane that happens to also skip attribution.
        let root = repo("docs/guides/** linguist-generated=true\n");
        let staged = vec!["docs/guides/mail.md".to_string()];
        let resolve = |n: &str| n.to_string();
        let v = guard_check_verdict(
            &root,
            "demo",
            Some("IvoryRidge"),
            &staged,
            &[],
            &resolve,
            Some("docs: [bd-foreign1]\n"),
        );

        assert_eq!(
            v.report["blocked"][0]["reason"],
            json!("unreserved"),
            "declared projection is not shared-safe: {}",
            v.report
        );
        assert_eq!(v.exit_code, 1);
        let _ = std::fs::remove_dir_all(root);
    }
}
