// governed-by: ADR-0041
//! `toron next | ack | done | inbox append | send | task` (ADR-0041): CLI-only comms
//! verbs over the shared durable root, behind `TORON_COMMS_INBOX` or `[comms] inbox_log`.
//!
//! Exit codes: 0 ok, 1 no urgent (urgent-peek only), 2 flag off / usage,
//! 3 refused by the delivery FSM or the envelope rules (the refusal JSON is
//! printed on stdout), 4 ingest degraded (mail ingest failed; the served
//! rows are stale and must never read as empty).

use std::io::Read as _;
use std::process::ExitCode;

use serde_json::json;
use toron_core::inbox::InboxError;

use crate::cli::{BoardCommand, Command, InboxCommand};
use crate::output::{OutputFormat, render_json};

/// Derive the daemon's `/v1/*` base URL from `TORON_MCP_URL` / `FLYWHEEL_TORON_URL`
/// unless `TORON_CLI_DIRECT=1` is set.
pub fn daemon_v1_base_url(
    env_url: Option<&str>,
    fallback_url: Option<&str>,
    direct: bool,
) -> Option<String> {
    if direct {
        return None;
    }
    let raw = env_url
        .or(fallback_url)
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    let trimmed = raw.trim_end_matches('/');
    let base = trimmed.strip_suffix("/mcp").unwrap_or(trimmed);
    Some(base.to_string())
}

fn live_daemon_v1_base() -> Option<String> {
    let direct = std::env::var("TORON_CLI_DIRECT")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    let toron_mcp = std::env::var("TORON_MCP_URL").ok();
    let fw_url = std::env::var("FLYWHEEL_TORON_URL").ok();
    daemon_v1_base_url(toron_mcp.as_deref(), fw_url.as_deref(), direct)
}

async fn try_daemon_v1_post(
    path: &str,
    payload: serde_json::Value,
) -> Option<(u16, serde_json::Value)> {
    let base = live_daemon_v1_base()?;
    let url = format!("{base}{path}");
    tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(Some(std::time::Duration::from_millis(500)))
            .timeout_global(Some(std::time::Duration::from_secs(3)))
            .http_status_as_error(false)
            .build()
            .into();
        let body_str = serde_json::to_string(&payload).ok()?;
        let mut resp = agent
            .post(&url)
            .header("content-type", "application/json")
            .send(body_str)
            .ok()?;
        let status = resp.status().as_u16();
        let text = resp.body_mut().read_to_string().ok()?;
        let body: serde_json::Value = serde_json::from_str(&text).ok()?;
        Some((status, body))
    })
    .await
    .ok()
    .flatten()
}

#[tokio::main]
pub async fn run_comms(
    command: &Command,
    format: OutputFormat,
    cfg: &crate::config::Config,
) -> anyhow::Result<ExitCode> {
    let command_is_board = matches!(command, Command::Board(_));
    if !command_is_board && !toron_core::inbox::try_enabled_with(cfg.comms_inbox_log())? {
        eprintln!("toron: error: {}", InboxError::Disabled);
        return Ok(ExitCode::from(2));
    }
    if let Command::Next(a) = command {
        if a.urgent && !a.peek {
            eprintln!("toron: error: --urgent requires --peek");
            return Ok(ExitCode::from(2));
        }
        if let Some((200, v)) = try_daemon_v1_post(
            "/v1/next",
            json!({
                "project": a.project,
                "who": a.who,
                "stream": a.stream,
                "peek": a.peek,
                "urgent": a.urgent,
            }),
        )
        .await
            && v.get("batch").is_some_and(serde_json::Value::is_object)
        {
            let degraded = v["degraded"].as_str();
            if a.urgent {
                if let Some(err) = degraded {
                    println!("degraded: mail ingest failed ({err})");
                    return Ok(ExitCode::from(4));
                }
                if v["urgent_pending"].as_bool().unwrap_or(false) {
                    println!("urgent");
                    return Ok(ExitCode::SUCCESS);
                }
                return Ok(ExitCode::from(1));
            }
            if format == OutputFormat::Human {
                print_next_human_json(&v["batch"], a.peek);
                if let Some(err) = degraded {
                    println!("degraded: mail ingest failed ({err})");
                    return Ok(ExitCode::from(4));
                }
                return Ok(ExitCode::SUCCESS);
            }
            let out_val = match degraded {
                Some(err) => with_degraded(v["batch"].clone(), err),
                None => v["batch"].clone(),
            };
            render_json(&out_val, format);
            return Ok(if degraded.is_some() {
                ExitCode::from(4)
            } else {
                ExitCode::SUCCESS
            });
        }
    }
    if let Command::Board(board_cmd) = command {
        return run_board(board_cmd, format).await;
    }
    let mailbox = toron_mcp::demo_mailbox().await?;
    let store = mailbox.inbox_store_with(cfg.comms_inbox_log())?;
    // Ingest degradation rides out of the `Next` arm (JSON field + human
    // line + exit 4); other verbs never set it.
    let mut next_degraded: Option<String> = None;
    let out = match command {
        Command::Next(a) => {
            if a.urgent && !a.peek {
                eprintln!("toron: error: --urgent requires --peek");
                return Ok(ExitCode::from(2));
            }
            let reader = mailbox.comms_npub(&a.project, &a.who).await?;
            // Every delivered DM ingests into the reader's log first, so
            // `next` sees each message (per-message cursor + Delivered).
            // Ingest trouble must not take `next` down with it: loud, then
            // serve the rows already in the log — but never as a clean read.
            // A degraded read exits 4 so the harness hook (`next --urgent
            // --peek >/dev/null 2>&1`) cannot mistake it for "no mail".
            next_degraded = match mailbox
                .ingest_delivered_mail(&a.project, &a.who, &store)
                .await
            {
                Ok(_) => None,
                Err(e) => {
                    eprintln!("toron: mail ingest failed ({e:#}); serving logged rows only");
                    Some(format!("{e:#}"))
                }
            };
            let batch = store.next(&reader, a.stream.as_deref(), a.peek).await;
            if batch.is_ok() {
                let now_us = toron_core::mailbox::lease_liveness::host_now_us();
                mailbox.record_inbox_poll(&a.project, &a.who, now_us).await;
            }
            if batch.is_ok() && !a.peek {
                // bd-yc6jf: a committed read advances the same high-water a
                // `mail inbox --since last` read advances, so a CLI pull at
                // or after a mail's created_at suppresses the next ring.
                // Best-effort and fail-open: a lost bump re-rings (the old
                // behavior) instead of skipping a wake.
                let _ = mailbox.note_committed_read(&a.project, &a.who).await;
            }
            match batch {
                Ok(b) if a.urgent => {
                    if let Some(err) = next_degraded.as_deref() {
                        println!("degraded: mail ingest failed ({err})");
                        return Ok(ExitCode::from(4));
                    }
                    if toron_core::inbox::urgent_pending(&b.items) {
                        println!("urgent");
                        return Ok(ExitCode::SUCCESS);
                    }
                    return Ok(ExitCode::from(1));
                }
                Ok(b) if format == OutputFormat::Human => {
                    print_next_human(&b, a.peek);
                    if let Some(err) = next_degraded.as_deref() {
                        println!("degraded: mail ingest failed ({err})");
                        return Ok(ExitCode::from(4));
                    }
                    return Ok(ExitCode::SUCCESS);
                }
                other => other.map(|b| serde_json::to_value(b).unwrap_or_default()),
            }
        }
        Command::Ack(a) => {
            let reader = mailbox.comms_npub(&a.project, &a.who).await?;
            let accept = a.verdict == "accept";
            if !accept && a.reason.as_deref().is_none_or(str::is_empty) {
                eprintln!("toron: error: ack reject needs --reason");
                return Ok(ExitCode::from(2));
            }
            store
                .ack(&reader, &a.msg, accept, a.reason.as_deref())
                .await
                .map(|s| json!({"msg": a.msg, "recipient": reader, "state": s}))
        }
        Command::Done(a) => {
            let reader = mailbox.comms_npub(&a.project, &a.who).await?;
            store.done(&reader, &a.task, &a.evidence).await.map(
                |(msg, s)| json!({"task": a.task, "msg": msg, "recipient": reader, "state": s}),
            )
        }
        Command::Inbox(InboxCommand::Append(a)) => {
            let raw = read_input(&a.file)?;
            match toron_core::envelope::Envelope::from_json(&raw) {
                Ok(env) => store.append_envelope(&env).await.map(|ap| {
                    json!({"msg": env.msg.key(), "stream": env.to.stream(), "offset": ap.offset, "replay": ap.replay})
                }),
                Err(r) => Err(InboxError::Reject(r)),
            }
        }
        Command::Inbox(InboxCommand::Send(a)) => {
            let sender = mailbox.comms_npub(&a.project, &a.who).await?;
            let mut sends = Vec::new();
            if let Some(f) = &a.file {
                let raw = read_input(f)?;
                match toron_core::envelope::Envelope::from_json(&raw) {
                    Ok(env) if env.from == sender => sends.push(env),
                    Ok(env) => {
                        let r = toron_core::delivery::Reject::SenderIsNotProducer {
                            from: env.from,
                            producer: sender,
                        };
                        render_json(
                            &json!({"refused": r, "error": "envelope `from` is not --as"}),
                            format,
                        );
                        return Ok(ExitCode::from(3));
                    }
                    Err(r) => {
                        render_json(&json!({"refused": r, "error": r.to_string()}), format);
                        return Ok(ExitCode::from(3));
                    }
                }
            }
            toron_core::outbox::send_and_recover(store, sender, sends)
                .await
                .map(|r| serde_json::to_value(r).unwrap_or_default())
        }
        Command::Inbox(InboxCommand::Task(a)) => {
            let viewer = mailbox.comms_npub(&a.project, &a.who).await?;
            store.by_task(&a.task, &viewer).await.map(|messages| {
                json!({"task": a.task, "viewer": viewer, "count": messages.len(), "messages": messages})
            })
        }
        Command::Inbox(InboxCommand::Msg(a)) => {
            let Some(msg) = toron_core::envelope::MsgId::parse_key(&a.key) else {
                render_json(
                    &json!({"refused": "bad_key", "error": "key must be producer:epoch:seq"}),
                    format,
                );
                return Ok(ExitCode::from(3));
            };
            let viewer = mailbox.comms_npub(&a.project, &a.who).await?;
            store.read_triple(&msg, &viewer).await.map(|found| {
                json!({"key": a.key, "viewer": viewer, "found": found.is_some(), "message": found})
            })
        }
        Command::Inbox(InboxCommand::Cursor(a)) => {
            match mailbox.cursor_us(&a.project, &a.who).await {
                Ok(cursor) => Ok(json!({
                    "project": a.project,
                    "agent": a.who,
                    "cursor_us": cursor,
                })),
                Err(e) => {
                    render_json(
                        &json!({"refused": "cursor", "error": e.to_string()}),
                        format,
                    );
                    return Ok(ExitCode::from(3));
                }
            }
        }
        _ => unreachable!("run_comms only handles the comms verbs"),
    };
    match out {
        Ok(v) => {
            if let Some(err) = next_degraded {
                render_json(&with_degraded(v, &err), format);
                return Ok(ExitCode::from(4));
            }
            render_json(&v, format);
            Ok(ExitCode::SUCCESS)
        }
        Err(InboxError::Reject(r)) => {
            if let Some(err) = next_degraded {
                render_json(
                    &json!({"refused": r, "error": r.to_string(), "ingest_degraded": err}),
                    format,
                );
                return Ok(ExitCode::from(4));
            }
            render_json(&json!({"refused": r, "error": r.to_string()}), format);
            Ok(ExitCode::from(3))
        }
        Err(e @ InboxError::NoSuchDelivery { .. }) => {
            if let Some(err) = next_degraded {
                render_json(
                    &json!({"refused": "no_such_delivery", "error": e.to_string(), "ingest_degraded": err}),
                    format,
                );
                return Ok(ExitCode::from(4));
            }
            render_json(
                &json!({"refused": "no_such_delivery", "error": e.to_string()}),
                format,
            );
            Ok(ExitCode::from(3))
        }
        Err(InboxError::Append(e)) => {
            if let Some(err) = next_degraded {
                render_json(
                    &json!({"refused": "append", "error": e.to_string(), "ingest_degraded": err}),
                    format,
                );
                return Ok(ExitCode::from(4));
            }
            render_json(
                &json!({"refused": "append", "error": e.to_string()}),
                format,
            );
            Ok(ExitCode::from(3))
        }
        Err(e) => Err(e.into()),
    }
}

fn print_next_human_json(b: &serde_json::Value, peek: bool) {
    let mode = if peek {
        " (peek: nothing committed)"
    } else {
        ""
    };
    let items = b["items"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    println!(
        "inbox {} cursor {} -> {}{mode}, {} message(s)",
        b["reader"].as_str().unwrap_or(""),
        b["cursor_before"],
        b["cursor_after"],
        items.len()
    );
    for it in items {
        let env = &it["envelope"];
        let title = env["body"]["title"]
            .as_str()
            .or_else(|| env["body"]["text"].as_str())
            .unwrap_or("");
        println!();
        println!(
            "#{} {} {} {} from {}",
            it["offset"],
            it["msg_key"].as_str().unwrap_or(""),
            env["class"].as_str().unwrap_or(""),
            env["contract"].as_str().unwrap_or(""),
            env["from"].as_str().unwrap_or(""),
        );
        if !title.is_empty() {
            println!("  {title}");
        }
        println!("  phase: {}", it["phase"].as_str().unwrap_or(""));
    }
}

/// Human view of `toron next`: one block per message, ending in the verbs
/// the reader may run now (redesign §6.2 "Allowed now").
fn print_next_human(b: &toron_core::inbox::NextBatch, peek: bool) {
    let mode = if peek {
        " (peek: nothing committed)"
    } else {
        ""
    };
    println!(
        "inbox {} cursor {} -> {}{mode}, {} message(s)",
        b.reader,
        b.cursor_before,
        b.cursor_after,
        b.items.len()
    );
    for it in &b.items {
        let env = &it.envelope;
        let title = env
            .body
            .get("title")
            .or_else(|| env.body.get("text"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        println!();
        println!(
            "#{} {} {:?} {} from {}",
            it.offset, it.msg_key, env.class, env.contract, env.from
        );
        if !title.is_empty() {
            println!("  {title}");
        }
        println!("  phase: {}", it.phase.as_str());
        if it.allowed_now.is_empty() {
            println!("  Allowed now: (nothing)");
        } else {
            println!("  Allowed now: {}", it.allowed_now.join(" | "));
        }
    }
}

fn read_input(file: &str) -> std::io::Result<String> {
    if file == "-" {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        Ok(s)
    } else {
        std::fs::read_to_string(file)
    }
}

async fn run_board(cmd: &BoardCommand, format: OutputFormat) -> anyhow::Result<ExitCode> {
    match cmd {
        BoardCommand::Post(a) => {
            if let Some((status, body)) = try_daemon_v1_post(
                "/v1/board/post",
                json!({
                    "project": a.project,
                    "channel": a.channel,
                    "author": a.who,
                    "body": a.body,
                    "mentions": a.mentions,
                    "sensitivity": a.sensitivity,
                }),
            )
            .await
            {
                if status == 200 {
                    render_json(&body, format);
                    return Ok(ExitCode::SUCCESS);
                }
                if status == 400 {
                    eprintln!(
                        "toron: {}",
                        body["error"].as_str().unwrap_or("board_post refused")
                    );
                    return Ok(ExitCode::from(3));
                }
            }
            let mb = toron_mcp::demo_mailbox().await?;
            match mb
                .board_post(
                    &a.project,
                    &a.channel,
                    &a.who,
                    &a.body,
                    &a.mentions,
                    a.sensitivity.as_deref(),
                )
                .await
            {
                Ok(msg) => {
                    render_json(&serde_json::to_value(msg)?, format);
                    Ok(ExitCode::SUCCESS)
                }
                Err(e)
                    if e.downcast_ref::<toron_core::mailbox::board::BoardRefusal>()
                        .is_some() =>
                {
                    eprintln!("toron: {e:#}");
                    Ok(ExitCode::from(3))
                }
                Err(e) => Err(e),
            }
        }
        BoardCommand::Read(a) => {
            if let Some((200, body)) = try_daemon_v1_post(
                "/v1/board/read",
                json!({
                    "project": a.project,
                    "channel": a.channel,
                    "since_seq": a.since_seq,
                    "limit": a.limit,
                }),
            )
            .await
            {
                render_json(&body, format);
                return Ok(ExitCode::SUCCESS);
            }
            let mb = toron_mcp::demo_mailbox().await?;
            let res = mb
                .board_read(&a.project, &a.channel, a.since_seq, a.limit)
                .await?;
            render_json(&serde_json::to_value(res)?, format);
            Ok(ExitCode::SUCCESS)
        }
        BoardCommand::Wait(a) => {
            if let Some((200, body)) = try_daemon_v1_post(
                "/v1/board/wait",
                json!({
                    "project": a.project,
                    "channel": a.channel,
                    "since_seq": a.since_seq,
                    "mentions_only": a.mentions_only,
                    "recipient": a.who,
                    "timeout_ms": a.timeout_ms,
                }),
            )
            .await
            {
                render_json(&body, format);
                return Ok(ExitCode::SUCCESS);
            }
            let mb = toron_mcp::demo_mailbox().await?;
            let res = mb
                .board_wait(
                    &a.project,
                    &a.channel,
                    a.since_seq,
                    a.mentions_only,
                    a.who.as_deref(),
                    a.timeout_ms,
                )
                .await?;
            render_json(&serde_json::to_value(res)?, format);
            Ok(ExitCode::SUCCESS)
        }
    }
}

/// Attach the ingest-degradation signal to a `next` JSON result. Healthy
/// results carry no such field; degraded ones name the failure so a quiet
/// read is never mistaken for an empty one.
fn with_degraded(mut v: serde_json::Value, err: &str) -> serde_json::Value {
    if let Some(o) = v.as_object_mut() {
        o.insert("ingest_degraded".into(), err.into());
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_next_json_carries_no_degraded_field() {
        let v = json!({"reader": "r", "items": []});
        assert!(v.get("ingest_degraded").is_none());
    }

    #[test]
    fn degraded_next_json_names_the_failure() {
        let v = with_degraded(json!({"reader": "r", "items": []}), "boom");
        assert_eq!(
            v.get("ingest_degraded").and_then(|e| e.as_str()),
            Some("boom")
        );
    }

    #[test]
    fn degraded_exit_is_four_not_zero() {
        assert_ne!(ExitCode::from(4), ExitCode::SUCCESS);
        assert_ne!(ExitCode::from(4), ExitCode::from(1));
        assert_ne!(ExitCode::from(4), ExitCode::from(3));
    }

    #[test]
    fn daemon_v1_base_url_strips_mcp_suffix_and_honors_direct_bypass() {
        assert_eq!(
            daemon_v1_base_url(Some("http://127.0.0.1:18080/mcp/"), None, false).as_deref(),
            Some("http://127.0.0.1:18080")
        );
        assert_eq!(
            daemon_v1_base_url(None, Some("http://127.0.0.1:18080"), false).as_deref(),
            Some("http://127.0.0.1:18080")
        );
        assert_eq!(
            daemon_v1_base_url(Some("http://127.0.0.1:18080/mcp"), None, true),
            None
        );
    }
}
