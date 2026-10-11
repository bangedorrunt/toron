// governed-by: ADR-0041
//! `toron next | ack | done | inbox append` (ADR-0041): CLI-only comms
//! verbs over the shared durable root, behind `TORON_COMMS_INBOX`.
//!
//! Exit codes: 0 ok, 2 flag off / usage, 3 refused by the delivery FSM or
//! the envelope rules (the refusal JSON is printed on stdout).

use std::io::Read as _;
use std::process::ExitCode;

use serde_json::json;
use toron_core::inbox::InboxError;

use crate::cli::{Command, InboxCommand};
use crate::output::{OutputFormat, render_json};

#[tokio::main]
pub async fn run_comms(command: &Command, format: OutputFormat) -> anyhow::Result<ExitCode> {
    if !toron_core::inbox::enabled() {
        eprintln!("toron: error: {}", InboxError::Disabled);
        return Ok(ExitCode::from(2));
    }
    let mailbox = toron_mcp::demo_mailbox().await?;
    let store = mailbox.inbox_store()?;
    let out = match command {
        Command::Next(a) => {
            let reader = mailbox.comms_npub(&a.project, &a.who).await?;
            match store.next(&reader, a.stream.as_deref(), a.peek).await {
                Ok(b) if format == OutputFormat::Human => {
                    print_next_human(&b, a.peek);
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
        _ => unreachable!("run_comms only handles the comms verbs"),
    };
    match out {
        Ok(v) => {
            render_json(&v, format);
            Ok(ExitCode::SUCCESS)
        }
        Err(InboxError::Reject(r)) => {
            render_json(&json!({"refused": r, "error": r.to_string()}), format);
            Ok(ExitCode::from(3))
        }
        Err(e @ InboxError::NoSuchDelivery { .. }) => {
            render_json(
                &json!({"refused": "no_such_delivery", "error": e.to_string()}),
                format,
            );
            Ok(ExitCode::from(3))
        }
        Err(InboxError::Append(e)) => {
            render_json(
                &json!({"refused": "append", "error": e.to_string()}),
                format,
            );
            Ok(ExitCode::from(3))
        }
        Err(e) => Err(e.into()),
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
