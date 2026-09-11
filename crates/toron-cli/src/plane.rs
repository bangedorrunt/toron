// governed-by: ADR-0003
// governed-by: ADR-0020
// governed-by: ADR-0023
#![forbid(unsafe_code)]

//! Two-plane shared surface (ADR-0020 Spec 2): channel/react (plane S),
//! job (plane J), presence (plane N), watch (plane W). CLI-first; the MCP
//! cluster stays frozen.
//!
//! Plane P (private NIP-17 gift-wrap) stays `toron mail *`. These handlers
//! never gift-wrap: kind-9 posts are plaintext to channel members.
//!
//! Identity (ADR-0018): writes sign with the `--as` agent's REGISTERED key
//! (ncryptsec under `{project}/agents/{name}/key.ncryptsec`). Unknown `--as`
//! is a [`UsageError`] (exit 2) — the CLI never calls `Keys::generate()` in a
//! write path. Agent keys are encrypted with the mailbox default passphrase
//! ("toron-dev", matching `Mailbox::agent_passphrase` when
//! `with_agent_passphrase` was never called — `demo_mailbox` does not call it).
//!
//! Channel lifecycle is persisted on the bus so a fresh CLI process can
//! rebuild the in-memory `ChannelStore` from replay (cross-process SC-27):
//!   - kind 39000 group metadata (ADR-0023): `d`=`h`=id, `name`, `private`,
//!     `t`=project; close adds `closed`. Signed by the creator.
//!   - kind 39002 members snapshot: `d`=`h`=id, `p`=each member. Signed by
//!     the joining/leaving actor. Legacy ingest: 40/41.
//!
//! Posts (kind 9) and reactions (kind 7) ride the bus as their own events.
//!
//! Presence (20001) is ephemeral on the bus (module contract); `presence list`
//! reads a CLI-side last-wins projection (`{data_dir}/presence.jsonl`) so a
//! separate process sees another process's heartbeat within the TTL.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use nostr::prelude::{Event, EventId, Keys, Kind, PublicKey, Tag, Timestamp};
use serde_json::Value;
use toron_core::bus::{Bus, Filter, SubscriptionEvent};
use toron_core::mailbox::{Mailbox, WhoIs, aliases, collect_replay};
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::toron_kind;
use toron_core::nostr::presence::{PresencePayload, PresenceState, build_presence_event};
use toron_workflow::channels::{
    CHANNEL_CREATE, CHANNEL_CREATE_LEGACY, CHANNEL_MEMBERS, CHANNEL_MESSAGE,
    CHANNEL_MESSAGE_LEGACY, CHANNEL_METADATA_LEGACY, ChannelStore, REACTION,
};
use toron_workflow::coalesce::MentionCoalescer;
use toron_workflow::jobs::{
    JobError, JobRequest, JobResult, JobState, build_job_cancel, build_job_claim, build_job_error,
    build_job_progress, build_job_result, first_claimer, job_parent, job_state,
};
use toron_workflow::mention::{build_job_from_coalesced, wire_mention_dispatch};

use crate::cli::{
    ChannelCloseArgs, ChannelCommand, ChannelCreateArgs, ChannelJoinArgs, ChannelListArgs,
    ChannelMembersArgs, ChannelPostArgs, ChannelPurgeArgs, ChannelReadArgs, JobCancelArgs,
    JobClaimArgs, JobCommand, JobErrorArgs, JobListArgs, JobProgressArgs, JobResultArgs,
    JobShowArgs, JobStateArg, JobStatusArg, PresenceCommand, PresenceListArgs, PresencePublishArgs,
    PresenceStateArg, ReactArgs, WaitArgs, WatchArgs,
};

/// Default presence TTL (mirrors `bus::presence::DEFAULT_PRESENCE_TTL`).
const PRESENCE_TTL_SECS: u64 = 60;

/// Usage error → exit 2 (clig.dev, ADR-0003). Unknown `--as` and malformed
/// invocations clap cannot express (e.g. `--once` without `--timeout-ms`)
/// land here.
#[derive(Debug)]
pub struct UsageError(pub String);

impl std::fmt::Display for UsageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for UsageError {}

/// Pretty-print a JSON value to stdout.
fn print_json(value: &Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// Monotonic-ish millis for the mention coalescer (SystemTime since epoch).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The project slug an event is scoped to (mirrors InProcessBus's rule:
/// `toron:`-prefixed `t` values are internal markers, not slugs).
fn event_project(ev: &Event) -> Option<&str> {
    ev.tags.iter().find_map(|t| {
        if t.kind() == "t" {
            let c = t.content()?;
            if c.starts_with("toron:") {
                return None;
            }
            Some(c)
        } else {
            None
        }
    })
}

/// True when the event belongs to `project`: project-scoped events must match
/// the slug; un-scoped events (presence, reactions, DM-path jobs) are
/// project-agnostic on the local bus and pass through.
fn in_project(ev: &Event, project: &str) -> bool {
    match event_project(ev) {
        Some(slug) => slug == project,
        None => true,
    }
}

/// Collect all events matching `filter` via a TRUSTED subscription (the CLI
/// holds the bus by construction — like the workflow host), draining replay
/// until the EOSE-equivalent marker.
async fn collect_trusted(bus: &dyn Bus, filter: Filter) -> anyhow::Result<Vec<Event>> {
    let mut sub = bus.subscribe_trusted(filter).await?;
    let mut events = Vec::new();
    while let Some(SubscriptionEvent::Event(e)) = sub.next().await {
        events.push(e);
    }
    Ok(events)
}

/// Resolve `--as <name>` to the agent's registered keys (ADR-0018).
///
/// Unknown name → [`UsageError`] (exit 2). No `Keys::generate()` — if the
/// key file is missing the write is refused.
async fn resolve_agent(mailbox: &Mailbox, project: &str, name: &str) -> anyhow::Result<Keys> {
    let whois = mailbox
        .whois(project, name)
        .await
        .map_err(|_| UsageError(format!("unknown agent name: {name} for project {project}")))?;
    load_agent_key(mailbox, project, &whois).await
}

/// Load the registered agent's ncryptsec from the project identity root.
async fn load_agent_key(mailbox: &Mailbox, project: &str, whois: &WhoIs) -> anyhow::Result<Keys> {
    let canonical = mailbox.canonical_project(project).await;
    let path = aliases::project_root(mailbox.data_dir(), &canonical)
        .join("agents")
        .join(&whois.name)
        .join("key.ncryptsec");
    toron_core::keys::load_encrypted_key(&path, "toron-dev").map_err(|e| {
        anyhow::anyhow!(
            "agent {} has no usable local key at {} ({e}); register the agent first",
            whois.name,
            path.display()
        )
    })
}

/// Rebuild the in-memory channel store from bus lifecycle events so a
/// fresh process sees rooms created by other processes (cross-process SC-27).
/// Canonical: 39000/39002. Legacy ingest: 40/41.
async fn rebuild_store(mailbox: &Mailbox, project: &str) -> anyhow::Result<ChannelStore> {
    let store = ChannelStore::new();
    let filter = Filter::new()
        .kinds([
            Kind::GroupMetadata,
            Kind::GroupMembers,
            toron_kind(CHANNEL_CREATE_LEGACY),
            toron_kind(CHANNEL_METADATA_LEGACY),
        ])
        .hashtag(project);
    let events = collect_replay(mailbox.bus().as_ref(), filter).await?;
    for ev in events {
        match ev.kind.as_u16() {
            CHANNEL_CREATE | CHANNEL_CREATE_LEGACY => {
                let id = ev
                    .tags
                    .iter()
                    .find(|t| t.kind() == "d")
                    .and_then(|t| t.content())
                    .or_else(|| {
                        ev.tags
                            .iter()
                            .find(|t| t.kind() == "h")
                            .and_then(|t| t.content())
                    })
                    .unwrap_or_default();
                let name = ev
                    .tags
                    .iter()
                    .find(|t| t.as_slice().first().map(String::as_str) == Some("name"))
                    .and_then(|t| t.as_slice().get(1).cloned())
                    .unwrap_or_default();
                let private = ev.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("private")
                        && t.as_slice().get(1).map(String::as_str) == Some("true")
                });
                let _ = store.create_channel(&ev.pubkey, id, &name, private, Some(project));
                let closed = ev.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("closed")
                        && t.as_slice().get(1).map(String::as_str) != Some("false")
                });
                if closed {
                    let _ = store.close(id, &ev.pubkey);
                }
            }
            CHANNEL_METADATA_LEGACY => {
                let id = ev
                    .tags
                    .iter()
                    .find(|t| t.kind() == "h")
                    .and_then(|t| t.content())
                    .unwrap_or_default();
                match ev.content.as_str() {
                    "join" => {
                        let _ = store.join(id, &ev.pubkey);
                    }
                    "leave" => {
                        let _ = store.leave(id, &ev.pubkey);
                    }
                    "close" => {
                        let _ = store.close(id, &ev.pubkey);
                    }
                    _ => {}
                }
            }
            CHANNEL_MEMBERS => {
                let id = ev
                    .tags
                    .iter()
                    .find_map(|t| {
                        (t.kind() == "h" || t.kind() == "d")
                            .then(|| t.content())
                            .flatten()
                    })
                    .unwrap_or_default();
                // Content JSON is the snapshot source of truth: rust-nostr
                // strips a p-tag that equals the signer, which would drop the
                // joiner from a 39002 they just signed.
                let members: Vec<String> = serde_json::from_str(&ev.content).unwrap_or_else(|_| {
                    ev.tags
                        .iter()
                        .filter(|t| t.as_slice().first().map(String::as_str) == Some("p"))
                        .filter_map(|t| t.as_slice().get(1).cloned())
                        .collect()
                });
                let _ = store.replace_members(id, members);
            }
            _ => {}
        }
    }
    Ok(store)
}

/// The `h` tag of an event (channel binding).
fn event_channel(ev: &Event) -> Option<String> {
    ev.tags
        .iter()
        .find(|t| t.kind() == "h")
        .and_then(|t| t.content())
        .map(str::to_string)
}

/// `--since` RFC3339 → unix seconds.
fn parse_since(s: &str) -> anyhow::Result<u64> {
    let dt = chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|e| anyhow::anyhow!("bad --since {s:?}: {e}"))?;
    Ok(dt.timestamp().max(0) as u64)
}

// ---------------------------------------------------------------------------
// plane S — channels + reactions
// ---------------------------------------------------------------------------

/// Publish the channel-create lifecycle event (kind 39000).
async fn publish_create(
    bus: &dyn Bus,
    keys: &Keys,
    args: &ChannelCreateArgs,
) -> anyhow::Result<Event> {
    let ev = build_event(
        keys,
        Kind::GroupMetadata,
        "channel-create",
        vec![
            Tag::parse(["d", &args.id]).expect("d"),
            Tag::parse(["h", &args.id]).expect("h"),
            Tag::parse(["name", &args.name]).expect("name"),
            Tag::parse(["private", if args.private { "true" } else { "false" }]).expect("private"),
            Tag::parse(["t", &args.project]).expect("t"),
        ],
        Timestamp::now(),
    )?;
    bus.publish(ev.clone()).await?;
    Ok(ev)
}

/// Publish a kind-39002 member snapshot after join/leave.
async fn publish_members(
    bus: &dyn Bus,
    keys: &Keys,
    channel: &str,
    project: &str,
    members: &[String],
) -> anyhow::Result<Event> {
    let mut tags = vec![
        Tag::parse(["d", channel]).expect("d"),
        Tag::parse(["h", channel]).expect("h"),
        Tag::parse(["t", project]).expect("t"),
    ];
    for m in members {
        if let Ok(pk) = PublicKey::from_hex(m) {
            tags.push(Tag::public_key(pk));
        }
    }
    let content = serde_json::to_string(members).unwrap_or_else(|_| "[]".into());
    let ev = build_event(keys, Kind::GroupMembers, content, tags, Timestamp::now())?;
    bus.publish(ev.clone()).await?;
    Ok(ev)
}

async fn channel_create(mailbox: &Mailbox, args: &ChannelCreateArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;
    store.create_channel(
        &keys.public_key(),
        &args.id,
        &args.name,
        args.private,
        Some(&args.project),
    )?;
    let ev = publish_create(mailbox.bus().as_ref(), &keys, args).await?;
    print_json(&serde_json::json!({
        "channel_id": args.id,
        "name": args.name,
        "private": args.private,
        "creator": keys.public_key().to_hex(),
        "event_id": ev.id.to_hex(),
    }))
}

async fn channel_join(mailbox: &Mailbox, args: &ChannelJoinArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;
    if args.if_missing && store.is_member(&args.id, &keys.public_key()) {
        print_json(&serde_json::json!({
            "channel_id": args.id,
            "member": keys.public_key().to_hex(),
            "event_id": null,
            "if_missing": true,
        }))?;
        return Ok(());
    }
    store
        .join(&args.id, &keys.public_key())
        .with_context(|| format!("join {} as {}", args.id, args.as_name))?;
    let members = store.members(&args.id);
    let ev = publish_members(
        mailbox.bus().as_ref(),
        &keys,
        &args.id,
        &args.project,
        &members,
    )
    .await?;
    print_json(&serde_json::json!({
        "channel_id": args.id,
        "member": keys.public_key().to_hex(),
        "event_id": ev.id.to_hex(),
    }))?;
    Ok(())
}

async fn channel_leave(mailbox: &Mailbox, args: &ChannelJoinArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;
    store
        .leave(&args.id, &keys.public_key())
        .with_context(|| format!("leave {} as {}", args.id, args.as_name))?;
    let members = store.members(&args.id);
    let ev = publish_members(
        mailbox.bus().as_ref(),
        &keys,
        &args.id,
        &args.project,
        &members,
    )
    .await?;
    print_json(&serde_json::json!({
        "channel_id": args.id,
        "member": keys.public_key().to_hex(),
        "event_id": ev.id.to_hex(),
    }))?;
    Ok(())
}
/// Publish the channel-close tombstone (kind 39000 + `closed`).
async fn publish_close(
    bus: &dyn Bus,
    keys: &Keys,
    channel: &str,
    project: &str,
    name: &str,
    private: bool,
) -> anyhow::Result<Event> {
    let ev = build_event(
        keys,
        Kind::GroupMetadata,
        "channel-close",
        vec![
            Tag::parse(["d", channel]).expect("d"),
            Tag::parse(["h", channel]).expect("h"),
            Tag::parse(["name", name]).expect("name"),
            Tag::parse(["private", if private { "true" } else { "false" }]).expect("private"),
            Tag::parse(["closed", "true"]).expect("closed"),
            Tag::parse(["t", project]).expect("t"),
        ],
        Timestamp::now(),
    )?;
    bus.publish(ev.clone()).await?;
    Ok(ev)
}

/// `toron channel close <id>` — retire a channel (creator-only,
/// idempotent). The tombstone rides the bus; posts/joins refuse afterwards.
async fn channel_close(mailbox: &Mailbox, args: &ChannelCloseArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;
    let ch = store
        .get(&args.id)
        .with_context(|| format!("channel not found: {}", args.id))?;
    store
        .close(&args.id, &keys.public_key())
        .with_context(|| format!("close channel {} as {}", args.id, args.as_name))?;
    let ev = publish_close(
        mailbox.bus().as_ref(),
        &keys,
        &args.id,
        &args.project,
        &ch.name,
        ch.private,
    )
    .await?;
    let _ = ch; // (kept: future closed-at bookkeeping)
    print_json(&serde_json::json!({
        "channel": args.id,
        "closed": true,
        "already_closed": ch.closed,
        "tombstone_id": ev.id.to_hex(),
    }))
}

/// `toron channel purge <id>` — operator-grade removal of a CLOSED
/// channel's local bus events (creator or operator only; audited to
/// {data_dir}/channel_purge.log).
async fn channel_purge(mailbox: &Mailbox, args: &ChannelPurgeArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;
    let ch = store
        .get(&args.id)
        .with_context(|| format!("channel not found: {}", args.id))?;
    if !ch.closed {
        anyhow::bail!(
            "channel {} is still open — close it first (lifecycle: open -> closed -> purged): \
             toron channel close {} --project {} --as <creator>",
            args.id,
            args.id,
            args.project
        );
    }
    let caller = keys.public_key().to_hex();
    let is_creator = ch.creator == caller;
    let is_operator = mailbox
        .operator()
        .is_some_and(|op| op.public_key().to_hex() == caller);
    if !is_creator && !is_operator {
        anyhow::bail!(
            "purge refused: {} is neither the channel creator nor the operator \
             (bd-channel-close-purge-bsqb)",
            args.as_name
        );
    }

    // Every bus event bound to the channel (lifecycle + posts + reactions),
    // scoped to this project so a same-named channel in another project is
    // untouched.
    let filter = Filter::new()
        .kinds([
            Kind::GroupMetadata,
            Kind::GroupMembers,
            toron_kind(CHANNEL_CREATE_LEGACY),
            toron_kind(CHANNEL_METADATA_LEGACY),
            toron_kind(CHANNEL_MESSAGE),
            toron_kind(CHANNEL_MESSAGE_LEGACY),
            toron_kind(REACTION),
        ])
        .custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_H, &args.id);
    let events = collect_trusted(mailbox.bus().as_ref(), filter).await?;
    let ids: Vec<String> = events
        .iter()
        .filter(|ev| in_project(ev, &args.project))
        .filter(|ev| event_channel(ev).as_deref() == Some(args.id.as_str()))
        .map(|ev| ev.id.to_hex())
        .collect();
    let removed = mailbox
        .bus()
        .purge_ids(&ids)
        .await
        .map_err(|e| anyhow::anyhow!("purge failed: {e}"))?;

    // Audit trail (operator-grade action): who, what, how many.
    let log_line = serde_json::json!({
        "ts": chrono::Utc::now().to_rfc3339(),
        "project": args.project,
        "channel": args.id,
        "by": args.as_name,
        "role": if is_creator { "creator" } else { "operator" },
        "removed_events": removed,
    });
    let log_path = mailbox.data_dir().join("channel_purge.log");
    use std::io::Write as _;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        let _ = writeln!(f, "{log_line}");
    }

    print_json(&serde_json::json!({
        "channel": args.id,
        "purged": removed,
        "audit_log": log_path.display().to_string(),
    }))
}

async fn channel_list(mailbox: &Mailbox, args: &ChannelListArgs) -> anyhow::Result<()> {
    let reader = match &args.as_name {
        Some(name) => {
            let whois = mailbox.whois(&args.project, name).await.map_err(|_| {
                UsageError(format!(
                    "unknown agent name: {name} for project {}",
                    args.project
                ))
            })?;
            Some(PublicKey::from_hex(&whois.npub).context("whois npub parses")?)
        }
        None => None,
    };
    let store = rebuild_store(mailbox, &args.project).await?;
    let channels: Vec<Value> = store
        .list()
        .into_iter()
        .filter(|c| {
            // bd-channel-close-purge-bsqb: retired channels are clutter —
            // hidden unless --all, and flagged when shown.
            if c.closed && !args.all {
                return false;
            }
            // Private channels appear only to members; open channels to anyone.
            if !c.private {
                return true;
            }
            match &reader {
                Some(r) => c.is_member(r),
                None => false,
            }
        })
        .map(|c| {
            serde_json::json!({
                "id": c.id,
                "name": c.name,
                "private": c.private,
                "closed": c.closed,
                "project": c.project,
                "creator": c.creator,
                "members": c.members.iter().collect::<Vec<_>>(),
            })
        })
        .collect();
    print_json(&serde_json::Value::Array(channels))
}

async fn channel_members(mailbox: &Mailbox, args: &ChannelMembersArgs) -> anyhow::Result<()> {
    let store = rebuild_store(mailbox, &args.project).await?;
    let members = store.members(&args.id);
    print_json(&serde_json::json!({
        "channel_id": args.id,
        "members": members,
    }))
}

async fn channel_post(mailbox: &Mailbox, args: &ChannelPostArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;

    // bd-channel-truncation-238e: exactly one of --body / --file. A file
    // post chunks at 700 chars into an (1/N)-marked thread (the swarm's
    // crew-report convention, now the mechanism instead of a brief rule).
    let bodies: Vec<String> = match (&args.body, &args.file) {
        (Some(b), None) => vec![b.clone()],
        (None, Some(f)) => toron_workflow::channels::split_channel_post(
            &std::fs::read_to_string(f).with_context(|| format!("reading {}", f.display()))?,
            700,
        ),
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!("--body and --file are mutually exclusive"));
        }
        (None, None) => {
            return Err(anyhow::anyhow!("provide --body or --file <path>"));
        }
    };
    for (i, chunk) in bodies.iter().enumerate() {
        post_one(
            mailbox,
            &store,
            &keys,
            args,
            chunk,
            i == 0, // mentions ride the first chunk only
        )
        .await?;
    }
    if bodies.len() > 1 {
        eprintln!(
            "posted {} chunks ((1/{})-marked thread)",
            bodies.len(),
            bodies.len()
        );
    }
    Ok(())
}

/// One kind-9 post + mention dispatch (the former inline body of
/// `channel_post`, factored for the --file chunk loop).
async fn post_one(
    mailbox: &Mailbox,
    store: &toron_workflow::channels::ChannelStore,
    keys: &Keys,
    args: &ChannelPostArgs,
    body: &str,
    with_mentions: bool,
) -> anyhow::Result<()> {
    // Resolve --mention names → npubs via the project identity index.
    let mut mention_pks: Vec<PublicKey> = Vec::new();
    if !with_mentions {
        // chunked thread: dispatch on the first chunk only
    }
    for name in args.mention.iter().filter(|_| with_mentions) {
        let whois = mailbox
            .whois(&args.project, name)
            .await
            .map_err(|_| UsageError(format!("unknown --mention agent: {name}")))?;
        mention_pks.push(PublicKey::from_hex(&whois.npub).context("whois npub parses")?);
    }

    // Membership-gated store post (kind 9; non-member → error exit 1).
    let ev = store
        .post(keys, &args.id, body, &mention_pks)
        .with_context(|| format!("post to channel {}", args.id))?;
    mailbox.bus().publish(ev.clone()).await?;

    // ADR-0020 Spec 4: a kind-9 post with @mentions + p-tags dispatches one
    // 43001 per assignee through the mention coalescer. The CLI drives the
    // dispatcher inline (same wiring the workflow host uses) and force-flushes
    // so the job is visible immediately (SC-27 expects one 43001 after the
    // post, not a 2s coalesce wait).
    if !args.mention.is_empty() {
        let mut coalescer = MentionCoalescer::with_defaults();
        let mut jobs =
            wire_mention_dispatch(&ev, &mut coalescer, keys, Some(&args.project), now_ms());
        for batch in coalescer.force_flush_all() {
            if let Ok(job) =
                build_job_from_coalesced(&batch, keys, Some(&args.project), Timestamp::now())
            {
                jobs.push(job);
            }
        }
        for job in jobs {
            mailbox.bus().publish(job).await?;
        }
    }

    // bd-verify-after-send-fxtv: read-back verification — the send is
    // confirmed only when the projection serves the event id back.
    let mut verified = false;
    if let Some(secs) = args.wait_ack {
        let timeout = Duration::from_secs(secs.max(1));
        verified = wait_for_readback(mailbox, &args.id, &ev.id.to_hex(), timeout).await?;
        if !verified {
            anyhow::bail!(
                "verify-after-send FAILED: event {} was not readable from channel {} \
                 within {timeout:?} — delivery is NOT confirmed",
                ev.id.to_hex(),
                args.id
            );
        }
    }

    print_json(&serde_json::json!({
        "event_id": ev.id.to_hex(),
        "kind": 9,
        "channel": args.id,
        "mentions": mention_pks.iter().map(|p| p.to_hex()).collect::<Vec<_>>(),
        "verified": verified,
    }))
}

/// bd-verify-after-send-fxtv: poll the channel projection until the
/// just-published event id reads back (delivery is claimed only after it is
/// observed). Returns false on timeout — the caller fails loud.
async fn wait_for_readback(
    mailbox: &Mailbox,
    channel: &str,
    event_id: &str,
    timeout: Duration,
) -> anyhow::Result<bool> {
    let filter = Filter::new()
        .kinds([toron_kind(CHANNEL_MESSAGE)])
        .custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_H, channel);
    let deadline = Instant::now() + timeout;
    loop {
        let events = collect_trusted(mailbox.bus().as_ref(), filter.clone()).await?;
        if events.iter().any(|ev| ev.id.to_hex() == event_id) {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn channel_read(mailbox: &Mailbox, args: &ChannelReadArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;
    let ch = store
        .get(&args.id)
        .with_context(|| format!("channel not found: {}", args.id))?;
    // Membership gate: private channels are members-only; open channels are
    // readable by anyone on the project bus (ADR-0020 Spec 0).
    if ch.private && !store.can_read(&args.id, &keys.public_key()) {
        anyhow::bail!(
            "not a member of private channel {} — membership is the only gate",
            args.id
        );
    }

    // bd-channel-close-purge-bsqb: a closed channel is retired — history is
    // gone at purge; read refuses so a closed channel never looks live.
    if ch.closed {
        anyhow::bail!(
            "channel {} is closed (retired) — history lives until purge: \
             toron channel purge {} --project {} --as <creator|operator>",
            args.id,
            args.id,
            args.project
        );
    }

    // bd-delivery-visibility-g96y: `--author <pin>` narrows to one author —
    // "did the scout report?" is a single query. Resolved through whois so
    // the caller names the pin, never the npub.
    let author_pk: Option<PublicKey> = match &args.author {
        Some(name) => {
            let whois = mailbox
                .whois(&args.project, name)
                .await
                .map_err(|_| UsageError(format!("unknown --author agent: {name}")))?;
            Some(PublicKey::from_hex(&whois.npub).context("whois npub parses")?)
        }
        None => None,
    };

    let since = match &args.since {
        Some(s) => parse_since(s)?,
        None => 0,
    };
    let filter = Filter::new()
        .kinds([
            toron_kind(CHANNEL_MESSAGE),
            toron_kind(CHANNEL_MESSAGE_LEGACY),
            toron_kind(REACTION),
        ])
        .custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_H, &args.id);
    let mut events = collect_trusted(mailbox.bus().as_ref(), filter).await?;
    events.retain(|ev| {
        in_project(ev, &args.project)
            && ev.created_at.as_secs() >= since
            && event_channel(ev).as_deref() == Some(args.id.as_str())
    });
    if let Some(pk) = &author_pk {
        events.retain(|ev| ev.pubkey == *pk);
    }
    // Newest first; honor --limit after sorting.
    events.sort_by_key(|ev| std::cmp::Reverse(ev.created_at.as_secs()));
    if let Some(limit) = args.limit {
        events.truncate(limit);
    }
    let rows: Vec<Value> = events
        .iter()
        .map(|ev| {
            let react_to = ev
                .tags
                .iter()
                .find(|t| t.kind() == "e")
                .and_then(|t| t.content())
                .map(str::to_string);
            let mentions: Vec<String> = ev
                .tags
                .iter()
                .filter(|t| t.kind() == "p")
                .filter_map(|t| t.content())
                .map(str::to_string)
                .collect();
            serde_json::json!({
                "event_id": ev.id.to_hex(),
                "channel_id": args.id,
                "author": ev.pubkey.to_hex(),
                "kind": ev.kind.as_u16(),
                "content": ev.content,
                "react_to": react_to,
                "mentions": mentions,
                "created_at": ev.created_at.as_secs(),
            })
        })
        .collect();

    // bd-delivery-visibility-g96y: with --author, append the one-command
    // delivery check — count + newest post ts for that author.
    if let Some(pk) = &author_pk {
        let posts = events.iter().filter(|ev| ev.kind.as_u16() == 9).count();
        let newest = events.first().map(|ev| ev.created_at.as_secs());
        print_json(&serde_json::json!({
            "channel_id": args.id,
            "author": pk.to_hex(),
            "author_name": args.author,
            "delivered_posts": posts,
            "newest_post_at": newest,
            "events": rows,
        }))?;
        return Ok(());
    }
    print_json(&serde_json::Value::Array(rows))
}

async fn react(mailbox: &Mailbox, args: &ReactArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let store = rebuild_store(mailbox, &args.project).await?;

    // Resolve the channel from --channel or the target event's `h` tag.
    let channel = match &args.channel {
        Some(c) => c.clone(),
        None => {
            let target = EventId::from_hex(&args.to)
                .map_err(|_| anyhow::anyhow!("bad --to event id: {}", args.to))?;
            let filter = Filter::new().id(target).kinds([
                toron_kind(CHANNEL_MESSAGE),
                toron_kind(CHANNEL_MESSAGE_LEGACY),
            ]);
            let hits = collect_trusted(mailbox.bus().as_ref(), filter).await?;
            hits.into_iter()
                .find(|ev| ev.id == target && in_project(ev, &args.project))
                .and_then(|ev| event_channel(&ev))
                .with_context(|| format!("cannot resolve channel of event {}", args.to))?
        }
    };

    // bd-13lb: synchronous post visibility — `channel read` projects from the
    // bus (rumor-time G7), but `store.react` looked up only the in-memory
    // `ChannelStoreInner.events` which is empty after `rebuild_store` in a
    // fresh process. Hydrate the target post from the bus so `react` sees
    // what `read` sees without a sleep.
    {
        let target = EventId::from_hex(&args.to)
            .map_err(|_| anyhow::anyhow!("bad --to event id: {}", args.to))?;
        let already = store
            .history(&channel)
            .iter()
            .any(|e| e.event_id == args.to);
        if !already {
            let filter = Filter::new().id(target).kinds([
                toron_kind(CHANNEL_MESSAGE),
                toron_kind(CHANNEL_MESSAGE_LEGACY),
            ]);
            if let Ok(hits) = collect_trusted(mailbox.bus().as_ref(), filter).await
                && let Some(ev) = hits.into_iter().find(|ev| {
                    ev.id == target
                        && in_project(ev, &args.project)
                        && event_channel(ev).as_deref() == Some(channel.as_str())
                })
            {
                let _ = store.hydrate_message(&ev);
            }
        }
    }

    let ev = store
        .react(&keys, &channel, &args.to, &args.emoji)
        .with_context(|| format!("react in channel {channel}"))?;
    mailbox.bus().publish(ev.clone()).await?;
    print_json(&serde_json::json!({
        "event_id": ev.id.to_hex(),
        "kind": 7,
        "channel": channel,
        "react_to": args.to,
    }))
}

// ---------------------------------------------------------------------------
// plane J — jobs 43001-43006
// ---------------------------------------------------------------------------

/// Collect every job-lifecycle event for `project` (43001-43006). Un-scoped
/// events pass (DM-path jobs may lack `t`); scoped events must match the slug.
async fn collect_jobs(mailbox: &Mailbox, project: &str) -> anyhow::Result<Vec<Event>> {
    let filter = Filter::new().kinds([
        toron_kind(43001),
        toron_kind(43002),
        toron_kind(43003),
        toron_kind(43004),
        toron_kind(43005),
        toron_kind(43006),
    ]);
    let events = collect_trusted(mailbox.bus().as_ref(), filter).await?;
    Ok(events
        .into_iter()
        .filter(|ev| in_project(ev, project))
        .collect())
}

/// Find the 43001 request event for a job id.
async fn find_request(mailbox: &Mailbox, project: &str, job_id: &str) -> anyhow::Result<Event> {
    let job = EventId::from_hex(job_id)
        .map_err(|_| anyhow::anyhow!("bad job id {job_id} (expected a 43001 event id)"))?;
    let events = collect_jobs(mailbox, project).await?;
    events
        .into_iter()
        .find(|ev| ev.id == job && ev.kind.as_u16() == 43001)
        .with_context(|| format!("job {job_id} not found in project {project}"))
}

/// The `assignee` tag of a job request (canonical name).
fn request_assignee(ev: &Event) -> Option<String> {
    ev.tags
        .iter()
        .find(|t| t.as_slice().first().map(String::as_str) == Some("assignee"))
        .and_then(|t| t.as_slice().get(1).cloned())
}

fn state_name(state: &JobState) -> &'static str {
    match state {
        JobState::Open => "open",
        JobState::Claimed(_) => "claimed",
        JobState::InProgress(_) => "in_progress",
        JobState::Done(_) => "done",
        JobState::Cancelled => "cancelled",
        JobState::Failed(_) => "error",
    }
}

/// One job row for list/show.
fn job_row(job: &Event, events: &[Event]) -> Value {
    let state = job_state(job.id, events);
    let claimer = match &state {
        JobState::Claimed(c) | JobState::InProgress(c) => Some(c.clone()),
        _ => None,
    };
    let request: JobRequest = serde_json::from_str(&job.content).unwrap_or_else(|_| JobRequest {
        title: job.content.clone(),
        spec: serde_json::Value::Null,
        assignee: None,
    });
    serde_json::json!({
        "job_id": job.id.to_hex(),
        "title": request.title,
        "assignee": request_assignee(job).or(request.assignee),
        "state": state_name(&state),
        "requester": job.pubkey.to_hex(),
        "claimer": claimer,
        "created_at": job.created_at.as_secs(),
        "spec": request.spec,
    })
}

async fn job_list(mailbox: &Mailbox, args: &JobListArgs) -> anyhow::Result<()> {
    if let Some(name) = &args.as_name {
        // Reads may carry an identity; unknown is still a usage error.
        let _ = mailbox.whois(&args.project, name).await.map_err(|_| {
            UsageError(format!(
                "unknown agent name: {name} for project {}",
                args.project
            ))
        })?;
    }
    let events = collect_jobs(mailbox, &args.project).await?;
    let requests: Vec<&Event> = events
        .iter()
        .filter(|ev| ev.kind.as_u16() == 43001)
        .collect();
    let mut rows: Vec<Value> = Vec::new();
    for job in requests {
        let state = job_state(job.id, &events);
        // --state filter.
        let state_pass = match (args.state, &state) {
            (None, _) => true,
            (Some(JobStateArg::Open), JobState::Open) => true,
            (Some(JobStateArg::Claimed), JobState::Claimed(_) | JobState::InProgress(_)) => true,
            (Some(JobStateArg::Done), JobState::Done(_)) => true,
            (Some(JobStateArg::Error), JobState::Failed(_)) => true,
            (Some(_), _) => false,
        };
        if !state_pass {
            continue;
        }
        // --assignee filter (tag or payload).
        if let Some(want) = &args.assignee {
            let has = request_assignee(job)
                .or_else(|| {
                    serde_json::from_str::<JobRequest>(&job.content)
                        .ok()
                        .and_then(|r| r.assignee)
                })
                .map(|a| a == *want)
                .unwrap_or(false);
            if !has {
                continue;
            }
        }
        rows.push(job_row(job, &events));
    }
    // Newest first.
    rows.sort_by(|a, b| {
        let ka = a["created_at"].as_u64().unwrap_or(0);
        let kb = b["created_at"].as_u64().unwrap_or(0);
        kb.cmp(&ka)
    });
    print_json(&serde_json::Value::Array(rows))
}

async fn job_show(mailbox: &Mailbox, args: &JobShowArgs) -> anyhow::Result<()> {
    let job = find_request(mailbox, &args.project, &args.job_id).await?;
    let events = collect_jobs(mailbox, &args.project).await?;
    print_json(&job_row(&job, &events))
}

/// Claim a job: publish a 43002 signed by `--as`, then resolve the winner
/// from the stored 43002 stream. First claimer wins; a second claimer gets
/// `won: false` and exits 1 (never a silent success).
async fn job_claim(mailbox: &Mailbox, args: &JobClaimArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    // Verify the job exists before claiming.
    let _ = find_request(mailbox, &args.project, &args.job_id).await?;
    let job = EventId::from_hex(&args.job_id).context("job id parses")?;

    let claim = build_job_claim(&keys, job, Timestamp::now())?;
    mailbox.bus().publish(claim.clone()).await?;

    // Cross-process convergence: another process may be claiming the same
    // job at nearly the same moment (SC-28). A settle window lets the other
    // claim land in the shared store so BOTH processes compute the winner
    // over the SAME claim set — exactly one `won: true`, the other exit 1.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Winner = first observed 43002 (store order; deterministic tie-break).
    let events = collect_jobs(mailbox, &args.project).await?;
    let claims: Vec<Event> = events
        .iter()
        .filter(|ev| ev.kind.as_u16() == 43002 && job_parent(ev) == Some(job))
        .cloned()
        .collect();
    let winner = first_claimer(job, &claims);
    let won = winner.as_deref() == Some(keys.public_key().to_hex().as_str());

    print_json(&serde_json::json!({
        "job_id": args.job_id,
        "kind": 43002,
        "claimer": args.as_name,
        "npub": keys.public_key().to_hex(),
        "won": won,
    }))?;
    if won {
        Ok(())
    } else {
        anyhow::bail!(
            "job claim lost: {} already claimed {}",
            winner.unwrap_or_else(|| "another agent".to_string()),
            args.job_id
        )
    }
}

/// Requester-or-operator check for cancel (ADR-0020 Spec 2).
fn can_cancel(mailbox: &Mailbox, request: &Event, canceller_npub: &str) -> bool {
    if request.pubkey.to_hex() == canceller_npub {
        return true;
    }
    mailbox
        .operator()
        .map(|op| op.public_key().to_hex() == canceller_npub)
        .unwrap_or(false)
}

async fn job_progress(mailbox: &Mailbox, args: &JobProgressArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let _ = find_request(mailbox, &args.project, &args.job_id).await?;
    let job = EventId::from_hex(&args.job_id).context("job id parses")?;
    let ev = build_job_progress(&keys, job, &args.body, Timestamp::now())?;
    mailbox.bus().publish(ev.clone()).await?;
    print_json(&serde_json::json!({
        "job_id": args.job_id,
        "kind": 43003,
        "event_id": ev.id.to_hex(),
    }))
}

async fn job_result(mailbox: &Mailbox, args: &JobResultArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let _ = find_request(mailbox, &args.project, &args.job_id).await?;
    let job = EventId::from_hex(&args.job_id).context("job id parses")?;
    let result = JobResult {
        status: match args.status {
            JobStatusArg::Ok => "ok".to_string(),
            JobStatusArg::Failed => "failed".to_string(),
        },
        body: args.body.clone(),
    };
    let ev = build_job_result(&keys, job, &result, Timestamp::now())?;
    mailbox.bus().publish(ev.clone()).await?;
    print_json(&serde_json::json!({
        "job_id": args.job_id,
        "kind": 43004,
        "status": result.status,
        "event_id": ev.id.to_hex(),
    }))
}

async fn job_error(mailbox: &Mailbox, args: &JobErrorArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let _ = find_request(mailbox, &args.project, &args.job_id).await?;
    let job = EventId::from_hex(&args.job_id).context("job id parses")?;
    let err = JobError {
        message: args.message.clone(),
    };
    let ev = build_job_error(&keys, job, &err, Timestamp::now())?;
    mailbox.bus().publish(ev.clone()).await?;
    print_json(&serde_json::json!({
        "job_id": args.job_id,
        "kind": 43006,
        "event_id": ev.id.to_hex(),
    }))
}

async fn job_cancel(mailbox: &Mailbox, args: &JobCancelArgs) -> anyhow::Result<()> {
    let keys = resolve_agent(mailbox, &args.project, &args.as_name).await?;
    let request = find_request(mailbox, &args.project, &args.job_id).await?;
    if !can_cancel(mailbox, &request, &keys.public_key().to_hex()) {
        anyhow::bail!(
            "only the job requester or the operator may cancel {} (canceller {})",
            args.job_id,
            args.as_name
        );
    }
    let job = EventId::from_hex(&args.job_id).context("job id parses")?;
    let ev = build_job_cancel(&keys, job, "cancelled by requester", Timestamp::now())?;
    mailbox.bus().publish(ev.clone()).await?;
    print_json(&serde_json::json!({
        "job_id": args.job_id,
        "kind": 43005,
        "event_id": ev.id.to_hex(),
    }))
}

// ---------------------------------------------------------------------------
// plane N — presence 20001
// ---------------------------------------------------------------------------

fn presence_state(arg: PresenceStateArg) -> PresenceState {
    match arg {
        PresenceStateArg::Idle => PresenceState::Idle,
        PresenceStateArg::Working => PresenceState::Working,
        PresenceStateArg::Blocked => PresenceState::Blocked,
        PresenceStateArg::Done => PresenceState::Done,
    }
}

fn state_str(state: &PresenceState) -> &'static str {
    match state {
        PresenceState::Idle => "idle",
        PresenceState::Working => "working",
        PresenceState::Blocked => "blocked",
        PresenceState::Done => "done",
        PresenceState::Unknown => "unknown",
    }
}

/// CLI-side presence projection path (`<root>/presence.jsonl`). The bus
/// never stores 20001 (ephemeral contract); this last-wins file lets a
/// separate process read another process's heartbeat within the TTL.
/// bd-m5ym: the Mailbox data_dir is `<root>/projects` in production, so the
/// root is lifted via `mailbox_root` — never `projects/presence.jsonl`.
fn presence_file(mailbox: &Mailbox) -> PathBuf {
    toron_core::config::mailbox_root(mailbox.data_dir()).join("presence.jsonl")
}

/// Load the presence projection: last heartbeat per npub, newest ts wins.
fn read_presence(mailbox: &Mailbox, project: &str, now_secs: u64) -> Vec<Value> {
    let ttl = std::env::var("TORON_PRESENCE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(PRESENCE_TTL_SECS);
    let Ok(body) = std::fs::read_to_string(presence_file(mailbox)) else {
        return Vec::new();
    };
    let mut by_npub: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for line in body.lines() {
        let Ok(v) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if v.get("project").and_then(|p| p.as_str()) != Some(project) {
            continue;
        }
        let npub = v.get("npub").and_then(|n| n.as_str()).unwrap_or_default();
        let ts = v.get("ts").and_then(|t| t.as_u64()).unwrap_or(0);
        // Stale heartbeats drop out (broker-expiry parity).
        if now_secs.saturating_sub(ts) > ttl {
            continue;
        }
        let is_newer = by_npub
            .get(npub)
            .map(|old| old.get("ts").and_then(|t| t.as_u64()).unwrap_or(0) <= ts)
            .unwrap_or(true);
        if is_newer {
            by_npub.insert(npub.to_string(), v);
        }
    }
    let mut out: Vec<Value> = by_npub.into_values().collect();
    out.sort_by(|a, b| {
        let ta = a["ts"].as_u64().unwrap_or(0);
        let tb = b["ts"].as_u64().unwrap_or(0);
        tb.cmp(&ta)
    });
    out
}

/// bd-1wwc: presence flag/env resolution (functional core). Flags win;
/// blank values read as absent. `env` is a provider so tests inject a map
/// while production reads the process environment (laziness-protocol: never
/// demand what the pane already knows).
fn resolve_presence_project(
    flag: Option<&str>,
    env: &dyn Fn(&str) -> Option<String>,
) -> Option<String> {
    nonblank(flag.map(str::to_string)).or_else(|| nonblank(env("TORON_PROJECT")))
}

/// bd-1wwc: --as ladder — explicit flag, then TORON_AGENT, FLYWHEEL_MAIL_AS,
/// AGENT_NAME (the guard_agent order in robot.rs, reused not reinvented).
fn resolve_presence_as(flag: Option<&str>, env: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    nonblank(flag.map(str::to_string))
        .or_else(|| nonblank(env("TORON_AGENT")))
        .or_else(|| nonblank(env("FLYWHEEL_MAIL_AS")))
        .or_else(|| nonblank(env("AGENT_NAME")))
}

fn nonblank(s: Option<String>) -> Option<String> {
    s.filter(|v| !v.trim().is_empty())
}

/// Production env provider for the resolvers above.
fn live_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

async fn presence_publish(mailbox: &Mailbox, args: &PresencePublishArgs) -> anyhow::Result<()> {
    let project =
        resolve_presence_project(args.project.as_deref(), &live_env).ok_or_else(|| {
            anyhow::anyhow!(
                "pass --project or export TORON_PROJECT (no project in flags or pane env)"
            )
        })?;
    let as_name = resolve_presence_as(args.as_name.as_deref(), &live_env).ok_or_else(|| {
        anyhow::anyhow!("pass --as or export TORON_AGENT / FLYWHEEL_MAIL_AS / AGENT_NAME (no pin in flags or pane env)")
    })?;
    let keys = resolve_agent(mailbox, &project, &as_name).await?;
    let now = Timestamp::now();
    let payload = PresencePayload {
        state: presence_state(args.state),
        status: args.status.clone(),
        ts: now.as_secs(),
    };
    let ev = build_presence_event(&keys, &payload, now)?;
    mailbox.bus().publish(ev).await?;

    // Append to the CLI projection (last-wins by npub on read).
    let whois = mailbox
        .whois(&project, &as_name)
        .await
        .context("whois for presence projection")?;
    let line = serde_json::json!({
        "name": whois.name,
        "npub": keys.public_key().to_hex(),
        "state": state_str(&payload.state),
        "status": payload.status,
        "ts": payload.ts,
        "project": project,
    });
    let mut s = line.to_string();
    s.push('\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(presence_file(mailbox))
        .context("open presence projection")?;
    use std::io::Write;
    file.write_all(s.as_bytes())
        .context("write presence projection")?;
    print_json(&serde_json::json!({
        "npub": keys.public_key().to_hex(),
        "name": whois.name,
        "state": state_str(&payload.state),
        "status": payload.status,
        "ts": payload.ts,
    }))
}

async fn presence_list(mailbox: &Mailbox, args: &PresenceListArgs) -> anyhow::Result<()> {
    let project =
        resolve_presence_project(args.project.as_deref(), &live_env).ok_or_else(|| {
            anyhow::anyhow!(
                "pass --project or export TORON_PROJECT (no project in flags or pane env)"
            )
        })?;
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rows = read_presence(mailbox, &project, now_secs);
    print_json(&serde_json::Value::Array(rows))
}

// ---------------------------------------------------------------------------
// plane W — watch (NIP-01 REQ)
// ---------------------------------------------------------------------------

fn parse_kinds(s: &str) -> anyhow::Result<Vec<u16>> {
    s.split(',')
        .map(|k| {
            k.trim()
                .parse::<u16>()
                .map_err(|e| anyhow::anyhow!("bad kind {k:?}: {e}"))
        })
        .collect()
}

/// Print one event in the binding watch shape `{kind, id, pubkey, content,
/// tags, created_at}` as NDJSON. `created_at` is gap 1 fix: the dispatcher
/// needs a persisted plane-S cursor to avoid replay storms.
fn print_watch_event(ev: &Event) {
    let tags: Vec<Vec<String>> = ev.tags.iter().map(|t| t.clone().to_vec()).collect();
    let v = serde_json::json!({
        "kind": ev.kind.as_u16(),
        "id": ev.id.to_hex(),
        "pubkey": ev.pubkey.to_hex(),
        "content": ev.content,
        "tags": tags,
        "created_at": ev.created_at.as_secs(),
    });
    println!(
        "{}",
        serde_json::to_string(&v).expect("watch event serializes")
    );
}

async fn watch(mailbox: &Mailbox, args: &WatchArgs) -> anyhow::Result<()> {
    if args.live {
        return watch_live(mailbox, args).await;
    }
    // 1059 gift-wrap is p-gated to the watcher's own inbox (ADR-0020 Spec 6).
    let kinds = parse_kinds(&args.kinds)?;
    let self_npub = if kinds.contains(&1059) {
        let name = args.as_name.as_deref().ok_or_else(|| {
            UsageError("--as <name> is required to watch kind 1059 (p-gated to self)".to_string())
        })?;
        let whois = mailbox.whois(&args.project, name).await.map_err(|_| {
            UsageError(format!(
                "unknown agent name: {name} for project {}",
                args.project
            ))
        })?;
        Some(whois.npub)
    } else {
        None
    };

    let timeout_ms = args.timeout_ms;
    if args.once && timeout_ms.is_none() {
        return Err(anyhow::Error::from(UsageError(
            "--once requires --timeout-ms (no infinite hang in CI)".to_string(),
        )));
    }

    let mut filter = Filter::new().kinds(kinds.iter().map(|k| toron_kind(*k)));
    if let Some(channel) = &args.channel {
        filter = filter.custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_H, channel);
    }
    if let Some(np) = &self_npub {
        filter = filter.custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_P, np);
    }

    eprintln!("watching kinds={}", args.kinds);
    let deadline = timeout_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
    let mut seen: HashSet<String> = HashSet::new();
    loop {
        // Re-snapshot sqlite each tick: flywheel doorbell runs `toron watch`
        // in a *separate process* from the publisher, so an in-process
        // subscribe would miss every live post. The 100ms cadence was the
        // RSS bomb (24GB / 5h per child, bd-741i). 1s + snapshot memo is
        // enough for doorbell and cheap when the store is unchanged.
        let events = collect_trusted(mailbox.bus().as_ref(), filter.clone()).await?;
        for ev in events {
            if !seen.insert(ev.id.to_hex()) {
                continue;
            }
            if !in_project(&ev, &args.project) {
                continue;
            }
            if let Some(ch) = &args.channel
                && event_channel(&ev).as_deref() != Some(ch.as_str())
            {
                continue;
            }
            print_watch_event(&ev);
            if args.once {
                return Ok(());
            }
        }
        if let Some(d) = deadline
            && Instant::now() >= d
        {
            if args.once {
                eprintln!("watch: timeout");
                std::process::exit(1);
            }
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// bd-rkom: wait --corr subscribe-until-receipt (E9) — shares the live
/// subscription with `watch --live` (one bus source). Unparks when the
/// receipt appears in the durable `receipts` store; no thread.sleep poll loop.
///
/// ADR-0028 E2 (zfnx.6): WaitCorr interest vocabulary. The wait subscribes
/// to the already-held doorbell kinds (`DOORBELL_WATCH_KINDS` mirror:
/// 9/14/20001/30013/43001/30031/39002) plus 1059 (gift-wrapped receipt
/// acks) and 43004 (job results) for the direct receipt paths. Any bus
/// event only re-checks the store fast path — the store is the truth, the
/// bus is the wake. No second watch child, no wake kind, no poll.
pub const WAIT_WAKE_KINDS: &[u16] = &[9, 14, 1059, 20001, 30013, 43001, 43004, 30031, 39002];

async fn wait_for_corr(mailbox: &Mailbox, args: &WaitArgs) -> anyhow::Result<()> {
    let timeout = args.timeout.unwrap_or(30);
    let deadline = if timeout == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(timeout))
    };
    let canonical = mailbox.canonical_project(&args.project).await;
    let store = mailbox.receipt_store().map_err(|e| {
        anyhow::anyhow!("wait --corr for project {canonical} requires receipts store: {e}")
    })?;
    // Fast path: already present
    if let Some(r) = store.get(&args.corr).await? {
        let v = serde_json::json!({
            "corr": r.corr,
            "state": r.state.as_str(),
            "wake": r.wake.map(|w| w.as_str()),
            "due_at": r.due_at,
            "result_path": r.result_path,
            "updated_at": r.updated_at,
        });
        println!("{}", serde_json::to_string(&v)?);
        return Ok(());
    }
    // Subscribe to live bus (any kind that could lead to a receipt). The
    // receipts store itself is not a bus kind, so we wake on any new bus
    // event and re-check the store — still subscribe-driven, not poll.
    // Kinds are WAIT_WAKE_KINDS (E2 held-subscription vocabulary).
    let filter = Filter::new().kinds(WAIT_WAKE_KINDS.iter().map(|k| toron_kind(*k)));
    let mut sub = mailbox.bus().subscribe_trusted(filter).await?;
    loop {
        if let Some(d) = deadline
            && Instant::now() >= d
        {
            anyhow::bail!(
                "wait --corr {} for project {canonical} timed out after {}s",
                args.corr,
                timeout
            );
        }
        // Check store after each wake (including immediately)
        if let Some(r) = store.get(&args.corr).await? {
            let v = serde_json::json!({
                "corr": r.corr,
                "state": r.state.as_str(),
                "wake": r.wake.map(|w| w.as_str()),
                "due_at": r.due_at,
                "result_path": r.result_path,
                "updated_at": r.updated_at,
            });
            println!("{}", serde_json::to_string(&v)?);
            return Ok(());
        }
        // Unpark on bus event or deadline only — no 200ms costume poll.
        if let Some(d) = deadline {
            let remain = d.saturating_duration_since(Instant::now());
            tokio::select! {
                _ = tokio::time::sleep(remain) => {
                    anyhow::bail!(
                        "wait --corr {} for project {canonical} timed out after {}s",
                        args.corr,
                        timeout
                    );
                }
                ev = sub.next() => {
                    if ev.is_none() {
                        continue;
                    }
                }
            }
        } else {
            let ev = sub.next().await;
            if ev.is_none() {
                continue;
            }
        }
    }
}

/// bd-rkom: live subscription (E7) — the same source the flywheel dispatcher
/// uses (`bus.subscribe`, not the 1s snapshot). Every line yields WatchNdjson
/// for the consumer; snapshot stays as `--once` debug only. No second doorbell.
async fn watch_live(mailbox: &Mailbox, args: &WatchArgs) -> anyhow::Result<()> {
    let kinds = parse_kinds(&args.kinds)?;
    let self_npub = if kinds.contains(&1059) {
        let name = args.as_name.as_deref().ok_or_else(|| {
            UsageError("--as <name> is required to watch kind 1059 (p-gated to self)".to_string())
        })?;
        let whois = mailbox.whois(&args.project, name).await.map_err(|_| {
            UsageError(format!(
                "unknown agent name: {name} for project {}",
                args.project
            ))
        })?;
        Some(whois.npub)
    } else {
        None
    };
    let mut filter = Filter::new().kinds(kinds.iter().map(|k| toron_kind(*k)));
    if let Some(channel) = &args.channel {
        filter = filter.custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_H, channel);
    }
    if let Some(np) = &self_npub {
        filter = filter.custom_tag(nostr::filter::SingleLetterTag::LOWERCASE_P, np);
    }
    eprintln!("watch --live kinds={}", args.kinds);
    // Collect replay first (so late joiners see history), then stream live.
    let replay = collect_trusted(mailbox.bus().as_ref(), filter.clone()).await?;
    let mut seen: HashSet<String> = HashSet::new();
    for ev in replay {
        if !seen.insert(ev.id.to_hex()) {
            continue;
        }
        if !in_project(&ev, &args.project) {
            continue;
        }
        if let Some(ch) = &args.channel
            && event_channel(&ev).as_deref() != Some(ch.as_str())
        {
            continue;
        }
        print_watch_event(&ev);
    }
    // Live tail — shares one subscription with `wait --corr` (same bus source).
    let mut sub = mailbox.bus().subscribe_trusted(filter).await?;
    while let Some(ev) = sub.next().await {
        if let SubscriptionEvent::Event(event) = ev {
            if !seen.insert(event.id.to_hex()) {
                continue;
            }
            if !in_project(&event, &args.project) {
                continue;
            }
            if let Some(ch) = &args.channel
                && event_channel(&event).as_deref() != Some(ch.as_str())
            {
                continue;
            }
            print_watch_event(&event);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

/// Run a two-plane command against the shared mailbox (built once per
/// invocation, the same in-process bus the MCP server uses).
pub async fn run_plane(mailbox: &Mailbox, command: &crate::cli::Command) -> anyhow::Result<()> {
    match command {
        crate::cli::Command::Channel(cmd) => match cmd {
            ChannelCommand::Create(args) => channel_create(mailbox, args).await,
            ChannelCommand::Join(args) => channel_join(mailbox, args).await,
            ChannelCommand::Leave(args) => channel_leave(mailbox, args).await,
            ChannelCommand::List(args) => channel_list(mailbox, args).await,
            ChannelCommand::Members(args) => channel_members(mailbox, args).await,
            ChannelCommand::Post(args) => channel_post(mailbox, args).await,
            ChannelCommand::Read(args) => channel_read(mailbox, args).await,
            ChannelCommand::Close(args) => channel_close(mailbox, args).await,
            ChannelCommand::Purge(args) => channel_purge(mailbox, args).await,
        },
        crate::cli::Command::React(args) => react(mailbox, args).await,
        crate::cli::Command::Job(cmd) => match cmd {
            JobCommand::List(args) => job_list(mailbox, args).await,
            JobCommand::Show(args) => job_show(mailbox, args).await,
            JobCommand::Claim(args) => job_claim(mailbox, args).await,
            JobCommand::Progress(args) => job_progress(mailbox, args).await,
            JobCommand::Result(args) => job_result(mailbox, args).await,
            JobCommand::Error(args) => job_error(mailbox, args).await,
            JobCommand::Cancel(args) => job_cancel(mailbox, args).await,
        },
        crate::cli::Command::Presence(cmd) => match cmd {
            PresenceCommand::Publish(args) => presence_publish(mailbox, args).await,
            PresenceCommand::List(args) => presence_list(mailbox, args).await,
        },
        crate::cli::Command::Watch(args) => watch(mailbox, args).await,
        crate::cli::Command::Wait(args) => wait_for_corr(mailbox, args).await,
        _ => anyhow::bail!("not a two-plane command"),
    }
}

/// Re-export for `main.rs`: build the demo mailbox once and dispatch.
/// `#[tokio::main]` mirrors `run_robot` — called from sync `run()`.
#[tokio::main]
pub async fn run_plane_command(command: &crate::cli::Command) -> anyhow::Result<()> {
    let mailbox = toron_mcp::demo_mailbox().await?;
    run_plane(&mailbox, command).await
}

#[cfg(test)]
mod live_watch_tests {
    use super::*;
    use crate::cli::WaitArgs;
    use std::sync::Arc;
    use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
    use toron_core::index::schema;
    use toron_core::mailbox::send_tags::Wake;
    use toron_core::mailbox::{Mailbox, Receipt, ReceiptState};

    async fn test_mailbox(
        tag: &str,
    ) -> (
        Mailbox,
        sqlx::SqlitePool,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-live-watch-{}-{}.sqlite",
            tag,
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-live-watch-data-{}-{}",
            tag,
            uuid::Uuid::new_v4()
        ));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(toron_core::keys::generate()),
        )
        .with_index_pool(pool.clone());
        (mailbox, pool, db_path, data_dir)
    }

    #[tokio::test]
    async fn live_watch_yields_kind9_lines() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("yield").await;
        // Publish a channel post (kind 9) directly via bus
        let keys = toron_core::keys::generate();
        let ev = toron_core::nostr::event::build_event(
            &keys,
            toron_core::nostr::kinds::toron_kind(9),
            "hello live",
            vec![
                toron_core::nostr::Tag::parse(["h", "test-chan"]).unwrap(),
                toron_core::nostr::Tag::parse(["t", "demo"]).unwrap(),
            ],
            toron_core::nostr::Timestamp::now(),
        )
        .expect("event");
        mailbox.bus().publish(ev.clone()).await.expect("publish");
        // Live subscription should see it (same source as watch --live)
        let filter =
            toron_core::bus::Filter::new().kinds([toron_core::nostr::kinds::toron_kind(9)]);
        let mut sub = mailbox.bus().subscribe(filter).await.expect("sub");
        // Also check via collect_trusted (replay)
        let replay = collect_trusted(
            mailbox.bus().as_ref(),
            toron_core::bus::Filter::new().kinds([toron_core::nostr::kinds::toron_kind(9)]),
        )
        .await
        .expect("replay");
        assert!(
            replay.iter().any(|e| e.id == ev.id),
            "replay must contain published kind-9"
        );
        // Live: publish another and see it via subscription
        let ev2 = toron_core::nostr::event::build_event(
            &keys,
            toron_core::nostr::kinds::toron_kind(9),
            "second",
            vec![
                toron_core::nostr::Tag::parse(["h", "test-chan2"]).unwrap(),
                toron_core::nostr::Tag::parse(["t", "demo"]).unwrap(),
            ],
            toron_core::nostr::Timestamp::now(),
        )
        .expect("event2");
        mailbox.bus().publish(ev2.clone()).await.expect("publish2");
        // Deterministic: await delivery with a deadline instead of a fixed
        // sleep (a 100ms sleep raced bus delivery under load and flaked).
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
            .await
            .expect("live delivery within 5s");
        assert!(
            got.is_some(),
            "live subscription must yield the kind-9 event"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn live_watch_wait_corr_unparks_on_receipt() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("wait-unpark").await;
        let store = mailbox.receipt_store().expect("store");
        let corr = format!("test-corr-{}", uuid::Uuid::new_v4());
        // Spawn waiter in background
        let mb_clone = mailbox.clone();
        let corr_clone = corr.clone();
        let handle = tokio::spawn(async move {
            let args = WaitArgs {
                corr: corr_clone,
                timeout: Some(5),
                project: "demo".into(),
            };
            wait_for_corr(&mb_clone, &args).await
        });
        // Give waiter time to subscribe
        // No fixed subscribe-delay sleep: a single wake published before the
        // waiter's subscription is live is LOST, and the waiter deadlines —
        // the flake. Publish a wake stream inside the waiter's 5s deadline
        // instead; whichever wake lands after subscribe unparks it.
        // Insert receipt — should unpark waiter
        let receipt = Receipt {
            corr: corr.clone(),
            state: ReceiptState::Owed,
            wake: Some(Wake::Tasked),
            due_at: None,
            result_path: None,
            updated_at: 1_700_000_000_000_000,
        };
        store.upsert(&receipt).await.expect("upsert");
        // Also publish a bus event to wake the waiter (receipts store alone doesn't wake, but bus event does)
        let keys = toron_core::keys::generate();
        let ev = toron_core::nostr::event::build_event(
            &keys,
            toron_core::nostr::kinds::toron_kind(9),
            "wake",
            vec![toron_core::nostr::Tag::parse(["t", "demo"]).unwrap()],
            toron_core::nostr::Timestamp::now(),
        )
        .expect("ev");
        let mb_wake = mailbox.clone();
        let wake_task = tokio::spawn(async move {
            for _ in 0..30 {
                mb_wake
                    .bus()
                    .publish(ev.clone())
                    .await
                    .expect("publish wake");
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(4), handle)
            .await
            .expect("timeout")
            .expect("join")
            .expect("wait ok");
        wake_task.abort();
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn live_watch_wait_corr_timeout() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("wait-timeout").await;
        let args = WaitArgs {
            corr: "non-existent-corr-xyz".into(),
            timeout: Some(1),
            project: "demo".into(),
        };
        let res = wait_for_corr(&mailbox, &args).await;
        assert!(res.is_err(), "wait should timeout on missing corr");
        let msg = format!("{res:?}");
        assert!(
            msg.contains("timed out") || msg.contains("timeout"),
            "timeout message: {msg}"
        );
        assert!(
            msg.contains("demo"),
            "error must name resolved project: {msg}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// ADR-0028 E2: WaitCorr interest rides the already-held doorbell
    /// subscription vocabulary (DOORBELL_WATCH_KINDS mirror + 1059/43004 for
    /// the direct receipt paths). No second watch child, no wake kind.
    #[test]
    fn live_watch_wait_wake_kinds_cover_doorbell() {
        for kind in [9u16, 14, 20001, 30013, 43001, 30031, 39002] {
            assert!(
                WAIT_WAKE_KINDS.contains(&kind),
                "wait wake kinds must cover doorbell kind {kind}"
            );
        }
    }

    /// ADR-0028 E2/S5: tasked corr survives walker death via sqlite — a
    /// receipt that landed before the wait starts resolves on the store
    /// fast path with no bus event at all (no live walker required).
    #[tokio::test]
    async fn live_watch_wait_corr_survives_walker_death() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("wait-walkerdeath").await;
        let store = mailbox.receipt_store().expect("store");
        let corr = format!("test-corr-dead-{}", uuid::Uuid::new_v4());
        store
            .upsert(&Receipt {
                corr: corr.clone(),
                state: ReceiptState::Owed,
                wake: Some(Wake::Tasked),
                due_at: None,
                result_path: None,
                updated_at: 1_700_000_000_000_000,
            })
            .await
            .expect("upsert");
        // Walker is dead: no bus event is ever published. The wait must
        // still resolve from the durable store, fast (well under timeout).
        let args = WaitArgs {
            corr,
            timeout: Some(5),
            project: "demo".into(),
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            wait_for_corr(&mailbox, &args),
        )
        .await
        .expect("no hang without a live walker")
        .expect("wait ok from sqlite fast path");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

/// bd-1wwc: presence --project/--as default from pane env (RED first).
#[cfg(test)]
mod presence_env_tests {
    use super::{resolve_presence_as, resolve_presence_project};
    use std::collections::HashMap;

    fn env_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn lookup(map: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn project_flag_wins_over_env() {
        assert_eq!(
            resolve_presence_project(
                Some("flag-proj"),
                &lookup(&env_map(&[("TORON_PROJECT", "env-proj")]))
            ),
            Some("flag-proj".to_string())
        );
    }

    #[test]
    fn project_falls_back_to_env() {
        assert_eq!(
            resolve_presence_project(None, &lookup(&env_map(&[("TORON_PROJECT", "env-proj")]))),
            Some("env-proj".to_string())
        );
    }

    #[test]
    fn project_empty_env_is_absent() {
        assert_eq!(
            resolve_presence_project(None, &lookup(&env_map(&[("TORON_PROJECT", "  ")]))),
            None
        );
    }

    #[test]
    fn project_nothing_is_none() {
        assert_eq!(resolve_presence_project(None, &lookup(&env_map(&[]))), None);
    }

    #[test]
    fn as_flag_wins_over_env() {
        assert_eq!(
            resolve_presence_as(
                Some("FlagPin"),
                &lookup(&env_map(&[("TORON_AGENT", "EnvPin")]))
            ),
            Some("FlagPin".to_string())
        );
    }

    #[test]
    fn as_ladder_toron_agent_first() {
        assert_eq!(
            resolve_presence_as(
                None,
                &lookup(&env_map(&[
                    ("TORON_AGENT", "FirstPin"),
                    ("FLYWHEEL_MAIL_AS", "SecondPin"),
                    ("AGENT_NAME", "ThirdPin"),
                ]))
            ),
            Some("FirstPin".to_string())
        );
    }

    #[test]
    fn as_ladder_flywheel_second() {
        assert_eq!(
            resolve_presence_as(
                None,
                &lookup(&env_map(&[
                    ("FLYWHEEL_MAIL_AS", "SecondPin"),
                    ("AGENT_NAME", "ThirdPin")
                ]))
            ),
            Some("SecondPin".to_string())
        );
    }

    #[test]
    fn as_ladder_agent_name_third() {
        assert_eq!(
            resolve_presence_as(None, &lookup(&env_map(&[("AGENT_NAME", "ThirdPin")]))),
            Some("ThirdPin".to_string())
        );
    }

    #[test]
    fn as_nothing_is_none() {
        assert_eq!(resolve_presence_as(None, &lookup(&env_map(&[]))), None);
    }
}
