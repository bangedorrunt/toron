// governed-by: ADR-0003, ADR-0037
// governed-by: ADR-0004 (SendMessage.overseer, operator signing — Ship 5)
// governed-by: ADR-0017 (project-root canonicalization)
#![forbid(unsafe_code)]

//! Messaging cluster (bd-led.5.3): send_message, reply_message, fetch_inbox,
//! acknowledge_message, mark_message_read — G3/G7/G8 + NIP-17.
//!
//! Wire mapping (ADR-0003 G3):
//! - **to+cc**: one visible rumor with `["p", npub, relay, "to"|"cc"]` tags;
//!   gift-wrapped one copy per to/cc recipient.
//! - **bcc**: a **variant rumor** per bcc recipient listing `to+cc+that bcc`
//!   (omitting other bccs) — true BCC, cryptographically enforced. Bcc
//!   recipients cannot see each other.
//! - `subject` → NIP-14 `subject`; `importance`, `ack_required`,
//!   `sender_token` → tags on the rumor; thread reply → NIP-10
//!   `["e", root, relay, "reply"]`.
//! - Unknown recipient **aborts the whole send** (no partial delivery).
//!
//! Acks/reads (G8): private NIP-17 gift-wrapped kind-14 to the original sender
//! only, tagged `toron-kind`=`ack`|`read` + a NIP-10 `e` reference. Idempotent
//! (second call returns the stored timestamp). NEVER public replaceable events.
//!
//! Read state (bd-5icg): `mark_message_read`/`acknowledge_message` ALSO write a
//! per-recipient `read_ts` into the `message_reads` SQLite projection (never
//! into Nostr events), and `fetch_inbox(unread_only: true)` filters on it.
//! `fetch_inbox` itself stays READ-ONLY — only the explicit receipt tools mark
//! read (keeps the flywheel digest honest; matches the tool's `read_only_hint`).
//! Read receipts are lost on an index rebuild (projection-only, documented).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::mailbox::ids::Corr;
use crate::nostr::UnsignedEvent;
use crate::nostr::event::build_event;
use crate::nostr::kinds::toron_kind;
use crate::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};
use sqlx::Row;

use crate::bus::filter::ToronFilter;
use crate::bus::scoping::is_metadata_hiding_kind;
use crate::mailbox::identity::ListAgentsOptions;
use crate::mailbox::unwrap::{
    DualProjectionSink, OVERSEER_TAG, ProjectGitProjectionSink, ProjectionSink,
    SqliteProjectionSink, UnwrappedMail, order_by_rumor_time, tag_value,
};
use crate::nostr::gift_wrap::{self, UnwrappedGift};
use crate::nostr::identity as nident;
use sha2::Digest;

use super::Mailbox;
use super::error::{MailError, MailErrorCode};

/// A recipient role on the wire (`p` tag role, G3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipientRole {
    To,
    Cc,
    Bcc,
}

impl RecipientRole {
    const fn tag_value(self) -> &'static str {
        match self {
            Self::To => "to",
            Self::Cc => "cc",
            Self::Bcc => "bcc",
        }
    }
}

/// One resolved recipient (name → npub, G2) ready for wrapping.
#[derive(Debug, Clone)]
struct ResolvedRecipient {
    name: String,
    npub: PublicKey,
    role: RecipientRole,
}

/// Send options mirroring the AM-Rust `send_message` tool surface.
#[derive(Debug, Clone, Default)]
pub struct SendMessage {
    pub project_slug: String,
    pub sender_name: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub importance: Option<String>,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub sender_token: Option<String>,
    /// ADR-0004 Ship 5: operator Overseer override. When true, the message is
    /// signed with the OPERATOR key (not an agent key), gains the
    /// `['t', 'toron:overseer']` tag, and SKIPS contact-policy enforcement at
    /// the projection (`block_all` cannot block the operator). Requires the
    /// mailbox to hold an operator key.
    pub overseer: bool,
    /// ADR-0007 F6: extra RUMOR tags appended pre-wrap (e.g. the
    /// `toron:workflow-emitted` stamp from the workflow ActionRunner). Gift
    /// wraps stay metadata-hiding — these tags live INSIDE the rumor, never
    /// on the kind-1059 envelope.
    pub extra_tags: Vec<Tag>,
}

/// Delivery record returned by `send_message` (parity shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub project: String,
    pub payload: DeliveryPayload,
}

/// One recipient's delivery payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryPayload {
    pub recipient: String,
    pub event_id: String,
}

/// Result of `send_message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResult {
    pub deliveries: Vec<Delivery>,
    pub count: usize,
    pub verified_sender: bool,
}

/// A projected inbox message (rumor-time order, G7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxMessage {
    /// Message id = rumor's canonical id (content-addressed, G7).
    pub id: String,
    pub sender: String,
    pub subject: String,
    pub body_md: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub created_ts_us: i64,
    /// i64-µs read timestamp from the `message_reads` projection (bd-5icg);
    /// `None` when the recipient never marked this message read (unread), or
    /// when no index pool is attached (read state cannot be known).
    pub read_ts_us: Option<i64>,
    /// S5 (bd-dup-assign-collapse-ws21): when this row is the surviving
    /// representative of a collapsed duplicate group (identical sender +
    /// subject + body-hash within the collapse window), this holds the id of
    /// the group's FIRST occurrence. `None` = standalone message (or the
    /// group's first — survivors are the latest copy, marked with the
    /// original's id). Wake-side staleness is flywheel's concern (S8); toron
    /// only stops double-delivering.
    pub duplicate_of: Option<String>,
    /// ADR-0027 E2: assignment tags projected from the rumor (never 1059).
    /// Typed: a corrupt corr tag fails the projection loudly (bd-05yr).
    pub corr: Option<Corr>,
    pub wake: Option<String>,
    pub priority: Option<String>,
    pub audience: Option<String>,
    pub result_path: Option<String>,
    /// HumanDecision live producer: `verdict=` extra_tag on the rumor.
    pub verdict: Option<String>,
    /// ADR-0010 D9 (bd-l0fz): the structural done-hold — `bead=<id>` extra_tag
    /// on the rumor. The flywheel dispatch guard matches THIS, never the
    /// `[{id}] done` subject.
    pub bead: Option<String>,
}

/// S5 collapse window: identical (sender, subject, body-hash) messages
/// within this window collapse to one inbox row (i64-µs, 1h default).
pub const DUP_COLLAPSE_WINDOW_US: i64 = 3_600_000_000;

/// Default ack-SLA grace: shared by the overdue scan and the
/// `ack_overdue_only` inbox branch so both agree on what "overdue" means.
/// No per-call SLA arg on the inbox path (schema freeze — one meaning).
pub const ACK_SLA_DEFAULT_SECS: i64 = 30 * 60;

/// bd-4v1a: how many reminders ONE obligation gets before it is terminally
/// muted. A post nobody acks stays overdue forever, so an unbounded reminder
/// re-nags forever — measured live, 95,077 of the bus's 100,000 rows were
/// this one reminder plane. After the budget is spent the obligation is not
/// nagged again; it stays visible instead (the reader's inbox, the
/// `ack_sla_wakes.wakes` counter, `toron ack-sla`'s `muted` list).
pub const ACK_SLA_WAKE_BUDGET: i64 = 3;

/// bd-4v1a: how many reminders ONE scan may publish. One scan cannot flood
/// the bus.
pub const ACK_SLA_SCAN_MAX_EMIT: usize = 500;

/// bd-4v1a: the scan deadline. The doorbell kills `toron ack-sla` at 15s, so
/// a scan that outruns that never reports anything at all.
pub const ACK_SLA_SCAN_DEADLINE_MS: u64 = 10_000;

/// `fetch_inbox` options.
#[derive(Debug, Clone, Default)]
pub struct FetchInbox {
    pub project_slug: String,
    pub agent_name: String,
    pub unread_only: bool,
    pub limit: Option<usize>,
    pub include_bodies: bool,
    /// Keep only ack-overdue rows: ack_required + high/urgent +
    /// older than `ACK_SLA_DEFAULT_SECS` + this recipient unacked. Same rule
    /// as the SLA scan, applied post-projection so every inbox path inherits
    /// it. `false` = byte-identical to before the field existed.
    pub ack_overdue_only: bool,
    /// Keep only messages with importance ∈ {high, urgent} (F11 / c3b.3 —
    /// applied post-projection, same for EVERY inbox path).
    pub urgent_only: bool,
    /// Keep only messages with `created_ts_us` strictly newer than this
    /// RFC3339 timestamp. `None` = no time bound. A malformed timestamp is a
    /// `MailError::Malformed` — never a silently ignored filter.
    pub since_ts: Option<String>,
    /// Keep only messages whose projected `thread_id` equals this value
    /// (bd-harvest-thread-filter-yp9r). Applied post-unwrap — the thread tag
    /// lives INSIDE the encrypted gift wrap, so no SQL/bus filter can see it;
    /// the filter runs in the shared core stage so every inbox path (surface,
    /// MCP tool, CLI robot) inherits it. `None` = no thread bound, behavior
    /// byte-identical to before this field existed.
    pub thread_id: Option<String>,
}

/// Parse an RFC3339 timestamp into epoch micros (the unit every toron row's
/// `created_ts_us` uses). A bad timestamp is a `MailError::Malformed` — never
/// a silently ignored filter (F11 / c3b.3 honesty). `pub(crate)`: the search
/// surface path (surface.rs `date_start`/`date_end`) shares it.
pub(crate) fn parse_rfc3339_us(ts: &str) -> Result<i64, MailError> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .map(|dt| dt.timestamp_micros())
        .map_err(|e| {
            MailError::new(
                MailErrorCode::Malformed,
                format!("timestamp {ts:?} is not RFC3339: {e}"),
            )
        })
}

/// `fetch_inbox` post-projection filters (urgent / since) that live in the
/// SHARED core inbox query (bd-mcp-inbox-filter-qyyq): the MailSurface path
/// and every MCP tool path inherit them from `Mailbox::fetch_inbox` — no
/// per-path filter copies. Applied BEFORE `limit` so a bound narrows what
/// the limit truncates.
fn filter_inbox_messages(
    items: Vec<InboxMessage>,
    q: &FetchInbox,
) -> Result<Vec<InboxMessage>, MailError> {
    let since_us = q.since_ts.as_deref().map(parse_rfc3339_us).transpose()?;
    let overdue_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
        - ACK_SLA_DEFAULT_SECS * 1_000_000;
    Ok(items
        .into_iter()
        .filter(|m| {
            if q.urgent_only && !matches!(m.importance.as_str(), "high" | "urgent") {
                return false;
            }
            if let Some(s) = since_us
                && m.created_ts_us <= s
            {
                return false;
            }
            if let Some(t) = &q.thread_id
                && m.thread_id.as_deref() != Some(t.as_str())
            {
                return false;
            }
            let ack_overdue = m.ack_required
                && matches!(m.importance.as_str(), "high" | "urgent")
                && m.created_ts_us < overdue_us
                && m.read_ts_us.is_none();
            !q.ack_overdue_only || ack_overdue
        })
        .collect())
}

/// A receipt outcome (idempotent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptResult {
    pub message_id: String,
    pub acknowledged: bool,
    pub acknowledged_at_us: Option<i64>,
    /// ADR-0010 F5: read and ack are distinct shapes — true only for
    /// `mark_message_read`, so a successful read never looks like a
    /// failed ack (`{acknowledged: false}`) on the shared envelope.
    pub read: bool,
}

impl Mailbox {
    /// `send_message`: resolve names → npubs (fail-fast on unknown), build the
    /// visible rumor + per-bcc variants, gift-wrap one copy per recipient,
    /// publish every wrap. Returns per-recipient delivery records.
    ///
    /// NIP-17 fan-out: N recipients ⇒ N kind-1059 wraps, each unwrapping to the
    /// same kind-14 rumor (to/cc) or the bcc-variant rumor. Gift wraps are
    /// metadata-hiding: they carry NO project tags (A3).
    pub async fn send_message(&self, args: &SendMessage) -> anyhow::Result<SendResult> {
        let parsed_tags = crate::mailbox::send_tags::SendTags::parse(&args.extra_tags)
            .map_err(|e| anyhow::anyhow!(e.message))?;
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await;
        for raw in args.to.iter().chain(args.cc.iter()).chain(args.bcc.iter()) {
            if super::surface::is_group_target(raw) {
                return Err(anyhow::anyhow!(super::surface::group_refused_msg(raw)));
            }
        }
        let resolved = self.resolve_recipients(&args).await?;
        if resolved.is_empty() {
            anyhow::bail!("no recipients (to/cc/bcc all empty)");
        }
        parsed_tags
            .validate_for_send(resolved.len(), args.overseer)
            .map_err(|e| anyhow::anyhow!(e.message))?;
        {
            use crate::mailbox::send_tags::{ThreadId, reject_thread_as_corr};
            use std::str::FromStr;
            let thread = args
                .thread_id
                .as_deref()
                .map(ThreadId::from_str)
                .transpose()
                .map_err(|e| anyhow::anyhow!(e))?;
            let corr = parsed_tags.corr.clone();
            reject_thread_as_corr(thread.as_ref(), corr.as_ref())
                .map_err(|e| anyhow::anyhow!(e.message))?;
        }

        let now_ms = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock) / 1000;
        {
            let exempt = if args.overseer {
                super::surface::Exempt::Operator
            } else {
                super::surface::Exempt::None
            };
            for r in resolved.iter() {
                let key = super::surface::loop_key_mail(&args.sender_name, &r.name);
                let history = self.loop_tracker.history(&key);
                if !super::surface::loop_ok(&history, now_ms, exempt) {
                    return Err(anyhow::anyhow!(super::surface::loop_refused_msg(
                        format!("{} -> {} burst", args.sender_name, r.name).as_str()
                    )));
                }
            }
            for r in resolved.iter() {
                let key = super::surface::loop_key_mail(&args.sender_name, &r.name);
                let _ = self.loop_tracker.check_and_record(&key, now_ms, exempt);
            }
        }
        let mut receipt_bookkeeping_failed = false;
        if let Some(corr) = parsed_tags.corr.clone() {
            match self.receipt_store() {
                Ok(store) => {
                    let receipt = crate::mailbox::receipts::Receipt {
                        corr: corr.as_str().to_string(),
                        state: crate::mailbox::receipts::ReceiptState::Owed,
                        wake: parsed_tags.wake,
                        due_at: None,
                        result_path: parsed_tags.result_path.clone(),
                        updated_at: now_ms,
                    };
                    if let Err(e) = store.upsert(&receipt).await {
                        eprintln!("toron: receipts store unavailable (receipt not recorded): {e}");
                        receipt_bookkeeping_failed = true;
                    } else {
                        let pool = store.pool().clone();
                        tokio::spawn(async move {
                            let s = crate::mailbox::receipts::ReceiptStore::new(pool);
                            let now_ms = crate::index::writer::Clock::now_us(
                                &crate::index::writer::SystemClock,
                            ) / 1000;
                            let _ = s.prune_expired(now_ms, 30 * 24 * 60 * 60 * 1000).await;
                            let _ = s.prune_to_cap(1000, now_ms).await;
                        });
                    }
                }
                Err(e) => {
                    eprintln!("toron: receipts store unavailable (receipt not recorded): {e}");
                    receipt_bookkeeping_failed = true;
                }
            }
        }
        let sender_keys = if args.overseer {
            self.operator().cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "overseer send requires an operator key (set operator_key / TORON_OPERATOR_KEY)"
                )
            })?
        } else {
            self.agent_keys_for_resolved(&args.project_slug, &args.sender_name)
                .await?
        };

        let rumor_ts = Timestamp::now().as_secs();

        let (mut pending, mut visible_id) =
            build_pending_rumors(&sender_keys, &args, &resolved, rumor_ts);

        let mut replay: Option<Vec<Delivery>> = None;
        let mut claim: Option<(String, String)> = None;
        if let Some(corr) = parsed_tags.corr.clone() {
            use crate::mailbox::idempotent::{SendClaim, send_key};
            if let Ok(store) = self.idempotent_store() {
                let intended: Vec<(String, String)> = pending
                    .iter()
                    .map(|(r, _, id_hex)| (r.name.clone(), id_hex.clone()))
                    .collect();
                let intended_json =
                    serde_json::to_string(&intended).unwrap_or_else(|_| "[]".into());
                let now_ms =
                    crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock) / 1000;
                let key = send_key(
                    &args.project_slug,
                    &args.sender_name,
                    corr.as_str(),
                    &args.subject,
                    &args.body_md,
                );
                claim = Some((key.clone(), intended_json.clone()));
                match store
                    .claim_or_duplicate(
                        &key,
                        &visible_id,
                        &args.project_slug,
                        &args.sender_name,
                        corr.as_str(),
                        &intended_json,
                        rumor_ts as i64,
                        now_ms,
                    )
                    .await
                {
                    Ok(SendClaim::Duplicate {
                        event_id: _,
                        deliveries_json,
                        published,
                        rumor_created_at,
                    }) => match rumor_created_at {
                        Some(ts) if !published => {
                            let (rebuilt, rebuilt_visible) =
                                build_pending_rumors(&sender_keys, &args, &resolved, ts as u64);
                            pending = rebuilt;
                            visible_id = rebuilt_visible;
                            claim = Some((key.clone(), {
                                let intended: Vec<(String, String)> = pending
                                    .iter()
                                    .map(|(r, _, id_hex)| (r.name.clone(), id_hex.clone()))
                                    .collect();
                                serde_json::to_string(&intended).unwrap_or_else(|_| "[]".into())
                            }));
                        }
                        _ => {
                            let stored: Vec<(String, String)> =
                                serde_json::from_str(&deliveries_json).unwrap_or_default();
                            let deliveries = stored
                                .into_iter()
                                .map(|(recipient, event_id)| Delivery {
                                    project: args.project_slug.clone(),
                                    payload: DeliveryPayload {
                                        recipient,
                                        event_id,
                                    },
                                })
                                .collect::<Vec<_>>();
                            replay = Some(deliveries);
                        }
                    },
                    Ok(SendClaim::New) => {
                        let pool = store.pool().clone();
                        tokio::spawn(async move {
                            let s = crate::mailbox::idempotent::IdempotentSendStore::new(pool);
                            let now = crate::index::writer::Clock::now_us(
                                &crate::index::writer::SystemClock,
                            ) / 1000;
                            let _ = s.prune_expired(now).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("toron: idempotent-send claim failed (sending anyway): {e}");
                    }
                }
            }
        }

        if let Some(deliveries) = replay {
            let count = deliveries.len();
            return Ok(SendResult {
                count,
                deliveries,
                verified_sender: true,
            });
        }

        let pending_len = pending.len();
        let mut deliveries: Vec<Delivery> = Vec::new();

        let outer_urgent = args.importance.as_deref() == Some("urgent");
        for (r, rumor, rumor_id) in pending {
            let wrap =
                gift_wrap::gift_wrap_with_urgency(&sender_keys, &r.npub, rumor, outer_urgent)
                    .map_err(|e| {
                        anyhow::anyhow!("gift wrap failed for recipient {}: {e}", r.name)
                    })?;
            match self.publish_dm(&wrap).await {
                Ok(true) => {
                    deliveries.push(Delivery {
                        project: args.project_slug.clone(),
                        payload: DeliveryPayload {
                            recipient: r.name.clone(),
                            event_id: rumor_id,
                        },
                    });
                }
                Ok(false) => {
                    eprintln!(
                        "{}",
                        crate::mailbox::receipts::resend_guidance(
                            parsed_tags.corr.as_ref().map(Corr::as_str)
                        )
                    );
                }
                Err(e) => {
                    eprintln!("toron: DM publish failed for {}: {e}", r.name);
                    eprintln!(
                        "{}",
                        crate::mailbox::receipts::resend_guidance(
                            parsed_tags.corr.as_ref().map(Corr::as_str)
                        )
                    );
                }
            }
        }

        if deliveries.len() == pending_len
            && let Some((key, intended_json)) = claim.as_ref()
            && let Ok(store) = self.idempotent_store()
            && let Err(e) = store.mark_published(key, &visible_id, intended_json).await
        {
            eprintln!(
                "toron: send claim not marked published (a retry re-publishes the same event): {e}"
            );
        }

        if receipt_bookkeeping_failed
            && !deliveries.is_empty()
            && let Some(corr) = parsed_tags.corr.clone()
        {
            eprintln!("{}", crate::mailbox::receipts::DEGRADED_NOTE);
            if let Ok(store) = self.receipt_store() {
                let model = match store.get(corr.as_str()).await.ok().flatten() {
                    Some(row) => crate::mailbox::receipts_core::ReceiptsModel::with_row(
                        corr.as_str(),
                        row.state.into(),
                        row.result_path.clone(),
                        row.updated_at,
                    ),
                    None => crate::mailbox::receipts_core::ReceiptsModel::new(),
                };
                let (_, fx) = crate::mailbox::receipts_core::tick(
                    &model,
                    &crate::mailbox::receipts_core::ReceiptsMsg::MarkDegraded {
                        corr: corr.as_str().to_string(),
                        now_ms,
                    },
                );
                if let Some(crate::mailbox::receipts_core::ReceiptEffect::Stamp {
                    result_path,
                    updated_at_ms,
                    ..
                }) = fx.into_iter().next()
                {
                    let degraded = crate::mailbox::receipts::Receipt {
                        corr: corr.as_str().to_string(),
                        state: crate::mailbox::receipts::ReceiptState::Degraded,
                        wake: parsed_tags.wake,
                        due_at: None,
                        result_path: result_path.or(parsed_tags.result_path.clone()),
                        updated_at: updated_at_ms,
                    };
                    if let Err(e) = store.upsert(&degraded).await {
                        eprintln!("toron: degraded receipt could not be recorded: {e}");
                    }
                }
            }
        }

        let thread_ref = args.thread_id.clone().unwrap_or_else(|| visible_id.clone());
        let importance = args
            .importance
            .clone()
            .unwrap_or_else(|| "normal".to_string());
        for r in &resolved {
            let wake_content = format!(
                "[doorbell] mail from {} thread {} importance {}",
                args.sender_name, thread_ref, importance
            );
            let tags = vec![
                Tag::parse(["p", &r.npub.to_hex()]).expect("p tag"),
                Tag::parse(["t", &args.project_slug]).expect("t tag"),
                Tag::parse(["h", &thread_ref]).expect("h tag"),
                Tag::parse(["toron-kind", "mail-delivered"]).expect("toron-kind"),
                Tag::parse(["thread", &thread_ref]).expect("thread"),
                Tag::parse(["importance", &importance]).expect("importance"),
                Tag::parse(["from", &args.sender_name]).expect("from"),
            ];
            if let Ok(ev) = build_event(
                &sender_keys,
                toron_kind(9),
                wake_content,
                tags,
                Timestamp::now(),
            ) {
                let _ = self.bus.publish(ev).await;
            }
        }

        let now_us = super::lease_liveness::host_now_us();
        self.record_inbox_poll(&args.project_slug, &args.sender_name, now_us)
            .await;

        Ok(SendResult {
            count: deliveries.len(),
            deliveries,
            verified_sender: true,
        })
    }

    /// `reply_message`: like `send_message` but attaches the thread as a NIP-10
    /// `["e", root, relay, "reply"]` tag on the rumor. Recipients default to
    /// the original message's sender when `to` is empty.
    pub async fn reply_message(&self, args: &SendMessage) -> anyhow::Result<SendResult> {
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await;
        let root = args
            .thread_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("reply requires thread_id (original message id)"))?;
        let mut reply = args.clone();
        reply.thread_id = Some(root);
        self.send_message(&reply).await
    }

    /// bd-since-cursor-70cf: the durable plane-P high-water for
    /// (project, agent). Missing row = 0 (everything is new on a cold
    /// start). Requires the index pool — without it there is no durable
    /// cursor store and `--since last` refuses loudly instead of guessing.
    async fn read_inbox_cursor(&self, project: &str, agent: &str) -> anyhow::Result<i64> {
        let Some(pool) = self.index_pool.as_ref() else {
            anyhow::bail!(
                "--since \"last\" requires the durable cursor store (index pool); \
                 pass an explicit RFC3339 timestamp instead"
            );
        };
        let row: Option<i64> = sqlx::query_scalar(
            "SELECT last_seen_us FROM inbox_cursors \
             WHERE project_slug = ?1 AND agent_name = ?2 AND plane = 'P'",
        )
        .bind(project)
        .bind(agent)
        .fetch_optional(pool)
        .await?;
        Ok(row.unwrap_or(0))
    }

    /// bd-oracle-dead-source-72az / bd-liveness-any-command-mc34: a
    /// successful authenticated command proves the actor's process is
    /// alive, so `fetch_inbox` (the one choke point behind CLI `mail
    /// inbox` and the MCP `fetch_inbox` tool), `toron next`, acquire,
    /// renew, and send timestamp the actor with the host clock passed in
    /// at the read/effect edge — including empty inboxes, where the
    /// delivery high-water never advances. Best-effort by design (returns
    /// `()`, never `Result`): a liveness timestamp must never break the
    /// command it annotates. Writes `last_poll_us`, never `last_seen_us`:
    /// the delivery cursor is rumor-time, and wall-clock would skip
    /// late-arriving messages with older rumor timestamps.
    pub async fn record_inbox_poll(&self, project: &str, agent: &str, now_us: i64) {
        let Some(pool) = self.index_pool.as_ref() else {
            return;
        };
        let _ = sqlx::query(
            "INSERT INTO inbox_cursors (project_slug, agent_name, plane, last_seen_us, last_poll_us) \
             VALUES (?1, ?2, 'P', 0, ?3) \
             ON CONFLICT(project_slug, agent_name, plane) \
             DO UPDATE SET last_poll_us = MAX(last_poll_us, excluded.last_poll_us)",
        )
        .bind(project)
        .bind(agent)
        .bind(now_us)
        .execute(pool)
        .await;
    }
    /// bd-since-cursor-70cf: bump the cursor to the newest VISIBLE message
    /// ts — one atomic UPSERT with MAX, so concurrent readers never rewind
    /// the high-water and a crash mid-poll never skips messages.
    async fn bump_inbox_cursor(
        &self,
        project: &str,
        agent: &str,
        seen_us: i64,
    ) -> anyhow::Result<()> {
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(());
        };
        sqlx::query(
            "INSERT INTO inbox_cursors (project_slug, agent_name, plane, last_seen_us) \
             VALUES (?1, ?2, 'P', ?3) \
             ON CONFLICT(project_slug, agent_name, plane) \
             DO UPDATE SET last_seen_us = MAX(last_seen_us, excluded.last_seen_us)",
        )
        .bind(project)
        .bind(agent)
        .bind(seen_us)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// The reader's delivered kind-1059 mail, unwrapped (shared collection
    /// behind `fetch_inbox` and the `next` ingest: contact handshakes are
    /// protocol, not mail, in both views).
    async fn collect_unwrapped_mail(
        &self,
        project_slug: &str,
        agent_name: &str,
    ) -> anyhow::Result<(crate::nostr::PublicKey, Vec<UnwrappedMail>)> {
        let agent_keys = self
            .agent_keys_for_resolved(project_slug, agent_name)
            .await?;
        let npub = agent_keys.public_key();

        let filter = ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(npub)
            .limit(crate::bus::event_store::MAX_STORED_EVENTS as usize)
            .build();
        let events = super::collect_replay(self.bus.as_ref(), filter).await?;

        let mut mails: Vec<UnwrappedMail> = Vec::new();
        for wrap in &events {
            if let Some(gift) = self.cached_unwrap(&agent_keys, wrap, npub) {
                let rumor = gift.rumor;
                if let Some(kind) = tag_value(&rumor.tags, "toron-kind")
                    && (kind == "contact-request" || kind == "contact-response")
                {
                    continue;
                }
                mails.push(UnwrappedMail {
                    recipient: npub,
                    sender: gift.sender,
                    rumor_id: rumor.clone().id(),
                    wrap_event_id: wrap.id,
                    created_at: rumor.created_at,
                    rumor,
                });
            }
        }
        Ok((npub, mails))
    }

    /// bd-next-envelope-ingest-yl0cx: every delivered DM becomes visible to
    /// `next` — one `request@1` envelope row per message in the reader's
    /// inbox log, so `next` commits cursor + Delivered PER MESSAGE. Exact
    /// dedupe by rumor id (re-ingest adds nothing); a stale claim heals by
    /// re-appending instead of going invisible. Returns rows added. This is
    /// not a high-water view: each message keeps its own row and delivery.
    pub async fn ingest_delivered_mail(
        &self,
        project: &str,
        agent_name: &str,
        store: &crate::inbox::InboxStore,
    ) -> anyhow::Result<usize> {
        use crate::inbox::{INBOX_STREAM, InboxError};
        use crate::kernel::AppendError;
        let project_slug = self.canonical_project(project).await;
        let (npub, mails) = self
            .collect_unwrapped_mail(&project_slug, agent_name)
            .await?;
        let reader = npub.to_hex();
        let mut added = 0usize;
        for (mail, _seq) in order_by_rumor_time(mails) {
            let Some(msg) = inbox_from(mail) else {
                continue;
            };
            let rumor_hex = msg.id.clone();
            let sender = msg.sender.clone();
            let mut env =
                mail_envelope(&project_slug, &sender, &reader, &msg.subject, &msg.body_md);
            // Exact dedupe: a landed claim with a live row means done; a
            // stale claim (crash between claim and append) heals below.
            if let Some(k) = store.mail_ingest_get(&reader, &rumor_hex).await?
                && store.has_log_row(INBOX_STREAM, &reader, &k).await?
            {
                continue;
            }
            let mut done = false;
            for _ in 0..8 {
                // Per-scope sequence: one sender into many readers never
                // shares a row, so cross-reader ingest never contends.
                let (epoch, seq) = store.ingest_seq_for(&sender, &reader).await?;
                env.msg.epoch = epoch;
                env.msg.seq = seq;
                let k = env.msg.key();
                if !store.mail_ingest_claim(&reader, &rumor_hex, &k).await? {
                    // Lost the claim race: the winner landed the row (done)
                    // or crashed first (steal the claim and append below).
                    if let Some(wk) = store.mail_ingest_get(&reader, &rumor_hex).await?
                        && store.has_log_row(INBOX_STREAM, &reader, &wk).await?
                    {
                        done = true;
                        break;
                    }
                    store.mail_ingest_set_key(&reader, &rumor_hex, &k).await?;
                }
                match store.append_envelope(&env).await {
                    Ok(_) => {
                        added += 1;
                        done = true;
                        break;
                    }
                    // Our triple moved under us (concurrent ingester) or is
                    // foreign-occupied: recompute and retry.
                    Err(InboxError::Append(AppendError::SeqGap { .. }))
                    | Err(InboxError::PayloadConflict { .. }) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            if !done {
                anyhow::bail!(
                    "mail ingest contention for {rumor_hex}: 8 tries lost the producer race"
                );
            }
        }
        Ok(added)
    }

    /// `fetch_inbox`: project + agent projection of unwrapped kind-1059 mail to
    /// the agent's local key, ordered by rumor time (G7), never envelope time.
    pub async fn fetch_inbox(&self, args: &FetchInbox) -> anyhow::Result<Vec<InboxMessage>> {
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await;
        let (npub, mails) = self
            .collect_unwrapped_mail(&args.project_slug, &args.agent_name)
            .await?;

        let ordered = order_by_rumor_time(mails);
        self.heal_search_projection(&ordered).await;
        let mut messages: Vec<InboxMessage> = ordered
            .into_iter()
            .filter_map(|(m, _seq)| inbox_from(m))
            .collect();

        messages = collapse_duplicates(messages);

        let mut read_ts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        if let Some(pool) = self.index_pool.as_ref() {
            let set = self.project_read_set(&args.project_slug).await;
            let ph: Vec<String> = (1..=set.len()).map(|i| format!("?{i}")).collect();
            let sql = format!(
                "SELECT message_id, read_ts_us FROM message_reads \
                 WHERE recipient = ?{} AND project_slug IN ({})",
                set.len() + 1,
                ph.join(", ")
            );
            let mut q = sqlx::query_as::<_, (String, i64)>(&sql);
            for s in &set {
                q = q.bind(s);
            }
            let rows: Vec<(String, i64)> = q
                .bind(npub.to_hex())
                .fetch_all(pool)
                .await
                .unwrap_or_default();
            read_ts.extend(rows);
        }
        for m in &mut messages {
            m.read_ts_us = read_ts.get(&m.id).copied();
        }
        if let Some(pool) = self.index_pool.as_ref() {
            let ids: Vec<String> = messages.iter().map(|m| m.id.clone()).collect();
            if !ids.is_empty() {
                let placeholders = std::iter::repeat_n("?", ids.len())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "SELECT event_id FROM messages WHERE deleted = 1 AND event_id IN ({placeholders})"
                );
                let mut q = sqlx::query(&sql);
                for id in &ids {
                    q = q.bind(id);
                }
                let deleted: Vec<String> = q
                    .fetch_all(pool)
                    .await
                    .map(|rows| {
                        rows.into_iter()
                            .filter_map(|r| r.try_get::<String, _>(0).ok())
                            .collect()
                    })
                    .unwrap_or_default();
                if !deleted.is_empty() {
                    messages.retain(|m| !deleted.contains(&m.id));
                }
            }
        }
        if args.unread_only {
            messages.retain(|m| m.read_ts_us.is_none());
        }
        let since_last = args.since_ts.as_deref() == Some("last");
        let cursor_us: i64 = if since_last {
            self.read_inbox_cursor(&args.project_slug, &args.agent_name)
                .await?
        } else {
            0
        };
        let high_water_us = messages.iter().map(|m| m.created_ts_us).max().unwrap_or(0);
        if args.urgent_only || args.since_ts.is_some() || args.thread_id.is_some() {
            let mut q = args.clone();
            if since_last {
                q.since_ts = None;
            }
            messages = filter_inbox_messages(messages, &q)?;
            if since_last {
                messages.retain(|m| m.created_ts_us > cursor_us);
            }
        }
        if since_last {
            self.bump_inbox_cursor(
                &args.project_slug,
                &args.agent_name,
                high_water_us.max(cursor_us),
            )
            .await?;
        }
        let now_us = super::lease_liveness::host_now_us();
        self.record_inbox_poll(&args.project_slug, &args.agent_name, now_us)
            .await;

        let mut messages = if let Some(n) = args.limit
            && n < messages.len()
        {
            messages.split_off(messages.len() - n)
        } else {
            messages
        };
        self.resolve_sender_names(&mut messages).await;
        Ok(messages)
    }

    /// bd-fts-projection-lag: heal the search projection at READ time.
    ///
    /// The `messages`/FTS projection's only writer used to be the daemon's
    /// unwrap loop — a CLI-only session (no projecting daemon) left every
    /// fresh mail invisible to `search_messages`. `fetch_inbox` is the
    /// recipient's read door: it has already decrypted each wrap, so writing
    /// the unwrapped mail through the same dual sink the daemon uses heals
    /// the lag with zero extra crypto. The write is the daemon's exact
    /// contract — git-first (G11), idempotent on rumor-id (`DedupKey` +
    /// `ON CONFLICT DO NOTHING`), so repeated fetches and the daemon's own
    /// subscription loop converge on the same rows. The pre-check is a
    /// chunked bulk SQL lookup — already-projected history (the steady-state
    /// fleet-poll case) skips the sink entirely.
    ///
    /// Best-effort: any sink error degrades to the pre-fix behavior (search
    /// lags until a projecting daemon runs); the inbox read itself never
    /// fails. Triggers do NOT fire here — reads are not deliveries; the
    /// daemon loop owns `message_posted` side effects.
    async fn heal_search_projection(&self, ordered: &[(UnwrappedMail, u64)]) {
        let Some(pool) = self.index_pool.as_ref() else {
            return;
        };
        if ordered.is_empty() {
            return;
        }
        let ids: Vec<String> = ordered.iter().map(|(m, _)| m.rumor_id.to_hex()).collect();
        let mut projected: std::collections::HashSet<String> = std::collections::HashSet::new();
        for chunk in ids.chunks(500) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT event_id FROM messages WHERE event_id IN ({placeholders})");
            let mut q = sqlx::query(&sql);
            for id in chunk {
                q = q.bind(id);
            }
            match q.fetch_all(pool).await {
                Ok(rows) => projected.extend(
                    rows.into_iter()
                        .filter_map(|r| r.try_get::<String, _>(0).ok()),
                ),
                Err(e) => {
                    tracing::debug!("projection pre-check failed (healing write covers it): {e}");
                }
            }
        }
        let todo: Vec<(&UnwrappedMail, u64)> = ordered
            .iter()
            .filter(|(m, _)| !projected.contains(&m.rumor_id.to_hex()))
            .map(|(m, s)| (m, *s))
            .collect();
        if todo.is_empty() {
            return;
        }
        let roster: Vec<(String, String, String)> =
            sqlx::query_as("SELECT project_slug, name, pubkey FROM agents")
                .fetch_all(pool)
                .await
                .unwrap_or_default();
        let npub_to_project: HashMap<String, String> = roster
            .iter()
            .map(|(slug, _name, npub)| (npub.clone(), slug.clone()))
            .collect();
        let npub_to_name: HashMap<String, String> = roster
            .iter()
            .map(|(_slug, name, npub)| (npub.clone(), name.clone()))
            .collect();
        let sqlite: Arc<dyn ProjectionSink> = Arc::new(
            SqliteProjectionSink::new(pool.clone())
                .with_projects(npub_to_project.clone())
                .with_memberships(&roster)
                .with_names(npub_to_name),
        );
        let git: Arc<dyn ProjectionSink> = Arc::new(ProjectGitProjectionSink::for_mailbox(
            self.data_dir.clone(),
            npub_to_project,
        ));
        let dual = DualProjectionSink::new(git, sqlite);
        for (mail, seq) in &todo {
            if let Err(e) = dual.write(mail, *seq).await {
                tracing::debug!(
                    rumor_id = %mail.rumor_id,
                    "search-projection heal failed (index lags until next fetch/daemon): {e}"
                );
            }
        }
    }

    /// Best-effort batch npub→name resolution for inbox entries. Mutates
    /// `sender` in place; unknown pubkeys keep their hex value. One query per
    /// fetch (parameterized over the unique-sender set).
    async fn resolve_sender_names(&self, messages: &mut [InboxMessage]) {
        let Some(pool) = self.index_pool.as_ref() else {
            return;
        };
        let mut pubkeys: std::collections::HashSet<String> = std::collections::HashSet::new();
        for m in messages.iter() {
            if m.sender.chars().all(|c| c.is_ascii_hexdigit()) && !m.sender.is_empty() {
                pubkeys.insert(m.sender.clone());
            }
        }
        if pubkeys.is_empty() {
            return;
        }
        let placeholders = std::iter::repeat_n("?", pubkeys.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT pubkey, name FROM agents WHERE pubkey IN ({placeholders}) GROUP BY pubkey"
        );
        let mut q = sqlx::query_as::<_, (String, String)>(&sql);
        for pk in &pubkeys {
            q = q.bind(pk);
        }
        let map: std::collections::HashMap<String, String> = match q.fetch_all(pool).await {
            Ok(rows) => rows.into_iter().collect(),
            Err(e) => {
                tracing::debug!("sender-name resolve failed (degrade to hex): {e}");
                return;
            }
        };
        for m in messages.iter_mut() {
            if let Some(name) = map.get(&m.sender) {
                m.sender = name.clone();
            }
        }
    }

    /// `acknowledge_message`: private NIP-17 kind-14 to the original sender
    /// (`toron-kind=ack` + NIP-10 `e` to the original). Idempotent: a second
    /// ack returns the stored timestamp without re-sending.
    pub async fn acknowledge_message(
        &self,
        project_slug: &str,
        agent_name: &str,
        message_id: &str,
    ) -> anyhow::Result<ReceiptResult> {
        self.send_receipt(project_slug, agent_name, message_id, "ack", true)
            .await
    }

    /// `mark_message_read`: private NIP-17 kind-14 to the original sender
    /// (`toron-kind=read` + NIP-10 `e`). Idempotent.
    pub async fn mark_message_read(
        &self,
        project_slug: &str,
        agent_name: &str,
        message_id: &str,
    ) -> anyhow::Result<ReceiptResult> {
        self.send_receipt(project_slug, agent_name, message_id, "read", false)
            .await
    }

    /// bd-i1o3.2: Ack SLA expiry (extends bd-mail-ack-sla-tracking-k0j).
    /// When an `importance=high|urgent` + `ack_required` message has no ack
    /// past the SLA (default 30 min), emit a metadata-only wake naming
    /// `post+thread+audience` onto the bus (kind `ACK_SLA_WAKE` +
    /// `toron-kind=ack-sla-expired`). Reuses the existing `messages` +
    /// receipts tracking structures; never builds a second tracker. Direct
    /// path stays cheap (one wake per expiry, not a poll).
    ///
    /// bd-4v1a: the reminder rides `ACK_SLA_WAKE` (30015), NOT the kind-9
    /// ROOM plane. A reminder is a wake, not a chat message: on the room
    /// plane every reminder competed with real crew posts for the bus's
    /// capped rows (measured live: 95,077 of 100,000 rows were this one
    /// reminder) and the doorbell forwarded every one of them into its
    /// single-serial wake brain, which dropped 1,879 real wake lines. The
    /// doorbell still sees the obligation — through `toron ack-sla`'s
    /// `expired` list, which is what its own scan reads.
    pub async fn emit_ack_sla_expired(
        &self,
        project_slug: &str,
        post_id: &str,
        thread_id: Option<&str>,
        audience: &[String],
    ) -> anyhow::Result<()> {
        let project_slug = self.canonical_project(project_slug).await;
        let signer = self.ack_sla_signer(&project_slug).await?;
        self.emit_ack_sla_expired_signed(&signer, &project_slug, post_id, thread_id, audience)
            .await
    }

    /// The signer for an ack-SLA reminder: the operator key when there is
    /// one, else the first agent key in the project (the reminder must be
    /// signed; the operator is the truthful signer).
    ///
    /// bd-4v1a: resolved ONCE per scan. Resolving it per candidate re-read
    /// and re-decrypted the key file thousands of times, and that was the
    /// two-minute scan the doorbell killed at its 15s timeout.
    async fn ack_sla_signer(&self, project_slug: &str) -> anyhow::Result<Keys> {
        if let Some(op) = self.operator().cloned() {
            return Ok(op);
        }
        let agents = self
            .list_agents(project_slug, ListAgentsOptions::default())
            .await
            .unwrap_or_default();
        for a in agents {
            if let Ok(k) = self.agent_keys_for(project_slug, &a.name) {
                return Ok(k);
            }
        }
        Err(anyhow::anyhow!("no signer for ack-sla wake"))
    }

    /// The signed reminder itself.
    async fn emit_ack_sla_expired_signed(
        &self,
        signer: &Keys,
        project_slug: &str,
        post_id: &str,
        thread_id: Option<&str>,
        audience: &[String],
    ) -> anyhow::Result<()> {
        let tid = thread_id.unwrap_or(post_id);
        let aud = if audience.is_empty() {
            "unknown".to_string()
        } else {
            audience.join(",")
        };
        let content =
            format!("[doorbell] ack-sla-expired post {post_id} thread {tid} audience {aud}");
        let mut tags = vec![
            Tag::parse(["t", project_slug]).expect("t"),
            Tag::parse(["h", tid]).expect("h"),
            Tag::parse(["toron-kind", "ack-sla-expired"]).expect("toron-kind"),
            Tag::parse(["post", post_id]).expect("post"),
            Tag::parse(["thread", tid]).expect("thread"),
            Tag::parse(["audience", &aud]).expect("audience"),
        ];
        for a in audience {
            if let Ok(pk) = PublicKey::from_hex(a) {
                if let Ok(t) = Tag::parse(["p", &pk.to_hex()]) {
                    tags.push(t);
                }
            } else if let Ok(t) = Tag::parse(["p", a]) {
                tags.push(t);
            }
        }
        if let Ok(ev) = build_event(
            signer,
            toron_kind(crate::nostr::kinds::ACK_SLA_WAKE),
            content,
            tags,
            Timestamp::now(),
        ) {
            let _ = self.bus.publish(ev).await;
        }
        Ok(())
    }

    /// Scan `messages` for overdue `ack_required` + `high|urgent` posts and
    /// emit one `ack-sla-expired` wake per overdue post whose SLA has lapsed
    /// and whose receipt is still missing. SLA consults `message_reads` +
    /// `receipts` sqlite — never the process HashMap.
    pub async fn check_ack_sla_expired(
        &self,
        project_slug: &str,
        sla_secs: Option<u64>,
    ) -> anyhow::Result<Vec<String>> {
        self.check_ack_sla_expired_within(
            project_slug,
            sla_secs,
            std::time::Instant::now() + std::time::Duration::from_millis(ACK_SLA_SCAN_DEADLINE_MS),
            ACK_SLA_SCAN_MAX_EMIT,
        )
        .await
    }

    /// The scan, with its bounds exposed: `deadline` stops the scan and
    /// `max_emit` caps how many reminders one scan publishes.
    ///
    /// bd-4v1a — the obligation is the CONVERSATION and the scan is bounded:
    /// - **Scope.** The dedupe key is `(scope, audience)`, where the scope is
    ///   the post's thread (the post id when it has no thread). A supervisor
    ///   that re-posts one request every two minutes produced 714 posts in
    ///   one thread and 714 reminders per window (measured live: 95,077 of
    ///   the bus's 100,000 rows were this one reminder plane). The audience
    ///   stays part of the key: one reader's ack must not silence the
    ///   reminder for a co-recipient who has not read it.
    /// - **Terminal posts.** A tombstoned post (`deleted = 1`) is unreadable
    ///   and an un-addressed post (`to_list = '[]'`) has no reader: neither
    ///   can ever be acked, so neither is ever reminded.
    /// - **Budget.** `ACK_SLA_WAKE_BUDGET` reminders per obligation, then the
    ///   obligation is terminally muted: the nag stops, the obligation does
    ///   not (it stays in the reader's inbox, and the counter that muted it
    ///   is durable in `ack_sla_wakes.wakes`).
    /// - **Bounds.** `deadline` stops the scan, `max_emit` caps what one scan
    ///   publishes, and the signer is resolved once for the whole scan.
    async fn check_ack_sla_expired_within(
        &self,
        project_slug: &str,
        sla_secs: Option<u64>,
        deadline: std::time::Instant,
        max_emit: usize,
    ) -> anyhow::Result<Vec<String>> {
        let project_slug = self.canonical_project(project_slug).await;
        let sla = sla_secs.map(|s| s as i64).unwrap_or(ACK_SLA_DEFAULT_SECS);
        let cutoff_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - sla * 1_000_000;
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(Vec::new());
        };
        let rows: Vec<(String, Option<String>, String)> = sqlx::query_as(
            "SELECT event_id, thread_id, to_list FROM messages \
             WHERE project_slug = ?1 AND ack_required = 1 \
             AND importance IN ('high','urgent') \
             AND deleted = 0 \
             AND to_list <> '[]' \
             AND created_ts_us < ?2 \
             ORDER BY created_ts_us ASC",
        )
        .bind(&project_slug)
        .bind(cutoff_us)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let window_us = sla * 1_000_000;
        let mut emitted = Vec::new();
        let mut signer: Option<Keys> = None;
        let mut scope_state: HashMap<(String, String), (i64, i64)> = HashMap::new();
        for (post_id, thread_id, to_list_json) in rows {
            if std::time::Instant::now() >= deadline {
                break;
            }
            if emitted.len() >= max_emit {
                break;
            }
            let audience: Vec<String> = serde_json::from_str(&to_list_json).unwrap_or_default();
            let mut unique: Vec<String> = audience.clone();
            unique.sort();
            unique.dedup();
            let mut handled = !unique.is_empty();
            if handled {
                for chunk in unique.chunks(50) {
                    let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                    let sql = format!(
                        "SELECT COUNT(DISTINCT recipient) FROM message_reads \
                         WHERE message_id = ? AND acked = 1 AND recipient IN ({placeholders})"
                    );
                    let mut query = sqlx::query_scalar::<_, i64>(&sql).bind(&post_id);
                    for recipient in chunk {
                        query = query.bind(recipient);
                    }
                    let acked: i64 = query.fetch_one(pool).await.unwrap_or(0);
                    if acked as usize != chunk.len() {
                        handled = false;
                        break;
                    }
                }
            }
            if handled {
                continue;
            }
            if let Ok(store) = self.receipt_store()
                && let Ok(Some(r)) = store.get(&post_id).await
                && matches!(
                    r.state,
                    crate::mailbox::receipts::ReceiptState::Acked
                        | crate::mailbox::receipts::ReceiptState::Resulted
                )
            {
                continue;
            }
            let scope_id = ack_sla_scope_id(thread_id.as_deref(), &post_id);
            let wake_key = ack_sla_wake_audience_key(&audience);
            let scope_key = (scope_id.clone(), wake_key.clone());
            let (wakes, emitted_us) = match scope_state.get(&scope_key) {
                Some(state) => *state,
                None => {
                    let state = self.ack_sla_scope_state(&scope_id, &wake_key).await;
                    scope_state.insert(scope_key.clone(), state);
                    state
                }
            };
            if wakes >= ACK_SLA_WAKE_BUDGET {
                continue;
            }
            if emitted_us >= now_us - window_us {
                continue;
            }
            if signer.is_none() {
                signer = self.ack_sla_signer(&project_slug).await.ok();
            }
            if let Some(s) = signer.as_ref() {
                let tid = thread_id.as_deref();
                let _ = self
                    .emit_ack_sla_expired_signed(s, &project_slug, &post_id, tid, &audience)
                    .await;
            }
            self.ack_sla_record_wake(&scope_id, &wake_key).await;
            scope_state.insert(scope_key, (wakes + 1, now_us));
            emitted.push(post_id);
        }
        Ok(emitted)
    }

    /// bd-4v1a: the durable reminder state of one obligation — how many
    /// reminders it has been given (`wakes`) and when the last one went out
    /// (`emitted_us`).
    ///
    /// A read failure answers `(0, 0)` (announce again) rather than swallowing
    /// the post: an unreadable record must not silence a real overdue ack, so
    /// the failure mode is a duplicate reminder, never a dropped one.
    async fn ack_sla_scope_state(&self, scope_id: &str, audience_key: &str) -> (i64, i64) {
        let Some(pool) = self.index_pool.as_ref() else {
            return (0, 0);
        };
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT wakes, emitted_us FROM ack_sla_wakes WHERE scope_id = ?1 AND audience = ?2",
        )
        .bind(scope_id)
        .bind(audience_key)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or((0, 0))
    }

    /// bd-4v1a: record that one obligation was reminded. `wakes` counts the
    /// reminders it has been given, and only ever goes up: the budget is the
    /// terminal state.
    async fn ack_sla_record_wake(&self, scope_id: &str, audience_key: &str) {
        let Some(pool) = self.index_pool.as_ref() else {
            return;
        };
        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let _ = sqlx::query(
            "INSERT INTO ack_sla_wakes (scope_id, audience, emitted_us, wakes) \
             VALUES (?1, ?2, ?3, 1) \
             ON CONFLICT (scope_id, audience) DO UPDATE SET \
             emitted_us = excluded.emitted_us, wakes = wakes + 1",
        )
        .bind(scope_id)
        .bind(audience_key)
        .bind(now_us)
        .execute(pool)
        .await;
    }

    /// Shared ack/read path (G8).
    ///
    /// ADR-0017 D5 — ack authorization + identity-drift repair:
    /// 1. The wrap is looked up with the ACKER's own keys first (recipient
    ///    identity, authorization case (a)).
    /// 2. On a miss (identity drift: the message was addressed to a
    ///    different identity — the 2026-08-14 split-root repro), the actual
    ///    recipient is discovered across the project's agents (alias-set
    ///    expanded), then the acker must be authorized as
    ///    (b) bound to the same pane as the recipient (herdr, best-effort),
    ///    or (c) a participant of the message's thread (sender of any thread
    ///    message, or able to unwrap any wrap in the same thread).
    ///    Otherwise the ack is REFUSED — "not authorized".
    async fn send_receipt(
        &self,
        project_slug: &str,
        agent_name: &str,
        message_id: &str,
        kind: &str,
        acknowledged: bool,
    ) -> anyhow::Result<ReceiptResult> {
        let project_slug = &self.canonical_project(project_slug).await;
        let agent_keys = self
            .agent_keys_for_resolved(project_slug, agent_name)
            .await?;
        let npub = agent_keys.public_key();

        let original = match self.find_message_for_keys(&agent_keys, message_id).await? {
            Some(gift) => gift,
            None => {
                let (gift, recipient_name) = self
                    .find_message_across_agents(project_slug, message_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("message not found for agent {agent_name}: {message_id}")
                    })?;
                self.authorize_ack(project_slug, agent_name, &recipient_name, &gift)
                    .await?;
                gift
            }
        };
        let sender_npub = original.sender;

        let corr_opt = original.rumor.tags.iter().find_map(|t| {
            let s = t.as_slice();
            if s.first().map(String::as_str) == Some("corr") && s.len() > 1 {
                Some(s[1].clone())
            } else {
                None
            }
        });

        let id = EventId::from_hex(message_id).unwrap_or_else(|_| original.rumor.clone().id());
        if let Some(ts) = self.receipt_time(project_slug, agent_name, &id, kind) {
            if let Some(corr_s) = corr_opt.clone()
                && let Ok(corr) = crate::mailbox::ids::Corr::from_str(&corr_s)
                && let Ok(store) = self.receipt_store()
            {
                let now_ms =
                    crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock) / 1000;
                let _ = store.transition_acked(&corr, now_ms).await;
            }
            return Ok(ReceiptResult {
                message_id: message_id.to_string(),
                acknowledged,
                acknowledged_at_us: Some(ts),
                read: kind == "read",
            });
        }

        let tags = vec![
            Tag::parse(["toron-kind", kind])
                .map_err(|e| anyhow::anyhow!("failed to build toron-kind tag: {e}"))?,
            Tag::event(original.rumor.clone().id()),
        ];
        let rumor = gift_wrap::build_rumor(
            npub,
            Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            tags,
            "",
        );
        let wrap = gift_wrap::gift_wrap(&agent_keys, &sender_npub, rumor)
            .map_err(|e| anyhow::anyhow!("gift wrap failed for receipt to {sender_npub}: {e}"))?;
        let ts_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        self.publish_dm(&wrap).await?;
        self.record_receipt(project_slug, agent_name, &id, kind, ts_us);
        self.record_read_state(project_slug, npub, &id, ts_us, kind == "ack")
            .await;
        if let Some(corr_s) = corr_opt
            && let Ok(corr) = crate::mailbox::ids::Corr::from_str(&corr_s)
            && let Ok(store) = self.receipt_store()
        {
            let now_ms =
                crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock) / 1000;
            if let Err(e) = store.transition_acked(&corr, now_ms).await {
                eprintln!("toron: receipt transition to acked failed for corr {corr}: {e}");
            }
        }
        Ok(ReceiptResult {
            message_id: message_id.to_string(),
            acknowledged,
            acknowledged_at_us: Some(ts_us),
            read: kind == "read",
        })
    }

    /// Find the unwrapped gift for `message_id` addressed to `keys`
    /// (replay the recipient's wraps, match the rumor id). `None` when this
    /// identity was not the recipient.
    async fn find_message_for_keys(
        &self,
        keys: &Keys,
        message_id: &str,
    ) -> anyhow::Result<Option<UnwrappedGift>> {
        let npub = keys.public_key();
        let filter = ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(npub)
            .build();
        let events = super::collect_replay(self.bus.as_ref(), filter).await?;
        Ok(events.iter().find_map(|wrap| {
            let gift = gift_wrap::unwrap(keys, wrap).ok()?;
            (gift.rumor.clone().id().to_hex() == message_id).then_some(gift)
        }))
    }

    /// Identity-drift repair (ADR-0017 D5): discover the actual recipient of
    /// `message_id` across the project's agents (alias-set expanded). Tries
    /// every agent's local key until the wrap unwraps.
    async fn find_message_across_agents(
        &self,
        project_slug: &str,
        message_id: &str,
    ) -> anyhow::Result<Option<(UnwrappedGift, String)>> {
        let agents = self
            .list_agents(project_slug, ListAgentsOptions::default())
            .await?;
        for agent in agents {
            let Ok(keys) = self.agent_keys_for(project_slug, &agent.name) else {
                continue;
            };
            if let Some(gift) = self.find_message_for_keys(&keys, message_id).await? {
                return Ok(Some((gift, agent.name)));
            }
        }
        Ok(None)
    }

    /// ADR-0017 D5 authorization: the acker (whose own keys did NOT unwrap
    /// the wrap — otherwise case (a) already applied) must be
    /// (b) bound to the same pane as the recipient (herdr, best-effort), or
    /// (c) a participant of the message's thread — the sender of the
    /// message, or able to unwrap ANY wrap in the same thread.
    async fn authorize_ack(
        &self,
        project_slug: &str,
        acker_name: &str,
        recipient_name: &str,
        gift: &UnwrappedGift,
    ) -> anyhow::Result<()> {
        let acker_keys = self
            .agent_keys_for_resolved(project_slug, acker_name)
            .await?;
        if acker_keys.public_key() == gift.sender {
            return Ok(());
        }

        if let Some(plane) = self.plane.as_deref()
            && let Ok(agents) = plane.agent_list()
        {
            let pane_of = |name: &str| {
                agents
                    .iter()
                    .find(|a| a.name.as_deref() == Some(name))
                    .map(|a| a.pane_id.clone())
                    .unwrap_or_default()
            };
            let (acker_pane, recipient_pane) = (pane_of(acker_name), pane_of(recipient_name));
            if !acker_pane.is_empty() && acker_pane == recipient_pane {
                return Ok(());
            }
        }

        if let Some(thread_id) = Self::rumor_thread_id(&gift.rumor) {
            let acker_npub = acker_keys.public_key();
            let filter = ToronFilter::new()
                .kinds([crate::nostr::Kind::GiftWrap])
                .pubkey(acker_npub)
                .build();
            let events = super::collect_replay(self.bus.as_ref(), filter).await?;
            let in_thread = events.iter().any(|wrap| {
                gift_wrap::unwrap(&acker_keys, wrap)
                    .ok()
                    .map(|g| {
                        let rumor_id = g.rumor.clone().id().to_hex();
                        rumor_id == thread_id
                            || Self::rumor_thread_id(&g.rumor).as_ref() == Some(&thread_id)
                    })
                    .unwrap_or(false)
            });
            if in_thread {
                return Ok(());
            }
        }

        anyhow::bail!(
            "agent {acker_name} is not authorized to ack message {} (not the recipient, not pane-bound, not a thread participant)",
            gift.rumor.clone().id().to_hex()
        )
    }

    /// The thread id of a rumor: the literal `thread` tag, else the NIP-10
    /// `e` root tag (mirrors the projection's thread attribution).
    fn rumor_thread_id(rumor: &crate::nostr::UnsignedEvent) -> Option<String> {
        rumor.tags.iter().find_map(|t| {
            let s = t.as_slice();
            match s.first().map(String::as_str) {
                Some("thread") | Some("e") if s.len() > 1 => Some(s[1].to_string()),
                _ => None,
            }
        })
    }

    /// Publish a DM wrap (metadata-hiding: never adds project scoping tags).
    pub(crate) async fn publish_dm(&self, wrap: &Event) -> anyhow::Result<bool> {
        debug_assert!(
            is_metadata_hiding_kind(wrap.kind),
            "gift wraps must be metadata-hiding"
        );
        let ok = self.bus.publish(wrap.clone()).await?;
        Ok(ok.stored)
    }

    /// Resolve all recipient names → npubs via the project's kind-30013
    /// profiles (G2). Unknown name fails the whole send.
    ///
    /// ADR-0018 (swarm mode): routing targets are canonical names
    /// (`flywheel-<slug>-<role><seq>`), npubs, or folded legacy aliases
    /// (`agent_name_aliases` row → canonical). Anything else fails with an
    /// actionable error naming the grammar. Open mode keeps the legacy
    /// any-name resolution (AM-Rust parity).
    async fn resolve_recipients(
        &self,
        args: &SendMessage,
    ) -> anyhow::Result<Vec<ResolvedRecipient>> {
        let profiles = self.project_profiles(&args.project_slug).await?;
        let mode = self.project_identity_mode(&args.project_slug).await;
        let mut out: Vec<ResolvedRecipient> = Vec::new();
        for (names, role) in [
            (args.to.iter().collect::<Vec<_>>(), RecipientRole::To),
            (args.cc.iter().collect::<Vec<_>>(), RecipientRole::Cc),
            (args.bcc.iter().collect::<Vec<_>>(), RecipientRole::Bcc),
        ] {
            for name in names {
                if let Some(role_name) = name.strip_prefix("role:") {
                    if role_name.trim().is_empty() {
                        anyhow::bail!("role name must be non-empty after 'role:' (got {name:?})");
                    }
                    let pin = match self.role_pin(&args.project_slug, role_name).await {
                        Some(p) => p,
                        None => {
                            let available = self
                                .list_role_pins(&args.project_slug)
                                .await
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(r, _)| r)
                                .collect::<Vec<_>>()
                                .join(", ");
                            let avail_msg = if available.is_empty() {
                                "no roles registered".to_string()
                            } else {
                                format!("available roles: {available}")
                            };
                            anyhow::bail!(
                                "unknown role: {role_name} (register it with `toron role pin --project {} {role_name} <pin>`; {avail_msg})",
                                args.project_slug
                            );
                        }
                    };
                    let target = self.resolve_agent_name(&args.project_slug, &pin).await?;
                    let npub = nident::resolve_name(&args.project_slug, profiles.iter(), &target)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "unknown recipient for role {role_name}: pin {target} (register it in project {} first)",
                                args.project_slug
                            )
                        })?;
                    out.push(ResolvedRecipient {
                        name: target,
                        npub,
                        role,
                    });
                    continue;
                }
                let qualified = match nident::split_qualified(name) {
                    Some((pin, proj)) if self.project_has_agents(proj).await => Some((pin, proj)),
                    _ => None,
                };
                let (row_name, npub) = if let Some((pin, proj)) = qualified {
                    let canonical = self.canonical_project(proj).await;
                    let target = self.resolve_agent_name(&canonical, pin).await?;
                    let key = self
                        .pin_binding_in(&canonical, &target)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("unknown recipient: {target}@{canonical} (not registered in project {canonical})"))?;
                    (target, crate::nostr::PublicKey::from_hex(&key)?)
                } else if super::naming::is_npub(name) {
                    (
                        name.clone(),
                        crate::nostr::PublicKey::from_hex(name)
                            .map_err(|_| anyhow::anyhow!("invalid npub recipient: {name}"))?,
                    )
                } else {
                    let target = if mode == super::naming::IdentityMode::Swarm {
                        self.resolve_agent_name(&args.project_slug, name).await?
                    } else {
                        let canonical = self.canonical_project(&args.project_slug).await;
                        self.agent_name_alias(&canonical, name)
                            .await
                            .unwrap_or_else(|| name.clone())
                    };
                    let local = nident::resolve_name(&args.project_slug, profiles.iter(), &target);
                    let npub = self
                        .resolve_pin_key(&args.project_slug, &target, local)
                        .await?;
                    (target, npub)
                };
                out.push(ResolvedRecipient {
                    name: row_name,
                    npub,
                    role,
                });
            }
        }
        Ok(out)
    }

    /// ADR-0037 W3: a pin's `(project, pubkey)` bindings in every project
    /// (the `agents` index). Empty without an index pool.
    pub(crate) async fn pin_bindings(&self, name: &str) -> anyhow::Result<Vec<(String, String)>> {
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(
            sqlx::query_as("SELECT project_slug, pubkey FROM agents WHERE name = ?1")
                .bind(name)
                .fetch_all(pool)
                .await?,
        )
    }

    /// ADR-0037 W4: the key `name` is bound to in exactly `project_slug`.
    pub(crate) async fn pin_binding_in(
        &self,
        project_slug: &str,
        name: &str,
    ) -> anyhow::Result<Option<String>> {
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(None);
        };
        Ok(
            sqlx::query_scalar("SELECT pubkey FROM agents WHERE project_slug = ?1 AND name = ?2")
                .bind(project_slug)
                .bind(name)
                .fetch_optional(pool)
                .await?,
        )
    }

    /// ADR-0037 W4: does `project` (any spelling) have registrations? Only
    /// such a suffix makes `pin@project` a qualified address.
    pub(crate) async fn project_has_agents(&self, project: &str) -> bool {
        let Some(pool) = self.index_pool.as_ref() else {
            return false;
        };
        let canonical = self.canonical_project(project).await;
        sqlx::query_scalar::<_, i64>("SELECT 1 FROM agents WHERE project_slug = ?1 LIMIT 1")
            .bind(&canonical)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    /// ADR-0037 W3: resolve a (folded) recipient name to its ONE key across
    /// every project. One key anywhere sends; a name registered nowhere
    /// fails loud; a legacy split refuses and names the `pin@project`
    /// forms. `local` is the sender project's profile binding, counted so a
    /// pool-less mailbox still routes in-project.
    pub(crate) async fn resolve_pin_key(
        &self,
        project_slug: &str,
        target: &str,
        local: Option<crate::nostr::PublicKey>,
    ) -> anyhow::Result<crate::nostr::PublicKey> {
        let mut binds = self.pin_bindings(target).await?;
        if let Some(k) = local {
            binds.push((self.canonical_project(project_slug).await, k.to_hex()));
        }
        match nident::pin_keys(&binds) {
            nident::PinKeys::One(hex) => Ok(crate::nostr::PublicKey::from_hex(&hex)?),
            nident::PinKeys::Unknown => Err(anyhow::anyhow!(
                "unknown recipient: {target} (register it in project {project_slug} first)"
            )),
            nident::PinKeys::Split(split) => Err(anyhow::anyhow!(
                "{}",
                nident::split_pin_refusal(target, &split)
            )),
        }
    }

    /// Load the sender/agent's keypair from its ncryptsec file (G1).
    ///
    /// bd-cmgu: the flywheel CLI keys projects by their ABSOLUTE PATH slug
    /// (`/Users/.../workspace/toron`) while the MCP surface uses the short
    /// key (`toron`) — two roots under one data dir. On a primary miss the
    /// lookup falls back to the basename-matched alternate root (both
    /// directions), so an agent registered via one surface is usable from
    /// the other with either project identifier.
    pub(crate) fn agent_keys_for(&self, project_slug: &str, name: &str) -> anyhow::Result<Keys> {
        let path = self.agent_key_path(project_slug, name);
        if let Some(k) = self.load_agent_key(&path) {
            return Ok(k);
        }
        if let Some(k) = self.alt_agent_key(project_slug, name) {
            return Ok(k);
        }
        Err(anyhow::anyhow!(
            "agent {name} has no local key in project {project_slug} (create_agent_identity or register first)"
        ))
    }

    /// bd-pin-public-plane-s-easy.1: `agent_keys_for` through the ONE name
    /// resolver — a SLUG folds to its PIN's key (`flywheel-toron-coder` →
    /// `BlueRidge`), so acting as the slug loads the pin key. The sync
    /// `agent_keys_for` above stays the exact-name primitive for callers
    /// that already hold the registered name (build slots, contacts,
    /// reservations pass pins under the new convention); every messaging
    /// entry that accepts a user-supplied name routes through here.
    pub(crate) async fn agent_keys_for_resolved(
        &self,
        project_slug: &str,
        name: &str,
    ) -> anyhow::Result<Keys> {
        let canonical = self.canonical_project(project_slug).await;
        let resolved = self.resolve_agent_name(&canonical, name).await?;
        self.agent_keys_for(&canonical, &resolved)
    }

    /// Canonical hex pubkey for `name` in `project_slug`, or `None` when the
    /// agent holds no local key here (bd-guard-holder-identity-fold-dqog).
    ///
    /// Why this is public: the strict sweep-guard must attribute a commit to
    /// the SAME identity `reserve acquire --as <name>` stamped its lease
    /// with. Acquire resolves a name through LOCAL KEY MATERIAL
    /// ([`Self::agent_keys_for`], never the roster), so the guard resolves it
    /// the same way — a name→pubkey map that survives a roster read failure.
    /// The alias-resolving [`Self::agent_keys_for_resolved`] comes first (a
    /// slug acts as its pin), then the exact-name primitive as the fallback.
    ///
    /// Fail-closed: no local key = `None`. Never a guess, never a wildcard.
    pub async fn agent_pubkey_hex(&self, project_slug: &str, name: &str) -> Option<String> {
        if let Ok(keys) = self.agent_keys_for_resolved(project_slug, name).await {
            return Some(keys.public_key().to_hex());
        }
        let canonical = self.canonical_project(project_slug).await;
        self.agent_keys_for(&canonical, name)
            .ok()
            .map(|keys| keys.public_key().to_hex())
    }

    /// Per-message NIP-17 wrap idempotency cache (not assignment SLA).
    fn receipt_time(&self, project: &str, agent: &str, id: &EventId, kind: &str) -> Option<i64> {
        self.wrap_times()
            .lock()
            .get(&receipt_key(project, agent, id, kind))
            .copied()
    }

    fn record_receipt(&self, project: &str, agent: &str, id: &EventId, kind: &str, ts_us: i64) {
        self.wrap_times()
            .lock()
            .insert(receipt_key(project, agent, id, kind), ts_us);
    }

    /// bd-5icg: write the per-recipient read receipt into the `message_reads`
    /// projection (both `mark_message_read` and `acknowledge_message` land
    /// here — an ack implies the message was read). Projection state only:
    /// never written into Nostr events; lost on index rebuild (documented).
    /// Best-effort: with no index pool attached the write is skipped (the
    /// NIP-17 receipt + in-memory idempotency cache still work).
    async fn record_read_state(
        &self,
        project_slug: &str,
        recipient_npub: PublicKey,
        message_id: &EventId,
        ts_us: i64,
        acked: bool,
    ) {
        let Some(pool) = self.index_pool.as_ref() else {
            return;
        };
        if let Err(e) = sqlx::query(
            "INSERT INTO message_reads (project_slug, message_id, recipient, read_ts_us, acked) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT (message_id, recipient) DO UPDATE SET \
             acked = MAX(message_reads.acked, excluded.acked)",
        )
        .bind(project_slug)
        .bind(message_id.to_hex())
        .bind(recipient_npub.to_hex())
        .bind(ts_us)
        .bind(acked)
        .execute(pool)
        .await
        {
            tracing::warn!("read-state write failed (degrade): {e}");
        }
    }

    fn wrap_times(&self) -> &parking_lot::Mutex<HashMap<String, i64>> {
        &self.wrap_idempotency
    }
}

/// Build the visible rumor (to+cc only; bcc NEVER appears on it).
///
/// Tag construction is infallible for toron-controlled inputs; any failure
/// here is a programming error surfaced via `expect` on a known-static tag
/// shape (kept narrow — only the recipient `p` tag, which is npub-derived).
/// Build every rumor for one send: the visible rumor (to/cc) plus one
/// variant per bcc, each with its own event id.
///
/// `created_at` is the rumor's timestamp (unix seconds) — part of the event
/// id, so a retry that re-publishes an unfinished claim rebuilds the SAME
/// event id from the stored timestamp (bd-ff23). One send, one timestamp.
fn build_pending_rumors(
    sender_keys: &Keys,
    args: &SendMessage,
    resolved: &[ResolvedRecipient],
    created_at: u64,
) -> (Vec<(ResolvedRecipient, UnsignedEvent, String)>, String) {
    let mut visible = build_visible_rumor(&sender_keys.public_key(), args, resolved, created_at);
    let visible_id = visible.id().to_hex();
    let mut pending: Vec<(ResolvedRecipient, UnsignedEvent, String)> = Vec::new();
    for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
        let mut rumor = visible.clone();
        let id_hex = rumor.id().to_hex();
        pending.push((r.clone(), rumor, id_hex));
    }
    for r in resolved.iter().filter(|r| r.role == RecipientRole::Bcc) {
        let mut variant = build_bcc_rumor(&sender_keys.public_key(), args, resolved, r, created_at);
        let id_hex = variant.id().to_hex();
        pending.push((r.clone(), variant, id_hex));
    }
    (pending, visible_id)
}

fn build_visible_rumor(
    author: &PublicKey,
    args: &SendMessage,
    resolved: &[ResolvedRecipient],
    created_at: u64,
) -> UnsignedEvent {
    let mut tags: Vec<Tag> = Vec::new();
    for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
        tags.push(
            Tag::parse(["p", &r.npub.to_hex(), "", r.role.tag_value()])
                .expect("static p tag shape (npub hex + role literal)"),
        );
    }
    add_message_tags(&mut tags, args);
    gift_wrap::build_rumor(
        *author,
        Timestamp::from_secs(created_at),
        crate::nostr::Kind::PrivateDirectMessage,
        tags,
        args.body_md.clone(),
    )
}

/// Build the bcc-variant rumor: to+cc+THIS bcc only (other bccs invisible).
fn build_bcc_rumor(
    author: &PublicKey,
    args: &SendMessage,
    resolved: &[ResolvedRecipient],
    bcc: &ResolvedRecipient,
    created_at: u64,
) -> UnsignedEvent {
    let mut tags: Vec<Tag> = Vec::new();
    for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
        tags.push(
            Tag::parse(["p", &r.npub.to_hex(), "", r.role.tag_value()])
                .expect("static p tag shape (npub hex + role literal)"),
        );
    }
    tags.push(
        Tag::parse(["p", &bcc.npub.to_hex(), "", "bcc"])
            .expect("static bcc p tag shape (npub hex)"),
    );
    add_message_tags(&mut tags, args);
    gift_wrap::build_rumor(
        *author,
        Timestamp::from_secs(created_at),
        crate::nostr::Kind::PrivateDirectMessage,
        tags,
        args.body_md.clone(),
    )
}

/// Add subject (NIP-14), importance/ack_required/sender_token/thread (G3) tags.
///
/// All tag strings here are either toron-controlled literals or user input
/// that nostr-sdk validates. A parse failure on user input (e.g. an
/// over-long subject) is silently dropped rather than panicking — the message
/// still sends, just without that tag. This matches AM-Rust's tolerant
/// handling of optional metadata.
fn add_message_tags(tags: &mut Vec<Tag>, args: &SendMessage) {
    let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
    if let Ok(t) = Tag::parse(["ts_ms", &now_us.to_string()]) {
        tags.push(t);
    }
    if !args.project_slug.is_empty()
        && let Ok(t) = Tag::parse(["t", &args.project_slug])
    {
        tags.push(t);
    }
    if !args.subject.is_empty()
        && let Ok(t) = Tag::parse(["subject", &args.subject])
    {
        tags.push(t);
    }
    if let Some(imp) = &args.importance
        && let Ok(t) = Tag::parse(["importance", imp])
    {
        tags.push(t);
    }
    if args.ack_required
        && let Ok(t) = Tag::parse(["ack-required"])
    {
        tags.push(t);
    }
    if let Some(token) = &args.sender_token
        && let Ok(t) = Tag::parse(["sender-token", token])
    {
        tags.push(t);
    }
    if args.overseer
        && let Ok(t) = Tag::parse(["t", OVERSEER_TAG])
    {
        tags.push(t);
    }
    tags.extend(args.extra_tags.iter().cloned());
    if let Some(thread) = &args.thread_id {
        if let Ok(t) = Tag::parse(["thread", thread]) {
            tags.push(t);
        }
        if let Ok(root) = EventId::from_hex(thread)
            && let Ok(t) = Tag::parse(["e", &root.to_hex(), "", "root"])
        {
            tags.push(t);
        }
    }
}

/// Project an unwrapped mail into the inbox view.
/// A delivered DM as a `request@1` envelope for the reader's inbox log
/// (bd-next-envelope-ingest-yl0cx). Producer and sender are the author's
/// npub (the log's identity rule); the reader rides `to`; the project is
/// the context. Request (not notify): the per-message door ends each mail
/// with an ack, and only obligation classes admit one — `allowed_now`
/// already suggests `toron ack` for a delivered request. Pure and total:
/// empty subject+body still yields a valid envelope (nothing delivered
/// stays invisible).
fn mail_envelope(
    project: &str,
    sender_hex: &str,
    reader_hex: &str,
    subject: &str,
    body_md: &str,
) -> crate::envelope::Envelope {
    use crate::envelope::{Class, ContextId, ContractId, ENVELOPE_V, Envelope, MsgId, Recipient};
    let mut text = String::new();
    if !subject.trim().is_empty() {
        text.push_str(subject.trim());
    }
    if !body_md.trim().is_empty() {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(body_md.trim());
    }
    if text.is_empty() {
        text.push_str("(no content)");
    }
    Envelope {
        v: ENVELOPE_V,
        msg: MsgId {
            producer: sender_hex.into(),
            epoch: 0,
            seq: 0,
        },
        from: sender_hex.into(),
        to: Recipient::Npub(reader_hex.into()),
        class: Class::Notify,
        contract: ContractId::new("notify", 1),
        context: ContextId(project.into()),
        task: None,
        generation: 0,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({ "text": text }),
    }
}

fn inbox_from(mail: UnwrappedMail) -> Option<InboxMessage> {
    use std::str::FromStr;
    let tags = &mail.rumor.tags;
    let subject = tag_value(tags, "subject").unwrap_or_default().to_string();
    let importance = tag_value(tags, "importance")
        .unwrap_or("normal")
        .to_string();
    let ack_required = tag_has_flag(tags, "ack-required");
    let thread_id = tag_value(tags, "thread")
        .map(ToString::to_string)
        .or_else(|| {
            tags.iter().find_map(|t| {
                let parts = t.as_slice();
                (parts.first().map(String::as_str) == Some("e"))
                    .then(|| parts.get(1).map(ToString::to_string))
                    .flatten()
            })
        });
    let corr = match tag_value(tags, "corr").map(Corr::from_str) {
        Some(Err(e)) => {
            tracing::warn!(
                rumor_id = %mail.rumor_id.to_hex(),
                sender = %mail.sender.to_hex(),
                "dropping inbox row with corrupt corr tag ({e}); mail preserved in archive"
            );
            return None;
        }
        Some(Ok(corr)) => Some(corr),
        None => None,
    };
    Some(InboxMessage {
        id: mail.rumor_id.to_hex(),
        sender: mail.sender.to_hex(),
        subject,
        body_md: mail.rumor.content.clone(),
        importance,
        ack_required,
        thread_id,
        created_ts_us: mail.ts_us(),
        read_ts_us: None,
        duplicate_of: None,
        corr,
        wake: tag_value(tags, "wake").map(ToString::to_string),
        priority: tag_value(tags, "priority").map(ToString::to_string),
        audience: tag_value(tags, "audience").map(ToString::to_string),
        result_path: tag_value(tags, "result_path").map(ToString::to_string),
        verdict: tag_value(tags, "verdict").map(ToString::to_string),
        bead: tag_value(tags, "bead").map(ToString::to_string),
    })
}

/// S5 (bd-dup-assign-collapse-ws21): projection-level duplicate collapse.
///
/// Messages with the same (sender, subject, body-hash) within the collapse
/// window collapse into ONE inbox row. The surviving row is the LATEST copy,
/// marked `duplicate_of: Some(<first-id>)`; earlier identical copies are
/// dropped from the projection (they still exist in the archive/event
/// stream — this is projection-only, honest for E2E gift-wrap where a
/// re-sent wrap is a genuinely new event).
///
/// The window is measured from the FIRST occurrence: an identical message
/// arriving 4 days later is NOT collapsed (it is a fresh, not a duplicate —
/// staleness handling is flywheel's wake-side concern, S8).
pub fn collapse_duplicates(messages: Vec<InboxMessage>) -> Vec<InboxMessage> {
    use std::collections::HashMap;
    let mut groups: HashMap<(String, String, String), (String, i64, usize)> = HashMap::new();
    let mut out: Vec<InboxMessage> = Vec::with_capacity(messages.len());
    for m in messages {
        let body_hash = sha2::Sha256::digest(m.body_md.as_bytes());
        let hash_hex = format!("{body_hash:x}");
        let key = (m.sender.clone(), m.subject.clone(), hash_hex);
        match groups.get(&key) {
            Some((first_id, first_ts, idx))
                if m.created_ts_us - first_ts <= DUP_COLLAPSE_WINDOW_US =>
            {
                let mut survivor = m;
                survivor.duplicate_of = Some(first_id.clone());
                out[*idx] = survivor;
            }
            _ => {
                out.push(m.clone());
                groups.insert(key, (m.id, m.created_ts_us, out.len() - 1));
            }
        }
    }
    out
}

/// True when a flag tag (no value) is present, e.g. `["ack-required"]`.
fn tag_has_flag(tags: &crate::nostr::Tags, name: &str) -> bool {
    tags.iter()
        .any(|t| t.as_slice().first().map(String::as_str) == Some(name))
}

/// Cache key for a receipt: `project:agent:kind:event_id`.
fn receipt_key(project: &str, agent: &str, id: &EventId, kind: &str) -> String {
    format!("{project}:{agent}:{kind}:{id}")
}

/// bd-ejp4: the dedupe key for an ack-sla wake — the SORTED, DEDUPED audience.
///
/// Sorted so recipient order never splits one audience into two keys, and
/// hashed rather than stored raw because this is a join key, not data anyone
/// reads. An empty audience still produces a stable key: that is the
/// un-addressed case, and it must dedupe like any other rather than becoming an
/// unkeyed exemption.
fn ack_sla_wake_audience_key(audience: &[String]) -> String {
    let mut sorted: Vec<&str> = audience.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let joined = sorted.join(",");
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    use std::hash::{Hash, Hasher};
    joined.hash(&mut hasher);
    format!("{joined}#{:016x}", hasher.finish())
}

/// bd-4v1a: the reminder obligation of a post is the CONVERSATION it belongs
/// to. A thread that re-posts one request every two minutes is ONE
/// obligation, not 714 (measured live: 714 posts in one thread, 714
/// reminders per SLA window). A post with no thread is its own obligation.
fn ack_sla_scope_id(thread_id: Option<&str>, post_id: &str) -> String {
    match thread_id {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => post_id.to_string(),
    }
}

/// Verify a gift wrap unwraps to the expected rumor content (acceptance
/// helper — also used by tests).
pub fn unwrap_for_recipient(
    recipient: &Keys,
    wrap: &Event,
) -> Result<UnwrappedGift, gift_wrap::GiftWrapError> {
    gift_wrap::unwrap(recipient, wrap)
}

/// Compile-time guarantee: a send with `to=["Unknown"]` never produces a wrap.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::mailbox::search::SearchMessages;
    use crate::nostr::identity::AgentProfile;
    use std::sync::Arc;

    /// bd-j5sk + ADR-0037 W3: the send path resolves a name to ONE key and
    /// REFUSES a split pin; it never warns-and-sends.
    ///
    /// A source tripwire, because the property is about the call SITE: the
    /// pure `pin_keys` / `split_pin_refusal` have their own tests, and those
    /// pass whether the sender obeys them or not. Needles are spliced so a
    /// comment cannot satisfy them.
    #[test]
    fn the_send_path_refuses_a_split_pin() {
        // Whitespace-free so rustfmt's line breaking cannot hide the call.
        let src: String = include_str!("messaging.rs")
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let call = ["self.resolve_pin_key", "(&args.project_slug"].concat();
        assert!(
            src.contains(&call),
            "resolve_recipients routes names through resolve_pin_key"
        );
        let split = ["nident::PinKeys::", "Split(split)=>"].concat();
        let i = src.find(&split).expect("the split arm exists");
        let rest = &src[i..];
        let branch = &rest[..rest.find("///").expect("the split arm closes")];
        assert!(
            branch.contains("Err("),
            "the split arm must REFUSE: {branch}"
        );
        assert!(
            !branch.contains("eprintln!"),
            "a warning is the bd-j5sk defect itself"
        );
        let old = ["ambiguous_name", "_warning"].concat();
        assert!(!src.contains(&old), "the warning is deleted (ADR-0037 W3)");
    }

    const PASSPHRASE: &str = "messaging-test-pass";

    /// NIP-10: a hex thread_id must emit a marked `e` tag (`root`), not a
    /// positional `["e", id]` pair.
    #[test]
    fn reply_hex_thread_emits_marked_nip10_root_e_tag() {
        let mut tags = Vec::new();
        let args = SendMessage {
            thread_id: Some(
                "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b".into(),
            ),
            subject: "reply".into(),
            ..Default::default()
        };
        add_message_tags(&mut tags, &args);
        let e = tags
            .iter()
            .find(|t| t.as_slice().first().map(String::as_str) == Some("e"))
            .expect("e tag");
        let parts = e.as_slice();
        assert_eq!(
            parts.get(3).map(String::as_str),
            Some("root"),
            "NIP-10 marker must be root, tags={parts:?}"
        );
        assert_eq!(
            parts.get(1).map(String::as_str),
            Some("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
        );
    }

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-msg-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-msg-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE);
        (mailbox, db_path, data_dir)
    }

    /// ADR-0018: mailbox with a migrated index pool (mode + alias tables).
    async fn test_mailbox_with_pool(
        tag: &str,
    ) -> (
        Mailbox,
        sqlx::SqlitePool,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-msg-pool-{tag}-{}.sqlite",
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
            "toron-msg-pool-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool.clone());
        (mailbox, pool, db_path, data_dir)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        }
    }

    async fn register(mailbox: &Mailbox, project: &str, name: &str) -> (Keys, PublicKey) {
        let agent = mailbox
            .register_agent(project, &profile(name))
            .await
            .expect("register");
        let keys = mailbox
            .load_agent_key(std::path::Path::new(&agent.key_path))
            .expect("load key");
        let npub = PublicKey::from_hex(&agent.npub).expect("npub");
        (keys, npub)
    }

    /// Drain gift-wrap events addressed to `npub` from the bus.
    async fn wraps_to(mailbox: &Mailbox, npub: &PublicKey, limit: usize) -> Vec<Event> {
        let filter = ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(*npub)
            .build();
        let events = super::super::collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("collect");
        events.into_iter().take(limit).collect()
    }

    /// S5 (bd-dup-assign-collapse-ws21): projection-level duplicate collapse.
    /// Identical (sender, subject, body-hash) within the window collapse into
    /// one row carrying `duplicate_of: <first-id>`; outside the window they
    /// stay separate.
    #[test]
    fn collapse_duplicates_within_window_marks_duplicate_of() {
        let mk = |id: &str, sender: &str, subject: &str, body: &str, ts: i64| InboxMessage {
            id: id.into(),
            sender: sender.into(),
            subject: subject.into(),
            body_md: body.into(),
            importance: "normal".into(),
            ack_required: true,
            thread_id: None,
            created_ts_us: ts,
            read_ts_us: None,
            duplicate_of: None,
            corr: None,
            wake: None,
            priority: None,
            audience: None,
            result_path: None,
            verdict: None,
            bead: None,
        };

        let first = mk(
            "first",
            "PiAlpha",
            "assign",
            "build it",
            1_700_000_000_000_000,
        );
        let dup = mk(
            "dup",
            "PiAlpha",
            "assign",
            "build it",
            1_700_000_000_010_000,
        );
        let collapsed = collapse_duplicates(vec![first.clone(), dup]);
        assert_eq!(collapsed.len(), 1, "duplicates collapse to one row");
        assert_eq!(collapsed[0].id, "dup", "latest copy survives");
        assert_eq!(
            collapsed[0].duplicate_of.as_deref(),
            Some("first"),
            "survivor carries duplicate_of = first id"
        );

        let old = mk(
            "old",
            "PiAlpha",
            "assign",
            "build it",
            1_700_000_000_000_000,
        );
        let late = mk(
            "late",
            "PiAlpha",
            "assign",
            "build it",
            1_700_007_200_000_000,
        );
        let kept = collapse_duplicates(vec![old, late]);
        assert_eq!(kept.len(), 2, "outside window stays separate");
        assert!(kept.iter().all(|m| m.duplicate_of.is_none()));

        let a = mk("a", "PiAlpha", "assign", "build it", 1_700_000_000_000_000);
        let b = mk(
            "b",
            "PiAlpha",
            "assign",
            "build other",
            1_700_000_000_010_000,
        );
        let kept2 = collapse_duplicates(vec![a, b]);
        assert_eq!(kept2.len(), 2, "different body is not a duplicate");

        let c = mk("c", "Beta", "assign", "build it", 1_700_000_000_000_000);
        let kept3 = collapse_duplicates(vec![first, c]);
        assert_eq!(kept3.len(), 2, "different sender is not a duplicate");
    }

    /// S5 integration: two identical sends land as ONE inbox row with
    /// `duplicate_of`; a third with different body stays separate.
    #[tokio::test]
    async fn re_register_after_key_delete_converges_identity_bd_0pcd() {
        let (mailbox, db_path, data_dir) = test_mailbox("npub-rot").await;
        let (_sender_keys, _sender_npub) = register(&mailbox, "demo", "GreenCastle").await;
        let (_k1, old_npub) = register(&mailbox, "demo", "BlueLake").await;

        let key_dir = mailbox
            .agent_key_path("demo", "BlueLake")
            .parent()
            .expect("key dir")
            .to_path_buf();
        std::fs::remove_dir_all(&key_dir).expect("wipe agents/<name>");

        let re = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("re-register");
        let new_npub = PublicKey::from_hex(&re.npub).expect("npub");
        assert_ne!(
            new_npub, old_npub,
            "fresh key material derives a fresh npub"
        );

        let w = mailbox.whois("demo", "BlueLake").await.expect("whois");
        assert_eq!(
            PublicKey::from_hex(&w.npub).expect("whois npub"),
            new_npub,
            "whois must serve the NEW npub, not the stale profile event"
        );

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "ping".into(),
            body_md: "after rotation".into(),
            ..Default::default()
        };
        let sent = mailbox.send_message(&args).await.expect("send");
        assert_eq!(sent.count, 1, "send claims one delivery");

        let wraps = wraps_to(&mailbox, &new_npub, 1).await;
        assert_eq!(wraps.len(), 1, "gift-wrap addressed to the NEW npub");
        let keys_now = mailbox
            .load_agent_key(std::path::Path::new(&re.key_path))
            .expect("reload on-disk key");
        unwrap_for_recipient(&keys_now, &wraps[0]).expect("unwrap with on-disk key");

        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                include_bodies: true,
                ..Default::default()
            })
            .await
            .expect("inbox");
        assert_eq!(
            inbox.len(),
            1,
            "inbox must fill after key rotation + re-register"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-asbc: `limit` must be a newest-N window, not an oldest-N head.
    ///
    /// The list arrives from `order_by_rumor_time` in ASCENDING rumor-time
    /// order, so a `.take(n)` over it keeps the OLDEST n and structurally
    /// drops whatever just arrived. That is invisible on a short inbox and
    /// silently fatal for any consumer polling an inbox for something new.
    ///
    /// The returned rows stay ascending: the limit narrows the window, it does
    /// not reorder it. A caller that renders the inbox or walks it in order
    /// must not see a different order just because it asked for fewer rows.
    #[tokio::test]
    async fn fetch_inbox_limit_keeps_the_newest_rows_still_ascending() {
        let (mailbox, db_path, data_dir) = test_mailbox("limitnewest").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_recv, _npub) = register(&mailbox, "demo", "BlueLake").await;

        for i in 1..=4 {
            mailbox
                .send_message(&SendMessage {
                    project_slug: "demo".into(),
                    sender_name: "GreenCastle".into(),
                    to: vec!["BlueLake".into()],
                    subject: format!("m{i}"),
                    body_md: format!("body {i}"),
                    ..Default::default()
                })
                .await
                .unwrap_or_else(|e| panic!("send {i}: {e}"));
        }

        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .expect("inbox");

        let subjects: Vec<&str> = inbox.iter().map(|m| m.subject.as_str()).collect();
        assert_eq!(
            subjects,
            vec!["m3", "m4"],
            "limit keeps the NEWEST rows, and still returns them ascending"
        );

        let all = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                limit: None,
                ..Default::default()
            })
            .await
            .expect("inbox unlimited");
        assert_eq!(all.len(), 4, "no limit returns every row");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn fetch_inbox_collapses_duplicate_assigns() {
        let (mailbox, db_path, data_dir) = test_mailbox("dup").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_recv, _npub) = register(&mailbox, "demo", "BlueLake").await;

        let send = |body: &str| SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "assign".into(),
            body_md: body.into(),
            ..Default::default()
        };

        mailbox
            .send_message(&send("build it"))
            .await
            .expect("send 1");
        mailbox
            .send_message(&send("build it"))
            .await
            .expect("send 2 (dup)");
        mailbox
            .send_message(&send("build other"))
            .await
            .expect("send 3 (distinct)");

        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                include_bodies: true,
                ..Default::default()
            })
            .await
            .expect("inbox");
        assert_eq!(inbox.len(), 2, "duplicate collapses to one row");
        let dupes: Vec<&InboxMessage> = inbox
            .iter()
            .filter(|m| m.subject == "assign" && m.body_md == "build it")
            .collect();
        assert_eq!(dupes.len(), 1, "identical assigns collapsed");
        assert!(
            dupes[0].duplicate_of.is_some(),
            "survivor marked with duplicate_of = first id"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-since-cursor-70cf: `--since last` reads the durable high-water,
    /// returns only newer messages, then bumps the cursor — so a
    /// crashed/re-spawned agent resumes exactly where the last poll stopped.
    #[tokio::test]
    async fn since_cursor_last_persists_across_reads() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("sincecursor").await;
        let (_s, _) = register(&mailbox, "demo", "SenderSC").await;
        let (_r, _) = register(&mailbox, "demo", "ReaderSC").await;

        let send = |subject: &str| SendMessage {
            project_slug: "demo".into(),
            sender_name: "SenderSC".into(),
            to: vec!["ReaderSC".into()],
            subject: subject.into(),
            body_md: "cursor probe".into(),
            ..Default::default()
        };
        let fi = FetchInbox {
            project_slug: "demo".into(),
            agent_name: "ReaderSC".into(),
            since_ts: Some("last".into()),
            ..Default::default()
        };

        mailbox
            .send_message(&send("first"))
            .await
            .expect("send first");
        let cold = mailbox.fetch_inbox(&fi).await.expect("cold since last");
        assert_eq!(cold.len(), 1, "cold cursor = 0: the first message is new");

        let none = mailbox.fetch_inbox(&fi).await.expect("re-poll");
        assert!(none.is_empty(), "re-poll with no new mail is empty");

        mailbox
            .send_message(&send("second"))
            .await
            .expect("send second");
        let warm = mailbox.fetch_inbox(&fi).await.expect("warm since last");
        assert_eq!(warm.len(), 1, "only the message after the cursor");
        assert_eq!(warm[0].subject, "second");

        let past = FetchInbox {
            project_slug: "demo".into(),
            agent_name: "ReaderSC".into(),
            since_ts: Some("2000-01-01T00:00:00Z".into()),
            ..Default::default()
        };
        assert!(
            !mailbox
                .fetch_inbox(&past)
                .await
                .expect("rfc3339")
                .is_empty(),
            "explicit RFC3339 path unchanged"
        );
        let bad = FetchInbox {
            project_slug: "demo".into(),
            agent_name: "ReaderSC".into(),
            since_ts: Some("garbage".into()),
            ..Default::default()
        };
        assert!(mailbox.fetch_inbox(&bad).await.is_err(), "malformed errors");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_send_n_recipients_n_wraps_same_rumor() {
        let (mailbox, db_path, data_dir) = test_mailbox("fanout").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv_a, npub_a) = register(&mailbox, "demo", "BlueLake").await;
        let (recv_b, npub_b) = register(&mailbox, "demo", "RoseDune").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into(), "RoseDune".into()],
            subject: "review".into(),
            body_md: "please review".into(),
            ..Default::default()
        };
        let result = mailbox.send_message(&args).await.expect("send");
        assert_eq!(result.count, 2, "2 recipients → 2 wraps");
        assert!(result.verified_sender);

        let wraps_a = wraps_to(&mailbox, &npub_a, 2).await;
        let wraps_b = wraps_to(&mailbox, &npub_b, 2).await;
        assert_eq!(wraps_a.len(), 1);
        assert_eq!(wraps_b.len(), 1);
        let rumor_a = unwrap_for_recipient(&recv_a, &wraps_a[0])
            .expect("unwrap a")
            .rumor;
        let rumor_b = unwrap_for_recipient(&recv_b, &wraps_b[0])
            .expect("unwrap b")
            .rumor;
        assert_eq!(rumor_a.content, "please review");
        assert_eq!(rumor_b.content, "please review");
        assert_eq!(rumor_a.kind, crate::nostr::Kind::PrivateDirectMessage);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn overseer_send_signs_with_operator_and_tags() {
        let (mailbox, db_path, data_dir) = test_mailbox("overseer").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;
        let operator_npub = mailbox.operator().expect("operator").public_key();

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: String::new(),
            to: vec!["BlueLake".into()],
            subject: "Operator override".into(),
            body_md: "handle this".into(),
            importance: Some("high".into()),
            overseer: true,
            ..Default::default()
        };
        let result = mailbox.send_message(&args).await.expect("overseer send");
        assert_eq!(result.count, 1, "delivered to the recipient");

        let wraps = wraps_to(&mailbox, &npub, 2).await;
        assert_eq!(wraps.len(), 1, "one wrap for the recipient");
        let rumor = unwrap_for_recipient(&recv, &wraps[0])
            .expect("unwrap")
            .rumor;
        assert_eq!(
            rumor.pubkey, operator_npub,
            "overseer rumor is signed by the operator key"
        );
        let has_overseer = rumor.tags.iter().any(|t| {
            t.as_slice().first().map(String::as_str) == Some("t")
                && t.as_slice().get(1).map(String::as_str) == Some(OVERSEER_TAG)
        });
        assert!(has_overseer, "rumor carries the toron:overseer tag");
        let has_high = rumor.tags.iter().any(|t| {
            t.as_slice().first().map(String::as_str) == Some("importance")
                && t.as_slice().get(1).map(String::as_str) == Some("high")
        });
        assert!(has_high, "rumor carries importance=high");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_bcc_variant_privacy() {
        let (mailbox, db_path, data_dir) = test_mailbox("bcc").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv_to, npub_to) = register(&mailbox, "demo", "BlueLake").await;
        let (recv_cc, npub_cc) = register(&mailbox, "demo", "RoseDune").await;
        let (recv_bcc1, npub_b1) = register(&mailbox, "demo", "CoralFinch").await;
        let (recv_bcc2, npub_b2) = register(&mailbox, "demo", "DustySparrow").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            cc: vec!["RoseDune".into()],
            bcc: vec!["CoralFinch".into(), "DustySparrow".into()],
            subject: "bcc test".into(),
            body_md: "secret".into(),
            ..Default::default()
        };
        let result = mailbox.send_message(&args).await.expect("send");
        assert_eq!(result.count, 4, "to(1)+cc(1)+bcc(2) = 4 wraps");

        let to_wrap = &wraps_to(&mailbox, &npub_to, 2).await[0];
        let cc_wrap = &wraps_to(&mailbox, &npub_cc, 2).await[0];
        let b1_wrap = &wraps_to(&mailbox, &npub_b1, 2).await[0];
        let b2_wrap = &wraps_to(&mailbox, &npub_b2, 2).await[0];

        let p_npubs = |gift: &UnwrappedGift| -> Vec<String> {
            gift.rumor
                .tags
                .iter()
                .filter(|t| t.as_slice().first().map(String::as_str) == Some("p"))
                .map(|t| t.as_slice()[1].clone())
                .collect()
        };

        let to_gift = unwrap_for_recipient(&recv_to, to_wrap).expect("to");
        let to_ps = p_npubs(&to_gift);
        assert!(to_ps.contains(&npub_to.to_hex()));
        assert!(to_ps.contains(&npub_cc.to_hex()));
        assert!(!to_ps.contains(&npub_b1.to_hex()), "to cannot see bcc1");
        assert!(!to_ps.contains(&npub_b2.to_hex()), "to cannot see bcc2");

        let cc_gift = unwrap_for_recipient(&recv_cc, cc_wrap).expect("cc");
        let cc_ps = p_npubs(&cc_gift);
        assert!(cc_ps.contains(&npub_to.to_hex()));
        assert!(cc_ps.contains(&npub_cc.to_hex()));
        assert!(!cc_ps.contains(&npub_b1.to_hex()), "cc cannot see bcc1");
        assert!(!cc_ps.contains(&npub_b2.to_hex()), "cc cannot see bcc2");

        let b1_gift = unwrap_for_recipient(&recv_bcc1, b1_wrap).expect("b1");
        let b1_ps = p_npubs(&b1_gift);
        assert!(b1_ps.contains(&npub_to.to_hex()));
        assert!(b1_ps.contains(&npub_cc.to_hex()));
        assert!(b1_ps.contains(&npub_b1.to_hex()), "b1 sees itself");
        assert!(!b1_ps.contains(&npub_b2.to_hex()), "b1 cannot see bcc2");

        let b2_gift = unwrap_for_recipient(&recv_bcc2, b2_wrap).expect("b2");
        let b2_ps = p_npubs(&b2_gift);
        assert!(b2_ps.contains(&npub_b2.to_hex()));
        assert!(!b2_ps.contains(&npub_b1.to_hex()), "b2 cannot see bcc1");

        let outsider = crate::keys::generate();
        assert!(unwrap_for_recipient(&outsider, to_wrap).is_err());
        assert!(unwrap_for_recipient(&outsider, b1_wrap).is_err());
        assert!(unwrap_for_recipient(&outsider, b2_wrap).is_err());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_unknown_recipient_aborts_whole_send() {
        let (mailbox, db_path, data_dir) = test_mailbox("unknown").await;
        register(&mailbox, "demo", "GreenCastle").await;
        register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into(), "Ghost".into()],
            subject: "x".into(),
            body_md: "y".into(),
            ..Default::default()
        };
        let err = mailbox.send_message(&args).await.expect_err("unknown name");
        assert!(err.to_string().contains("unknown recipient"), "{err}");
        let (_, npub) = {
            let p = mailbox.project_profiles("demo").await.expect("profiles");
            let npub = crate::nostr::identity::resolve_name("demo", p.iter(), "BlueLake")
                .expect("blue npub");
            ((), npub)
        };
        let wraps = wraps_to(&mailbox, &npub, 2).await;
        assert!(wraps.is_empty(), "no partial delivery on abort");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-py9o acceptance gate at the fetch level: the SECOND identical
    /// `fetch_inbox` decrypts NOTHING — every stored wrap hydrates from the
    /// unwrap memo; only genuinely new wraps may ever be decrypted.
    #[tokio::test]
    async fn test_fetch_inbox_second_call_decrypts_nothing_bd_py9o() {
        let (mailbox, db_path, data_dir) = test_mailbox("inbox-incr").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_recv, _) = register(&mailbox, "demo", "BlueLake").await;

        for subject in ["a", "b", "c"] {
            mailbox
                .send_message(&SendMessage {
                    project_slug: "demo".into(),
                    sender_name: "GreenCastle".into(),
                    to: vec!["BlueLake".into()],
                    subject: subject.into(),
                    body_md: format!("body {subject}"),
                    ..Default::default()
                })
                .await
                .expect("send");
        }

        let args = FetchInbox {
            project_slug: "demo".into(),
            agent_name: "BlueLake".into(),
            ..Default::default()
        };
        let first = mailbox.fetch_inbox(&args).await.expect("fetch 1");
        assert_eq!(first.len(), 3);
        let misses_after_first = mailbox.unwrap_cache_misses();
        assert!(
            misses_after_first >= 3,
            "first fetch cold-decrypts each wrap once"
        );

        let second = mailbox.fetch_inbox(&args).await.expect("fetch 2");
        assert_eq!(second.len(), 3);
        assert_eq!(
            second.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
            first.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
            "identical inbox across fetches"
        );
        assert_eq!(
            mailbox.unwrap_cache_misses(),
            misses_after_first,
            "SECOND identical fetch must decrypt nothing (bd-py9o)"
        );
        assert!(mailbox.unwrap_cache_hits() >= 3, "memo served every wrap");

        mailbox
            .send_message(&SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "d".into(),
                body_md: "body d".into(),
                ..Default::default()
            })
            .await
            .expect("send d");
        let third = mailbox.fetch_inbox(&args).await.expect("fetch 3");
        assert_eq!(third.len(), 4);
        assert_eq!(
            mailbox.unwrap_cache_misses(),
            misses_after_first + 1,
            "only the new wrap may be decrypted"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_fetch_inbox_orders_by_rumor_time() {
        let (mailbox, db_path, data_dir) = test_mailbox("inbox").await;
        let (sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_recv, _npub) = register(&mailbox, "demo", "BlueLake").await;

        for subject in ["first", "second"] {
            let args = SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: subject.into(),
                body_md: format!("body {subject}"),
                ..Default::default()
            };
            mailbox.send_message(&args).await.expect("send");
        }

        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                ..Default::default()
            })
            .await
            .expect("fetch");
        assert_eq!(inbox.len(), 2);
        assert!(inbox.iter().any(|m| m.subject == "first"));
        assert!(inbox.iter().any(|m| m.subject == "second"));
        assert!(
            inbox
                .iter()
                .all(|m| m.sender == sender.public_key().to_hex())
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-bg8t fence: read at T1, ack at T2 > T1 → acknowledged_at_us is
    /// the ACK's own time (differs from the read receipt, ≥ T1), a repeat
    /// ack never regresses it, and the durable read_ts_us keeps T1.
    #[tokio::test]
    async fn test_ack_after_read_records_ack_time_not_read_time() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("bg8t").await;
        let (_sender, npub_sender) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub_recv) = register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "bg8t".into(),
            body_md: "read me, then ack me".into(),
            ack_required: true,
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");

        let wrap = &wraps_to(&mailbox, &npub_recv, 2).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();

        let read = mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("read");
        let t1 = read.acknowledged_at_us.expect("read receipt ts");

        tokio::time::timeout(std::time::Duration::from_millis(1), async {})
            .await
            .ok();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let ack = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack");
        let t2 = ack.acknowledged_at_us.expect("ack ts");
        assert!(
            t2 > t1,
            "bd-bg8t: ack time must exceed the earlier read receipt time ({t1})"
        );

        let again = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack again");
        assert_eq!(again.acknowledged_at_us, Some(t2), "no regression");

        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT read_ts_us FROM message_reads \
             WHERE project_slug = 'demo' AND recipient = ?1 AND message_id = ?2",
        )
        .bind(npub_recv.to_hex())
        .bind(&message_id)
        .fetch_optional(&pool)
        .await
        .expect("query");
        assert_eq!(row.map(|r| r.0), Some(t1));

        let _sender_wraps = wraps_to(&mailbox, &npub_sender, 4).await;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_ack_idempotent_and_private() {
        let (mailbox, db_path, data_dir) = test_mailbox("ack").await;
        let (sender, npub_sender) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub_recv) = register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "ack me".into(),
            body_md: "please".into(),
            ack_required: true,
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");

        let wrap = &wraps_to(&mailbox, &npub_recv, 2).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();

        let first = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack");
        assert!(first.acknowledged);
        let ts = first.acknowledged_at_us.expect("ts");

        let second = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack 2");
        assert_eq!(second.acknowledged_at_us, Some(ts), "idempotent ack");

        let _sender_wraps = wraps_to(&mailbox, &npub_sender, 4).await;
        let ack_wraps = wraps_to(&mailbox, &npub_sender, 4).await;
        assert!(
            ack_wraps.iter().any(|w| {
                gift_wrap::unwrap(&sender, w)
                    .map(|g| {
                        g.rumor
                            .tags
                            .iter()
                            .any(|t| t.as_slice() == ["toron-kind".to_string(), "ack".to_string()])
                    })
                    .unwrap_or(false)
            }),
            "sender received a private ack wrap"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_ack_by_thread_participant_authorized() {
        let (mailbox, db_path, data_dir) = test_mailbox("ack-thread").await;
        let (gc, gc_npub) = register(&mailbox, "demo", "GreenCastle").await;
        let (bl, bl_npub) = register(&mailbox, "demo", "BlueLake").await;
        let (_rd, _) = register(&mailbox, "demo", "RedDune").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "root".into(),
            body_md: "thread root".into(),
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send msg1");
        let wrap1 = &wraps_to(&mailbox, &bl_npub, 2).await[0];
        let msg1 = unwrap_for_recipient(&bl, wrap1).expect("unwrap msg1");
        let thread_id = msg1.rumor.clone().id().to_hex();

        let reply = SendMessage {
            project_slug: "demo".into(),
            sender_name: "RedDune".into(),
            to: vec!["GreenCastle".into()],
            subject: "reply".into(),
            body_md: "in thread".into(),
            thread_id: Some(thread_id.clone()),
            ..Default::default()
        };
        mailbox.reply_message(&reply).await.expect("reply msg2");
        let msg2_id = wraps_to(&mailbox, &gc_npub, 4)
            .await
            .iter()
            .find_map(|w| {
                gift_wrap::unwrap(&gc, w)
                    .ok()
                    .map(|mut g| g.rumor.id().to_hex())
            })
            .expect("msg2 id");

        let res = mailbox
            .acknowledge_message("demo", "BlueLake", &msg2_id)
            .await
            .expect("participant ack accepted");
        assert!(res.acknowledged, "thread participant may ack");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_ack_by_unrelated_agent_refused() {
        let (mailbox, db_path, data_dir) = test_mailbox("ack-refused").await;
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (bl, bl_npub) = register(&mailbox, "demo", "BlueLake").await;
        let (_df, _) = register(&mailbox, "demo", "DarkForest").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "private".into(),
            body_md: "only bl".into(),
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");
        let wrap = &wraps_to(&mailbox, &bl_npub, 2).await[0];
        let message_id = unwrap_for_recipient(&bl, wrap)
            .expect("unwrap")
            .rumor
            .id()
            .to_hex();

        let err = mailbox
            .acknowledge_message("demo", "DarkForest", &message_id)
            .await
            .expect_err("unrelated agent refused");
        assert!(
            err.to_string().contains("not authorized"),
            "refusal names authorization: {err}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn test_receipt_key_shapes() {
        let id =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id");
        let k = receipt_key("demo", "BlueLake", &id, "ack");
        assert_eq!(k, format!("demo:BlueLake:ack:{id}"));
    }

    /// bd-5icg core round trip: mark-read → unread-only fetch excludes it;
    /// the non-unread path is unchanged; read_ts_us is exposed on the row.
    #[tokio::test]
    async fn test_mark_read_excludes_from_unread_only_fetch() {
        let (mailbox, db_path, data_dir) = test_mailbox("unread").await;
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let mailbox = mailbox.with_index_pool(pool);
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;

        for subject in ["first", "second"] {
            let args = SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: subject.into(),
                body_md: format!("body {subject}"),
                ..Default::default()
            };
            mailbox.send_message(&args).await.expect("send");
        }

        let unread = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                unread_only: true,
                ..Default::default()
            })
            .await
            .expect("fetch unread");
        assert_eq!(unread.len(), 2, "both fresh");
        assert!(unread.iter().all(|m| m.read_ts_us.is_none()));

        let wrap = &wraps_to(&mailbox, &npub, 4).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();
        let first_read = mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("mark read");
        assert_eq!(first_read.message_id, message_id);

        let unread = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                unread_only: true,
                ..Default::default()
            })
            .await
            .expect("fetch unread 2");
        assert_eq!(unread.len(), 1, "read message filtered out");
        assert!(
            !unread.iter().any(|m| m.id == message_id),
            "mark-read message absent from unread-only"
        );

        let all = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                ..Default::default()
            })
            .await
            .expect("fetch all");
        assert_eq!(all.len(), 2, "non-unread path returns everything");
        let read_row = all.iter().find(|m| m.id == message_id).expect("read row");
        assert!(
            read_row.read_ts_us.is_some(),
            "read_ts_us exposed on the read row"
        );
        assert!(
            all.iter()
                .filter(|m| m.id != message_id)
                .all(|m| m.read_ts_us.is_none()),
            "other rows stay unread"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-5icg: read state survives a process restart — reopen the SQLite
    /// projection + bus on the same db/data dir; unread-only still excludes
    /// the previously-read message.
    #[tokio::test]
    async fn test_read_state_survives_restart() {
        let (mailbox, db_path, data_dir) = test_mailbox("restart").await;
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let mailbox = mailbox.with_index_pool(pool.clone());
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;

        for subject in ["first", "second"] {
            let args = SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: subject.into(),
                body_md: format!("body {subject}"),
                ..Default::default()
            };
            mailbox.send_message(&args).await.expect("send");
        }
        let wrap = &wraps_to(&mailbox, &npub, 4).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();
        mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("mark read");

        drop(mailbox);

        let pool2 = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("reconnect");
        let bus2 = InProcessBus::open(pool2.clone(), InProcessBusConfig::default())
            .await
            .expect("bus reopen");
        let operator2 = crate::keys::generate();
        let mailbox2 = Mailbox::new(Arc::new(bus2), data_dir.clone(), Some(operator2))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool2);

        let unread = mailbox2
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                unread_only: true,
                ..Default::default()
            })
            .await
            .expect("fetch after restart");
        assert_eq!(unread.len(), 1, "read state persisted across restart");
        assert!(
            !unread.iter().any(|m| m.id == message_id),
            "previously-read message stays excluded after restart"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-5icg: acknowledge_message also marks the message read (an ack
    /// implies it was read), and unread-only excludes it.
    #[tokio::test]
    async fn test_acknowledge_also_marks_read() {
        let (mailbox, db_path, data_dir) = test_mailbox("ackread").await;
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let mailbox = mailbox.with_index_pool(pool);
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "ack me".into(),
            body_md: "please".into(),
            ack_required: true,
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");

        let wrap = &wraps_to(&mailbox, &npub, 4).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();
        mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack");

        let unread = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                unread_only: true,
                ..Default::default()
            })
            .await
            .expect("fetch unread");
        assert!(
            unread.is_empty(),
            "acked message counts as read for unread-only"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-5icg honest degrade: without an index pool the read state cannot be
    /// known — unread_only returns everything (pre-bd-5icg behavior) and every
    /// row reports read_ts_us None. The receipt path still works.
    #[tokio::test]
    async fn test_unread_only_without_pool_degrades_to_all() {
        let (mailbox, db_path, data_dir) = test_mailbox("nopool").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "x".into(),
            body_md: "y".into(),
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");

        let wrap = &wraps_to(&mailbox, &npub, 4).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();
        mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("mark read (no pool: NIP-17 receipt still works)");

        let unread = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                unread_only: true,
                ..Default::default()
            })
            .await
            .expect("fetch");
        assert_eq!(unread.len(), 1, "no pool → cannot filter → everything");
        assert!(
            unread.iter().all(|m| m.read_ts_us.is_none()),
            "no pool → read_ts_us unknown (None)"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-next-envelope-ingest-yl0cx: every delivered DM is visible to
    /// `next` as a NOTICE, not a task. A captain mail is not an obligation;
    /// a notify is terminal on delivery, so the reader does not ack it and
    /// the opaque `request@1` rows that made a peer ask "needs-decision"
    /// cannot recur. The NIP-17 mail inbox remains the read/ack door.
    #[tokio::test]
    async fn next_ingest_two_mails_are_notify_and_drain_once() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("next-ingest").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        mailbox
            .register_agent("demo", &profile("GreenCastle"))
            .await
            .expect("sender");
        let (_recv, npub) = register(&mailbox, "demo", "BlueLake").await;
        let reader = npub.to_hex();
        mailbox
            .send_message(&send_args("demo", "GreenCastle", &["BlueLake"]))
            .await
            .expect("send one");
        mailbox
            .send_message(&send_args("demo", "GreenCastle", &["BlueLake"]))
            .await
            .expect("send two");
        let store = mailbox.inbox_store().expect("store");
        let blind = store.next(&reader, None, false).await.expect("next");
        assert!(
            blind.items.is_empty(),
            "without ingest the envelope log holds no mail"
        );
        let added = mailbox
            .ingest_delivered_mail("demo", "BlueLake", &store)
            .await
            .expect("ingest");
        assert_eq!(added, 2, "one row per delivered message");
        let batch = store.next(&reader, None, false).await.expect("next");
        assert_eq!(batch.items.len(), 2, "both messages visible");
        assert!(
            batch
                .items
                .iter()
                .all(|i| i.envelope.class == crate::envelope::Class::Notify),
            "mail is a notice, not a request/task"
        );
        let drained = store.next(&reader, None, false).await.expect("next");
        assert!(
            drained.items.is_empty(),
            "a notify is terminal on delivery; no ack is required and nothing repeats"
        );
        let added_again = mailbox
            .ingest_delivered_mail("demo", "BlueLake", &store)
            .await
            .expect("re-ingest");
        assert_eq!(added_again, 0, "re-ingest adds nothing");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// One sender into many readers ingests concurrently without the retry
    /// bail: the sequence belongs to the appended scope `(sender, reader)`
    /// (bd-next-readpath-p0-9k47), so scopes never share a row. Ten readers
    /// on one broadcast exceed the 8-retry budget under the old global
    /// counter; per-scope sequences complete with no contention at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn inbox_concurrent_ingest_one_sender_many_readers_no_bail() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("concurrent-ingest").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        mailbox
            .register_agent("demo", &profile("GreenCastle"))
            .await
            .expect("sender");
        const READERS: usize = 10;
        let mut names = Vec::new();
        for i in 0..READERS {
            let name = format!("Reader{i:02}");
            register(&mailbox, "demo", &name).await;
            names.push(name);
        }
        let to: Vec<&str> = names.iter().map(String::as_str).collect();
        mailbox
            .send_message(&send_args("demo", "GreenCastle", &to))
            .await
            .expect("broadcast to all readers");
        let store = mailbox.inbox_store().expect("store");
        let mut tasks = Vec::new();
        for name in names.clone() {
            let m = mailbox.clone();
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                m.ingest_delivered_mail("demo", &name, &s).await
            }));
        }
        for t in tasks {
            let added = t.await.expect("join").expect("ingest must not bail");
            assert_eq!(added, 1, "one row per reader");
        }
        for name in names {
            let npub = mailbox
                .agent_keys_for_resolved("demo", &name)
                .await
                .expect("keys")
                .public_key()
                .to_hex();
            let batch = store.next(&npub, None, true).await.expect("peek");
            assert_eq!(batch.items.len(), 1, "{name} sees its mail");
        }
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    fn send_args(project: &str, sender: &str, to: &[&str]) -> SendMessage {
        SendMessage {
            project_slug: project.into(),
            sender_name: sender.into(),
            to: to.iter().map(ToString::to_string).collect(),
            subject: "ADR-0018".into(),
            body_md: "canonical routing".into(),
            cc: vec![],
            bcc: vec![],
            importance: None,
            ack_required: false,
            thread_id: None,
            sender_token: None,
            overseer: false,
            extra_tags: vec![],
        }
    }

    #[tokio::test]
    async fn swarm_mode_send_accepts_canonical_and_npub_rejects_legacy() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("swarm-send").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc"))
            .await
            .expect("sender canonical");
        let (_recv_keys, recv_npub) = register(&mailbox, "toron", "flywheel-toron-oc2").await;
        mailbox
            .register_agent("toron", &profile("lane-4-catalog"))
            .await
            .expect("legacy registered pre-swarm");
        mailbox.set_identity_mode("toron", "swarm").await;

        mailbox
            .send_message(&send_args(
                "toron",
                "flywheel-toron-oc",
                &["flywheel-toron-oc2"],
            ))
            .await
            .expect("canonical recipient");
        assert!(
            !wraps_to(&mailbox, &recv_npub, 2).await.is_empty(),
            "canonical send delivered"
        );

        mailbox
            .send_message(&send_args(
                "toron",
                "flywheel-toron-oc",
                &[&recv_npub.to_hex()],
            ))
            .await
            .expect("npub recipient");
        assert_eq!(
            wraps_to(&mailbox, &recv_npub, 4).await.len(),
            2,
            "npub send delivered"
        );

        let err = mailbox
            .send_message(&send_args(
                "toron",
                "flywheel-toron-oc",
                &["lane-4-catalog"],
            ))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("flywheel-toron-<role><seq>"),
            "error names the grammar: {msg}"
        );

        let err = mailbox
            .send_message(&send_args(
                "toron",
                "flywheel-toron-oc",
                &["flywheel-toron-oc2", "lane-4-catalog"],
            ))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("non-routable"),
            "mixed send fails: {err}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn swarm_mode_send_resolves_folded_alias_recipient() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("swarm-send-alias").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc"))
            .await
            .expect("sender");
        let (_recv_keys, recv_npub) = register(&mailbox, "toron", "flywheel-toron-oc2").await;
        mailbox
            .register_agent("toron", &profile("DustySparrow"))
            .await
            .expect("legacy pre-swarm");
        mailbox.set_identity_mode("toron", "swarm").await;
        mailbox
            .fold_agent_name_alias("toron", "DustySparrow", "flywheel-toron-oc2")
            .await;

        mailbox
            .send_message(&send_args("toron", "flywheel-toron-oc", &["DustySparrow"]))
            .await
            .expect("folded alias recipient");
        let wraps = wraps_to(&mailbox, &recv_npub, 2).await;
        assert_eq!(wraps.len(), 1, "alias folds to canonical npub");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn open_mode_send_keeps_legacy_recipient_routing() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("open-send").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        mailbox
            .register_agent("demo", &profile("GreenCastle"))
            .await
            .expect("sender");
        let (_recv_keys, recv_npub) = register(&mailbox, "demo", "BlueLake").await;

        mailbox
            .send_message(&send_args("demo", "GreenCastle", &["BlueLake"]))
            .await
            .expect("legacy name routes in open mode");
        assert!(
            !wraps_to(&mailbox, &recv_npub, 1).await.is_empty(),
            "open mode send delivered"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-dbdi: open-mode `send --to captain` must agree with `whois captain`.
    /// whois folds the captain alias in every mode; the send path folded it
    /// only in Swarm, so open-mode crews saw whois resolve while send failed
    /// with "unknown recipient: captain".
    #[tokio::test]
    async fn open_mode_send_to_captain_alias_resolves() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("open-send-captain").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        mailbox
            .register_agent("demo", &profile("GreenCastle"))
            .await
            .expect("sender");
        let (_recv_keys, recv_npub) = register(&mailbox, "demo", "BlueLake").await;
        mailbox
            .set_project_captain("demo", "BlueLake")
            .await
            .expect("set captain");

        let w = mailbox
            .whois("demo", "captain")
            .await
            .expect("whois captain resolves via fold");
        assert_eq!(w.name, "BlueLake");
        mailbox
            .send_message(&send_args("demo", "GreenCastle", &["captain"]))
            .await
            .expect("send to captain agrees with whois");
        assert!(
            !wraps_to(&mailbox, &recv_npub, 1).await.is_empty(),
            "captain alias delivered to the folded pin"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn swarm_send_to_slug_resolves_to_pin_recipient() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("send-slug").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc"))
            .await
            .expect("sender canonical");
        let (_recv_keys, recv_npub) = register(&mailbox, "toron", "BlueRidge").await;
        mailbox.set_identity_mode("toron", "swarm").await;
        mailbox
            .fold_agent_name_alias("toron", "flywheel-toron-coder", "BlueRidge")
            .await;

        mailbox
            .send_message(&send_args(
                "toron",
                "flywheel-toron-oc",
                &["flywheel-toron-coder"],
            ))
            .await
            .expect("send to slug resolves via fold");
        let wraps = wraps_to(&mailbox, &recv_npub, 2).await;
        assert_eq!(wraps.len(), 1, "slug folds to pin npub");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-wh6k regression: the resolved ROW carries the canonical pin.
    /// Send to the folded slug → the delivery row names the PIN; read state
    /// written via the slug lands under the same npub; fetch-as-pin sees it
    /// READ and fetch-as-slug lists NO distinct unread copy (one inbox, one
    /// label — the slug view can never fork a parallel unread row).
    #[tokio::test]
    async fn swarm_slug_send_rows_canonical_pin_single_read_state() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("slug-row-pin").await;
        mailbox.ensure_project("toron").await.expect("ensure");
        mailbox
            .register_agent("toron", &profile("flywheel-toron-oc"))
            .await
            .expect("sender");
        let (_recv_keys, _recv_npub) = register(&mailbox, "toron", "BlueRidge").await;
        mailbox.set_identity_mode("toron", "swarm").await;
        mailbox
            .fold_agent_name_alias("toron", "flywheel-toron-coder", "BlueRidge")
            .await;

        let sent = mailbox
            .send_message(&send_args(
                "toron",
                "flywheel-toron-oc",
                &["flywheel-toron-coder"],
            ))
            .await
            .expect("send to slug");
        assert_eq!(sent.count, 1);
        assert_eq!(
            sent.deliveries[0].payload.recipient, "BlueRidge",
            "row recipient is the post-fold canonical pin"
        );

        let id = sent.deliveries[0].payload.event_id.clone();
        mailbox
            .mark_message_read("toron", "flywheel-toron-coder", &id)
            .await
            .expect("read as slug");

        let as_pin = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "toron".into(),
                agent_name: "BlueRidge".into(),
                ..Default::default()
            })
            .await
            .expect("fetch as pin");
        assert_eq!(as_pin.len(), 1, "pin view sees the delivery");
        assert!(
            as_pin[0].read_ts_us.is_some(),
            "slug-written read state is visible to the pin view"
        );

        let as_slug_unread = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "toron".into(),
                agent_name: "flywheel-toron-coder".into(),
                unread_only: true,
                ..Default::default()
            })
            .await
            .expect("fetch as slug");
        assert!(
            as_slug_unread.is_empty(),
            "slug view is not a parallel unread inbox"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn agent_keys_for_resolved_loads_pin_key_for_slug() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("keys-slug").await;
        mailbox
            .ensure_project_with_mode("toron", Some("swarm"))
            .await
            .expect("ensure swarm");
        let (pin_keys, pin_npub) = register(&mailbox, "toron", "BlueRidge").await;
        mailbox
            .fold_agent_name_alias("toron", "flywheel-toron-coder", "BlueRidge")
            .await;

        mailbox
            .send_message(&send_args("toron", "flywheel-toron-coder", &["BlueRidge"]))
            .await
            .expect("send as slug uses pin key");
        let wraps = wraps_to(&mailbox, &pin_npub, 2).await;
        assert_eq!(wraps.len(), 1, "wrap signed by the pin key (sender=pin)");
        assert_eq!(
            pin_keys.public_key().to_hex(),
            pin_npub.to_hex(),
            "register helper returns the pin identity"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn wake_mail_delivered_is_metadata_only() {
        let (mailbox, db_path, data_dir) = test_mailbox("wake-mail").await;
        let (_s, sn) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rn) = register(&mailbox, "demo", "BlueLake").await;
        let _ = sn;
        let _ = rn;
        mailbox
            .send_message(&SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "SECRET_SUBJECT_123".into(),
                body_md: "SECRET_BODY_456".into(),
                importance: Some("high".into()),
                ack_required: true,
                thread_id: Some("thread-xyz".into()),
                ..Default::default()
            })
            .await
            .expect("send");
        let filter = crate::bus::Filter::new()
            .kinds([crate::nostr::kinds::toron_kind(9)])
            .hashtag("demo");
        let events = crate::mailbox::collect_replay(mailbox.bus().as_ref(), filter)
            .await
            .expect("replay");
        let wake = events
            .iter()
            .find(|e| {
                e.tags
                    .iter()
                    .any(|t| t.as_slice() == ["toron-kind", "mail-delivered"])
            })
            .expect("mail-delivered wake present");
        assert!(wake.content.contains("GreenCastle"), "wake names from pin");
        assert!(wake.content.contains("thread-xyz"), "wake names thread_id");
        assert!(wake.content.contains("high"), "wake names importance");
        assert!(
            !wake.content.contains("SECRET_SUBJECT_123"),
            "wake must not leak subject"
        );
        assert!(
            !wake.content.contains("SECRET_BODY_456"),
            "wake must not leak body"
        );
        assert!(
            wake.tags.iter().any(|t| t.as_slice()[0] == "p"),
            "wake is addressed via p tag"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn wake_ack_sla_expired_names_post_thread_audience() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("wake-ack").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        let pool = mailbox.index_pool.as_ref().unwrap().clone();
        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - 2 * 60 * 60 * 1_000_000;
        sqlx::query(
            "INSERT INTO messages (event_id, project_slug, sender, sender_pubkey, subject, body, importance, ack_required, to_list, cc_list, created_ts_us, local_seq) \
             VALUES (?1, 'demo', 'GreenCastle', 'aaaa', 'urgent subject', 'urgent body', 'high', 1, '[\"BlueLake\"]', '[]', ?2, 1)",
        )
        .bind("post-123")
        .bind(old_us)
        .execute(&pool)
        .await
        .expect("insert overdue msg");
        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("check");
        assert!(
            emitted.contains(&"post-123".to_string()),
            "overdue post emitted"
        );
        let filter = crate::bus::Filter::new()
            .kinds([crate::nostr::kinds::toron_kind(
                crate::nostr::kinds::ACK_SLA_WAKE,
            )])
            .hashtag("demo");
        let events = crate::mailbox::collect_replay(mailbox.bus().as_ref(), filter)
            .await
            .expect("replay");
        let wake = events
            .iter()
            .find(|e| {
                e.tags
                    .iter()
                    .any(|t| t.as_slice() == ["toron-kind", "ack-sla-expired"])
            })
            .expect("ack-sla wake present");
        assert!(wake.content.contains("post-123"), "wake names post");
        assert!(!wake.content.contains("urgent subject"), "no subject leak");
        assert!(!wake.content.contains("urgent body"), "no body leak");
        let has_pin_p_tag = wake
            .tags
            .iter()
            .any(|t| t.len() >= 2 && t[0] == "p" && t[1] == "BlueLake");
        assert!(
            has_pin_p_tag,
            "ack-sla wake must p-tag pin names, tags={:?}",
            wake.tags
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    fn overdue_row(
        id: &str,
        importance: &str,
        created_ts_us: i64,
        read: bool,
        ack: bool,
    ) -> InboxMessage {
        InboxMessage {
            id: id.into(),
            sender: "GreenCastle".into(),
            subject: "s".into(),
            body_md: "b".into(),
            importance: importance.into(),
            ack_required: ack,
            thread_id: None,
            created_ts_us,
            read_ts_us: if read { Some(created_ts_us + 1) } else { None },
            duplicate_of: None,
            corr: None,
            wake: None,
            priority: None,
            audience: None,
            result_path: None,
            verdict: None,
            bead: None,
        }
    }

    fn overdue_query() -> FetchInbox {
        FetchInbox {
            project_slug: "demo".into(),
            agent_name: "BlueLake".into(),
            ack_overdue_only: true,
            ..Default::default()
        }
    }

    #[test]
    fn ack_overdue_only_keeps_unacked_required_past_sla() {
        let now = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let old = now - 2 * 60 * 60 * 1_000_000;
        let rows = vec![
            overdue_row("keep", "high", old, false, true),
            overdue_row("read", "high", old, true, true),
            overdue_row("noack", "high", old, false, false),
            overdue_row("fresh", "urgent", now, false, true),
            overdue_row("low", "low", old, false, true),
        ];
        let kept = filter_inbox_messages(rows, &overdue_query()).expect("filter");
        let ids: Vec<&str> = kept.iter().map(|m| m.id.as_str()).collect();
        assert_eq!(ids, vec!["keep"], "only the overdue row survives");
    }

    #[test]
    fn ack_overdue_only_off_changes_nothing() {
        let now = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let old = now - 2 * 60 * 60 * 1_000_000;
        let rows = vec![
            overdue_row("a", "high", old, false, true),
            overdue_row("b", "low", old, false, false),
        ];
        let q = FetchInbox {
            project_slug: "demo".into(),
            agent_name: "BlueLake".into(),
            ..Default::default()
        };
        let kept = filter_inbox_messages(rows, &q).expect("filter");
        assert_eq!(kept.len(), 2);
    }

    #[tokio::test]
    async fn sla_scan_partial_read_still_overdue_per_recipient() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("sla-perrcp").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_, bnpub) = register(&mailbox, "demo", "BlueLake").await;
        let (_, cnpub) = register(&mailbox, "demo", "RoseDune").await;
        let pool = mailbox.index_pool.as_ref().unwrap().clone();
        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - 2 * 60 * 60 * 1_000_000;
        let to_list = serde_json::to_string(&vec![bnpub.to_hex(), cnpub.to_hex()]).unwrap();
        sqlx::query(
            "INSERT INTO messages (event_id, project_slug, sender, sender_pubkey, subject, body, importance, ack_required, to_list, cc_list, created_ts_us, local_seq) \
             VALUES ('post-fan', 'demo', 'GreenCastle', 'aaaa', 's', 'b', 'high', 1, ?1, '[]', ?2, 1)",
        )
        .bind(&to_list)
        .bind(old_us)
        .execute(&pool)
        .await
        .expect("insert fan-out");
        sqlx::query(
            "INSERT INTO message_reads (project_slug, message_id, recipient, read_ts_us) \
             VALUES ('demo', 'post-fan', ?1, ?2)",
        )
        .bind(bnpub.to_hex())
        .bind(old_us + 1)
        .execute(&pool)
        .await
        .expect("BlueLake reads");
        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("check");
        assert!(
            emitted.contains(&"post-fan".to_string()),
            "partial read still overdue for RoseDune"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn sla_scan_read_is_not_ack_and_ack_silences() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("sla-allread").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_, bnpub) = register(&mailbox, "demo", "BlueLake").await;
        let (_, cnpub) = register(&mailbox, "demo", "RoseDune").await;
        let pool = mailbox.index_pool.as_ref().unwrap().clone();
        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - 2 * 60 * 60 * 1_000_000;
        let to_list = serde_json::to_string(&vec![bnpub.to_hex(), cnpub.to_hex()]).unwrap();
        sqlx::query(
            "INSERT INTO messages (event_id, project_slug, sender, sender_pubkey, subject, body, importance, ack_required, to_list, cc_list, created_ts_us, local_seq) \
             VALUES ('post-both', 'demo', 'GreenCastle', 'aaaa', 's', 'b', 'high', 1, ?1, '[]', ?2, 1)",
        )
        .bind(&to_list)
        .bind(old_us)
        .execute(&pool)
        .await
        .expect("insert fan-out");
        for npub in [bnpub.to_hex(), cnpub.to_hex()] {
            sqlx::query(
                "INSERT INTO message_reads (project_slug, message_id, recipient, read_ts_us) \
                 VALUES ('demo', 'post-both', ?1, ?2)",
            )
            .bind(npub)
            .bind(old_us + 1)
            .execute(&pool)
            .await
            .expect("read");
        }
        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("check");
        assert!(
            emitted.contains(&"post-both".to_string()),
            "read alone must not satisfy the ack SLA"
        );

        // Acking both recipients is the fact that silences the obligation.
        for npub in [bnpub.to_hex(), cnpub.to_hex()] {
            sqlx::query(
                "UPDATE message_reads SET acked = 1 \
                 WHERE message_id = 'post-both' AND recipient = ?1",
            )
            .bind(npub)
            .execute(&pool)
            .await
            .expect("ack");
        }
        // Let the SLA window elapse again so the scan can re-decide.
        sqlx::query("UPDATE ack_sla_wakes SET emitted_us = ?1")
            .bind(old_us)
            .execute(&pool)
            .await
            .expect("backdate wake");
        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("check after ack");
        assert!(
            !emitted.contains(&"post-both".to_string()),
            "all acked → silent"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The end-to-end consequence of the projection sink writing an EMPTY
    /// `to_list`: the SLA scan treats an empty audience as "cannot prove
    /// handled", so a fully-read overdue post is re-emitted on EVERY scan,
    /// forever. The sibling tests above hand-write `to_list` by raw INSERT,
    /// which is a state production could never produce — this one goes through
    /// the real send → project → scan path, so it is the link they skip.
    #[tokio::test]
    async fn sla_scan_silences_a_really_projected_fully_acked_post() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("sla-e2e").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        mailbox.spawn_unwrap_loop();
        let (_, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_, bnpub) = register(&mailbox, "demo", "BlueLake").await;
        let (_, cnpub) = register(&mailbox, "demo", "RoseDune").await;
        let pool = mailbox.index_pool.as_ref().unwrap().clone();

        mailbox
            .send_message(&crate::mailbox::SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                cc: vec!["RoseDune".into()],
                subject: "sla-e2e".into(),
                body_md: "please ack".into(),
                importance: Some("high".into()),
                ack_required: true,
                ..Default::default()
            })
            .await
            .expect("send");

        let mut row: Option<(String, String, String)> = None;
        for _ in 0..200 {
            row = sqlx::query_as::<_, (String, String, String)>(
                "SELECT event_id, to_list, cc_list FROM messages WHERE subject = 'sla-e2e'",
            )
            .fetch_optional(&pool)
            .await
            .expect("select");
            if row.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let (post_id, to_json, cc_json) = row.expect("projected row must exist");
        let to_seen: Vec<String> = serde_json::from_str(&to_json).expect("to json");
        let cc_seen: Vec<String> = serde_json::from_str(&cc_json).expect("cc json");
        assert_eq!(
            to_seen,
            vec![bnpub.to_hex()],
            "production projection must persist the `to` audience"
        );
        assert_eq!(
            cc_seen,
            vec![cnpub.to_hex()],
            "production projection must persist the `cc` audience"
        );

        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - 2 * 60 * 60 * 1_000_000;
        sqlx::query("UPDATE messages SET created_ts_us = ?1 WHERE subject = 'sla-e2e'")
            .bind(old_us)
            .execute(&pool)
            .await
            .expect("backdate");
        for npub in [bnpub.to_hex(), cnpub.to_hex()] {
            sqlx::query(
                "INSERT INTO message_reads (project_slug, message_id, recipient, read_ts_us, acked) \
                 VALUES ('demo', ?1, ?2, ?3, 1)",
            )
            .bind(&post_id)
            .bind(npub)
            .bind(old_us + 1)
            .execute(&pool)
            .await
            .expect("ack");
        }

        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("check");
        assert!(
            !emitted.contains(&post_id),
            "a fully-acked post with a REAL audience must not be re-emitted; \
             empty to_list re-emits it on every scan forever"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ejp4: the scan runs in a LOOP, so an overdue post that nobody has
    /// read is genuinely overdue on every pass. Without a durable
    /// already-woken record that means one wake per scan per post forever:
    /// measured live at 98,539 wake events from only 129 distinct posts
    /// (~771 each) in 13 hours, which filled the bus to its cap and got the
    /// mail gift-wrapped out from under the readers.
    ///
    /// The counter shape matters: a test that only checks "the second scan
    /// emitted nothing" would also pass if the first scan had emitted nothing
    /// at all, so it asserts the pair explicitly.
    #[tokio::test]
    async fn sla_scan_wakes_an_unread_post_exactly_once() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("sla-dedupe").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        mailbox.spawn_unwrap_loop();
        let (_, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_, bnpub) = register(&mailbox, "demo", "BlueLake").await;
        let pool = mailbox.index_pool.as_ref().unwrap().clone();

        mailbox
            .send_message(&crate::mailbox::SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "sla-dedupe".into(),
                body_md: "please ack".into(),
                importance: Some("high".into()),
                ack_required: true,
                ..Default::default()
            })
            .await
            .expect("send");

        let mut row: Option<String> = None;
        for _ in 0..200 {
            row = sqlx::query_scalar::<_, String>(
                "SELECT event_id FROM messages WHERE subject = 'sla-dedupe'",
            )
            .fetch_optional(&pool)
            .await
            .expect("select");
            if row.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let post_id = row.expect("projected row must exist");

        let to_json: String =
            sqlx::query_scalar("SELECT to_list FROM messages WHERE event_id = ?1")
                .bind(&post_id)
                .fetch_one(&pool)
                .await
                .expect("to_list");
        let to_seen: Vec<String> = serde_json::from_str(&to_json).expect("to json");
        assert_eq!(
            to_seen,
            vec![bnpub.to_hex()],
            "the wake dedupe key is only meaningful if the audience projected"
        );

        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - 2 * 60 * 60 * 1_000_000;
        sqlx::query("UPDATE messages SET created_ts_us = ?1 WHERE subject = 'sla-dedupe'")
            .bind(old_us)
            .execute(&pool)
            .await
            .expect("backdate");

        let first = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("first scan");
        assert_eq!(
            first,
            vec![post_id.clone()],
            "an overdue unread post with a real audience must be woken once"
        );

        let second = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("second scan");
        assert!(
            !second.contains(&post_id),
            "an already-woken post must not be re-woken on a later scan; \
             this is the 771x flood"
        );
        let third = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("third scan");
        assert!(
            !third.contains(&post_id),
            "the dedupe must hold for every later scan, not just the second"
        );

        sqlx::query(
            "UPDATE ack_sla_wakes SET emitted_us = ?1 \
             WHERE scope_id = ?2 AND audience = ?3",
        )
        .bind(old_us)
        .bind(&post_id)
        .bind(ack_sla_wake_audience_key(&[bnpub.to_hex()]))
        .execute(&pool)
        .await
        .expect("backdate the wake record");
        let after_window = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("scan after the window");
        assert_eq!(
            after_window,
            vec![post_id.clone()],
            "a still-unread overdue post must be re-woken once its window \
             lapses; a permanent mute would trade the flood for a silent drop"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ejp4: the dedupe key is the AUDIENCE, not the post. The scan's own
    /// contract is per-recipient — one reader's ack must not silence the wake
    /// for a co-recipient who has not read it — so a changed audience has to
    /// produce a new key and still be woken. Collapsing this to a per-post key
    /// would silence real overdue acks.
    #[tokio::test]
    async fn sla_wake_dedupe_key_follows_the_audience_not_just_the_post() {
        let a = ack_sla_wake_audience_key(&["aa".into(), "bb".into()]);
        let b = ack_sla_wake_audience_key(&["bb".into(), "aa".into()]);
        assert_eq!(
            a, b,
            "recipient order must not split one audience into two keys"
        );

        let c = ack_sla_wake_audience_key(&["aa".into(), "bb".into(), "cc".into()]);
        assert_ne!(
            a, c,
            "a new co-recipient must produce a new key so they still get woken"
        );

        let dup = ack_sla_wake_audience_key(&["aa".into(), "aa".into()]);
        assert_eq!(
            ack_sla_wake_audience_key(&["aa".into()]),
            dup,
            "a repeated recipient is one address, not two"
        );

        let empty = ack_sla_wake_audience_key(&[]);
        assert_eq!(empty, ack_sla_wake_audience_key(&[]));
        assert_ne!(empty, a, "an empty audience is not a named audience");
    }

    /// bd-ejp4: the record has to survive a restart. The live flood outlived
    /// a daemon restart, so an in-process guard would have let it resume; this
    /// pins the row to durable sqlite, which is the only place it can live.
    #[tokio::test]
    async fn ack_sla_wake_record_is_durable_sqlite() {
        let path =
            std::env::temp_dir().join(format!("ack-sla-durable-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = crate::index::schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");

        sqlx::query(
            "INSERT INTO ack_sla_wakes (scope_id, audience, emitted_us) VALUES (?1, ?2, ?3)",
        )
        .bind("post-1")
        .bind("aud-1")
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("insert");
        drop(pool);
        let reopened = crate::index::schema::connect(path.to_str().unwrap())
            .await
            .expect("reconnect");
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ack_sla_wakes WHERE scope_id = 'post-1' AND audience = 'aud-1'",
        )
        .fetch_one(&reopened)
        .await
        .expect("count");
        assert_eq!(
            n, 1,
            "the wake record must outlive the process that wrote it"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// An overdue ack-required post, inserted straight into the projection
    /// (the scan reads `messages`, not the wire).
    async fn insert_overdue_post(
        pool: &sqlx::SqlitePool,
        event_id: &str,
        thread_id: Option<&str>,
        to_list: &str,
        age_secs: i64,
    ) {
        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - age_secs * 1_000_000;
        sqlx::query(
            "INSERT INTO messages (event_id, project_slug, sender, sender_pubkey, subject, body, \
             importance, ack_required, thread_id, to_list, cc_list, created_ts_us, local_seq) \
             VALUES (?1, 'demo', 'GreenCastle', 'aaaa', 'subject', 'body', 'high', 1, ?2, ?3, '[]', ?4, 1)",
        )
        .bind(event_id)
        .bind(thread_id)
        .bind(to_list)
        .bind(old_us)
        .execute(pool)
        .await
        .expect("insert overdue post");
    }

    /// The reminders on the bus (kind 30015).
    async fn published_ack_sla_wakes(mailbox: &Mailbox) -> Vec<Event> {
        let filter = crate::bus::Filter::new()
            .kinds([crate::nostr::kinds::toron_kind(
                crate::nostr::kinds::ACK_SLA_WAKE,
            )])
            .hashtag("demo");
        let events = crate::mailbox::collect_replay(mailbox.bus().as_ref(), filter)
            .await
            .expect("replay");
        events
            .into_iter()
            .filter(|e| {
                e.tags
                    .iter()
                    .any(|t| t.as_slice() == ["toron-kind", "ack-sla-expired"])
            })
            .collect()
    }

    /// bd-4v1a: the reminder is a WAKE, not a room post. On the kind-9 room
    /// plane every reminder competed with real crew posts for the bus's
    /// capped rows (measured live: 95,077 of 100,000 rows were this one
    /// reminder) and the doorbell's wake brain forwarded every one of them,
    /// dropping 1,879 real wake lines.
    #[tokio::test]
    async fn ack_sla_wake_rides_its_own_kind_not_the_room_plane() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-kind").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        insert_overdue_post(
            &pool,
            "post-kind",
            Some("thread-kind"),
            "[\"BlueLake\"]",
            7200,
        )
        .await;

        mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("scan");

        assert_eq!(
            published_ack_sla_wakes(&mailbox).await.len(),
            1,
            "the reminder is published on ACK_SLA_WAKE"
        );
        let room = crate::bus::Filter::new()
            .kinds([crate::nostr::kinds::toron_kind(
                crate::nostr::kinds::CHANNEL_MESSAGE,
            )])
            .hashtag("demo");
        let room_events = crate::mailbox::collect_replay(mailbox.bus().as_ref(), room)
            .await
            .expect("replay");
        assert!(
            !room_events.iter().any(|e| e
                .tags
                .iter()
                .any(|t| t.as_slice() == ["toron-kind", "ack-sla-expired"])),
            "the reminder must not ride the kind-9 room plane"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4v1a: a tombstoned post is unreadable, so it can never be acked —
    /// re-waking it is a nag with no possible answer.
    #[tokio::test]
    async fn ack_sla_tombstoned_post_is_never_reminded() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-tombstone").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        insert_overdue_post(
            &pool,
            "post-dead",
            Some("thread-dead"),
            "[\"BlueLake\"]",
            7200,
        )
        .await;
        sqlx::query("UPDATE messages SET deleted = 1 WHERE event_id = 'post-dead'")
            .execute(&pool)
            .await
            .expect("tombstone the post");

        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("scan");

        assert!(
            emitted.is_empty(),
            "a tombstoned post is never reminded: {emitted:?}"
        );
        assert!(
            published_ack_sla_wakes(&mailbox).await.is_empty(),
            "a tombstoned post publishes no reminder"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4v1a: an un-addressed post has no reader, so the reminder can never
    /// reach anyone who could act on it.
    #[tokio::test]
    async fn ack_sla_unaddressed_post_is_never_reminded() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-unaddressed").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        insert_overdue_post(&pool, "post-noaddr", Some("thread-noaddr"), "[]", 7200).await;

        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("scan");

        assert!(
            emitted.is_empty(),
            "an un-addressed post is never reminded: {emitted:?}"
        );
        assert!(
            published_ack_sla_wakes(&mailbox).await.is_empty(),
            "an un-addressed post publishes no reminder"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4v1a: the obligation is the CONVERSATION. A supervisor that
    /// re-posts one request every two minutes produced 714 posts in one
    /// thread and 714 reminders per window; one conversation owes one
    /// reminder.
    #[tokio::test]
    async fn ack_sla_one_conversation_owes_one_reminder_per_window() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-conversation").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        for i in 0..5 {
            insert_overdue_post(
                &pool,
                &format!("post-conv-{i}"),
                Some("thread-conv"),
                "[\"BlueLake\"]",
                7200 + i,
            )
            .await;
        }

        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("scan");

        assert_eq!(
            published_ack_sla_wakes(&mailbox).await.len(),
            1,
            "one conversation owes one reminder per window"
        );
        assert_eq!(emitted.len(), 1, "one obligation is reported: {emitted:?}");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4v1a: an unbounded reminder re-nags a post nobody acks forever
    /// (95,077 of the bus's 100,000 rows were one reminder plane). The budget
    /// is the terminal state.
    #[tokio::test]
    async fn ack_sla_reminder_budget_mutes_the_obligation() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-budget").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        insert_overdue_post(
            &pool,
            "post-budget",
            Some("thread-budget"),
            "[\"BlueLake\"]",
            7200,
        )
        .await;

        for i in 0..ACK_SLA_WAKE_BUDGET {
            let emitted = mailbox
                .check_ack_sla_expired("demo", Some(30 * 60))
                .await
                .expect("scan");
            assert_eq!(emitted.len(), 1, "reminder {i} is owed: {emitted:?}");
            sqlx::query("UPDATE ack_sla_wakes SET emitted_us = emitted_us - ?1")
                .bind(2 * 60 * 60 * 1_000_000_i64)
                .execute(&pool)
                .await
                .expect("backdate the reminder");
        }

        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("scan");
        assert!(
            emitted.is_empty(),
            "the budget mutes the obligation: {emitted:?}"
        );
        let wakes: i64 =
            sqlx::query_scalar("SELECT wakes FROM ack_sla_wakes WHERE scope_id = 'thread-budget'")
                .fetch_one(&pool)
                .await
                .expect("budget counter");
        assert_eq!(
            wakes, ACK_SLA_WAKE_BUDGET,
            "the budget is spent and the spend is recorded"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4v1a: one scan cannot run forever. The doorbell kills `toron
    /// ack-sla` at 15s, so a scan that outruns that reports nothing at all.
    #[tokio::test]
    async fn ack_sla_scan_deadline_stops_the_scan() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-deadline").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        insert_overdue_post(
            &pool,
            "post-late",
            Some("thread-late"),
            "[\"BlueLake\"]",
            7200,
        )
        .await;

        let emitted = mailbox
            .check_ack_sla_expired_within(
                "demo",
                Some(30 * 60),
                std::time::Instant::now() - std::time::Duration::from_secs(1),
                ACK_SLA_SCAN_MAX_EMIT,
            )
            .await
            .expect("scan");

        assert!(
            emitted.is_empty(),
            "an expired deadline stops the scan: {emitted:?}"
        );
        assert!(
            published_ack_sla_wakes(&mailbox).await.is_empty(),
            "a stopped scan publishes nothing"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4v1a: one scan cannot flood the bus.
    #[tokio::test]
    async fn ack_sla_scan_publication_cap_bounds_one_scan() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("wake-cap").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_gc, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_bl, _) = register(&mailbox, "demo", "BlueLake").await;
        for i in 0..3 {
            let thread = format!("thread-cap-{i}");
            insert_overdue_post(
                &pool,
                &format!("post-cap-{i}"),
                Some(&thread),
                "[\"BlueLake\"]",
                7200 + i,
            )
            .await;
        }

        let emitted = mailbox
            .check_ack_sla_expired_within(
                "demo",
                Some(30 * 60),
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                1,
            )
            .await
            .expect("scan");

        assert_eq!(
            emitted.len(),
            1,
            "one scan publishes at most its cap: {emitted:?}"
        );
        assert_eq!(published_ack_sla_wakes(&mailbox).await.len(), 1);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    fn idempotent_send_args(corr: &str) -> SendMessage {
        SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "question".into(),
            body_md: "do the thing".into(),
            extra_tags: vec![crate::nostr::Tag::parse(["corr", corr]).expect("corr tag")],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn idempotent_send_retry_returns_same_event_id() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("idem-retry").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let first = mailbox
            .send_message(&idempotent_send_args("C-RETRY"))
            .await
            .expect("send 1");
        assert_eq!(first.count, 1);
        let id1 = first.deliveries[0].payload.event_id.clone();
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "one rumor after first send"
        );
        let second = mailbox
            .send_message(&idempotent_send_args("C-RETRY"))
            .await
            .expect("retry");
        assert_eq!(
            second.deliveries[0].payload.event_id, id1,
            "retry converges on the first event id"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "no second rumor on retry"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ff23: the send is a durable command keyed by its event id. A crash
    /// between the claim and the publish must NOT lose the mail: the claim
    /// row carries the rumor's timestamp (so the retry rebuilds the SAME
    /// event id) and whether the publish completed. An unpublished claim is
    /// re-published; a published one is replayed without publishing again.
    #[tokio::test]
    async fn idempotent_send_retry_after_crash_before_publish_still_sends() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("idem-crash").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let store = mailbox.idempotent_store().expect("store");
        let key = crate::mailbox::idempotent::send_key(
            "demo",
            "GreenCastle",
            "C-CRASH",
            "question",
            "do the thing",
        );
        let now_ms = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock) / 1000;
        let claim = store
            .claim_or_duplicate(
                &key,
                "seeded-id",
                "demo",
                "GreenCastle",
                "C-CRASH",
                "[]",
                1_700_000_000,
                now_ms,
            )
            .await
            .expect("claim");
        assert_eq!(claim, crate::mailbox::idempotent::SendClaim::New);

        let retry = mailbox
            .send_message(&idempotent_send_args("C-CRASH"))
            .await
            .expect("retry");
        assert_eq!(
            retry.count, 1,
            "a crash before the publish must not lose the mail"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "one rumor after the retry"
        );

        let again = mailbox
            .send_message(&idempotent_send_args("C-CRASH"))
            .await
            .expect("retry 2");
        assert_eq!(
            again.deliveries[0].payload.event_id, retry.deliveries[0].payload.event_id,
            "the retry converges on one event id"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "no second rumor"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// BCC × corr interaction: a same-corr retry with bcc recipients
    /// replays the FIRST send's per-variant deliveries and publishes
    /// nothing new. Pins the pending-set (visible rumor for to/cc,
    /// per-bcc variants) × idempotent-claim interaction that any phase
    /// split of `send_message` must preserve (bd-kkid).
    #[tokio::test]
    async fn idempotent_send_retry_with_bcc_replays_variants() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("idem-bcc").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let (_b, bnpub) = register(&mailbox, "demo", "CoralFinch").await;
        let args = || SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            bcc: vec!["CoralFinch".into()],
            subject: "question".into(),
            body_md: "do the thing".into(),
            extra_tags: vec![crate::nostr::Tag::parse(["corr", "C-BCC-RETRY"]).expect("corr tag")],
            ..Default::default()
        };
        let first = mailbox.send_message(&args()).await.expect("send 1");
        assert_eq!(first.count, 2, "to+bcc = 2 wraps");
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "one to wrap after first send"
        );
        assert_eq!(
            wraps_to(&mailbox, &bnpub, 10).await.len(),
            1,
            "one bcc wrap after first send"
        );
        let second = mailbox.send_message(&args()).await.expect("retry");
        assert_eq!(second.count, 2, "retry replays both deliveries");
        assert_eq!(
            second.deliveries, first.deliveries,
            "retry returns the FIRST send's delivery set"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "no new to wrap on retry"
        );
        assert_eq!(
            wraps_to(&mailbox, &bnpub, 10).await.len(),
            1,
            "no new bcc wrap on retry"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn idempotent_send_crash_window_claim_yields_one_rumor() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("idem-crash").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let key = crate::mailbox::idempotent::send_key(
            "demo",
            "GreenCastle",
            "C-CRASH",
            "question",
            "do the thing",
        );
        let recipients = serde_json::json!([["BlueLake", "crash-event-id"]]).to_string();
        sqlx::query(
            "INSERT INTO idempotent_sends \
             (send_key, event_id, project_slug, sender_name, corr, recipients_json, created_at) \
             VALUES (?1, ?2, 'demo', 'GreenCastle', 'C-CRASH', ?3, ?4)",
        )
        .bind(&key)
        .bind("crash-event-id")
        .bind(&recipients)
        .bind(crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock) / 1000)
        .execute(&pool)
        .await
        .expect("seed claim");
        let retried = mailbox
            .send_message(&idempotent_send_args("C-CRASH"))
            .await
            .expect("retry after crash");
        assert_eq!(
            retried.deliveries[0].payload.event_id, "crash-event-id",
            "retry returns the claimed event id"
        );
        assert!(
            wraps_to(&mailbox, &rnpub, 10).await.is_empty(),
            "no rumor published on retry"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn idempotent_send_different_corr_sends_twice() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("idem-distinct").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let a = mailbox
            .send_message(&idempotent_send_args("C-A"))
            .await
            .expect("send A");
        let b = mailbox
            .send_message(&idempotent_send_args("C-B"))
            .await
            .expect("send B");
        assert_ne!(
            a.deliveries[0].payload.event_id, b.deliveries[0].payload.event_id,
            "distinct corr values are distinct sends"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            2,
            "two rumors for two corrs"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn idempotent_send_expired_claim_resends() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("idem-expired").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let key = crate::mailbox::idempotent::send_key(
            "demo",
            "GreenCastle",
            "C-OLD",
            "question",
            "do the thing",
        );
        let recipients = serde_json::json!([["BlueLake", "stale-event-id"]]).to_string();
        sqlx::query(
            "INSERT INTO idempotent_sends \
             (send_key, event_id, project_slug, sender_name, corr, recipients_json, created_at) \
             VALUES (?1, ?2, 'demo', 'GreenCastle', 'C-OLD', ?3, 1)",
        )
        .bind(&key)
        .bind("stale-event-id")
        .bind(&recipients)
        .execute(&pool)
        .await
        .expect("seed stale claim");
        let fresh = mailbox
            .send_message(&idempotent_send_args("C-OLD"))
            .await
            .expect("send after expiry");
        assert_ne!(
            fresh.deliveries[0].payload.event_id, "stale-event-id",
            "expired claim does not suppress the send"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            1,
            "one fresh rumor after expiry"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// ADR-0010 F5: a successful read must not look like a failed ack —
    /// the read receipt carries its own `read: true` semantic.
    #[tokio::test]
    async fn adr0010_f56_mail_surface_read_receipt_has_own_semantic() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("f56-read").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub_recv) = register(&mailbox, "demo", "BlueLake").await;
        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "s".into(),
            body_md: "b".into(),
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");
        let wrap = &wraps_to(&mailbox, &npub_recv, 2).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();
        let read = mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("read");
        assert!(read.read, "read receipt carries read:true");
        assert!(
            !read.acknowledged,
            "read is not an ack — no shared ack envelope"
        );
        assert!(read.acknowledged_at_us.is_some());
        let ack = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack");
        assert!(!ack.read, "ack receipt carries read:false");
        assert!(ack.acknowledged);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// ADR-0010 F5: a pending contact request must not render as a
    /// subjectless empty-body mail row — it is protocol control, not mail.
    #[tokio::test]
    async fn adr0010_f56_mail_surface_contact_request_absent_from_inbox() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("f56-contact").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, _) = register(&mailbox, "demo", "BlueLake").await;
        mailbox
            .request_contact(&crate::mailbox::contacts::RequestContact {
                project_slug: "demo".into(),
                from_agent: "GreenCastle".into(),
                to_agent: "BlueLake".into(),
                to_project: None,
                reason: None,
            })
            .await
            .expect("request contact");
        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                unread_only: false,
                limit: None,
                include_bodies: true,
                urgent_only: false,
                since_ts: None,
                thread_id: None,
                ack_overdue_only: false,
            })
            .await
            .expect("inbox");
        assert!(
            inbox.is_empty(),
            "contact-request control must not render as a mail row, got {inbox:?}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// ADR-0010 F6: re-using a corr for a different body must never
    /// silently inherit the first event id — it mints a new event.
    #[tokio::test]
    async fn adr0010_f56_mail_surface_corr_reuse_with_different_body_mints_new_event() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("f56-corr").await;
        let (_s, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_r, rnpub) = register(&mailbox, "demo", "BlueLake").await;
        let first = mailbox
            .send_message(&idempotent_send_args("C-REUSE"))
            .await
            .expect("send 1");
        let id1 = first.deliveries[0].payload.event_id.clone();
        let mut second_args = idempotent_send_args("C-REUSE");
        second_args.body_md = "a completely different body".into();
        let second = mailbox.send_message(&second_args).await.expect("send 2");
        let id2 = second.deliveries[0].payload.event_id.clone();
        assert_ne!(
            id1, id2,
            "same corr with different body must not inherit the first event id"
        );
        assert_eq!(
            wraps_to(&mailbox, &rnpub, 10).await.len(),
            2,
            "different content mints a new rumor"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn fetch_inbox_projects_to_search_index() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("ftslag").await;
        let _ = register(&mailbox, "ftslag-proj", "FtsSender").await;
        let _ = register(&mailbox, "ftslag-proj", "FtsRecv").await;

        mailbox
            .send_message(&SendMessage {
                project_slug: "ftslag-proj".into(),
                sender_name: "FtsSender".into(),
                to: vec!["FtsRecv".into()],
                subject: "fts projection lag probe".into(),
                body_md: "xrayspectrumunique body".into(),
                ..Default::default()
            })
            .await
            .expect("send");

        let found = mailbox
            .search_messages(&SearchMessages {
                project_key: "ftslag-proj".into(),
                query: "xrayspectrumunique".into(),
                ..Default::default()
            })
            .await
            .expect("search");
        assert!(found.is_empty(), "pre-fetch: nothing projected yet");

        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "ftslag-proj".into(),
                agent_name: "FtsRecv".into(),
                ..Default::default()
            })
            .await
            .expect("fetch");
        assert_eq!(inbox.len(), 1, "fetch sees the mail");

        let found = mailbox
            .search_messages(&SearchMessages {
                project_key: "ftslag-proj".into(),
                query: "xrayspectrumunique".into(),
                ..Default::default()
            })
            .await
            .expect("search");
        assert_eq!(found.len(), 1, "post-fetch: searchable without a daemon");
        assert_eq!(found[0].subject, "fts projection lag probe");

        mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "ftslag-proj".into(),
                agent_name: "FtsRecv".into(),
                ..Default::default()
            })
            .await
            .expect("refetch");
        let found = mailbox
            .search_messages(&SearchMessages {
                project_key: "ftslag-proj".into(),
                query: "xrayspectrumunique".into(),
                ..Default::default()
            })
            .await
            .expect("search");
        assert_eq!(found.len(), 1, "re-fetch is dedup-safe");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
        drop(pool);
    }

    /// bd-oracle-dead-source-72az: a `fetch_inbox` read timestamps the
    /// reader's poll column even with an empty inbox — and leaves the
    /// `--since last` delivery cursor untouched (wall-clock must never
    /// masquerade as rumor-time high-water).
    #[tokio::test]
    async fn fetch_inbox_records_poll_without_moving_delivery_cursor() {
        let (mailbox, pool, db_path, data_dir) = test_mailbox_with_pool("poll-stamp").await;
        register(&mailbox, "demo", "BlueLake").await;
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                ..Default::default()
            })
            .await
            .expect("empty inbox still reads");
        let row: (i64, i64) = sqlx::query_as(
            "SELECT last_seen_us, last_poll_us FROM inbox_cursors \
             WHERE project_slug = 'demo' AND agent_name = 'BlueLake' AND plane = 'P'",
        )
        .fetch_one(&pool)
        .await
        .expect("poll row");
        assert!(row.1 >= before, "poll stamped at the read: {row:?}");
        assert_eq!(row.0, 0, "delivery cursor untouched by a poll: {row:?}");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
        drop(pool);
    }

    /// wave6-b: the messaging presence path (`authorize_ack` same-pane arm)
    /// reads the injected port. Same pane admits; a stranger on another pane
    /// with no thread role is refused.
    #[tokio::test]
    async fn authorize_ack_reads_injected_plane() {
        use crate::plane_port::AgentInfo;
        use crate::plane_port::AgentStatus;
        use crate::plane_port::PaneInfo;
        use crate::plane_port::{MockPanePlane, TOKEN_PIN};
        fn agent(name: &str, pane_id: &str) -> AgentInfo {
            AgentInfo {
                agent: None,
                agent_status: AgentStatus::Unknown,
                cwd: None,
                display_agent: None,
                focused: false,
                foreground_cwd: None,
                interactive_ready: false,
                launch_pending: false,
                name: Some(name.into()),
                pane_id: pane_id.into(),
                revision: 0,
                state_change_seq: 0,
                completion_seq: None,
                state_labels: serde_json::Map::new(),
                tab_id: String::new(),
                terminal_id: String::new(),
                terminal_title: None,
                workspace_id: String::new(),
            }
        }
        fn pane(pane_id: &str, pin: &str) -> PaneInfo {
            let mut tokens = serde_json::Map::new();
            tokens.insert(TOKEN_PIN.into(), serde_json::Value::String(pin.into()));
            PaneInfo {
                pane_id: pane_id.into(),
                terminal_id: String::new(),
                workspace_id: String::new(),
                tab_id: String::new(),
                focused: false,
                agent_status: crate::plane_port::AgentStatus::Unknown,
                revision: 0,
                title: None,
                cwd: None,
                agent: None,
                agent_session: None,
                display_agent: None,
                foreground_cwd: None,
                label: None,
                scroll: None,
                state_labels: serde_json::Map::new(),
                terminal_title: None,
                terminal_title_stripped: None,
                tokens,
            }
        }
        let (mailbox, _db, _dir) = test_mailbox("plane-ack").await;
        for (pin, program) in [
            ("BlueLake", None),
            ("GreenCastle", None),
            ("DustySparrow", None),
        ] {
            let mut p = profile(pin);
            p.program = program;
            mailbox.register_agent("demo", &p).await.expect("register");
        }
        // GreenCastle sends BlueLake one message; BlueLake's keys unwrap it.
        mailbox
            .send_message(&SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "ping".into(),
                body_md: "ack me".into(),
                ..Default::default()
            })
            .await
            .expect("send");
        let acker = mailbox
            .whois("demo", "BlueLake")
            .await
            .expect("whois acker");
        let acker_npub = crate::nostr::PublicKey::from_hex(&acker.npub).expect("acker npub");
        let wraps = wraps_to(&mailbox, &acker_npub, 1).await;
        assert_eq!(wraps.len(), 1);
        let re = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("re-read keys");
        let acker_keys = mailbox
            .load_agent_key(std::path::Path::new(&re.key_path))
            .expect("acker keys");
        let gift = unwrap_for_recipient(&acker_keys, &wraps[0]).expect("unwrap");
        // Same pane for acker + recipient: admitted via the injected plane.
        // (The ack arm reads the agent roster: both pins ride pane b:1.)
        let together = MockPanePlane::new(
            vec![pane("b:1", "BlueLake"), pane("b:1", "GreenCastle")],
            vec![agent("BlueLake", "b:1"), agent("GreenCastle", "b:1")],
        );
        let mailboxed = mailbox.with_plane(Arc::new(together));
        mailboxed
            .authorize_ack("demo", "BlueLake", "GreenCastle", &gift)
            .await
            .expect("same pane admits");
        // Stranger on another pane, no thread role: refused.
        let apart = MockPanePlane::new(
            vec![pane("b:1", "BlueLake"), pane("b:2", "DustySparrow")],
            vec![agent("BlueLake", "b:1"), agent("DustySparrow", "b:2")],
        );
        let mailboxed = mailboxed.with_plane(Arc::new(apart));
        assert!(
            mailboxed
                .authorize_ack("demo", "DustySparrow", "GreenCastle", &gift)
                .await
                .is_err(),
            "stranger on another pane is refused"
        );
    }
}
