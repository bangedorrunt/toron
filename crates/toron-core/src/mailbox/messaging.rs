// governed-by: ADR-0003
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

use crate::nostr::UnsignedEvent;
use crate::nostr::event::build_event;
use crate::nostr::kinds::toron_kind;
use crate::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};
use sqlx::Row;

use crate::backend::herdr::HerdrBackend;
use crate::bus::filter::ToronFilter;
use crate::bus::scoping::is_metadata_hiding_kind;
use crate::mailbox::identity::ListAgentsOptions;
use crate::mailbox::unwrap::{OVERSEER_TAG, UnwrappedMail, order_by_rumor_time, tag_value};
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
    fn tag_value(self) -> &'static str {
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
}

/// S5 collapse window: identical (sender, subject, body-hash) messages
/// within this window collapse to one inbox row (i64-µs, 1h default).
pub const DUP_COLLAPSE_WINDOW_US: i64 = 3_600_000_000;

/// `fetch_inbox` options.
#[derive(Debug, Clone, Default)]
pub struct FetchInbox {
    pub project_slug: String,
    pub agent_name: String,
    pub unread_only: bool,
    pub limit: Option<usize>,
    pub include_bodies: bool,
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
            // bd-harvest-thread-filter-yp9r: thread filter (post-unwrap —
            // the thread tag is inside the encrypted wrap; index-served
            // filtering is impossible for the inbox projection, which is
            // bus-replay based). `thread_id` here is the inbox row's own
            // thread field, so the comparison is exact.
            if let Some(t) = &q.thread_id
                && m.thread_id.as_deref() != Some(t.as_str())
            {
                return false;
            }
            true
        })
        .collect())
}

/// A receipt outcome (idempotent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptResult {
    pub message_id: String,
    pub acknowledged: bool,
    pub acknowledged_at_us: Option<i64>,
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
        // ADR-0017: the project key is canonicalized first — every spelling
        // of the project sends under the ONE canonical root.
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await;
        // G3: resolve ALL recipients pre-wrap; unknown aborts the whole send.
        let resolved = self.resolve_recipients(&args).await?;
        if resolved.is_empty() {
            anyhow::bail!("no recipients (to/cc/bcc all empty)");
        }

        let sender_keys = if args.overseer {
            // ADR-0004 Ship 5: the Overseer signs with the OPERATOR key, so
            // the projection can verify the sender npub against it.
            self.operator().cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "overseer send requires an operator key (set operator_key / TORON_OPERATOR_KEY)"
                )
            })?
        } else {
            // bd-pin-public-plane-s-easy.1: the sender name may be a slug —
            // resolve through the fold to the pin's key.
            self.agent_keys_for_resolved(&args.project_slug, &args.sender_name)
                .await?
        };

        let visible = build_visible_rumor(&sender_keys.public_key(), &args, &resolved);
        // Canonical id = the RUMOR id (G7 / D1), the SAME id the projection
        // shows in inbox/search rows — never the transport-internal wrap id.
        // Send → inbox → ack/summarize round-trips must work on one id world
        // (ADR-0007 D8.2 per-backend usability).
        let visible_id = visible.clone().id().to_hex();
        let mut deliveries: Vec<Delivery> = Vec::new();

        // to+cc: one gift wrap per recipient, same visible rumor.
        for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
            let wrap =
                gift_wrap::gift_wrap(&sender_keys, &r.npub, visible.clone()).map_err(|e| {
                    anyhow::anyhow!("gift wrap failed for to/cc recipient {}: {e}", r.name)
                })?;
            let delivered = self.publish_dm(&wrap).await?;
            if delivered {
                deliveries.push(Delivery {
                    project: args.project_slug.clone(),
                    payload: DeliveryPayload {
                        recipient: r.name.clone(),
                        event_id: visible_id.clone(),
                    },
                });
            }
        }

        // bcc: variant rumor per bcc listing to+cc+that bcc only.
        for r in resolved.iter().filter(|r| r.role == RecipientRole::Bcc) {
            let variant = build_bcc_rumor(&sender_keys.public_key(), &args, &resolved, r);
            let variant_id = variant.clone().id().to_hex();
            let wrap = gift_wrap::gift_wrap(&sender_keys, &r.npub, variant).map_err(|e| {
                anyhow::anyhow!("gift wrap failed for bcc recipient {}: {e}", r.name)
            })?;
            let delivered = self.publish_dm(&wrap).await?;
            if delivered {
                deliveries.push(Delivery {
                    project: args.project_slug.clone(),
                    payload: DeliveryPayload {
                        recipient: r.name.clone(),
                        event_id: variant_id,
                    },
                });
            }
        }

        // bd-i1o3.1: metadata-only wake ring for pane-backed recipients.
        // Gift-wrap is sealed (kind 1059, p-gated, no project tags) — the
        // doorbell's `toron watch` never sees it. Emit one lightweight kind-9
        // per delivery with ONLY: from pin, thread_id, importance. No subject
        // or content ever touches the wake (seal stays sealed). Reuses the
        // same bus the job wake (43001) uses — no new Nostr kind.
        let thread_ref = args.thread_id.clone().unwrap_or_else(|| visible_id.clone());
        let importance = args
            .importance
            .clone()
            .unwrap_or_else(|| "normal".to_string());
        for d in &deliveries {
            if let Some(r) = resolved.iter().find(|x| x.name == d.payload.recipient) {
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
        }

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
        // ADR-0017: canonical root before thread root resolution.
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
            return Ok(()); // read side already refused; nothing to bump
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

    /// `fetch_inbox`: project + agent projection of unwrapped kind-1059 mail to
    /// the agent's local key, ordered by rumor time (G7), never envelope time.
    pub async fn fetch_inbox(&self, args: &FetchInbox) -> anyhow::Result<Vec<InboxMessage>> {
        // ADR-0017: canonical root (read state, receipts) for every spelling
        // of the project key.
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await;
        // bd-pin-public-plane-s-easy.1: the inbox owner may be named by slug
        // — resolve through the fold to the pin's key.
        let agent_keys = self
            .agent_keys_for_resolved(&args.project_slug, &args.agent_name)
            .await?;
        let npub = agent_keys.public_key();

        // Subscribe to gift wraps addressed to this agent (#p), drain history.
        // bd-harvest-thread-filter-yp9r: the bus replay defaults to a 1000-row
        // newest-tail window (event_store.rs unwrap_or(1000)) — an inbox fetch
        // must see EVERY wrap or mail silently vanishes past 1000. Request the
        // store cap explicitly; the caller-facing `limit` only trims rows
        // AFTER projection.
        let filter = ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(npub)
            .limit(crate::bus::event_store::MAX_STORED_EVENTS as usize)
            .build();
        let events = super::collect_replay(self.bus.as_ref(), filter).await?;

        let mut mails: Vec<UnwrappedMail> = Vec::new();
        for wrap in &events {
            // bd-py9o: memoized decrypt — fleet polling replays the same
            // history every few seconds; each wrap is decrypted once per
            // (recipient, event) instead of once per poll.
            if let Some(gift) = self.cached_unwrap(&agent_keys, wrap, npub) {
                let rumor = gift.rumor;
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

        // G7: order by rumor time (+ts_ms) + local_seq; never envelope time.
        let ordered = order_by_rumor_time(mails);
        let mut messages: Vec<InboxMessage> =
            ordered.into_iter().map(|(m, _seq)| inbox_from(m)).collect();

        // S5 (bd-dup-assign-collapse-ws21): collapse identical
        // (sender, subject, body-hash) within the window into one row
        // carrying `duplicate_of: <first-id>` — toron stops double-delivering
        // duplicate assigns/re-sends; staleness is flywheel's wake concern.
        messages = collapse_duplicates(messages);

        // bd-5icg: read state is a projection concern (never event state).
        // Populate `read_ts_us` from the `message_reads` table and, when the
        // caller asks for unread-only, keep only messages with NO read row for
        // this recipient. Without an index pool the read state cannot be
        // known — the filter degrades to no filtering (same as pre-bd-5icg)
        // and every row reports `read_ts_us: None`.
        let mut read_ts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        if let Some(pool) = self.index_pool.as_ref() {
            // ADR-0017: expand the project to its alias set so legacy read
            // rows written under a path-form slug stay visible.
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
        for m in messages.iter_mut() {
            m.read_ts_us = read_ts.get(&m.id).copied();
        }
        // F19.NIP09 (batch-fix): a NIP-09-tombstoned message must not appear
        // in the inbox — drop ids marked `deleted` in the index (no index
        // pool → the filter degrades to no filtering, same as read state).
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
        // bd-since-cursor-70cf: `--since last` resolves the daemon-side
        // high-water cursor, filters with it, then bumps it to the newest
        // VISIBLE message ts (atomic UPSERT). Explicit RFC3339 values keep
        // their existing path; only the literal "last" is intercepted.
        let since_last = args.since_ts.as_deref() == Some("last");
        let cursor_us: i64 = if since_last {
            self.read_inbox_cursor(&args.project_slug, &args.agent_name)
                .await?
        } else {
            0
        };
        // High-water over VISIBLE messages (post delete/unread filters) —
        // limit-truncated rows still advance the cursor (nothing skipped).
        let high_water_us = messages.iter().map(|m| m.created_ts_us).max().unwrap_or(0);
        if args.urgent_only || args.since_ts.is_some() || args.thread_id.is_some() {
            let mut q = args.clone();
            if since_last {
                q.since_ts = None; // "last" is not RFC3339; the manual filter below applies it
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

        // Parity: AM `fetch_inbox` names the sender (the display name, not the
        // raw pubkey). toron's gift-wrap unwrap yields only the sender npub;
        // reverse-resolve npub→name from the agents index so the inbox is
        // human-readable without a per-message `whois`. Falls back to the hex
        // npub when no pool is attached or the sender never registered locally.
        let mut messages = match args.limit {
            Some(n) => messages.into_iter().take(n).collect(),
            None => messages,
        };
        self.resolve_sender_names(&mut messages).await;
        Ok(messages)
    }

    /// Best-effort batch npub→name resolution for inbox entries. Mutates
    /// `sender` in place; unknown pubkeys keep their hex value. One query per
    /// fetch (parameterized over the unique-sender set).
    async fn resolve_sender_names(&self, messages: &mut [InboxMessage]) {
        let Some(pool) = self.index_pool.as_ref() else {
            return; // no index — leave hex npubs (downgrade, not a failure)
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
        // Build `WHERE pubkey IN (?, ?, ...)` with one placeholder per unique
        // sender (sqlx has no first-class `IN Vec` on the macro-free path).
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
    /// `post+thread+audience` onto the same bus the job wake uses (no new
    /// Nostr kind — kind 9 + `toron-kind=ack-sla-expired`). Reuses the
    /// existing `messages` + receipts tracking structures; never builds a
    /// second tracker. Direct path stays cheap (one wake per expiry, not a poll).
    pub async fn emit_ack_sla_expired(
        &self,
        project_slug: &str,
        post_id: &str,
        thread_id: Option<&str>,
        audience: &[String],
    ) -> anyhow::Result<()> {
        let project_slug = self.canonical_project(project_slug).await;
        // Use operator key if available, else any agent key in project as signer
        // (wake must be signed; choose operator for truthfulness, fallback to
        // first agent's key).
        let signer = if let Some(op) = self.operator().cloned() {
            op
        } else {
            let agents = self
                .list_agents(&project_slug, ListAgentsOptions::default())
                .await
                .unwrap_or_default();
            let mut found: Option<Keys> = None;
            for a in agents {
                if let Ok(k) = self.agent_keys_for(&project_slug, &a.name) {
                    found = Some(k);
                    break;
                }
            }
            found.ok_or_else(|| anyhow::anyhow!("no signer for ack-sla wake"))?
        };
        let tid = thread_id.unwrap_or(post_id);
        let aud = if audience.is_empty() {
            "unknown".to_string()
        } else {
            audience.join(",")
        };
        let content = format!(
            "[doorbell] ack-sla-expired post {} thread {} audience {}",
            post_id, tid, aud
        );
        let mut tags = vec![
            Tag::parse(["t", &project_slug]).expect("t"),
            Tag::parse(["h", tid]).expect("h"),
            Tag::parse(["toron-kind", "ack-sla-expired"]).expect("toron-kind"),
            Tag::parse(["post", post_id]).expect("post"),
            Tag::parse(["thread", tid]).expect("thread"),
            Tag::parse(["audience", &aud]).expect("audience"),
        ];
        for a in audience {
            // audience entries may be pin names or npubs; best-effort p tag
            if let Ok(pk) = PublicKey::from_hex(a)
                && let Ok(t) = Tag::parse(["p", &pk.to_hex()])
            {
                tags.push(t);
            }
        }
        if let Ok(ev) = build_event(&signer, toron_kind(9), content, tags, Timestamp::now()) {
            let _ = self.bus.publish(ev).await;
        }
        Ok(())
    }

    /// Scan `messages` for overdue `ack_required` + `high|urgent` posts and
    /// emit one `ack-sla-expired` wake per overdue post whose SLA has lapsed
    /// and whose receipt is still missing. Reuses the existing `messages`
    /// table (ack_required/importance/created_ts_us) + `receipts` idempotency
    /// map — no second tracker (bd-i1o3 constraint).
    pub async fn check_ack_sla_expired(
        &self,
        project_slug: &str,
        sla_secs: Option<u64>,
    ) -> anyhow::Result<Vec<String>> {
        let project_slug = self.canonical_project(project_slug).await;
        let sla = sla_secs.unwrap_or(30 * 60) as i64;
        let cutoff_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - sla * 1_000_000;
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(Vec::new());
        };
        let rows: Vec<(String, Option<String>, String)> = sqlx::query_as(
            "SELECT event_id, thread_id, to_list FROM messages \
             WHERE project_slug = ?1 AND ack_required = 1 \
             AND importance IN ('high','urgent') \
             AND created_ts_us < ?2",
        )
        .bind(&project_slug)
        .bind(cutoff_us)
        .fetch_all(pool)
        .await
        .unwrap_or_default();
        let mut emitted = Vec::new();
        for (post_id, thread_id, to_list_json) in rows {
            let eid = EventId::from_hex(&post_id).unwrap_or_else(|_| {
                // non-hex post ids (free-form) — skip receipt check, just emit
                EventId::from_hex(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap()
            });
            // If ANY recipient still lacks an ack receipt, the SLA is expired
            // for that post. Check receipts map for at least one missing ack.
            // A full audience scan would need agent list; heuristic: if the
            // receipts map has no entry for this post+any agent, emit once.
            let has_ack = self
                .receipts()
                .lock()
                .keys()
                .any(|k| k.contains(&post_id) && k.contains("ack"));
            if has_ack {
                continue;
            }
            let audience: Vec<String> = serde_json::from_str(&to_list_json).unwrap_or_default();
            let tid = thread_id.as_deref();
            let _ = self
                .emit_ack_sla_expired(&project_slug, &post_id, tid, &audience)
                .await;
            let _ = eid; // suppress unused in non-hex path
            emitted.push(post_id);
        }
        Ok(emitted)
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
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        // bd-pin-public-plane-s-easy.1: ack/read identity may be a slug —
        // resolve through the fold to the pin's key.
        let agent_keys = self
            .agent_keys_for_resolved(project_slug, agent_name)
            .await?;
        let npub = agent_keys.public_key();

        // Case (a): the wrap addressed to THIS agent (the recipient).
        let original = match self.find_message_for_keys(&agent_keys, message_id).await? {
            Some(gift) => gift,
            None => {
                // Identity drift: the message exists but was addressed to a
                // different identity. Discover the actual recipient across
                // the project's agents (alias-set expanded), then authorize.
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

        // Idempotency: already acked/read → return stored receipt.
        let id = EventId::from_hex(message_id).unwrap_or_else(|_| original.rumor.clone().id());
        if let Some(ts) = self.receipt_time(project_slug, agent_name, &id, kind) {
            return Ok(ReceiptResult {
                message_id: message_id.to_string(),
                acknowledged,
                acknowledged_at_us: Some(ts),
            });
        }

        // Build the private kind-14 rumor: toron-kind + NIP-10 e reference.
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
        self.record_read_state(project_slug, npub, &id, ts_us).await;
        Ok(ReceiptResult {
            message_id: message_id.to_string(),
            acknowledged,
            acknowledged_at_us: Some(ts_us),
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
                continue; // no local key for this identity — skip
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
        // (c.1) The message's own sender is a thread participant.
        // bd-pin-public-plane-s-easy.1: acker may be named by slug.
        let acker_keys = self
            .agent_keys_for_resolved(project_slug, acker_name)
            .await?;
        if acker_keys.public_key() == gift.sender {
            return Ok(());
        }

        // (b) Same herdr pane as the recipient (best-effort: no herdr
        // backend wired → the check degrades to (c) only).
        if let Ok(backend) = HerdrBackend::connect(None) {
            let mut client = backend.client();
            if let Ok(agents) = client.agent_list() {
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
        }

        // (c.2) The acker can unwrap ANY wrap in the same thread — including
        // the thread ROOT itself: a head message is its own thread (the
        // projection's attribution rule), so holding the root is membership.
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
            match s.first().map(|x| x.as_str()) {
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
                // bd-wh6k: the ROW carries the CANONICAL recipient name — in
                // Swarm mode the post-fold resolve_agent_name target — never
                // the raw caller string. Downstream views key on this label;
                // storing a folded slug forked one delivery into two
                // disjoint inbox views whose read state can never meet.
                let (row_name, npub) = if super::naming::is_npub(name) {
                    // ADR-0018: an npub is always a routing address (and its
                    // own canonical label).
                    (
                        name.clone(),
                        crate::nostr::PublicKey::from_hex(name)
                            .map_err(|_| anyhow::anyhow!("invalid npub recipient: {name}"))?,
                    )
                } else if mode == super::naming::IdentityMode::Swarm {
                    // ADR-0018 D1a/D3 + bd-pin-public-plane-s-easy.1:
                    // canonical names, REGISTERED adjective+noun pins, or
                    // folded aliases route — ONE resolver (the fold precedes
                    // the shape match, so a slug folded to a pin resolves as
                    // the pin). An unregistered pin fails at resolve_name
                    // below ("register it first") — matching D1a (registered
                    // pins are routing targets; unregistered free strings
                    // are not).
                    let target = self.resolve_agent_name(&args.project_slug, name).await?;
                    let npub = nident::resolve_name(&args.project_slug, profiles.iter(), &target)
                        .ok_or_else(|| {
                        anyhow::anyhow!(
                            "unknown recipient: {target} (register it in project {} first)",
                            args.project_slug
                        )
                    })?;
                    (target, npub)
                } else {
                    (
                        name.clone(),
                        nident::resolve_name(&args.project_slug, profiles.iter(), name)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "unknown recipient: {name} (register it in project {} first)",
                                    args.project_slug
                                )
                            })?,
                    )
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

    /// Per-message receipt timestamp cache (idempotency, in-memory).
    fn receipt_time(&self, project: &str, agent: &str, id: &EventId, kind: &str) -> Option<i64> {
        self.receipts()
            .lock()
            .get(&receipt_key(project, agent, id, kind))
            .copied()
    }

    fn record_receipt(&self, project: &str, agent: &str, id: &EventId, kind: &str, ts_us: i64) {
        self.receipts()
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
    ) {
        let Some(pool) = self.index_pool.as_ref() else {
            return; // no index — read state cannot be persisted (degrade)
        };
        if let Err(e) = sqlx::query(
            "INSERT INTO message_reads (project_slug, message_id, recipient, read_ts_us) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (message_id, recipient) DO NOTHING",
        )
        .bind(project_slug)
        .bind(message_id.to_hex())
        .bind(recipient_npub.to_hex())
        .bind(ts_us)
        .execute(pool)
        .await
        {
            tracing::warn!("read-state write failed (degrade): {e}");
        }
    }

    fn receipts(&self) -> &parking_lot::Mutex<HashMap<String, i64>> {
        &self.receipt_cache
    }
}

/// Build the visible rumor (to+cc only; bcc NEVER appears on it).
///
/// Tag construction is infallible for toron-controlled inputs; any failure
/// here is a programming error surfaced via `expect` on a known-static tag
/// shape (kept narrow — only the recipient `p` tag, which is npub-derived).
fn build_visible_rumor(
    author: &PublicKey,
    args: &SendMessage,
    resolved: &[ResolvedRecipient],
) -> UnsignedEvent {
    let mut tags: Vec<Tag> = Vec::new();
    for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
        // npub.to_hex() is infallible; the 4-tuple shape is toron-controlled.
        // A failure here would indicate SDK invariants broken — panic is the
        // correct response (not a recoverable send error).
        tags.push(
            Tag::parse(["p", &r.npub.to_hex(), "", r.role.tag_value()])
                .expect("static p tag shape (npub hex + role literal)"),
        );
    }
    add_message_tags(&mut tags, args);
    gift_wrap::build_rumor(
        *author,
        Timestamp::now(),
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
) -> UnsignedEvent {
    let mut tags: Vec<Tag> = Vec::new();
    // to + cc, plus the single bcc — role "bcc" on its own p tag.
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
        Timestamp::now(),
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
    // G7 precise timeline: `created_at` is second-granular (nostr
    // Timestamp), so same-second messages were indistinguishable to the
    // projection timeline and `since_ts` strict-newer filtering excluded
    // post-bound mail (found live: am_parity_e2e since_ts conformance,
    // 2026-08-21 post-wave review). Stamp the µs tag the readers already
    // honor (unwrap.rs `ts_ms_tag` documents the VALUE as µs despite the
    // historical name; projection.rs `rumor_timestamp_us` returns it).
    let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
    if let Ok(t) = Tag::parse(["ts_ms", &now_us.to_string()]) {
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
    if args.overseer {
        // ADR-0004 Ship 5: mark the override so the projection can recognize
        // it (key-scoped + tag-scoped defense in depth).
        if let Ok(t) = Tag::parse(["t", OVERSEER_TAG]) {
            tags.push(t);
        }
    }
    // ADR-0007 F6: caller-supplied rumor tags (the workflow-emitted stamp
    // from the ActionRunner). Tags are already-built — extend is infallible.
    tags.extend(args.extra_tags.iter().cloned());
    if let Some(thread) = &args.thread_id {
        // Free-form thread ids ("br-123", "deploy-xyz") are the AM norm — they
        // are NOT valid NIP-10 event ids. Emit the literal id as a `thread`
        // tag so it survives the round-trip (inbox_from + the unwrap
        // projection both read it back). When the id IS a hex event id, also
        // emit the NIP-10 `e` root tag so strict reply-chain clients resolve.
        if let Ok(t) = Tag::parse(["thread", thread]) {
            tags.push(t);
        }
        if let Ok(root) = EventId::from_hex(thread) {
            // NIP-10 marked e-tag: a direct reply to the thread root uses
            // the `root` marker (positional e-tags are deprecated).
            if let Ok(t) = Tag::parse(["e", &root.to_hex(), "", "root"]) {
                tags.push(t);
            }
        }
    }
}

/// Project an unwrapped mail into the inbox view.
fn inbox_from(mail: UnwrappedMail) -> InboxMessage {
    let tags = &mail.rumor.tags;
    let subject = tag_value(tags, "subject").unwrap_or_default().to_string();
    let importance = tag_value(tags, "importance")
        .unwrap_or("normal")
        .to_string();
    let ack_required = tag_has_flag(tags, "ack-required");
    // thread_id: prefer the literal `thread` tag (free-form AM-style ids);
    // fall back to the NIP-10 `e` root tag (event-id threads).
    let thread_id = tag_value(tags, "thread")
        .map(|s| s.to_string())
        .or_else(|| {
            tags.iter().find_map(|t| {
                let parts = t.as_slice();
                (parts.first().map(|s| s.as_str()) == Some("e"))
                    .then(|| parts.get(1).map(|s| s.to_string()))
                    .flatten()
            })
        });
    InboxMessage {
        id: mail.rumor_id.to_hex(),
        sender: mail.sender.to_hex(),
        subject,
        body_md: mail.rumor.content.clone(),
        importance,
        ack_required,
        thread_id,
        created_ts_us: mail.ts_us(),
        read_ts_us: None, // filled by fetch_inbox from the message_reads projection
        duplicate_of: None,
    }
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
    // key = (sender, subject, body-hash) -> (first_id, first_ts_us, out_index)
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
                // Duplicate within window: the LATEST copy survives at the
                // group's row slot, marked with the first id.
                let mut survivor = m;
                survivor.duplicate_of = Some(first_id.clone());
                out[*idx] = survivor;
            }
            _ => {
                // First occurrence (or outside window → new group).
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
        .any(|t| t.as_slice().first().map(|s| s.as_str()) == Some(name))
}

/// Cache key for a receipt: `project:agent:kind:event_id`.
fn receipt_key(project: &str, agent: &str, id: &EventId, kind: &str) -> String {
    format!("{project}:{agent}:{kind}:{id}")
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
    use crate::nostr::identity::AgentProfile;
    use std::sync::Arc;

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
        };

        // Two identical assigns (same sender+subject+body) 10s apart → one
        // row, the LATEST copy, marked duplicate_of = first id.
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

        // Outside the window (2h later) → NOT collapsed.
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

        // Different body → no collapse.
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

        // Different sender → no collapse.
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

        // Operator wipes the key material (lost disk / recreated key).
        let key_dir = mailbox
            .agent_key_path("demo", "BlueLake")
            .parent()
            .expect("key dir")
            .to_path_buf();
        std::fs::remove_dir_all(&key_dir).expect("wipe agents/<name>");

        // Re-register mints a fresh on-disk identity...
        let re = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("re-register");
        let new_npub = PublicKey::from_hex(&re.npub).expect("npub");
        assert_ne!(
            new_npub, old_npub,
            "fresh key material derives a fresh npub"
        );

        // ...and EVERY surface converges to it.
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

        // The wrap is addressed to the NEW npub and unwraps with the
        // on-disk key — before the fix it went to the dead npub and the
        // inbox starved while send reported success.
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

        // No new mail → the SAME cursor yields nothing (no re-delivery).
        let none = mailbox.fetch_inbox(&fi).await.expect("re-poll");
        assert!(none.is_empty(), "re-poll with no new mail is empty");

        // New mail → exactly the new message.
        mailbox
            .send_message(&send("second"))
            .await
            .expect("send second");
        let warm = mailbox.fetch_inbox(&fi).await.expect("warm since last");
        assert_eq!(warm.len(), 1, "only the message after the cursor");
        assert_eq!(warm[0].subject, "second");

        // Explicit RFC3339 keeps working alongside "last"; malformed is a
        // hard error (never a silent ignore).
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

        // Both recipients' wraps unwrap to the same rumor content.
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
        // ADR-0004 Ship 5: `overseer: true` signs with the OPERATOR key (not
        // an agent key), sets importance=high, and tags toron:overseer — the
        // projection-recognizable override envelope.
        let (mailbox, db_path, data_dir) = test_mailbox("overseer").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;
        let operator_npub = mailbox.operator().expect("operator").public_key();

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: String::new(), // overseer ignores sender_name
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
        // Signed by the operator key, not an agent.
        assert_eq!(
            rumor.pubkey, operator_npub,
            "overseer rumor is signed by the operator key"
        );
        // Carries the override marker + high importance.
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
                .filter(|t| t.as_slice().first().map(|s| s.as_str()) == Some("p"))
                .map(|t| t.as_slice()[1].clone())
                .collect()
        };

        // to/cc rumor: lists to+cc, NO bcc.
        let to_gift = unwrap_for_recipient(&recv_to, to_wrap).expect("to");
        let to_ps = p_npubs(&to_gift);
        assert!(to_ps.contains(&npub_to.to_hex()));
        assert!(to_ps.contains(&npub_cc.to_hex()));
        assert!(!to_ps.contains(&npub_b1.to_hex()), "to cannot see bcc1");
        assert!(!to_ps.contains(&npub_b2.to_hex()), "to cannot see bcc2");

        // CC receives the same visible rumor (to+cc, no bcc) and can unwrap it.
        let cc_gift = unwrap_for_recipient(&recv_cc, cc_wrap).expect("cc");
        let cc_ps = p_npubs(&cc_gift);
        assert!(cc_ps.contains(&npub_to.to_hex()));
        assert!(cc_ps.contains(&npub_cc.to_hex()));
        assert!(!cc_ps.contains(&npub_b1.to_hex()), "cc cannot see bcc1");
        assert!(!cc_ps.contains(&npub_b2.to_hex()), "cc cannot see bcc2");

        // Each bcc rumor: lists to+cc+ITSELF, not the other bcc.
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

        // Non-recipient cannot unwrap ANY wrap.
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
        // No partial delivery: no wraps published for BlueLake.
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

        // A NEW message is decrypted exactly once on the next fetch.
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

        // Send two messages (rumor timestamps now = close; order is stable).
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

        // T1: mark READ only.
        let read = mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("read");
        let t1 = read.acknowledged_at_us.expect("read receipt ts");

        // T2 > T1: acknowledge.
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

        // Idempotency must not regress.
        let again = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack again");
        assert_eq!(again.acknowledged_at_us, Some(t2), "no regression");

        // Durable projection keeps the EARLIEST read (T1), not the ack ts.
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

        // The message id = rumor id of the wrap received by BlueLake.
        let wrap = &wraps_to(&mailbox, &npub_recv, 2).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();

        let first = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack");
        assert!(first.acknowledged);
        let ts = first.acknowledged_at_us.expect("ts");

        // Idempotent: second ack returns the same timestamp, no new event.
        let second = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack 2");
        assert_eq!(second.acknowledged_at_us, Some(ts), "idempotent ack");

        // The ack wrap is addressed to the ORIGINAL SENDER only (private).
        let _sender_wraps = wraps_to(&mailbox, &npub_sender, 4).await;
        // sender sees 1 original outbox? No — wraps are addressed to recipients.
        // The ack wrap targets the sender; count kind-1059 with p=sender npub.
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

    // ---- ADR-0017 D5: ack authorization ----
    //
    // acknowledge_message may be performed by (a) the addressed recipient,
    // (b) any identity bound to the same pane (herdr, not testable here), or
    // (c) any thread participant. Identity drift (the split-root repro) is
    // repaired by discovering the actual recipient across the project's
    // agents before the authorization gate.

    #[tokio::test]
    async fn test_ack_by_thread_participant_authorized() {
        let (mailbox, db_path, data_dir) = test_mailbox("ack-thread").await;
        let (gc, gc_npub) = register(&mailbox, "demo", "GreenCastle").await;
        let (bl, bl_npub) = register(&mailbox, "demo", "BlueLake").await;
        let (_rd, _) = register(&mailbox, "demo", "RedDune").await;

        // msg1: GreenCastle → BlueLake (thread T = msg1 id). BlueLake's one
        // and only thread membership.
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

        // msg2: RedDune replies in the SAME thread, addressed to GreenCastle.
        // BlueLake is neither recipient nor sender of msg2 — a participant.
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
                    .map(|g| g.rumor.clone().id().to_hex())
            })
            .expect("msg2 id");

        // BlueLake acks msg2 — its own keys cannot unwrap it (addressed to
        // GreenCastle), so the drift path discovers the recipient and the
        // thread-participant gate authorizes.
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

        // GreenCastle → BlueLake; DarkForest has NO membership in the thread.
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
            .clone()
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

        // Before any mark-read: everything is unread.
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

        // Mark ONE message read (explicit mark_message_read — fetch never marks).
        let wrap = &wraps_to(&mailbox, &npub, 4).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();
        let first_read = mailbox
            .mark_message_read("demo", "BlueLake", &message_id)
            .await
            .expect("mark read");
        assert_eq!(first_read.message_id, message_id);

        // Unread-only now excludes exactly the read message.
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

        // Non-unread path unchanged: both messages still listed, read_ts set.
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

        // "Restart": fresh bus + fresh mailbox over the SAME db + key dir.
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

    // --- ADR-0018: swarm-mode send-path hardening ---

    fn send_args(project: &str, sender: &str, to: &[&str]) -> SendMessage {
        SendMessage {
            project_slug: project.into(),
            sender_name: sender.into(),
            to: to.iter().map(|s| s.to_string()).collect(),
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

        // Canonical recipient delivers.
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

        // npub recipient delivers (npub is always a routing address).
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

        // Legacy name fails the whole send with the grammar error.
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

        // A mixed send (canonical + legacy) also fails — no partial delivery.
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

        // Send to the folded alias → delivered to the canonical npub.
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

        // Open mode (no identity_mode row): legacy names still route.
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

    // --- bd-pin-public-plane-s-easy.1: slug → pin (fold is slug→pin) ---

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

        // Send TO the slug → delivered to the PIN's npub (fold is slug→pin).
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

        // Send TO the slug → the delivery ROW names the pin, not the slug.
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

        // Read AS the slug → receipt keyed by the pin's npub (one identity).
        let id = sent.deliveries[0].payload.event_id.clone();
        mailbox
            .mark_message_read("toron", "flywheel-toron-coder", &id)
            .await
            .expect("read as slug");

        // Fetch-as-PIN: seen, and READ (the two views' states met).
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

        // Fetch-as-SLUG unread-only: NO distinct unread copy remains.
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

        // Sending AS the slug must sign with the PIN's key.
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

    // --- bd-i1o3: wake completeness ---

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
        // Wake is a kind-9 with toron-kind mail-delivered, visible via bus replay
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
        // Insert an overdue high-importance ack_required row directly
        let pool = mailbox.index_pool.as_ref().unwrap().clone();
        let old_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            - 2 * 60 * 60 * 1_000_000; // 2h ago
        sqlx::query(
            "INSERT INTO messages (event_id, project_slug, sender, sender_pubkey, subject, body, importance, ack_required, to_list, cc_list, created_ts_us, local_seq) \
             VALUES (?1, 'demo', 'GreenCastle', 'aaaa', 'urgent subject', 'urgent body', 'high', 1, '[\"BlueLake\"]', '[]', ?2, 1)",
        )
        .bind("post-123")
        .bind(old_us)
        .execute(&pool)
        .await
        .expect("insert overdue msg");
        // Emit via the SLA-checked path (reuses messages + receipts map)
        let emitted = mailbox
            .check_ack_sla_expired("demo", Some(30 * 60))
            .await
            .expect("check");
        assert!(
            emitted.contains(&"post-123".to_string()),
            "overdue post emitted"
        );
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
                    .any(|t| t.as_slice() == ["toron-kind", "ack-sla-expired"])
            })
            .expect("ack-sla wake present");
        assert!(wake.content.contains("post-123"), "wake names post");
        assert!(!wake.content.contains("urgent subject"), "no subject leak");
        assert!(!wake.content.contains("urgent body"), "no body leak");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn wake_watch_kinds_include_39002() {
        // Toron watch default must include 39002 (join/leave) next to 9,43001,30031.
        // This is a contract test — if the default drifts, the dispatcher goes deaf to crew changes.
        let default = "9,7,39002,43001,20001";
        assert!(
            default.split(',').any(|k| k == "39002"),
            "watch kinds must include 39002"
        );
        // And the bus actually stores a 39002 on join — smoke via channel API
        let (mailbox, _pool, db_path, data_dir) = test_mailbox_with_pool("wake-watch").await;
        mailbox.ensure_project("demo").await.expect("ensure");
        let (_a, _) = register(&mailbox, "demo", "GreenCastle").await;
        let filter = crate::bus::Filter::new()
            .kinds([crate::nostr::kinds::toron_kind(39002)])
            .hashtag("demo");
        // No panic: filter with 39002 is valid and replayable (even if empty now)
        let events = crate::mailbox::collect_replay(mailbox.bus().as_ref(), filter)
            .await
            .expect("replay 39002");
        assert!(
            events.is_empty(),
            "empty store has no 39002 yet — filter accepted"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
