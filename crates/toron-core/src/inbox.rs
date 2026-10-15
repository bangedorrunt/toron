// governed-by: ADR-0040, ADR-0041
//! The per-recipient inbox log (redesign §2.2-2.4) and its delivery verbs.
//!
//! * [`InboxStore`] is the async, SQLite-backed store: L16 appends (same
//!   triple once, gaps rejected, stale epochs fenced, epoch state never
//!   expires), per-recipient reads, the one cursor notion, and delivery
//!   state moved only through [`DeliveryFsm::step`].
//! * [`ToronLog`] is the same store as a [`kernel::DurableLog`] for an
//!   elm-effect `Loop`. The loop runs on its own OS thread (it is `!Send`);
//!   `ToronLog` reaches the async store through a request/reply channel to a
//!   task on a Tokio runtime it does not share. Called on a Tokio worker
//!   thread it refuses with `AppendError::Unavailable` instead of blocking
//!   or panicking (the F2 nested-runtime regression guard).
//! * Verbs: [`InboxStore::next`] (read + commit the reader's own cursor and
//!   the `Delivered` facts in one transaction), [`InboxStore::ack`],
//!   [`InboxStore::done`]. Only the recipient's identity can move its
//!   cursor or its deliveries.

use std::time::Duration;

use serde::Serialize;
use sqlx::{Row as _, SqlitePool};

use crate::delivery::{DeliveryEvent, DeliveryFsm, DeliveryState, Phase, Reject};
use crate::envelope::{Class, Envelope, MsgId};
use crate::kernel::{self, AppendError, DurableLog, Offset};

/// The SQLite effect journal (`kernel::JournalStore`), named here beside
/// [`ToronLog`] as the build plan lists them.
pub use crate::journal::{JournalDb, ToronJournal};

/// The stream envelopes live on.
pub const INBOX_STREAM: &str = "inbox";
/// The stream a sender loop's durable intents live on.
pub const JOURNAL_STREAM: &str = "journal";

/// `TORON_COMMS_INBOX`: the Phase 1 flag (ADR-0041). Off by default; the
/// verbs refuse while it is off.
pub fn enabled() -> bool {
    matches!(
        std::env::var("TORON_COMMS_INBOX").as_deref(),
        Ok("1" | "true" | "on")
    )
}

/// Failures of the inbox verbs.
#[derive(Debug, thiserror::Error)]
pub enum InboxError {
    /// The log refused the append (L16).
    #[error(transparent)]
    Append(#[from] AppendError),
    /// The envelope or the transition was refused.
    #[error(transparent)]
    Reject(#[from] Reject),
    /// No delivery for that message and recipient.
    #[error("no delivery {msg_key} for {recipient}")]
    NoSuchDelivery { msg_key: String, recipient: String },
    /// The triple is already in the log with a DIFFERENT payload. A replay
    /// carries the same bytes it carried the first time; anything else is a
    /// producer that reused a (producer, epoch, seq) for new content, and
    /// answering "replay" would silently drop that content
    /// (bd-envelope-conflict-3d7u).
    #[error(
        "payload conflict on {msg_key}: the log already holds different content under this producer triple"
    )]
    PayloadConflict { msg_key: String },
    /// The flag is off.
    #[error("comms inbox is off: set TORON_COMMS_INBOX=1")]
    Disabled,
    /// Store failure.
    #[error("store: {0}")]
    Store(#[from] sqlx::Error),
}

/// One appended row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Appended {
    /// Minted offset.
    pub offset: u64,
    /// True when the triple was already in the log (no new row).
    pub replay: bool,
}

/// A producer epoch handed out by [`InboxStore::claim_epoch`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct EpochClaim {
    /// The producer the epoch belongs to.
    pub producer: String,
    /// The claimed epoch: strictly above every epoch ever claimed or
    /// appended for this producer. The claimant's first seq is 0.
    pub epoch: u64,
    /// The highest epoch the log knew for this producer before the claim
    /// (`None` = a new producer). Everything at or below it is now fenced.
    pub previous: Option<u64>,
}

/// One envelope of a task, as [`InboxStore::by_task`] reports it.
#[derive(Clone, Debug, Serialize)]
pub struct TaskMessage {
    /// `producer:epoch:seq`.
    pub msg_key: String,
    /// Log offset.
    pub offset: u64,
    /// The recipient this delivery row is for (an npub or group stream).
    pub recipient: String,
    /// Delivery phase now.
    pub phase: Phase,
    /// Current assignment generation.
    pub generation: u32,
    /// Who holds the claim, once acknowledged.
    pub holder: Option<String>,
    /// The envelope.
    pub envelope: Envelope,
}

/// The task lookup (bd-n24w). Both tables are reached by index: the delivery
/// rows by `inbox_delivery_by_task`, the log rows by `inbox_log_by_msg_key`.
/// `CROSS JOIN` pins the order: the task's few delivery rows drive, and each
/// reaches its log row by key. Left to itself the planner starts from the
/// log's `stream` index and walks the whole inbox; the unary `+` keeps
/// `stream` a plain filter so `msg_key` is the only index in play. Exported so
/// a test can pin the query plan.
pub const BY_TASK_SQL: &str = "SELECT d.msg_key, d.recipient, d.state_json, l.log_offset, l.payload \
     FROM inbox_delivery d \
     CROSS JOIN inbox_log l ON l.msg_key = d.msg_key AND +l.stream = 'inbox' \
     WHERE d.task_id = ?1 \
     ORDER BY l.log_offset, d.recipient";

/// One message handed to its reader by [`InboxStore::next`].
#[derive(Clone, Debug, Serialize)]
pub struct InboxItem {
    /// Log offset.
    pub offset: u64,
    /// `producer:epoch:seq`.
    pub msg_key: String,
    /// The envelope.
    pub envelope: Envelope,
    /// Delivery phase after this read.
    pub phase: Phase,
    /// The verbs the reader may run next (redesign §6.2 "Allowed now").
    pub allowed_now: Vec<String>,
}

/// Result of [`InboxStore::next`].
#[derive(Clone, Debug, Serialize)]
pub struct NextBatch {
    /// The reader's npub.
    pub reader: String,
    /// Cursor before this read.
    pub cursor_before: u64,
    /// Cursor after (unchanged on `peek`).
    pub cursor_after: u64,
    /// Messages past the cursor.
    pub items: Vec<InboxItem>,
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default()
}

/// Mint the kernel's opaque offset (zero-padded, lexicographic = numeric).
pub fn mint(offset: u64) -> Offset {
    Offset::At(format!("{offset:020}"))
}

/// Parse an L16 envelope header (same rules and errors as `MemLog`).
fn parse_l16(json: &str) -> Result<(String, u64, u64, serde_json::Value), AppendError> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|e| AppendError::NotAnEnvelope(e.to_string()))?;
    let version = value
        .get("v")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| AppendError::NotAnEnvelope("missing v".into()))?;
    if version != u64::from(kernel::SCHEMA_VERSION) {
        return Err(AppendError::UnknownVersion(version as u32));
    }
    let p = value.get("producer").ok_or(AppendError::PartialIdentity)?;
    let id = p.get("producer_id").and_then(serde_json::Value::as_str);
    let epoch = p.get("epoch").and_then(serde_json::Value::as_u64);
    let seq = p.get("seq").and_then(serde_json::Value::as_u64);
    let (Some(id), Some(epoch), Some(seq)) = (id, epoch, seq) else {
        return Err(AppendError::PartialIdentity);
    };
    let payload = value
        .get("payload")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    Ok((id.to_string(), epoch, seq, payload))
}

/// The L16 wire form of an envelope: the msg triple is the producer triple.
pub fn l16_json(env: &Envelope) -> String {
    serde_json::json!({
        "v": kernel::SCHEMA_VERSION,
        "producer": {"producer_id": env.msg.producer, "epoch": env.msg.epoch, "seq": env.msg.seq},
        "payload": env,
    })
    .to_string()
}

/// Why an append failed inside the store: the kernel's own refusals, plus
/// the one verdict the vendored [`AppendError`] has no variant for.
#[derive(Debug)]
enum Refusal {
    Log(AppendError),
    PayloadConflict { msg_key: String },
}

impl From<AppendError> for Refusal {
    fn from(e: AppendError) -> Self {
        Self::Log(e)
    }
}

fn unavailable(e: impl std::fmt::Display) -> AppendError {
    AppendError::Unavailable(e.to_string())
}

/// Async SQLite-backed inbox store (the index pool).
#[derive(Clone, Debug)]
pub struct InboxStore {
    pool: SqlitePool,
}

impl InboxStore {
    /// Wrap a migrated index pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// The pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// L16 append of one wire envelope onto `stream`. On the inbox stream
    /// the payload must be a valid [`Envelope`] whose `msg` equals the
    /// producer triple. Fence before dedupe, as in the kernel's laws.
    ///
    /// A triple already in the log is a replay only when its payload is the
    /// same. A different payload under the same triple is refused. Through
    /// this kernel-facing entry the refusal is `NotAnEnvelope` naming the
    /// conflict (the vendored enum is closed); [`Self::append_envelope`]
    /// returns the typed [`InboxError::PayloadConflict`].
    pub async fn append_json(&self, stream: &str, json: &str) -> Result<Appended, AppendError> {
        self.append_checked(stream, json)
            .await
            .map_err(|r| match r {
                Refusal::Log(e) => e,
                Refusal::PayloadConflict { msg_key } => AppendError::NotAnEnvelope(format!(
                    "payload conflict on {msg_key}: the log already holds different content under this producer triple"
                )),
            })
    }

    async fn append_checked(&self, stream: &str, json: &str) -> Result<Appended, Refusal> {
        let (producer, epoch, seq, payload) = parse_l16(json)?;
        let (recipient, msg_key, delivery) = if stream == INBOX_STREAM {
            let env: Envelope = serde_json::from_value(payload.clone())
                .map_err(|e| AppendError::NotAnEnvelope(format!("inbox payload: {e}")))?;
            env.validate()
                .map_err(|r| AppendError::NotAnEnvelope(r.to_string()))?;
            if env.msg
                != (MsgId {
                    producer: producer.clone(),
                    epoch,
                    seq,
                })
            {
                return Err(AppendError::NotAnEnvelope(
                    "envelope msg differs from the producer triple".into(),
                )
                .into());
            }
            let stream_key = env.to.stream();
            let d = DeliveryState::intent(env.class, stream_key.clone(), env.generation);
            (stream_key, env.msg.key(), Some((d, env.task.map(|t| t.0))))
        } else {
            (
                String::new(),
                MsgId {
                    producer: producer.clone(),
                    epoch,
                    seq,
                }
                .key(),
                None,
            )
        };
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(unavailable)?;
        let state: Option<(i64, i64)> =
            sqlx::query_as("SELECT epoch, last_seq FROM inbox_producer_state WHERE producer = ?1")
                .bind(&producer)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        // The fence is the higher of the producer's last appended epoch and
        // its highest claimed epoch (bd-epoch-claim-z5m5): a claim fences
        // every older incarnation before the new one has appended anything.
        let claimed: Option<i64> =
            sqlx::query_scalar("SELECT epoch FROM inbox_epoch_claim WHERE producer = ?1")
                .bind(&producer)
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        let fence = state.map(|(e, _)| e).max(claimed);
        if let Some(current) = fence
            && epoch < current as u64
        {
            return Err(AppendError::StaleEpoch {
                producer,
                sent: epoch,
                current: current as u64,
            }
            .into());
        }
        let existing: Option<(i64, String)> = sqlx::query_as(
            "SELECT log_offset, payload FROM inbox_log WHERE producer = ?1 AND epoch = ?2 AND seq = ?3",
        )
        .bind(&producer)
        .bind(epoch as i64)
        .bind(seq as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some((offset, stored)) = existing {
            // Same triple: a replay only if it is the same content.
            let same = serde_json::from_str::<serde_json::Value>(&stored)
                .is_ok_and(|stored| stored == payload);
            if !same {
                return Err(Refusal::PayloadConflict { msg_key });
            }
            return Ok(Appended {
                offset: offset as u64,
                replay: true,
            });
        }
        let last = match state {
            Some((current, last)) if current as u64 == epoch => Some(last as u64),
            _ => None,
        };
        let expected = last.map_or(0, |l| l + 1);
        if seq != expected {
            return Err(AppendError::SeqGap { got: seq, last }.into());
        }
        let now = now_us();
        let offset: i64 = sqlx::query_scalar(
            "INSERT INTO inbox_log (stream, recipient, producer, epoch, seq, msg_key, payload, appended_ts_us) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING log_offset",
        )
        .bind(stream)
        .bind(&recipient)
        .bind(&producer)
        .bind(epoch as i64)
        .bind(seq as i64)
        .bind(&msg_key)
        .bind(payload.to_string())
        .bind(now)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query(
            "INSERT INTO inbox_producer_state (producer, epoch, last_seq) VALUES (?1, ?2, ?3) \
             ON CONFLICT (producer) DO UPDATE SET epoch = excluded.epoch, last_seq = excluded.last_seq",
        )
        .bind(&producer)
        .bind(epoch as i64)
        .bind(seq as i64)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some((intent, task_id)) = delivery {
            let sent = DeliveryFsm::step(
                &intent,
                &DeliveryEvent::Appended {
                    offset: offset as u64,
                },
            )
            .expect("intent -> sent is always legal");
            write_delivery(&mut tx, &msg_key, &sent, task_id.as_deref(), None, now)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Appended {
            offset: offset as u64,
            replay: false,
        })
    }

    /// Validate and append one envelope to the inbox (the `mail.append`
    /// provider). Idempotent on `msg` for the same content; a different payload
    /// under an existing `msg` is [`InboxError::PayloadConflict`].
    pub async fn append_envelope(&self, env: &Envelope) -> Result<Appended, InboxError> {
        env.validate()?;
        match self.append_checked(INBOX_STREAM, &l16_json(env)).await {
            Ok(appended) => Ok(appended),
            Err(Refusal::Log(e)) => Err(e.into()),
            Err(Refusal::PayloadConflict { msg_key }) => {
                Err(InboxError::PayloadConflict { msg_key })
            }
        }
    }

    /// Reconcile query: the offset holding `msg`, if the log has it.
    pub async fn lookup(&self, msg: &MsgId) -> Result<Option<u64>, sqlx::Error> {
        let off: Option<i64> = sqlx::query_scalar(
            "SELECT log_offset FROM inbox_log WHERE producer = ?1 AND epoch = ?2 AND seq = ?3",
        )
        .bind(&msg.producer)
        .bind(msg.epoch as i64)
        .bind(msg.seq as i64)
        .fetch_optional(&self.pool)
        .await?;
        Ok(off.map(|o| o as u64))
    }

    /// Every payload on `stream` after `after` (`None` = from the start).
    /// An `after` that is not an offset of this stream is `UnknownOffset`.
    pub async fn read_stream(
        &self,
        stream: &str,
        after: Option<u64>,
    ) -> Result<Vec<(u64, String)>, AppendError> {
        if let Some(a) = after {
            let known: Option<i64> = sqlx::query_scalar(
                "SELECT log_offset FROM inbox_log WHERE stream = ?1 AND log_offset = ?2",
            )
            .bind(stream)
            .bind(a as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(unavailable)?;
            if known.is_none() {
                return Err(AppendError::UnknownOffset);
            }
        }
        let rows = sqlx::query(
            "SELECT log_offset, payload FROM inbox_log WHERE stream = ?1 AND log_offset > ?2 ORDER BY log_offset",
        )
        .bind(stream)
        .bind(after.map_or(0, |a| a as i64))
        .fetch_all(&self.pool)
        .await
        .map_err(unavailable)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<i64, _>(0) as u64, r.get::<String, _>(1)))
            .collect())
    }

    /// The reader's committed cursor on `stream` (0 = nothing read).
    pub async fn cursor(&self, reader: &str, stream: &str) -> Result<u64, sqlx::Error> {
        let c: Option<i64> = sqlx::query_scalar(
            "SELECT log_offset FROM inbox_cursor WHERE reader_npub = ?1 AND stream = ?2",
        )
        .bind(reader)
        .bind(stream)
        .fetch_optional(&self.pool)
        .await?;
        Ok(c.map_or(0, |c| c as u64))
    }

    /// `toron next`: messages for `reader` past its cursor. Without `peek`,
    /// the cursor and the `Delivered` facts commit in ONE transaction (L16).
    /// `stream` defaults to the reader's own; naming anyone else's is
    /// refused (`NotRecipient`): only the recipient advances its cursor.
    pub async fn next(
        &self,
        reader: &str,
        stream: Option<&str>,
        peek: bool,
    ) -> Result<NextBatch, InboxError> {
        let stream = stream.unwrap_or(reader);
        if stream != reader {
            return Err(Reject::NotRecipient {
                actor: reader.into(),
                recipient: stream.into(),
            }
            .into());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let before: i64 = sqlx::query_scalar(
            "SELECT log_offset FROM inbox_cursor WHERE reader_npub = ?1 AND stream = ?2",
        )
        .bind(reader)
        .bind(stream)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(0);
        let rows = sqlx::query(
            "SELECT log_offset, msg_key, payload FROM inbox_log \
             WHERE stream = ?1 AND recipient = ?2 AND log_offset > ?3 ORDER BY log_offset",
        )
        .bind(INBOX_STREAM)
        .bind(stream)
        .bind(before)
        .fetch_all(&mut *tx)
        .await?;
        let after = rows.last().map_or(before, |r| r.get::<i64, _>(0));
        let now = now_us();
        let mut items = Vec::with_capacity(rows.len());
        for r in rows {
            let offset = r.get::<i64, _>(0) as u64;
            let msg_key: String = r.get(1);
            let envelope: Envelope = serde_json::from_str(&r.get::<String, _>(2))
                .map_err(|e| Reject::BadEnvelope(format!("stored payload: {e}")))?;
            let state = read_delivery(&mut tx, &msg_key, stream).await?;
            let state = match state {
                Some((s, _)) if !peek && s.phase == Phase::Sent => {
                    let d = DeliveryFsm::step(
                        &s,
                        &DeliveryEvent::Read {
                            reader: reader.into(),
                            cursor: after as u64,
                        },
                    )?;
                    write_delivery(&mut tx, &msg_key, &d, None, None, now).await?;
                    d
                }
                Some((s, _)) => s,
                None => {
                    return Err(InboxError::NoSuchDelivery {
                        msg_key,
                        recipient: stream.into(),
                    });
                }
            };
            items.push(InboxItem {
                offset,
                allowed_now: allowed_now(&state, &msg_key, &envelope),
                msg_key,
                envelope,
                phase: state.phase,
            });
        }
        if !peek && after > before {
            sqlx::query(
                "INSERT INTO inbox_cursor (reader_npub, stream, log_offset, updated_ts_us) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT (reader_npub, stream) DO UPDATE SET log_offset = excluded.log_offset, \
                 updated_ts_us = excluded.updated_ts_us WHERE excluded.log_offset > inbox_cursor.log_offset",
            )
            .bind(reader)
            .bind(stream)
            .bind(after)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(NextBatch {
            reader: reader.into(),
            cursor_before: before as u64,
            cursor_after: if peek { before as u64 } else { after as u64 },
            items,
        })
    }

    /// Apply one event to the delivery `(msg_key, recipient)` through the FSM.
    pub async fn step(
        &self,
        msg_key: &str,
        recipient: &str,
        event: &DeliveryEvent,
        evidence: Option<&serde_json::Value>,
    ) -> Result<DeliveryState, InboxError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let Some((s, _)) = read_delivery(&mut tx, msg_key, recipient).await? else {
            return Err(InboxError::NoSuchDelivery {
                msg_key: msg_key.into(),
                recipient: recipient.into(),
            });
        };
        let next = DeliveryFsm::step(&s, event)?;
        write_delivery(&mut tx, msg_key, &next, None, evidence, now_us()).await?;
        tx.commit().await?;
        Ok(next)
    }

    /// `toron ack <msg> accept|reject`.
    pub async fn ack(
        &self,
        reader: &str,
        msg_key: &str,
        accept: bool,
        reason: Option<&str>,
    ) -> Result<DeliveryState, InboxError> {
        let s = self.delivery(msg_key, reader).await?;
        let event = if accept {
            DeliveryEvent::Accept {
                by: reader.into(),
                generation: s.generation,
            }
        } else {
            DeliveryEvent::Refuse {
                by: reader.into(),
                reason: reason.unwrap_or_default().into(),
            }
        };
        self.step(msg_key, reader, &event, None).await
    }

    /// `toron done <task> --evidence …`: the reader's acknowledged task.
    pub async fn done(
        &self,
        reader: &str,
        task_id: &str,
        evidence: &[String],
    ) -> Result<(String, DeliveryState), InboxError> {
        let key: Option<String> = sqlx::query_scalar(
            "SELECT msg_key FROM inbox_delivery WHERE task_id = ?1 AND recipient = ?2 ORDER BY updated_ts_us DESC LIMIT 1",
        )
        .bind(task_id)
        .bind(reader)
        .fetch_optional(&self.pool)
        .await?;
        let Some(msg_key) = key else {
            return Err(InboxError::NoSuchDelivery {
                msg_key: format!("task {task_id}"),
                recipient: reader.into(),
            });
        };
        let s = self.delivery(&msg_key, reader).await?;
        let ev = serde_json::json!({ "evidence": evidence });
        let next = self
            .step(
                &msg_key,
                reader,
                &DeliveryEvent::Done {
                    by: reader.into(),
                    generation: s.generation,
                },
                Some(&ev),
            )
            .await?;
        Ok((msg_key, next))
    }

    /// Every envelope of `task_id` that `viewer` may see, oldest first, with
    /// its delivery state (bd-n24w): the way for a sender to find a task's
    /// whole conversation (assignment, replies, receipts) without walking
    /// each recipient's log. `viewer` sees a message only if it sent it or
    /// is its recipient, as everywhere else in the inbox; anyone else gets
    /// an empty list, the same answer as for an unknown task id.
    pub async fn by_task(
        &self,
        task_id: &str,
        viewer: &str,
    ) -> Result<Vec<TaskMessage>, InboxError> {
        let rows = sqlx::query(BY_TASK_SQL)
            .bind(task_id)
            .fetch_all(&self.pool)
            .await?;
        let mut out = Vec::new();
        for r in rows {
            let msg_key: String = r.get(0);
            let recipient: String = r.get(1);
            let state: DeliveryState = serde_json::from_str(&r.get::<String, _>(2))
                .map_err(|e| Reject::BadEnvelope(format!("stored delivery: {e}")))?;
            let envelope: Envelope = serde_json::from_str(&r.get::<String, _>(4))
                .map_err(|e| Reject::BadEnvelope(format!("stored payload: {e}")))?;
            if envelope.from != viewer && recipient != viewer {
                continue;
            }
            out.push(TaskMessage {
                msg_key,
                offset: r.get::<i64, _>(3) as u64,
                recipient,
                phase: state.phase,
                generation: state.generation,
                holder: state.holder,
                envelope,
            });
        }
        Ok(out)
    }

    /// Current delivery state.
    pub async fn delivery(
        &self,
        msg_key: &str,
        recipient: &str,
    ) -> Result<DeliveryState, InboxError> {
        let mut conn = self.pool.acquire().await?;
        read_delivery(&mut conn, msg_key, recipient)
            .await?
            .map(|(s, _)| s)
            .ok_or_else(|| InboxError::NoSuchDelivery {
                msg_key: msg_key.into(),
                recipient: recipient.into(),
            })
    }

    /// A producer's current epoch: the higher of its last appended epoch and
    /// its highest claimed one (`None` = never appended or claimed). A
    /// restarted sender journals under `epoch + 1`, which fences its old self.
    pub async fn producer_epoch(&self, producer: &str) -> Result<Option<u64>, sqlx::Error> {
        let e: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(epoch) FROM (
                 SELECT epoch FROM inbox_producer_state WHERE producer = ?1
                 UNION ALL
                 SELECT epoch FROM inbox_epoch_claim WHERE producer = ?1
             )",
        )
        .bind(producer)
        .fetch_one(&self.pool)
        .await?;
        Ok(e.map(|e| e as u64))
    }

    /// Atomically claim the next epoch for `producer` (bd-epoch-claim-z5m5).
    ///
    /// One `BEGIN IMMEDIATE` transaction reads the highest epoch the log
    /// knows (appended or already claimed) and records the next one for
    /// `holder`, so two callers, in one process or many, never receive the
    /// same epoch. The first claim for a producer is epoch 0. A claimed epoch
    /// fences every lower one at once ([`AppendError::StaleEpoch`]); the
    /// claimant then appends from seq 0. An epoch whose holder never appends
    /// is simply skipped: epochs are unique and increasing, not contiguous in
    /// the log.
    pub async fn claim_epoch(
        &self,
        producer: &str,
        holder: &str,
    ) -> Result<EpochClaim, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let previous: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(epoch) FROM (
                 SELECT epoch FROM inbox_producer_state WHERE producer = ?1
                 UNION ALL
                 SELECT epoch FROM inbox_epoch_claim WHERE producer = ?1
             )",
        )
        .bind(producer)
        .fetch_one(&mut *tx)
        .await?;
        let epoch = previous.map_or(0, |p| p + 1);
        sqlx::query(
            "INSERT INTO inbox_epoch_claim (producer, epoch, holder, claimed_ts_us) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (producer) DO UPDATE SET \
                 epoch = excluded.epoch, holder = excluded.holder, \
                 claimed_ts_us = excluded.claimed_ts_us",
        )
        .bind(producer)
        .bind(epoch)
        .bind(holder)
        .bind(now_us())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(EpochClaim {
            producer: producer.to_string(),
            epoch: epoch as u64,
            previous: previous.map(|p| p as u64),
        })
    }

    /// Head offset of the reader's inbox (0 = empty): `head > cursor` is the
    /// level the wake decision reads (redesign §4.1).
    pub async fn head(&self, recipient: &str) -> Result<u64, sqlx::Error> {
        let h: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(log_offset) FROM inbox_log WHERE stream = ?1 AND recipient = ?2",
        )
        .bind(INBOX_STREAM)
        .bind(recipient)
        .fetch_one(&self.pool)
        .await?;
        Ok(h.map_or(0, |h| h as u64))
    }
}

async fn read_delivery(
    conn: &mut sqlx::SqliteConnection,
    msg_key: &str,
    recipient: &str,
) -> Result<Option<(DeliveryState, Option<String>)>, sqlx::Error> {
    let row = sqlx::query("SELECT state_json, evidence_json FROM inbox_delivery WHERE msg_key = ?1 AND recipient = ?2")
        .bind(msg_key)
        .bind(recipient)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|r| {
        let s: DeliveryState = serde_json::from_str(&r.get::<String, _>(0))
            .expect("state_json is written by write_delivery");
        (s, r.get::<Option<String>, _>(1))
    }))
}

async fn write_delivery(
    conn: &mut sqlx::SqliteConnection,
    msg_key: &str,
    s: &DeliveryState,
    task_id: Option<&str>,
    evidence: Option<&serde_json::Value>,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO inbox_delivery (msg_key, recipient, phase, gen, task_id, state_json, evidence_json, updated_ts_us) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
         ON CONFLICT (msg_key, recipient) DO UPDATE SET phase = excluded.phase, gen = excluded.gen, \
         task_id = COALESCE(excluded.task_id, inbox_delivery.task_id), state_json = excluded.state_json, \
         evidence_json = COALESCE(excluded.evidence_json, inbox_delivery.evidence_json), \
         updated_ts_us = excluded.updated_ts_us",
    )
    .bind(msg_key)
    .bind(&s.recipient)
    .bind(s.phase.as_str())
    .bind(i64::from(s.generation))
    .bind(task_id)
    .bind(serde_json::to_string(s).expect("state serializes"))
    .bind(evidence.map(|e| e.to_string()))
    .bind(now)
    .execute(conn)
    .await?;
    Ok(())
}

/// The verbs a reader may run on a delivery now (the projection the CLI
/// prints as "Allowed now").
pub fn allowed_now(s: &DeliveryState, msg_key: &str, env: &Envelope) -> Vec<String> {
    match (s.phase, s.class) {
        (Phase::Delivered, Class::Request | Class::Reply | Class::Task | Class::Control) => {
            vec![
                format!("toron ack {msg_key} accept"),
                format!("toron ack {msg_key} reject --reason <why>"),
            ]
        }
        (Phase::Acknowledged, Class::Task) => {
            let task = env.task.as_ref().map_or("<task>", |t| t.0.as_str());
            vec![format!("toron done {task} --evidence <ref>")]
        }
        (Phase::Sent, _) => vec![format!("toron next   (marks {msg_key} delivered)")],
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// ToronLog: the store as a kernel DurableLog, bridged to the async store.
// ---------------------------------------------------------------------------

enum Req {
    Append {
        json: String,
        reply: std::sync::mpsc::Sender<Result<Offset, AppendError>>,
    },
    Read {
        after: Option<u64>,
        reply: std::sync::mpsc::Sender<Result<Vec<(Offset, String)>, AppendError>>,
    },
    Lookup {
        msg: MsgId,
        reply: std::sync::mpsc::Sender<Result<Option<u64>, AppendError>>,
    },
}

/// How long a bridge call waits for the store before answering
/// `Unavailable` (the outcome is then unknown: reconcile).
pub const BRIDGE_TIMEOUT: Duration = Duration::from_secs(30);

/// One stream of the inbox store as a [`kernel::DurableLog`].
#[derive(Clone)]
pub struct ToronLog {
    stream: String,
    tx: tokio::sync::mpsc::UnboundedSender<Req>,
}

impl ToronLog {
    /// Spawn the store task for `stream` on `rt` and return the sync handle.
    /// Hand the handle to a `Loop` running on its own OS thread.
    pub fn spawn(
        store: InboxStore,
        stream: impl Into<String>,
        rt: &tokio::runtime::Handle,
    ) -> Self {
        let stream = stream.into();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Req>();
        let task_stream = stream.clone();
        rt.spawn(async move {
            while let Some(req) = rx.recv().await {
                match req {
                    Req::Append { json, reply } => {
                        let r = store
                            .append_json(&task_stream, &json)
                            .await
                            .map(|a| mint(a.offset));
                        let _ = reply.send(r);
                    }
                    Req::Read { after, reply } => {
                        let r = store
                            .read_stream(&task_stream, after)
                            .await
                            .map(|rows| rows.into_iter().map(|(o, p)| (mint(o), p)).collect());
                        let _ = reply.send(r);
                    }
                    Req::Lookup { msg, reply } => {
                        let _ = reply.send(store.lookup(&msg).await.map_err(unavailable));
                    }
                }
            }
        });
        Self { stream, tx }
    }

    /// The stream this handle appends to.
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// The reconcile query through the bridge: the offset holding `msg`.
    pub fn lookup(&self, msg: &MsgId) -> Result<Option<u64>, AppendError> {
        let msg = msg.clone();
        self.call(|reply| Req::Lookup { msg, reply })
    }

    fn call<T>(
        &self,
        make: impl FnOnce(std::sync::mpsc::Sender<Result<T, AppendError>>) -> Req,
    ) -> Result<T, AppendError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(AppendError::Unavailable(
                "ToronLog called on a Tokio runtime thread: host the loop on a dedicated OS thread (F2)".into(),
            ));
        }
        let (reply, rx) = std::sync::mpsc::channel();
        self.tx
            .send(make(reply))
            .map_err(|_| unavailable("inbox store task is gone"))?;
        rx.recv_timeout(BRIDGE_TIMEOUT)
            .map_err(|e| unavailable(format!("inbox store did not answer: {e}")))?
    }
}

impl DurableLog for ToronLog {
    fn append_json(&self, envelope: &str) -> Result<Offset, AppendError> {
        let json = envelope.to_string();
        self.call(|reply| Req::Append { json, reply })
    }

    fn read_from(&self, from: &Offset) -> Result<Vec<(Offset, String)>, AppendError> {
        let after = match from {
            Offset::Start => None,
            Offset::Now => return Ok(Vec::new()),
            Offset::At(raw) => Some(raw.parse::<u64>().map_err(|_| AppendError::UnknownOffset)?),
        };
        self.call(|reply| Req::Read { after, reply })
    }
}
