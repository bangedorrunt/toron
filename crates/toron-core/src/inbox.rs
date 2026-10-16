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
use sqlx::{Row as _, SqliteConnection, SqlitePool};

use crate::authority::{
    Authority, CONTEXT_CONTRACT, ROLE_CONTRACT, Relation, Role, Standing, VERDICT_CONTRACT,
};
use crate::contracts::{
    self, Body, context_v1::ContextV1, lead_v1::LeadV1, role_v1::RoleV1, steer_v1::SteerV1,
};
use crate::delivery::{DeliveryEvent, DeliveryFsm, DeliveryState, Phase, Reject};
use crate::envelope::{Class, Envelope, MsgId};
use crate::kernel::{self, AppendError, DurableLog, Offset};
use crate::topology::{self, Topology};

/// The SQLite effect journal (`kernel::JournalStore`), named here beside
/// [`ToronLog`] as the build plan lists them.
pub use crate::journal::{JournalDb, ToronJournal};

/// The stream envelopes live on.
pub const INBOX_STREAM: &str = "inbox";
/// The stream a sender loop's durable intents live on.
pub const JOURNAL_STREAM: &str = "journal";

/// The comms flag (ADR-0041), decided from the `TORON_COMMS_INBOX` value and
/// the `[comms] inbox_log` config key (bd-652a). Pure, so the whole table is
/// testable.
///
/// * The env var wins whenever it is set to anything but the empty string:
///   `1|true|on` is on, `0|false|off` is off, and any other value is off
///   (fail closed; a typo never turns the log on).
/// * Unset or empty, the config key decides.
/// * Neither: off.
pub fn resolve_enabled(env: Option<&str>, config: Option<bool>) -> bool {
    match env {
        Some("1" | "true" | "on") => true,
        Some("") | None => config.unwrap_or(false),
        Some(_) => false,
    }
}

/// The flag from the environment alone (no config in hand). Off by default;
/// the verbs refuse while it is off.
pub fn enabled() -> bool {
    enabled_with(None)
}

/// The flag with the loaded `[comms] inbox_log` value (`None` = key absent).
pub fn enabled_with(config: Option<bool>) -> bool {
    resolve_enabled(std::env::var("TORON_COMMS_INBOX").ok().as_deref(), config)
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
    #[error("comms inbox is off: set TORON_COMMS_INBOX=1 or `[comms] inbox_log = true`")]
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

/// One stored envelope, as [`InboxStore::read_triple`] reports it.
#[derive(Clone, Debug, Serialize)]
pub struct StoredEnvelope {
    /// `producer:epoch:seq`.
    pub msg_key: String,
    /// Log offset.
    pub offset: u64,
    /// The recipient stream this row was appended for (an npub or group).
    pub recipient: String,
    /// True when a newer epoch of this producer exists (appended or claimed),
    /// so nothing more can be appended at this envelope's epoch. The row is
    /// still stored and still readable.
    pub fenced: bool,
    /// The envelope exactly as stored.
    pub envelope: Envelope,
}

/// The triple read (bd-read-by-producer-seq-uozq): one probe of the
/// `UNIQUE (producer, epoch, seq)` index. Exported so a test can pin its plan.
pub const BY_TRIPLE_SQL: &str = "SELECT msg_key, log_offset, recipient, payload \
     FROM inbox_log WHERE producer = ?1 AND epoch = ?2 AND seq = ?3 AND stream = 'inbox'";

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

/// True when a peeked batch holds an urgent steer. Only `steer@1` whose
/// `priority` field is the string `urgent` counts. Notice text is not read.
pub fn urgent_pending(items: &[InboxItem]) -> bool {
    items.iter().any(|it| {
        it.envelope.contract.name == "steer"
            && it.envelope.contract.major == 1
            && it
                .envelope
                .body
                .get("priority")
                .and_then(serde_json::Value::as_str)
                == Some("urgent")
    })
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
    PayloadConflict {
        msg_key: String,
    },
    /// A typed refusal of the envelope (authority, role fence).
    Reject(Reject),
}

impl From<AppendError> for Refusal {
    fn from(e: AppendError) -> Self {
        Self::Log(e)
    }
}

impl From<Reject> for Refusal {
    fn from(r: Reject) -> Self {
        Self::Reject(r)
    }
}

impl From<Refusal> for InboxError {
    fn from(r: Refusal) -> Self {
        match r {
            Refusal::Log(e) => e.into(),
            Refusal::PayloadConflict { msg_key } => Self::PayloadConflict { msg_key },
            Refusal::Reject(r) => r.into(),
        }
    }
}

fn unavailable(e: impl std::fmt::Display) -> AppendError {
    AppendError::Unavailable(e.to_string())
}

/// The current `lead@1` row for one context.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeadRow {
    /// Who holds the lease (the fact's `holder`, not checked as an npub).
    pub holder: String,
    /// The epoch a steer must name while this row is current.
    pub sup_epoch: u64,
}

/// Async SQLite-backed inbox store (the index pool).
#[derive(Clone, Debug)]
pub struct InboxStore {
    pool: SqlitePool,
    /// The operator key's npub (64 lowercase hex), when this store knows it.
    /// The operator acts with captain authority and alone starts a context.
    operator: Option<String>,
    authority: Authority,
}

impl InboxStore {
    /// Wrap a migrated index pool. The store enforces [`Authority::standard`]
    /// and knows no operator: open it through `Mailbox::inbox_store*`, or add
    /// [`Self::with_operator`], to let the operator key start contexts and act
    /// in governed ones.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            operator: None,
            authority: Authority::standard(),
        }
    }

    /// The operator key's npub (64 lowercase hex). Without it nobody can
    /// append a context's first role fact, so no context can become governed
    /// through this store.
    #[must_use]
    pub fn with_operator(mut self, npub: impl Into<String>) -> Self {
        self.operator = Some(npub.into());
        self
    }

    /// Replace the authority table (tests and negative controls only).
    #[must_use]
    pub fn with_authority(mut self, authority: Authority) -> Self {
        self.authority = authority;
        self
    }

    /// The operator npub this store was given.
    pub fn operator(&self) -> Option<&str> {
        self.operator.as_deref()
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
                // The vendored enum is closed: through the kernel entry a typed
                // refusal is its text. `append_envelope` returns it typed.
                Refusal::Reject(r) => AppendError::NotAnEnvelope(r.to_string()),
            })
    }

    async fn append_checked(&self, stream: &str, json: &str) -> Result<Appended, Refusal> {
        let (producer, epoch, seq, payload) = parse_l16(json)?;
        let (recipient, msg_key, delivery, envelope) = if stream == INBOX_STREAM {
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
            let task_id = env.task.clone().map(|t| t.0);
            (stream_key, env.msg.key(), Some((d, task_id)), Some(env))
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
        // Authority (Phase 4.1): after the fence and the replay answer, before
        // anything is written. A replay of a message already in the log was
        // authorised when it landed and stays idempotent.
        let facts = match &envelope {
            Some(env) => self.authorize_in(&mut tx, env).await?,
            None => AppendFacts::none(),
        };
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
        if let (Some(role), Some(env)) = (&facts.role, &envelope) {
            write_role(&mut tx, env, role, now)
                .await
                .map_err(unavailable)?;
        }
        if let (Some(context), Some(env)) = (&facts.context, &envelope) {
            write_context(&mut tx, env, context)
                .await
                .map_err(unavailable)?;
        }
        if let (Some(steer), Some(env)) = (&facts.steer, &envelope) {
            write_steer(&mut tx, env, steer)
                .await
                .map_err(unavailable)?;
            // Redirect fences the task's current gen; cancel ends it. Pause
            // and resume record the steer and leave the gen alone.
            apply_steer(&mut tx, env, steer, now)
                .await
                .map_err(unavailable)?;
        }
        if let (Some(lead), Some(env)) = (&facts.lead, &envelope) {
            write_lead(&mut tx, env, lead).await.map_err(unavailable)?;
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
        self.append_checked(INBOX_STREAM, &l16_json(env))
            .await
            .map_err(InboxError::from)
    }

    /// Would [`Self::append_envelope`] pass the authority check for `env`
    /// right now? A dry run for senders (flywheel's pre-flight): it reads the
    /// role facts and writes nothing. It does not look at the epoch, the seq
    /// or the replay state, which the append decides. A role fact is also
    /// checked against its fence (`gen`).
    pub async fn authorize(&self, env: &Envelope) -> Result<(), InboxError> {
        env.validate()?;
        let mut conn = self.pool.acquire().await?;
        self.authorize_in(&mut conn, env)
            .await
            .map(|_| ())
            .map_err(InboxError::from)
    }

    /// The authority check of one envelope inside the caller's connection
    /// (the append's transaction, or a plain connection for the dry run).
    /// Returns the role or context fact to project, when the envelope is one.
    async fn authorize_in(
        &self,
        conn: &mut SqliteConnection,
        env: &Envelope,
    ) -> Result<AppendFacts, Refusal> {
        let context = env.context.0.as_str();
        let governed = context_is_governed(conn, context)
            .await
            .map_err(unavailable)?;
        let standing = self
            .standing_in(conn, context, &env.from)
            .await
            .map_err(unavailable)?;
        let relation = if env.class == Class::Control && env.contract.name == VERDICT_CONTRACT {
            own_task_in(conn, env).await.map_err(unavailable)?
        } else {
            Relation::Other
        };
        self.authority
            .check_send(standing, governed, env.class, &env.contract, relation)?;
        let current_context = context_row(conn, context).await.map_err(unavailable)?;
        if env.class == Class::Control && env.contract.name == ROLE_CONTRACT {
            let Body::Role(role) = contracts::parse(env)? else {
                unreachable!("role@1 parses as a role body");
            };
            let current = role_row(conn, context, &role.npub)
                .await
                .map_err(unavailable)?;
            Authority::check_grant(
                standing,
                current.as_ref().map(|c| c.role),
                role.role,
                governed,
            )?;
            if let Some(cur) = &current
                && role.generation <= cur.generation
            {
                return Err(Reject::StaleGen {
                    sent: role.generation,
                    current: cur.generation,
                }
                .into());
            }
            return Ok(AppendFacts {
                role: Some(role),
                ..AppendFacts::none()
            });
        }
        if env.class == Class::Control && env.contract.name == CONTEXT_CONTRACT {
            let Body::Context(fact) = contracts::parse(env)? else {
                unreachable!("context@1 parses as a context body");
            };
            Authority::check_set_context(standing)?;
            if let Some(cur) = &current_context
                && fact.generation <= cur.generation
            {
                return Err(Reject::StaleGen {
                    sent: fact.generation,
                    current: cur.generation,
                }
                .into());
            }
            return Ok(AppendFacts {
                context: Some(fact),
                ..AppendFacts::none()
            });
        }
        if env.class == Class::Control && env.contract.name == "lead" {
            let Body::Lead(fact) = contracts::parse(env)? else {
                unreachable!("lead@1 parses as a lead body");
            };
            let current = lead_epoch(conn, context).await.map_err(unavailable)?;
            Authority::check_set_lead(standing, current.is_some())?;
            if let Some(cur) = current
                && fact.sup_epoch <= cur
            {
                return Err(Reject::StaleSupEpoch {
                    sent: fact.sup_epoch,
                    current: cur,
                }
                .into());
            }
            return Ok(AppendFacts {
                lead: Some(fact),
                ..AppendFacts::none()
            });
        }
        if env.class == Class::Control && env.contract.name == "steer" {
            let Body::Steer(steer) = contracts::parse(env)? else {
                unreachable!("steer@1 parses as a steer body");
            };
            // No lead row: do not invent an epoch. A row fences the steer to
            // that epoch exactly; a lower or higher one writes nothing.
            if let Some(cur) = lead_epoch(conn, context).await.map_err(unavailable)?
                && steer.sup_epoch != cur
            {
                return Err(Reject::StaleSupEpoch {
                    sent: steer.sup_epoch,
                    current: cur,
                }
                .into());
            }
            // A steer is the control fact itself, not a swarm send. Return
            // before the supervisor-topology refusal, the same way a role
            // fact does, so it is stored rather than answered `NotYet`.
            return Ok(AppendFacts {
                steer: Some(steer),
                ..AppendFacts::none()
            });
        }
        if env.class == Class::Control
            && (env.contract.name == "goal" || env.contract.name == "escalate")
        {
            // Same exemption as steer: a supervisor context stores the fact.
            // There is no projection table; the log row is the record.
            // `check_send` above still refuses a crew `goal`.
            return Ok(AppendFacts::none());
        }
        // A supervisor context is stored, not projected. Do not run the send
        // as a swarm. Role and context facts above are the governance log.
        if current_context.is_some_and(|c| c.topology == Topology::Supervisor) {
            return Err(Reject::NotYet {
                what: "supervisor".into(),
            }
            .into());
        }
        Ok(AppendFacts::none())
    }

    /// Who `npub` is in `context`: the operator, a role holder, or unroled.
    async fn standing_in(
        &self,
        conn: &mut SqliteConnection,
        context: &str,
        npub: &str,
    ) -> Result<Standing, sqlx::Error> {
        if self.operator.as_deref() == Some(npub) {
            return Ok(Standing::Operator);
        }
        Ok(match role_row(conn, context, npub).await? {
            Some(fact) => Standing::Holds(fact.role),
            None => Standing::Unroled,
        })
    }

    /// Who `npub` is in `context` (operator, role holder, or unroled).
    pub async fn standing(&self, context: &str, npub: &str) -> Result<Standing, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.standing_in(&mut conn, context, npub).await
    }

    /// The current topology fact for `context`, if any.
    pub async fn context_of(&self, context: &str) -> Result<Option<ContextFact>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        context_row(&mut conn, context).await
    }

    /// The current lead row for `context`, if a `lead@1` fact has been stored.
    /// This is the epoch authority. Callers append a strictly higher
    /// `sup_epoch`; they do not invent one beside this row.
    pub async fn lead_of(&self, context: &str) -> Result<Option<LeadRow>, sqlx::Error> {
        let row = sqlx::query("SELECT holder, sup_epoch FROM inbox_lead WHERE context = ?1")
            .bind(context)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| {
            let epoch: i64 = r.get(1);
            LeadRow {
                holder: r.get(0),
                sup_epoch: epoch as u64,
            }
        }))
    }

    /// The current role fact for `npub` in `context`, if any.
    pub async fn role_of(
        &self,
        context: &str,
        npub: &str,
    ) -> Result<Option<RoleFact>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        role_row(&mut conn, context, npub).await
    }

    /// Every current role fact of `context`, ordered by npub. Empty = the
    /// context is ungoverned.
    pub async fn roles(&self, context: &str) -> Result<Vec<RoleFact>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT context, npub, role, gen, msg_key, issuer FROM inbox_role \
             WHERE context = ?1 ORDER BY npub",
        )
        .bind(context)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(role_fact_of).collect()
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
                .map_err(|e| Reject::bad_envelope(format!("stored payload: {e}")))?;
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
            let topology = context_row(&mut tx, &envelope.context.0)
                .await?
                .map(|c| c.topology);
            let role = role_row(&mut tx, &envelope.context.0, reader)
                .await?
                .map(|r| r.role);
            // Both facts, or today's phase table. No fact: project is not called.
            let mut allowed_now = allowed_now_for(&state, &msg_key, &envelope, topology, role);
            // The head item only. `next` already refused a reader who is not
            // the recipient, so this is their own stream. No pending steer:
            // `allowed_now_for` is left as it was.
            if items.is_empty()
                && let Some(task) = &envelope.task
                && let Some((steer_key, _)) =
                    pending_steer(&mut tx, &envelope.context.0, &task.0).await?
            {
                allowed_now = steer_pending_verbs(&steer_key);
            }
            items.push(InboxItem {
                offset,
                allowed_now,
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
        let env = load_env(&mut tx, msg_key).await?;
        if let Some(env) = &env
            && let Some(reject) = steer_blocks(&mut tx, msg_key, env, event).await?
        {
            return Err(reject.into());
        }
        let next = DeliveryFsm::step(&s, event)?;
        // Accepting the steer message is what clears `pending`. A reject
        // leaves it pending: the crew has not taken the new instruction.
        if let Some(env) = &env
            && env.contract.name == "steer"
            && matches!(event, DeliveryEvent::Accept { .. })
        {
            sqlx::query(
                "UPDATE inbox_steer SET state = 'acked' WHERE msg_key = ?1 AND state = 'pending'",
            )
            .bind(msg_key)
            .execute(&mut *tx)
            .await?;
        }
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
        // The task row, not a later steer/control that names the same task id.
        // A steer is class Control; `done` fences the Task delivery.
        let key: Option<String> = sqlx::query_scalar(
            "SELECT msg_key FROM inbox_delivery WHERE task_id = ?1 AND recipient = ?2 \
             AND json_extract(state_json, '$.class') = 'task' \
             ORDER BY updated_ts_us DESC LIMIT 1",
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
        self.authorize_done(reader, &msg_key).await?;
        let s = self.delivery(&msg_key, reader).await?;
        // The gen the crew was assigned is the task envelope's, which a
        // redirect does not rewrite. The delivery gen moves. Passing the
        // assignment gen is what makes the old one `StaleGen` after a bump;
        // with no steer the two are equal, so `done` is unchanged.
        let generation = assignment_gen(&self.pool, &msg_key)
            .await?
            .unwrap_or(s.generation);
        let ev = serde_json::json!({ "evidence": evidence });
        let next = self
            .step(
                &msg_key,
                reader,
                &DeliveryEvent::Done {
                    by: reader.into(),
                    generation,
                },
                Some(&ev),
            )
            .await?;
        Ok((msg_key, next))
    }

    /// `done` is the one non-envelope verb the authority table names: a
    /// verifier or a peer may not run it in a governed context.
    async fn authorize_done(&self, reader: &str, msg_key: &str) -> Result<(), InboxError> {
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM inbox_log WHERE msg_key = ?1 AND stream = 'inbox' LIMIT 1",
        )
        .bind(msg_key)
        .fetch_optional(&self.pool)
        .await?;
        let Some(payload) = payload else {
            return Ok(());
        };
        let env: Envelope = serde_json::from_str(&payload)
            .map_err(|e| Reject::bad_envelope(format!("stored payload: {e}")))?;
        let mut conn = self.pool.acquire().await?;
        let governed = context_is_governed(&mut conn, &env.context.0).await?;
        let standing = self.standing_in(&mut conn, &env.context.0, reader).await?;
        self.authority.check_done(standing, governed)?;
        Ok(())
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
                .map_err(|e| Reject::bad_envelope(format!("stored delivery: {e}")))?;
            let envelope: Envelope = serde_json::from_str(&r.get::<String, _>(4))
                .map_err(|e| Reject::bad_envelope(format!("stored payload: {e}")))?;
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

    /// The envelope stored under `msg` (`producer:epoch:seq`), whether or not
    /// its epoch has since been fenced (bd-read-by-producer-seq-uozq). A
    /// sender that must check what a recovered or replayed triple actually
    /// says (a claimed epoch fences every older one, so a re-append of the
    /// old triple is refused as `StaleEpoch` before content is compared) reads
    /// it here instead. `viewer` sees the envelope only if it sent it or it was
    /// appended for it; anyone else gets `None`, the same answer as for an
    /// unknown triple. Read-only.
    pub async fn read_triple(
        &self,
        msg: &MsgId,
        viewer: &str,
    ) -> Result<Option<StoredEnvelope>, InboxError> {
        let Some(row) = sqlx::query(BY_TRIPLE_SQL)
            .bind(&msg.producer)
            .bind(msg.epoch as i64)
            .bind(msg.seq as i64)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let recipient: String = row.get(2);
        let envelope: Envelope = serde_json::from_str(&row.get::<String, _>(3))
            .map_err(|e| Reject::bad_envelope(format!("stored payload: {e}")))?;
        if envelope.from != viewer && recipient != viewer {
            return Ok(None);
        }
        let fenced = self
            .producer_epoch(&msg.producer)
            .await?
            .is_some_and(|current| current > msg.epoch);
        Ok(Some(StoredEnvelope {
            msg_key: row.get(0),
            offset: row.get::<i64, _>(1) as u64,
            recipient,
            fenced,
            envelope,
        }))
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

/// Facts an accepted append projects into a side table. Both empty for an
/// ordinary send.
struct AppendFacts {
    role: Option<RoleV1>,
    context: Option<ContextV1>,
    steer: Option<SteerV1>,
    lead: Option<LeadV1>,
}

impl AppendFacts {
    fn none() -> Self {
        Self {
            role: None,
            context: None,
            steer: None,
            lead: None,
        }
    }
}

/// The lead epoch on record for `context`, if a lead fact has been stored.
async fn lead_epoch(
    conn: &mut SqliteConnection,
    context: &str,
) -> Result<Option<u64>, sqlx::Error> {
    let epoch: Option<i64> =
        sqlx::query_scalar("SELECT sup_epoch FROM inbox_lead WHERE context = ?1")
            .bind(context)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(epoch.map(|e| e as u64))
}

/// Project an accepted lead fact. The caller already fenced `sup_epoch`.
async fn write_lead(
    conn: &mut SqliteConnection,
    env: &Envelope,
    fact: &LeadV1,
) -> Result<(), sqlx::Error> {
    let epoch =
        i64::try_from(fact.sup_epoch).map_err(|e| sqlx::Error::Decode(e.to_string().into()))?;
    sqlx::query(
        "INSERT INTO inbox_lead (context, holder, sup_epoch, msg_key) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT (context) DO UPDATE SET holder = excluded.holder, \
         sup_epoch = excluded.sup_epoch, msg_key = excluded.msg_key",
    )
    .bind(&env.context.0)
    .bind(&fact.holder)
    .bind(epoch)
    .bind(env.msg.key())
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The envelope stored under `msg_key`, if the row is one. A bad payload is
/// `None`: the steer gate then does not fire, and today's transition stands.
async fn load_env(
    conn: &mut SqliteConnection,
    msg_key: &str,
) -> Result<Option<Envelope>, sqlx::Error> {
    let payload: Option<String> = sqlx::query_scalar(
        "SELECT payload FROM inbox_log WHERE msg_key = ?1 AND stream = 'inbox' LIMIT 1",
    )
    .bind(msg_key)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(serde_json::from_str(&payload).ok())
}

/// The pending steer for `(context, task)`, if one is waiting: `(msg_key, op)`.
async fn pending_steer(
    conn: &mut SqliteConnection,
    context: &str,
    task: &str,
) -> Result<Option<(String, String)>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT msg_key, op FROM inbox_steer \
         WHERE context = ?1 AND task = ?2 AND state = 'pending' \
         ORDER BY rowid DESC LIMIT 1",
    )
    .bind(context)
    .bind(task)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|r| (r.get(0), r.get(1))))
}

/// Crew `done` / `progress` / a non-steer ack write nothing while a steer on
/// this task is `pending`. The steer message, and an ack of it, are allowed.
async fn steer_blocks(
    conn: &mut SqliteConnection,
    msg_key: &str,
    env: &Envelope,
    event: &DeliveryEvent,
) -> Result<Option<Reject>, sqlx::Error> {
    let crew_verb = matches!(
        event,
        DeliveryEvent::Done { .. }
            | DeliveryEvent::Progress { .. }
            | DeliveryEvent::Accept { .. }
            | DeliveryEvent::Refuse { .. }
    );
    if !crew_verb {
        return Ok(None);
    }
    if env.contract.name == "steer"
        && matches!(
            event,
            DeliveryEvent::Accept { .. } | DeliveryEvent::Refuse { .. }
        )
    {
        return Ok(None);
    }
    let Some(task) = &env.task else {
        return Ok(None);
    };
    let Some((id, op)) = pending_steer(conn, &env.context.0, &task.0).await? else {
        return Ok(None);
    };
    if id == msg_key {
        return Ok(None);
    }
    Ok(Some(Reject::SteerPending { id, op }))
}

/// The generation on the task envelope (the gen the assignee was given).
/// `None` when the row is not an envelope; the caller keeps the delivery gen.
async fn assignment_gen(pool: &SqlitePool, msg_key: &str) -> Result<Option<u32>, sqlx::Error> {
    let payload: Option<String> = sqlx::query_scalar(
        "SELECT payload FROM inbox_log WHERE msg_key = ?1 AND stream = 'inbox' LIMIT 1",
    )
    .bind(msg_key)
    .fetch_optional(pool)
    .await?;
    let Some(payload) = payload else {
        return Ok(None);
    };
    let Ok(env) = serde_json::from_str::<Envelope>(&payload) else {
        return Ok(None);
    };
    Ok(Some(env.generation))
}

/// Project an accepted steer and retire the previous pending one for this task.
async fn write_steer(
    conn: &mut SqliteConnection,
    env: &Envelope,
    steer: &SteerV1,
) -> Result<(), sqlx::Error> {
    let Some(task) = &env.task else {
        return Ok(());
    };
    let epoch =
        i64::try_from(steer.sup_epoch).map_err(|e| sqlx::Error::Decode(e.to_string().into()))?;
    sqlx::query(
        "UPDATE inbox_steer SET state = 'superseded' \
         WHERE context = ?1 AND task = ?2 AND state = 'pending'",
    )
    .bind(&env.context.0)
    .bind(&task.0)
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO inbox_steer \
         (msg_key, context, task, gen, op, priority, sup_epoch, state) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending')",
    )
    .bind(env.msg.key())
    .bind(&env.context.0)
    .bind(&task.0)
    .bind(i64::from(env.generation))
    .bind(&steer.op)
    .bind(&steer.priority)
    .bind(epoch)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Redirect bumps the task delivery's gen in place (the phase stays, so a
/// later `done` at the old gen is `StaleGen` rather than terminal).
/// Cancel moves it to `Cancelled`. Pause and resume do neither.
async fn apply_steer(
    conn: &mut SqliteConnection,
    env: &Envelope,
    steer: &SteerV1,
    now: i64,
) -> Result<(), sqlx::Error> {
    let Some(task) = &env.task else {
        return Ok(());
    };
    let rows = sqlx::query("SELECT msg_key, state_json FROM inbox_delivery WHERE task_id = ?1")
        .bind(&task.0)
        .fetch_all(&mut *conn)
        .await?;
    for row in rows {
        let key: String = row.get(0);
        let mut state: DeliveryState = serde_json::from_str(&row.get::<String, _>(1))
            .map_err(|e| sqlx::Error::Decode(e.to_string().into()))?;
        if state.class != Class::Task || state.is_terminal() {
            continue;
        }
        match steer.op.as_str() {
            "redirect" => state.generation = state.generation.saturating_add(1),
            "cancel" => state.phase = Phase::Cancelled,
            _ => continue,
        }
        write_delivery(conn, &key, &state, Some(&task.0), None, now).await?;
    }
    Ok(())
}

/// One current topology fact (the `inbox_context` projection of a `context@1`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ContextFact {
    /// The context.
    pub context: String,
    /// Its choreography.
    pub topology: Topology,
    /// The fence of this fact (wire name `gen`).
    #[serde(rename = "gen")]
    pub generation: u32,
    /// The log message that carried the fact (`producer:epoch:seq`).
    pub msg_key: String,
}

fn context_fact_of(r: &sqlx::sqlite::SqliteRow) -> Result<ContextFact, sqlx::Error> {
    let topology: String = r.get(1);
    Ok(ContextFact {
        context: r.get(0),
        topology: Topology::parse(&topology).ok_or_else(|| {
            sqlx::Error::Decode(
                format!("inbox_context holds an unknown topology {topology:?}").into(),
            )
        })?,
        generation: r.get::<i64, _>(2) as u32,
        msg_key: r.get(3),
    })
}

async fn context_row(
    conn: &mut SqliteConnection,
    context: &str,
) -> Result<Option<ContextFact>, sqlx::Error> {
    let row =
        sqlx::query("SELECT context, topology, gen, msg_key FROM inbox_context WHERE context = ?1")
            .bind(context)
            .fetch_optional(conn)
            .await?;
    row.as_ref().map(context_fact_of).transpose()
}

async fn write_context(
    conn: &mut SqliteConnection,
    env: &Envelope,
    fact: &ContextV1,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO inbox_context (context, topology, gen, msg_key) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT (context) DO UPDATE SET topology = excluded.topology, \
         gen = excluded.gen, msg_key = excluded.msg_key",
    )
    .bind(&env.context.0)
    .bind(fact.topology.as_str())
    .bind(i64::from(fact.generation))
    .bind(env.msg.key())
    .execute(conn)
    .await?;
    Ok(())
}

/// One current role fact (the `inbox_role` projection of a `role@1`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RoleFact {
    /// The context the role holds in.
    pub context: String,
    /// The agent.
    pub npub: String,
    /// Its role.
    pub role: Role,
    /// The fence of this fact (wire name `gen`).
    #[serde(rename = "gen")]
    pub generation: u32,
    /// The log message that carried the fact (`producer:epoch:seq`).
    pub msg_key: String,
    /// Who issued it (an npub).
    pub issuer: String,
}

fn role_fact_of(r: &sqlx::sqlite::SqliteRow) -> Result<RoleFact, sqlx::Error> {
    let role: String = r.get(2);
    Ok(RoleFact {
        context: r.get(0),
        npub: r.get(1),
        role: Role::parse(&role).ok_or_else(|| {
            sqlx::Error::Decode(format!("inbox_role holds an unknown role {role:?}").into())
        })?,
        generation: r.get::<i64, _>(3) as u32,
        msg_key: r.get(4),
        issuer: r.get(5),
    })
}

async fn role_row(
    conn: &mut SqliteConnection,
    context: &str,
    npub: &str,
) -> Result<Option<RoleFact>, sqlx::Error> {
    let row = sqlx::query(
        "SELECT context, npub, role, gen, msg_key, issuer FROM inbox_role \
         WHERE context = ?1 AND npub = ?2",
    )
    .bind(context)
    .bind(npub)
    .fetch_optional(conn)
    .await?;
    row.as_ref().map(role_fact_of).transpose()
}

/// A context is governed once it holds any role fact.
async fn context_is_governed(
    conn: &mut SqliteConnection,
    context: &str,
) -> Result<bool, sqlx::Error> {
    let any: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM inbox_role WHERE context = ?1 LIMIT 1")
            .bind(context)
            .fetch_optional(conn)
            .await?;
    Ok(any.is_some())
}

/// Did `env.from` send a `task` envelope for `env.task`? (The supervisor's
/// "not a verdict on its own task" rule reads this.)
async fn own_task_in(conn: &mut SqliteConnection, env: &Envelope) -> Result<Relation, sqlx::Error> {
    let Some(task) = &env.task else {
        return Ok(Relation::Other);
    };
    let own: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM inbox_delivery d \
         JOIN inbox_log l ON l.msg_key = d.msg_key AND l.stream = 'inbox' \
         WHERE d.task_id = ?1 AND l.producer = ?2 \
         AND json_extract(d.state_json, '$.class') = 'task' LIMIT 1",
    )
    .bind(&task.0)
    .bind(&env.from)
    .fetch_optional(conn)
    .await?;
    Ok(if own.is_some() {
        Relation::OwnTask
    } else {
        Relation::Other
    })
}

/// Project an accepted role fact (inside the append's transaction).
async fn write_role(
    conn: &mut SqliteConnection,
    env: &Envelope,
    role: &RoleV1,
    now: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO inbox_role (context, npub, role, gen, msg_key, issuer, set_ts_us) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
         ON CONFLICT (context, npub) DO UPDATE SET role = excluded.role, gen = excluded.gen, \
         msg_key = excluded.msg_key, issuer = excluded.issuer, set_ts_us = excluded.set_ts_us",
    )
    .bind(&env.context.0)
    .bind(&role.npub)
    .bind(role.role.as_str())
    .bind(i64::from(role.generation))
    .bind(env.msg.key())
    .bind(&env.from)
    .bind(now)
    .execute(conn)
    .await?;
    Ok(())
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

/// The two ack lines a reader is limited to while a steer on this task is
/// still `pending`. `msg_key` is the steer message, not the task.
pub fn steer_pending_verbs(msg_key: &str) -> Vec<String> {
    vec![
        format!("toron ack {msg_key} accept"),
        format!("toron ack {msg_key} reject --reason <why>"),
    ]
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

/// [`allowed_now`] once a context has a topology fact and the viewer holds a role.
///
/// Either fact missing: today's phase table, and [`topology::project`] is not
/// called (a flag-off reader never reaches `next`, and a flag-on context with
/// no topology fact stays on the phase table). Both present: the projection.
/// Supervisor projects to nothing here, not to the swarm verb list; the typed
/// refusal of a send is [`Reject::NotYet`].
pub fn allowed_now_for(
    s: &DeliveryState,
    msg_key: &str,
    env: &Envelope,
    topology: Option<Topology>,
    role: Option<Role>,
) -> Vec<String> {
    let (Some(topology), Some(role)) = (topology, role) else {
        return allowed_now(s, msg_key, env);
    };
    let Ok(global) = topology::global_of(topology) else {
        return Vec::new();
    };
    match topology::project(global, role, 0) {
        Ok(now) => now.verbs,
        Err(_) => Vec::new(),
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
