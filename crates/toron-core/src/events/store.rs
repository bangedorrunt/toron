// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! Durable event subscriptions (ADR-0039 D3, bd-kz33f).
//! Identity is the hash of (principal, delivery, name, RFC-8785 args), so an
//! identical re-subscribe extends the same row instead of creating a new one.

use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;

pub const MAX_PIN_TTL_US: i64 = 24 * 60 * 60 * 1_000_000;
/// A delivery row is a hint, not a second bus: keep the table bounded by both
/// age and count per subscription, pruned on append and by the sweep (ADR-0039).
pub const MAX_DELIVERIES_PER_SUB: i64 = 1000;
pub const DELIVERY_TTL_US: i64 = 24 * 60 * 60 * 1_000_000;

/// Deterministic canonical JSON for identity hashing (RFC 8785 shape: sorted
/// object keys, no insignificant whitespace). Numbers use serde_json's stable
/// rendering; the hash is an identity key, not a signed value.
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => serde_json::to_string(s).expect("string encodes"),
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let body = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).expect("key encodes"),
                        canonical_json(&map[k])
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
    }
}

pub fn args_hash(args: &Value) -> String {
    hex::encode(Sha256::digest(canonical_json(args).as_bytes()))
}

/// The one gap/cursor rule shared by poll and stream: if `after` fell off the
/// pruned end, report a gap and resume from the oldest servable row.
pub fn gap_floor(after: u64, oldest: Option<i64>) -> (bool, i64) {
    let truncated = oldest.is_some_and(|oldest| (after as i64) < oldest - 1);
    let start = oldest
        .map(|o| (after as i64).max(o - 1))
        .unwrap_or(after as i64);
    (truncated, start)
}

fn subscription_id(principal: &str, delivery: &str, name: &str, hash: &str) -> String {
    hex::encode(Sha256::digest(
        format!("{principal}\0{delivery}\0{name}\0{hash}").as_bytes(),
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    pub id: String,
    pub principal: String,
    pub delivery: String,
    pub name: String,
    pub args_hash: String,
    pub cursor: u64,
    pub created_us: i64,
    pub expires_us: Option<i64>,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub seq: u64,
    pub payload: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    pub events: Vec<Delivery>,
    pub cursor: u64,
    pub has_more: bool,
    pub truncated: bool,
}

/// Count-only view for one participant. Deliberately carries no ids, no
/// payloads and no hints — a signal link learns how busy a seat is, never what
/// the messages say (bd-2ghz8 least-privilege).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrincipalCounts {
    pub pending: u64,
    pub urgent: u64,
    pub waking: u64,
    pub cursor: u64,
}

/// Subscription names whose undelivered events count as urgent.
fn urgent_name(name: &str) -> bool {
    matches!(name, "reservation.conflict" | "ack_sla_expired")
}

#[derive(Clone, Debug)]
pub struct SubscribeSpec {
    pub principal: String,
    pub is_pin: bool,
    pub delivery: String,
    pub name: String,
    pub args: Value,
    pub ttl_us: Option<i64>,
    pub now_us: i64,
}

#[async_trait]
pub trait SubscriptionStore: Send + Sync {
    /// Upsert on identity. A pin's TTL is capped at 24h; the operator may ask
    /// for no expiry (`None`). Identical re-subscribe keeps the same id and
    /// extends the expiry (AE2).
    async fn subscribe(&self, spec: &SubscribeSpec) -> anyhow::Result<Subscription>;

    async fn get(&self, id: &str) -> anyhow::Result<Option<Subscription>>;
    async fn list_active(&self) -> anyhow::Result<Vec<Subscription>>;
    async fn append(&self, id: &str, payload: &Value, now_us: i64) -> anyhow::Result<()>;
    async fn poll(&self, id: &str, after: u64, limit: u32) -> anyhow::Result<Page>;
    async fn unsubscribe(&self, id: &str, principal: &str) -> anyhow::Result<bool>;
    /// Execute a kernel Terminate effect against one subscription.
    async fn terminate(&self, id: &str) -> anyhow::Result<()>;
    /// Prune old delivery rows without touching subscription states.
    async fn prune_deliveries(&self, now_us: i64) -> anyhow::Result<()>;
    async fn sweep(&self, now_us: i64) -> anyhow::Result<usize>;

    /// Counts for one participant across its live subscriptions. Polls are
    /// used internally but only the integers escape — no payload, id or hint
    /// is returned to the caller (bd-2ghz8).
    async fn counts_for(&self, principal: &str, now_us: i64) -> anyhow::Result<PrincipalCounts> {
        let mut out = PrincipalCounts::default();
        for sub in self.list_active().await? {
            if sub.principal != principal || sub.expires_us.is_some_and(|e| e <= now_us) {
                continue;
            }
            out.cursor = out.cursor.max(sub.cursor);
            out.waking = out.waking.saturating_add(1);
            let page = self
                .poll(&sub.id, sub.cursor, MAX_DELIVERIES_PER_SUB as u32)
                .await?;
            let n = page.events.len() as u64;
            out.pending = out.pending.saturating_add(n);
            if urgent_name(&sub.name) {
                out.urgent = out.urgent.saturating_add(n);
            }
        }
        Ok(out)
    }
}

#[derive(Clone)]
pub struct SqliteSubscriptionStore {
    pool: SqlitePool,
}

impl SqliteSubscriptionStore {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

fn effective_ttl(requested: Option<i64>, is_pin: bool) -> anyhow::Result<Option<i64>> {
    match (requested, is_pin) {
        (None, true) => anyhow::bail!("a pin token cannot request a subscription without expiry"),
        (None, false) => Ok(None),
        (Some(ttl), true) if ttl <= 0 || ttl > MAX_PIN_TTL_US => Ok(Some(MAX_PIN_TTL_US)),
        (Some(ttl), _) => Ok(Some(ttl)),
    }
}

#[async_trait]
impl SubscriptionStore for SqliteSubscriptionStore {
    async fn subscribe(&self, spec: &SubscribeSpec) -> anyhow::Result<Subscription> {
        let SubscribeSpec {
            principal,
            is_pin,
            delivery,
            name,
            args,
            ttl_us,
            now_us,
        } = spec;
        let ttl = effective_ttl(*ttl_us, *is_pin)?;
        let now_us = *now_us;
        let expires = ttl.map(|t| now_us.saturating_add(t));
        let hash = args_hash(args);
        let id = subscription_id(principal, delivery, name, &hash);
        if let Some(row) = self.get(&id).await? {
            let expires = ttl.map(|t| now_us.saturating_add(t));
            sqlx::query(
                "UPDATE event_subscriptions SET expires_us = ?, state = 'active' WHERE id = ?",
            )
            .bind(expires)
            .bind(&id)
            .execute(&self.pool)
            .await?;
            return Ok(Subscription {
                expires_us: expires,
                ..row
            });
        }
        sqlx::query(
            "INSERT INTO event_subscriptions \
             (id, principal, delivery, name, args_hash, cursor, created_us, expires_us, state) \
             VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'active')",
        )
        .bind(&id)
        .bind(principal)
        .bind(delivery)
        .bind(name)
        .bind(&hash)
        .bind(now_us)
        .bind(expires)
        .execute(&self.pool)
        .await?;
        Ok(Subscription {
            id,
            principal: principal.clone(),
            delivery: delivery.clone(),
            name: name.clone(),
            args_hash: hash,
            cursor: 0,
            created_us: now_us,
            expires_us: expires,
            state: "active".into(),
        })
    }

    async fn get(&self, id: &str) -> anyhow::Result<Option<Subscription>> {
        let row = sqlx::query_as::<_, (
            String, String, String, String, String, i64, i64, Option<i64>, String,
        )>(
            "SELECT id, principal, delivery, name, args_hash, cursor, created_us, expires_us, state \
             FROM event_subscriptions WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(id, principal, delivery, name, args_hash, cursor, created_us, expires_us, state)| {
                Subscription {
                    id,
                    principal,
                    delivery,
                    name,
                    args_hash,
                    cursor: cursor as u64,
                    created_us,
                    expires_us,
                    state,
                }
            },
        ))
    }

    async fn list_active(&self) -> anyhow::Result<Vec<Subscription>> {
        let rows = sqlx::query_as::<_, (
            String, String, String, String, String, i64, i64, Option<i64>, String,
        )>(
            "SELECT id, principal, delivery, name, args_hash, cursor, created_us, expires_us, state \
             FROM event_subscriptions WHERE state = 'active' ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    principal,
                    delivery,
                    name,
                    args_hash,
                    cursor,
                    created_us,
                    expires_us,
                    state,
                )| {
                    Subscription {
                        id,
                        principal,
                        delivery,
                        name,
                        args_hash,
                        cursor: cursor as u64,
                        created_us,
                        expires_us,
                        state,
                    }
                },
            )
            .collect())
    }

    async fn append(&self, id: &str, payload: &Value, now_us: i64) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO event_deliveries (subscription_id, payload, created_us) VALUES (?, ?, ?)",
        )
        .bind(id)
        .bind(serde_json::to_string(payload)?)
        .bind(now_us)
        .execute(&self.pool)
        .await?;
        // Bound by age, then by count: keep only the newest N rows.
        sqlx::query(
            "DELETE FROM event_deliveries WHERE subscription_id = ? \
             AND (created_us < ? OR seq NOT IN ( \
                 SELECT seq FROM event_deliveries WHERE subscription_id = ? \
                 ORDER BY seq DESC LIMIT ?))",
        )
        .bind(id)
        .bind(now_us.saturating_sub(DELIVERY_TTL_US))
        .bind(id)
        .bind(MAX_DELIVERIES_PER_SUB)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn poll(&self, id: &str, after: u64, limit: u32) -> anyhow::Result<Page> {
        // bd-sptj4 gap contract: if the cursor fell off the pruned end, report
        // truncated and hand back a fresh servable cursor instead of lying.
        let oldest: Option<i64> =
            sqlx::query_scalar("SELECT MIN(seq) FROM event_deliveries WHERE subscription_id = ?")
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
        let (truncated, start) = gap_floor(after, oldest);
        let rows = sqlx::query_as::<_, (i64, String)>(
            "SELECT seq, payload FROM event_deliveries \
             WHERE subscription_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
        )
        .bind(id)
        .bind(start)
        .bind(limit as i64 + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = rows.len() > limit as usize;
        let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();
        let cursor = rows.last().map(|(seq, _)| *seq as u64).unwrap_or(after);
        let events = rows
            .into_iter()
            .map(|(seq, payload)| Delivery {
                seq: seq as u64,
                payload,
            })
            .collect();
        Ok(Page {
            events,
            cursor,
            has_more,
            truncated,
        })
    }

    async fn unsubscribe(&self, id: &str, principal: &str) -> anyhow::Result<bool> {
        let Some(row) = self.get(id).await? else {
            return Ok(true); // idempotent on unknown id
        };
        if row.principal != principal {
            return Ok(false); // another principal's id is denied
        }
        sqlx::query("UPDATE event_subscriptions SET state = 'terminated' WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(true)
    }

    async fn terminate(&self, id: &str) -> anyhow::Result<()> {
        sqlx::query("UPDATE event_subscriptions SET state = 'terminated' WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn prune_deliveries(&self, now_us: i64) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM event_deliveries WHERE created_us < ?")
            .bind(now_us.saturating_sub(DELIVERY_TTL_US))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn sweep(&self, now_us: i64) -> anyhow::Result<usize> {
        self.prune_deliveries(now_us).await?;
        let out = sqlx::query(
            "UPDATE event_subscriptions SET state = 'terminated' \
             WHERE state = 'active' AND expires_us IS NOT NULL AND expires_us <= ?",
        )
        .bind(now_us)
        .execute(&self.pool)
        .await?;
        Ok(out.rows_affected() as usize)
    }
}
