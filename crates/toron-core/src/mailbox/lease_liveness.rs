// governed-by: ADR-0003
//! Lease-liveness oracle for the toron daemon (bd-lease-liveness-oracle-ro8i).
//!
//! The seam already exists: [`crate::mailbox::reservations`] resurrects an
//! expired SQL-mirror lease while a sync oracle vouches the holder is live,
//! and stays pure NIP-40 with no oracle installed. Tests installed ad-hoc
//! closures; the daemon installed nothing, so a manual holder idle past the
//! 180 s TTL lost its paths even mid-edit. This module is the daemon's
//! installer: a background task refreshes a pubkey → last-activity map every
//! [`REFRESH_SECS`], and the installed predicate is a pure membership read
//! under a lock — no async query and no ambient clock read inside it (the
//! refresher passes `now` in; effect-oriented substitution in tests).
//!
//! Activity source: `inbox_cursors.last_seen_us` joined to the roster npub,
//! NOT the latest event authored by the pubkey. The bus stores `created_at`
//! in whole seconds AND gift wraps randomize it up to two days into the
//! past, so a MAX(created_at) oracle would call live agents stale and stale
//! agents live. The cursor is host-clock micros advanced on every inbox
//! fetch — the ambient poll every live agent already runs — and the table
//! is one row per (project, agent, plane), so the refresh is a single
//! indexed aggregate bounded to agents active inside the TTL window.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use super::reservations::{
    DEFAULT_RESERVATION_TTL_SECS, clear_lease_liveness_checker, install_lease_liveness_checker,
};

/// How often the daemon refreshes the live set. 15 s against a 180 s TTL:
/// a holder that goes quiet frees its paths at most one refresh late, and
/// a holder that polls stays vouched with 12x headroom.
pub const REFRESH_SECS: u64 = 15;

/// Pubkey hex → last-seen micros. Only currently-live pubkeys are present;
/// the refresher applies the TTL cutoff, so the predicate is membership.
pub type ActivityMap = HashMap<String, i64>;

/// Pure recency fold: keep the freshest `last_seen_us` per pubkey, dropping
/// rows at or before the cutoff. The SQL already filters, so this is the
/// second half tests pin without a database.
pub fn live_set_from_cursors(
    rows: impl IntoIterator<Item = (String, i64)>,
    cutoff_us: i64,
) -> ActivityMap {
    let mut map = ActivityMap::new();
    for (pubkey, seen_us) in rows {
        if seen_us <= cutoff_us {
            continue;
        }
        map.entry(pubkey)
            .and_modify(|v| *v = (*v).max(seen_us))
            .or_insert(seen_us);
    }
    map
}

/// One refresh against the store: the newest inbox poll per roster pubkey,
/// bounded to polls inside the TTL window (stale rows never leave SQLite).
/// `now_us` is passed in (host clock at the tick edge), never read here.
pub async fn read_activity_snapshot(
    pool: &sqlx::SqlitePool,
    ttl_secs: u64,
    now_us: i64,
) -> ActivityMap {
    let cutoff_us = now_us.saturating_sub(ttl_secs as i64 * 1_000_000);
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT a.pubkey, MAX(c.last_seen_us) \
         FROM inbox_cursors c \
         JOIN agents a \
           ON a.project_slug = c.project_slug AND a.name = c.agent_name \
         WHERE c.last_seen_us > ?1 \
         GROUP BY a.pubkey",
    )
    .bind(cutoff_us)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    live_set_from_cursors(rows, cutoff_us)
}

/// Daemon guard: installs the membership oracle and refreshes it until
/// dropped. Drop aborts the task and clears the oracle (shutdown hygiene —
/// a stale live set must never outlive the refresher that owns it).
pub struct LeaseLivenessGuard {
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for LeaseLivenessGuard {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
        clear_lease_liveness_checker();
    }
}

impl LeaseLivenessGuard {
    async fn install(pool: sqlx::SqlitePool, ttl_secs: u64) -> Self {
        let live: Arc<RwLock<ActivityMap>> = Arc::new(RwLock::new(ActivityMap::new()));
        // Initial load before the predicate goes live: an immediate serve
        // must vouch holders polled before boot, not after the first tick.
        let now_us = host_now_us();
        *live.write().unwrap_or_else(|e| e.into_inner()) =
            read_activity_snapshot(&pool, ttl_secs, now_us).await;
        let refreshed = Arc::clone(&live);
        install_lease_liveness_checker(Arc::new(move |pubkey_hex: &str| {
            refreshed
                .read()
                .map(|m| m.contains_key(pubkey_hex))
                .unwrap_or(false)
        }));
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(REFRESH_SECS));
            tick.tick().await;
            loop {
                tick.tick().await;
                let now_us = host_now_us();
                let snap = read_activity_snapshot(&pool, ttl_secs, now_us).await;
                if let Ok(mut m) = live.write() {
                    *m = snap;
                }
            }
        });
        Self {
            handle: Some(handle),
        }
    }
}

/// Install the oracle for a serving mailbox and hold the guard for the
/// serve lifetime. No index pool (one-shot consumers) → no oracle, pure
/// NIP-40, exactly as before.
pub async fn install_for_daemon(
    pool: Option<&sqlx::SqlitePool>,
    ttl_secs: u64,
) -> Option<LeaseLivenessGuard> {
    match pool {
        Some(p) => Some(LeaseLivenessGuard::install(p.clone(), ttl_secs).await),
        None => None,
    }
}

/// Convenience: the daemon's TTL is the reservation heartbeat, unchanged.
pub async fn install_for_daemon_default_ttl(
    pool: Option<&sqlx::SqlitePool>,
) -> Option<LeaseLivenessGuard> {
    install_for_daemon(pool, DEFAULT_RESERVATION_TTL_SECS).await
}

/// Host clock micros. Called ONLY at the refresh-tick edge and passed down;
/// the predicate and the fold never read the clock (substitution in tests).
fn host_now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fold keeps only rows strictly fresher than the cutoff, and the
    /// freshest poll per pubkey — no database, no clock, no sleeps.
    #[test]
    fn live_set_keeps_freshest_poll_per_pubkey_and_drops_stale() {
        let map = live_set_from_cursors(
            vec![
                ("aa".to_string(), 1_000),
                ("aa".to_string(), 3_000),
                ("aa".to_string(), 2_000),
                ("bb".to_string(), 999),
                ("cc".to_string(), 1_001),
            ],
            1_000,
        );
        assert_eq!(map.get("aa"), Some(&3_000));
        assert!(!map.contains_key("bb"), "at the cutoff is stale: {map:?}");
        assert_eq!(map.get("cc"), Some(&1_001));
        assert!(live_set_from_cursors(Vec::new(), 0).is_empty());
    }

    /// The snapshot reads the cursor→roster join and bounds itself to the
    /// TTL window: a quiet holder is absent, never vouched stale.
    #[tokio::test]
    async fn snapshot_joins_cursors_to_roster_and_bounds_to_ttl() {
        let db = std::env::temp_dir().join(format!("toron-live-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = crate::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");
        let now_us: i64 = 1_790_000_000_000_000;
        let fresh_us = now_us - 10_000_000;
        let stale_us = now_us - 10_000 * 1_000_000;
        for (name, pubkey) in [("LivePin", "aa"), ("QuietPin", "bb")] {
            sqlx::query(
                "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
                 VALUES ('demo', ?1, ?2, ?3)",
            )
            .bind(name)
            .bind(pubkey)
            .bind(now_us)
            .execute(&pool)
            .await
            .expect("agent");
        }
        // A cursor for an unregistered name joins to nothing (never vouched).
        for (name, seen) in [
            ("LivePin", fresh_us),
            ("QuietPin", stale_us),
            ("GhostPin", fresh_us),
        ] {
            sqlx::query(
                "INSERT INTO inbox_cursors (project_slug, agent_name, plane, last_seen_us) \
                 VALUES ('demo', ?1, 'P', ?2)",
            )
            .bind(name)
            .bind(seen)
            .execute(&pool)
            .await
            .expect("cursor");
        }
        let map = read_activity_snapshot(&pool, 180, now_us).await;
        assert_eq!(map.get("aa"), Some(&fresh_us));
        assert!(!map.contains_key("bb"), "quiet holder absent: {map:?}");
        assert_eq!(map.len(), 1, "ghost names never vouch: {map:?}");
        let _ = std::fs::remove_file(&db);
    }
}
