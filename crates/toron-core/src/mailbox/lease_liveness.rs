// governed-by: ADR-0003
//! Lease-liveness source for every toron consumer.
//!
//! The authority is the shared SQLite inbox-cursor/agent-activity snapshot:
//! `toron serve`, an in-process `Mailbox`, and the CLI all read the same rows,
//! so an expired lease cannot look dead in one process and live in another.
//! There is no process-local predicate and no refresh loop to drift.
//!
//! Activity source: `MAX(inbox_cursors.last_seen_us,
//! inbox_cursors.last_poll_us)` joined to the roster npub, NOT the latest
//! event authored by the pubkey. The bus stores `created_at` in whole seconds
//! AND gift wraps randomize it up to two days into the past, so a
//! MAX(created_at) read would call live agents stale and stale agents live.

#![forbid(unsafe_code)]

use std::collections::HashMap;

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
///
/// Reads the liveliest of the delivery cursor and the poll stamp:
/// `last_seen_us` advances only when messages exist, `last_poll_us` on
/// every read (bd-oracle-dead-source-72az — an empty inbox still proves
/// the reader is alive).
///
/// Returns `None` when the read fails — a transient SQLite error is a
/// value, never a silent empty map (an error read as empty would free an
/// expired lease held by a LIVE agent for one refresh: the exact race the
/// oracle exists to prevent). The refresher keeps the previous map.
pub async fn read_activity_snapshot(
    pool: &sqlx::SqlitePool,
    ttl_secs: u64,
    now_us: i64,
) -> Option<ActivityMap> {
    let cutoff_us = now_us.saturating_sub(ttl_secs as i64 * 1_000_000);
    let rows: Vec<(String, i64)> = match sqlx::query_as(
        "SELECT a.pubkey, MAX(MAX(c.last_seen_us, c.last_poll_us)) \
         FROM inbox_cursors c \
         JOIN agents a \
           ON a.project_slug = c.project_slug AND a.name = c.agent_name \
         WHERE MAX(c.last_seen_us, c.last_poll_us) > ?1 \
         GROUP BY a.pubkey",
    )
    .bind(cutoff_us)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            SNAPSHOT_FAILED.store(false, std::sync::atomic::Ordering::Relaxed);
            rows
        }
        Err(e) => {
            if !SNAPSHOT_FAILED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(
                    error = %e,
                    "lease-liveness snapshot failed; keeping the previous live set"
                );
            }
            return None;
        }
    };
    Some(live_set_from_cursors(rows, cutoff_us))
}

/// Set while the last snapshot failed (log-once latch for the warn above).
static SNAPSHOT_FAILED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The keep-previous decision, pure: a failed read (`None`) never clears
/// the live set — the refresher holds what it had until SQLite answers.
/// Tested without a database.
pub fn apply_snapshot(prev: ActivityMap, next: Option<ActivityMap>) -> ActivityMap {
    next.unwrap_or(prev)
}

/// Host clock micros. Called ONLY at an effect edge by the command that
/// records activity; the value travels as an argument from there (the
/// predicate and the fold never read the clock — substitution in tests).
pub fn host_now_us() -> i64 {
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

    /// The keep-previous decision, pure: a failed read keeps the live set,
    /// a good read replaces it — no database, no clock.
    #[test]
    fn apply_snapshot_keeps_previous_map_on_failed_read() {
        let prev: ActivityMap = [("live".to_string(), 9_000)].into_iter().collect();
        let kept = apply_snapshot(prev.clone(), None);
        assert_eq!(kept, prev, "an error never publishes an empty live set");
        let next: ActivityMap = [("new".to_string(), 1_000)].into_iter().collect();
        assert_eq!(apply_snapshot(prev, Some(next.clone())), next);
        assert!(apply_snapshot(ActivityMap::new(), None).is_empty());
    }

    /// A dead store reads as failure (`None`), never as an empty live set.
    #[tokio::test]
    async fn snapshot_on_closed_pool_returns_none_not_empty() {
        let db =
            std::env::temp_dir().join(format!("toron-live-closed-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = crate::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        pool.close().await;
        assert!(
            read_activity_snapshot(&pool, 180, 1_790_000_000_000_000)
                .await
                .is_none(),
            "a transient store error must not vouch-nobody as a map"
        );
        let _ = std::fs::remove_file(&db);
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
        for (name, pubkey) in [("LivePin", "aa"), ("QuietPin", "bb"), ("PollPin", "cc")] {
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
        sqlx::query(
            "INSERT INTO inbox_cursors (project_slug, agent_name, plane, last_seen_us, last_poll_us) \
             VALUES ('demo', 'PollPin', 'P', ?1, ?2)",
        )
        .bind(stale_us)
        .bind(fresh_us)
        .execute(&pool)
        .await
        .expect("poll cursor");
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
        let map = read_activity_snapshot(&pool, 180, now_us)
            .await
            .expect("snapshot");
        assert_eq!(map.get("aa"), Some(&fresh_us));
        assert!(!map.contains_key("bb"), "quiet holder absent: {map:?}");
        assert_eq!(
            map.get("cc"),
            Some(&fresh_us),
            "a poll vouches with an empty inbox (stale cursor, fresh poll): {map:?}"
        );
        assert_eq!(map.len(), 2, "ghost names never vouch: {map:?}");
        let _ = std::fs::remove_file(&db);
    }
}
