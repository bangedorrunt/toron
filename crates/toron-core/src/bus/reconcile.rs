// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! NIP-77 negentropy delta reconciliation for DR / federation rejoin (A5).
//!
//! Two independent recovery sources (ADR-0003 G11): Git archive OR bus/relay
//! replay. When a projection must be rebuilt from a bus/relay, a full replay
//! is O(all-events). Negentropy syncs only the missing delta.
//!
//! This module implements the reconciliation primitive over two in-process
//! stores (both sides of the NIP-77 protocol exchange). The same item set is
//! used on both sides so the fingerprint tree converges. A full-replay
//! fallback guarantees convergence when negentropy is unavailable or fails.

use nostr_sdk::nostr::{Event, JsonUtil};

use super::event_store::BusEventStore;

/// Outcome of a reconciliation run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncStats {
    /// Events fetched from the remote and inserted locally.
    pub synced: usize,
    /// True when negentropy delta sync was used; false when the replay
    /// fallback ran.
    pub used_negentropy: bool,
}

/// Load every stored event (id + created_at) for building the negentropy
/// storage.
async fn all_entries(store: &BusEventStore) -> anyhow::Result<Vec<(u64, negentropy::Id)>> {
    // Query raw id hex + created_at.
    let rows = sqlx::query("SELECT event_id, created_at FROM bus_events")
        .fetch_all(&store.pool)
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id_hex: String = sqlx::Row::get(&row, 0);
        let created_at: i64 = sqlx::Row::get(&row, 1);
        if let Ok(id_bytes) = hex::decode(&id_hex) {
            if let Ok(id) = negentropy::Id::from_slice(&id_bytes) {
                out.push((created_at.max(0) as u64, id));
            }
        }
    }
    Ok(out)
}

/// Fetch events by id.
async fn fetch_by_ids(store: &BusEventStore, ids: &[negentropy::Id]) -> anyhow::Result<Vec<Event>> {
    let mut out = Vec::new();
    for id in ids {
        let hex_id = hex::encode(id.to_bytes());
        let row = sqlx::query("SELECT raw_json FROM bus_events WHERE event_id = ?1")
            .bind(&hex_id)
            .fetch_optional(&store.pool)
            .await?;
        if let Some(row) = row {
            let raw: String = sqlx::Row::get(&row, 0);
            if let Ok(ev) = Event::from_json(&raw) {
                out.push(ev);
            }
        }
    }
    Ok(out)
}

impl BusEventStore {
    /// Negentropy delta sync: reconcile this store against `remote`, pulling
    /// only the events this side is missing.
    ///
    /// Both sides build their negentropy storage from the same item universe
    /// (id + created_at). The protocol exchange finds the delta; this side
    /// fetches + inserts the missing events. Falls back to full replay on any
    /// negentropy error.
    pub async fn sync_delta(&self, remote: &BusEventStore) -> anyhow::Result<SyncStats> {
        match self.negentropy_exchange(remote).await {
            Ok(synced) => Ok(SyncStats {
                synced,
                used_negentropy: true,
            }),
            Err(_) => Ok(self.sync_replay(remote).await?.with_negentropy(false)),
        }
    }

    /// Run the negentropy client/server exchange in-process.
    async fn negentropy_exchange(&self, remote: &BusEventStore) -> anyhow::Result<usize> {
        // Build both storages from the same universe.
        let local_entries = all_entries(self).await?;
        let remote_entries = all_entries(remote).await?;

        let mut local_vec = negentropy::NegentropyStorageVector::new();
        for (created_at, id) in &local_entries {
            local_vec.insert(*created_at, *id)?;
        }
        local_vec.seal()?;
        let mut remote_vec = negentropy::NegentropyStorageVector::new();
        for (created_at, id) in &remote_entries {
            remote_vec.insert(*created_at, *id)?;
        }
        remote_vec.seal()?;

        // Client initiates; server reconciles; client resolves have/need ids.
        let mut client = negentropy::Negentropy::owned(local_vec, 1_000_000)?;
        let mut server = negentropy::Negentropy::owned(remote_vec, 1_000_000)?;

        let init = client.initiate()?;
        let resp = server.reconcile(&init)?;

        // Collect ids the remote has that we do not.
        let mut have_ids = Vec::new();
        let mut need_ids = Vec::new();
        let _ = client.reconcile_with_ids(&resp, &mut have_ids, &mut need_ids)?;

        if need_ids.is_empty() {
            return Ok(0);
        }

        // Fetch the missing events from the remote and insert locally.
        let missing = fetch_by_ids(remote, &need_ids).await?;
        let mut synced = 0usize;
        for ev in &missing {
            if self.insert(ev).await? {
                synced += 1;
            }
        }
        Ok(synced)
    }

    /// Full-replay fallback: replay everything the remote has and insert the
    /// missing events locally. Always converges.
    pub async fn sync_replay(&self, remote: &BusEventStore) -> anyhow::Result<SyncStats> {
        let remote_events = remote.all_events().await?;
        let mut synced = 0usize;
        for ev in &remote_events {
            if self.insert(ev).await? {
                synced += 1;
            }
        }
        Ok(SyncStats {
            synced,
            used_negentropy: false,
        })
    }

    /// All stored events (used by the replay fallback).
    pub async fn all_events(&self) -> anyhow::Result<Vec<Event>> {
        let rows = sqlx::query("SELECT raw_json FROM bus_events ORDER BY created_at ASC")
            .fetch_all(&self.pool)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let raw: String = sqlx::Row::get(&row, 0);
            if let Ok(ev) = Event::from_json(&raw) {
                out.push(ev);
            }
        }
        Ok(out)
    }
}

impl SyncStats {
    fn with_negentropy(mut self, used: bool) -> Self {
        self.used_negentropy = used;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::PROJECT_MANIFEST;
    use nostr_sdk::nostr::{Keys, Tag, Timestamp};

    async fn temp_store() -> (BusEventStore, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("toron-sync-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = crate::index::schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        let store = BusEventStore::new(pool).await.expect("store");
        (store, path)
    }

    async fn insert_n(store: &BusEventStore, keys: &Keys, n: usize, slug: &str) {
        for i in 0..n {
            let ev = build_event(
                keys,
                crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
                format!("{slug}-{i}"),
                vec![Tag::parse(["t", slug]).expect("t")],
                Timestamp::from(1_700_000_000_u64 + i as u64),
            )
            .expect("build");
            store.insert(&ev).await.expect("insert");
        }
    }

    /// Two stores diverge by N events; negentropy converges pulling only the
    /// delta.
    #[tokio::test]
    async fn test_negentropy_converges_on_diverged_stores() {
        let (store_a, pa) = temp_store().await;
        let (store_b, pb) = temp_store().await;
        let keys = Keys::generate();

        // A has 5, B has 5 shared + 3 extra.
        insert_n(&store_a, &keys, 5, "shared").await;
        insert_n(&store_b, &keys, 8, "shared").await;

        assert_eq!(store_a.count().await.unwrap(), 5);
        assert_eq!(store_b.count().await.unwrap(), 8);

        // A syncs delta from B: only the 3 missing.
        let stats = store_a.sync_delta(&store_b).await.expect("sync");
        assert_eq!(stats.synced, 3, "only the missing delta is pulled");
        assert!(stats.used_negentropy, "negentropy path used");
        assert_eq!(store_a.count().await.unwrap(), 8, "converged");

        // Idempotent: syncing again pulls nothing.
        let again = store_a.sync_delta(&store_b).await.expect("sync again");
        assert_eq!(again.synced, 0);
        let _ = std::fs::remove_file(&pa);
        let _ = std::fs::remove_file(&pb);
    }

    /// Convergence in the other direction (B missing events A has).
    #[tokio::test]
    async fn test_negentropy_syncs_both_directions() {
        let (store_a, pa) = temp_store().await;
        let (store_b, pb) = temp_store().await;
        let keys = Keys::generate();

        insert_n(&store_a, &keys, 3, "a").await;
        insert_n(&store_b, &keys, 7, "b").await;

        // B syncs from A: pulls the 3 A-only events.
        let stats = store_b.sync_delta(&store_a).await.expect("sync");
        assert_eq!(stats.synced, 3);
        assert!(stats.used_negentropy);
        assert_eq!(store_b.count().await.unwrap(), 10);
        let _ = std::fs::remove_file(&pa);
        let _ = std::fs::remove_file(&pb);
    }

    /// Full replay fallback also converges.
    #[tokio::test]
    async fn test_replay_fallback_converges() {
        let (store_a, pa) = temp_store().await;
        let (store_b, pb) = temp_store().await;
        let keys = Keys::generate();

        insert_n(&store_a, &keys, 4, "replay").await;
        insert_n(&store_b, &keys, 10, "replay").await;

        let stats = store_a.sync_replay(&store_b).await.expect("replay");
        assert_eq!(stats.synced, 6);
        assert!(!stats.used_negentropy, "fallback marks replay path");
        assert_eq!(store_a.count().await.unwrap(), 10);
        let _ = std::fs::remove_file(&pa);
        let _ = std::fs::remove_file(&pb);
    }

    /// When stores are identical, sync pulls nothing.
    #[tokio::test]
    async fn test_sync_identical_stores_noop() {
        let (store_a, pa) = temp_store().await;
        let (store_b, pb) = temp_store().await;
        let keys = Keys::generate();
        insert_n(&store_a, &keys, 4, "same").await;
        insert_n(&store_b, &keys, 4, "same").await;

        let stats = store_a.sync_delta(&store_b).await.expect("sync");
        assert_eq!(stats.synced, 0);
        assert!(stats.used_negentropy);
        let _ = std::fs::remove_file(&pa);
        let _ = std::fs::remove_file(&pb);
    }
}
