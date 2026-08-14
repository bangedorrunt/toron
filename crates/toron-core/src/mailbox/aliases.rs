// governed-by: ADR-0017
//! Project-root canonicalization (ADR-0017).
//!
//! Every project key resolves to the canonical short slug. Path-derived keys
//! (the CLI's absolute-path slug `/Users/.../workspace/toron`, the MCP
//! surface's short key `toron`) alias into the canonical root through the
//! `project_aliases` table in the archive DB — one root per project, one set
//! of agent keys, one registration row.
//!
//! Write path — [`canonicalize`]: a path-form key resolves to its basename
//! short slug and persists an alias row; all state lands under the canonical
//! root.
//! Read path — [`alias_read_set`]: the caller's own spelling plus the
//! canonical root plus every stored alias of that root. Pre-ADR-0017 rows
//! written under path-form slugs stay visible (aliased read-only; ADR-0017
//! D3 — nothing is moved, so a merge is never unsafe).
//! Migration — [`reconcile_aliases`]: fold pre-existing data-dir roots whose
//! relative slug is a path form into the alias table (daemon open).

use std::path::Path;

use sqlx::SqlitePool;

/// A project key is in path form when it contains a path separator
/// (`/Users/.../workspace/toron`, `Users/.../toron`). Canonical keys are
/// short slugs with no separator.
pub fn path_form(slug: &str) -> bool {
    slug.contains('/')
}

/// The canonical short slug of a path-form project key: its basename
/// (`/Users/.../workspace/toron` → `toron`). `None` for keys with no
/// basename (e.g. `".."`, `/`).
pub fn short_slug(slug: &str) -> Option<String> {
    let stripped = slug.trim_start_matches('/');
    let base = Path::new(stripped).file_name()?.to_str()?;
    if base.is_empty() {
        None
    } else {
        Some(base.to_string())
    }
}

/// Resolve `slug` to its canonical project key.
///
/// Path-form keys: persisted alias row lookup first, then insert-on-conflict
/// (first writer wins the canonical mapping), then re-read. With no index
/// pool the table is unavailable and the pure basename derivation stands in
/// (identical result in the common case; the pool makes it durable).
pub async fn canonicalize(pool: Option<&SqlitePool>, slug: &str) -> String {
    if !path_form(slug) {
        return slug.to_string();
    }
    let Some(short) = short_slug(slug) else {
        return slug.to_string();
    };
    let Some(pool) = pool else {
        return short;
    };
    if let Ok(Some(canonical)) =
        sqlx::query_scalar::<_, String>("SELECT canonical FROM project_aliases WHERE alias = ?1")
            .bind(slug)
            .fetch_optional(pool)
            .await
    {
        return canonical;
    }
    let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
    let _ = sqlx::query(
        "INSERT INTO project_aliases (alias, canonical, created_ts_us) \
         VALUES (?1, ?2, ?3) ON CONFLICT (alias) DO NOTHING",
    )
    .bind(slug)
    .bind(&short)
    .bind(now_us)
    .execute(pool)
    .await;
    // Re-read: a concurrent canonicalize may have won the race.
    sqlx::query_scalar::<_, String>("SELECT canonical FROM project_aliases WHERE alias = ?1")
        .bind(slug)
        .fetch_one(pool)
        .await
        .unwrap_or(short)
}

/// The read-side alias expansion of `slug`: the caller's own spelling, the
/// canonical root, and every stored alias of that root. Queries over this
/// set see rows written under ANY spelling (pre-canonicalization path-slug
/// rows included — aliased read-only, ADR-0017 D3).
pub async fn alias_read_set(pool: Option<&SqlitePool>, slug: &str) -> Vec<String> {
    let mut out: Vec<String> = vec![slug.to_string()];
    let canonical = canonicalize(pool, slug).await;
    if !out.contains(&canonical) {
        out.push(canonical.clone());
    }
    if let Some(pool) = pool {
        if let Ok(rows) = sqlx::query_scalar::<_, String>(
            "SELECT alias FROM project_aliases WHERE canonical = ?1",
        )
        .bind(&canonical)
        .fetch_all(pool)
        .await
        {
            for alias in rows {
                if !out.contains(&alias) {
                    out.push(alias);
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Fold pre-existing data-dir project roots into the alias table: every root
/// whose relative slug is a path form gets `slug → basename` (insert-on-
/// conflict; existing rows untouched). Runs on daemon open so legacy
/// path-slug state is immediately visible through canonical lookups.
/// Returns the number of new alias rows.
pub async fn reconcile_aliases(pool: &SqlitePool, data_dir: &Path) -> anyhow::Result<usize> {
    let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
    let mut inserted = 0usize;
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let Some(slug) = path
                .strip_prefix(data_dir)
                .ok()
                .map(|s| s.to_string_lossy().to_string())
            else {
                continue;
            };
            // Only real project roots (contain an agents dir), mirroring the
            // bd-f5tt walk; canonical short-slug roots need no alias.
            if path_form(&slug) && path.join("agents").is_dir() {
                if let Some(short) = short_slug(&slug) {
                    // Both spellings fold (mirroring project_slug_aliases):
                    // legacy events/rows may carry the leading-separator
                    // form while the on-disk root is relative.
                    for alias in [slug.clone(), format!("/{slug}")] {
                        let res = sqlx::query(
                            "INSERT INTO project_aliases (alias, canonical, created_ts_us) \
                             VALUES (?1, ?2, ?3) ON CONFLICT (alias) DO NOTHING",
                        )
                        .bind(&alias)
                        .bind(&short)
                        .bind(now_us)
                        .execute(pool)
                        .await?;
                        inserted += res.rows_affected() as usize;
                    }
                }
            }
            stack.push(path);
        }
    }
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn test_pool(tag: &str) -> SqlitePool {
        let db_path = std::env::temp_dir().join(format!(
            "toron-alias-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap()).await.expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        pool
    }

    #[test]
    fn path_form_detects_separators() {
        assert!(path_form("/Users/bangedorrunt/workspace/toron"));
        assert!(path_form("Users/bangedorrunt/workspace/toron"));
        assert!(!path_form("toron"));
        assert!(!path_form("toron.dev"));
    }

    #[test]
    fn short_slug_is_the_basename() {
        assert_eq!(
            short_slug("/Users/bangedorrunt/workspace/toron").as_deref(),
            Some("toron")
        );
        assert_eq!(short_slug("Users/workspace/toron").as_deref(), Some("toron"));
        assert_eq!(short_slug("toron").as_deref(), Some("toron"));
        assert_eq!(short_slug("/").as_deref(), None);
    }

    #[tokio::test]
    async fn canonicalize_path_form_without_pool_derives_basename() {
        assert_eq!(
            canonicalize(None, "/Users/bangedorrunt/workspace/toron").await,
            "toron"
        );
        assert_eq!(canonicalize(None, "toron").await, "toron");
        assert_eq!(canonicalize(None, "tmp/x/workspace/demo").await, "demo");
    }

    #[tokio::test]
    async fn canonicalize_persists_alias_row_and_is_idempotent() {
        let pool = test_pool("canon").await;
        let path_slug = "/Users/bangedorrunt/workspace/toron";
        assert_eq!(canonicalize(Some(&pool), path_slug).await, "toron");
        assert_eq!(canonicalize(Some(&pool), path_slug).await, "toron");
        assert_eq!(canonicalize(Some(&pool), "toron").await, "toron");
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_aliases WHERE alias = ?1")
                .bind(path_slug)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(n, 1, "alias row persisted exactly once");
    }

    #[tokio::test]
    async fn first_writer_wins_the_canonical_mapping() {
        let pool = test_pool("first-wins").await;
        // Two path forms sharing a basename: the first canonicalize wins.
        canonicalize(Some(&pool), "/a/b/toron").await;
        assert_eq!(canonicalize(Some(&pool), "/c/d/toron").await, "toron");
    }

    #[tokio::test]
    async fn read_set_spans_canonical_plus_all_aliases() {
        let pool = test_pool("readset").await;
        canonicalize(Some(&pool), "/Users/bangedorrunt/workspace/toron").await;
        canonicalize(Some(&pool), "/srv/legacy/toron").await;
        let set = alias_read_set(Some(&pool), "/Users/bangedorrunt/workspace/toron").await;
        assert!(set.contains(&"toron".to_string()));
        assert!(set.contains(&"/Users/bangedorrunt/workspace/toron".to_string()));
        assert!(
            set.contains(&"/srv/legacy/toron".to_string()),
            "legacy path-slug rows stay addressable: {set:?}"
        );
        // Reading via the canonical slug sees every alias too.
        let via_canonical = alias_read_set(Some(&pool), "toron").await;
        assert!(via_canonical.contains(&"/srv/legacy/toron".to_string()));
    }

    #[tokio::test]
    async fn reconcile_folds_existing_path_slug_roots() {
        let pool = test_pool("reconcile").await;
        let data_dir = std::env::temp_dir().join(format!(
            "toron-alias-data-{}",
            uuid::Uuid::new_v4()
        ));
        let legacy = data_dir.join("Users/bangedorrunt/workspace/toron");
        std::fs::create_dir_all(legacy.join("agents")).unwrap();
        std::fs::create_dir_all(data_dir.join("toron").join("agents")).unwrap();

        let inserted = reconcile_aliases(&pool, &data_dir).await.expect("reconcile");
        assert_eq!(inserted, 2, "both spellings of the path-form root fold");

        let set = alias_read_set(Some(&pool), "toron").await;
        assert!(set.contains(&"Users/bangedorrunt/workspace/toron".to_string()));
        assert!(set.contains(&"/Users/bangedorrunt/workspace/toron".to_string()));

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn reconcile_is_idempotent() {
        let pool = test_pool("reconcile2").await;
        let data_dir = std::env::temp_dir().join(format!(
            "toron-alias-data-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(data_dir.join("Users/a/toron").join("agents")).unwrap();
        assert_eq!(reconcile_aliases(&pool, &data_dir).await.unwrap(), 2);
        assert_eq!(reconcile_aliases(&pool, &data_dir).await.unwrap(), 0);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
