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

use std::path::{Path, PathBuf};

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
    // bd-rtkx: the alias row is the durable half of the path→slug relation.
    // Swallowing the write error made the relation silently in-memory: the
    // canonical slug still came back (the basename derivation stands in), so
    // the row was simply never there for the read side to expand over.
    if let Err(e) = sqlx::query(
        "INSERT INTO project_aliases (alias, canonical, created_ts_us) \
         VALUES (?1, ?2, ?3) ON CONFLICT (alias) DO NOTHING",
    )
    .bind(slug)
    .bind(&short)
    .bind(now_us)
    .execute(pool)
    .await
    {
        tracing::warn!(
            alias = slug,
            canonical = &short,
            error = %e,
            "project alias row NOT persisted — the path→slug relation stays in-memory only"
        );
    }
    // Re-read: a concurrent canonicalize may have won the race.
    sqlx::query_scalar::<_, String>("SELECT canonical FROM project_aliases WHERE alias = ?1")
        .bind(slug)
        .fetch_one(pool)
        .await
        .unwrap_or(short)
}

/// Escape the LIKE wildcards (`%`, `_`, `\`) in a project slug.
fn like_escaped(slug: &str) -> String {
    slug.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

/// The path-form spellings of `canonical` that the IDENTITY ROSTER carries.
///
/// bd-rtkx: the alias table is a CACHE of the path→slug relation. A row
/// written under a path form before that cache existed is in the DATA, not in
/// the cache — so a read that expands only over the cache cannot see it and
/// the identity is invisible from the short key, even though the path key IS
/// the same project. The spelling is matched by its basename: a path form
/// ends in `/<canonical>`, and nothing else does.
async fn roster_path_forms(pool: &SqlitePool, canonical: &str) -> Vec<String> {
    let pattern = format!("%/{}", like_escaped(canonical));
    sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT project_slug FROM agents WHERE project_slug LIKE ?1 ESCAPE '\\'",
    )
    .bind(pattern)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

/// The read-side alias expansion of `slug`: the caller's own spelling, the
/// canonical root, every stored alias of that root, and the path-form
/// spellings the roster carries. Queries over this set see rows written
/// under ANY spelling (pre-canonicalization path-slug rows included —
/// aliased read-only, ADR-0017 D3).
///
/// bd-rtkx: the last group is the one a cached alias table cannot supply. A
/// row written under a path form before the table existed is in the DATA, so
/// the read set finds it there — the path key IS the same project, whether
/// or not the alias row was ever written.
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
        for spelling in roster_path_forms(pool, &canonical).await {
            if !out.contains(&spelling) {
                out.push(spelling);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// The filesystem-safe project root under `data_dir` for a project key.
///
/// A project key is a logical id, never a filesystem path: path-form keys
/// collapse to the canonical short slug (basename), and degenerate keys
/// (`"."`, `".."`, `"/"`, empty) — which would escape `data_dir` via
/// `PathBuf::join` discarding the base — fall back to a sanitized token.
/// Mailbox project roots can therefore never escape the data root (S10
/// workspace hygiene: gate tests booting mailboxes with cwd-derived roots
/// must not scatter `.archive.lock` + `messages/` trees across ~/workspace).
pub fn project_root(data_dir: &Path, slug: &str) -> PathBuf {
    let base = short_slug(slug).unwrap_or_default();
    let base_is_safe = !base.is_empty()
        && base != "."
        && base != ".."
        && !base.contains('/')
        && !base.contains('\\');
    let safe = if base_is_safe {
        base
    } else {
        let s: String = slug
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if s.is_empty() || s == "." || s == ".." {
            "project".to_string()
        } else {
            s
        }
    };
    data_dir.join(safe)
}

/// Fold pre-existing data-dir project roots into the alias table: every root
/// whose relative slug is a path form gets `slug → basename` (insert-on-
/// conflict; existing rows untouched). Runs on daemon open so legacy
/// path-slug state is immediately visible through canonical lookups.
/// Returns the number of new alias rows.
///
/// bd-rtkx: the FILESYSTEM is only half the story. A project root on disk is
/// named by its canonical slug (the path form collapses when the root is
/// built), so the walk below finds nothing — while the DATA (the identity
/// roster) still carries rows written under a path form. Those rows are the
/// same project, so their spelling gets its alias row here too: the table
/// then carries path → slug for every spelling the data uses.
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
            if path_form(&slug)
                && path.join("agents").is_dir()
                && let Some(short) = short_slug(&slug)
            {
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
            stack.push(path);
        }
    }
    // bd-rtkx: the roster's path-form spellings get their alias rows (both
    // the leading-separator and the relative form — legacy rows carry
    // either).
    let spellings: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT project_slug FROM agents WHERE project_slug LIKE '%/%' ESCAPE '\\'",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    for spelling in spellings {
        let Some(short) = short_slug(&spelling) else {
            continue;
        };
        let relative = spelling.trim_start_matches('/').to_string();
        // Both spellings fold (the legacy rows carry either form): the
        // leading-separator one and the relative one.
        let forms = if spelling.starts_with('/') {
            vec![spelling.clone(), relative.clone()]
        } else {
            vec![spelling.clone(), format!("/{relative}")]
        };
        for alias in forms {
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
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn test_pool(tag: &str) -> SqlitePool {
        let db_path =
            std::env::temp_dir().join(format!("toron-alias-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
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
        assert_eq!(
            short_slug("Users/workspace/toron").as_deref(),
            Some("toron")
        );
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
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM project_aliases WHERE alias = ?1")
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
        let data_dir =
            std::env::temp_dir().join(format!("toron-alias-data-{}", uuid::Uuid::new_v4()));
        let legacy = data_dir.join("Users/bangedorrunt/workspace/toron");
        std::fs::create_dir_all(legacy.join("agents")).unwrap();
        std::fs::create_dir_all(data_dir.join("toron").join("agents")).unwrap();

        let inserted = reconcile_aliases(&pool, &data_dir)
            .await
            .expect("reconcile");
        assert_eq!(inserted, 2, "both spellings of the path-form root fold");

        let set = alias_read_set(Some(&pool), "toron").await;
        assert!(set.contains(&"Users/bangedorrunt/workspace/toron".to_string()));
        assert!(set.contains(&"/Users/bangedorrunt/workspace/toron".to_string()));

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn reconcile_is_idempotent() {
        let pool = test_pool("reconcile2").await;
        let data_dir =
            std::env::temp_dir().join(format!("toron-alias-data-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(data_dir.join("Users/a/toron").join("agents")).unwrap();
        assert_eq!(reconcile_aliases(&pool, &data_dir).await.unwrap(), 2);
        assert_eq!(reconcile_aliases(&pool, &data_dir).await.unwrap(), 0);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-rtkx: a roster row written under a path form BEFORE the alias
    /// table existed is in the DATA, not in the cache. The read set must
    /// still expand over it — the path key IS the same project, whether or
    /// not the alias row was ever written.
    #[tokio::test]
    async fn the_read_set_finds_a_path_form_roster_row_with_no_alias_row() {
        let pool = test_pool("roster-path").await;
        let path_slug = "/Users/bangedorrunt/workspace/toron";
        sqlx::query(
            "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
             VALUES (?1, ?2, ?3, 1)",
        )
        .bind(path_slug)
        .bind("DustySparrow")
        .bind("aa")
        .execute(&pool)
        .await
        .expect("legacy path-form roster row");
        // No alias row: the cache never saw this spelling.
        let cached: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM project_aliases WHERE alias = ?1")
                .bind(path_slug)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(cached, 0, "precondition: the alias row was never written");

        let set = alias_read_set(Some(&pool), "toron").await;
        assert!(
            set.iter().any(|s| s == path_slug),
            "the short key must see the path-form row: {set:?}"
        );

        // A DIFFERENT project that merely ends with the same word is not in
        // the set — the spelling is matched by its basename, not by a suffix.
        sqlx::query(
            "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
             VALUES (?1, ?2, ?3, 1)",
        )
        .bind("/Users/bangedorrunt/workspace/other-toron")
        .bind("NotToron")
        .bind("bb")
        .execute(&pool)
        .await
        .expect("other path-form row");
        let set = alias_read_set(Some(&pool), "toron").await;
        assert!(
            !set.iter().any(|s| s.ends_with("/other-toron")),
            "a sibling project is not the same project: {set:?}"
        );
    }

    /// bd-rtkx: the reconcile folds the roster's path-form spellings into
    /// the alias table, so the table carries path → slug for every spelling
    /// the data uses — the durable half of the relation.
    #[tokio::test]
    async fn the_reconcile_folds_the_rosters_path_form_spellings() {
        let pool = test_pool("reconcile-roster").await;
        let data_dir =
            std::env::temp_dir().join(format!("toron-alias-data-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&data_dir).unwrap();
        sqlx::query(
            "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
             VALUES (?1, ?2, ?3, 1)",
        )
        .bind("/Users/bangedorrunt/workspace/toron")
        .bind("DustySparrow")
        .bind("aa")
        .execute(&pool)
        .await
        .expect("legacy path-form roster row");

        let inserted = reconcile_aliases(&pool, &data_dir)
            .await
            .expect("reconcile");
        assert_eq!(inserted, 2, "both spellings of the path form fold");

        for alias in [
            "/Users/bangedorrunt/workspace/toron",
            "Users/bangedorrunt/workspace/toron",
        ] {
            let canonical: Option<String> =
                sqlx::query_scalar("SELECT canonical FROM project_aliases WHERE alias = ?1")
                    .bind(alias)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                canonical.as_deref(),
                Some("toron"),
                "the alias table carries {alias} -> toron"
            );
        }
        // Idempotent: the second run adds nothing.
        assert_eq!(reconcile_aliases(&pool, &data_dir).await.unwrap(), 0);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn project_root_never_escapes_data_dir() {
        let data_dir = std::env::temp_dir().join(format!("toron-proot-{}", uuid::Uuid::new_v4()));
        // Plain short slug: nested under data_dir.
        assert_eq!(
            project_root(&data_dir, "demo"),
            data_dir.join("demo"),
            "short slug nests under data_dir"
        );
        // Absolute path-form key (the S10 pollution class: gate tests boot
        // mailboxes with cwd-derived roots like /Users/.../workspace/s66667).
        let root = project_root(&data_dir, "/Users/bangedorrunt/workspace/s66667");
        assert!(
            root.starts_with(&data_dir),
            "absolute key must stay under data_dir, got {}",
            root.display()
        );
        assert_eq!(root, data_dir.join("s66667"));
        // Relative path-form key without the leading separator.
        assert_eq!(
            project_root(&data_dir, "Users/bangedorrunt/workspace/s66667"),
            data_dir.join("s66667")
        );
        // Degenerate keys must not escape data_dir either.
        for bad in ["..", ".", "/", "", "../escape"] {
            let root = project_root(&data_dir, bad);
            assert!(
                root.starts_with(&data_dir),
                "degenerate key {bad:?} must stay under data_dir, got {}",
                root.display()
            );
        }
        // Backslashes (windows-style) must not escape either.
        let root = project_root(&data_dir, "..\\escape");
        assert!(
            root.starts_with(&data_dir),
            "backslash key must stay under data_dir, got {}",
            root.display()
        );
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn project_root_canonical_key_unchanged() {
        let data_dir = PathBuf::from("/data");
        assert_eq!(
            project_root(&data_dir, "toron"),
            PathBuf::from("/data/toron")
        );
        assert_eq!(
            project_root(&data_dir, "toron.dev"),
            PathBuf::from("/data/toron.dev")
        );
    }
}
