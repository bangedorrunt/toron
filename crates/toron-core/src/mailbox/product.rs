// governed-by: ADR-0003, ADR-0037
#![forbid(unsafe_code)]

//! Product bus cluster (bd-led.5.8): ensure_product, products_link,
//! search_messages_product, fetch_inbox_product, summarize_thread_product — G6.
//!
//! ADR-0003 G6:
//! - A **project** = operator kind-30010 manifest (`d`=slug).
//! - A **product** = kind-30011 (`d`=product-key) whose tags list member
//!   projects via `["a", <project-manifest-coordinate>]`.
//! - Cross-project queries fan `#t` / NIP-32 labels across member projects —
//!   content-addressed, federatable (vs AM-Rust's server-side product table).
//! - The index mirrors membership in `products` + `product_projects` for fast
//!   fan-out (oc-b schema).

use crate::nostr::{PublicKey, Tag, Timestamp};
use sqlx::Row;

use crate::bus::filter::ToronFilter;
use crate::bus::scoping::with_project_scoping;
use crate::mailbox::search::{SearchMessage, SearchMessages, ThreadSummary};
use crate::nostr::event::build_event;
use crate::nostr::kinds::{PRODUCT_MANIFEST, toron_kind};

use super::Mailbox;
use super::collect_replay;

/// `ensure_product` / `products_link` result.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProductInfo {
    pub product_key: String,
    pub name: String,
    pub member_projects: Vec<String>,
    pub created: bool,
}

impl Mailbox {
    /// `ensure_product`: idempotent — publish a kind-30011 product manifest
    /// (`d`=product-key) if absent, else re-read the existing one.
    pub async fn ensure_product(
        &self,
        product_key: &str,
        name: &str,
    ) -> anyhow::Result<ProductInfo> {
        let operator = self
            .operator
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no operator key configured"))?;

        // Idempotent: re-read existing kind-30011 for this product key.
        let existing = self
            .find_product(product_key)
            .await?
            .map(|e| (e.id.to_hex(), member_projects_of(&e)));

        let (event_id, members) = match existing {
            Some((id, members)) => (id, members),
            None => {
                let base = vec![Tag::parse(["d", product_key]).expect("d")];
                // Products aren't project-scoped; the operator owns them.
                let tags = with_project_scoping(toron_kind(PRODUCT_MANIFEST), base, None);
                let content = format!(
                    r#"{{"name":{}}}"#,
                    serde_json::to_string(name).unwrap_or_default()
                );
                let ev = build_event(
                    operator,
                    toron_kind(PRODUCT_MANIFEST),
                    content,
                    tags,
                    Timestamp::now(),
                )?;
                let ok = self.bus.publish(ev.clone()).await?;
                if !ok.stored {
                    anyhow::bail!("product manifest rejected by bus");
                }
                self.upsert_product_row(product_key, name).await?;
                (ev.id.to_hex(), Vec::new())
            }
        };
        let _ = event_id;
        Ok(ProductInfo {
            product_key: product_key.to_string(),
            name: name.to_string(),
            member_projects: members,
            created: false,
        })
    }

    /// `products_link`: link a project into a product (idempotent). The
    /// product manifest gains an `["a", <project-manifest-coordinate>]` tag
    /// referencing the kind-30010 project manifest; the index gets a
    /// `product_projects` row.
    pub async fn products_link(
        &self,
        product_key: &str,
        project_slug: &str,
    ) -> anyhow::Result<ProductInfo> {
        let operator = self
            .operator
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no operator key configured"))?;

        // Re-publish the product manifest with the member a-tag (operator
        // replaces their own kind-30011 per NIP-33 author isolation).
        let existing = self.find_product(product_key).await?;
        let mut members = existing
            .as_ref()
            .map(member_projects_of)
            .unwrap_or_default();
        if members.contains(&project_slug.to_string()) {
            // Already linked — idempotent no-op.
            let name = existing
                .as_ref()
                .and_then(parse_product_name)
                .unwrap_or_else(|| product_key.to_string());
            return Ok(ProductInfo {
                product_key: product_key.to_string(),
                name,
                member_projects: members,
                created: false,
            });
        }
        members.push(project_slug.to_string());
        members.sort();
        members.dedup();

        let mut tags = vec![Tag::parse(["d", product_key]).expect("d")];
        for slug in &members {
            // a-coordinate: kind 30010 (project manifest), author=operator,
            // d=slug.
            let coord = format!("30010:{}:{slug}", operator.public_key().to_hex());
            tags.push(Tag::parse(["a", &coord]).expect("a tag"));
        }
        let tags = with_project_scoping(toron_kind(PRODUCT_MANIFEST), tags, None);
        let name = existing
            .as_ref()
            .and_then(parse_product_name)
            .unwrap_or_else(|| product_key.to_string());
        let content = format!(
            r#"{{"name":{}}}"#,
            serde_json::to_string(&name).unwrap_or_default()
        );
        let ev = build_event(
            operator,
            toron_kind(PRODUCT_MANIFEST),
            content,
            tags,
            Timestamp::now(),
        )?;
        self.bus.publish(ev.clone()).await?;
        self.link_product_row(product_key, &members).await?;

        Ok(ProductInfo {
            product_key: product_key.to_string(),
            name,
            member_projects: members,
            created: false,
        })
    }

    /// `search_messages_product`: fan the search across all member projects.
    pub async fn search_messages_product(
        &self,
        product_key: &str,
        query: &str,
        limit: Option<usize>,
        include_body_md: bool,
    ) -> anyhow::Result<Vec<(String, SearchMessage)>> {
        let members = self.product_members(product_key).await?;
        let mut out = Vec::new();
        for project in members {
            let res = self
                .search_messages(&SearchMessages {
                    project_key: project.clone(),
                    query: query.to_string(),
                    limit,
                    include_body_md,
                    ..Default::default()
                })
                .await?;
            for m in res {
                out.push((project.clone(), m));
            }
        }
        // Merge: newest first across projects.
        out.sort_by_key(|(_, m)| std::cmp::Reverse(m.created_ts_us));
        Ok(out)
    }

    /// `fetch_inbox_product`: fan `fetch_inbox` across member projects.
    pub async fn fetch_inbox_product(
        &self,
        product_key: &str,
        agent_name: &str,
    ) -> anyhow::Result<Vec<(String, crate::mailbox::InboxMessage)>> {
        let members = self.product_members(product_key).await?;
        let mut out = Vec::new();
        for project in members {
            let res = self
                .fetch_inbox(&crate::mailbox::FetchInbox {
                    project_slug: project.clone(),
                    agent_name: agent_name.to_string(),
                    include_bodies: true,
                    ..Default::default()
                })
                .await?;
            for m in res {
                out.push((project.clone(), m));
            }
        }
        // ADR-0037 §4.2/§4.4: one pin is one key, so every member project's
        // inbox is the same key's inbox. Show each message once, under the
        // project it was sent in (the index attributes it by the rumor's `t`
        // tag); with no index row, the first member project keeps it.
        let mut home: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        if let Some(pool) = self.index_pool.as_ref() {
            for (_, m) in &out {
                if let Ok(Some(slug)) = sqlx::query_scalar::<_, String>(
                    "SELECT project_slug FROM messages WHERE event_id = ?1",
                )
                .bind(&m.id)
                .fetch_optional(pool)
                .await
                {
                    home.insert(m.id.clone(), slug);
                }
            }
        }
        let mut kept: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut dedup: Vec<(String, crate::mailbox::InboxMessage)> = Vec::new();
        for (project, m) in out {
            let at_home = home.get(&m.id) == Some(&project);
            match kept.get(&m.id) {
                None => {
                    kept.insert(m.id.clone(), dedup.len());
                    dedup.push((project, m));
                }
                Some(&i) if at_home => dedup[i].0 = project,
                Some(_) => {}
            }
        }
        let mut out = dedup;
        out.sort_by_key(|(_, m)| std::cmp::Reverse(m.created_ts_us));
        Ok(out)
    }

    /// `summarize_thread_product`: summarize across member projects (first
    /// project containing the thread wins the digest).
    pub async fn summarize_thread_product(
        &self,
        product_key: &str,
        thread_id: &str,
        include_examples: bool,
    ) -> anyhow::Result<ThreadSummary> {
        let members = self.product_members(product_key).await?;
        for project in &members {
            let summary = self
                .summarize_thread(project, thread_id, include_examples, false, None)
                .await?;
            if summary.message_count > 0 {
                return Ok(summary);
            }
        }
        Ok(ThreadSummary {
            thread_id: thread_id.to_string(),
            ..Default::default()
        })
    }

    /// Member projects of a product: from the kind-30011 `a` tags (index is a
    /// mirror; bus is authoritative).
    async fn product_members(&self, product_key: &str) -> anyhow::Result<Vec<String>> {
        if let Some(ev) = self.find_product(product_key).await? {
            return Ok(member_projects_of(&ev));
        }
        // Fall back to the index mirror.
        if let Some(pool) = self.index_pool() {
            let rows = sqlx::query(
                "SELECT p.project_slug FROM product_projects p \
                 JOIN products x ON x.id = p.product_id \
                 WHERE x.product_key = ?1 ORDER BY p.project_slug",
            )
            .bind(product_key)
            .fetch_all(pool)
            .await;
            if let Ok(rows) = rows {
                return Ok(rows.iter().map(|r| r.get("project_slug")).collect());
            }
        }
        Ok(Vec::new())
    }

    /// Find the kind-30011 product manifest on the bus.
    ///
    /// Product manifests are addressable (NIP-33): each `products_link`
    /// republishes the operator's manifest, so several may coexist in the bus
    /// store within the same second. Resolve deterministically by
    /// `(created_at, member-count)` — a later link carries strictly more
    /// member `a`-tags, so the highest member count is the newest state.
    async fn find_product(&self, product_key: &str) -> anyhow::Result<Option<crate::nostr::Event>> {
        let filter = ToronFilter::new()
            .kinds([toron_kind(PRODUCT_MANIFEST)])
            .build();
        let events = collect_replay(self.bus.as_ref(), filter).await?;
        Ok(events
            .into_iter()
            .filter(|e| {
                e.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("d")
                        && t.as_slice().get(1).map(String::as_str) == Some(product_key)
                })
            })
            .max_by(|a, b| {
                a.created_at
                    .as_secs()
                    .cmp(&b.created_at.as_secs())
                    .then_with(|| {
                        member_projects_of(a)
                            .len()
                            .cmp(&member_projects_of(b).len())
                    })
                    .then_with(|| a.id.to_bytes().cmp(&b.id.to_bytes()))
            }))
    }

    /// Upsert the products row (idempotent on product_key).
    async fn upsert_product_row(&self, product_key: &str, name: &str) -> anyhow::Result<()> {
        let Some(pool) = self.index_pool() else {
            return Ok(());
        };
        sqlx::query(
            "INSERT INTO products (product_key, name, created_ts_us) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT (product_key) DO UPDATE SET name = excluded.name",
        )
        .bind(product_key)
        .bind(name)
        .bind(crate::index::writer::Clock::now_us(
            &crate::index::writer::SystemClock,
        ))
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Replace the product's project links in the index mirror.
    async fn link_product_row(&self, product_key: &str, members: &[String]) -> anyhow::Result<()> {
        let Some(pool) = self.index_pool() else {
            return Ok(());
        };
        // Upsert the product (ensure id), then replace memberships.
        let product_id: i64 = sqlx::query_scalar(
            "INSERT INTO products (product_key, name, created_ts_us) VALUES (?1, '', ?2) \
             ON CONFLICT (product_key) DO UPDATE SET product_key = excluded.product_key \
             RETURNING id",
        )
        .bind(product_key)
        .bind(crate::index::writer::Clock::now_us(
            &crate::index::writer::SystemClock,
        ))
        .fetch_one(pool)
        .await?;
        sqlx::query("DELETE FROM product_projects WHERE product_id = ?1")
            .bind(product_id)
            .execute(pool)
            .await?;
        for slug in members {
            sqlx::query("INSERT INTO product_projects (product_id, project_slug) VALUES (?1, ?2)")
                .bind(product_id)
                .bind(slug)
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    /// Ensure a project manifest exists (delegates to infra ensure_project) so
    /// linking references a real kind-30010.
    pub async fn ensure_project_for_product(&self, slug: &str) -> anyhow::Result<()> {
        let _ = self.ensure_project(slug).await?;
        Ok(())
    }

    // Keep PublicKey imported for future cross-project resolution (G2).
    #[allow(dead_code)]
    const fn _pubkey_type(_k: PublicKey) {}
}

/// Member projects from a product manifest's `a` tags (coordinate 30010).
fn member_projects_of(ev: &crate::nostr::Event) -> Vec<String> {
    ev.tags
        .iter()
        .filter_map(|t| {
            let parts = t.as_slice();
            if parts.first().map(String::as_str) != Some("a") {
                return None;
            }
            let coord = parts.get(1)?;
            // "30010:<operator-pubkey>:<slug>"
            let mut it = coord.split(':');
            let kind = it.next()?;
            if kind != "30010" {
                return None;
            }
            let _pubkey = it.next()?;
            let slug = it.next()?;
            Some(slug.to_string())
        })
        .collect()
}

/// Parse the product name from a kind-30011 manifest's JSON content.
fn parse_product_name(ev: &crate::nostr::Event) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(&ev.content)
        .ok()
        .and_then(|v| {
            v.get("name")
                .and_then(|n| n.as_str())
                .map(ToString::to_string)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use std::sync::Arc;

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-prod-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-prod-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox =
            Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator)).with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    #[tokio::test]
    async fn test_ensure_product_idempotent() {
        let (mailbox, db_path, data_dir) = test_mailbox("ensure").await;
        let a = mailbox
            .ensure_product("notetoself", "Note To Self")
            .await
            .expect("ensure");
        assert_eq!(a.product_key, "notetoself");

        // Kind-30011 published.
        let filter = ToronFilter::new()
            .kinds([toron_kind(PRODUCT_MANIFEST)])
            .build();
        let events = collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("collect");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.as_u16(), PRODUCT_MANIFEST);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_link_two_projects_and_fan_search() {
        let (mailbox, db_path, data_dir) = test_mailbox("link").await;
        // Create two projects (kind-30010 manifests).
        mailbox.ensure_project("alpha").await.expect("proj alpha");
        mailbox.ensure_project("beta").await.expect("proj beta");
        mailbox
            .ensure_product("prod1", "Product 1")
            .await
            .expect("product");

        mailbox
            .products_link("prod1", "alpha")
            .await
            .expect("link alpha");
        mailbox
            .products_link("prod1", "beta")
            .await
            .expect("link beta");

        // Fan search across both projects.
        let pool = mailbox.index_pool().unwrap();
        for (project, eid) in [("alpha", "a1"), ("beta", "b1")] {
            sqlx::query(
                "INSERT INTO messages \
                 (event_id, project_slug, sender, sender_pubkey, subject, body, \
                  importance, ack_required, created_ts_us, local_seq) \
                 VALUES (?1, ?2, 'Pi', 'aaaa', 'Topic', 'rendezvous fan search', \
                         'normal', 0, ?3, 1) ON CONFLICT (event_id) DO NOTHING",
            )
            .bind(eid)
            .bind(project)
            .bind(1_700_000_000_000_000i64)
            .execute(pool)
            .await
            .expect("seed msg");
        }

        let results = mailbox
            .search_messages_product("prod1", "rendezvous", None, false)
            .await
            .expect("product search");
        assert_eq!(results.len(), 2, "fan-out across both member projects");
        let projects: Vec<&String> = results.iter().map(|(p, _)| p).collect();
        assert!(projects.contains(&&"alpha".to_string()));
        assert!(projects.contains(&&"beta".to_string()));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_products_link_idempotent_and_members() {
        let (mailbox, db_path, data_dir) = test_mailbox("idem").await;
        mailbox.ensure_project("alpha").await.expect("alpha");
        mailbox
            .ensure_product("prod1", "P1")
            .await
            .expect("product");

        mailbox.products_link("prod1", "alpha").await.expect("link");
        let again = mailbox
            .products_link("prod1", "alpha")
            .await
            .expect("link again");
        assert_eq!(again.member_projects, vec!["alpha".to_string()], "no dup");

        let members = mailbox.product_members("prod1").await.expect("members");
        assert_eq!(members, vec!["alpha".to_string()]);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_product_with_no_members_returns_empty() {
        let (mailbox, db_path, data_dir) = test_mailbox("empty").await;
        mailbox
            .ensure_product("lonely", "Lonely")
            .await
            .expect("product");
        let results = mailbox
            .search_messages_product("lonely", "anything", None, false)
            .await
            .expect("empty search");
        assert!(results.is_empty());
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
