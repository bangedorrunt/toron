// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Search cluster (bd-led.5.6): search_messages, summarize_thread.
//!
//! FTS5 tokenizer contract (ADR-0003, oc-b bd-led.4.1):
//! `porter unicode61 remove_diacritics 2`, `prefix='2 3'`. Ranking is
//! **relevance-compatible** (SQLite bm25), NOT Search-V3 identical; recency
//! ties exact. NIP-50 accelerator is a later optional add — not parity-scope.

use sqlx::Row;

use super::Mailbox;

/// A search result message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchMessage {
    pub id: String,
    pub subject: String,
    pub from: String,
    pub created_ts_us: i64,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub body_md: Option<String>,
}

/// `search_messages` options.
#[derive(Debug, Clone, Default)]
pub struct SearchMessages {
    pub project_key: String,
    pub query: String,
    pub limit: Option<usize>,
    pub ranking: Option<String>, // "relevance" | "recency"
    pub sender_name: Option<String>,
    pub importance: Option<String>,
    pub thread_id: Option<String>,
    pub since_us: Option<i64>,
    pub until_us: Option<i64>,
    pub include_body_md: bool,
}

/// `summarize_thread` result (deterministic digest; LLM mode optional no-op).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ThreadSummary {
    pub thread_id: String,
    pub participants: Vec<String>,
    pub key_points: Vec<String>,
    pub action_items: Vec<String>,
    pub message_count: usize,
}

/// Quote FTS tokens containing characters outside `[A-Za-z0-9_*"]` so MATCH
/// does not read them as `column:term` (re:) or syntax (toron.dev, @npub).
/// Leaves boolean operators (AND/OR/NOT), already-quoted tokens, and
/// trailing-star prefix queries (`word*`) alone. Embedded quotes are escaped
/// by doubling.
pub(crate) fn sanitize_fts_query(q: &str) -> String {
    q.split_whitespace()
        .map(|tok| {
            let upper = tok.to_ascii_uppercase();
            if upper == "AND" || upper == "OR" || upper == "NOT" {
                return tok.to_string();
            }
            if tok.starts_with('"') || tok.ends_with('"') {
                return tok.to_string();
            }
            if tok.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'*') {
                return tok.to_string();
            }
            let esc = tok.replace('"', "\"\"");
            format!("\"{esc}\"")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

impl Mailbox {
    /// `search_messages`: FTS5 MATCH over the indexed messages for a project.
    ///
    /// The query string is passed to FTS5 MATCH (porter/unicode61/prefix
    /// contract). Filters: sender, importance, thread, date window. Ranking:
    /// `relevance` (bm25, ties by recency) default; `recency` (created_ts_us
    /// desc, ties exact).
    pub async fn search_messages(
        &self,
        args: &SearchMessages,
    ) -> anyhow::Result<Vec<SearchMessage>> {
        let pool = self
            .index_pool()
            .ok_or_else(|| anyhow::anyhow!("no index pool attached (Mailbox::with_index_pool)"))?;

        // An EMPTY query is invalid FTS5 ("syntax error near \"\"") and a bare
        // `*` is not a valid match-all — skip the FTS join entirely when there
        // is no text filter (thread-only / digest searches). Relevance ranking
        // degrades to recency (nothing to score).
        let empty_query = args.query.trim().is_empty();
        let (mut sql, mut bind_id, mut params) = if empty_query {
            (
                String::from(
                    "SELECT m.event_id, m.subject, m.sender, m.created_ts_us, m.importance, \
                            m.ack_required, m.thread_id, m.body \
                     FROM messages m \
                     WHERE m.project_slug = ?1 \
                       AND m.deleted = 0",
                ),
                2i32,
                vec![args.project_key.clone()],
            )
        } else {
            // Sanitize: FTS5 treats `foo-bar` as column `foo` term `bar` → "no such
            // column". Quote hyphenated tokens (bd-mail-actor / bead ids).
            let query = sanitize_fts_query(&args.query);
            (
                String::from(
                    "SELECT m.event_id, m.subject, m.sender, m.created_ts_us, m.importance, \
                            m.ack_required, m.thread_id, m.body \
                     FROM messages m \
                     JOIN messages_fts f ON f.rowid = m.id \
                     WHERE messages_fts MATCH ?1 \
                       AND m.project_slug = ?2 \
                       AND m.deleted = 0",
                ),
                3i32,
                vec![query, args.project_key.clone()],
            )
        };
        let since: Option<i64> = args.since_us;
        let until: Option<i64> = args.until_us;

        // Build a WHERE clause with placeholders and bind each in order.
        let mut filter_sql = String::new();
        if let Some(s) = args.sender_name.clone() {
            // The filter takes a NAME but the column stores the hex pubkey —
            // resolve through the agents table (per project). An unknown
            // name matches nothing (subquery → NULL), which is the correct
            // filter semantics.
            filter_sql.push_str(&format!(
                " AND m.sender_pubkey = (SELECT pubkey FROM agents WHERE project_slug = ?{} AND name = ?{})",
                bind_id,
                bind_id + 1
            ));
            params.push(args.project_key.clone());
            params.push(s);
            bind_id += 2;
        }
        if let Some(i) = args.importance.clone() {
            filter_sql.push_str(&format!(" AND m.importance = ?{}", bind_id));
            params.push(i);
            bind_id += 1;
        }
        if let Some(t) = args.thread_id.clone() {
            filter_sql.push_str(&format!(" AND m.thread_id = ?{}", bind_id));
            params.push(t);
            bind_id += 1;
        }
        if let Some(s) = since {
            filter_sql.push_str(&format!(" AND m.created_ts_us >= ?{}", bind_id));
            params.push(s.to_string());
            bind_id += 1;
        }
        if let Some(u) = until {
            filter_sql.push_str(&format!(" AND m.created_ts_us <= ?{}", bind_id));
            params.push(u.to_string());
            bind_id += 1;
        }

        sql.push_str(&filter_sql);
        match args.ranking.as_deref().unwrap_or("relevance") {
            "recency" => sql.push_str(" ORDER BY m.created_ts_us DESC, m.event_id ASC"),
            _ if empty_query => sql.push_str(" ORDER BY m.created_ts_us DESC, m.event_id ASC"),
            _ => sql.push_str(" ORDER BY bm25(messages_fts), m.created_ts_us DESC"),
        }
        let limit = args.limit.unwrap_or(100).min(1000);
        sql.push_str(&format!(" LIMIT ?{}", bind_id));
        params.push(limit.to_string());

        let mut q = sqlx::query(&sql);
        for p in &params {
            q = q.bind(p);
        }
        let rows = q.fetch_all(pool).await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let subject: String = row.get("subject");
            let from: String = row.get("sender");
            let created_ts_us: i64 = row.get("created_ts_us");
            let importance: String = row.get("importance");
            let ack_required: i64 = row.get("ack_required");
            let thread_id: Option<String> = row.get("thread_id");
            let body: String = row.get("body");
            out.push(SearchMessage {
                id: row.get("event_id"),
                subject,
                from,
                created_ts_us,
                importance,
                ack_required: ack_required != 0,
                thread_id,
                body_md: args.include_body_md.then_some(body),
            });
        }
        Ok(out)
    }

    /// `summarize_thread`: deterministic digest of a thread's messages —
    /// participants, key points (distinct subjects), action items (ack-required
    /// messages). `llm_mode` is an optional no-op (parity surface).
    pub async fn summarize_thread(
        &self,
        project_key: &str,
        thread_id: &str,
        _include_examples: bool,
        _llm_mode: bool,
        _llm_model: Option<&str>,
    ) -> anyhow::Result<ThreadSummary> {
        let pool = self
            .index_pool()
            .ok_or_else(|| anyhow::anyhow!("no index pool attached (Mailbox::with_index_pool)"))?;
        let rows = sqlx::query(
            "SELECT event_id, subject, sender, importance, ack_required, body \
             FROM messages WHERE project_slug = ?1 AND thread_id = ?2 \
             AND deleted = 0 \
             ORDER BY created_ts_us ASC, local_seq ASC",
        )
        .bind(project_key)
        .bind(thread_id)
        .fetch_all(pool)
        .await?;

        let mut participants: Vec<String> = Vec::new();
        let mut subjects: Vec<String> = Vec::new();
        let mut actions: Vec<String> = Vec::new();
        let mut count = 0usize;
        for row in &rows {
            count += 1;
            let sender: String = row.get("sender");
            if !participants.contains(&sender) {
                participants.push(sender);
            }
            let subject: String = row.get("subject");
            let ack: i64 = row.get("ack_required");
            if !subject.is_empty() && !subjects.contains(&subject) {
                subjects.push(subject.clone());
            }
            if ack != 0 {
                let action = if subject.is_empty() {
                    "message requiring acknowledgement".to_string()
                } else {
                    subject.clone()
                };
                if !actions.contains(&action) {
                    actions.push(action);
                }
            }
        }
        Ok(ThreadSummary {
            thread_id: thread_id.to_string(),
            participants,
            key_points: subjects,
            action_items: actions,
            message_count: count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use std::sync::Arc;

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-search-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-search-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox =
            Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator)).with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    /// Seed a message row directly into the index (projection writes through
    /// the SqliteProjectionSink path in real usage).
    #[allow(clippy::too_many_arguments)]
    async fn seed(
        mailbox: &Mailbox,
        project: &str,
        event_id: &str,
        subject: &str,
        body: &str,
        importance: &str,
        thread_id: Option<&str>,
        ts_us: i64,
    ) {
        let pool = mailbox.index_pool().unwrap();
        sqlx::query(
            "INSERT INTO messages \
             (event_id, project_slug, sender, sender_pubkey, subject, body, \
              importance, ack_required, thread_id, created_ts_us, local_seq) \
             VALUES (?1, ?2, 'PiAlpha', 'aaaa', ?3, ?4, ?5, 0, ?6, ?7, ?8) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event_id)
        .bind(project)
        .bind(subject)
        .bind(body)
        .bind(importance)
        .bind(thread_id)
        .bind(ts_us)
        .bind(1)
        .execute(pool)
        .await
        .expect("seed");
    }

    #[test]
    fn sanitize_fts_quotes_hyphenated_tokens() {
        assert_eq!(
            sanitize_fts_query("bd-adr7-residual starting"),
            "\"bd-adr7-residual\" starting"
        );
        assert_eq!(sanitize_fts_query("foo AND bar"), "foo AND bar");
        assert_eq!(
            sanitize_fts_query("\"already-quoted\""),
            "\"already-quoted\""
        );
    }

    /// Acceptance (S1): any token with characters outside [A-Za-z0-9_*"]
    /// gets quoted — colon, dot, @, unicode forms. Operators, quoted
    /// phrases and trailing-star prefix queries stay bare.
    #[test]
    fn sanitize_fts_quotes_special_char_tokens() {
        assert_eq!(sanitize_fts_query("re:"), "\"re:\"");
        assert_eq!(sanitize_fts_query("toron.dev"), "\"toron.dev\"");
        assert_eq!(sanitize_fts_query("@npub1abc"), "\"@npub1abc\"");
        assert_eq!(sanitize_fts_query("@npub1abc\u{2026}"), "\"@npub1abc\u{2026}\"");
        assert_eq!(sanitize_fts_query("a-b: c.d"), "\"a-b:\" \"c.d\"");
        assert_eq!(sanitize_fts_query("word*"), "word*");
        assert_eq!(sanitize_fts_query("hello_world foo"), "hello_world foo");
        assert_eq!(
            sanitize_fts_query("\"already quoted\" AND foo"),
            "\"already quoted\" AND foo"
        );
        assert_eq!(sanitize_fts_query("say\"hi-there"), "\"say\"\"hi-there\"");
    }

    /// Round-trip: search_messages must return hits (not SQL errors) for
    /// the real-world forms that broke MATCH — re:, toron.dev, @npub.
    #[tokio::test]
    async fn test_search_special_char_tokens_roundtrip() {
        let (mailbox, db_path, data_dir) = test_mailbox("special-fts").await;
        seed(
            &mailbox,
            "demo",
            "e-dom",
            "Site",
            "ping toron.dev for the catalog",
            "normal",
            None,
            1_700_000_000_000_100,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "e-npub",
            "Keys",
            "message @npub1abc\u{2026}hello sealed",
            "normal",
            None,
            1_700_000_000_000_200,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "e-re",
            "Re: question one",
            "answer",
            "normal",
            None,
            1_700_000_000_000_300,
        )
        .await;

        let dom = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "toron.dev".into(),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("toron.dev must not SQL-error");
        assert!(
            dom.iter().any(|m| m.id == "e-dom"),
            "toron.dev hit, got {:?}",
            dom.iter().map(|m| m.id.clone()).collect::<Vec<_>>()
        );

        let npub = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "@npub1abc".into(),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("@npub1abc must not SQL-error");
        assert!(
            npub.iter().any(|m| m.id == "e-npub"),
            "@npub hit, got {:?}",
            npub.iter().map(|m| m.id.clone()).collect::<Vec<_>>()
        );

        let re = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "re:".into(),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("re: must not SQL-error");
        assert!(
            re.iter().any(|m| m.id == "e-re"),
            "re: hit, got {:?}",
            re.iter().map(|m| m.id.clone()).collect::<Vec<_>>()
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Regression: an EMPTY query is invalid FTS5 ("syntax error near """)
    /// — thread-only / digest searches pass `query: ""` and must skip the
    /// FTS join, not error. Recency ranking still applies.
    #[tokio::test]
    async fn test_search_empty_query_returns_thread_without_error() {
        let (mailbox, db_path, data_dir) = test_mailbox("empty-fts").await;
        seed(
            &mailbox,
            "demo",
            "e-q1",
            "question one",
            "ask",
            "urgent",
            Some("collab-t1"),
            1_700_000_000_000_200,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "e-r1",
            "Re: question one",
            "reply",
            "normal",
            Some("collab-t1"),
            1_700_000_000_000_300,
        )
        .await;
        let hits = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: String::new(), // thread-only search
                thread_id: Some("collab-t1".into()),
                ranking: Some("recency".into()),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("empty query must not SQL-error");
        assert_eq!(hits.len(), 2, "both thread messages returned");
        assert_eq!(
            hits[0].id, "e-r1",
            "recency ranking: newest first (empty query, no FTS score)"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_search_hyphenated_bead_id_does_not_error() {
        let (mailbox, db_path, data_dir) = test_mailbox("hyphen-fts").await;
        seed(
            &mailbox,
            "demo",
            "e-hyphen",
            "[bd-adr7-residual-vxnv.1] starting",
            "wire trigger_tx",
            "normal",
            Some("bd-adr7-residual-vxnv.1"),
            1_700_000_000_000_100,
        )
        .await;
        let hits = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "bd-adr7-residual-vxnv.1".into(),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("hyphen query must not SQL-error");
        assert!(
            hits.iter().any(|h| h.subject.contains("bd-adr7")),
            "expected subject hit, got {hits:?}"
        );
        let _ = (db_path, data_dir);
    }

    #[tokio::test]
    async fn test_search_relevance_and_filters() {
        let (mailbox, db_path, data_dir) = test_mailbox("relevance").await;
        seed(
            &mailbox,
            "demo",
            "e1",
            "Running builds",
            "quick brown foxes jump fence",
            "normal",
            Some("T1"),
            1_700_000_000_000_000,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "e2",
            "Lunch",
            "rendezvous café résumé",
            "urgent",
            Some("T2"),
            1_700_000_000_000_001,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "e3",
            "Lunch plans",
            "quick rendezvous again",
            "normal",
            None,
            1_700_000_000_000_002,
        )
        .await;

        // Porter stem: "Running" → run, "builds" → build.
        let res = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "run".into(),
                ranking: Some("relevance".into()),
                ..Default::default()
            })
            .await
            .expect("search run");
        assert!(
            res.iter().any(|m| m.id == "e1"),
            "porter stem run -> running, got {:?}",
            res.iter().map(|m| m.id.clone()).collect::<Vec<_>>()
        );

        // remove_diacritics: café → cafe.
        let res2 = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "cafe".into(),
                ..Default::default()
            })
            .await
            .expect("search cafe");
        assert!(res2.iter().any(|m| m.id == "e2"));

        // Importance filter.
        let res3 = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "rendezvous".into(),
                importance: Some("urgent".into()),
                ..Default::default()
            })
            .await
            .expect("search urgent");
        assert_eq!(res3.len(), 1);
        assert_eq!(res3[0].id, "e2");

        // Thread filter.
        let res4 = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "quick".into(),
                thread_id: Some("T1".into()),
                ..Default::default()
            })
            .await
            .expect("search thread");
        assert_eq!(res4.len(), 1);
        assert_eq!(res4[0].id, "e1");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_search_recency_ranking() {
        let (mailbox, db_path, data_dir) = test_mailbox("recency").await;
        seed(
            &mailbox,
            "demo",
            "older",
            "Topic alpha",
            "rendezvous older",
            "normal",
            None,
            1_000_000_000_000_000,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "newer",
            "Topic beta",
            "rendezvous newer",
            "normal",
            None,
            2_000_000_000_000_000,
        )
        .await;

        let res = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "rendezvous".into(),
                ranking: Some("recency".into()),
                ..Default::default()
            })
            .await
            .expect("recency");
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].id, "newer", "recency ranking: newest first");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_search_include_bodies_and_limit() {
        let (mailbox, db_path, data_dir) = test_mailbox("body").await;
        seed(
            &mailbox,
            "demo",
            "e1",
            "A",
            "rendezvous body one",
            "normal",
            None,
            1,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "e2",
            "B",
            "rendezvous body two",
            "normal",
            None,
            2,
        )
        .await;

        let res = mailbox
            .search_messages(&SearchMessages {
                project_key: "demo".into(),
                query: "rendezvous".into(),
                limit: Some(1),
                include_body_md: true,
                ..Default::default()
            })
            .await
            .expect("search");
        assert_eq!(res.len(), 1);
        assert!(res[0].body_md.is_some(), "include_body_md returns the body");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_summarize_thread_digest() {
        let (mailbox, db_path, data_dir) = test_mailbox("thread").await;
        seed(
            &mailbox,
            "demo",
            "t1",
            "Fix the flaky test",
            "body",
            "normal",
            Some("T1"),
            1,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "t2",
            "Fix the flaky test",
            "reply",
            "normal",
            Some("T1"),
            2,
        )
        .await;
        seed(
            &mailbox,
            "demo",
            "t3",
            "Ship it",
            "body",
            "normal",
            Some("T1"),
            3,
        )
        .await;

        let summary = mailbox
            .summarize_thread("demo", "T1", false, false, None)
            .await
            .expect("summary");
        assert_eq!(summary.message_count, 3);
        assert!(summary.participants.contains(&"PiAlpha".to_string()));
        assert!(
            summary
                .key_points
                .contains(&"Fix the flaky test".to_string())
        );
        assert!(summary.key_points.contains(&"Ship it".to_string()));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
