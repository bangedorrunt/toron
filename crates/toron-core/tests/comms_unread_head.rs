// governed-by: ADR-0041
//! `InboxStore::unread_head`: the recipient's oldest unread message as one
//! point read, so a host's urgency check reads one row, or none.
//!
//! The head is defined as the first item `next` would serve past the same
//! offset; the parity test holds that as the log and the cursor move. The
//! plan test pins the index seek, and the 100k test shows the read stays one
//! row while the unread backlog and the other recipients' rows grow.
//!
//! `InboxStore::pending_urgent` is the oldest unread urgent row: `Some`
//! exactly when `next --peek --urgent` says urgent. Its SQL predicate is held
//! to `urgency_of` and to the additive partial index, and the 100k test
//! puts the urgent steer at the front and at the back of the backlog.

use std::num::NonZeroU64;
use std::time::Instant;

use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{
    HeadRow, INBOX_STREAM, InboxStore, PENDING_URGENT_SQL, UNREAD_HEAD_SQL, UNREAD_SQL,
    URGENT_WHERE, urgency_of, urgent_pending,
};
use toron_core::index::schema;
use toron_core::wake::Urgency;

const OP: &str = "0000000000000000000000000000000000000000000000000000000000000001";
/// The steering key, and the lead holder, so the fixture holds with or
/// without the F7 holder fence.
const SYS: &str = "5959595959595959595959595959595959595959595959595959595959595901";
const CREW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
const OTHER: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02";
const CTX: &str = "proj-head";

async fn store(tag: &str) -> InboxStore {
    let path =
        std::env::temp_dir().join(format!("toron-head-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool).with_operator(OP)
}

#[allow(clippy::too_many_arguments)]
fn env(
    from: &str,
    seq: u64,
    to: &str,
    class: Class,
    name: &str,
    task: Option<&str>,
    body: serde_json::Value,
) -> Envelope {
    Envelope {
        v: 1,
        msg: MsgId {
            producer: from.into(),
            epoch: 0,
            seq,
        },
        from: from.into(),
        to: Recipient::Npub(to.into()),
        class,
        contract: ContractId::new(name, 1),
        context: ContextId(CTX.into()),
        task: task.map(|t| TaskId(t.into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body,
    }
}

fn lead() -> Envelope {
    env(
        OP,
        0,
        SYS,
        Class::Control,
        "lead",
        None,
        serde_json::json!({"holder": SYS, "sup_epoch": 1}),
    )
}

fn steer(seq: u64, to: &str, priority: &str) -> Envelope {
    env(
        SYS,
        seq,
        to,
        Class::Control,
        "steer",
        Some("t-1"),
        serde_json::json!({"op": "pause", "priority": priority, "sup_epoch": 1}),
    )
}

fn notify(from: &str, seq: u64, to: &str, text: &str) -> Envelope {
    env(
        from,
        seq,
        to,
        Class::Notify,
        "notify",
        None,
        serde_json::json!({"text": text}),
    )
}

/// The head `next --peek` serves, in `HeadRow` terms.
async fn next_head(s: &InboxStore, reader: &str) -> (u64, Option<HeadRow>) {
    let b = s.next(reader, None, true).await.unwrap();
    let head = b.items.first().map(|it| HeadRow {
        offset: NonZeroU64::new(it.offset).unwrap(),
        msg_key: it.msg_key.clone(),
        contract: it.envelope.contract.clone(),
        class: it.envelope.class,
        urgency: urgency_of(&it.envelope),
    });
    (b.cursor_before, head)
}

/// What `next --peek --urgent` decides (`urgent_pending`), and the first
/// urgent item of the same batch, in `HeadRow` terms.
async fn next_urgent(s: &InboxStore, reader: &str) -> (u64, Option<HeadRow>, bool) {
    let b = s.next(reader, None, true).await.unwrap();
    let first = b
        .items
        .iter()
        .find(|it| urgency_of(&it.envelope) == Urgency::Urgent)
        .map(|it| HeadRow {
            offset: NonZeroU64::new(it.offset).unwrap(),
            msg_key: it.msg_key.clone(),
            contract: it.envelope.contract.clone(),
            class: it.envelope.class,
            urgency: Urgency::Urgent,
        });
    (b.cursor_before, first, urgent_pending(&b.items))
}

#[tokio::test]
async fn an_empty_inbox_has_no_head() {
    let s = store("empty").await;
    assert_eq!(s.unread_head(CREW, 0).await.unwrap(), None);
    // Another recipient's mail is not this recipient's head.
    s.append_envelope(&notify(SYS, 0, OTHER, "hi"))
        .await
        .unwrap();
    assert_eq!(s.unread_head(CREW, 0).await.unwrap(), None);
    assert!(s.unread_head(OTHER, 0).await.unwrap().is_some());
}

#[tokio::test]
async fn an_urgent_steer_head_is_a_control_steer_and_urgent() {
    let s = store("steer").await;
    s.append_envelope(&lead()).await.unwrap();
    let a = s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap();
    let h = s.unread_head(CREW, 0).await.unwrap().expect("a head");
    assert_eq!(h.offset.get(), a.offset);
    assert_eq!(h.msg_key, format!("{SYS}:0:0"));
    assert_eq!(h.contract, ContractId::new("steer", 1));
    assert_eq!(h.class, Class::Control);
    assert_eq!(h.urgency, Urgency::Urgent);
    // A normal-priority steer is a steer head and not urgent.
    let s = store("steer-normal").await;
    s.append_envelope(&lead()).await.unwrap();
    s.append_envelope(&steer(0, CREW, "normal")).await.unwrap();
    let h = s.unread_head(CREW, 0).await.unwrap().unwrap();
    assert_eq!(h.contract, ContractId::new("steer", 1));
    assert_eq!(h.urgency, Urgency::Normal);
}

#[tokio::test]
async fn a_notify_head_is_normal_whatever_its_text() {
    let s = store("notify").await;
    s.append_envelope(&notify(SYS, 0, CREW, "priority: urgent"))
        .await
        .unwrap();
    let h = s.unread_head(CREW, 0).await.unwrap().unwrap();
    assert_eq!(h.contract, ContractId::new("notify", 1));
    assert_eq!(h.class, Class::Notify);
    assert_eq!(h.urgency, Urgency::Normal);
}

#[tokio::test]
async fn the_head_is_the_oldest_row_past_the_offset() {
    let s = store("after").await;
    s.append_envelope(&lead()).await.unwrap();
    let n = s
        .append_envelope(&notify(OP, 1, CREW, "first"))
        .await
        .unwrap();
    s.append_envelope(&notify(SYS, 0, OTHER, "not yours"))
        .await
        .unwrap();
    let st = s.append_envelope(&steer(1, CREW, "urgent")).await.unwrap();
    // Oldest first, not newest: the notify, though an urgent steer is behind it.
    let h = s.unread_head(CREW, 0).await.unwrap().unwrap();
    assert_eq!(h.offset.get(), n.offset);
    assert_eq!(h.urgency, Urgency::Normal);
    // Past the notify, the steer; the other recipient's row is skipped.
    let h = s.unread_head(CREW, n.offset).await.unwrap().unwrap();
    assert_eq!(h.offset.get(), st.offset);
    assert_eq!(h.urgency, Urgency::Urgent);
    // Past the last row, nothing.
    assert_eq!(s.unread_head(CREW, st.offset).await.unwrap(), None);
}

#[tokio::test]
async fn after_next_and_an_ack_the_head_moves_past_the_read_row() {
    let s = store("ack").await;
    s.append_envelope(&lead()).await.unwrap();
    s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap();
    let read = s.next(CREW, None, false).await.unwrap();
    assert_eq!(read.items.len(), 1);
    let key = read.items[0].msg_key.clone();
    s.ack(CREW, &key, true, None).await.expect("crew accepts");
    // The cursor is keyed by the recipient stream (the npub), as `next` commits it.
    let cursor = s.cursor(CREW, CREW).await.unwrap();
    assert_eq!(cursor, read.cursor_after);
    // Read and acked: nothing unread past the cursor.
    assert_eq!(s.unread_head(CREW, cursor).await.unwrap(), None);
    // The next row is the head again.
    let a = s
        .append_envelope(&notify(OP, 1, CREW, "later"))
        .await
        .unwrap();
    let h = s.unread_head(CREW, cursor).await.unwrap().unwrap();
    assert_eq!(h.offset.get(), a.offset);
    assert_eq!(h.contract, ContractId::new("notify", 1));
}

#[tokio::test]
async fn the_head_is_the_first_item_next_serves_as_the_log_and_cursor_move() {
    let s = store("parity").await;
    s.append_envelope(&lead()).await.unwrap();
    let (mut op, mut sys) = (1u64, 0u64);
    for round in 0..12u64 {
        // Mixed mail for both recipients; urgent and normal steers.
        match round % 4 {
            0 => {
                s.append_envelope(&notify(OP, op, CREW, "n")).await.unwrap();
                op += 1;
            }
            1 => {
                let p = if round % 8 == 1 { "urgent" } else { "normal" };
                s.append_envelope(&steer(sys, CREW, p)).await.unwrap();
                sys += 1;
            }
            2 => {
                s.append_envelope(&notify(OP, op, OTHER, "x"))
                    .await
                    .unwrap();
                op += 1;
            }
            _ => {}
        }
        let (cursor, served) = next_head(&s, CREW).await;
        assert_eq!(
            s.unread_head(CREW, cursor).await.unwrap(),
            served,
            "round {round}: the head is what next would serve"
        );
        let (cursor, urgent, any) = next_urgent(&s, CREW).await;
        let pending = s.pending_urgent(CREW, cursor).await.unwrap();
        assert_eq!(pending, urgent, "round {round}: the first urgent item");
        assert_eq!(pending.is_some(), any, "round {round}: --peek --urgent");
        // Every third round the crew reads, so the cursor moves.
        if round % 3 == 2 {
            s.next(CREW, None, false).await.unwrap();
        }
    }
    let (cursor, served) = next_head(&s, CREW).await;
    assert_eq!(s.unread_head(CREW, cursor).await.unwrap(), served);
}

#[tokio::test]
async fn the_head_read_is_one_index_seek_at_100k_rows() {
    let s = store("100k").await;
    s.append_envelope(&lead()).await.unwrap();
    let first = s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap();
    // 100k more rows: 50k unread for CREW behind its head, 50k for OTHER.
    // Bulk-inserted in one statement (a fixture shortcut: they copy the
    // head's payload and get their own triples), not through append.
    let pool = s.pool().clone();
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100000) \
         INSERT INTO inbox_log (stream, recipient, producer, epoch, seq, msg_key, payload, appended_ts_us) \
         SELECT 'inbox', CASE WHEN i % 2 = 0 THEN ?1 ELSE ?2 END, 'bulk', 0, i, 'bulk:0:' || i, \
                (SELECT payload FROM inbox_log WHERE log_offset = ?3), 0 FROM n",
    )
    .bind(CREW)
    .bind(OTHER)
    .bind(first.offset as i64)
    .execute(&pool)
    .await
    .unwrap();
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbox_log")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(total > 100_000);

    // The plan: one search of the recipient index, bounded by the offset, no
    // sort and no scan.
    let plan: Vec<String> = sqlx::query(&format!("EXPLAIN QUERY PLAN {UNREAD_HEAD_SQL}"))
        .bind(INBOX_STREAM)
        .bind(CREW)
        .bind(0i64)
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| sqlx::Row::get::<String, _>(r, 3))
        .collect();
    assert_eq!(plan.len(), 1, "{plan:?}");
    assert!(
        plan[0].starts_with("SEARCH inbox_log USING")
            && plan[0].contains("inbox_log_by_recipient")
            && plan[0].contains("log_offset>?"),
        "{plan:?}"
    );
    assert!(
        !plan
            .iter()
            .any(|p| p.contains("TEMP B-TREE") || p.starts_with("SCAN"))
    );

    // One row back, at the head and past it, however long the backlog.
    let t = Instant::now();
    let runs = 200u32;
    let mut h = None;
    for _ in 0..runs {
        h = s.unread_head(CREW, 0).await.unwrap();
    }
    let head_us = t.elapsed().as_micros() / u128::from(runs);
    let h = h.unwrap();
    assert_eq!(h.offset.get(), first.offset);
    assert_eq!(h.urgency, Urgency::Urgent);
    let mid = s.unread_head(CREW, 60_000).await.unwrap().unwrap();
    assert!(mid.offset.get() > 60_000);

    // The same predicate without the limit: what reading the whole unread
    // set costs, for scale.
    let t = Instant::now();
    let all = sqlx::query(UNREAD_SQL)
        .bind(INBOX_STREAM)
        .bind(CREW)
        .bind(0i64)
        .fetch_all(&pool)
        .await
        .unwrap();
    let all_us = t.elapsed().as_micros();
    assert_eq!(all.len(), 50_001);
    println!(
        "bench: {total} rows in the log; unread_head 1 row in {head_us} us (mean of {runs}); \
         the full unread read {} rows in {all_us} us",
        all.len()
    );
}

#[tokio::test]
async fn an_older_notify_ahead_of_an_urgent_steer_is_pending_urgent() {
    let s = store("pu-behind").await;
    s.append_envelope(&lead()).await.unwrap();
    let n = s
        .append_envelope(&notify(OP, 1, CREW, "older"))
        .await
        .unwrap();
    s.append_envelope(&steer(0, CREW, "normal")).await.unwrap();
    s.append_envelope(&steer(1, OTHER, "urgent")).await.unwrap();
    let st = s.append_envelope(&steer(2, CREW, "urgent")).await.unwrap();
    s.append_envelope(&notify(OP, 2, CREW, "newer"))
        .await
        .unwrap();
    // FIFO head is the notify; the pending urgent row is the steer behind it,
    // not the other recipient's.
    assert_eq!(
        s.unread_head(CREW, 0).await.unwrap().unwrap().offset.get(),
        n.offset
    );
    let p = s
        .pending_urgent(CREW, 0)
        .await
        .unwrap()
        .expect("an urgent steer is pending");
    assert_eq!(p.offset.get(), st.offset);
    assert_eq!(p.contract, ContractId::new("steer", 1));
    assert_eq!(p.class, Class::Control);
    assert_eq!(p.urgency, Urgency::Urgent);
    // Past it, nothing urgent is pending.
    assert_eq!(s.pending_urgent(CREW, st.offset).await.unwrap(), None);
}

#[tokio::test]
async fn only_notifies_and_normal_steers_are_not_pending_urgent() {
    let s = store("pu-none").await;
    assert_eq!(s.pending_urgent(CREW, 0).await.unwrap(), None);
    s.append_envelope(&lead()).await.unwrap();
    s.append_envelope(&notify(OP, 1, CREW, "priority: urgent"))
        .await
        .unwrap();
    s.append_envelope(&steer(0, CREW, "normal")).await.unwrap();
    s.append_envelope(&notify(OP, 2, CREW, "URGENT"))
        .await
        .unwrap();
    assert_eq!(s.pending_urgent(CREW, 0).await.unwrap(), None);
    let (_, _, any) = next_urgent(&s, CREW).await;
    assert!(!any, "--peek --urgent agrees");
}

#[tokio::test]
async fn a_read_and_acked_urgent_steer_is_not_pending() {
    let s = store("pu-acked").await;
    s.append_envelope(&lead()).await.unwrap();
    s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap();
    assert!(s.pending_urgent(CREW, 0).await.unwrap().is_some());
    let read = s.next(CREW, None, false).await.unwrap();
    s.ack(CREW, &read.items[0].msg_key, true, None)
        .await
        .expect("crew accepts");
    let cursor = s.cursor(CREW, CREW).await.unwrap();
    assert_eq!(s.pending_urgent(CREW, cursor).await.unwrap(), None);
    let (c, _, any) = next_urgent(&s, CREW).await;
    assert_eq!(c, cursor);
    assert!(!any);
    // A later normal steer does not revive it.
    s.append_envelope(&steer(1, CREW, "normal")).await.unwrap();
    assert_eq!(s.pending_urgent(CREW, cursor).await.unwrap(), None);
}

/// One whitespace-normalised spelling, for comparing SQL texts.
fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[tokio::test]
async fn the_sql_urgency_predicate_is_urgency_of_and_the_index() {
    // The partial index declares the same predicate the query uses, so the
    // planner can match them.
    let ix = schema::ADDITIVE_INDEXES
        .iter()
        .find(|ix| ix.name == "inbox_log_urgent")
        .expect("the urgent index is additive");
    let migration = ix.ddl;
    assert!(
        squash(migration).contains(&format!("WHERE {};", squash(URGENT_WHERE))),
        "the additive index and URGENT_WHERE drifted"
    );
    assert!(squash(PENDING_URGENT_SQL).contains(&squash(URGENT_WHERE)));
    // And the predicate is urgency_of, case by case.
    let s = store("pred").await;
    let cases = [
        ("steer", 1, serde_json::json!({"priority": "urgent"})),
        ("steer", 1, serde_json::json!({"priority": "normal"})),
        ("steer", 1, serde_json::json!({})),
        ("steer", 1, serde_json::json!({"priority": true})),
        ("steer", 1, serde_json::json!({"priority": "URGENT"})),
        ("steer", 2, serde_json::json!({"priority": "urgent"})),
        (
            "notify",
            1,
            serde_json::json!({"priority": "urgent", "text": "urgent"}),
        ),
        ("cancel", 1, serde_json::json!({"priority": "urgent"})),
        ("escalate", 1, serde_json::json!({"priority": "urgent"})),
    ];
    for (name, major, body) in cases {
        let mut e = notify(OP, 0, CREW, "x");
        e.contract = ContractId::new(name, major);
        e.body = body.clone();
        let sql = format!("SELECT ({URGENT_WHERE}) IS 1 FROM (SELECT ?1 AS payload)");
        let in_sql: bool = sqlx::query_scalar(&sql)
            .bind(serde_json::to_string(&e).unwrap())
            .fetch_one(s.pool())
            .await
            .unwrap();
        assert_eq!(
            in_sql,
            urgency_of(&e) == Urgency::Urgent,
            "{name}@{major} {body}"
        );
    }
}

/// `rows` bulk rows for CREW and OTHER (half each), copying the payload at
/// `template`: a fixture shortcut, not append.
async fn bulk(s: &InboxStore, template: u64, rows: u32, tag: &str) {
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?4) \
         INSERT INTO inbox_log (stream, recipient, producer, epoch, seq, msg_key, payload, appended_ts_us) \
         SELECT 'inbox', CASE WHEN i % 2 = 0 THEN ?1 ELSE ?2 END, ?5, 0, i, ?5 || ':0:' || i, \
                (SELECT payload FROM inbox_log WHERE log_offset = ?3), 0 FROM n",
    )
    .bind(CREW)
    .bind(OTHER)
    .bind(template as i64)
    .bind(i64::from(rows))
    .bind(tag)
    .execute(s.pool())
    .await
    .unwrap();
}

async fn plan_of(s: &InboxStore, sql: &str) -> Vec<String> {
    sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
        .bind(INBOX_STREAM)
        .bind(CREW)
        .bind(0i64)
        .fetch_all(s.pool())
        .await
        .unwrap()
        .iter()
        .map(|r| sqlx::Row::get::<String, _>(r, 3))
        .collect()
}

/// Mean microseconds of `pending_urgent(CREW, 0)` over `runs`, and its answer.
async fn time_pending(s: &InboxStore, runs: u32) -> (u128, Option<HeadRow>) {
    let t = Instant::now();
    let mut h = None;
    for _ in 0..runs {
        h = s.pending_urgent(CREW, 0).await.unwrap();
    }
    (t.elapsed().as_micros() / u128::from(runs), h)
}

/// What answering the same question by reading the unread set costs: every
/// unread row, each payload parsed and judged by `urgency_of`.
async fn time_scan(s: &InboxStore) -> (u128, usize) {
    let t = Instant::now();
    let rows = sqlx::query(UNREAD_SQL)
        .bind(INBOX_STREAM)
        .bind(CREW)
        .bind(0i64)
        .fetch_all(s.pool())
        .await
        .unwrap();
    let n = rows.len();
    let _first = rows.iter().find(|r| {
        let e: Envelope = serde_json::from_str(&sqlx::Row::get::<String, _>(*r, 2)).unwrap();
        urgency_of(&e) == Urgency::Urgent
    });
    (t.elapsed().as_micros(), n)
}

#[tokio::test]
async fn pending_urgent_is_one_partial_index_seek_at_100k_rows_front_and_back() {
    for at_front in [true, false] {
        let where_ = if at_front { "front" } else { "back" };
        let s = store(&format!("pu-100k-{where_}")).await;
        s.append_envelope(&lead()).await.unwrap();
        let template = s
            .append_envelope(&notify(OP, 1, CREW, "normal"))
            .await
            .unwrap()
            .offset;
        let urgent = if at_front {
            let u = s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap();
            bulk(&s, template, 100_000, "bulk").await;
            u
        } else {
            bulk(&s, template, 100_000, "bulk").await;
            s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap()
        };

        let plan = plan_of(&s, PENDING_URGENT_SQL).await;
        assert_eq!(plan.len(), 1, "{where_}: {plan:?}");
        assert!(
            plan[0].starts_with("SEARCH inbox_log USING INDEX inbox_log_urgent")
                && plan[0].contains("log_offset>?"),
            "{where_}: {plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|p| p.contains("TEMP B-TREE") || p.starts_with("SCAN"))
        );

        let runs = 200;
        let (us, h) = time_pending(&s, runs).await;
        let h = h.expect("the urgent steer is pending");
        assert_eq!(h.offset.get(), urgent.offset, "{where_}");
        assert_eq!(h.urgency, Urgency::Urgent);
        let (scan_us, scanned) = time_scan(&s).await;
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbox_log")
            .fetch_one(s.pool())
            .await
            .unwrap();
        assert!(total > 100_000);
        println!(
            "bench pending_urgent, urgent steer at the {where_} (offset {}): {total} rows in the log, {scanned} \
             unread for the reader; pending_urgent 1 row in {us} us (mean of {runs}); \
             reading the unread set and judging each row: {scanned} rows in {scan_us} us; \
             plan: {}",
            urgent.offset, plan[0]
        );
    }
}

/// The highest versioned migration on main 5cdd555, before the urgent index.
/// A pre-index binary resolves exactly this.
const PRE_INDEX_MAX_MIGRATION: i64 = 30;

#[tokio::test]
async fn a_store_with_the_urgent_index_opens_on_the_pre_index_path() {
    // A store opened by this build: migrated, and the additive index applied.
    let path =
        std::env::temp_dir().join(format!("toron-head-compat-{}.sqlite", uuid::Uuid::new_v4()));
    let db = path.to_str().unwrap().to_string();
    let pool = schema::connect(&db).await.unwrap();
    schema::migrate(&pool).await.unwrap();
    let v = schema::store_version(&pool).await.unwrap();
    assert_eq!(
        v,
        schema::StoreVersion {
            min_binary: PRE_INDEX_MAX_MIGRATION,
            additive: vec!["inbox_log_urgent", "inbox_stream_close"],
        },
        "the index raises no version a binary checks"
    );
    let s = InboxStore::new(pool).with_operator(OP);
    s.append_envelope(&lead()).await.unwrap();
    s.append_envelope(&notify(OP, 1, CREW, "before"))
        .await
        .unwrap();
    drop(s);

    // The pre-index open path: the versioned migrations alone, as 5cdd555's
    // `schema::migrate` runs them, with no additive step. It resolves the
    // same versions and opens the store with no skew.
    let max = schema::MIGRATOR.iter().map(|m| m.version).max();
    assert_eq!(max, Some(PRE_INDEX_MAX_MIGRATION));
    let old = schema::connect(&db).await.unwrap();
    schema::MIGRATOR
        .run(&old)
        .await
        .expect("a pre-index binary opens the store: no store_skew");

    // It writes and reads as before. Its insert lands in the index too:
    // SQLite maintains a partial index on every insert.
    let s = InboxStore::new(old).with_operator(OP);
    let st = s.append_envelope(&steer(0, CREW, "urgent")).await.unwrap();
    let b = s.next(CREW, None, true).await.unwrap();
    assert_eq!(b.items.len(), 2);
    assert_eq!(b.items[1].offset, st.offset);
    assert_eq!(
        plan_of(&s, PENDING_URGENT_SQL).await[0],
        "SEARCH inbox_log USING INDEX inbox_log_urgent (stream=? AND recipient=? AND log_offset>?)"
    );
    let p = s.pending_urgent(CREW, 0).await.unwrap().unwrap();
    assert_eq!(
        p.offset.get(),
        st.offset,
        "the old binary's write is in the index"
    );

    // And this build reopens it: still no version raised, index still there.
    schema::migrate(s.pool()).await.unwrap();
    assert_eq!(
        schema::store_version(s.pool()).await.unwrap().min_binary,
        PRE_INDEX_MAX_MIGRATION
    );
}

#[tokio::test]
async fn pending_urgent_is_correct_without_the_index() {
    let s = store("noindex").await;
    sqlx::query("DROP INDEX inbox_log_urgent")
        .execute(s.pool())
        .await
        .unwrap();
    assert_eq!(
        schema::store_version(s.pool()).await.unwrap().additive,
        vec!["inbox_stream_close"],
        "the dropped index is gone; the additive table is still there"
    );
    // The planner falls back to the recipient index: slower, same answers.
    let plan = plan_of(&s, PENDING_URGENT_SQL).await;
    assert!(
        plan[0].contains("USING INDEX inbox_log_by_recipient"),
        "{plan:?}"
    );

    assert_eq!(s.pending_urgent(CREW, 0).await.unwrap(), None);
    s.append_envelope(&lead()).await.unwrap();
    s.append_envelope(&notify(OP, 1, CREW, "priority: urgent"))
        .await
        .unwrap();
    s.append_envelope(&steer(0, CREW, "normal")).await.unwrap();
    assert_eq!(
        s.pending_urgent(CREW, 0).await.unwrap(),
        None,
        "only normal rows"
    );
    s.append_envelope(&steer(1, OTHER, "urgent")).await.unwrap();
    let st = s.append_envelope(&steer(2, CREW, "urgent")).await.unwrap();
    let p = s
        .pending_urgent(CREW, 0)
        .await
        .unwrap()
        .expect("behind the notify");
    assert_eq!(p.offset.get(), st.offset);
    let (c, first, any) = next_urgent(&s, CREW).await;
    assert_eq!((Some(p), true), (first, any));
    let read = s.next(CREW, None, false).await.unwrap();
    s.ack(CREW, &read.items[2].msg_key, true, None)
        .await
        .unwrap();
    let cursor = s.cursor(CREW, CREW).await.unwrap();
    assert!(cursor > c);
    assert_eq!(
        s.pending_urgent(CREW, cursor).await.unwrap(),
        None,
        "read and acked"
    );

    // The next open by this build puts the index back.
    schema::migrate(s.pool()).await.unwrap();
    assert_eq!(
        schema::store_version(s.pool()).await.unwrap().additive,
        vec!["inbox_log_urgent", "inbox_stream_close"]
    );
}
