// governed-by: ADR-0041
//! `InboxStore::unread_head`: the recipient's oldest unread message as one
//! point read, so a host's urgency check reads one row, or none.
//!
//! The head is defined as the first item `next` would serve past the same
//! offset; the parity test holds that as the log and the cursor move. The
//! plan test pins the index seek, and the 100k test shows the read stays one
//! row while the unread backlog and the other recipients' rows grow.

use std::num::NonZeroU64;
use std::time::Instant;

use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{
    HeadRow, INBOX_STREAM, InboxStore, UNREAD_HEAD_SQL, UNREAD_SQL, urgency_of,
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
