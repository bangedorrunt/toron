# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# `InboxStore::unread_head` and `InboxStore::pending_urgent` (flywheel #53's
# point urgency read): the recipient's oldest unread row, and its oldest
# unread urgent row, past an offset, each as one index seek.
#
#   scripts/verify-mutations.sh scripts/mutations/comms-unread-head.spec.sh --cargo mbx
#
# Each mutation breaks ONE rule and the named test in the `comms_unread_head`
# integration suite must go red: the after-offset filter (in the shared point
# read, and in the query `next` shares), oldest-first order, the steer-priority
# urgency rule, pending_urgent ignoring urgency (in SQL, or by reading the FIFO
# head), the partial index drifting from the query's predicate, and the
# index losing its additive status (recording a version an older binary
# checks) or not being applied.
#
# Not run, because they cannot go red:
# - dropping `LIMIT 1` gives the same rows, since `fetch_optional` takes the
#   first row either way. Its cost is pinned by the plan and 100k tests.
# - dropping pending_urgent's `urgency_of` confirm while the SQL predicate
#   stands: the predicate is held equal to `urgency_of` case by case
#   (the_sql_urgency_predicate_is_urgency_of_and_the_index), so this mutant
#   changes no answer.
# The CLI parity (toron-cli comms_unread_head.rs) is its own test.

CARGO_ARGS=(-p toron-core --test comms_unread_head)
TEST_FILTER=(--quiet)

INBOX=crates/toron-core/src/inbox.rs
IDX=crates/toron-core/schema/additive/inbox_log_urgent.sql
SCHEMA=crates/toron-core/src/index/schema.rs

mutation "H1  the point read ignores after_offset" \
    file=$INBOX \
    expect=the_head_is_the_oldest_row_past_the_offset \
    find='            .bind(after as i64)' \
    replace='            .bind(0i64)'

mutation "H2  the shared unread query drops the offset filter" \
    file=$INBOX \
    expect=the_head_is_the_oldest_row_past_the_offset \
    find='AND log_offset > ?3"' \
    replace='AND log_offset > ?3 * 0"'

mutation "H3  the head is the newest row, not the oldest" \
    file=$INBOX \
    expect=the_head_is_the_oldest_row_past_the_offset \
    find=' ORDER BY log_offset LIMIT 1");' \
    replace=' ORDER BY log_offset DESC LIMIT 1");'

mutation "H4  every steer is urgent (priority not read)" \
    file=$INBOX \
    expect=an_urgent_steer_head_is_a_control_steer_and_urgent \
    find='.and_then(serde_json::Value::as_str) == Some("urgent");' \
    replace='.and_then(serde_json::Value::as_str).is_some();'

mutation "U1  pending_urgent ignores urgency in SQL (any unread row)" \
    file=$INBOX \
    expect=an_older_notify_ahead_of_an_urgent_steer_is_pending_urgent \
    find='    urgent_where!(),' \
    replace='    "1 = 1",'

mutation "U2  pending_urgent reads the FIFO head, not the urgent row" \
    file=$INBOX \
    expect=an_older_notify_ahead_of_an_urgent_steer_is_pending_urgent \
    find='.point_read(PENDING_URGENT_SQL, recipient, after)' \
    replace='.point_read(UNREAD_HEAD_SQL, recipient, after)'

mutation "U3  the partial index drifts from the query predicate" \
    file=$IDX \
    expect=the_sql_urgency_predicate_is_urgency_of_and_the_index \
    find="'\$.body.priority') = 'urgent';" \
    replace="'\$.body.priority') = 'URGENT';"

mutation "V1  the additive index records a version again (old binaries locked out)" \
    file=$SCHEMA \
    expect=a_store_with_the_urgent_index_opens_on_the_pre_index_path \
    find='        sqlx::query(ix.ddl)' \
    replace='        sqlx::query("INSERT OR IGNORE INTO _sqlx_migrations (version, description, success, checksum, execution_time) VALUES (31, '"'"'additive'"'"', 1, x'"'"'00'"'"', 0)").execute(pool).await?; sqlx::query(ix.ddl)'

mutation "V2  migrate never applies the additive index" \
    file=$SCHEMA \
    expect=pending_urgent_is_one_partial_index_seek_at_100k_rows_front_and_back \
    find='        out = ensure_additive(pool).await;' \
    replace='        out = Ok(());'
