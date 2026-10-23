# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# `InboxStore::unread_head` (flywheel #53's point urgency read): the
# recipient's oldest unread row past an offset, as one index seek.
#
#   scripts/verify-mutations.sh scripts/mutations/comms-unread-head.spec.sh --cargo mbx
#
# Each mutation breaks ONE rule of the head read and the named test in the
# `comms_unread_head` integration suite must go red: the after-offset filter
# (in the head read alone, and in the query `next` shares), oldest-first
# order, and the steer-priority urgency rule.
#
# What this does not prove: dropping `LIMIT 1` is an equivalent mutant for
# the result (`fetch_optional` takes the first row either way); its cost is
# pinned by the plan and 100k test, not by a red assertion. The CLI parity
# (toron-cli comms_unread_head.rs) is its own test.

CARGO_ARGS=(-p toron-core --test comms_unread_head)
TEST_FILTER=(--quiet)

INBOX=crates/toron-core/src/inbox.rs

mutation "H1  the head read ignores after_offset" \
    file=$INBOX \
    expect=the_head_is_the_oldest_row_past_the_offset \
    find='            .bind(after as i64)' \
    replace='            .bind(0i64)'

mutation "H2  the shared unread query drops the offset filter" \
    file=$INBOX \
    expect=the_head_is_the_oldest_row_past_the_offset \
    find='AND log_offset > ?3 ORDER BY log_offset"' \
    replace='AND log_offset > ?3 * 0 ORDER BY log_offset"'

mutation "H3  the head is the newest row, not the oldest" \
    file=$INBOX \
    expect=the_head_is_the_oldest_row_past_the_offset \
    find='AND log_offset > ?3 ORDER BY log_offset"' \
    replace='AND log_offset > ?3 ORDER BY log_offset DESC"'

mutation "H4  every steer is urgent (priority not read)" \
    file=$INBOX \
    expect=an_urgent_steer_head_is_a_control_steer_and_urgent \
    find='.and_then(serde_json::Value::as_str) == Some("urgent");' \
    replace='.and_then(serde_json::Value::as_str).is_some();'
