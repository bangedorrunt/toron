# Sourced by scripts/verify-mutations.sh.
# bd-nmilo: a dirty path with a live holder must never be handed to a waiter.
# If dirty evidence stops extending the lease, the waiter steals the path and
# the mailbox witness goes RED.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(dirty_path_stays_with_live_holder_clean_path_hands_to_waiter)

mutation "D2  dirty evidence no longer extends — waiter steals a dirty path" \
    file=crates/toron-core/src/mailbox/dirty_path.rs \
    expect=dirty_path_stays_with_live_holder_clean_path_hands_to_waiter \
    find='PathEvidence::Dirty { .. } => true,' \
    replace='PathEvidence::Dirty { .. } => false,'
