# Sourced by scripts/verify-mutations.sh.
# bd-nmilo: a dirty live holder must keep its lease. Removing that decision
# turns the dirty-evidence witness RED.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(dirty_live_extends_and_unknown_falls_back)

mutation "D1  dirty evidence no longer extends a lease" \
    file=crates/toron-core/src/mailbox/dirty_path.rs \
    expect=dirty_live_extends_and_unknown_falls_back \
    find='PathEvidence::Dirty { .. } => true,' \
    replace='PathEvidence::Dirty { .. } => false,'
