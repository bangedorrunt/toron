# Sourced by scripts/verify-mutations.sh.
# bd-ll7bs: an expired lease with waiters must hand over on the lease-board
# READ. Removing the read trigger leaves the waiters stranded; the expiry
# witness must go RED.

CARGO_ARGS=(-p toron-cli --test reserve_waitlist)
TEST_FILTER=(expiry_hands_over_without_any_release_call)

mutation "E1  lease-board reads never trigger expiry handoff" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    expect=expiry_hands_over_without_any_release_call \
    find='.is_ok_and(|w| !w.is_empty())' \
    replace='.is_ok_and(|w| false)'
