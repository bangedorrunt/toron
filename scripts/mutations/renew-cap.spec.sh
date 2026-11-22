# Sourced by scripts/verify-mutations.sh.
# bd-kyute: renew must not undo the acquire cap. Removing the absolute-expiry
# clamp lets renew(3600) push a lease past now+900; witness must go RED.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(test_renew_counts_healthy_leases_bd_mbh0)

mutation "R1  renew ignores the absolute expiry cap" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    expect=test_renew_counts_healthy_leases_bd_mbh0 \
    find='                .min(cap_abs_secs);' \
    replace='                ;'
