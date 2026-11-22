# Sourced by scripts/verify-mutations.sh.
# bd-kyute: a hold is a heartbeat. Removing the one-hour cap lets a dead pin
# block a file for months; the clamp witness must go RED.

CARGO_ARGS=(-p toron-cli --test reserve_waitlist)
TEST_FILTER=(ttl_above_the_cap_is_clamped)

mutation "C1  no cap on reserve --ttl-secs" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    expect=ttl_above_the_cap_is_clamped \
    find='pub const MAX_RESERVATION_TTL_SECS: u64 = 60 * 60;' \
    replace='pub const MAX_RESERVATION_TTL_SECS: u64 = 60 * 60 * 24 * 365;'
