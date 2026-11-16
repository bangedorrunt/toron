# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# The default lease TTL is the reservation HEARTBEAT (3 minutes), not the
# 90-day envelope and not an idle lease. The test measures the stamped expiry
# against the lease event's own created_at, so a wrong default is a wrong
# number; the upper bound is a compile-time const assertion so the gate fails
# at build time rather than merely agreeing with itself at runtime.
#
# T1 sets the default above the heartbeat bound; the const assertion refuses to
# compile, which is the RED the test's name promises.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(test_default_lease_ttl_is_the_reservation_heartbeat_not_the_envelope)

mutation "T1  the default lease TTL stops being the heartbeat" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    find='pub const DEFAULT_RESERVATION_TTL_SECS: u64 = 3 * 60;' \
    replace='pub const DEFAULT_RESERVATION_TTL_SECS: u64 = 2000;' \
    expect=test_default_lease_ttl_is_the_reservation_heartbeat_not_the_envelope
