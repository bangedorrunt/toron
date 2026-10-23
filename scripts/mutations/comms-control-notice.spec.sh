# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-toron-known-contracts-control-b7wx, toron half: the urgent ring budget
# is driven by an urgent steer read off the log, and a Control-class captain
# notice (escalate@1) is not urgent.
#
#   scripts/verify-mutations.sh scripts/mutations/comms-control-notice.spec.sh --cargo mbx
#
# Judged by the `comms_control_notice` integration suite (real append, real
# `next`).

CARGO_ARGS=(-p toron-core --test comms_control_notice)
TEST_FILTER=(--quiet)

INBOX=crates/toron-core/src/inbox.rs
WAKE=crates/toron-core/src/wake.rs

# Not a mutation here, a recorded finding: replacing `contract.name == "steer"`
# in `urgent_pending` with `class == Class::Control` keeps this suite GREEN
# (run 2026-10-23 with `expect_green`: DEAD-CONFIRMED). No other Control
# contract admits a `priority` field (every body is `deny_unknown_fields`),
# so the schemas, not the name check, keep a notice off the urgent budget
# today. U4 shows that schema guard is load-bearing for `cancel@1`. The name
# check stays as defence in depth for a future Control body with `priority`.

mutation "U2  an urgent read keeps the normal budget" \
    file=$WAKE \
    expect=an_urgent_steer_in_the_log_drives_the_smaller_ring_budget \
    find='            Self::Urgent => MAX_RINGS_URGENT,' \
    replace='            Self::Urgent => MAX_RINGS,'

mutation "U3  the priority field is not read (every steer is urgent)" \
    file=$INBOX \
    expect=an_urgent_steer_in_the_log_drives_the_smaller_ring_budget \
    find='                == Some("urgent")' \
    replace='                != Some("never")'

mutation "U4  cancel@1 admits a priority field" \
    file=crates/toron-core/src/contracts/cancel_v1.rs \
    expect=an_urgent_steer_in_the_log_drives_the_smaller_ring_budget \
    find=$'#[serde(deny_unknown_fields)]\npub struct CancelV1 {}' \
    replace=$'pub struct CancelV1 {}'
