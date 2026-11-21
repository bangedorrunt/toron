# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034
#
# bd-uak1x U4/U6 verify: a 30x is a failure (never followed, never delivered),
# and an oversized response body is refused before it is read.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(a_30x_redirect_is_a_failure_not_a_delivery a_body_over_the_cap_is_refused)

RETRY=crates/toron-core/src/events/retry.rs
EGRESS=crates/toron-core/src/bus/egress.rs

mutation "W1  a redirect is treated as a successful delivery" \
    file=$RETRY \
    expect=a_30x_redirect_is_a_failure_not_a_delivery \
    find='        Outcome::Status(code, _) if (300..=399).contains(code) => Decision::Terminated("redirect"),' \
    replace='        Outcome::Status(code, _) if (300..=399).contains(code) => Decision::Delivered,'

mutation "W2  the response body cap is never enforced" \
    file=$EGRESS \
    expect=a_body_over_the_cap_is_refused \
    find='    if len > policy.max_body_bytes {' \
    replace='    if false {'
