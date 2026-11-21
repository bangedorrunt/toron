# Sourced by scripts/verify-mutations.sh. bd-w32ho: the auth guards live in
# route_events after the U1 stub was deleted; removing either must go RED.

CARGO_ARGS=(-p toron-mcp --test events_auth)
TEST_FILTER=(a_pin_token_cannot_reach_operator_only_verbs_but_operator_can tools_stay_token_less_r13)

mutation "A1  the D6 authorization guard is gone" \
    file=crates/toron-mcp/src/events/mod.rs \
    expect=a_pin_token_cannot_reach_operator_only_verbs_but_operator_can \
    find='    if !d6_authorize(principal, request_verb(method, params, principal)) {' \
    replace='    if false {'

mutation "A2  non-events methods are answered here" \
    file=crates/toron-mcp/src/events/mod.rs \
    expect=tools_stay_token_less_r13 \
    find='    if !method.starts_with("events/") {' \
    replace='    if false {'
