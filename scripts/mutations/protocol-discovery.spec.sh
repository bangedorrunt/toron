#!/usr/bin/env bash
# Sourced by scripts/verify-mutations.sh.
# bd-958by: pi's auto probe sends server/discover with no _meta. Restoring the
# metadata requirement must turn the witness RED.

CARGO_ARGS=(-p toron-mcp --test protocol_negotiation)
TEST_FILTER=(bare_server_discover_needs_no_metadata)

mutation "D1  discover goes back through rmcp's metadata gate" \
    file=crates/toron-mcp/src/events_stream.rs \
    expect=bare_server_discover_needs_no_metadata \
    find='    if request_value["method"].as_str() == Some("server/discover") {' \
    replace='    if false {'
