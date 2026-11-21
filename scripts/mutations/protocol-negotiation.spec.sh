#!/usr/bin/env bash
# Sourced by scripts/verify-mutations.sh.
# bd-9te8e: advertising only 2026-07-28 is the bug that broke pi. Restoring it
# must turn the advertisement witness RED.

CARGO_ARGS=(-p toron-mcp --lib)
TEST_FILTER=(all_five_protocol_versions_are_advertised_newest_first)

mutation "N1  only the newest protocol version is advertised" \
    file=crates/toron-mcp/src/server.rs \
    expect=all_five_protocol_versions_are_advertised_newest_first \
    find='    ProtocolVersion::V_2026_07_28,
    ProtocolVersion::V_2025_11_25,' \
    replace='    ProtocolVersion::V_2026_07_28,
    ProtocolVersion::V_2026_07_28,'
