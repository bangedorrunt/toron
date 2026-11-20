# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034
# ADR-0039 U1 rework: prove the three non-classifier guards can fail.

CARGO_ARGS=(-p toron-mcp --test events_list)
TEST_FILTER=(legacy_2025_client_calls_tools_without_per_request_meta capabilities_advertise_the_events_extension_only_under_extensions known_names_on_unimplemented_methods_are_errors_never_ok)

SRV=crates/toron-mcp/src/server.rs
EVT=crates/toron-mcp/src/events/mod.rs

mutation "U1  global stateless metadata requirement is restored" \
    file=$SRV \
    expect=legacy_2025_client_calls_tools_without_per_request_meta \
    find=$'                .with_legacy_session_mode(false)' \
    replace=$'                .with_legacy_session_mode(false)\n                .with_stateless_protocol_metadata_required(true)'

mutation "U2  capability value reverts to a custom version key" \
    file=$SRV \
    expect=capabilities_advertise_the_events_extension_only_under_extensions \
    find='let events = serde_json::json!({"listChanged": false});' \
    replace='let events = serde_json::json!({"version": "2026-11-20"});'

mutation "U3  subscribe fakes success for a known name" \
    file=$EVT \
    expect=known_names_on_unimplemented_methods_are_errors_never_ok \
    find=$'            Ok(name) => Err(ErrorData::new(\n                ErrorCode::METHOD_NOT_FOUND,\n                format!("{method} for {name} is not implemented in U1"),\n                None,\n            )),' \
    replace='            Ok(_name) => Ok(CustomResult(json!({"status": "not_implemented"}))),'
