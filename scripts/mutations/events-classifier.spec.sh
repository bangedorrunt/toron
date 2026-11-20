# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034
# ADR-0039 U1 rework: the pure classifier must never leak another pin's facts,
# and the catalogue rows must use SEP-3415 camelCase wire keys.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(every_event_kind_requires_the_subscribers_own_subject catalogue_rows_use_sep3415_wire_keys)

CAT=crates/toron-core/src/events/catalogue.rs

mutation "E1  the own-pin subject guard is removed" \
    file=$CAT \
    expect=every_event_kind_requires_the_subscribers_own_subject \
    find='    if fact.subject_pin.as_deref() != Some(subscriber_pin) {' \
    replace='    if false {'

mutation "E2  the SEP-3415 camelCase wire keys are dropped" \
    file=$CAT \
    expect=catalogue_rows_use_sep3415_wire_keys \
    find='#[serde(rename_all = "camelCase")]' \
    replace='#[serde(rename_all = "snake_case")]'
