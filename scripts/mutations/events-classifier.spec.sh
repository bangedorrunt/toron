# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# ADR-0039 U1: the bus-fact classifier must not promote chatter or another
# pin's mail to an event for this subscriber.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(addressed_mail_maps_only_to_its_recipient sla_and_chatter_classify_as_specified)

CAT=crates/toron-core/src/events/catalogue.rs

mutation "E1  the recipient boundary is ignored" \
    file=$CAT \
    expect=addressed_mail_maps_only_to_its_recipient \
    find='        && fact.addressed_pin.as_deref() == Some(subscriber_pin)' \
    replace='        && true'
