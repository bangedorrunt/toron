# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# The "newest wins" tiebreak.
#
# A Nostr timestamp is whole seconds, so two events for one key can share a
# `created_at`. Keying on the timestamp alone left the winner to iteration
# order — replay order, which nothing defines. The tiebreak keys on the event id,
# the sha256 of the serialized event: unique, and identical on every relay.
#
# Five call sites use it: two contact-policy lookups, profile-name resolution,
# last-active provenance, and the project manifest in infra.rs.
#
# What this spec does NOT prove, stated plainly rather than left implied:
#
#   * The call sites. `newest_tests` exercises `NewestEvent::newest` directly, so
#     reverting a call site back to `max_by_key(|e| e.created_at)` leaves the
#     helper untouched and every test here green. That is the builder-vs-verdict
#     seam again: a helper test cannot see what a caller does with it. Pinning a
#     call site needs an integration test that publishes two same-second events
#     and asserts which policy wins, which needs a mailbox and a projection.
#     Until that exists, `grep -rn 'max_by_key(|e| e.created_at' crates/` is the
#     only guard on migration completeness, and it is a source scan.
#
#   * An empty chain. `max_by_key` on an empty iterator is `None` by the
#     standard library's definition, so the empty-chain test documents intent
#     rather than pinning behaviour, and no mutation of the helper can break it.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(newest_tests)

# The tiebreak removed: the bug. With no id in the key, a shared timestamp
# resolves by position, and the reversed-order case is what catches it.
mutation "N1  the event id dropped from the tiebreak" \
    file=crates/toron-core/src/mailbox/mod.rs \
    expect=newest_breaks_a_tie_by_event_id_not_by_position \
    find='self.into_iter().max_by_key(|e| (e.created_at, e.id))' \
    replace='self.into_iter().max_by_key(|e| e.created_at)'

# The tiebreak made to dominate the timestamp, which would silently reorder
# events that are genuinely older. The precedence is the other half of the rule,
# and the fixture searches for a content pair that makes it observable.
mutation "N2  the id outranks the timestamp" \
    file=crates/toron-core/src/mailbox/mod.rs \
    expect=newest_prefers_the_later_timestamp_over_any_id \
    find='self.into_iter().max_by_key(|e| (e.created_at, e.id))' \
    replace='self.into_iter().max_by_key(|e| (e.id, e.created_at))'