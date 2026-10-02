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
# Why the call-site mutations need their own bus (bd-9w94):
#
#   Every call site is fed by `collect_replay`, and the default SQLite store
#   always returns `(created_at, event_id)` ASC — `event_store::scan_ids` sorts
#   DESC then reverses. So on `InProcessBus` a created_at-only max returns the
#   SAME event as the tiebreak, and N3-N7 below would all read VACUOUS against a
#   spec-driven test on that bus. They are not vacuous against
#   `newest_call_sites`, which drives the surfaces through `ArrivalOrderBus`:
#   `Bus::subscribe` promises no ordering, and `InProcessBus`'s live fanout
#   delivers a subscriber's tail in publish order, so arrival order is a legal
#   bus behaviour rather than an invented one.
#
#   The fixtures there sort the built events so the winner arrives LAST-but-one.
#   Event ids are content hashes, so which event wins is a coin flip; a fixture
#   that published in a fixed order would pass about half the time under each of
#   these mutations while reading as coverage.
#
# What this spec still does NOT prove, stated plainly rather than left implied:
#
#   * An empty chain. `max_by_key` on an empty iterator is `None` by the
#     standard library's definition, so the empty-chain test documents intent
#     rather than pinning behaviour, and no mutation of the helper can break it.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(newest)

# --- the helper -----------------------------------------------------------
# N1/N2 pin the rule itself. N3-N7 pin that each caller actually applies it;
# a helper test cannot see what a caller does with it, which is the
# builder-vs-verdict seam the first version of this spec walked into.

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

# --- the call sites -------------------------------------------------------
# Each of these reverts ONE caller to the pre-bd-d2zl form. Green would mean
# that caller's test cannot see the decision it claims to cover.

# contacts.rs `contact_policy_for`: the per-recipient policy lookup.
# Two-line needle on purpose: both `.newest()` calls in this file sit at the same
# indentation, so a one-line needle would mutate the SAME site twice and leave
# the second mutation VACUOUS against a test that never saw it.
mutation "N3  contact_policy_for drops the id tiebreak" \
    file=crates/toron-core/src/mailbox/contacts.rs \
    expect=contact_policy_for_picks_the_higher_id_on_a_shared_second \
    find=$'                    .filter(|e| e.pubkey == *agent_npub)\n                    .newest()' \
    replace=$'                    .filter(|e| e.pubkey == *agent_npub)\n                    .max_by_key(|e| e.created_at)'

# contacts.rs `BusPolicySource::load_roster`: the roster snapshot, one replay
# serving every agent in a project.
mutation "N4  the roster policy snapshot drops the id tiebreak" \
    file=crates/toron-core/src/mailbox/contacts.rs \
    expect=roster_policy_snapshot_picks_the_higher_id_on_a_shared_second \
    find=$'                    .filter(|e| e.pubkey.to_hex() == *hex)\n                    .newest()' \
    replace=$'                    .filter(|e| e.pubkey.to_hex() == *hex)\n                    .max_by_key(|e| e.created_at)'

# infra.rs `find_manifest`: the project manifest, reached through ensure_project.
mutation "N5  the project manifest lookup drops the id tiebreak" \
    file=crates/toron-core/src/mailbox/infra.rs \
    expect=ensure_project_names_the_higher_id_manifest_on_a_shared_second \
    find='Ok(events.iter().newest().cloned())' \
    replace='Ok(events.iter().max_by_key(|e| e.created_at).cloned())'

# identity.rs `whois` npub branch: the display name from the newest profile's
# `d` tag (an agent that renamed itself twice inside one second).
mutation "N6  whois takes the name from the last profile, not the newest" \
    file=crates/toron-core/src/mailbox/identity.rs \
    expect=whois_reads_the_higher_id_profile_on_a_shared_second \
    find=$'                .newest()\n                .and_then(|e| {' \
    replace=$'                .max_by_key(|e| e.created_at)\n                .and_then(|e| {'

# identity.rs `whois_build`: the profile FIELDS and last-active provenance.
mutation "N7  whois takes profile fields from the last profile" \
    file=crates/toron-core/src/mailbox/identity.rs \
    expect=whois_reads_the_higher_id_profile_on_a_shared_second \
    find=$'            .newest()\n            .ok_or_else(|| anyhow::anyhow!("profile event missing for {name}"))?;' \
    replace=$'            .max_by_key(|e| e.created_at)\n            .ok_or_else(|| anyhow::anyhow!("profile event missing for {name}"))?;'
