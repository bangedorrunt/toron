# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-fbiza (4): the beads-lifecycle polish loop through the durable driver must
# stop on the unchanged-digest round, after the minimum, and say why.
#
#   scripts/verify-mutations.sh scripts/mutations/beads-lifecycle-polish.spec.sh
#
# What this does not prove: the flywheel-side fence that produces the digest
# (`polish_round_contract`), covered by its own unit test.

CARGO_ARGS=(-p toron-workflow --test durable_host)
TEST_FILTER=(beads_lifecycle_polish_stops_on_an_unchanged_digest)

DURABLE=crates/toron-workflow/src/engine/durable.rs

mutation "P1  the loop honours the minimum one round late" \
    file=$DURABLE \
    expect=beads_lifecycle_polish_stops_on_an_unchanged_digest \
    find='let verdict = if progress.iteration >= progress.min {' \
    replace='let verdict = if progress.iteration > progress.min {'

mutation "P2  the stop reason lies about the arm that stopped it" \
    file=$DURABLE \
    expect=beads_lifecycle_polish_stops_on_an_unchanged_digest \
    find='"stopped": "until",' \
    replace='"stopped": "max",'
