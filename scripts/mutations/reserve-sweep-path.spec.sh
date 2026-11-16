#!/usr/bin/env bash
# Sourced by scripts/verify-mutations.sh.
# bd-9zqjo: `reserve sweep --closed-beads <path>` is the documented form. It
# was a bare boolean, so clap refused the path and departed pins had to be
# released one reason at a time.

CARGO_ARGS=(-p toron-cli --test cli_robot)
TEST_FILTER=(reserve_sweep)

mutation "S1  the bare form demands a path, so it stops being optional" \
    file=crates/toron-cli/src/cli.rs \
    expect=reserve_sweep_releases_only_closed_reasons \
    find='num_args = 0..=1' \
    replace='num_args = 1..=1'

mutation "S2  the explicit tracker path is accepted but ignored" \
    file=crates/toron-cli/src/robot.rs \
    expect=reserve_sweep_releases_only_closed_reasons \
    find='    let closed = crate::tracker::closed_bead_ids_at(&issues);' \
    replace='    let _ = &issues;
    let closed = crate::tracker::closed_bead_ids(&repo);'

mutation "S3  the bare form stops reading the repo tracker" \
    file=crates/toron-cli/src/robot.rs \
    expect=reserve_sweep_releases_only_closed_reasons \
    find='    let issues = if explicit.is_empty() {
        crate::tracker::issues_path(&repo)
    } else {
        std::path::PathBuf::from(explicit)
    };' \
    replace='    let issues = std::path::PathBuf::from(explicit);'
