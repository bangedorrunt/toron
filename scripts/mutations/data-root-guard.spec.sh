# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# The data-root guard, across a process spawn: a `toron` a test runs is
# under test because the harness SAYS so (`TORON_UNDER_TEST`, set by
# `toron_core::testkit::toron_command`), never because of where the binary
# sits. Under test it never resolves the operator's data root or config home
# from an inherited HOME / XDG_*. This is the 2026-10-24 store-skew
# incident's guard, and it holds under mbx and plain cargo alike.
#
#   scripts/verify-mutations.sh scripts/mutations/data-root-guard.spec.sh --cargo mbx
#
# Every mutation is driven through the REAL binary by `data_root_guard`, on a
# fake operator home that does not exist and cannot be created, so a
# surviving mutant fails on `create_dir_all` and never reaches a real store.
# `spawn_harness` is the lint that every spawn site uses the harness.
# The in-process rules (cfg(test), the home mapping table) are in
# data-root-guard-unit.spec.sh.

CARGO_ARGS=(-p toron-cli --test data_root_guard --test spawn_harness)
TEST_FILTER=(--quiet)

CONFIG=crates/toron-core/src/config/mod.rs
TESTKIT=crates/toron-core/src/testkit.rs
LINT=crates/toron-cli/tests/spawn_harness.rs

mutation "G1  the TORON_UNDER_TEST marker is ignored by the child" \
    file=$CONFIG \
    expect=a_spawned_toron_with_the_operators_home_lands_in_the_harness_sandbox \
    find='if let Some(marker) = marker {' \
    replace='if let Some(marker) = marker.filter(|_| false) {'

mutation "G2  the harness does not set the marker on the child" \
    file=$TESTKIT \
    expect=the_harness_pins_every_root_inside_its_sandbox \
    find='cmd.env(crate::config::UNDER_TEST_ENV, &root)' \
    replace='cmd.env("TORON_UNDER_TEST_X", &root)'

mutation "G3  the config home is not sandboxed under test (operator_nsec)" \
    file=$CONFIG \
    expect=a_spawned_toron_with_the_operators_home_lands_in_the_harness_sandbox \
    find='"XDG_CONFIG_HOME" => home.join(".config"),' \
    replace='"XDG_CONFIG_HOME" => value,'

mutation "G4  the harness does not pin the child's config home" \
    file=$TESTKIT \
    expect=the_harness_pins_every_root_inside_its_sandbox \
    find='.env("XDG_CONFIG_HOME", home.join(".config"))' \
    replace='.env("XDG_CONFIG_HOME_X", home.join(".config"))'

mutation "G5  an ambient XDG_DATA_HOME is honoured under test (the bd-fize order)" \
    file=$CONFIG \
    expect=a_spawned_toron_with_the_operators_home_lands_in_the_harness_sandbox \
    find='Some(root) if sandbox.is_scratch(&root) => root,' \
    replace='Some(root) if sandbox.is_scratch(&root) || env.xdg_data_home.is_some() => root,'

mutation "G6  TORON_DATA_DIR is ignored under test" \
    file=$CONFIG \
    expect=an_explicit_data_dir_pin_still_wins_under_test \
    find='if let Some(dir) = &env.toron_data_dir {' \
    replace='if let Some(dir) = env.toron_data_dir.as_ref().filter(|_| env.sandbox.is_none()) {'

mutation "G7  a fixture home under the temp root is not scratch" \
    file=$CONFIG \
    expect=a_scratch_fixture_home_is_still_honoured_under_test \
    find='(!self.temp_dir.as_os_str().is_empty() && root.starts_with(&self.temp_dir))' \
    replace='false'

mutation "G8  the spawn lint lets Command::new(CARGO_BIN_EXE_*) through" \
    file=$LINT \
    expect=the_checker_catches_each_kind_of_bypass \
    find='                bypass = true;' \
    replace='                bypass = false;'

mutation "G9  the spawn lint lets an unjustified CARGO_BIN_EXE_ mention through" \
    file=$LINT \
    expect=the_checker_catches_each_kind_of_bypass \
    find='            if !justified {' \
    replace='            if false && !justified {'
