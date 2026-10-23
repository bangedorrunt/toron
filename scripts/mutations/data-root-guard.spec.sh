# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# The data-root guard: a dev build (the `toron` a test spawns, which sits in a
# cargo target dir) never resolves an operator's data root from an inherited
# HOME / XDG_DATA_HOME. This is the 2026-10-24 store-skew incident's guard.
#
#   scripts/verify-mutations.sh scripts/mutations/data-root-guard.spec.sh --cargo mbx
#
# Every mutation is driven through the REAL binary by the `data_root_guard`
# integration suite, on a fake operator home that does not exist and cannot be
# created, so a surviving mutant fails on `create_dir_all` and never reaches a
# real store. Each one breaks ONE rule: dev-build detection, the ambient
# XDG_DATA_HOME being refused, the sandbox shared with the spawning test, the
# explicit TORON_DATA_DIR pin, and a scratch fixture home being honoured.
#
# The pure resolution table (`config::tests`) pins the same rules in-process;
# this spec proves them where the incident happened, across a process spawn.

CARGO_ARGS=(-p toron-cli --test data_root_guard)
TEST_FILTER=(--quiet)

CONFIG=crates/toron-core/src/config/mod.rs

mutation "G1  a binary in a target dir is not recognised as a dev build" \
    file=$CONFIG \
    expect=a_spawned_dev_build_with_the_operators_xdg_lands_in_the_tests_sandbox \
    find='.any(|dir| dir.join("CACHEDIR.TAG").is_file())' \
    replace='.any(|dir| dir.join("CACHEDIR.TAG").is_file() && false)'

mutation "G2  an ambient XDG_DATA_HOME is honoured in a sandbox (the bd-fize order)" \
    file=$CONFIG \
    expect=a_spawned_dev_build_with_the_operators_xdg_lands_in_the_tests_sandbox \
    find='Some(root) if sandbox.is_scratch(&root) => root,' \
    replace='Some(root) if sandbox.is_scratch(&root) || env.xdg_data_home.is_some() => root,'

mutation "G3  a dev build keys its sandbox by its own pid, not the spawning test's" \
    file=$CONFIG \
    expect=a_spawned_dev_build_with_the_operators_xdg_lands_in_the_tests_sandbox \
    find='key: parent_pid(),' \
    replace='key: std::process::id(),'

mutation "G4  TORON_DATA_DIR is ignored in a sandbox" \
    file=$CONFIG \
    expect=an_explicit_data_dir_pin_still_wins_for_a_dev_build \
    find='if let Some(dir) = &env.toron_data_dir {' \
    replace='if let Some(dir) = env.toron_data_dir.as_ref().filter(|_| env.sandbox.is_none()) {'

mutation "G5  a fixture home under the temp root is not scratch" \
    file=$CONFIG \
    expect=a_scratch_fixture_home_is_still_honoured_by_a_dev_build \
    find='(!self.temp_dir.as_os_str().is_empty() && root.starts_with(&self.temp_dir))' \
    replace='false'
