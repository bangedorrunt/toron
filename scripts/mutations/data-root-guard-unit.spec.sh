# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# The data-root guard, in-process (`config::tests`): cfg(test) / a test
# binary is under test with no marker at all; a marker that is not scratch is
# not trusted as a root; and every home-shaped variable (HOME, XDG_STATE_HOME
# for wake.sock and the passphrase files, XDG_CONFIG_HOME for operator_nsec)
# maps into the sandboxed home.
#
#   scripts/verify-mutations.sh scripts/mutations/data-root-guard-unit.spec.sh --cargo mbx

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(config:: --quiet)

CONFIG=crates/toron-core/src/config/mod.rs

mutation "U1  cfg(test) / a test binary is not under test" \
    file=$CONFIG \
    expect=this_test_binarys_config_and_state_paths_are_sandboxed \
    find='cfg!(test) || is_test_process(),' \
    replace='false,'

mutation "U2  a marker naming a real home is trusted as the sandbox root" \
    file=$CONFIG \
    expect=the_marker_decides_under_test_and_names_the_sandbox \
    find='if marker.is_absolute() && sandbox.is_scratch(marker) {' \
    replace='if marker.is_absolute() {'

mutation "U3  HOME is not sandboxed under test" \
    file=$CONFIG \
    expect=home_shaped_variables_map_into_the_sandboxed_home_under_test \
    find='        _ => home,' \
    replace='        _ => value,'

mutation "U4  XDG_STATE_HOME (wake.sock, passphrase) is not sandboxed under test" \
    file=$CONFIG \
    expect=home_shaped_variables_map_into_the_sandboxed_home_under_test \
    find='"XDG_STATE_HOME" => home.join(".local").join("state"),' \
    replace='"XDG_STATE_HOME" => value,'

