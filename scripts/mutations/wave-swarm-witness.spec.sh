# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-fbiza (3): the wave-swarm gate shape, claimed but never settled, must end
# in a journaled typed `result_timeout` failure through the durable driver.
#
#   scripts/verify-mutations.sh scripts/mutations/wave-swarm-witness.spec.sh
#
# What this does not prove: the 43001 publication itself (unpublished-job tests
# in durable.rs) and the graph driver's await path, which has no dispatch-await.

CARGO_ARGS=(-p toron-workflow --test durable_host)
TEST_FILTER=(wave_swarm_gate_result_timeout_is_a_journaled_typed_failure)

DURABLE=crates/toron-workflow/src/engine/durable.rs

mutation "W1  a claimed job never times out" \
    file=$DURABLE \
    expect=wave_swarm_gate_result_timeout_is_a_journaled_typed_failure \
    find=$'                        result_timeout.map(|t| {\n                            format!(\n                                "dispatch await {node}: result_timeout after {t}s with no 43004/43006"\n                            )\n                        }),' \
    replace='                        None,'

mutation "W2  the timeout message stops naming the bound" \
    file=$DURABLE \
    expect=wave_swarm_gate_result_timeout_is_a_journaled_typed_failure \
    find='"dispatch await {node}: result_timeout after {t}s with no 43004/43006"' \
    replace='"dispatch await {node}: timed out after {t}s"'
