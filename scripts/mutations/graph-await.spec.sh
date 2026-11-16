# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-jhz8x.5: a `dispatch_job await: result` GRAPH node must wait for its job's
# terminal. P1 settles on the publish receipt (the hole this bead closed); T1
# never enforces the deadline, so an unclaimed job would wait forever.

CARGO_ARGS=(-p toron-workflow --lib)
TEST_FILTER=(
    graph_dispatch_await
    graph_resume_reissues
)

DURABLE=crates/toron-workflow/src/engine/durable.rs

mutation "P1  a graph dispatch settles on its publish receipt" \
    file=$DURABLE \
    expect=graph_dispatch_await_waits_for_the_job_terminal \
    find='            let dispatch_await = match node_def.as_ref().map(|n| &n.action) {
                Some(Action::DispatchJob {
                    await_: Some(DispatchAwait::Result),
                    no_claim_timeout,
                    result_timeout,
                    ..
                }) => Some((*no_claim_timeout, *result_timeout)),
                _ => None,
            };' \
    replace='            let dispatch_await: Option<(u64, Option<u64>)> = None;'

mutation "T1  an awaited job never times out" \
    file=$DURABLE \
    expect=graph_dispatch_await_times_out_typed \
    find='            for (node, until_ms, error) in job_deadlines {' \
    replace='            for (node, until_ms, error) in job_deadlines.into_iter().take(0) {'

mutation "O1  a claim does not switch the deadline to the result bound" \
    file=$DURABLE \
    expect=graph_dispatch_await_waits_for_the_job_terminal \
    find='            if let Some(NodeState::AwaitingJob {
                claimed,
                result_deadline_ms: slot,
                ..
            }) = next.node_state.get_mut(node)
            {
                *claimed = true;
                *slot = result_deadline_ms;
            }' \
    replace='            if let Some(NodeState::AwaitingJob {
                result_deadline_ms: slot,
                ..
            }) = next.node_state.get_mut(node)
            {
                *slot = result_deadline_ms;
            }'
