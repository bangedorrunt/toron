# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-6setu.2 (D8, toron half): a coalesce batch is keyed by (assignee, run,
# node), so two nodes of one run to one assignee publish TWO jobs; and a
# dispatched batch names its run and node on the kind-43001 while a hand
# mention carries neither. N1 merges the two nodes back into one batch; N2
# drops the tags.

CARGO_ARGS=(-p toron-workflow)
TEST_FILTER=(
    two_nodes_of_one_run_flush_as_two_batches
    a_dispatch_job_carries_run_and_node_tags
    a_hand_mention_carries_no_run_or_node_tag
)

COALESCE=crates/toron-workflow/src/coalesce.rs
HOST=crates/toron-workflow/src/engine/host.rs

mutation "N1  the coalesce key drops the run and node" \
    file=$COALESCE \
    expect=two_nodes_of_one_run_flush_as_two_batches \
    find='        let key = CoalesceKey {
            assignee: mention.assignee.clone(),
            run_id: mention.run_id.clone(),
            node_path: mention.node_path.clone(),
        };' \
    replace='        let key = CoalesceKey::assignee_only(mention.assignee.clone());'

mutation "N2  the job stops naming its run and node" \
    file=$HOST \
    expect=a_dispatch_job_carries_run_and_node_tags \
    find='            tags.push(Tag::parse([name, value]).expect("run/node tag parses"));' \
    replace='            let _ = (name, value);'
