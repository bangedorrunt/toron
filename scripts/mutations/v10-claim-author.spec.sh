# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-6setu.6 (V10 at the claim): a review job refuses a claimant that authored
# one of the nodes it reads IN THE SAME RUN. B1 drops the refusal entirely; A1
# drops the run scope, so a different run's author is refused too.

CARGO_ARGS=(-p toron-workflow --lib)
TEST_FILTER=(a_review_job_refuses_its_own_author_within_the_run)

JOBS=crates/toron-workflow/src/jobs.rs

mutation "B1  the claim never refuses an author" \
    file=$JOBS \
    expect=a_review_job_refuses_its_own_author_within_the_run \
    find='            (r == &run && exclude.iter().any(|x| x == node)).then(|| (node.clone(), r.clone()))' \
    replace='            (false && exclude.iter().any(|x| x == node)).then(|| (node.clone(), r.clone()))'

mutation "A1  the claim ignores the run scope" \
    file=$JOBS \
    expect=a_review_job_refuses_its_own_author_within_the_run \
    find='            (r == &run && exclude.iter().any(|x| x == node)).then(|| (node.clone(), r.clone()))' \
    replace='            (exclude.iter().any(|x| x == node)).then(|| (node.clone(), r.clone()))'
