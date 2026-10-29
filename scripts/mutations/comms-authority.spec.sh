# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# Comms Phase 4.1: roles and authority at append.
#
# Part 1, the pure table (authority.rs unit tests):
#   scripts/verify-mutations.sh scripts/mutations/comms-authority.spec.sh --cargo mbx
# Part 2 (comms-authority-append.spec.sh) breaks the enforcement in inbox.rs and
# one table row, judged by the `comms_authority` integration suite.
#
# Every mutation breaks ONE decision of redesign §6.1 (the table, the own-task
# rule, `done`, who may issue a role, the standing of an unroled sender) and the
# named test must go red. Green would mean that decision is not covered.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(authority::)

AUTH=crates/toron-core/src/authority.rs

# --- the table ------------------------------------------------------------

mutation "A1  crew may task" \
    file=$AUTH \
    expect=crew_cannot_task_verdict_or_approve \
    find=$'    any(Role::Crew, Class::Reply),\n    only(Role::Crew, Class::Control, "escalate"),' \
    replace=$'    any(Role::Crew, Class::Reply),\n    any(Role::Crew, Class::Task),\n    only(Role::Crew, Class::Control, "escalate"),'

mutation "A2  crew may verdict" \
    file=$AUTH \
    expect=crew_cannot_task_verdict_or_approve \
    find=$'    any(Role::Crew, Class::Reply),\n    only(Role::Crew, Class::Control, "escalate"),' \
    replace=$'    any(Role::Crew, Class::Reply),\n    only(Role::Crew, Class::Control, VERDICT_CONTRACT),\n    only(Role::Crew, Class::Control, "escalate"),'

mutation "A3  crew may issue any control (approval, steer, ...)" \
    file=$AUTH \
    expect=crew_cannot_task_verdict_or_approve \
    find=$'    any(Role::Crew, Class::Reply),\n    only(Role::Crew, Class::Control, "escalate"),' \
    replace=$'    any(Role::Crew, Class::Reply),\n    any(Role::Crew, Class::Control),\n    only(Role::Crew, Class::Control, "escalate"),'

mutation "A4  peer may task" \
    file=$AUTH \
    expect=a_peer_cannot_task_or_control \
    find=$'    any(Role::Peer, Class::Reply),\n    // loop-self' \
    replace=$'    any(Role::Peer, Class::Reply),\n    any(Role::Peer, Class::Task),\n    // loop-self'

mutation "A5  peer may control" \
    file=$AUTH \
    expect=a_peer_cannot_task_or_control \
    find=$'    any(Role::Peer, Class::Reply),\n    // loop-self' \
    replace=$'    any(Role::Peer, Class::Reply),\n    any(Role::Peer, Class::Control),\n    // loop-self'

mutation "A6  supervisor may approve" \
    file=$AUTH \
    expect=a_supervisor_tasks_steers_and_cancels_but_never_approves \
    find=$'    only(Role::Supervisor, Class::Control, ROLE_CONTRACT),\n' \
    replace=$'    only(Role::Supervisor, Class::Control, ROLE_CONTRACT),\n    any(Role::Supervisor, Class::Control),\n'

mutation "A7  verifier may notify" \
    file=$AUTH \
    expect=a_verifier_may_only_verdict_and_never_done \
    find=$'    only(Role::Verifier, Class::Control, VERDICT_CONTRACT),\n];' \
    replace=$'    only(Role::Verifier, Class::Control, VERDICT_CONTRACT),\n    any(Role::Verifier, Class::Notify),\n];'

mutation "A8  verifier may task" \
    file=$AUTH \
    expect=a_verifier_may_only_verdict_and_never_done \
    find=$'    only(Role::Verifier, Class::Control, VERDICT_CONTRACT),\n];' \
    replace=$'    only(Role::Verifier, Class::Control, VERDICT_CONTRACT),\n    any(Role::Verifier, Class::Task),\n];'

mutation "A9  loop_self may approve" \
    file=$AUTH \
    expect=loop_self_tasks_and_ticks_but_cannot_approve \
    find=$'    only(Role::LoopSelf, Class::Control, "tick"),\n' \
    replace=$'    only(Role::LoopSelf, Class::Control, "tick"),\n    any(Role::LoopSelf, Class::Control),\n'

mutation "A10 loop_self cannot self-tick" \
    file=$AUTH \
    expect=loop_self_tasks_and_ticks_but_cannot_approve \
    find=$'    only(Role::LoopSelf, Class::Control, "tick"),\n' \
    replace=$'    // (tick removed)\n'

# --- the own-task rule -----------------------------------------------------

mutation "O1  a supervisor may verdict its own task" \
    file=$AUTH \
    expect=a_supervisor_cannot_verdict_its_own_task_but_can_verdict_anothers \
    find=$'        let own_verdict = relation == Relation::OwnTask\n' \
    replace=$'        let own_verdict = false && relation == Relation::OwnTask\n'

mutation "O2  nobody may verdict a task of another's (the rule over-reaches)" \
    file=$AUTH \
    expect=a_supervisor_cannot_verdict_its_own_task_but_can_verdict_anothers \
    find=$'        let own_verdict = relation == Relation::OwnTask\n' \
    replace=$'        let own_verdict = relation != Relation::OwnTask\n'

# --- done ------------------------------------------------------------------

mutation "D1  a verifier may done" \
    file=$AUTH \
    expect=a_verifier_may_only_verdict_and_never_done \
    find='&[Role::Captain, Role::Supervisor, Role::Crew, Role::LoopSelf];' \
    replace='&[Role::Captain, Role::Supervisor, Role::Crew, Role::LoopSelf, Role::Verifier];'

mutation "D2  a peer may done" \
    file=$AUTH \
    expect=a_verifier_may_only_verdict_and_never_done \
    find='&[Role::Captain, Role::Supervisor, Role::Crew, Role::LoopSelf];' \
    replace='&[Role::Captain, Role::Supervisor, Role::Crew, Role::LoopSelf, Role::Peer];'

# --- who may issue a role --------------------------------------------------

mutation "G1  a supervisor may issue any role" \
    file=$AUTH \
    expect=who_may_issue_which_role \
    find=$'                new.is_leaf()\n                    && match current {' \
    replace=$'                true\n                    && match current {'

mutation "G2  a captain may issue captain" \
    file=$AUTH \
    expect=who_may_issue_which_role \
    find='!matches!(new, Role::Captain) && !matches!(current, Some(Role::Captain))' \
    replace='!matches!(current, Some(Role::Captain))'

mutation "G3  anyone may bootstrap a context" \
    file=$AUTH \
    expect=who_may_issue_which_role \
    find='return matches!(issuer, Standing::Operator) && matches!(new, Role::Captain);' \
    replace='return matches!(new, Role::Captain);'

mutation "G4  a crew may issue roles" \
    file=$AUTH \
    expect=who_may_issue_which_role \
    find='            Standing::Holds(_) | Standing::Unroled => false,' \
    replace='            Standing::Holds(_) | Standing::Unroled => true,'

# --- standing and governed -------------------------------------------------

mutation "S1  an unroled sender passes in a governed context" \
    file=$AUTH \
    expect=an_ungoverned_context_allows_everything_and_a_governed_one_needs_a_role \
    find='return Err(unauthorized(None, class, contract.to_string(), Vec::new()));' \
    replace='return Ok(());'

