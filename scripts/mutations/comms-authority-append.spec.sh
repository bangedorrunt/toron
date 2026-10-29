# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# Comms Phase 4.1: roles and authority at append.
#
# Part 2, enforcement at append (inbox.rs), judged by the `comms_authority`
# integration suite. Part 1 is comms-authority.spec.sh (the pure table).
#
#   scripts/verify-mutations.sh scripts/mutations/comms-authority-append.spec.sh --cargo mbx
#
# Every mutation breaks ONE decision (the enforcement call, the governed rule,
# the operator's standing, the role-fact grant check, the gen fence, `done`) and
# the named test must go red. One table row is repeated here (E3) to show the
# table is also enforced end to end, not only in the pure check.
#
# What this does not prove: the CLI exit-3 path (toron-cli comms_authority.rs)
# renders the same reject and is covered by its own tests; no mutation touches it.

CARGO_ARGS=(-p toron-core --test comms_authority)
TEST_FILTER=(--quiet)

AUTH=crates/toron-core/src/authority.rs
INBOX=crates/toron-core/src/inbox.rs

mutation "D4  done is not enforced at all" \
    file=$INBOX \
    expect=a_verifier_cannot_done_and_a_crew_can \
    find='        self.authority.check_done(standing, governed)?;' \
    replace='        let _ = (standing, governed);'

# --- who may issue a role --------------------------------------------------

mutation "G5  the role-fact grant check is skipped at append" \
    file=$INBOX \
    expect=a_role_fact_from_a_non_operator_without_the_rank_is_refused \
    find=$'            Authority::check_grant(\n                standing,\n                current.as_ref().map(|c| c.role),\n                role.role,\n                governed,\n            )?;' \
    replace='            let _ = (standing, &current, governed);'

mutation "G6  the gen fence admits an equal gen" \
    file=$INBOX \
    expect=a_role_fact_is_fenced_by_gen \
    find='&& role.generation <= cur.generation' \
    replace='&& role.generation < cur.generation'

# --- standing and governed -------------------------------------------------

mutation "S2  a context is never governed" \
    file=$INBOX \
    expect=the_authority_table_is_enforced_at_append \
    find='    Ok(any.is_some())' \
    replace='    Ok(false && any.is_some())'

mutation "S3  the operator is not recognised" \
    file=$INBOX \
    expect=the_first_role_fact_is_the_operators_captain \
    find='        if self.operator.as_deref() == Some(npub) {' \
    replace='        if self.operator.as_deref() != Some(npub) {'

# --- the enforcement call --------------------------------------------------

mutation "E1  append does not call the authority check" \
    file=$INBOX \
    expect=the_authority_table_is_enforced_at_append \
    find='            Some(env) => self.authorize_in(&mut tx, env).await?,' \
    replace='            Some(_) => AppendFacts::none(),'

mutation "E2  a role fact is checked but never projected" \
    file=$INBOX \
    expect=the_first_role_fact_is_the_operators_captain \
    find='        if let (Some(role), Some(env)) = (&facts.role, &envelope) {' \
    replace='        if let (Some(role), Some(env)) = (facts.role.as_ref().filter(|_| false), &envelope) {'


mutation "E3  crew may task (table row, seen through the append)" \
    file=$AUTH \
    expect=the_authority_table_is_enforced_at_append \
    find=$'    any(Role::Crew, Class::Reply),\n    only(Role::Crew, Class::Control, "escalate"),' \
    replace=$'    any(Role::Crew, Class::Reply),\n    any(Role::Crew, Class::Task),\n    only(Role::Crew, Class::Control, "escalate"),'

mutation "E4  a crew may not done (table, seen through `done`)" \
    file=$AUTH \
    expect=a_verifier_cannot_done_and_a_crew_can \
    find='&[Role::Captain, Role::Supervisor, Role::Crew, Role::LoopSelf];' \
    replace='&[Role::Captain, Role::Supervisor, Role::LoopSelf];'
