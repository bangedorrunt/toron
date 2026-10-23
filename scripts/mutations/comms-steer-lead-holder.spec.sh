# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# F7 (stack review 2026-10-23): the steer fence binds the sender to the lead
# holder, and when no lead@1 row exists only the operator key may steer.
#
#   scripts/verify-mutations.sh scripts/mutations/comms-steer-lead-holder.spec.sh --cargo mbx
#
# Each mutation breaks ONE decision of the fence and the named test in the
# `comms_steer` integration suite must go red: the holder binding, the
# operator-only no-lead rule (both directions), and the fence call itself.
#
# What this does not prove: the CLI exit-3 render of `no_lead` /
# `not_lead_holder` (toron-cli comms_authority.rs) and the pure table test
# (authority::tests), which are covered by their own tests.

CARGO_ARGS=(-p toron-core --test comms_steer)
TEST_FILTER=(--quiet)

AUTH=crates/toron-core/src/authority.rs
INBOX=crates/toron-core/src/inbox.rs

mutation "L1  the holder is not checked (any key at the epoch steers)" \
    file=$AUTH \
    expect=a_non_holder_at_the_current_epoch_is_refused \
    find='        if from != holder {' \
    replace='        if false && from != holder {'

mutation "L2  no lead row accepts a non-operator steer" \
    file=$AUTH \
    expect=with_no_lead_row_only_the_operator_steers \
    find=$'            return Err(Reject::NoLead {\n                context: context.to_string(),\n            });' \
    replace=$'            let _ = context;\n            return Ok(());'

mutation "L4  no lead row refuses the operator too" \
    file=$AUTH \
    expect=with_no_lead_row_only_the_operator_steers \
    find='            if matches!(sender, Standing::Operator) {' \
    replace='            if false && matches!(sender, Standing::Operator) {'

mutation "L3  append does not call the steer fence" \
    file=$INBOX \
    expect=a_non_holder_at_the_current_epoch_is_refused \
    find=$'            Authority::check_steer(\n                lead.as_ref().map(|l| (l.holder.as_str(), l.sup_epoch)),\n                &env.from,\n                standing,\n                steer.sup_epoch,\n                context,\n            )?;' \
    replace=$'            let _ = (&lead, &steer.sup_epoch);'
