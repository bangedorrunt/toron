# Sourced by scripts/verify-mutations.sh.
# bd-nmilo: the repo root must come from the registered project path. Falling
# back to Unknown makes every lease look dirty-by-default; witness must RED.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(registered_repo_root_drops_a_clean_path_past_grace)

mutation "RR1  ignore the registered repo root" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    expect=registered_repo_root_drops_a_clean_path_past_grace \
    find='groups.entry(m.7.clone()).or_default().push(i);' \
    replace='groups.entry(None).or_default().push(i);'
