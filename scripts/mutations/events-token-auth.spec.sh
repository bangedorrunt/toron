# Sourced by scripts/verify-mutations.sh.
# bd-6bgrd: a revoked token must be refused, and concurrent mints must not
# lose a record to a read-modify-write race.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(missing_unknown_and_revoked_tokens_do_not_resolve concurrent_inserts_both_persist)

mutation "R1  a revoked token still resolves" \
    file=crates/toron-core/src/events/principal.rs \
    expect=missing_unknown_and_revoked_tokens_do_not_resolve \
    find='    if record.revoked {
        return None;
    }' \
    replace='    if false && record.revoked {
        return None;
    }'

mutation "L1  insert/revoke lose the exclusive lock" \
    file=crates/toron-core/src/events/principal.rs \
    expect=concurrent_inserts_both_persist \
    find='        let _lock = exclusive_lock(&self.path)?;
        let mut records = read(&self.path)?;
        if records.iter().any(|r| r.hash == record.hash) {' \
    replace='        let mut records = read(&self.path)?;
        if records.iter().any(|r| r.hash == record.hash) {'
