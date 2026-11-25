# Sourced by scripts/verify-mutations.sh.
# bd-tqg1: the skew probe names the real close method (rex api_revision 20: session.destroy).

CARGO_ARGS=(-p toron-plane --lib)
TEST_FILTER=(skew_probe_accepts_the_session_destroy_rename)

mutation "S1  probe demands only the retired session.kill" \
    file=crates/toron-plane/src/backend/rex_cli.rs \
    expect=skew_probe_accepts_the_session_destroy_rename \
    find='    &["session.kill", "session.destroy"],' \
    replace='    &["session.kill"],'

mutation "S2  probe never reports a missing close method" \
    file=crates/toron-plane/src/backend/rex_cli.rs \
    expect=skew_probe_accepts_the_session_destroy_rename \
    find='    &["session.kill", "session.destroy"],' \
    replace='    &["session.list"],'
