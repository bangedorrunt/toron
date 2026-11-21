# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# bd-3pbzc: a side agent (com.toron.logrotate) must never take the primary
# mail daemon down. The generated plist has to be valid XML, the side agent
# must be installed AFTER serve is bootstrapped, and its failure must be a
# warning rather than an error out of install_supervised.
#
#   scripts/verify-mutations.sh scripts/mutations/bd-3pbzc-logrotate.spec.sh --cargo mbx

CARGO_ARGS=(-p toron-cli --bin toron)
TEST_FILTER=(logrotate_)

TARGET=crates/toron-cli/src/platform/macos.rs

mutation "E1  the shell script is not XML-escaped" \
    file=$TARGET \
    expect=logrotate_plist_is_valid_xml \
    find="    let script = script.replace('&', \"&amp;\");" \
    replace="    let script = script.replace('&', \"&\");"

mutation "E2  a logrotate failure aborts the whole install" \
    file=$TARGET \
    expect=logrotate_failure_cannot_take_serve_down \
    find='        eprintln!("warning: com.toron.logrotate install failed (serve is already running): {e}");' \
    replace='        return Err(e);'

mutation "E3  the side agent is installed before serve is bootstrapped" \
    file=$TARGET \
    expect=logrotate_failure_cannot_take_serve_down \
    find='    let _ = out;
    let out = launchctl_cmd()' \
    replace='    let _ = out;
    let _ = install_logrotate_agent(&dir, &uid);
    let out = launchctl_cmd()'
