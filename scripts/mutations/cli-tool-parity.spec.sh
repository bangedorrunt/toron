# Sourced by scripts/verify-mutations.sh.
# bd-5jeyj: every live MCP tool maps to a CLI command that exists. Dropping a
# table row or pointing one at a missing command must turn the parity test RED.

CARGO_ARGS=(-p toron-cli --test cli_tool_parity)
TEST_FILTER=(every_live_tool_has_a_parity_row every_mapped_cli_command_exists)

mutation "P1  a tool loses its CLI mapping" \
    file=crates/toron-mcp/src/catalog.rs \
    expect=every_live_tool_has_a_parity_row \
    find='    ("ask_judgment", "judgment ask"),' \
    replace='    ("ask_judgment_typo", "judgment ask"),'

mutation "P2  a mapping points at a missing command" \
    file=crates/toron-mcp/src/catalog.rs \
    expect=every_mapped_cli_command_exists \
    find='    ("ask_judgment", "judgment ask"),' \
    replace='    ("ask_judgment", "judgment nope"),'
