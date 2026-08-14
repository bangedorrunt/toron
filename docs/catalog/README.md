# docs/catalog/ — generated surface catalog (governed-by: toron.dev ADR-0002)

`toron-mcp.json` in this directory is **generated** — never hand-edit it.
It is the toron.dev catalog contract (ADR-0002 D3): all 38 AM-Rust parity
MCP tools (name, group, description, input/output JSON schemas verbatim from
the `#[tool]` definitions), the 25 resources, and the CLI `Command` enum.

## Regenerate

```bash
cargo run -p toron-cli --bin toron -- catalog --json --output docs/catalog/toron-mcp.json
```

## Freshness gate

```bash
cargo test catalog_freshness
```

A surface change (tool / arg / schema / resource / CLI command) without a
catalog regen fails red — the change is unmergeable until regenerated and
committed (the `cargo fmt --check` pattern, ADR-0002 D3.2).

## Contract notes

- Exactly 38 tools (frozen names, toron ADR-0007 C3). The bd-h3oj additive
  `release_file_reservations_by_reason` is excluded by contract.
- Group names + order: Identity · Messaging · Contacts · File-reservations ·
  Search · Macros · Product-bus · Build-slots · Infrastructure (9 groups).
- Tools extracted via `ToolServer::tool_router().list_all()` (rmcp), so
  schemas are the live wire schemas — never a second copy.
- Resources from `crates/toron-mcp/src/resources.rs` (25).
- CLI commands introspected from clap (`Cli::command()`).
