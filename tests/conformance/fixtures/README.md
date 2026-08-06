# Conformance Fixtures

// governed-by: ADR-0003

First-party golden fixtures for the AM-Rust parity conformance suite.

## License note

These fixtures are **first-party artifacts** derived from:

- The public interface of `Dicklesworthstone/mcp_agent_mail_rust` (README tool
  and resource tables + `#[tool(...)]`/`#[resource(...)]` attribute metadata),
  cataloged in `docs/parity/am-rust-tool-catalog.md` (bd-0qg).
- The rmcp/schemars `list_tools` JSON Schema 2020-12 output for toron-mcp
  (bd-led.7.1+) — the derived schemas ARE the conformance golden source.
- The AM tokenizer contract (`porter unicode61 remove_diacritics 2`,
  `prefix='2 3'`) per ADR-0003.

**No AM-Rust test code is vendored.** AM-Rust is `LicenseRef-MIT-Rider`;
its test suite is not copied, adapted, or imported. Names, argument shapes,
and URIs are interface facts, not creative expression.

## Files

| File | Purpose | Count |
|------|---------|-------|
| `tools.schema.json` | inputSchema for all 38 tools (JSON Schema 2020-12) | 38 |
| `resources.json` | all 25 MCP resource URIs + scope | 25 |
| `tokenizer.json` | FTS5 tokenizer contract + expected token cases | 5 |
| `semantics.json` | filter/importance/thread/date semantic cases | — |
| `samples.json` | representative request/response samples for schema validation | 3 |

## Usage

`bd-77u.2` (conformance harness) and `toron-e2e` SC-1..SC-8 reference these
fixtures. Tool names MUST match `docs/parity/am-rust-tool-catalog.md` verbatim.

## Regeneration

Schema fixtures are derive-and-snapshot: capture `list_tools` from a live
toron-mcp server once `bd-led.7.2` wires the 38 tools, and diff against
`tools.schema.json`. Behavioral fixtures (tokenizer/semantics) are validated at
runtime against the SQLite FTS5 projection.
