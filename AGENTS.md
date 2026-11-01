# toron

Nostr-native agent mailbox. One `toron` binary (daemon + MCP + robot CLI).
Flywheel embeds this; never the reverse. Deps one-way:
**flywheel → toron-workflow → toron-core**. Never reverse.

MCP surface: **41 tools** (38 AM-Rust parity + 3 toron additive:
`release_file_reservations_by_reason`, `list_file_reservations`,
`ask_judgment`) and **25 resources**. The committed catalog
(`docs/catalog/toron-mcp.json`) is the 38-parity subset by contract.
Ops crib: `~/.agents/skills/toron/SKILL.md`. ADRs: `docs/decisions/`.

Orchestration door is flywheel (`arrive` → `briefing --json` → argv). Do not
invent a second selector.

## Structure

```
crates/toron-core/      mailbox, bus, nostr, archive, index, osc7501, backends
crates/toron-workflow/ YAML engine, cron, channels/jobs
crates/toron-mcp/      rmcp server (stdio + HTTP /mcp)
crates/toron-cli/      bin `toron` (+ TUI)
crates/toron-e2e/      SC-* scenarios (feature-gated)
docs/decisions/        ADRs
docs/catalog/          generated surface (toron.dev)
workflows/             product YAML
scripts/verify-gates.sh
```

One root `AGENTS.md` only — no per-crate copies.

## Where to look

| Task | Location |
|------|----------|
| Mail send/inbox/ack | `crates/toron-core/src/mailbox/` (`MailSurface`, `MessageId`) |
| Reservations / lease board | `crates/toron-core/src/mailbox/reservations.rs`; CLI `toron reserve …` |
| Wait / receipts / roles | `crates/toron-core/src/mailbox/{receipts,send_tags,identity}.rs` |
| Gift-wrap / kinds / identity | `crates/toron-core/src/nostr/` |
| Bus / federation | `crates/toron-core/src/bus/` |
| OSC 7501 board | `crates/toron-core/src/osc7501.rs` (`fold` + `FoldPolicy`; shared by hc + rex) |
| Multiplexer backends | `crates/toron-core/src/backend/` (`hc` OpenRPC client; flywheel selects the plane) |
| Workflow engine | `crates/toron-workflow/src/{schema,engine}/` — guide `docs/guides/workflow-engine.mdx` |
| Effect journal / run-state | core index migrations + `RunStateStore`; CLI `toron run-state …` |
| Cron (distinct type) | `crates/toron-workflow/src/cron.rs` |
| MCP tools / resources | `crates/toron-mcp/src/{tools,resources}.rs` |
| TUI (Elm core) | `crates/toron-cli/src/tui/` — pure `tick`; theme `theme.rs` |
| Daemon / CLI | `crates/toron-cli/src/{cli,daemon,main}.rs` |
| Daemon build identity | `crates/toron-core/src/serve_build.rs` + mcp `serve_lock.rs` |

### Crate essentials (folded from former nested AGENTS.md)

- **toron-core:** `Mailbox` concrete, `MailSurface` embedder trait. One SQLite
  write connection, WAL, i64-µs. Gift wraps: no project tags; unwrap verifies
  Schnorr before NIP-44. Presence (20001) ephemeral — never persist. Never
  write the live libgit2 index in `GitArchive::commit`. Force-release =
  operator revocation. Never `{:#?}` a `SecretKey`.
- **toron-workflow:** never imports flywheel. Embedder injects
  `ProfileResolver` / `WorkflowResolver` (default `None` → fail-closed).
  Cron ≠ `trigger: schedule`. DAG `edges:` forbidden — derive from `needs`.
  `Enforced` refuses a live effect with no runner; `Recording` is dry-arm.
- **toron-mcp:** `#[tool]`/`#[resource]` + schemars only. Frozen names unless
  ADR. Blossom at `/.blossom`, never collide `/mcp`. No passphrase/nsec in
  responses. Intentional AM-Rust shape diffs: `docs/parity/known-divergences.md`.

## Proof for a change

A green suite over a dirty tree is not a green tree. Proof =
`git archive HEAD` → build in scratch. `git add` only paths you edited —
never `git add -A`.

## Conventions

- Edition 2024. `#![forbid(unsafe_code)]` on every crate root.
- New/changed ADR-constrained files: `// governed-by: ADR-XXXX` at top.
- Frozen: 41 tool names (38 AM-Rust parity + 3 additive), 25 resources,
  kind registry. No new kinds/tools
  without an ADR. Additive tools need ADR + catalog regen + tests.
- CLI nouns: `channel` `job` `react` `presence` `watch`. Channel posts = kind
  **9**. Flag vocabulary: acting agent is always `--as`; project-scoped
  commands require `--project`.
- `MessageId` is String/hex — never `u64` in shared types. `t` tag = project
  slug only.
- Agent pins: CamelCase Adjective+Noun, enforced
  (`nostr::identity::validate_agent_pin`). One key per pin (ADR-0037).
  Address legacy split pins as `pin@project` or npub until re-keyed.
- Captain alias: `toron contact captain --project <slug> --pin <AdjNoun>`;
  mail `--to captain`.
- Corr authority: sender mints non-empty `corr`; sqlite owns
  `owed→acked→resulted` (monotonic).
- Catalog: `PATH="$PWD/target/debug:$PATH" toron catalog --json` then
  `mbx test catalog_freshness`. Bare `toron` may be a stale install.
- Commit: cite the bead you close (`Bead: <id>`). No live `bd-*` in published
  MCP/resource strings (`catalog_no_internal_ids`).
- Beads: agents use `br --json` / `--robot-*`; bare `br` is the human TUI.
- TDD for new features and bug fixes. No benches in this repo.
- Engineering principles: `flywheel principles list`.

## Effect law

Binding: `~/.agents/skills/effect-oriented/SKILL.md`. A core is
`tick(&Model, &Msg) -> (Model, Vec<Effect>)` — no clock/env/network in the
file. Live effects journal before the provider. Settled defects (do not
re-open): ReleaseIgnored, receipt tombstone, ask_judgment Prepared-before-call,
send_armor half-open probe, MarkDegraded — each has a witness test; see the
effect-law ADR acceptance record.

## Test sandbox (hard)

1. Deny-shims for `launchctl` and `systemctl` first on `PATH`; logs empty after.
2. Temp `HOME` + temp XDG; set explicit `TORON_DATA_DIR` / config dir for
   mailbox tests (mbx builds may lack `CACHEDIR.TAG`).
3. Spare ports only — never live **`:18080`** / **`:18980`**, never the live
   store `~/.local/share/toron/`, never a live hc/herdr session or
   `~/.config/herdr-core`. Never rewrite the operator passphrase/nsec.
4. Never print `operator_nsec` contents.

## Skill sync

CLI/MCP change → `Command` enum + tool attrs + this file +
`~/.agents/skills/toron/SKILL.md` + tool-count constants +
`toron catalog --json` / `catalog_freshness` when the catalog grows.
Wave-1: no new MCP tools; CLI-only nouns still sync steps 1/3/4.

## Anti-patterns

- `asupersync` / `fastmcp` (CI fails). MCP is `rmcp` + Tokio.
- Cross-key NIP-09 delete. Nested `block_on`. Silent-success stubs.
- Per-pid temp mailbox roots. Removing the atomic reservation conflict-check.
- Headless faking pane lifecycle kinds. Treating cron as `trigger: schedule`.
- Editing `docs/decisions/0001-*` as live policy (superseded).
- Reading empty dispatcher / `no live fleet` as an empty project — check inbox,
  `.unanswered`, `/health.injection` first.

## Commands

Prefix cargo with `mbx` (bare `cargo` misparses under the mbx shim).

```bash
bash scripts/verify-gates.sh
toron daemon status --json
mbx test --workspace
mbx test catalog_freshness
PATH="$PWD/target/debug:$PATH" toron catalog --json
mbx test -p toron-e2e --features p1   # SC-1..8
mbx test -p toron-e2e --features p2   # needs resonate on PATH
mbx test -p toron-e2e --features p4   # SC-18..25
mbx clippy --workspace --all-targets -- -D warnings
mbx fmt --check
mbx test --workspace --all-features
```

`mbx test --exclude-ignored` is invalid. Release profile: `strip = "none"`.

## Notes

- Daemon label `com.toron.serve`, HTTP `127.0.0.1:18080`. Passphrase file
  `~/.config/toron/passphrase` (0600). Operator key
  `~/.config/toron/operator_nsec` (0600) — one canonical path.
- `toron serve` requires `--mcp-stdio` or `--mcp-http`.
- Sibling orchestrator: `../flywheel` (default pane plane: **rex** /
  Superlogical; `hc` / herdr-core is the transport fallback). Map:
  `../flywheel/AGENTS.md`. Rex docs: https://www.superlogical.com/rex/docs .
  hc docs: https://herdr.dev/core/
