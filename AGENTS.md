# toron

Nostr-native agent mailbox. 39 MCP tools + 25 resources on Schnorr identity,
NIP-17 gift-wrap, NIP-29 channels. Flywheel embeds this; never the reverse.

**Generated:** 2026-08-13
**Commit:** 33f8276
**Branch:** main

## OVERVIEW

One `toron` binary (daemon + MCP + robot CLI). Workflows run on Resonate
through `toron-workflow`. Peer mail never goes through flywheel.

## STRUCTURE

```
toron/
├── crates/toron-core/       # mailbox, bus, nostr, archive, index
├── crates/toron-workflow/   # YAML engine, cron, spawn/dispatch
├── crates/toron-mcp/        # rmcp server (stdio + HTTP /mcp)
├── crates/toron-cli/        # bin `toron`
├── crates/toron-e2e/        # SC-* scenarios (feature-gated)
├── docs/decisions/          # ADRs (read before structural change)
├── docs/catalog/            # generated surface catalog (toron.dev ADR-0002 D3)
├── workflows/               # product YAML (not GitHub Actions)
└── scripts/verify-gates.sh  # ADR-0003 CI policy
```

Child maps: [toron-core](crates/toron-core/AGENTS.md) ·
[toron-workflow](crates/toron-workflow/AGENTS.md) ·
[toron-mcp](crates/toron-mcp/AGENTS.md)

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Mail send/inbox/ack | `crates/toron-core/src/mailbox/` (`MailSurface`, `MessageId`) |
| Channels / jobs / presence (ADR-0020) | `crates/toron-workflow/src/{channels,jobs}.rs`, `crates/toron-core/src/nostr/presence.rs`, CLI `toron channel\|job\|react\|presence\|watch` |
| Gift-wrap / kinds | `crates/toron-core/src/nostr/` |
| Bus / federation | `crates/toron-core/src/bus/` |
| Git archive | `crates/toron-core/src/archive/git.rs` |
| Workflow YAML / engine | `crates/toron-workflow/src/{schema,engine}/` |
| Cron (distinct type) | `crates/toron-workflow/src/cron.rs` (ADR-0012) |
| MCP tools | `crates/toron-mcp/src/tools/` |
| Catalog surface | `crates/toron-mcp/src/catalog.rs` + `crates/toron-cli/src/catalog.rs` (`docs/catalog/`) |
| CLI / daemon | `crates/toron-cli/src/{cli,daemon,main}.rs` |
| ADRs | `docs/decisions/` |

## CONVENTIONS

- Edition 2024. `#![forbid(unsafe_code)]` on every crate root.
- New/changed ADR-constrained files: `// governed-by: ADR-XXXX` at top.
- Deps one-way: **flywheel → toron-workflow → toron-core**. Never reverse.
- Frozen: 39 tool names (38 AM-Rust parity + 1 toron additive), 25 resources, kind registry. No new kinds/tools without an ADR. ADR-0020 (proposed) adds **CLI** nouns `channel` `job` `react` `presence` `watch` without new MCP tools. Channel posts emit kind **9** (not 42).
- Catalog: `toron catalog --json` regenerates `docs/catalog/toron-mcp.json`; `cargo test catalog_freshness` fails red on surface drift without regen (toron.dev ADR-0002 D3).
- `MessageId` is String/hex. Never `u64` in shared request/response types.
- `t` tag = project slug only. Never harness/model/job tags.
- TDD for new features and bug fixes. No benches in this repo.
- Beads: `br` CLI + `--json`. No beads MCP. Binary is `br`, never `bd`.
- Engineering principles (ADR-0019 Spec 5/6): `flywheel principles list`. Cite the name that changed the decision. Full leaves: `flywheel principles show <name>`.
- Wave-2 playbook lints (ADR-0019): `crates/toron-workflow/src/playbook_lint.rs` — feature requires `data_shape` input, bug-fix requires `repro` predecessor, prototype/arena require `n>=2` + mode rules (`build_slot: true` for compile, no repo build paths for sketch), forensics is read-only, `principles:` names validate against the 21-name registry. Entrypoints: `lint_workflow_with_principles(wf, &[&str])` (embedding passes the registry), `lint_workflow(wf)` (in-crate default).
- `std::env::set_var` only in integration tests that omit `forbid(unsafe_code)`.

## Skill Sync Contract

When you add/remove/rename a CLI command or MCP tool, update ALL of:
1. The `Command` enum (`crates/toron-cli/src/cli.rs`) — ground truth (compiles).
2. The tool `#[tool(name = "...")]` in `crates/toron-mcp/src/tools/` — MCP surface (ADR-0020 wave 1: **no new tools**; CLI-only nouns still need steps 1, 3, 4).
3. This AGENTS.md (STRUCTURE + CONVENTIONS sections).
4. The skill: `~/.agents/skills/toron/SKILL.md` CLI Surface + MCP Parity Surface + [crew-steering](references/crew-steering.md) — single canonical copy; grok and other harnesses read the same file (grok natively scans `~/.agents/skills/`, no mirror).
5. The tool-count constant (AGENTS.md header + SKILL.md frontmatter) — only when an MCP tool actually changes.
6. `toron catalog --json` + `cargo test catalog_freshness` when the CLI catalog grows.

The skill files are the canonical agent-facing reference. A command/tool not in
the skill is invisible to any agent that triggers the skill.

## ANTI-PATTERNS (THIS PROJECT)

- `asupersync` / `fastmcp` (CI fails). MCP is `rmcp` + Tokio.
- Cross-key NIP-09 delete. Force-release = operator `status:revoked`.
- Nested `block_on`. Compat shims. Silent-success stubs.
- `GitArchive::commit` writing the live libgit2 index (reverts source HEAD).
- Per-pid temp mailbox roots. Durable root is XDG (`$XDG_DATA_HOME/toron`).
- Removing the atomic reservation conflict-check.
- Headless faking pane 43001/43004 lifecycle.
- Treating cron as `trigger: schedule` (cron is a distinct YAML type).
- Editing `docs/decisions/0001-*` as live policy (superseded; Tokio is required).

## COMMANDS

```bash
bash scripts/verify-gates.sh
cargo test --workspace
cargo test catalog_freshness                  # catalog drift gate (toron.dev ADR-0002 D3)
toron catalog --json                          # emit 38-tool / 25-resource / CLI catalog
cargo test -p toron-e2e --features p1          # SC-1..8
cargo test -p toron-e2e --features p2          # needs resonate on PATH
cargo test -p toron-e2e --features p4          # SC-18..25 (implies p5)
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
cargo test --workspace --all-features          # honest full matrix; CI build does not
```

`cargo test --exclude-ignored` is invalid. Ignored tests: `-- --ignored`.

Release profile: `strip = "none"` (macOS arm64 LINKEDIT / sqlx_macros).

## NOTES

- Captain alias (bd-captain-alias-0g8a): `captain` is a routable fold onto
  the project's REGISTERED orchestrator pin — set via
  `toron contact captain --project <slug> --pin <adj+noun>`, resolved by
  `toron mail whois <slug> captain` and `mail send --to captain`. Standing
  orders address reports to `captain`, never to the host username.
- `toron serve` requires `--mcp-stdio` or `--mcp-http`. No-flag serve bails.
- Daemon label `com.toron.serve`, HTTP `127.0.0.1:18080`. Passphrase file
  `~/.config/toron/passphrase` (0600), never in plist/env flags.
- Daemon runs at `RUST_LOG=warn` (plist + `daemon.rs` templates). Hourly
  copy-truncate via LaunchAgent `com.toron.logrotate` (1 MiB, also rotates
  `resonate.out.log`). One `toron serve` per data dir (`serve.pid`). INFO
  rmcp churn once wrote a 604 MB serve.out.log; resonate INFO doorbell
  poll-fail once wrote 1.3 GB.
- Bare `cargo test --workspace` compiles e2e and runs 0 p1/p2 tests (feature
  gates). sc16–sc24 currently run un-gated.
- Root `tests/` and `examples/agent_ping_pong.rs` are ADR-0001 leftovers —
  not workspace members.
- Sibling orchestrator: `../flywheel` (herdr). Map: `../flywheel/AGENTS.md`.
- herdr backend client surface (pane primitives, agent_read source semantics,
  agent_not_idle, prompt gates): `~/.agents/skills/toron/SKILL.md` Backend
  section; harness lifecycle matrix (env injection, boot prime, integration
  preflight, alt-screen ladder): `~/.agents/skills/flywheel/SKILL.md` §7.
