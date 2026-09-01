# toron

Nostr-native agent mailbox. 40 MCP tools (38 AM-Rust parity + 2 toron additive:
`release_file_reservations_by_reason`, `list_file_reservations`) + 25 resources
on Schnorr identity, NIP-17 gift-wrap, NIP-29 channels. Flywheel embeds this;
never the reverse. The committed catalog (`docs/catalog/toron-mcp.json`) is the
38-parity subset by contract (additive excluded, `catalog_freshness` gated).

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
| Mail send/inbox/ack | `crates/toron-core/src/mailbox/` (`MailSurface`, `MessageId`) — `mail send` now carries `--corr/--wake/--priority/--audience/--result-path` via `extra_tags` (ADR-0027 E2) |
| Channels / jobs / presence (ADR-0020) | `crates/toron-workflow/src/{channels,jobs}.rs`, `crates/toron-core/src/nostr/presence.rs`, CLI `toron channel\|job\|react\|presence\|watch` |
| Wait / receipts / roles (ADR-0027) | `crates/toron-core/src/mailbox/{receipts.rs,send_tags.rs,identity.rs}`, CLI `toron wait --corr` (E9), `toron watch --live` (E7), `toron receipt get\|list` (E2), `toron role pin\|list\|clear` (E4) |
| Gift-wrap / kinds | `crates/toron-core/src/nostr/` |
| Bus / federation | `crates/toron-core/src/bus/` |
| Git archive | `crates/toron-core/src/archive/git.rs` |
| Workflow YAML / engine | `crates/toron-workflow/src/{schema,engine}/` |
| Cron (distinct type) | `crates/toron-workflow/src/cron.rs`  |
| MCP tools | `crates/toron-mcp/src/tools/` |
| Catalog surface | `crates/toron-mcp/src/catalog.rs` + `crates/toron-cli/src/catalog.rs` (`docs/catalog/`) |
| CLI / daemon | `crates/toron-cli/src/{cli,daemon,main}.rs` |
| ADRs | `docs/decisions/` |

## CONVENTIONS

- Edition 2024. `#![forbid(unsafe_code)]` on every crate root.
- New/changed ADR-constrained files: `// governed-by: ADR-XXXX` at top.
- Deps one-way: **flywheel → toron-workflow → toron-core**. Never reverse.
- Frozen: 40 tool names (38 AM-Rust parity + 2 toron additive: `release_file_reservations_by_reason` bd-h3oj, `list_file_reservations` bd-locks-list-daemon-xetz), 25 resources, kind registry. No new kinds/tools without an ADR. 
- ADR-0020 adds **CLI** nouns `channel` `job` `react` `presence` `watch`. Channel posts emit kind **9** (not 42). ADR-0023: channel create/close emit **39000**, join/leave emit **39002**; 40/41/42 ingest only.
- rust-nostr **0.45**: `nostr-sdk` 0.45.2 + `nostr` 0.45.3 (`nip44`/`nip49`/`nip59`/`os-rng`). Protocol types via `crate::nostr` / `nostr::prelude`. Sign with `FinalizeEvent::finalize`. `gift_wrap` is sync (`GiftWrapBuilder`).
- Catalog: `toron catalog --json` regenerates `docs/catalog/toron-mcp.json`; `cargo test catalog_freshness` fails red on surface drift without regen (toron.dev ADR-0002 D3).
- Flag vocabulary (bd-flag-vocab-conformance-0hyv): the acting agent is ALWAYS `--as` — `--from`/`--agent` are banned (enforced by `cargo test -p toron-cli --bin toron flag_conformance`) — and every project-scoped subcommand REQUIRES `--project` (no silent demo default; missing-key errors name the resolved project). Channel lifecycle (bd-channel-close-purge-bsqb): `channel close` = creator-only tombstone (closed refuses posts/joins, hides from `list`, creator may leave) → `channel purge` = creator/operator removal of a closed channel's bus events, audited to `{data_dir}/channel_purge.log`. Verify-after-send: `channel post --wait-ack <secs>` (bd-verify-after-send-fxtv). Delivery visibility: `channel read --author <pin>` (bd-delivery-visibility-g96y). Inbox cursor: `mail inbox --since last` persists per-(project, agent, plane) daemon-side (bd-since-cursor-70cf, migration 0007).
- ADR-0027 one-workspace tick (accepted 2026-09-01): `MailSurface` tag vocabulary `--corr/--wake/--priority/--audience/--result-path` rides `extra_tags` (rumor, never 1059); receipts durably in `receipts` sqlite (`receipt get/list`; owed-on-send producer lives in `send_message` — acked/resulted transitions are the bd-0027 remainder); `wait --corr` is subscribe-until-receipt (same `Subscribe` as `watch --live`); `role pin/list/clear` binds `wheelhouse` etc. via 30013 `role=` tags + `whois` alias; `watch --live` is `subscribe_trusted` (not 1s snapshot); no new MCP tools (CLI-only nouns per Skill Sync Contract, `catalog_freshness` must stay green).
- `MessageId` is String/hex. Never `u64` in shared request/response types.
- `t` tag = project slug only. Never harness/model/job tags.
- TDD for new features and bug fixes. No benches in this repo.
- Beads: bare `br` is the TUI (ratatui, since 2026-08-24) — humans run `br`, agents run `br --robot-*` with `--format json|toon` (never bare `br` in automation); `bv` is deprecated.
- TUI theme (hacker-night, tui-design skill): Ghostty v2 hacker-night (`#08080e` void / `#c8d0e8` fg / `#0db9d7` cyan / `#bb9af7` lav / `#73daca` mint / `#e0af68` amber / `#f7768e` red / `#89ddff` ice / `#1a1a3e` selection) is the primary theme. `beads/src/tui/theme.rs`, `toron-cli/src/theme.rs` (`HackerNight`), `flywheel/src/output.rs` (Slot: truecolor→256→16 degrade) are single sources — widget code uses semantic slots only. `toron tui` (`crates/toron-cli/src/tui.rs`, 80×12 gate, `?` help, vim `j/k g/G`) and `flywheel` viewer (`view`/`watch`/`status` + `steer_brief.rs`) follow the same palette; `br` TUI additionally lives in `beads/src/tui/{mod,app,ui,keys,theme}.rs` (split at 100 cols, board stack at 80). Use `tui-design` skill for layout (§1), responsive (§2), interaction (§3), visual hierarchy (§4) and anti-patterns (§7). Binary is `br`, never `bd`.
- Engineering principles (ADR-0019 Spec 5/6): `flywheel principles list`. Cite the name that changed the decision. Full leaves: `flywheel principles show <name>`.
- Wave-2 playbook lints (ADR-0019): `crates/toron-workflow/src/playbook_lint.rs` — feature requires `data_shape` input, bug-fix requires `repro` predecessor, prototype/arena require `n>=2` + mode rules (`build_slot: true` for compile, no repo build paths for sketch), forensics is read-only, `principles:` names validate against the 21-name registry. Entrypoints: `lint_workflow_with_principles(wf, &[&str])` (embedding passes the registry), `lint_workflow(wf)` (in-crate default).
- `std::env::set_var` only in integration tests that omit `forbid(unsafe_code)`.

## Skill Sync Contract

When you add/remove/rename a CLI command or MCP tool, update ALL of:
1. The `Command` enum (`crates/toron-cli/src/cli.rs`) — ground truth (compiles).
2. The tool `#[tool(name = "...")]` in `crates/toron-mcp/src/tools/` — MCP surface (ADR-0020 wave 1: **no new tools**; CLI-only nouns still need steps 1, 3, 4).
3. This AGENTS.md (STRUCTURE + CONVENTIONS sections).
4. The skill: `~/.agents/skills/toron/SKILL.md` CLI Surface + MCP Parity Surface + [crew-steering](references/crew-steering.md) — single canonical copy; grok and other harnesses read the same file (grok natively scans `~/.agents/skills/`, no mirror).
5. The tool-count constant (AGENTS.md header + SKILL.md frontmatter): 40 (38 parity + 2 additive). Only changes when an MCP tool actually changes.
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
toron catalog --json                          # emit 38-parity-tool catalog (40 served incl. additive) / 25-resource / CLI catalog
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

- Captain alias (bd-captain-alias-0g8a, bd-auqj): `captain` is a routable fold onto
  the project's REGISTERED orchestrator pin — set via
  `toron contact captain --project <slug> --pin <adj+noun>`, cleared via
  `toron contact captain --project <slug> --clear` (bd-auqj, parent death),
  resolved by `toron mail whois <slug> captain` and `mail send --to captain`. Standing
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
