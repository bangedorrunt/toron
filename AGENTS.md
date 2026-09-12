# toron

Nostr-native agent mailbox. 40 MCP tools (38 AM-Rust parity + 2 toron additive:
`release_file_reservations_by_reason`, `list_file_reservations`) + 25 resources
on Schnorr identity, NIP-17 gift-wrap, NIP-29 channels. Flywheel embeds this;
never the reverse. The committed catalog (`docs/catalog/toron-mcp.json`) is the
38-parity subset by contract (additive excluded, `catalog_freshness` gated).

**Generated:** 2026-09-04
**Commit:** efee855
**Branch:** main

## OVERVIEW

One `toron` binary (daemon + MCP + robot CLI). Workflows run on Resonate
through `toron-workflow`. Peer mail never goes through flywheel.

Orchestration door is flywheel (ADR-0011): `flywheel arrive` then
`flywheel briefing --json`, then run argv. Do not invent a second selector.

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
| Mail send/inbox/ack | `crates/toron-core/src/mailbox/` (`MailSurface`, `MessageId`) — `mail send` now carries `--corr/--wake/--priority/--audience/--result-path/--verdict` via `extra_tags` (ADR-0027 E2; `--verdict approve|deny` is ADR-0028 E4, kfo6) |
| Channels / jobs / presence (ADR-0020) | `crates/toron-workflow/src/{channels,jobs}.rs`, `crates/toron-core/src/nostr/presence.rs`, CLI `toron channel\|job\|react\|presence\|watch` |
| Wait / receipts / roles (ADR-0027) | `crates/toron-core/src/mailbox/{receipts.rs,send_tags.rs,identity.rs}`, CLI `toron wait --corr` (E9), `toron watch --live` (E7), `toron receipt get\|list\|set` (E2: `get/list` read, `set --state resulted` explicit verb), `toron role pin\|list\|clear` (E4), `toron ack-sla --project` (ack-SLA scan+publish, CLI-only) |
| Gift-wrap / kinds | `crates/toron-core/src/nostr/` |
| Bus / federation | `crates/toron-core/src/bus/` |
| Git archive | `crates/toron-core/src/archive/git.rs` |
| Workflow YAML / engine | `crates/toron-workflow/src/{schema,engine}/` — authoring/operator guide: `docs/guides/workflow-engine.mdx`. Design target is ADR-0032 (proposed): mailbox receipts stay mailbox receipts; the workflow-effect journal is proposed, not shipped |
| Workflow honesty (bd-i6kc, bd-o1hl) | `crates/toron-workflow/src/engine/boundary.rs` (exhaustive partition: `react`/`call_webhook` fail closed), `crates/toron-mcp/src/workflow_host.rs` (compat runner), `crates/toron-workflow/src/engine/control_flow.rs` (guard/retry/graph/composite parity) |
| Cron (distinct type) | `crates/toron-workflow/src/cron.rs`  |
| MCP tools | `crates/toron-mcp/src/tools/` |
| Catalog surface | `crates/toron-mcp/src/catalog.rs` + `crates/toron-cli/src/catalog.rs` (`docs/catalog/`) |
| CLI / daemon | `crates/toron-cli/src/{cli,daemon,main}.rs` (`agent bootstrap`: register/upsert + reserve in one call) |
| ADRs | `docs/decisions/` |

## CONVENTIONS

- Edition 2024. `#![forbid(unsafe_code)]` on every crate root.
- New/changed ADR-constrained files: `// governed-by: ADR-XXXX` at top.
- Deps one-way: **flywheel → toron-workflow → toron-core**. Never reverse.
- Frozen: 40 tool names (38 AM-Rust parity + 2 toron additive: `release_file_reservations_by_reason` bd-h3oj, `list_file_reservations` bd-locks-list-daemon-xetz), 25 resources, kind registry. No new kinds/tools without an ADR. 
- ADR-0020 adds **CLI** nouns `channel` `job` `react` `presence` `watch`. Channel posts emit kind **9** (not 42). ADR-0023: channel create/close emit **39000**, join/leave emit **39002**; 40/41/42 ingest only.
- rust-nostr **0.45**: `nostr-sdk` 0.45.2 + `nostr` 0.45.3 (`nip44`/`nip49`/`nip59`/`os-rng`). Protocol types via `crate::nostr` / `nostr::prelude`. Sign with `FinalizeEvent::finalize`. `gift_wrap` is sync (`GiftWrapBuilder`).
- Catalog: `toron catalog --json` regenerates `docs/catalog/toron-mcp.json`; `mbx test catalog_freshness` fails red on surface drift without regen (toron.dev ADR-0002 D3).
- Flag vocabulary (bd-flag-vocab-conformance-0hyv): the acting agent is ALWAYS `--as` (`--from`/`--agent` banned, gated by `mbx test -p toron-cli --bin toron flag_conformance`); every project-scoped subcommand REQUIRES `--project`. Channel lifecycle, verify-after-send (`--wait-ack`), delivery visibility (`read --author`), and the inbox cursor (`--since last`, migration 0007) are toron-skill surface; this repo owns their CLI parsing and bus behavior.
- ADR-0027 one-workspace tick (accepted 2026-09-01): tag vocabulary rides `extra_tags`, receipts live in `receipts` sqlite, `wait --corr` is subscribe-until-receipt, `role pin/list/clear` binds via 30013 tags. CLI-only nouns per the Skill Sync Contract below; `catalog_freshness` stays green. Semantics live in the toron skill; this repo owns `crates/toron-core/src/mailbox/{receipts.rs,send_tags.rs,identity.rs}`.
- Flywheel Tick Law (live): doorbell dispatcher and `swarm_loop` call `workspace::tick`; slice reducers stay private. Kind-9 is the ring; assignment enters from rumor 14 / FetchInbox→InboxRows.
- ADR-0028 E3 corr authority (accepted 2026-09-03): sender mints non-empty `corr` (`SendTags::parse` rejects empty; `Corr` vs `ThreadId` newtypes, `thread_id` is never `corr` — conflation fails loud in `send_message`); toron sqlite owns `owed→acked→resulted` (monotonic upsert: never downgrades `resulted`, never drags `acked` to `owed`; `owed` on send, `owed→acked` in `send_receipt` idempotent, `resulted` only via explicit `receipt set`).
- `MessageId` is String/hex. Never `u64` in shared request/response types.
- `t` tag = project slug only. Never harness/model/job tags.
- TDD for new features and bug fixes. No benches in this repo.
- Beads: bare `br` is the TUI (ratatui, since 2026-08-24) — humans run `br`, agents run `br --robot-*` with `--format json|toon` (never bare `br` in automation); `bv` is deprecated.
- TUI theme (hacker-night, tui-design skill): single source is `toron-cli/src/theme.rs` (`HackerNight`); widget code uses semantic slots only, never hex/ANSI. `toron tui` lives in `crates/toron-cli/src/tui.rs`. Skill §-level layout/interaction rules live in the tui-design skill; sibling palettes mirror in `flywheel/src/output.rs` and `beads/src/tui/theme.rs`.
- Engineering principles (ADR-0019 Spec 5/6): `flywheel principles list`. Cite the name that changed the decision. Full leaves: `flywheel principles show <name>`.
- Wave-2 playbook lints (ADR-0019): `crates/toron-workflow/src/playbook_lint.rs` — feature requires `data_shape` input, bug-fix requires `repro` predecessor, prototype/arena require `n>=2` + mode rules (`build_slot: true` for compile, no repo build paths for sketch), forensics is read-only, `principles:` names validate against the 21-name registry. Entrypoints: `lint_workflow_with_principles(wf, &[&str])` (embedding passes the registry), `lint_workflow(wf)` (in-crate default).
- `std::env::set_var` only in integration tests that omit `forbid(unsafe_code)`.
- RUST PRACTICES (distilled 2026-09-10, ADR-0031 accepted 2026-09-10): decision cores are pure — one Model, one Msg union, `tick(&Model, &Msg) -> (Model, Vec<Effect>)`, no I/O across the API, wall-clock enters only as Msg payloads, effects are journaled before execution. Guards are named pure predicates, never inline boolean soup; actions carry data, never closures; platform code lives in `platform/<os>.rs`, never `#[cfg(target_os)]` in core. Full checklist (multiplicative paths, frozen contracts, vendored-dep ledger, test constructors, VERIFY honesty) lives in the cited ADRs and applies to new decision-core code.

## Skill Sync Contract

When you add/remove/rename a CLI command or MCP tool, update ALL of:
1. The `Command` enum (`crates/toron-cli/src/cli.rs`) — ground truth (compiles).
2. The tool `#[tool(name = "...")]` in `crates/toron-mcp/src/tools/` — MCP surface (ADR-0020 wave 1: **no new tools**; CLI-only nouns still need steps 1, 3, 4).
3. This AGENTS.md (STRUCTURE + CONVENTIONS sections).
4. The skill: `~/.agents/skills/toron/SKILL.md` CLI Surface + Mail tool REQUIRED args + [crew-steering](references/crew-steering.md) — single canonical copy; grok and other harnesses read the same file (grok natively scans `~/.agents/skills/`, no mirror).
5. The tool-count constant (AGENTS.md header + SKILL.md frontmatter): 40 (38 parity + 2 additive). Only changes when an MCP tool actually changes.
6. `toron catalog --json` + `mbx test catalog_freshness` when the CLI catalog grows.

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
Rust runs through mbx (cargo cache): prefix every cargo invocation with `mbx` (bare `cargo` misparses under the mbx shim).

```bash
bash scripts/verify-gates.sh
mbx test --workspace
mbx test catalog_freshness                   # catalog drift gate (toron.dev ADR-0002 D3)
toron catalog --json                         # emit 38-parity-tool catalog (40 served incl. additive) / 25-resource / CLI catalog
mbx test -p toron-e2e --features p1          # SC-1..8
mbx test -p toron-e2e --features p2          # needs resonate on PATH
mbx test -p toron-e2e --features p4          # SC-18..25 (implies p5)
mbx clippy --workspace --all-targets -- -D warnings
mbx fmt --check
mbx test --workspace --all-features          # honest full matrix; CI build does not
```

`mbx test --exclude-ignored` is invalid. Ignored tests: `-- --ignored`.

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
- `mbx test --workspace` compiles e2e and runs 0 p1/p2 tests (feature
  gates). sc16–sc24 currently run un-gated.
- Root `tests/` and `examples/agent_ping_pong.rs` are ADR-0001 leftovers —
  not workspace members.
- Sibling orchestrator: `../flywheel` (herdr). Map: `../flywheel/AGENTS.md`.
- herdr backend client surface (pane primitives, agent_read source semantics,
  agent_not_idle, prompt gates): `~/.agents/skills/toron/SKILL.md` Backend
  section; harness lifecycle matrix (env injection, boot prime, integration
  preflight, alt-screen ladder): `~/.agents/skills/flywheel/SKILL.md` §7.
