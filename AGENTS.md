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

Orchestration door is flywheel: `flywheel arrive` then
`flywheel briefing --json`, then run argv. Do not invent a second selector.

## STRUCTURE

```
toron/
├── crates/toron-core/ # mailbox, bus, nostr, archive, index
├── crates/toron-workflow/ # YAML engine, cron, spawn/dispatch
├── crates/toron-mcp/ # rmcp server (stdio + HTTP /mcp)
├── crates/toron-cli/ # bin `toron`
├── crates/toron-e2e/ # SC-* scenarios (feature-gated)
├── docs/decisions/ # ADRs (read before structural change)
├── docs/catalog/ # generated surface catalog (toron.dev)
├── workflows/ # product YAML (not GitHub Actions)
└── scripts/verify-gates.sh # CI policy
```

Child maps: [toron-core](crates/toron-core/AGENTS.md) ·
[toron-workflow](crates/toron-workflow/AGENTS.md) ·
[toron-mcp](crates/toron-mcp/AGENTS.md)

## WHERE TO LOOK

| Task | Location |
|------|----------|
| Mail send/inbox/ack | `crates/toron-core/src/mailbox/` (`MailSurface`, `MessageId`) — `mail send` now carries `--corr/--wake/--priority/--audience/--result-path/--verdict` via `extra_tags` (`--verdict approve|deny` closes one human decision) |
| Channels / jobs / presence | `crates/toron-workflow/src/{channels,jobs}.rs`, `crates/toron-core/src/nostr/presence.rs`, CLI `toron channel\|job\|react\|presence\|watch` |
| File reservations / lease board | `crates/toron-core/src/mailbox/reservations.rs` (`list_file_reservations` is the one board filter both surfaces call); CLI `toron reserve acquire\|conflicts\|list\|renew\|release\|release-by-reason\|force-release\|request-release\|respond\|transfer`. `reserve list` shows the state the strict guard evaluates; `reserve release-by-reason <bead>` is the CLI door for release-on-bead-close (MCP `release_file_reservations_by_reason`, operator-signed) |
| Wait / receipts / roles | `crates/toron-core/src/mailbox/{receipts.rs,send_tags.rs,identity.rs}`, CLI `toron wait --corr`, `toron watch --live`, `toron receipt get\|list\|set` (`get/list` read, `set --state resulted` explicit verb), `toron role pin\|list\|clear`, `toron ack-sla --project` (ack-SLA scan+publish, CLI-only) |
| Gift-wrap / kinds | `crates/toron-core/src/nostr/` |
| Bus / federation | `crates/toron-core/src/bus/` |
| Git archive | `crates/toron-core/src/archive/git.rs` |
| Workflow YAML / engine | `crates/toron-workflow/src/{schema,engine}/` — authoring/operator guide: `docs/guides/workflow-engine.mdx`. Mailbox receipts stay mailbox receipts. The workflow-effect journal ships (toron-core migration `0010`, `EffectJournalMode`, `RunJournal` run lifecycle) |
| Workflow honesty | `crates/toron-workflow/src/engine/boundary.rs` (exhaustive partition: `react`/`call_webhook` fail closed), `crates/toron-mcp/src/workflow_host.rs` (compat runner), `crates/toron-workflow/src/engine/control_flow.rs` (guard/retry/graph/composite parity) |
| Run-state checkpoints | `crates/toron-core/src/index/run_state.rs` (migration `0013`, `RunStateStore`) + `toron run-state replay\|fork` (`crates/toron-cli/src/main.rs`). The WRITE path is the host's (opt-in, `FLYWHEEL_RUN_STATE_LOG`); the READ side here is `replay` (EXACT — a miss names the boundary, never falls back like `load_for_resume` does), `plan_fork` (pure, DRY by default; `ForkIntent::Perform` needs `AcknowledgeDispatchedEffects`), and `RunStateReplay::install` which ALWAYS refuses — seeding a live model from the log is the live host's authority call, so a reader with an installer would be a second seeding path. Contracts: `tests/run_state_conformance.rs` (store), `tests/run_state_read_side.rs` (read), `tests/run_state_mutation.rs` (spec S9.3 mutation check) |
| Cron (distinct type) | `crates/toron-workflow/src/cron.rs` |
| MCP tools | `crates/toron-mcp/src/tools/` |
| Catalog surface | `crates/toron-mcp/src/catalog.rs` + `crates/toron-cli/src/catalog.rs` (`docs/catalog/`) |
| TUI / operator console | `crates/toron-cli/src/tui/` — `model.rs`/`msg.rs`/`update.rs`/`keymap.rs` are the pure Elm core (ADR-0031 §7: `tick(&Model, Msg) -> (Model, Vec<Effect>)` is the only reducer); `host.rs` is the effect edge (live mailbox + fake); `view/` holds the tab renders; `tests.rs` holds determinism + interpreter proofs. `toron tui` reads the same one store the plane commands read (`plane_project`/`plane_agent` env ladders, bd-d41q refusal on a remote daemon) |
| CLI / daemon | `crates/toron-cli/src/{cli,daemon,main}.rs` (`agent bootstrap`: register/upsert + reserve in one call; `agent prune`: absorb the folded-slug leftovers `doctor` names) |
| ADRs | `docs/decisions/` |

## CONVENTIONS

- Edition 2024. `#![forbid(unsafe_code)]` on every crate root.
- New/changed ADR-constrained files: `// governed-by: ADR-XXXX` at top.
- Deps one-way: **flywheel → toron-workflow → toron-core**. Never reverse.
- Frozen: 40 tool names (38 AM-Rust parity + 2 toron additive: `release_file_reservations_by_reason`, `list_file_reservations`), 25 resources, kind registry. No new kinds/tools without an ADR.
- Additive discipline (proposed): additive tools/kinds allowed with ADR + catalog regen + tests + versioning; renumber and parity drift stay forbidden. 
- CLI nouns: `channel` `job` `react` `presence` `watch`. Channel posts emit kind **9** (not 42). Channel create/close emit **39000**, join/leave emit **39002**; 40/41/42 ingest only.
- Lease board + release-on-bead-close are CLI-reachable: `toron reserve list --project <p> [--holder <pin> --path <glob>]` and `toron reserve release-by-reason <bead> --project <p> --as <operator>`. No new MCP tool — both wrap existing operators (`list_file_reservations` / `release_file_reservations_by_reason`); `release-by-reason` is operator-signed and fails loud without an operator key.
- Folded-slug cleanup is CLI-reachable: `toron agent prune --project <p> [--as <pin>]` (preview with the global `-n/--dry-run`) absorbs the `legacy_slug_identities` leftovers `doctor` names — the standalone `agents` row goes away once the canonical pin is registered; a row whose target is unregistered is KEPT and reported. No new MCP tool: CLI-only noun (daemon arm sweeps the shared durable root directly, like `ack-sla`).
- rust-nostr **0.45**: `nostr-sdk` 0.45.2 + `nostr` 0.45.3 (`nip44`/`nip49`/`nip59`/`os-rng`). Protocol types via `crate::nostr` / `nostr::prelude`. Sign with `FinalizeEvent::finalize`. `gift_wrap` is sync (`GiftWrapBuilder`).
- Catalog: `toron catalog --json` regenerates `docs/catalog/toron-mcp.json`; `mbx test catalog_freshness` fails red on surface drift without regen (toron.dev). **Run it as `PATH="$PWD/target/debug:$PATH" toron catalog --json`** — a bare `toron` resolves to whatever is first on PATH, and a stale installed binary silently regenerates a stale catalog (measured 2026-09-29, cost a confusing regen).
- Commit guard, projected paths (`crates/toron-core/src/mailbox/guard.rs`): the bead-hunk rule charges every `bd-*` token the staged diff ADDS to the commit message. Correct for work this repo performs, wrong for content VENDORED from another repo, so generated paths are DECLARED in `.gitattributes` with `linguist-generated=true` and exempted. Three constraints hold it together: the exemption covers the HUNK rule only (a projected path still needs a reservation — it is never added to `SHARED_SAFE_PATHS`), it is evaluated per path with git's last-match-wins so a later `=false` really subtracts, and `projected_decl_fault` REFUSES any pattern matching compiled source (probes every crate's `src/lib.rs`) so `**` cannot switch the rule off. A fault fails the commit closed with exit 1 and names the pattern — never a silent fallback to enforcing. `staged_added_lines_in`'s `exclude` entries are GLOBS, not literal equality: an exact compare made every declaration match nothing while looking armed. **No live bead id may appear in any published string** — tool description, field description, or resource description. toron.dev projects the file verbatim, so a `bd-*` token there is this repo's private ledger leaking into another project's public docs (`mbx test -p toron-mcp --test catalog_no_internal_ids`). Cite an ADR instead; the bead belongs in the commit and in a `// governed-by: ADR-XXXX` line, never in a serialised string. Plain `//` and non-serialised `///` comments are unaffected.
- Run-state authority rule: the checkpoint log is the durable projection and is authoritative; the model is a pure function of it. That says which side is the RECORD, not that any component may SEED the model from it. `RunStateReplay::install` refuses on every path and names the owner, so a second seeding path cannot grow beside the live host's resume.
- Flag vocabulary: the acting agent is ALWAYS `--as` (`--from`/`--agent` banned, gated by `mbx test -p toron-cli --bin toron flag_conformance`); every project-scoped subcommand REQUIRES `--project`. Channel lifecycle, verify-after-send (`--wait-ack`), delivery visibility (`read --author`), and the inbox cursor (`--since last`, migration 0007) are toron-skill surface; this repo owns their CLI parsing and bus behavior.
- One-workspace tick (accepted 2026-09-01): tag vocabulary rides `extra_tags`, receipts live in `receipts` sqlite, `wait --corr` is subscribe-until-receipt, `role pin/list/clear` binds via 30013 tags. CLI-only nouns per the Skill Sync Contract below; `catalog_freshness` stays green. Semantics live in the toron skill; this repo owns `crates/toron-core/src/mailbox/{receipts.rs,send_tags.rs,identity.rs}`.
- Flywheel Tick Law (live): doorbell dispatcher and `swarm_loop` call `workspace::tick`; slice reducers stay private. Kind-9 is the ring; assignment enters from rumor 14 / FetchInbox→InboxRows.
- Corr authority (accepted 2026-09-03): sender mints non-empty `corr` (`SendTags::parse` rejects empty; `Corr` vs `ThreadId` newtypes, `thread_id` is never `corr` — conflation fails loud in `send_message`); toron sqlite owns `owed→acked→resulted` (monotonic upsert: never downgrades `resulted`, never drags `acked` to `owed`; `owed` on send, `owed→acked` in `send_receipt` idempotent, `resulted` only via explicit `receipt set`).
- `MessageId` is String/hex. Never `u64` in shared request/response types.
- A crew on a harness herdr cannot inject is MAIL-ONLY, and mail is a real
  channel rather than a fallback (measured 2026-09-28 with `freebuff`): the
  dispatcher reports `dispatcher=absent … no live fleet`, pane lookup returns
  `agent_not_found`, and the doorbell has no injectable pane, while the crew's
  inbox fetch, wake digest, and room posts all work. A cross-project send
  addressed by npub was delivered and indexed under the RECIPIENT's project
  slug, which is the proof. So never read an empty dispatcher as an empty
  project — it is a statement about the dispatcher, and a spawn route taken on
  it composes a second swarm beside a live crew. Check `toron mail inbox
  --since <RFC3339>` (never a duration), the doorbell's `<ws>.unanswered`, and
  `/health.injection`, where `pins > injectable` is the signature. Address such
  an agent by npub (`toron mail whois <project> <pin>`), never by name: identity
  is per-project, so a name sends to a key the recipient never reads while
  reporting `ok`. An `unanswered` pin is not proof of existence — one appeared
  in two `unanswered` lists and resolved in neither.
- Pane mail-identity stamp: `agent bootstrap` writes the pin
  onto the pane via display-only `pane.report_metadata` (`toron:mail-identity`
  source, `toron_pin`/`toron_project`/`toron_npub` tokens) — never
  `pane.report_agent` (that steals lifecycle authority). Resolve/cleanup read
  tokens first, `agent.list` names second; liveness always from live
  `pane.list`, never cached. `whois`/`list_agents` carry the resolved live
  pane as additive optionals `pane_id`/`workspace_id`/`tab_id` (`None` when no herdr backend is reachable, advisory, never a routing gate).
- `t` tag = project slug only. Never harness/model/job tags.
- TDD for new features and bug fixes. No benches in this repo.
- Beads: bare `br` is the TUI (ratatui, since 2026-08-24) — humans run `br`, agents run `br --robot-*` with `--format json|toon` (never bare `br` in automation); `bv` is deprecated.
- TUI theme (hacker-night, tui-design skill): single source is `toron-cli/src/theme.rs` (`HackerNight`); widget code uses semantic slots only, never hex/ANSI. `toron tui` lives in `crates/toron-cli/src/tui/`. Skill §-level layout/interaction rules live in the tui-design skill; sibling palettes mirror in `flywheel/src/output.rs` and `beads/src/tui/theme.rs`.
- TUI is an Elm core (ADR-0031 §7): keys → `Msg` in `tui/keymap.rs`, one reducer `tui::update::tick`, effects as data (`tui::msg::Effect`) run by `tui/mod.rs` with a bounded wait, results fed back as `Msg`. No I/O in the fold, and `update.rs` owns the refresh cadence (`SLOW_CADENCE` for bus replays; a tick with effects in flight dispatches nothing). Offline/online wording comes from the presence projection, never a second store.
- **Agent pins are CamelCase Adjective+Noun, and it is enforced.** `IvoryRidge`, `EmberOtter`, `TealMarlin` — exactly two capitalised words, letters only, no two capitals in a row. `toron agent bootstrap` REFUSES any other shape (`nostr::identity::validate_agent_pin`, bd-pinrule). **This is a namespace, not a label:** identity is per-PROJECT, so one agent holds a *different key* in every project it is registered in, and a name two agents can pick independently WILL collide. Measured 2026-09-28: `Buffy` is registered in two projects and `whois` returns two different npubs, so mail sent to that name from one project is encrypted to a key the recipient never reads while the send reports `ok` — not lost, INVISIBLE (bd-xroute). Project slugs are a *different* vocabulary and stay kebab-case; the validator refuses to claim otherwise, because a naming rule that reads as obviously wrong gets ignored. **This is a known defect with a proposed fix, not a permanent rule:** [ADR-0037](docs/decisions/0037-one-key-per-pin-project-scoped-visibility.md) moves project scope off the KEY and onto the message, making one pin one key everywhere. Until it is accepted and migrated, **address by npub** (`toron mail whois <project> <pin>`) — a bare name is only safe inside one project. Note `fetch_inbox` already filters gift wraps by pubkey alone (`messaging.rs:730-735`), so the read side needs no change and the fix is entirely registration-side.
- Engineering principles: `flywheel principles list`. Cite the name that changed the decision. Full leaves: `flywheel principles show <name>`.
- Wave-2 playbook lints: `crates/toron-workflow/src/playbook_lint.rs` — feature requires `data_shape` input, bug-fix requires `repro` predecessor, prototype/arena require `n>=2` + mode rules (`build_slot: true` for compile, no repo build paths for sketch), forensics is read-only, `principles:` names validate against the 21-name registry. Entrypoints: `lint_workflow_with_principles(wf, &[&str])` (embedding passes the registry), `lint_workflow(wf)` (in-crate default).
- `std::env::set_var` only in integration tests that omit `forbid(unsafe_code)`.
- Decision cores stay pure. The binding for new code and tests is the Effect law section below.

## Effect law for code and tests

Before writing or testing a decision core, journal, retry, timeout, breaker, receipt transition, or judgment call, read `~/.agents/skills/effect-oriented/SKILL.md`. That skill is the binding. Journal a live effect before the provider runs.

A core is `tick(&Model, &Msg) -> (Model, Vec<Effect>)`. The file does not read the clock, the environment, or the network. The host runs the effects and feeds results back as messages. Guards are named predicates. Actions carry data. Platform code lives in `platform/<os>.rs`.

A change is unfinished until:

- A determinism test shows one message log produces the same model and the same effects.
- An interpreter test uses a fake and covers success, typed failure, hang, and cancellation. It does not sleep real time or dial the network.
- A live effect is journaled before the provider runs. `Recording` is only for a dry arm. A live effect with no runner refuses.
- Retry wraps one idempotent effect. Fallback stays outside the retry. The breaker stays inside the retry. The attempt timeout wraps only the call.

A change that touches one of these functions closes that defect instead of copying it:

- `dispatch_core` `Release` decrements `used` even when that assignee is not in flight. Decrement only a slot that was held.
- A receipt prune is a tombstone. `upsert` must see it, in the same transaction as the write, or a later owed-on-send recreates the corr.
- `ask_judgment` runs the external call and journals afterward. Journal the attempt first. Ticking a fresh `JudgeModel` on every wake does not make the in-flight guard real.
- `send_armor` half-open admits every caller that arrives after the state flips. One probe, then the rest stay refused until that probe settles.

## Skill Sync Contract

When you add/remove/rename a CLI command or MCP tool, update ALL of:
1. The `Command` enum (`crates/toron-cli/src/cli.rs`) — ground truth (compiles).
2. The tool `#[tool(name = "...")]` in `crates/toron-mcp/src/tools/` — MCP surface (wave 1: **no new tools**; CLI-only nouns still need steps 1, 3, 4).
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
mbx test catalog_freshness # catalog drift gate (toron.dev)
toron catalog --json # emit 38-parity-tool catalog (40 served incl. additive) / 25-resource / CLI catalog
mbx test -p toron-e2e --features p1 # SC-1..8
mbx test -p toron-e2e --features p2 # needs resonate on PATH
mbx test -p toron-e2e --features p4 # SC-18..25 (implies p5)
mbx clippy --workspace --all-targets -- -D warnings
mbx fmt --check
mbx test --workspace --all-features # honest full matrix; CI build does not
```

`mbx test --exclude-ignored` is invalid. Ignored tests: `-- --ignored`.

Release profile: `strip = "none"` (macOS arm64 LINKEDIT / sqlx_macros).

## NOTES

- Captain alias: `captain` is a routable fold onto
  the project's REGISTERED orchestrator pin — set via
  `toron contact captain --project <slug> --pin <adj+noun>`, cleared via
  `toron contact captain --project <slug> --clear` (parent death),
  resolved by `toron mail whois <slug> captain` and `mail send --to captain`. Standing
  orders address reports to `captain`, never to the host username.
- `toron serve` requires `--mcp-stdio` or `--mcp-http`. No-flag serve bails.
- Daemon label `com.toron.serve`, HTTP `127.0.0.1:18080`. Passphrase file
  `~/.config/toron/passphrase` (0600), never in plist/env flags.
  **Operator key is `~/.config/toron/operator_nsec` (0600), and that is the
  ONE canonical path** (ADR-0038: `config::operator_key_path`, shared by the
  daemon unit, the mailbox host fallback, `key generate --role operator`, and
  `doctor`); the unit's `TORON_OPERATOR_KEY` is an explicit pin of it. A key at
  the old data-root `operator/operator.ncryptsec` is a **different identity** —
  the implicit mint REFUSES to mint a second one and names the `mv`, and
  `doctor` reports a legacy-only key as a red naming both paths.
- Daemon runs at `RUST_LOG=warn` (plist + `daemon.rs` templates). Hourly
  copy-truncate via LaunchAgent `com.toron.logrotate` (1 MiB, also rotates
  `resonate.out.log`). One `toron serve` per data dir (`serve.pid`). INFO
  rmcp churn once wrote a 604 MB serve.out.log; resonate INFO doorbell
  poll-fail once wrote 1.3 GB.
- `mbx test --workspace` compiles e2e and runs 0 p1/p2 tests (feature
  gates). sc16–sc24 currently run un-gated.
- Root `tests/` and `examples/agent_ping_pong.rs` are leftovers —
  not workspace members.
- Sibling orchestrator: `../flywheel` (herdr). Map: `../flywheel/AGENTS.md`.
- herdr backend client surface (pane primitives, agent_read source semantics,
  agent_not_idle, prompt gates): `~/.agents/skills/toron/SKILL.md` Backend
  section; harness lifecycle matrix (env injection, boot prime, integration
  preflight, alt-screen ladder): `~/.agents/skills/flywheel/SKILL.md` §7.
