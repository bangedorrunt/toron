# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Squads (ADR-0005 P6.1/P6.2, flywheel)** — `Squad` (named group of profile
  counts) + `Squad::from_recipe` mapping the legacy `Recipe{oc,pi,gk,extra}`
  onto profiles; `flywheel squad {list,show,spawn}` (spawn resolves members
  via `Profile::resolve`); `recipes list` stays an alias. P6.1/P6.2.
- **Runs dashboard (ADR-0005 P6.3/P6.4, flywheel)** — `flywheel runs
  {list,show,pending,digest}` reads the per-project JSONL logs into a run list
  with state + owner + advisory per-run cost; `digest --since N[smhd]`
  reports failures/parked/broken>N; `--robot-snapshot` now carries a `runs`
  array for agent drivers.
- **P6 — Squads + observability COMPLETE** (ADR-0005, epic
  `bd-p6-squads-observability-61td`): squad/recipe unification, squad CLI +
  recipes alias, runs dashboard, robot-snapshot runs section, and the P6
  Behavior Test Matrix rows (`recipe_to_squad`, `runs_view`, squad CLI,
  `snapshot_runs`) all green. P6 is the last closed phase before P4 (smart
  cron) under the ADR-0005 orchestration tier.
- **ADR-0004 primitives LIVE in the daemon** (bd-pkc part 1): the MCP
  daemon's inbound path now runs the `UnwrapPipeline` projection loop
  (`Mailbox::spawn_unwrap_loop`) — inbound gift-wraps are unwrapped with the
  local agent keys, gated by the G14 contact policy, operator-Overseer-bypass
  aware, and written idempotently to the index. Wired in `demo_mailbox()`.
  The pipeline rebuilds when the on-disk agent-key set changes (runtime
  registration). Agent-key signature is now file-path based (no argon2 KDF in
  the async loop — the ≈1s/key decrypt runs once per registration, not per
  wrap). `BusPolicySource::load_many` added for multi-recipient policy
  snapshots.

### Changed

- **Edition 2021 → 2024** across all 5 workspace crates. Unlocks `let_chains`
  / `if_let_guard` (stable, edition-2024-gated); applied to 10 non-test
  sites + the test suite (nested `if let` collapses, clippy
  `collapsible_if`-driven). `std::env::set_var`/`remove_var` are `unsafe` in
  edition 2024 — the env-dependent test cases moved from unit tests to
  integration tests (`tests/{guard_env,coalesce_env,git_binary_env}.rs`),
  env parsing extracted to pure fns (`parse_timeout_ms`, `parse_millis`).
  `gen` reserved-keyword collision in a CLI test identifier renamed.
- **`Backend` trait → native async-fn-in-trait (AFIT)** — removed the
  `async-trait` macro from the trait + herdr/tmux/mock impls and the
  per-call `Box<dyn Future>` boxing. `Bus` + `ProjectionSink` deliberately
  keep `async-trait` (they are used as `dyn`; AFIT/RPITIT traits are not
  dyn-compatible — measured). async_trait sites 10 → 6.
- **Toolchain: nightly → stable** (`rust-toolchain.toml`). The workspace
  uses zero `#![feature]` gates; edition 2024, let-chains, and AFIT are all
  stable. Removes nightly-only build risk. Note: this shell's nix-profile
  `cargo-clippy` (nightly driver) shadows rustup's — use `cargo +stable
  clippy` or the rustup binary; CI (rustup-based) unaffected.

### Fixed

- **Inbound gift-wraps attributed to their project** (2026-08-08) — the
  unwrap-loop projection stored an empty `project_slug` (the wrap hides
  metadata), so project-filtered search always returned `[]`. Registration now
  indexes the `agents` table with the verbatim project slug, and the pipeline
  resolves `project_slug` from the recipient key at projection time; absolute
  project keys round-trip exactly (leading separator preserved).
- **Absolute project keys no longer escape `data_dir`** — `agent_key_path`
  strips the leading separator (was: `PathBuf::join` discarded the prefix, so
  keys landed in the slug's own tree and the unwrap-loop key scan never found
  them → projection never ran); the local-key scan is a bounded recursive walk
  (MAX_DEPTH 16, symlink-safe).
- **`search_messages` envelope parity with agent-mail** — returns
  `{result, next_cursor}` (`s<sort>:i<last-id>`) when rows are non-empty,
  matching the AM contract so the flywheel parity harness can compare shapes.

### Notes

- Bead bd-3wc closed: the "nightly features" payoff for toron is delivered
  by stable modern Rust + edition 2024, not `#![feature]` gates.
- **Parity shape catalog** (bd-96a/bd-3mp): `docs/parity/known-divergences.md`
  documents the intentional toron-vs-AM-Rust response-shape divergences (npub
  vs integer id, `*_ts_us` vs ISO, event-id returns, nested-vs-flat
  summaries). Cross-checked against `toron-mcp` `ToJson` impls + `toron-core`
  mailbox. Companion to `docs/parity/am-rust-tool-catalog.md` (the frozen
  38-tool surface). Unblocks bd-jgd (outputSchema).
- **Research epics filed as notes** (bd-nyg, bd-9o9): ATC learning-loop
  framework (`docs/notes/2026-08-07-atc-learning-loop.md`) and the Forge
  NIP-34 plan (`docs/notes/2026-08-07-forge-nip34-research.md`) — the latter
  carries a kind-map CORRECTION vs the ADR-0003 §Forge draft (canonical
  NIP-34 assigns `30621`=git commit, `1630`=Open status; only `46011` is
  toron-custom).

## [1.1.0] - 2026-08-07

ADR-0004 Operational Hardening & Agent-Automation Efficiency (all 5 ships
shipped via beads, each governed-by ADR-0004). No MCP-surface change: the
38-tool / 25-resource / 9-cluster parity surface is preserved.

### Added

- **Mention coalescing** (`toron-workflow::coalesce`, Ship 1, bd-bzu):
  `MentionCoalescer` batches N `@mention`s to one kind-43001 job per
  assignee. Default 2s window (`TORON_COALESCE_WINDOW_MS`), 500ms
  immediate-flush sub-window (`TORON_COALESCE_SUBWINDOW_MS`) so a lone
  mention does not pay full window latency, hard cap 64, identical-mention
  dedup. Coalesced job keeps the wire shape (kind-43001, richer `spec`).
- **Workflow loop-prevention** (`toron-workflow::trigger_filter`, Ship 2,
  bd-nsv): `should_evaluate_trigger_event` excludes job kinds 43001–43006,
  workflow-run kinds 46001–46012, gift-wrap 1059, and events tagged
  `toron:workflow-emitted` at trigger INGRESS (not projection). Kind-only
  form for bus pre-filters. Regression test
  `self_send_message_workflow_cannot_loop`.
- **`toron doctor reconstruct`** (Ship 3, bd-jwa): disaster-recovery CLI
  rebuilding the SQLite index from the Git archive (`rebuild_index_from_git`,
  sc14a_dr_rebuild parity proof). Integrity + row pre-check → Healthy skip /
  dry-run preview / forensic bundle + quarantine → build fresh →
  convergence verify → atomic POSIX rename. Exit codes 0 ok · 1 failed
  (quarantine preserved) · 3 diagnostic. Flags `--dry-run`, `--project`,
  `--data-dir`.
- **Bounded workflow `if:` guard evaluation** (`toron-workflow::guard`,
  Ship 4, bd-n87): runtime evaluator (lint previously only validated
  syntax). 100ms timeout (`TORON_GUARD_TIMEOUT_MS`), 4 Buzz custom fns
  (`str_contains`, `str_starts_with`, `str_ends_with`, `str_len`),
  sandboxed evalexpr, fail-stop `TooComplex`/`TimedOut`/`TypeError` outcomes.
  Complexity pre-scan (chars, paren depth, bracket depth, subexpression
  count) prevents stack-overflow SIGABRT in evalexpr's recursive tree
  builder on adversarial expressions.
- **Operator Overseer bypass** (Ship 5, bd-o1h): operator-key-scoped
  override — operator-signed kind-14 messages tagged `toron:overseer` with
  `high`/`urgent` importance SKIP contact-policy enforcement at the
  projection (`block_all` cannot block the operator). Key-scoped, not
  name-scoped (an agent cannot spoof `HumanOverseer`). CLI
  `toron mail overseer --to <agent> --body <...>` signs with the operator
  key, sets importance=high, prepends the override preamble. Bypass requires
  the `toron:overseer` tag (operator key alone is not a blanket bypass).

### Changed

- All 38 MCP tools now declare `annotations` (`read_only_hint` /
  `destructive_hint`) — previously 27/38 lacked them (ADR-0003 rev7 L38).
- ADR-0004 Ship 5 env surface: `TORON_OPERATOR_KEY` (path to encrypted
  ncryptsec, matching the existing operator-key load) rather than a raw
  nsec in env.
- Crate versions aligned with the tag scheme: 0.1.0 → 1.1.0 across all
  workspace crates.

### Fixed

- **WAL checkpoint seal (critical)**: `build_fresh_index` now closes all
  pool connections deterministically (`close().await`), checkpoints on a
  single connection with an explicit busy check, and re-verifies the main
  file row count post-checkpoint — a busy checkpoint can no longer silently
  lose uncheckpointed WAL frames on the atomic rename (fail-loud, not
  fail-silent).
- **Guard timeout constant drift**: lint `GUARD_TIMEOUT_MS` now sources the
  runtime `guard::DEFAULT_TIMEOUT_MS` (single source of truth).
- **Guard complexity pre-scan** extended to `[`/`]` bracket depth (evalexpr
  array-literal nesting recursion surface).
- **Release builds on macOS arm64** (`cargo install`, `--release`): cargo's
  1.9x default `strip = "debuginfo"` corrupted the LINKEDIT segment of
  large proc-macro dylibs (`sqlx_macros` ≈ 8.8MB) — dyld rejected them at
  load with "mis-aligned LINKEDIT string pool", breaking
  `cargo install --locked --path` / any release build. Fixed with an
  explicit `[profile.release] strip = "none"` in the workspace manifest.
- **Leftover unused imports** in `trigger_filter.rs` test module removed.

### Security

- Overseer bypass is npub-scoped (seal signer == operator, NIP-59-enforced)
  + tag-scoped + importance-scoped — strictly stronger than AM-Rust's
  name-based `HumanOverseer`.

## [1.0.0] - 2026-08-06

ADR-0003 complete: Nostr-native agent mailbox — 38-tool / 25-resource
AM-Rust parity on Nostr, plus the collaboration & automation superset.
103 beads shipped. See `docs/decisions/0003-nostr-mailbox-fabric.md`.

### Added

- **Phase 1 — AM-Rust mailbox parity on Nostr**
  - Schnorr-signed identity, NIP-17/NIP-44/NIP-59 gift-wrap (true BCC,
    private acks)
  - NIP-49 ncryptsec key store, frozen kind registry
  - InProcessBus (Buzz pipeline: verify → auth → dedup → fanout → hooks),
    dual-emit tags
  - sqlx + FTS5 (porter unicode61 contract), write-behind cache, Git archive
    projection
  - Addressable replaceable + projection key-priority (operator > agent) for
    force-release
  - 38 rmcp `#[tool]` methods + 25 resources + subscriptions (<50ms inbox
    push)
  - Conformance harness, CI gates, CLI (operator + robot + bench), TUI
    dashboard
- **Phase 2 — Collaboration & Automation superset**
  - Resonate durable workflow engine — `request_approval` suspends, survives
    crash, resumes (Buzz WF-08 fix)
  - NIP-29 channels (membership-only gate, global-sub exclusion)
  - Job lifecycle 43001–43006 + @mention dispatch + A7 semaphore
  - Ephemeral presence 20001 + persona packs
- **Integrated mode** (Decision 3A): flywheel embeds toron-mcp in-process

### Notes

- `#![forbid(unsafe_code)]` workspace-wide; 0 asupersync; 0 fastmcp
  (CI-gated via `scripts/verify-gates.sh`).
