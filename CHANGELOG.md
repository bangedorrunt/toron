# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] — 2026-08-31 Tick Law + doorbell bind (GS-005 → GS-009)

### Added
- **Mailbox p-tag for pin names (GS-006, bd-gkw9)** — `emit_ack_sla_expired` now `p`-tags pin names (was hex-only, so `watch_targets` never resolved pin-name audience → doorbell never rang); `check_ack_sla_expired` now has a 60s production caller in the doorbell dispatcher (was test-only).
- **ADR-0026 accepted (both waves)** — `FailureKind` + classifier, funded preflight, fallback chains, bounded mail-health + `mail_degraded` + loop-identity fail-loud (all verified file:line, one recorded deviation: escalation rides eprintln+Blocked).

### Fixed
- **Ingress seam typed (bd-0or2)** — `ingress_to_loop_msg` with real `pane.agent_status_changed`/`pane.exited` strings (were invented strings ignored downstream); only `Closed` maps to `BrClose`.
- **Tick arms real (bd-ugd1)** — `Herdr` buffers into `pending_events` + drains at next `Tick`; `WaveGate`/`CommandVerified` via `LoopEvent` state machine (were `let _ =` no-ops).

### Changed
- Bump `toron-core`/`toron-workflow`/`toron-mcp`/`toron-cli` 1.2.0 → 1.3.0.

## [1.2.0] — 2026-08-30 orchestration engine + guard facts

- Engine: kind-aware `fail_run` (rate_limit/timeout same-node retry, bounded), R-E1/E2/E3
  lint rules, `VerifySink` exec contract pinned by tests, graph-canonical runtime pins
  (park proof, `{ok:false}` predicate).
- Guard: GitFact producer — attributed commits append facts to the orchestrator journal
  (best-effort, never blocks); guard check hardened against stale-holder false blocks
  (bd-0pjf tracks force-release propagation).
- Channels: react resolves just-posted targets from the bus (cross-process visibility;
  cli_robot + e2e sc30 green).
- CLI: `contact captain --clear` for parent-death policy (ADR-0025 W4a, bd-auqj).
- Tracker: bd-o5t7 doorbell hardening umbrella closed — all mirrored children landed;
  superseded-upward by the fact fabric (bd-cy0s).

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- **herdr protocol v20 conformance (2026-08-20)** — herdr upgraded to 0.8.2
  (socket protocol 19→20). Wire structs verified field-for-field against the
  live `herdr api schema --json`: additive only from this client's
  perspective — no struct changes needed. `HDR_PROTOCOL` 19→20 (successor of
  bd-4eze's v19 bump); fixture regenerated from the live 0.8.2-preview
  daemon (`tests/fixtures/herdr-schema-v20.json`, v19 fixture removed); new
  typed error code `agent_blocked` (protocol v20: `agent.prompt` rejects
  agents waiting at approval/question dialogs WITHOUT sending — previously
  flywheel prompts could type into a blocked agent's dialog; classification
  stays `SendVerdict::Coded`, no respawn/stall semantics). Empirical
  subscription-shape check: the daemon accepts `type` + dotted event names
  on `events.subscribe` (its own schema's EventMatch def with `event` +
  underscores diverges — herdr-side doc bug, client shape confirmed
  correct). Live round-trip + 27 backend tests + flywheel deps gate green
  ("herdr protocol: 20 (verified 20)").

- **Catalog pipeline (toron.dev ADR-0002 D3, 2026-08-14)** — `toron catalog`
  emits the machine-readable surface catalog to `docs/catalog/toron-mcp.json`:
  all 38 AM-Rust parity tools (name, group, description, input/output JSON
  schemas verbatim from the rmcp `#[tool]` definitions; the bd-h3oj additive
  excluded by contract) + the 25 resources + the CLI `Command` enum
  introspected from clap. `cargo test catalog_freshness` regenerates and
  diffs — a surface change without a regen fails red (unmergeable). New
  extractor modules: `toron-mcp/src/catalog.rs`, `toron-cli/src/catalog.rs`.
- **ADR-0014 — Kind Registry Specification (2026-08-14)** — formalizes all 12 custom
  Nostr kinds (fields, d-tag semantics, lifecycle states, `status:` field on 30031,
  @mention `p` tag convention, job lifecycle 43001-43006, approval kinds 46011/46012).
  Documents the process for adding/deprecating kinds. Amends ADR-0003.
- **ADR-0015 — Flywheel ↔ Toron Integration Contract (2026-08-14)** — formalizes the
  boundary: flywheel guarantees (one-way dep, single mail surface, `MessageId` String,
  single herdr client), toron guarantees (`MailSurface` superset, atomic reservations,
  crash-survivable workflows, durable root, addressable dedup, bounded @mention), what
  each side must NOT assume, and the versioning process.
- **Skill updates (2026-08-14)** — tool count 38→39 (38 AM-Rust parity + 1 toron additive
  `release_file_reservations_by_reason`). Skill version 0.2.0→0.3.0. Added 3 reference docs
  (mcp-surface, workflow-engine, daemon-ops). Added CLI table entries for `workflow`, `status`,
  `config`, `metrics`, `tui`, `doctor reconstruct`.
### Added

- **ADR-0012 — Standalone Cron Type (`bd-25g7`, 2026-08-11)** — cron is a FIRST-CLASS
  scheduled primitive DISTINCT from workflow (NOT a workflow-with-schedule-trigger). `CronSpec`
  (schedule + owner + scope + mode + `CronSpecPolicy` + a steps body composing workflows via
  `call_workflow`), `parse_cron`, `lint_cron`/`cron_lint_yaml` (distinct from workflow lint), and
  the engine cron path: a `CronManager` over `CronSpecRegistration`s, each a Resonate
  `ctx.sleep` durable-sleep loop firing the body as a durable child (`run_cron_tick`), with
  `catchup: latest` on wake. One engine, two input types (workflow + cron). Amends ADR-0005
  Spec 3.
- **ADR-0011 — Coalition Orchestration (toron-core substrate, `bd-foh8`, 2026-08-10)** — file
  reservations gain a `status: intent→held→released` tier (30031 state machine; status-less =
  held backward-compat). The conflict check splits HARD (held → block) vs SOFT (in-TTL intent →
  freeze+propose, NOT a hard block); `declare_intent` + intent TTL + must-convert (anti-griefing).
  Request-release negotiation + escalation. The atomic conflict-check floor is NEVER removed.
  Substrate for flywheel coalitions (NIP-29 channel + goal run).
- **ADR-0010 — Execution Wiring (engine surface, 2026-08-09/10)** — value-returning actions +
  `on_error: fail_run` (kind-46012 `status=failed`), the real-runner subprocess-reuse dispatch,
  headless (`harness -p --mode=json` + `runs/<run_id>.jsonl`) vs pane execution modes,
  `--execute`, the model registry (`models.toml` + per-harness injection + retry-on-429
  fallback), and per-step harness override.

### Fixed

- **Reservation replay-window truncation (P0, commit bd97afb, 2026-08-10)** — `BusEventStore`
  applied the SQL LIMIT (default 1000, ASC) to the raw scan BEFORE the kind filter, so with
  >1000 events the newest reservations were invisible to the conflict check → two distinct
  agents could hold the same exclusive path. Kind filter pushed into SQL. Verified live: a
  cross-agent acquire of a held path now hard-blocks.

- **P4.4 cron failure policy (ADR-0005 Spec 3, `bd-p4-bead-on-fail-0uaf`,
  toron-workflow)** — the DECISION layer of bead-on-failure + repeat-failure
  escalate. `cron_failure.rs`: pure `evaluate` (on_failure / on_repeat_failure
  at the exact `repeat_threshold` / on_success resets the streak), `is_failed`
  (kind-46012 `status=failed` shape), `failure_details`, `FailureBundle`, and
  a distinct-newtype `PolicySink` (TypeId-safe). The cron loop now captures
  each tick's child output and records a `PolicyEvent` on the sink; the
  embedding (flywheel) performs the side effects (`br create` bead / kind-14
  escalate DM). The consecutive streak is a durable loop local — a fresh loop
  (e.g. after restart) starts at 1, never double-counts. `PolicySink` is
  registered on the engine like the approval/spawn sinks. Rows green:
  `cron_failure_bead` (sc22) + `cron_escalate` (sc23, threshold exact);
  6 cron_failure unit tests; 123 toron-workflow tests; full e2e suite green.
- **P4.3 cron manager (ADR-0005 Spec 3, `bd-p4-cron-mgr-ur33`, toron-workflow)**
  — a Resonate **durable-sleep loop** drives each registered cron: the
  manager registers a `cron_loop` durable function; per cron, `cron:{name}`
  loops `ctx.sleep(until next tick)` → fires the owning workflow as a durable
  child → advances the durable checkpoint. Restart survival + no-double-fire
  fall out of Resonate replay (deterministic child ids, `cron:{name}`
  re-invocation resumes the same loop). Missed-tick policy = **`latest`**
  (`latest_due_at_or_before` picks the most-recent due tick at-or-before now —
  one run on wake, never a burst). Cron-expression parsing (`next_due`,
  5-field vixie → 6-field crate) lives in toron (ADR-0003 3A: no flywheel
  import); flywheel's `crons.toml` registry bridges via the `CronRegistry` DI
  seam. `cron` + `chrono` deps added. Rows: `cron_success` + `cron_restart`
  (sc21) + `cron_catchup` (sc24) green; 7 cron unit tests; 117 toron-workflow
  tests.

### Fixed

- **Approval/spawn promise-park regression (ADR-0005, `bd-p4-spawn-await-pxw6`)** —
  `ApprovalSink` and `SpawnSink` were the SAME concrete type
  (`Arc<Mutex<Vec<String>>>`); the Resonate dependency map keys by `TypeId`, so
  the second registration silently shadowed the first and every
  `get_dependency::<Sink>` resolved to the spawn sink. Approval runs parked
  their promise_id into the wrong channel — the embedding never saw it and
  `request_approval` appeared to hang (sc11/unit red post 0.6 migration).
  Both sinks are now distinct newtypes with `Deref` (unique `TypeId`, call
  sites unchanged). Approval + spawn + sc11 + sc18/sc19/sc20 all green;
  branch `then/else` test also now exercises the else arm.

### Added

- **P4 — Smart cron COMPLETE (ADR-0005, epic `bd-p4-smart-cron-xvbj` closed)** —
  self-reporting + defined-fixer cron fully landed: schedule trigger extension
  (P4.1), spawn-await + promise↔job binding (P4.2), Resonate durable-sleep
  cron manager with `latest` catch-up + restart survival (P4.3), failure policy
  with bead/escalate decisions (P4.4), sagas→workflow migration (P4.5), cron
  CLI (P4.6), and the full P4 Behavior Test Matrix (P4.7). **ADR-0005 now FULLY
  COMPLETE (P0–P6).** Final verification (BlackMarsh): verify-gates.sh PASS,
  toron-workflow lib 123/123, toron-e2e p1,p2,p4 39/39.
- **P4.2 spawn-await CLOSED (ADR-0005 Spec 4a, `bd-p4-spawn-await-pxw6`)** —
  spawn action + promise↔job binding + no_claim_timeout landed and closed by
  BlueOak; approval/spawn sink TypeId fix (see Fixed) shipped in the same bead.
- **P4.7 ALL P4 Behavior Test Matrix rows GREEN (ADR-0005, `bd-p4-tests-4cl7`,
  toron-e2e)** — harness fixtures (`tests/common/mod.rs`) + scenario IMPLS for
  every P4 row, against the landed P4.3/P4.4 code: `sc18` spawn_no_claim_timeout,
  `sc19` spawn_await_resume, `sc20` spawn_await_failure, `sc21` cron_success +
  cron_restart (BlueOak, hardened), `sc22` cron_failure_bead (FAILED run →
  PolicySink Bead event with full FailureBundle; human-in-loop when no fixer),
  `sc23` cron_escalate (repeat_failure ≥ threshold → Escalate event, streak ≥
  threshold), `sc24` cron_catchup (`latest_due_at_or_before` selects the latest
  missed tick — one, not a burst; manager wake fires a single tick). Full suite
  `cargo test -p toron-e2e --features p1,p2,p4` = 39/39 green; toron-workflow
  lib 123/123. The `p4` feature gate (p4 = p5). sc22/sc23 assert the policy
  DECISIONS (sink events); flywheel's `br create` / kind-14 DM execution is the
  embedding's wiring.
- **P4.7 P4 behavior-test HARNESS (ADR-0005, `bd-p4-tests-4cl7`, toron-e2e)** —
  shared cron fixtures (`tests/common/mod.rs`: `boot_server`/`wait_ready`,
  `cron_registry`/`fixture_registration`, `housekeeper_workflow`,
  `noop_runner`/`housekeeper_resolver`, `next_tick`) + scenario skeletons for
  the remaining FW rows — `sc22_cron_failure_bead` (P4.4: FAILED run → br bead
  with full failure bundle, human-in-loop when no fixer), `sc23_cron_escalate`
  (P4.4: repeat_failure ≥ threshold → kind-14 DM, importance=high/ack_required),
  `sc24_cron_catchup` (P4.3: missed tick → `latest` catch-up, no burst). All
  `#[ignore]`d until their P4.3/P4.4 dependencies land; the `p4` feature gate
  (p4 = p5) added. `sc21_cron_durable_loop` (BlueOak) covers `cron_success` +
  `cron_restart`. Run: `cargo test -p toron-e2e --features p4`.
- **P4.7 P4 Behavior Test Matrix rows (ADR-0005, `bd-p4-tests-4cl7`, toron-e2e)**
  — SC rows landed: `sc19_spawn_await_resume` (spawn+await SUSPENDS, resumes on
  kind-43004 resolve, run completes with the job result) and
  `sc20_spawn_await_failure` (kind-43006 reject → step fails, NOT a decision).
  Both green against BlueOak's P4.2 engine chain. `sc18_spawn_no_claim_timeout`
  (UT row) green. Run: `cargo test -p toron-e2e --features p5`.
- **P4.1 schedule trigger extension (ADR-0005 Spec 3, `bd-p4-schedule-ext-5v2s`,
  closed)** — trigger owner/scope + workflow cron policy landed (toron 2bc9840,
  flywheel 68fcc84). P4.1 closure was previously unrecorded in this changelog.
- **P4.2 spawn action + promise↔job binding (ADR-0005 Spec 4a,
  `bd-p4-spawn-await-pxw6`, engine chain)** — `spawn` action, `ProfileResolver`
  DI seam, `spawn_sink` side-channel, `resolve_spawn`/`reject_spawn` bridges,
  and the resonate-sdk 0.6 engine migration (a1fe947). The 43001 emit carries
  the promise_id in its `spec`; 43004/43006 resolve/reject the parked promise.
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

- **RESOLVED (working tree, BlueOak P4.2 lane): `request_approval` suspend
  regression** — root cause was a TypeId collision in the Resonate dependency
  map: `ApprovalSink` and `SpawnSink` were both `Arc<Mutex<Vec<String>>>` type
  aliases, so `get_dependency` resolved both to the last-registered sink after
  the resonate-sdk 0.6 migration (a1fe947). Fix: distinct newtypes (unique
  `TypeId`), `Deref` preserves call sites. sc11 (ADR-0004 suite) + engine
  approval unit tests green again. Full p1/p2 e2e suite green.
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

## [1.2.0] - 2026-08-24

Nostr protocol hygiene, NIP-29 group metadata (ADR-0023), rust-nostr 0.45.
Tracker: `bd-tt5o`, `bd-zb0y`.

### Added

- **ADR-0023 — NIP-29 group metadata** — channel create/close emit kind
  **39000** (`d`=`h`=channel id); join/leave emit **39002** member snapshots
  (`p` tags + JSON member list in content). Kinds 40/41/42 ingest-only.
  `ChannelStore` stays the membership gate. No 9000–9008 admin, no new MCP
  tools. Named rust-nostr kinds: `Kind::GroupMetadata` / `Kind::GroupMembers`.

### Changed

- **rust-nostr 0.45.2 / nostr 0.45.3** — protocol types live in the `nostr`
  crate (no `nostr_sdk::nostr` nest). Signing is `FinalizeEvent::finalize`.
  Relay bus uses `Client::notifications()` / `ClientNotification`. NIP
  features (`nip44`/`nip49`/`nip59`) plus `os-rng` sit on `nostr`.
- **`gift_wrap` is synchronous** — rust-nostr 0.45 `GiftWrapBuilder` is
  sync. Callers drop `.await`.

### Fixed

- **NIP-01 addressable latest-wins** — store rejects an older `created_at`
  for the same `(kind, author, d)`. Same-second upserts last-write-win
  (`Timestamp::now()` is 1s). Projection ties use the lowest event id.
- **Gift-wrap verify-before-decrypt** — envelope Schnorr sig is checked
  before NIP-44 decrypt (NIP-44/NIP-59).
- **NIP-10 marked `e` tags** — hex thread ids emit `["e", id, "", "root"]`
  (positional e-tags are deprecated).
- **NIP-25 reactions** — kind-7 now carries `p` (target author) and `k`
  (target kind) plus `e` and `h`.

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
