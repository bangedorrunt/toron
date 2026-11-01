# Resonate removal inventory

Read-only sweep at `65f68f24` on 2026-11-01, **plus** the replacement lane
landed at `7d9a9e7` (`durable_host.rs`, `durable_workflow.rs`, `kernel.rs`). Method:
`rg -n -i resonate`, `git ls-files`, `sed`. **682 matching lines across 75
files** (excludes `target/`, `Cargo.lock`); 37 are `.rs`. Nothing below is
inferred from an earlier note.

## 1. Cargo dependencies

| Manifest | Line | Exact text | Kind |
|---|---|---|---|
| `crates/toron-workflow/Cargo.toml` | 17 | `resonate-sdk = "0.6"` | `[dependencies]`, no features |
| `crates/toron-workflow/Cargo.toml` | 18 | `resonate-sdk-macros = "0.1"` | `[dependencies]`, no features |
| `crates/toron-e2e/Cargo.toml` | 26 | `resonate-sdk = "0.6"` | `[dependencies]` (crate is test-only) |
| `crates/toron-e2e/Cargo.toml` | 27 | `resonate-sdk-macros = "0.1"` | `[dependencies]` |
| `crates/toron-cli/Cargo.toml` | 29–33 | comment "`doctor` reads the resonate /health…" + `reqwest = { version = "0.13", features = ["blocking"] }` | not a resonate crate; exists only for the `/health` probe |
| `../flywheel/Cargo.toml` (sibling repo) | 40 | `resonate-sdk = "0.6.0"` | out of this workspace, **blocks removal** (§7) |

`crates/toron-core`, `crates/toron-mcp`, and the root manifest name no resonate
crate; `toron-mcp` reaches Resonate only through `toron-workflow`
(`workflow_host.rs:40`).

## 2. Source surfaces

`resonate_sdk` identifier: **53 occurrences in 6 files**;
`#[resonate_sdk::function]`: **8** (6 shipped, 2 test). `ctx.*` call sites:
`ctx.run` 18, `ctx.sleep` 11, `ctx.get_dependency` 20, `ctx.promise` 4,
`ctx.info` 1; `ctx.rpc`, `ctx.detached`, `ctx.options` **0**.

### toron-workflow — 204 lines, 15 files

| file:line | What it does |
|---|---|
| `src/engine/mod.rs:48-49` | `use resonate_sdk::prelude::{Resonate, ResonateConfig}` + `types::Value` |
| `:58,:60,:62` | `REMOTE_RESONATE_URL` (`https://resonate.example-tailnet.ts.net`), `LOCAL_RESONATE_URL` (`http://localhost:8001`), `RESONATE_URL_ENV` |
| `:74,:106,:112,:181` | `resonate_choice()` OnceLock cache, pure `resonate_choice_with`, env pin reader, `resolve_resonate_url` precedence pin > remote > local > embedded |
| `:130-177` | `ResonateChoice` enum + `url`/`is_degraded`/`degradation_detail` |
| `:201,:217` | `probe_candidate` 2s TCP probe; `host_port` parser |
| `:449` | `WorkflowEngine { resonate: Arc<Resonate> }` |
| `:478-786` | local constructor funnel; `Resonate::local()` at `:760`, `register(run_workflow\|run_graph_node\|run_parallel_copy)` at `:770-783` |
| `:794-1032` | remote constructor funnel; `ResonateConfig { url }` at `:1003`, same three registrations |
| `:1053` | `pub const fn resonate(&self) -> &Arc<Resonate>` (cron manager uses it) |
| `:1088` | `run()` → `resonate.run(run_id, run_workflow, input)` |
| `:1160,:1176` | `resolve_approval` / `reject_approval` → `resonate.promises.resolve/reject` |
| `:1193,:1206` | `resolve_spawn` / `reject_spawn` → same promise API |
| `:1217-1360` | 7 pure tests of server selection (no network) |
| `src/engine/control_flow.rs:189-190` | `#[resonate_sdk::function] fn run_workflow(ctx, RunInput)` — in a 3,340-line file; every `delay`/`park`/`approval`/`foreach`/`branch`/`call_workflow` arm |
| `:1759-1760` | `#[resonate_sdk::function] fn run_parallel_copy` |
| `:2894-2895` | `#[resonate_sdk::function] fn run_graph_node` |
| `:650,:663` | `park_now_ms(ctx)` reads the SDK clock; `render_park_until_ms` |
| `:701,:727` | park journal through injected runner / intent row |
| `:1104,:2312,:2955` | `Error::Suspended` propagation (park/approval unwinding) |
| 27 signatures | `-> resonate_sdk::prelude::Result<…>` — the crate's error type leaks through the public surface |
| `src/cron.rs:176,:236,:402` | `#[resonate_sdk::function] decide_tick`, `run_cron_tick`, `cron_loop` (`ctx.sleep` durable-sleep loop) |
| `src/cron.rs:587-680` | `CronManager`; registers the three functions on `engine.resonate()`; `ctx.run` child fan-out at `:390` |
| `src/engine/tests.rs:73,:118` | `remote_engine` / `remote_engine_with` boot a real server |
| `src/engine/park.rs:46`, `sinks.rs:18,39`, `cron_failure.rs:66`, `dispatch.rs:16,254,315`, `lint.rs:18,657`, `schema.rs:10,163,592`, `cron_elm.rs:6`, `lib.rs:14` | prose or error text naming Resonate (no code dependency) |
| `src/engine/durable.rs:16,:347-349` | The replacement model: `unsupported_reason` refuses `graph:` — "graph workflows stay on the old engine until the graph model lands" (no longer names Resonate) |
| `src/engine/durable_host.rs` (landed `7d9a9e7`) | `DurableRun`: owns `RunModel` + `OurEngine` row + `RunState`, persists model before engine verb; **0** resonate mentions |
| `src/cron_core.rs:15,148` | doc; `:406` is a **negative assertion** in the import fence — keep |
| `tests/reduce_action.rs:23` | comment: local runtime needs a reactor |

### toron-core — 51 lines, 3 files (no SDK dependency; process orchestration)

| file:line | What it does |
|---|---|
| `src/testkit.rs:22-28` | orphan ledger path `toron-resonate-children.ledger` |
| `:66-166` | `reap_orphaned_servers`, `pid_alive_and_is_our_resonate` (matches `resonate` in cmdline + port) |
| `:210-320` | SIGTERM→SIGKILL bounded drain; `ChildGuard::resonate_serve` spawns `resonate serve --storage-type sqlite --server-port <p> --server-host 127.0.0.1` |
| `:323` | `resonate_installed()` probes `resonate --version` |
| `:470-780` | 10 tests: reaper identity/port, ledger fencing, kill grace |
| `src/busy_owner.rs:7,103`, `src/nostr/kinds.rs:63,65` | doc comments only |

### toron-mcp — 15 lines, 1 file

| file:line | What it does |
|---|---|
| `src/workflow_host.rs:1083-1120` | Resolves `resonate_choice()` at host construction; remote → `WorkflowEngine::remote_with_…journal`, embedded → `local_with_…journal` |
| `:1151` | `over_bus` test embedding builds `WorkflowEngine::local_with_async_effect_runner` |
| `:4419-4469` | `host_lifecycle_parked_run_suspends_and_resolves` — real `ChildGuard::resonate_serve` + `WorkflowEngine::remote` |

### toron-cli — 59 lines, 4 files

| file:line | What it does |
|---|---|
| `src/operator.rs:500-562` | `check_resonate` — verdict on the *resolved* server; embedded ⇒ `ok:false` (GS-037) |
| `:569-623` | `https_health_status` GET `{url}/health` over reqwest; `https_health_verdict`; `check_resonate_tls` |
| `:634-681` | `check_resonate_at` — TCP connect + `GET /health HTTP/1.0`, 2xx only |
| `:701` | registered as the 4th doctor check |
| `:1484-1670` | `check_resonate_tests` — 6 unit tests of the verdict matrix |
| `src/platform/macos.rs:271-277` | logrotate script also truncates `flywheel/resonate.out.log`; test `:390-398` |
| `src/cli.rs:155` | doctor help string names resonate |
| `tests/cli_robot.rs:2070` | comment only |

### Dead files (not compiled, references crates that do not exist)

`tests/foundation_test.rs:3,21` and `tests/recovery_test.rs:3` import
`resonate_sdk::prelude::{Resonate, ResonateConfig}`, `toron::hub` and
`asupersync`. The root `Cargo.toml` has **no `[package]`**, and no crate named
`toron` or `asupersync` is in the workspace (`crates/` = core, mcp, cli, e2e,
workflow). These are ADR-0001-era corpses; delete, do not port.

## 3. Process / host surfaces

| Surface | Where | Notes |
|---|---|---|
| `resonate` binary on PATH | `toron-core/src/testkit.rs:261,323`; 12 `ChildGuard::resonate_serve` call sites in 11 files | test-only; production never spawns it |
| `resonate serve` args | `testkit.rs:261-274` | `--storage-type sqlite --server-port <p> --server-host 127.0.0.1` |
| remote server | `engine/mod.rs:58` | `https://resonate.example-tailnet.ts.net` (Tailscale; no unit in this repo) |
| local server | `engine/mod.rs:60` | `http://localhost:8001`, launchd `com.flywheel.resonate` (**outside this repo**) |
| `RESONATE_URL` | `engine/mod.rs:62,112`; `operator.rs:503` | only env var; no `RESONATE_*` others exist |
| `/health` probe | `operator.rs:540-560,569-681`; `workflows/docker-upgrade.yaml:50-54` (`http://127.0.0.1:8001/health`) | doctor fails closed on non-2xx |
| readiness probes (tests) | `e2e/tests/common/mod.rs:108`, `sc11:95`, `engine/tests.rs:73` | TCP connect only, not `/health` |
| CI | `.github/workflows/ci.yml:83-104` | job `e2e-p2` runs `cargo install resonate --locked`; **`continue-on-error: true` (line 89)** |
| docs claim of CI gate | `AGENTS.md:147` | `mbx test -p toron-e2e --features p2   # needs resonate on PATH` |
| local debris | `resonate.db` (135 KB, 2026-09-28) | gitignored (`.gitignore:9-11`), untracked; delete by hand |

No justfile, Makefile, `.mise.toml`, `docker-compose`, systemd unit, or launchd
plist for Resonate exists in this repo. `toron daemon install` never writes
`RESONATE_URL` into the unit (`daemon.rs`: 0 matches for `resonate`).

## 4. CLI surfaces

| Command | File | Resonate behavior |
|---|---|---|
| `toron doctor [--check]` | `cli.rs:150-157`, `operator.rs:701` | 4th check; FAIL exits 3. Only production health surface |
| `toron workflow run <file> --trigger` | `cli/workflow.rs:74-107` | builds `WorkflowEngine::local_with_async_effect_runner` — embedded Resonate runtime |
| `toron workflow invoke <file> --inputs` | `cli/workflow.rs:110-142` | same |
| `toron workflow lint\|validate\|dry-run` | `cli.rs:374-410` | no Resonate |

There is **no** `toron resonate …` subcommand, no `--resonate-url` flag, and no
CLI cron command (cron is driven out of repo). `toron daemon` verbs are
Resonate-agnostic.

## 5. Docs and fixtures

| Path | Lines | Class |
|---|---|---|
| `docs/decisions/0009-daemon-lifecycle-and-resonate-selection.md` | 11 | **Normative** — decides remote→local→embedded selection (§3) |
| `docs/decisions/0005-orchestration-layer.md` | 32 | **Normative** — ":971 Resonate is the only execution substrate"; P5 gate |
| `docs/decisions/0032-workflow-engine-honesty-jido-contracts.md` | 13 | **Normative** — ":18 Keep Resonate"; ":444 Resonate remains the execution substrate" |
| `docs/decisions/0012-standalone-cron-type.md` | 11 | **Normative** — "Spec 5 — Resonate-native (confirmed)" :87-88 |
| `docs/decisions/0003-nostr-mailbox-fabric.md` | 27 | **Normative** — Phase-2 differentiator, SC-11 witness (:960) |
| `docs/decisions/0015-flywheel-toron-integration-contract.md` | 3 | **Normative** — selection contract :61-63 |
| `docs/decisions/0022`, `0024`, `0025`, `0026`, `0007`, `0010`, `0020`, `0001`, `0004`, `0035`, `0036`, `README.md` | 1-19 each | Descriptive/constraint rows; 0036:65 calls Resonate a "DEAD-END as transport" |
| `README.md` | 14 | Descriptive — architecture, durability pitch, selection, link |
| `CHANGELOG.md` | 9 | Historical release notes |
| `docs/guides/workflow-engine.mdx` | 3 | Descriptive — "a Resonate promise" (:6), park clock (:50) |
| `docs/guides/daemon-ops.mdx`, `quick-start.mdx` | 2,1 | Descriptive — `flywheel deps` row, `FAIL resonate` |
| `docs/catalog/toron-mcp.json` | 1 | **Generated** (`generated_at` :174) — doctor description |
| `docs/notes/*` (2), `docs/review/*` (2) | 4,5 | Descriptive review prose |
| `workflows/docker-upgrade.yaml` | 3 | **Fixture** — `gate-resonate` health gate + notify text |
| `messages/2026/08/*.json` (5) | 5 | Archived bus events (operational data, not fixtures) |
| `AGENTS.md` | 1 | Operator instruction (`:147`) |

## 6. Tests

Subject **is** Resonate ⇒ delete with the feature:

| Test | File:line |
|---|---|
| `concurrency_spike` (SC-16, P5 gate; defines 2 `#[resonate_sdk::function]`) | `toron-e2e/tests/sc16_concurrency_spike.rs` (32 lines, whole file) |
| orphan reaper / ledger / kill-grace (10) | `toron-core/src/testkit.rs:470-780` |
| selection matrix (7, pure) | `toron-workflow/src/engine/mod.rs:1217-1360` |
| doctor verdict matrix (6) | `toron-cli/src/operator.rs:1484-1670` |
| `logrotate_plist_is_hourly_and_covers_resonate` | `toron-cli/src/platform/macos.rs:390-398` |

Run **on** Resonate, subject is workflow behavior ⇒ rewrite onto the new host:

| Test(s) | File:line | Needs real server |
|---|---|---|
| `approval_suspends_and_resumes`, `approval_rejection_is_a_decision_not_a_failure`, `spawn_await_resumes_on_job_result`, `spawn_await_failure_fails_the_step`, `park_desugared_request_approval_still_parks`, `approval_payload`, 3 `park_live_*` | `toron-workflow/src/engine/tests.rs:158,252,313,366,2689,2834,3710,3894,3950` | yes (6 skip guards in this file) |
| `cron_spec_loop_fires_tick_with_catchup_and_resume_dedup` + live cron | `toron-workflow/src/cron.rs:1186,1268` | yes |
| `host_lifecycle_parked_run_suspends_and_resolves` | `toron-mcp/src/workflow_host.rs:4419` | yes |
| SC-11 crash-resume | `toron-e2e/tests/sc11_workflow_approval.rs` (`#![cfg(feature="p2")]`) | yes |
| SC-17 graph DAG | `toron-e2e/tests/sc17_graph_dag.rs` | local embedded |
| SC-18/19/20 spawn-await, SC-21/22/23/24 cron | `toron-e2e/tests/sc18…sc24` | yes |
| `orphan_guard` scenarios | `toron-e2e/tests/orphan_guard.rs` | yes |
| `reduce_action`, `durable_model`, `durable_host`, `cron_elm` | `toron-workflow/tests/{reduce_action,durable_model,durable_host,cron_elm}.rs` | local; the last three are replacement witnesses |

Live-server SKIP guards: **24 early-`return` points across 12 files**. Only
`sc11` has a `#![cfg(feature)]` gate; sc16 and sc18–24 run in *every*
`cargo test` and `return` early — **pass**, not ignored.

## 7. Dependency-ordered removal plan

"Green" means the named test passes, not that a suite skips.

1. **Wire the linear durable host.** Files: `crates/toron-workflow/src/engine/durable_host.rs`
   (landed `7d9a9e7`), `crates/toron-core/src/index/durable_workflow.rs`
   (`WorkflowStateStore` + `RunState`/`SyncWorkflowState`, landed `7d9a9e7`),
   `crates/toron-workflow/src/engine/durable.rs` (pure fold). It has 5
   witnesses (`tests/durable_host.rs`: restart-at-unsettled-step, delay
   re-wake, approval watchdog, unsupported shapes, typed phase errors), and
   since `06270f3` every one of them runs on the production seam —
   `SyncWorkflowState`, `ToronEngineStore`, and `ToronJournal` over one SQLite
   pool. Still missing: a production caller (step 5).
2. **Extend the model to graphs.** `durable.rs:347-349` refuses every workflow
   with `graph.is_some()`, and `tests/durable_host.rs:322` pins that refusal.
   Witness: SC-17 rewritten. Cannot delete Resonate before this; `graph:` is
   shipped in `schema.rs` and SC-17 runs today.
3. **Wire cron.** Files: `crates/toron-workflow/src/cron_elm.rs` (exists),
   `tests/cron_elm.rs`. Must exist first: a production cadence driver in
   flywheel calling `ElmCron::tick`. **Missing today**: `ElmCron` has exactly
   one caller, its own test; `CronManager` has zero production callers in this
   workspace.
4. **Port the out-of-repo consumer.** `../flywheel/Cargo.toml:40` keeps
   `resonate-sdk = "0.6.0"` and `:50` links `toron-workflow`; 242 lines across
   `flywheel/src/{main,doorbell,durable}.rs` use it. Nothing in toron can be
   deleted while that builds — `WorkflowEngine::remote_*`, `run_workflow`,
   `resolve_approval`, `resolve_spawn`, `CronManager` are all public API it
   calls.
5. **Cut the run path over.** Files: `workflow_host.rs:1083-1160`,
   `toron-cli/src/workflow.rs:74-142`. Witness: SC-11 rewritten;
   `host_lifecycle_parked_run_suspends_and_resolves`; all 3 `park_live_*`.
   The cut is three contained moves, in this order:
   - **(a) One leaf policy.** `engine/leaf.rs` holds `LeafSeams` (async runner,
     sync runner, verify sink, journal mode) + `dispatch_leaf`. The Resonate
     path builds it with `LeafSeams::from_context`; the durable path builds it
     from the host's injected seams. One implementation, no copy.
   - **(b) The host loop.** `toron-mcp` owns it: `DurableRun::open` → dispatch
     each `ExecuteStep` through `dispatch_leaf` → `step_ok`/`step_failed` →
     run the engine verb on `ParkUntil`/`AwaitComplete`. The model decides the
     shape; `unsupported_reason` sends graphs and control-flow actions to the
     existing `WorkflowEngine::run` before any row exists.
   - **(c) The handles.** One pool, three handles: `ToronEngineStore`,
     `ToronJournal` (required — a fresh in-memory journal cannot settle a
     terminal edge after a restart), `SyncWorkflowState`. The host builds them
     once and passes them per run.
   Witness for the cut: a graph workflow still runs on the old path **and** a
   linear one completes after a daemon restart with no repeated settled step.
   The daemon cut has three side-channels that are easy to miss, because the
   old engine owned them and the model does not:
   - **Publishing an approval.** `RunEffect::AwaitApproval` must publish the
     kind-46011 event itself. The run id is the correlation key (it replaces
     the Resonate promise id), so an operator resolving an older event must
     still find the run.
   - **Resolving an approval.** `resolve_approval`/`reject_approval` become
     `DurableRun::approve` through the state store, not
     `Resonate::promises.resolve`. A resolution that arrives before the park
     is durable must wait for the model, not error.
   - **The park drain.** `PendingDrain` reads the engine's approval sink
     today; the durable path feeds it from `RunEffect::AwaitApproval`.
     Spawn parks are untouched: the model refuses `spawn`, so `SpawnSink`
     stays on the old path with the workflow.
6. **Delete server selection and the doctor check.** Files: `engine/mod.rs:52-238`,
   `operator.rs:500-681,701`, `toron-cli/tests/*`. **Decide first** what `doctor`
   reports for parked-run capability; deleting the check removes the only
   detector of a run that suspends forever. Witness: rewritten doctor unit
   tests, `toron doctor --check` on a host with no server.
7. **Delete the testkit server harness.** Files: `toron-core/src/testkit.rs:22-166,210-320,323,470-780`;
   callers `e2e/tests/{common/mod.rs,orphan_guard.rs,sc11,sc16,sc18..sc24}`,
   `workflow_host.rs:4419`, `cron.rs:946-1283`, `engine/tests.rs:69-150`.
   Witness: `orphan_guard` deleted, not skipped; `rg ChildGuard::resonate_serve`
   returns 0.
8. **Drop the dependencies.** Delete `toron-workflow/Cargo.toml:17-18`,
   `toron-e2e/Cargo.toml:26-27`, `[features] p5` and `p4 = ["p5"]`
   (`toron-e2e/Cargo.toml:12-15`), and `toron-cli/Cargo.toml:29-33` reqwest
   (only if no other blocking caller). Witness: `cargo metadata` shows no
   `resonate`; `cargo test --workspace` green.
9. **Supersede the decisions, then fix prose.** ADR-0032 (`:18` keep), ADR-0005
   (`:971` only substrate), ADR-0012 (`:87-88`), ADR-0009 (whole), ADR-0015
   (`:61-63`) are normative and must be amended/superseded in one new ADR
   before code deletion, or the repo documents a substrate it no longer has.
   Then `README.md`, `CHANGELOG.md`, `AGENTS.md:147`,
   `docs/guides/*.mdx`, `docs/catalog/toron-mcp.json` (regenerate),
   `workflows/docker-upgrade.yaml:50-54,73`.
10. **Delete the corpses.** `tests/foundation_test.rs`, `tests/recovery_test.rs`, local `resonate.db*` — nothing depends on them.

**Cannot remove yet:** graph workflows (step 2), the run-path cutover that
makes the host the real engine (step 5), cron cadence (step 3), and the
flywheel build (step 4). Steps 1–5 are the replacement path, not cleanup.

## 8. Where code disagrees with "the in-house engine replaces Resonate"

| Claim | Evidence |
|---|---|
| The replacement engine is the shipped run path | `workflow_host.rs:1092` builds `WorkflowEngine::remote_*`; the embedded arm at `:1114` also builds a Resonate runtime. `durable_host.rs` exists (landed `7d9a9e7`) but is declared only at `engine/mod.rs:36` and called only by `tests/durable_host.rs` — **no** production caller |
| Graphs are on the new engine | `durable.rs:347-349` returns `Unsupported` for any `graph:` workflow; `tests/durable_host.rs:322` asserts that refusal |
| Cron runs on the in-house kernel | `cron_elm.rs:6` calls itself "the replacement path", but `ElmCron` has one caller (its test). `CronManager` also has zero production callers — production cron is flywheel's |
| The framework is a reproducible vendored pin | Resolved 2026-11-02: the captain removed the vendoring decision. ADR-0040 §5 now records the live sibling `path` deps in `crates/toron-core/Cargo.toml`; the nonexistent `vendor/` + verify script are retired from the record and the workspace exclusion is gone |
| SC-11 is CI-gated (ADR-0003 :960) | `.github/workflows/ci.yml:89` sets `continue-on-error: true` on the only job that installs resonate, so a red SC-11 cannot block a merge |
| ADR-0009 names the selection API | ADR says `resonate_server_url_with` / `resonate_server_url`; code has `resonate_choice_with` / `resonate_choice` (`engine/mod.rs:74,106`) |
| A `RESONATE_URL` pin belongs in the launchd plist (`engine/mod.rs:68`) | `toron daemon install` writes no such variable: `daemon.rs` contains 0 matches for `resonate` |
| A green suite witnesses durability | 24 live-server guards `return` early and pass; 11 of 12 affected test files have no `#![cfg(feature)]` gate, so `cargo test` is green on a host that never ran a server |
