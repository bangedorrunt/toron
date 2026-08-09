# ADR-0008 Review Findings Table

Ranked findings across P0–P6 tracks. Fed into the batched fix pass (captain decision 4:
collect all findings, then batch-fix RED→GREEN). **No code edits until the full table is
reviewed and the captain approves.**

| ID | Track | Site (file:line) | Invariant | Verified | Risk | Fix-class |
|----|-------|------------------|-----------|----------|------|-----------|
| F2.NIP40 | P0 crypto | `crates/toron-core/src/mailbox/unwrap.rs:602-656` (`UnwrapPipeline::project`) | NIP-40 `expiration` honored on unwrap ingest — expired gift wrap rejected BEFORE sqlite/git projection | **RED** — no TTL check exists | **P0 crypto** — expired-but-never-seen gift wrap is ACCEPTED + projected, voiding ADR-0003 "Parity Superiority" replay/expiry claim | TTL check in unwrap pipeline (parse `expiration` tag, reject-if-past pre-write). RED test `expired_gift_wrap_rejected_on_unwrap` first (see `bd-p3-nip40-expiration-fix-6ezx`) |
| F3.COALESCER-LOCK | P1 concurrency | `crates/toron-mcp/src/workflow_host.rs:156-165` (runner FlushNow arm) + `:441-461` (trigger-loop tick flush) | std-Mutex guard on `MentionCoalescer` NOT held across the blocking publish (`publish_coalesced_job` → `thread::scope` + `rt.block_on(bus.publish)`) | **RED** — guard IS held across the publish; concurrent flush stalls for the publish duration (measured 498ms stall on 600ms publish) | **P1 stall** — any slow publish (relay latency/backpressure) freezes every other coalescer accessor (all `dispatch_job` actions + tick flushes) for the publish duration; poison risk (`expect`) covered by p1-poison-on-panic-audit | Drop the guard before publishing: clone the `Coalesced` under the lock, release, then publish. RED test `coalescer_lock_not_held_across_blocking_publish` authored + failing today |
| F4.S8 | P0 gate | `crates/toron-mcp/src/resources.rs:163-186` (`static_resource`) | S8 honest-stub gate: every placeholder field names a live tool via `status`; all 25 resource URIs resolve to non-empty JSON | **FIXED** — Rev-1 gate tightened + enforced by tests (metrics_core + schemas placeholders lacked `status` → added) | low (honesty/documentation) | Gate now mechanical: placeholder ⇒ `status` required; 25-URI coverage tests (`static_stubs_name_live_tool`, `all_25_resources_resolve_to_non_empty_json`) |
| F5.POISON | P1 recovery | `crates/toron-mcp/src/workflow_host.rs:153-159,441-454` (coalescer) | ADR-0007 D7: a panic inside a std-Mutex guard must not brick the service (poison recovered, not propagated) | **FIXED** — both production `.lock().expect("coalescer lock")` sites → `unwrap_or_else(\|e\| e.into_inner())`; 0 poison-propagating prod sites remain | P1 (was: panic-in-panic = dead service) | RED→GREEN `poisoned_coalescer_survives`; per-site table in F5.POISON section |

## F2.NIP40 — NIP-40 expiration NOT honored on unwrap ingest path (P0)

**Probe:** `bd-p0-nip40-ingest-probe-oyaa` (2026-08-09). Verdict: **RED**.

### Evidence (every ingest layer traced, no TTL check found)

1. **Unwrap pipeline** — `UnwrapPipeline::project`, crates/toron-core/src/mailbox/unwrap.rs:602-656.
   Flow: seen_wraps guard → `try_unwrap` → dedup check → G14 policy gate → ack cache →
   `sink.write`. No NIP-40 parse, no reject-if-past, no `expiration` mention anywhere in
   the module.
2. **Unwrap crypto** — `gift_wrap::unwrap`, crates/toron-core/src/nostr/gift_wrap.rs:88-118.
   Kind check → decrypt seal → verify seal → decrypt rumor → NIP-59 sender-mismatch.
   No TTL check.
3. **Bus live publish** — `InProcessBus::publish`, crates/toron-core/src/bus/in_process.rs:203-255.
   Pipeline: Verify → Auth → Dedup → Fanout. No expiration stage.
4. **Bus live fanout** — `InProcessBus::fanout`, crates/toron-core/src/bus/in_process.rs:342-374.
   Access-gated fanout only; no TTL filter.
5. **Only enforcement that exists: bus REPLAY + GC** —
   `BusEventStore::replay`, crates/toron-core/src/bus/event_store.rs:102-108
   (`WHERE (expires_at IS NULL OR expires_at > ?1)`), prune at :139. Covers the
   subscribe-time snapshot only; a LIVE-published expired wrap bypasses it and is
   delivered to the unwrap loop.

### Consequence

An expired gift wrap published to the bus while the unwrap loop is subscribed is
delivered → unwrapped → projected to SQLite + Git. ADR-0003 rev6's "NIP-40 expiration
TTL honored" (replay/expiry Parity Superiority row) holds for replay but not for the
live ingest path.

### Fix (deferred to `bd-p3-nip40-expiration-fix-6ezx`)

1. RED: `expired_gift_wrap_rejected_on_unwrap` — gift wrap with `expiration` tag in the
   past → pipeline rejects (no sqlite/git write). Confirmed to fail today.
2. GREEN: TTL check in unwrap pipeline before `sink.write` (parse `expiration` tag,
   compare vs now, reject-if-past).
3. REFACTOR: extract named helper.

## F3.COALESCER-LOCK — std-Mutex guard held across blocking publish (P1)

**Probe:** `bd-p1-std-guard-across-blockon-gwnt` (2026-08-09). Verdict: **RED**.

### Evidence (audit sweep: `lock()` sites within ±10 lines of `block_on`/`thread::scope`/`spawn_blocking`/`.await` across all crates)

1. **Runner FlushNow arm** — crates/toron-mcp/src/workflow_host.rs:156-165
   (`mailbox_runner`): `coalescer.lock().expect("coalescer lock")` held while
   `publish_coalesced_job` runs — that fn (workflow_host.rs:190-221) blocks on
   `std::thread::scope` + `rt.block_on(bus.publish)`.
2. **Trigger-loop tick flush** — crates/toron-mcp/src/workflow_host.rs:441-461
   (`run_trigger_loop`): `self.coalescer.lock().expect("coalescer lock")` held
   across the same blocking publish, once per flushed assignee.
3. **Siblings classified SAFE (not this class):** `cron.rs:169`,
   `engine.rs:497/820`, `index/writer.rs:145/152` = tokio (async) Mutex —
   safe across `.await`. `unwrap.rs:460/473/534/649`, `archive/git.rs:80/99` =
   short-lived sync guard, never held across an await/block. `mock_relay.rs:137`
   + engine `seen.lock().unwrap()` sites = test code.

### Consequence (classification: STALL, not cancel)

Every other coalescer accessor (`dispatch_job` actions + tick flushes on other
assignees, other host tasks) blocks for the full publish duration. Measured in
RED test: a concurrent flush stalled 498ms behind a 600ms publish. With relay
latency/timeouts the stall scales with the publish — the trigger loop itself
freezes mid-batch (per-event event processing halts).

### Fix (deferred to the ADR-0008 batch-fix pass)

1. RED: `coalescer_lock_not_held_across_blocking_publish`
   (crates/toron-mcp/src/workflow_host.rs, test module) — authored, confirmed
   FAILING today (498ms stall > 300ms bound).
2. GREEN: clone each `Coalesced` out of the guard, drop the guard, then
   `publish_coalesced_job` (both sites).
3. REFACTOR: extract a `flush_and_publish` helper with the lock scope explicit.

## F4.LOCK-ORDER — multi-lock deadlock ordering audit (P1) — GREEN

**Probe:** `bd-p1-lock-ordering-deadlock-71po` (2026-08-09). Verdict: **GREEN — no
deadlock cycle.** Lock-order graph documented below (bead deliverable; also the
regression baseline for future multi-lock additions).

### Method

Brace-matched per-function scan of all `crates/*/src` (`fn` blocks ×
`.lock()`/`.read()`/`.write()` sites) → 12 fns acquiring ≥2 distinct locks
(7 test-code, 5 real). Lock-order edges built per fn; DFS cycle check over the
graph. Result: **no cycle**.

### Lock-order graph (documented order)

| Lock pair | Sites (fn : line) | Acquisition pattern | Cycle risk |
|-----------|-------------------|---------------------|------------|
| `projected` → `archive` (GitProjectionSink) | `unwrap.rs::write` :379,383,391 | `projected` guards are temporaries (condition exprs / `insert` stmt); `archive` guard held across stage+commit; NEVER simultaneous | none (no reverse path exists) |
| `policies` → `approved` (BusPolicySource) | `contacts.rs::load_many` :393,408; `contacts.rs::fmt` :361-362 | sequential write guards; `fmt` holds READ+READ simultaneously (shared+shared — never blocks) | none |
| `per_tool` → `latency_hist` (MetricsRegistry) | `metrics.rs::snapshot` :109,121 | NESTED hold: `per_tool` guard held across loop, per-metric `latency_hist` temporary inside | none (only site acquiring both; `record()` :51 acquires `latency_hist` alone) |

### Bead suspects disposition

- mailbox holding bus + index lock: **absent** — bus publish is async (no lock);
  index access is sqlx (no mutex). No multi-lock fn in `mailbox/mod.rs`.
- workflow_host coalescer + bus: `coalescer` is acquired alone at both sites
  (runner arm :156, tick flush :441) — never nested with another lock; the
  blocking-publish hazard is the separate finding F3.COALESCER-LOCK.
- reservations map + expiry: **single lock** — no ≥2-lock fn in
  `reservations.rs` / `build_slots.rs`.
- Siblings (engine.rs `seen_clone`/`seen`, git.rs test, unwrap.rs test
  `archive` guards): test code, sequential/temporary — no production hazard.

### Fix required

None today. If a future change adds a second lock to a multi-lock fn, it MUST
acquire in the documented order above (projected→archive, policies→approved,
per_tool→latency_hist) or the graph cycle check re-runs.

## F5.POISON — poison-on-panic survivability audit (P1) — FIXED GREEN

**Probe:** `bd-p1-poison-on-panic-audit-jwpd` (2026-08-09). Verdict: **RED →
GREEN (fixed inline)**. The only poison-propagating std Mutex in production
was the `MentionCoalescer`; recovered via `PoisonError::into_inner`.

### Per-site verdict table (all 122 `.lock()` sites enumerated; std-Mutex subset)

| Site (file:line) | Lock type | Guard recovery (pre-fix) | Verdict | Disposition |
|------------------|-----------|--------------------------|---------|-------------|
| `workflow_host.rs:156` (runner FlushNow arm) | `std::sync::Mutex<MentionCoalescer>` | `.expect("coalescer lock")` → **propagate** | **RED** (shared state, poison bricks every flush) | **FIXED** — `unwrap_or_else(\|e\| e.into_inner())` |
| `workflow_host.rs:441` (trigger-loop tick flush) | same | `.expect("coalescer lock")` → **propagate** | **RED** (shared state) | **FIXED** — same recovery |
| `workflow_host.rs:686` (test `coalesced_burst...`) | same | `.expect("lock")` | test code, same pattern | **FIXED** for uniformity |
| `unwrap.rs` projected/seen_wraps/archive (all) | `parking_lot::Mutex` | n/a (parking_lot never poisons) | SAFE | — |
| `git.rs` archive/committed | `parking_lot::Mutex` | n/a | SAFE | — |
| `contacts.rs` policies/approved | `parking_lot::RwLock` | n/a | SAFE | — |
| `metrics.rs` per_tool/latency_hist | `parking_lot::Mutex` | n/a | SAFE | — |
| `bus/in_process.rs` subs | `parking_lot::RwLock` | n/a | SAFE | — |
| `engine.rs` approval/spawn sinks | `tokio::sync::Mutex` | n/a (tokio never poisons) | SAFE | — |
| engine.rs `seen`/`seen_clone` (test) | std (test `Mutex<Vec>` in engine tests) | `.unwrap()` | test code — a panic there fails the test, not the service | — |

### RED test + fix

- RED: `poisoned_coalescer_survives` (crates/toron-mcp/src/workflow_host.rs) —
  poisons the coalescer (panic inside the guard via catch_unwind), then runs
  the REAL runner DispatchJob arm; pre-fix the `.expect()` panicked
  (panic-in-panic = dead service) — confirmed failing. Post-fix: recovers,
  test GREEN.
- Fix: `unwrap_or_else(|e| e.into_inner())` at all 3 coalescer lock sites
  (ADR-0007 D7 recovery posture; parking_lot switch rejected — bigger dep
  change for the same result, captain decision 5 allows either).
- Workflow host module: 9/10 green (only the batch-deferred F3.COALESCER-LOCK
  RED test fails); clippy clean.

## S1/S2/S4/S5/S6/S7/F2.KDF — Re-verification GREEN (bd-p0-verify-green-claims-8z3p)

**Probe:** 2026-08-09, live tree (no code modifications). All 7 claims re-verified GREEN
at execution time. Actual output lines below.

| Claim | Test (file:line) | Actual output (2026-08-09) | Verdict |
|-------|------------------|-----------------------------|---------|
| S1 | `serve_wire_gift_wrap_fires_message_posted_once` (crates/toron-mcp/src/server.rs:446) | `test server::tests::serve_wire_gift_wrap_fires_message_posted_once ... ok` — `1 passed; 0 failed` (51.14s) | GREEN |
| S2 | `trigger_filter` module (crates/toron-workflow/src/trigger_filter.rs:161) + trigger/filter tests across toron-workflow | `trigger_filter::tests` 11/11 ok (`all_job_kinds_excluded`, `all_workflow_run_kinds_excluded`, `workflow_emitted_tag_rejects_kind_14`, `dispatch_job_output_cannot_loop`, `self_send_message_workflow_cannot_loop`, `tag_check_is_specific`, `gift_wrap_excluded`, `filter_operates_on_wire_events`, `kind_predicate_consistent_with_event_predicate_for_kinds`, `range_helpers_correct`, `legitimate_user_kinds_pass`); plus engine/schema/lint/cron trigger tests — toron-workflow lib `124 passed; 0 failed` | GREEN |
| S4 | id args all String — `AcknowledgeMessageArgs.message_id` (tools.rs:1512/1519), `MarkMessageReadArgs.message_id` (tools.rs:1523/1527), `ForceReleaseFileReservationArgs.file_reservation_id` (tools.rs:1607) | Audit: all three `String`, no i64/u64. **Note:** `RenewFileReservationsArgs.file_reservation_ids: Option<Vec<i64>>` (tools.rs:1590) + `ReleaseFileReservationsArgs` (:1598) are dead AM-parity selectors — documented as NOT part of the toron surface and ignored (crates/toron-core/src/mailbox/surface.rs:427, F11 honesty gap; paths-only is the truthful superset). S4 substance holds. | GREEN (note) |
| S5 | `foreach_over_list_collects` (crates/toron-workflow/src/engine.rs:2041) | `test engine::tests::foreach_over_list_collects ... ok` — `1 passed; 0 failed` | GREEN |
| S6 | `call_workflow_nested_subrun_returns_child_output` (crates/toron-workflow/src/engine.rs:2136) | `test engine::tests::call_workflow_nested_subrun_returns_child_output ... ok` — `1 passed; 0 failed` | GREEN |
| S7 | `coalesced_burst_publishes_one_stamped_43001` (crates/toron-mcp/src/workflow_host.rs:824) | `test workflow_host::tests::coalesced_burst_publishes_one_stamped_43001 ... ok` — `1 passed; 0 failed` | GREEN |
| F2.KDF | `unwrap_loop_kdf_cached_between_rebuilds_many_keys` (crates/toron-core/src/mailbox/mod.rs:540) | `test mailbox::tests::unwrap_loop_kdf_cached_between_rebuilds_many_keys ... ok` — `1 passed; 0 failed` (7.88s) | GREEN |

No regression on any gate. Note: the S2 "19/19" figure from debate time is not a literal
module count — `trigger_filter.rs` has 11 tests plus ~8 trigger/filter tests in
engine/schema/lint/cron modules; the full toron-workflow lib suite (124 tests) is green.
S4's dead `Vec<i64>` reservation-id selectors are tracked for the batch-fix pass (surface
doc says ignored; candidates for removal or honest schema amendment), but they do not
violate the S4 claim (the claim's three named args are String).

## F4.S8 — S8 honest-stub gate tightened + 25-URI coverage (bd-p0-s8-resources-stub-gate-rrfz)

**Probe:** 2026-08-09. Verdict: **FIXED in bead** (gate ambiguity resolved; 2 placeholders
lacking `status` found and repaired).

### Evidence (Rev-1 gate wording applied to resources.rs)

1. **`static_resource` (resources.rs:163-186)** serves 7 static tooling URIs.
   - `metrics_core` (`toron://tooling/metrics_core`): fields `"http": "not started"`,
     `"database": "not wired"`, `"storage": "not wired"` — honest values BUT **no `status`
     field naming the live tool** → RED under the Rev-1 gate (the charter's old
     "honest structured stub" wording let it pass without a rule).
   - `schemas` (`toron://tooling/schemas`): `{"note": "see tools.schema.json fixtures"}`
     — placeholder pointer **without `status`** → RED under the Rev-1 gate.
2. Data-backed templates (18) + discovery stubs already carry `status: live_via_tools`
   naming the live tools (`list_agents`, `fetch_inbox`, `search_messages`,
   `health_check`, …) → compliant.

### Fix (in this bead)

- `metrics_core` → `"status": "live via health_check / diagnostics tools"` (honest: the
  values stay "not wired" — the stub names the live path, per oc A1.2 honesty-over-cosmetics).
- `schemas` → `"status": "live via MCP tools/list schema introspection"`.
- Gate enforced by tests: `static_stubs_name_live_tool` (placeholder ⇒ `status` required),
  `all_25_resources_resolve_to_non_empty_json` (all 25 templates → non-empty JSON, stubs
  name the live tool), `static_resources_serve_json` extended to all 7 static URIs,
  `data_backed_resources_project_mailbox_data` extended to reservations/slots/identity/
  outbox/mailbox projections.
- Result: 8/8 resources tests green; toron-mcp acceptance 5/5 green (245s; first run had a
  flaky failure, clean re-run).
- No remaining placeholder without a `status` field; nothing faked to look real.

## F5.POISON — Poison-on-panic survivability audit (bd-p1-poison-on-panic-audit-jwpd)

**Probe:** 2026-08-09. Verdict: **FIXED** — 2 production poison-propagating std-Mutex sites
repaired; 0 remaining.

### Per-site verdict table (all std::sync::Mutex/RwLock — the only poisoning types)

| Site | Lock | Acquisition | Verdict |
|------|------|------------|---------|
| `toron-mcp/src/workflow_host.rs:153-159` (runner arm) | std `Mutex<MentionCoalescer>` | `.lock().expect("coalescer lock")` → **FIXED**: `.lock().unwrap_or_else(\|e\| e.into_inner())` | RECOVERED (poison re-init) |
| `toron-mcp/src/workflow_host.rs:441-454` (tick flush) | std `Mutex<MentionCoalescer>` | `.lock().expect("coalescer lock")` → **FIXED**: `.lock().unwrap_or_else(\|e\| e.into_inner())` | RECOVERED (poison re-init) |
| `toron-cli/src/operator.rs:592-630` (4 sites) | std `Mutex` (state) | `match state().lock() { Err(p) => p.into_inner() }` | RECOVERED (pre-existing) |
| parking_lot Mutex/RwLock — 12 files (unwrap.rs, in_process.rs, git.rs, writer.rs, herdr.rs, mock.rs, mock_relay.rs, presence.rs, contacts.rs, dispatch.rs, mailbox/mod.rs, +1) | non-poisoning | — | SAFE by construction (no PoisonError exists) |
| tokio::sync::Mutex (sc16 SpikeSink) | non-poisoning | — | SAFE by construction |
| engine.rs test module (9 std Mutex, 17 `.lock().unwrap()` sites, `StdMutex` alias, lines 1763-2386) | std | unwrap | TEST-ONLY — panic = test failure, not service death |
| e2e tests sc21 (3), sc24 (4), cli_robot/cli_tui (1 each), cli_operator OnceLock<Mutex<()>> | std | unwrap | TEST-ONLY scaffolding |
| guard.rs:379 `index.write().expect(...)` | git2 Index (NOT a lock) | — | FALSE POSITIVE (repo.index().write(), test) |

### RED→GREEN

- RED mechanism: std Mutex poison → `.lock().expect()` panics with PoisonError on every
  subsequent access (panic-in-panic = dead service). The charter S7 flag (coalescer
  `.expect`) was the only production instance.
- GREEN test `poisoned_coalescer_survives` (workflow_host.rs:1207): poisons the coalescer
  via `catch_unwind` panic inside the guard, asserts `is_poisoned()`, then runs a REAL
  `DispatchJob` through the runner arm — must not panic. **Passes** (0.01s).
- 3/4 coalescer-group tests green; the 1 failure is `coalescer_lock_not_held_across_blocking_publish`
  — F3.COALESCER-LOCK's intentional RED test (owned by bd-p1-std-guard-across-blockon-gwnt).

### Notes

- `PoisonError`-recovery (`unwrap_or_else(|e| e.into_inner())`) chosen over parking_lot
  migration — 2 sites, no dep-tree change, honest re-init (captain decision 5 weighing).
- The guard-hold-across-publish stall (F3) is tracked separately; the poison recovery here
  removes the *brick* risk on top of it.

## F1.LAG-SWALLOW — RelayBus broadcast lag silently dropped (P1, bd-p1-broadcast-lag-handling-6wm8)

**Probe:** 2026-08-09. Verdict: **RED** (swallow confirmed) → **decision: SURFACE**.

### Evidence

`crates/toron-core/src/bus/relay_bus.rs:196`:
`Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue` — the per-subscription
bridge task drops lag notifications with no log, no metric, no signal to the subscriber.

- The default in-process bus is per-subscriber mpsc (`in_process.rs:13`), so lag is
  impossible there; `RelayBus` (nostr-sdk Client over WS, broadcast channel cap 1024) is
  the ONLY lossy path and the production federation surface (captain decision 2).
- A dropped notification can be a workflow trigger / mailbox event; silent loss violates
  the ADR-0003 parity/durability posture ("agent-automation context budget is scarce" —
  ADR-0004 driver: a missed trigger is worse than a loud log).

### Decision

**Surface** — warn log with skipped count + ops metric (AtomicU64 counter on `RelayBus`,
exposed via `health()` message). Re-sync (auto re-subscribe) NOT added: sustained lag +
auto-re-REQ could storm relays; operator acts on the metric. Documented best-effort on the
federation path.

### RED test (authored, confirmed failing — fix in batch per captain decision 4)

`test_broadcast_lag_is_surfaced_not_swallowed` (relay_bus.rs tests): two subscriptions on
one RelayBus; sub A drained (keeps producer flowing), sub B undrained (mpsc 256 fills →
bridge blocks → broadcast receiver falls behind → `Lagged` fires deterministically);
1500-event burst > 256+1024 margin. Assertions:
1. Diagnostic: `received_b < published` (lag actually occurred) — PASSES today.
2. Contract: `health().message` contains "lag" — **FAILS today** (silently swallowed).

Verified: test RED on live tree; GREEN-shape confirmed by temporary wiring (counter field +
`Lagged(skipped)` increment + health surfacing) → test passes. Fix restores the metric;
implementation deferred to the batch-fix pass (charter: no code edits until full P0–P6
findings table is reviewed and captain approves).
