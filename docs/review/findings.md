# ADR-0008 Review Findings Table

Ranked findings across P0–P6 tracks. Fed into the batched fix pass (captain decision 4:
collect all findings, then batch-fix RED→GREEN). **No code edits until the full table is
reviewed and the captain approves.**

| ID | Track | Site (file:line) | Invariant | Verified | Risk | Fix-class |
|----|-------|------------------|-----------|----------|------|-----------|
| F2.NIP40 | P0 crypto | `crates/toron-core/src/mailbox/unwrap.rs` (`UnwrapPipeline::project`) | NIP-40 `expiration` honored on unwrap ingest — expired gift wrap rejected BEFORE sqlite/git projection | **FIXED (GREEN)** — `wrap.is_expired()` check at project() head → `ProjectOutcome::Expired`; RED test `expired_gift_wrap_rejected_on_unwrap` + regression `future_expiration_gift_wrap_accepted_on_unwrap` pass (bd-p3-nip40-expiration-fix-6ezx, 2026-08-09) | P0 crypto (fixed) | `ProjectOutcome::Expired` variant + `is_expired()` guard before unwrap/write |
| F3.COALESCER-LOCK | P1 concurrency | `crates/toron-mcp/src/workflow_host.rs:156-165` (runner FlushNow arm) + `:441-461` (trigger-loop tick flush) | std-Mutex guard on `MentionCoalescer` NOT held across the blocking publish (`publish_coalesced_job` → `thread::scope` + `rt.block_on(bus.publish)`) | **RED** — guard IS held across the publish; concurrent flush stalls for the publish duration (measured 498ms stall on 600ms publish) | **P1 stall** — any slow publish (relay latency/backpressure) freezes every other coalescer accessor (all `dispatch_job` actions + tick flushes) for the publish duration; poison risk (`expect`) covered by p1-poison-on-panic-audit | Drop the guard before publishing: clone the `Coalesced` under the lock, release, then publish. RED test `coalescer_lock_not_held_across_blocking_publish` authored + failing today |
| F4.S8 | P0 gate | `crates/toron-mcp/src/resources.rs:163-186` (`static_resource`) | S8 honest-stub gate: every placeholder field names a live tool via `status`; all 25 resource URIs resolve to non-empty JSON | **FIXED** — Rev-1 gate tightened + enforced by tests (metrics_core + schemas placeholders lacked `status` → added) | low (honesty/documentation) | Gate now mechanical: placeholder ⇒ `status` required; 25-URI coverage tests (`static_stubs_name_live_tool`, `all_25_resources_resolve_to_non_empty_json`) |
| F5.POISON | P1 recovery | `crates/toron-mcp/src/workflow_host.rs:153-159,441-454` (coalescer) | ADR-0007 D7: a panic inside a std-Mutex guard must not brick the service (poison recovered, not propagated) | **FIXED** — both production `.lock().expect("coalescer lock")` sites → `unwrap_or_else(\|e\| e.into_inner())`; 0 poison-propagating prod sites remain | P1 (was: panic-in-panic = dead service) | RED→GREEN `poisoned_coalescer_survives`; per-site table in F5.POISON section |

## F2.NIP40 — NIP-40 expiration honored on unwrap ingest path (FIXED, bd-p3-nip40-expiration-fix-6ezx)

**Probe:** `bd-p0-nip40-ingest-probe-oyaa` (2026-08-09). Initial verdict: **RED**.
**Fix landed 2026-08-09 by `bd-p3-nip40-expiration-fix-6ezx` → GREEN.**

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

### Fix (landed in `bd-p3-nip40-expiration-fix-6ezx`)

1. RED: `expired_gift_wrap_rejected_on_unwrap` — gift wrap with `expiration` tag in the
   past → pipeline rejects (no sqlite/git write). Confirmed failing before the fix.
2. GREEN: `UnwrapPipeline::project` head now guards `wrap.is_expired()` (nostr-sdk NIP-40,
   `tags.expiration() < now`) BEFORE unwrap cost and any sink write → returns new
   `ProjectOutcome::Expired`. Regression `future_expiration_gift_wrap_accepted_on_unwrap`
   (expiration +1 day) still projects.
3. REFACTOR: named variant `ProjectOutcome::Expired` (no exhaustive matches elsewhere);
   check lives in one place at project() head. 112/112 mailbox tests + clippy clean.

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

## F6.TOKIO-GUARD — tokio guard held across `.await` audit (P1) — GREEN

**Probe:** `bd-p1-await-while-locked-9a2v` (2026-08-09). Verdict: **GREEN — all
sites intentional or temporary; no deadlock/contention defect.**

### Per-site classification (every `.lock().await` / `.read()`/`.write().await`
site + every tokio Mutex/RwLock instantiation, multiline-aware scan)

| Site (file:line) | Lock | Held across await? | Classification | Contention / cancel-safety |
|------------------|------|--------------------|----------------|----------------------------|
| `index/writer.rs:145` (`execute`) | `IndexWriter.gate` (tokio `Mutex<()>`) | YES — across the sqlx write | **INTENTIONAL** — single dedicated write conn; reads ride the read pool, never block the writer | Serialized writers by design; RAII guard drops on cancel (tx rolled back by sqlx) |
| `index/writer.rs:152` (`execute_many`) | same gate | YES — across begin/query/commit | **INTENTIONAL** — atomic batch serialization | Same as above |
| `cron.rs:169` (`policy_sink`) | `PolicySink` (tokio `Mutex<Vec<PolicyEvent>>`) | NO — sync `push` after acquire; guard drops at statement end | temporary | None |
| `engine.rs:497` (`approval_sink`) | `ApprovalSink` (tokio `Mutex<Vec<String>>`) | NO — sync `push` | temporary | None |
| `engine.rs:820` (`spawn_sink`) | `SpawnSink` (tokio `Mutex<Vec<String>>`) | NO — sync `push` | temporary | None |
| `engine.rs:1350,1447` (`poll_promise_id`/`poll_spawn`, test helpers) | ApprovalSink/SpawnSink | NO — guard dropped right after `pop`; sleep happens BEFORE the lock | temporary | None |

### Deadlock / reentrancy

No nested acquisition anywhere (sinks never accessed inside a sink-guarded
section; `execute_many` never calls `execute`; the gate is the only writer
lock). No reverse-order pair → no cycle. All tokio guards are RAII —
cancellation mid-await drops the guard cleanly (unlike a hypothetical leaked
guard, none exists).

### Regression pin (GREEN — no defect existed)

`concurrent_writers_serialize_no_deadlock` (crates/toron-core/src/index/writer.rs):
8 tasks × 50 writes through the gate under a 30s bound + row-count assertion
(400 landed). Green 3/3; writer module 5/5; clippy clean.

## F7.ZERO-COERCION — unwrap_or(0) coercion audit (P2) — FIXED

**Probe:** `bd-p2-unwrap-or-zero-coercion-5ac3` (2026-08-09). Verdict: **2 real
hazards fixed; 65 benign; id-path (F3 residual) already clean.**

### Count-conservation (67 sites audited = 67 classified)

| Class | Count | Verdict |
|-------|-------|---------|
| Clock fallbacks (SystemTime pre-epoch, unreachable) — `now_us`/`now_ms`/`unix_now` (slots.rs:221, writer.rs:34, proof.rs:298, cron.rs:197, workflow_host.rs:242, index/projection.rs:90 ts_ms) | 6 | benign invariant |
| Counter/lookup defaults (empty list/count = absent state) — dispatch.rs:117/128, coalesce.rs:357, engine.rs:149-157, channels.rs:176, engine.rs:660, unwrap.rs:467, resources.rs:584 | 10 | benign invariant |
| Display/metadata fallbacks (Option<String> → "") — identity.rs:152-154/315, messaging.rs:298/671, mod.rs:348/357, unwrap.rs:476/501, surface.rs:183, herdr.rs:759, reservations.rs:404/468/545/882, blob.rs:67/139, product.rs:64/142, operator.rs:207/272/522/688, workflow.rs:84/108, main.rs:322, tools.rs:624/692/693/718/719/935/958, workflow_host.rs:92/121, guard.rs:161, infra.rs:97, blob_serve.rs:162, mock_relay.rs:135, channels.rs:176 | 30 | benign (serialization of String is infallible; absent tags = legit state) |
| Test code | 5 | engine.rs:1541/1756 asserts, resources.rs:422 pin, proof.rs:383/630 fixtures | benign |
| **Negative i64→u64 TTL wrap** — tools.rs:911/936/1090/1180/1309/1333 (`ttl_seconds`/`extend_seconds`/`file_reservation_ttl_seconds`) | 6 | **HAZARD — FIXED** via `nonneg_secs` helper (negative → None → default TTL / no-op extension); RED unit test `nonneg_secs_rejects_negative_wrap` |
| **SQL error mask** — resources.rs:422 `index_scan` | 1 | **HAZARD — FIXED** (`Result` propagation + 4 views log + return None instead of fabricating empty); RED test `index_scan_db_error_propagates_not_masked` (closed-pool) |
| id-path parse→0 (F3 residual) | 0 | already clean — no hex-id `parse().unwrap_or(0)` anywhere (F3/D8.2 landed) |
| proof.rs:84 `expires_at.unwrap_or(0)` binding input | 1 | benign — None and Some(0) hash identically, but Some(0) is rejected at the expiry check (:242); no exploit path |
| **Total** | **67** | 65 benign + 2 fixed |

### Fixes

1. `nonneg_secs(Option<i64>) -> Option<u64>` (tools.rs): `-1 as u64` ≈ 1.8e19 s
   previously produced astronomical leases/extensions on renew+acquire
   (reservations + build slots + file-reservation TTL). Negative → None →
   default TTL (90d) or no-op extension.
2. `index_scan` → `Result<Vec<SearchMessage>, sqlx::Error>`; outbox/message/
   thread/overdue views log + return None on DB failure (resource not served)
   instead of fabricating "0 messages" while the index is down.

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

## F2.SILENT-DROP — Silent Result/Option drop audit (P2, bd-p2-silent-result-drop-99rr)

**Probe:** 2026-08-09. ast-grep `let _ = $X;` (306 raw matches) + `.ok();` (1) + bare-call
check, filtered to non-test src. **31 real src sites classified, all enumerated below.**
Count-conservation: 306 raw = 275 test-module + 31 src (feeds bd-p2-count-conservation-v3ey).

### Classification (31/31, exact)

| Class | Count | Sites (file:line) | Rationale |
|-------|-------|-------------------|-----------|
| **DELIBERATE** — best-effort bus send (receiver-optional) | 6 | unwrap.rs:250 (documented trigger notify), relay_bus.rs:115, relay_bus.rs:197, presence.rs:97, mock.rs:73, in_process.rs:301 (EOSE) | Drop-on-full is the designed lossy path for these channels; unwrap.rs:251 has an explicit reason comment |
| **DELIBERATE** — error propagates via `?`, only Ok value dropped | 3 | relay_bus.rs:95, reconcile.rs:112, product.rs:351 | `let _ = ...?` surfaces errors; value genuinely unused |
| **DELIBERATE** — documented fire-and-forget | 3 | cron.rs:324 (comment cites durable-run terminal events), main.rs:153 (bench, comment cites timing-only semantics), operator.rs:597 (metrics persist — snapshot loss acceptable) | Explicit reasons in code |
| **DELIBERATE** — stale temp cleanup | 5 | operator.rs:338, 344, 359, 376, 377 | Best-effort file cleanup |
| **DELIBERATE** — suppress-unused in flow | 2 | product.rs:80, macros.rs:145 | Side effect already executed; silences unused-var |
| **DELIBERATE** — enum/unit discard | 1 | cron.rs:183 (`FailureAction::Log` — actions are the embedding's to perform) | Comment explains |
| **DELIBERATE** — builder/void-io | 7 | main.rs:49 (print_help), server.rs:298 (ensure_operator_key_mode), guard.rs:158, 239-242 (rhai ctx set_value/set_function) | Builder setup; missing binding degrades the sandbox only |
| **ACCIDENTAL** — silent failure (fix in batch) | 4 | relay_bus.rs:111, reservations.rs:409, reservations.rs:573, reservations.rs:574 | See below |

### ACCIDENTAL sites (fix in batch per captain decision 4)

1. **relay_bus.rs:111** — `let _ = client_for_task.handle_notifications(...).await` — the
   federation notification PUMP result is dropped. If the pump errors/exits, every relay
   subscription goes dead SILENTLY (no log, no health signal). Fix: log error + surface in
   health.
2. **reservations.rs:573-574** — `record_transition` drops `archive.stage_file` +
   `archive.commit` — the G13 audit-history write can silently fail (unwritable archive,
   git error), losing the acquire/release/revoked record with no signal. Fix: log warn on
   error (fn stays fire-and-forget but not silent).
3. **reservations.rs:409** — `let _ = self.publish_dm(&wrap).await` — force-release holder
   NOTIFICATION DM result dropped. If the DM fails, the force-released holder is never told.
   Fix: log warn on error.

### Notes

- No bare `fn_call();` returning Result/Option exists in src (cargo check clean → must_use
  would flag them); the silent class is exactly the explicit `let _ =` sites above.
- `.ok();` in src: 1 site (rebuild.rs:319) — test-module only, not counted.
- All 4 accidental fixes are log-or-surface changes (no control-flow reshuffle); no RED
  test authored — behavior unchanged, observability added. Batch-fix candidates.
