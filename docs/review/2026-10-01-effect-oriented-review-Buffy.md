# Effect-oriented review — toron

Reviewer: Buffy. Date: 2026-10-01. Branch `main` @ `c4e54aa`.
Lens: `~/.agents/skills/effect-oriented/SKILL.md` (revised 2026-10-01) plus the
`resonate` durable-execution skills. Read-only pass: no code changed here.

Scope: `crates/toron-core` (policy, mailbox), `crates/toron-workflow`
(engine/*, cron), `crates/toron-mcp` (send_armor). Every claim below cites a
file:line measured today.

**Verdict: the strongest effect-compliance surface in the stack.** Policy is
values, the four outcomes stay apart, intent is journaled before the provider,
and the clocks that matter are injected. Two gaps, both filed: `bd-35el`
(jitter seeds itself from a clock read inside math documented as pure) and
`bd-ncgv` (a Debug impl propagates mutex poison). The cancellation story for
`run_parallel` (`bd-7n2a`) and the ADR-0034 status drift (`bd-eqwq`) were
already filed on 2026-10-01.

## What holds (principle to evidence)

- **Policy is a value, not a literal** (principles 4, 10).
  `crates/toron-core/src/policy.rs` carries `Timeout { deadline, poll }` and
  `RetryPolicy { max_attempts, base, factor, max_delay, jitter }`; the delay
  math is pure `delay_for`/`should_retry`; only the two drivers sleep
  (`policy.rs:79` sync, `policy.rs:100` async). "Timeout inside, retry
  outside" is the doc's own order.
- **The four outcomes stay apart** (principle 2). `engine/park.rs` gives a
  park its own arm (`ParkDecision`, `EffectState::Parked`) with the module doc
  spelling out why mapping it onto `Settled` or `Failed` would lie; the park
  is interruption, not success and not a typed failure. `engine/reconcile.rs`
  maps timeouts and unparseable responses to `Ambiguous → unknown` with the
  invariant "a timeout is never proof of absence". `send_armor.rs` keeps
  `Ambiguous` (may have happened) apart from `BreakerOpen` (refused without
  touching the inner service).
- **Intent is journaled before the provider** (principle 15).
  `engine/boundary.rs`: `invoke` calls the dispatch closure exactly once; a
  truly effectful sink with no injected runner is either refused
  (`EffectJournalMode::Enforced`) or loudly marked
  (`EFFECT_JOURNAL_BYPASSED` / `EFFECT_JOURNAL_SINK_UNCOVERED`). The park's
  re-drive key derives from `(run_id, node, until_ms)` and the intent row is
  journaled before the park, so a crash leaves a visible `Parked` intent.
- **One retry coordinator** (principle 4). `boundary.rs` says "This module
  never retries"; retry lives in the one engine/host coordinator, and the lint
  refuses nested retry (prior review, `control_flow.rs:345-358`,
  `lint.rs:673-679`).
- **The scope owns children; waits are bounded** (principles 6, 10). Park is
  capped at `PARK_MAX_WAIT_SECS = 24h` and refuses rather than truncates;
  fan-out is capped (`MAX_PARALLEL_FANOUT = 32`, loud above the cap);
  reconciliation batches at 500; the send armor bounds one attempt at 30s and
  concurrency at 16; the breaker resets after 30s.
- **The breaker is the skill's stack order** (principle 4). Fallback outer,
  breaker, bulkhead, timeout inner (`send_armor.rs` module doc), one probe in
  flight in `HalfOpen`, and a `ClockFn` so tests drive recovery with no real
  sleeps (`bd-mbck`). Retry is deliberately absent with a named reason: a wire
  send is not idempotent, so retry lives in the journaled attempt planner
  where a same-key retry attaches to the stored outcome instead of re-firing.
- **Substitution tests** (principle 8). The breaker's six-scenario matrix runs
  on deterministic fakes; park's decision is pure over `(until_ms,
  max_wait_secs, now_ms)`; `control_flow_parity` scripts degradation cases so
  a parallel node never reaches the runner raw (prior review).
- **Parse once** (principle 11). `action_contract.rs` refuses a park without
  `max_wait_secs > 0` ("an uncapped park is an unbounded wait");
  `boundary.rs` partitions and validates before anything dispatches.
- **The reconciler is a pure fold step** (principle 13). `reconcile.rs`:
  "No I/O, no clock reads; the embedding supplies the view."

## Findings

### F1 — policy jitter reads the clock inside delay math documented as pure (`bd-35el`, P2)

`crates/toron-core/src/policy.rs:107-110`. The module doc says "delay math
stays pure so tests substitute without sleeping real seconds", and the
`ponytail:` note already names the deterministic alternative, but
`apply_jitter` seeds its xorshift from `Instant::now().elapsed()`. When
`jitter: true`, `delay_for` is nondeterministic and only a band can be
asserted (`jitter_stays_in_band`). This is the ambient-clock class the
effect-oriented skill bans ("Time, randomness, and environment are services,
not hidden reads inside the domain"; the resonate skills call wall-clock use
in control flow an anti-pattern). Fix shape: take entropy as data (a
`jitter_seed` filled once at the edge, or a `delay_for(attempt, entropy)`
parameter). Same class as flywheel `bd-0fvb`, now closed there.

### F2 — `VerifySink` Debug propagates mutex poison (`bd-ncgv`, P3)

`crates/toron-workflow/src/engine/sinks.rs:85`. `.lock().unwrap()` inside a
`Debug` impl: a poisoned lock turns a diagnostic formatting path into a
second panic. The repo's own F5.POISON convention (docs/review/findings.md)
fixed every other production lock to `unwrap_or_else(PoisonError::into_inner)`
(see the breaker's Debug at `send_armor.rs:139` for the shape). One line.

## Observations (not defects)

- **Edge clock reads done right**: `mailbox/mod.rs:383-395` and `cron.rs:420`
  read the clock at the host edge and hand `Instant`/timestamps to pure
  policy functions; the cron checkpoint re-derives `last_fired` from recorded
  tick results. This is the shape F1 should have taken.
- **`run_parallel` cancellation**: no sibling-cancellation path and no
  partial-failure test (`bd-7n2a`, filed 2026-10-01).
- **ADR-0034 status drift**: the file reads `status: proposed` while all
  seven gaps landed (`bd-eqwq`, filed 2026-10-01).

## Sources

- `~/.agents/skills/effect-oriented/SKILL.md` principles 2, 4, 6, 8, 10, 11,
  13, 15 and the anti-patterns list.
- `~/.agents/skills/resonate/durable-execution/SKILL.md` anti-patterns
  (wall clock in control flow; idempotency on external calls).
- Evidence commands: `rg -n "Instant::now|SystemTime::now" crates/` and
  per-file reads listed above, all run 2026-10-01 against `c4e54aa`.
