# ATC Learning Loop — Research Note (bd-nyg, ADR-0004 A5)

// governed-by: ADR-0004, ADR-0005

> **Status:** RESEARCH EPIC (priority 3, deferred). Not scheduled for build.
> Picks up when agent-automation quality is the binding constraint.
> Canonical bead: **bd-nyg**. Source: study doc
> `docs/notes/2026-08-07-buzz-agentmail-comparison.md` item A5/11.

## What this is

Agent Mail's long-term differentiator: an `atc_experiences` table records every
tool-call **decision** + **outcome**, a calibration tick loop periodically recomputes
routing/conflict/liveness policy from accumulated experiences, and outcome feedback
closes the loop. toron has **no learning** — every tool call is stateless w.r.t.
quality. This note scopes what a toron-native ATC loop would look like, borrowing the
**framework** (experience row + outcome) without Agent Mail's specific math.

## The spiritual analogue (reuse this pattern)

hachiko's calibration work — ADR-0040/0050 `CalibrationTracker`, `ForecastRecorder` —
has the exact shape toron needs:

1. **Record a prediction** (calibration record / forecast): what the system expects to happen.
2. **Resolve the outcome** (recalibrate / outcome record): what actually happened.
3. **Update the model** (tick): recompute the governing parameters from the deltas.

toron's ATC should be a straight port of that three-step shape, with the "prediction"
being a tool-call decision and the "outcome" being the observable success/failure.

## Proposed experience-row schema

New SQLite table `atc_experiences` (in `crates/toron-core/migrations/0002_atc.sql`,
mirroring the `0001_schema.sql` style):

| Column | Type | Purpose |
|---|---|---|
| `id` | INTEGER PK | local sequence |
| `decision_id` | TEXT | unique decision id (UUID) — survives retries |
| `tool` | TEXT | tool name (38-tool surface, verbatim) |
| `args_hash` | TEXT | stable hash of the tool's JSON args |
| `agent` | TEXT | agent name |
| `project` | TEXT | project slug |
| `outcome` | TEXT | `ok` / `error` / `timeout` / `rejected` |
| `latency_us` | INTEGER | elapsed µs (matches toron's `*_ts_us` convention) |
| `ts` | INTEGER | wall clock (µs) |
| `metadata` | TEXT | JSON: error kind, retry count, decision signal |

## Outcome feedback path (when does a tool call "succeed"?)

The hard design question. toron has natural outcome signals to tie into:

- **`send_message`/`reply_message`** → success = event stored + published on the bus
  (the current `Ok(stored)` return). Failure = bus rejection.
- **`fetch_inbox`** → success = response returned; latency measured.
- **`file_reservation_paths`** → success = `granted` non-empty; **outcome resolved
  later** by `release_file_reservations` (did the holder actually release?) or by
  `check_file_reservation_conflicts` (did a conflict surface?).
- **`acknowledge_message`** → outcome = the ack round-trip completed.
- **`acquire_build_slot`** → outcome = slot released cleanly or expired (TTL).
- **`register_agent`/`whois`** → outcome = the event landed on the bus.

Recommendation: **two-phase outcome**. Phase 1 = synchronous `ok`/`error` at call time
(after the tool returns). Phase 2 = deferred resolution where a later tool call or a
timeout closes the loop (reservation release, slot expiry, ack overdue). Phase 2 is
what distinguishes ATC from a plain log.

## Calibration tick

Periodic (cron/`tokio::interval`) recompute reading `atc_experiences`:

- Per-tool error rate + latency percentile → routing hints (which backend / relay to
  prefer).
- Per-(tool, agent) success rate → whether to flag an agent's tool usage for review.
- Conflict/liveness math (ADR-0004 G14 policy) tuned from observed outcomes, e.g. the
  `block_all` enforcement tuning, build-slot TTL policy.

NOT trading math. Keep the model trivial (EWMA of outcome + latency per tool) until
evidence justifies more.

## Storage

- `atc_experiences` table in the existing SQLite index (a new migration, additive).
- Optional Git promotion (like other index data) for cross-machine learning.

## Surface

- Robot mode `atc` subcommand first (`toron robot atc --since ...` → JSON of recent
  experiences + per-tool aggregates), **not** an MCP tool — the 38-tool surface is
  frozen (ADR-0005 invariant 2). An MCP tool would be a future, separate decision.
- `toron atc status` / `toron atc tick --dry-run` for the calibration loop.

## Constraints (ADR-0005 invariants, all apply)

1. No new kind — ATC data lives in SQLite, not on the bus.
2. No new MCP tool — robot/CLI surface only.
3. No dep inversion — toron-core owns the table; toron-cli/toron-workflow consume.
4. `#![forbid(unsafe_code)]`, Tokio, rmcp preserved.

## Open questions

1. **Do experiences need to be signed/auditable?** The `decision_id` + args hash
   gives integrity; Nostr-signing each row would be costly and is probably overkill.
2. **Where does the tick loop live?** Daemon task (toron-mcp server) vs a `toron atc
   tick` CLI invocation (cron-driven). The daemon task is more "always-on", the CLI is
   more local-first/honest.
3. **Outcome resolution source:** does Phase-2 outcome come from the daemon's own
   observation (bus events) or from the agent's next call? The former is more reliable.
4. **Privacy:** args_hash is a hash, not the args — good. `metadata` must avoid
   leaking message bodies.

## Recommendation

Keep as a research epic (multi-quarter). Do NOT start. If picked up: build the
experience row + two-phase outcome first (the value), defer the calibration math
until there's real outcome data to tune against. hachiko's CalibrationTracker is the
reference implementation to port the three-step shape from.

---

*Lane OC-C (BlackMarsh), 2026-08-07. Research only — no code shipped.*
