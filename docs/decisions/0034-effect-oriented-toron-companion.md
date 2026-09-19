<!-- governed-by: ADR-0003, ADR-0032, ADR-0033 -->
---
status: proposed
date: 2026-09-19
decision-makers: Captain (bangedorrunt)
consulted: effect-oriented skill (principles, rust.md, patterns.md); flywheel ADR-0015 §4; toron gaps audit (7 gaps)
informed: flywheel × toron maintainers
---

# ADR-0034: Effect-oriented discipline for toron (companion to flywheel ADR-0015 §4)

**Status:** Proposed (bead bd-lzo6, crew B). Acceptance is a captain word on this file.

**Decision in one sentence:** Land the seven audited toron effect gaps — typed
mailbox edge, bounded archive git, atomic slot acquire, MCP error envelope,
shared policy types, claim convergence, JSONL guard — without touching kinds,
tool names/counts (ADR-0033 owns that freeze), or the journal shape (ADR-0032).

## Context

- Flywheel ADR-0015 adopts effect-oriented discipline house-wide but does not
  govern toron: "Crew B authors the toron companion ADR first, then implements."
- The toron gaps audit found toron's core sound (journal, idempotency keys,
  `kill_on_drop` + timeout on the judgment subprocess) with handwritten edges:
  an unbounded `git commit` subprocess, check-then-act slot acquire, stringly
  MCP errors, per-site timeout/retry constants, sleep-window claim reads.
- Each gap lands independently, smallest first; every gap carries its own
  substitution/boundary test. Order below is build order.

## Decisions

### G1 — Typed `MailboxResult` edge, no per-site signature churn

- Add `pub type MailboxResult<T> = Result<T, MailError>` in
  `mailbox::error` next to the existing `MailSurfaceResult` trait.
- New fallible `MailSurface` methods return `MailboxResult`; existing
  `anyhow::Result` sites migrate only when touched (no flag-day churn).
- `mail_error_from_anyhow` stays the single anyhow→code bridge; classification
  on `code`, never message text (ADR-0007 R1/D2).
- Accept: alias exists; at least the slot + reservation surfaces return it;
  `code_string_round_trips`-style tests cover new codes.

### G2 — Bounded archive git subprocess (this ADR's first slice)

- `GitArchive::commit_via_binary` gets a hard timeout (default 30s) with
  kill-on-expiry: spawn + `try_wait` poll loop, `kill()` past deadline, error
  names the binary, the deadline, and the partial stderr tail.
- Binary validation at call time: when the configured path contains a `/` it
  must be an existing executable file, else fail fast (misconfiguration
  surfaces as config error, not a hung commit).
- libgit2 stays the default path; the binary path is the override only.
- Accept: hang-shim test (fake binary that sleeps) errors inside the bound;
  missing-binary test fails fast; existing archive tests green.

### G3 — Atomic slot acquire (UNIQUE + upsert convergence)

- `acquire_build_slot` is check-then-act across `active_slots` → bus publish →
  `insert_slot`: two concurrent acquirers can both pass the check.
- Fix at the store: a single conditional upsert that only wins when no
  conflicting active lease exists (conflict rule = current `conflict_for`
  semantics, evaluated in SQL inside one transaction); loser gets the holder
  list back, never a duplicate grant.
- The kind-30032 publish stays outside the transaction (bus is not
  transactional); publish-then-lose is handled by re-reading the winner and
  reporting conflict, never a phantom grant.
- Accept: concurrent-acquire test (N tasks, one slot, exclusive) grants
  exactly one; loser reports the holder.

### G4 — MCP error envelope `{code, retryable, hint}`

- Every MCP tool error returns a stable envelope: machine `code` (from
  `MailErrorCode` or a tool-local code enum), `retryable: bool` (only
  `Timeout`/`Io`/explicit transient are retryable; `PolicyDenied`/`NotFound`/
  config errors never are), and a one-line `hint` (what to run next).
- Envelope is additive metadata on the existing error shape (ADR-0033: no
  tool renames, no count change); the shapes fixture gains the envelope
  fields with a version bump, never an in-place edit.
- Accept: fixture pins envelope on 3+ representative tools; a
  `retryable_is_conservative` unit test asserts deny-codes are not retryable.

### G5 — Shared `policy::{Timeout, RetryPolicy}` type

- One `policy` module (core): `Timeout { each: Duration }`,
  `RetryPolicy { max_attempts, base, factor, jitter, retry_if }` with
  `retry_with_backoff(op)`; timeout-inside-retry ordering fixed once.
- Migrate settle/dispatch/judgment-adjacent loops off hand-rolled
  `sleep`+`attempt` counters; new remote edges take `policy` as a value
  (injected, never ambient constants).
- Accept: `fail_twice_then_succeed` substitution test; storm test stops at
  the cap; no new `tokio::time::sleep` retry loops in touched files.

### G6 — Store-watch convergence for job claims

- Job/claim handoffs that poll-and-sleep converge on store state instead:
  claim = one conditional write (winner deterministic); waiters subscribe to
  the projection change (watch channel / re-read on notify), bounded by the
  shared `Timeout`, never an open `sleep` window.
- Accept: two-claimant test converges on one winner; waiter returns within
  the bound with the winner's id; no fixed-sleep synchronization in the
  claim path.

### G7 — JSONL generation guard

- Every JSONL writer (beads sync, plane dumps, presence log) validates each
  row before append: exactly one JSON value per line, no embedded `\n`,
  trailing `\n` present; a violation fails the write with the row index,
  never a silently corrupt file.
- Readers keep accepting today's files (no migration); a `lint` entry point
  reports the first bad row for existing files.
- Accept: corrupt-row test refuses with index; round-trip test
  write→read→write is byte-stable.

## Explicitly not changing

Kind numbers/shapes (ADR-0003 frozen registry); MCP tool names/count
(ADR-0033); journal/effect-key shape (ADR-0032); Tick Law equivalents on the
toron side (bus/index layering); `decide`/briefing (flywheel-owned).

## Verification

Per-gap tests above + `cargo test -p toron-core --lib` green + clippy on
touched crates. Gaps land in order G2 → G1 → G4 → G5 → G3 → G6 → G7;
each commits separately with the bead id in the message.

## Tracker

Bead bd-lzo6 (crew B). Lane 0 (binary/migration-11 skew) closed in-session:
installed toron predated commit 27242c5 (migration 0011); rebuilt from HEAD,
presence + channels verified working.
