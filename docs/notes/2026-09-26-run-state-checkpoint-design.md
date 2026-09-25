# Run-state checkpoint store + time travel (flywheel B2/B3, toron-core half)

**Status:** design handoff, NOT an accepted ADR. Written 2026-09-26 by BrashOtter
for a toron-core crew. Source analysis: `flywheel/docs/analysis/langgraph-vs-flywheel.md`
(Parts 1, 3, 5).

**Read this first:** ADR-0035 (Elm kernel law) and ADR-0032 (workflow-engine
honesty). This work sits directly on both.

---

## 1. Goal

Give a long-running fold a **durable, replayable projection of its own state**,
so an operator can ask "what was the state at step N?" and replay or fork from
there, instead of grepping JSONL.

This is the toron-core half. The flywheel half (serialization, host wiring,
verbs) is a separate bead and depends on this landing first.

## 2. Non-goals — read before designing

- **Do NOT add a second model.** The state log is a *projection of the fold*,
  not a place a model is authored. See §3.
- **Do NOT reimplement the effect journal.** `workflow_effects` already
  persists `outcome_json` and `failure_json` and already guarantees
  journal-before-provider. This work **reads** those rows; it never writes a
  parallel effect table.
- **Do NOT add a time-travel UI.** No Studio equivalent, no graph renderer.
- **Do NOT touch `crates/toron-workflow/src/`.** The engine is not involved.
- **Do NOT migrate or renumber existing migrations.** Add `0013_*` only.

## 3. The authority rule (this is the hard part)

ADR-0035 §"Patterns to avoid" lists **"second stores"** as forbidden. A state log
is literally a store beside the model, so this needs an explicit resolution or
the work is illegitimate.

**The rule that makes it legal:**

> The log is the durable projection and is authoritative. The model is a pure
> function of the log. **No code path may seed the model from any other source.**

The failure mode the rule forbids is *two writable authorities that can
disagree* — e.g. today's `seed_swarm_resume` reading counters back into a model
that the log also claims to describe. Once the log exists, it replaces
projection seeding for the fields it covers. It does not sit beside it.

**Corollary for review:** if you cannot state, for any given field, whether the
log or the seeder owns it, the design is wrong. Say so in the PR rather than
shipping an ambiguous overlap.

## 4. Snapshot point — NOT the tick boundary

The obvious reading ("snapshot where the pure fold ends") is **wrong** on the
live host and is the single easiest mistake here.

`flywheel/src/doorbell_host.rs:342` calls
`handle_workspace_cmds(&mut self.model, ...)`, which **mutates the model after
`tick` returns**. A snapshot taken at the fold's return is an incomplete model
and would not resume.

The interpreter is:

```text
BrainMsg -> wmsg -> workspace::tick(&self.model, &wmsg) -> (next, cmds)
  self.model = next                                       // fold (pure)
  expand_reclassify_cmds(...)                             // pure
  handle_workspace_cmds(&mut self.model, &cmds, ...)      // effects, MUTATES model
  cmds -> Effect::Prompt / FetchInbox / ...               // brain effects
```

**The only correct snapshot point is after the effect layer returns**, because
that is the state the next `tick` will read. Encode this in the API so a caller
cannot get it wrong: name it for the resume point, not for the fold.

## 5. Schema — `0013_run_state_checkpoints.sql`

Follow the conventions in `0010_workflow_effects.sql` exactly: `TEXT` for
typed enums, int-microseconds for times, bounded evidence as `TEXT`, indexes on
the query path.

```sql
CREATE TABLE IF NOT EXISTS run_state_checkpoints (
    thread_id     TEXT    NOT NULL,
    seq           INTEGER NOT NULL,
    checkpoint_id TEXT    NOT NULL,   -- ULID: lexicographically sortable, newest last
    parent_checkpoint_id TEXT,
    model_json    TEXT    NOT NULL,   -- the fold's state at this boundary
    msg_kind      TEXT,               -- the Msg that produced this state
    effect_keys   TEXT,               -- JSON array: effects this boundary's cmds own
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (thread_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_run_state_thread_latest
    ON run_state_checkpoints (thread_id, checkpoint_id DESC);
```

**`checkpoint_id` must be a ULID** (or another monotonically-sortable id), not a
timestamp alone. `get latest` then is `ORDER BY checkpoint_id DESC LIMIT 1` and
`get by id` is an equality lookup on the primary key. A time-only key forces a
scan per lookup — do not do this.

## 6. Skip-and-replay comes free — use it

`WorkflowEffect` already persists `outcome_json` and `failure_json`. So on
resume from `seq = N`:

- an effect whose row is `settled` → replay `outcome_json` as the result
  message; **do not re-invoke the provider**
- an effect left `in_flight` → this is the honest-unknown case. Surface it as
  unknown. **Never infer success from a missing or partial row.**

This is the whole reason the work is cheap, and it is strictly stronger than
what LangGraph does (it delegates idempotency to the node author).

## 7. Retention — decide it now, not later

Checkpoints grow without bound. LangGraph hit exactly this and needed a beta
`DeltaChannel` to mitigate. Pick a policy up front (bounded rows per thread,
and/or age-based prune) and make it explicit in the API. A prune that can delete
a row a live resume still needs is a correctness bug, not a cleanup.

## 8. Replay and fork semantics

- `replay(thread_id, from_seq)` — read-only. Never performs effects.
- `fork(thread_id, from_seq)` — **dry by default.** A fork may have dispatched
  real external effects before the fork point; performing more from a fork
  point is a correctness hazard, not a feature. Any live fork must be an
  explicit, separate opt-in.

## 9. Acceptance

1. A new store type with `append`, `latest`, `get(seq)`, `history`, `prune`,
   all async, all over the existing index pool (no second SQLite file — the
   one-index rule in `0010`'s header comment is deliberate).
2. `tests/` conformance suite, substitution-only per ADR-0035: temp-file sqlite
   on the production code path, scripted providers, no network, no wall clock.
   Cover: append-then-read round trip; ordered history; `get by id` equals
   lookup (the one a broken DeltaChannel walk silently breaks); prune refuses to
   drop a row a live resume needs; a failed insert leaves no partial row.
3. **A mutation check.** Prove the suite fails when the invariant breaks. A
   guard that cannot fail is decoration — see
   `flywheel/tests/journal_mode_invariant.rs` for the pattern used there.
4. `mbx +nightly-aarch64-apple-darwin test` green, `verify-clippy.sh` exit 0.

## 10. Operational warning

**The toron working tree is NOT clean.** Uncommitted changes exist in:

```
crates/toron-cli/src/plane.rs
crates/toron-cli/tests/cli_operator.rs
crates/toron-core/tests/wake_conformance.rs
crates/toron-workflow/src/guard.rs
docs/guides/check.mjs
```

These are **someone else's in-flight work.** Do not commit them, do not revert
them, and do not `git add -A`. Reserve your paths through toron before touching
them, and stage explicitly.
