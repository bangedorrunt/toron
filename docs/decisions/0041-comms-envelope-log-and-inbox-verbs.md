<!-- governed-by: ADR-0003, ADR-0033, ADR-0037, ADR-0040 -->
---
status: accepted
date: 2026-10-11
decision-makers: bangedorrunt
consulted: stack-comms redesign proposal §2, §3, §11, §11.1, §12 (stack-comms/redesign-proposal.md); stack-comms build plan §4, §10 (stack-comms/build-plan.md)
informed: flywheel x toron x beads maintainers
---

# ADR-0041: Comms Envelope, Per-Recipient Log, and the `next`/`ack`/`done` Verbs

**Status:** Accepted — captain word 2026-10-11 ("just finish and be sure the implementation and vision match your intent"; the build plan's §10 proposals adopted), relayed by the flywheel bot for factory task `23283d6f`.

**Decision in one sentence:** toron carries every comms message as one typed envelope appended once to a per-recipient log keyed by the L16 producer triple; delivery facts are written only by the tools the reader runs (`toron next`, `toron ack`, `toron done`), and those verbs are CLI-only behind `TORON_COMMS_INBOX`.

**Builds on:** [ADR-0040](0040-adopt-elm-effect-for-the-comms-kernel.md) (the kernel types come from `toron_core::kernel`; `ToronLog` implements `kernel::DurableLog`). [ADR-0037](0037-one-key-per-pin-project-scoped-visibility.md) (an envelope addresses an npub; `--as <name>` resolves through the W1 one-name-one-key roster).

**Does not change:** the frozen 41-tool / 25-resource MCP surface (ADR-0033: no tool is added); NIP-17 mail (`mail send`) and its stores; the doorbell and pane path (flywheel's, Phase 2/3).

---

## 1. What lands (Phase 1)

| Piece | Where |
|---|---|
| Envelope v1: `MsgId{producer, epoch, seq}`, `Recipient::{Npub, Group}`, `Class`, `ContractId` (`task@1`), `ContextId`, `TaskId`, `gen`, `causation`, deadline, lease, body; `validate()`; JSON Schema (schemars) | `toron_core::envelope` |
| Contracts registry: `task@1` (`TaskV1{title, brief, scope_paths, acceptance, bead}`), `notify@1`; unknown major → `Reject::UnsupportedContract` | `toron_core::contracts` |
| Delivery FSM: 12 phases, pure `DeliveryFsm::step(&state, &event) -> Result<state, Reject>` | `toron_core::delivery` |
| Tables: `inbox_log` (`UNIQUE(producer, epoch, seq)`), `inbox_producer_state`, `inbox_cursor`, `inbox_delivery` | migration `0019_comms_inbox_log.sql` |
| `InboxStore` (async, sqlx, `BEGIN IMMEDIATE`) and `ToronLog: kernel::DurableLog` (OS-thread bridge to the store; `Unavailable` instead of `block_on` on a Tokio thread) | `toron_core::inbox` |
| The sender loop: `Outbox: kernel::Program`, `Durable{key = msg id}` intent before the append, `Unknown` → `plan_attempt` → `ReconcileFirst`, bounded at `MAX_RECONCILE` | `toron_core::outbox` |
| Verbs: `toron next [--peek]`, `toron ack <msg> accept|reject --reason`, `toron done <task> --evidence`, fixture door `toron inbox append --file` | `toron-cli/src/comms.rs` |
| Lean model: `Proto/Log.lean` (P1: idempotent append, stale epoch fenced, gap rejected, at most one row), `Proto/Delivery.lean` (P2: monotone, terminal absorbs); exhaustive vectors committed | `proto/` |

## 2. Rules

1. **One triple, one row.** The same `(producer, epoch, seq)` with the same content returns the same offset and writes nothing. The same triple with different content is a hard conflict (`InboxError::PayloadConflict`; `NotAnEnvelope("payload conflict ...")` through the kernel entry, since the vendored `AppendError` is closed) and also writes nothing; a replay is never answered for content the log does not hold (2026-10-16, bd-envelope-conflict-3d7u). A lower epoch is fenced before dedupe (`StaleEpoch`), a seq gap is refused, a new epoch starts at seq 0. Producer epoch state never expires. `ToronLog` passes `kernel::durable_log_laws::all`.
   **Epoch claim (2026-10-16, bd-epoch-claim-z5m5).** A producer does not choose its epoch: `InboxStore::claim_epoch(producer, holder)` hands out the next one in a single `BEGIN IMMEDIATE` transaction (migration 0022, `inbox_epoch_claim`), strictly above every epoch the log has appended or claimed for that producer (the first is 0). Appends are fenced against the higher of the appended and the claimed epoch, so a claim fences every older incarnation before the claimant has appended anything; an epoch whose holder never appends is skipped. Callers that kept a local epoch lease (flywheel's file lock) switch to this. *The sender's own journal is such a caller (bd-journal-claim-epoch-4blu):* `outbox::send_and_recover` claims its journal epoch (producer `<npub>#journal`) instead of computing `producer_epoch + 1`, so concurrent runs of one sender never share a journal epoch, a run that journaled nothing still reserves its epoch, and a newer run fences an older run's journal. Consequence for callers: two runs of one sender racing each other can fence each other's journal appends (a refused append never landed, so a retry cannot duplicate); callers retry with jitter, or serialise runs of one sender.
   **Task lookup (2026-10-16, bd-n24w).** `InboxStore::by_task(task_id, viewer)` and `toron inbox task <id> --project P --as WHO` list every envelope of a task the viewer sent or received, oldest first, with each delivery's live phase. It is read-only, reaches both tables by index (`inbox_delivery_by_task`, and `inbox_log_by_msg_key` from migration 0023; a test pins the plan), and answers a stranger exactly as it answers an unknown task id: an empty list.
2. **One table, many streams.** `inbox_log` backs every `ToronLog` stream (`inbox`, `journal`); the triple is unique across all of them. A sender loop's journal producer is `"{npub}#journal"`, so it can never collide with the envelope producer.
3. **Reads write delivery.** `toron next` commits the cursor advance and `Sent → Delivered` in one transaction; `--peek` commits neither. Only the recipient may read its stream (`not_recipient`); only the holder may finish (`not_holder`).
4. **The sender journals first.** The outbox appends its intent to its own journal before the provider runs. A crash between intent and append is recovered by reconciling (lookup by msg id), never by a blind retry; a crash after the append finds the row and settles without a second row.
5. **Folds are pure.** `Outbox`'s `Program` and the delivery FSM read no clock, env, filesystem, store or runtime (`tests/fold_purity.rs`, with a negative control on the interpreter). This test stands in for elm-effect-lints until that crate is wired.

## 3. Decisions (build plan §10, adopted)

- **Flag:** `TORON_COMMS_INBOX=1` (`1|true|on`). Off by default; every verb exits 2 with the flag's name while it is off. The `[comms] inbox_log` config key (2026-10-16, bd-652a) is the second switch, read by the CLI loader from the system, user and project config (a later layer that sets it wins; a layer that does not mention it leaves it alone). The env var overrides it whenever it is set and non-empty: `1|true|on` is on, `0|false|off` is off, and any other value is off, so a typo never opens the log. Neither set: off. The decision is the pure `inbox::resolve_enabled(env, config)`. Deployment note: a toron binary older than this change rejects a config file that contains `[comms]` as invalid and ignores the whole file, so install the new binary before adding the key.
- **Default-on date:** flywheel `send --via-inbox` is per-recipient opt-in from the Phase 2 merge, default-on **2026-10-25**, and the legacy payload-typing path is deleted no later than **2026-11-08** (spec §12's hard date).
- **CLI-only verbs.** `next`/`ack`/`done`/`inbox append` are CLI nouns that open the shared durable root directly (the `ack-sla` / `reserve yield` pattern). No MCP tool; an additive tool needs its own ADR-0033 record.
- **Wake fold location:** Phase 3 hosts it in the flywheel doorbell; the long-term home (`toron serve` vs doorbell) is decided at Phase 7.
- **beads scope:** `task@1.scope_paths` carries the bead's paths; the bead id rides in `task@1.bead`.
- **`T_orphan` = max(2 × lead lease TTL, 10 min)** (recorded in ADR-0037's log; used in Phase 4b).
- **Comms feature freeze:** no new doorbell/wake/send features outside the build plan until 4b lands (recorded in the build plan and briefs only).

## 4. Deviations from the build plan, on purpose

- `ToronJournal: kernel::JournalStore` moves to Phase 2: the Phase 1 sender journals intents through a `ToronLog` stream (`journal`), which is what the Loop's `DurableLog` needs; the effect-record journal arrives with flywheel's `mail.append` effect.
- The reader-side `InboxProgram` (cursor + wake + binding state) is Phase 3 work, where it replaces `DoorbellModel` + `WakeModel`. Phase 1's cursor lives in the store and is advanced by `next`.
- Lean vectors are exhaustive over a small domain (2160 delivery steps, 125 log traces) instead of `--seed N --count 10000` random draws: every (phase, class, event) case is covered, and `--check` fails if the committed copy drifts. Envelope validation is tested in Rust only (`Proto/Envelope.lean` deferred).

## 5. Verification

- `mbx test -p toron-core --test inbox_log` (13), `--test proto_vectors` (3, incl. a wrong-vector negative control), `--test fold_purity` (3, incl. a negative control).
- `cd proto && lake build && lake exe proto vectors --check vectors/delivery.jsonl && lake exe proto log-traces --check vectors/log_traces.jsonl` (CI job `lean`).
- Live (fixture daemon, spare port, throwaway XDG roots): build plan §4 cases (a)–(e).

## Status log

- **2026-10-11 — accepted** with the Phase 1 PR (stack-comms build).
- **2026-10-11 — Phase 2: `ToronJournal` lands** (the §4 deviation is closed). `toron_core::journal::{JournalDb, ToronJournal}` (re-exported from `toron_core::inbox`) is a `kernel::JournalStore` over migration `0021_comms_effect_journal.sql`:
  - `effect_journal(scope, key)` rows, plus an `effect_journal_log` change log that keeps a row snapshot per offset and trims per scope.
  - Every operation is one `BEGIN IMMEDIATE` transaction, so offset and state commit together and racing processes serialize.
  - The sync handle uses the `ToronLog` bridge (request/reply to a Tokio task, refused on a Tokio worker: F2).
  - The trait's infallible signatures answer an unreachable store with "nothing changed", and `record_intent` with an `Unknown` + `Reconcile` row. `try_*` return the error.
  - `kernel` additionally re-exports `JournalRow`, `JournalRead` and `JournalDisposition`.
  - Tests (`tests/toron_journal.rs`):
    - upstream's `journal_store` contract as a law suite run against both `InMemoryStore` and `ToronJournal`;
    - a 1,500-step differential against the reference;
    - reopen durability, scope isolation, two-process claim races (80 claims, 80 distinct generations), and the F2 refusal.
  - Mutations caught: dropping the generation fence fails the laws and the differential; a deferred `BEGIN` fails the race.
  - Not wired into a loop yet. Its first consumers are flywheel's effect runner and the Phase 4a supervisor claims.
