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

1. **One triple, one row.** The same `(producer, epoch, seq)` returns the same offset and writes nothing. A lower epoch is fenced before dedupe (`StaleEpoch`), a seq gap is refused, a new epoch starts at seq 0. Producer epoch state never expires. `ToronLog` passes `kernel::durable_log_laws::all`.
2. **One table, many streams.** `inbox_log` backs every `ToronLog` stream (`inbox`, `journal`); the triple is unique across all of them. A sender loop's journal producer is `"{npub}#journal"`, so it can never collide with the envelope producer.
3. **Reads write delivery.** `toron next` commits the cursor advance and `Sent → Delivered` in one transaction; `--peek` commits neither. Only the recipient may read its stream (`not_recipient`); only the holder may finish (`not_holder`).
4. **The sender journals first.** The outbox appends its intent to its own journal before the provider runs. A crash between intent and append is recovered by reconciling (lookup by msg id), never by a blind retry; a crash after the append finds the row and settles without a second row.
5. **Folds are pure.** `Outbox`'s `Program` and the delivery FSM read no clock, env, filesystem, store or runtime (`tests/fold_purity.rs`, with a negative control on the interpreter). This test stands in for elm-effect-lints until that crate is wired.

## 3. Decisions (build plan §10, adopted)

- **Flag:** `TORON_COMMS_INBOX=1` (`1|true|on`). Off by default; every verb exits 2 with the flag's name while it is off. A `[comms] inbox_log` config key is deferred until the config loader gains a `[comms]` table; the env var is the only switch in Phase 1.
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
