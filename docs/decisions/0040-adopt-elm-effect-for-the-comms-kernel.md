<!-- governed-by: ADR-0003, ADR-0034, ADR-0035, ADR-0037 -->
---
status: accepted
date: 2026-10-11
decision-makers: bangedorrunt
consulted: stack-comms redesign proposal §9A (stack-comms/redesign-proposal.md, 2026-10-11); elm-effect-rs ADR-0002 (L12, L14, L16, :463, :469-479, :644, :692); flywheel ADR-0017 (foldcore, rejected by this record's companion)
informed: flywheel x toron x beads x chiebukuro maintainers
---

# ADR-0040: Adopt elm-effect-elm and elm-effect-journal for the Comms Kernel

**Status:** Accepted — captain word 2026-10-11 18:44 AEDT ("go with your proposal, fully implement with live test"), relayed by the flywheel bot for factory task `23283d6f` (ship, flywheel-stack). The captain accepted every recommendation in the stack-comms redesign, including §9A.

**Decision in one sentence:** toron's comms kernel (envelope log, delivery FSM, wake fold, supervisor) is built on elm-effect-rs's Elm loop and effect journal — and only those two crates — reached through one port module, `toron_core::kernel`, with the protocol logic itself kept as pure `std + serde` functions.

**Amends:** [ADR-0035](0035-elm-kernel-law-and-judgment-plane.md). ADR-0035's kernel *law* is unchanged (one Model, one Msg, a pure `tick`/`update`, effects journaled before they run). What changes is *where `Program` and the journal types come from* for the comms kernel: ADR-0035 said it "does not wire … a second runtime", and flywheel ADR-0017 paraphrased toron's stance as "copy a core, not import a kernel". For the comms kernel, toron now imports the kernel.

**Does not amend:** the existing hand-written cores (`dispatch_core.rs`, `reservations_core.rs`, `receipts_core.rs`, `toron-workflow/src/cron_core.rs`) — they migrate only when touched; the one-way dependency `flywheel → toron-workflow → toron-core`; the frozen 41-tool / 25-resource surface and the kind registry (ADR-0014, ADR-0033); the async sqlx store (it stays the store); ADR-0003's Tokio-only, `forbid(unsafe_code)` rule for toron's own crates.

**Does not wire:** `elm-effect-unstable` (workflow, eventlog, persistence, rpc, http, sql, cli, ai, cluster); the `durable-streams` feature; `schema`/`stm`; any carrier combinator in protocol code; an OTP runtime (still ruled out, ADR-0035 :29).

---

## 1. Context

- The redesign (one envelope, one per-recipient log, tool-emitted delivery facts) leans on laws elm-effect-rs already states and tests: L12 (a program is a fold), L14 (intent first; `Unknown` has no default exit; generation and lease), L16 (producer triple `(producer, epoch, seq)`, opaque offsets, stale-epoch fencing). Building a parallel copy in toron is the "third spelling" elm-effect-rs ADR-0002 forbids (:463).
- `elm-effect-journal`'s `can_transition` is toron's golden table, copied pair for pair. `crates/toron-core/tests/kernel_port.rs::kernel_effect_table_is_toron_golden_table` now checks that equality across all 81 pairs.
- flywheel ADR-0017 (`foldcore`) proposed a zero-dependency crate instead. It was never built, has no dependents, and beads already chose elm-effect (`65fb50bd`). It is rejected in flywheel's own record.

## 2. Preconditions (Phase 0, landed upstream first)

elm-effect-rs's own rule is "implement the missing phases in this repository, then edit the sibling" (ADR-0002 :756). Upstream commit `604688cedf6dc9ec7600f34f0efa790ad6b4ef8e` (branch `phase0/journal-wire-epoch-fence`):

1. `elm-effect-journal/src/lib.rs` declares `pub mod epoch`, `plan`, `stream` (and `laws`); the `#[path]` test includes are gone.
2. `durable_log::AppendError::StaleEpoch { producer, sent, current }`; `MemLog` fences a lower epoch before dedupe. `laws::{same_triple_appends_once, seq_gap_is_rejected, stale_epoch_is_fenced, new_epoch_starts_at_zero, all}` check the contract through `&dyn DurableLog`, with negative controls upstream.
3. `Loop` documents single-thread hosting (`!Send`; inputs through a channel into `send`; no `block_on` inside Tokio). A `Send` variant is not provided; bead `uqh` (`interpret_in_env`) stays upstream's.

## 3. What toron uses

Only through `toron_core::kernel` (re-exports, no wrappers):

| Layer | Names |
|---|---|
| Elm loop | `Program`, `Update`, `Cmd`, `Durability`, `Failure`, `Interpret`, `InterpretError`, `Loop`, `Producer`, `Subscriber`, `Boot`, replay `Player`, `Snapshot` |
| Journal | `EffectState`, `can_transition`, `transition`, `TransitionError`, `JournalStore`, `InMemoryStore`, `plan_attempt`, `plan_park`, `may_settle`, `EffectView`, `ProviderCapability`, `DispatchPlan`, `ParkDecision` |
| L16 log | `DurableLog`, `Offset`, `ProducerId`, `AppendError`, `SCHEMA_VERSION`, `MemLog`, `EpochLog`, `Fence`, `Gone`, `durable_log_laws` |
| Carrier (minimum an `Interpret` impl and a host must name) | `Effect`, `Exit`, `Cause`, `ManagedRuntime` |
| Pin | `ELM_EFFECT_REV` |

## 4. Containment rules

1. **One port.** `crates/toron-core/src/kernel.rs` is the only file under `crates/` that names an elm-effect crate; `crates/toron-core/Cargo.toml` is the only manifest that depends on one. Enforced by `tests/kernel_fence.rs` (with a fixture-tree negative control).
2. **Pure protocol.** `DeliveryFsm::step`, `should_ring`, `SupervisorProgram::update` and the assignment guard are plain `std + serde`. Their `Program` impls are thin adapters. The Lean vectors (redesign §10) test the pure functions, so an upstream API move changes only adapters.
3. **Hosting.** One `Loop` per dedicated OS thread. `impl DurableLog for ToronLog` and `impl JournalStore` reach the async sqlx store through a request/reply channel; they never call `block_on` inside a Tokio context (the F2 nested-runtime panic). Phase 1 adds the loop-inside-Tokio regression test.
4. **Laws run on toron's log.** `ToronLog` must pass `kernel::durable_log_laws::all` (Phase 1 exit), the same checks upstream runs on `MemLog`.
5. **No new surface.** No MCP tool, no kind. `toron next` / `ack` / `done` are CLI verbs over existing tools until an ADR-0033 additive record says otherwise.

## 5. Pinning

- **Vendored, with public provenance:** the four crates elm-effect-rs needs to build the two (`elm-effect`, `elm-effect-schema`, `elm-effect-elm`, `elm-effect-journal`; `Cargo.toml` + `src/` only) are vendored under `vendor/elm-effect/crates/` at `ELM_EFFECT_REV`. `vendor/elm-effect/VENDORED_FROM` names the rev; `MANIFEST.sha256` lists every byte.
  - `scripts/verify-vendor-elm-effect.sh` (run by `scripts/verify-gates.sh`, so by CI) checks integrity against the manifest, agreement with `kernel::ELM_EFFECT_REV`, and provenance against `git archive <rev>`: from a local elm-effect-rs checkout when one holds the rev, otherwise by fetching exactly that rev from `upstream_url` (https://github.com/bangedorrunt/elm-effect-rs, public since 2026-10-11), so CI verifies provenance too. `ELM_EFFECT_REQUIRE_PROVENANCE=1` makes an unreachable upstream a failure.
  - `tests/kernel_port.rs` repeats the integrity and pin-agreement checks inside `cargo test`, with a tampered/missing/extra negative control.
  - The vendored tree is excluded from the toron workspace (`exclude = ["vendor"]`): not linted, formatted or tested as toron code. It contains `unsafe` in the carrier; ADR-0003's `forbid(unsafe_code)` binds toron's crates, and `verify-gates.sh` still scans only `crates/`.
  - `vendor/elm-effect/**` is **not** declared `linguist-generated`: the strict commit guard refuses a projected-path declaration over compiled source, and toron-core builds these crates. A vendor bump therefore goes through the normal bead-hunk rule (upstream `bd-*` tokens added by a bump must be attributed in the commit message).
- **Stays vendored (captain, 2026-10-11):** elm-effect-rs is public, but the vendored copy plus the manifest hash check remains the build input; `VENDORED_FROM` cites `upstream_url` + `rev`. A git dependency would add a network fetch to every toron build for no extra integrity. A floating branch stays forbidden (elm-effect-rs ADR-0002 :692).
- **Bumps** are dedicated changes: re-vendor, `--regen` the manifest, update `ELM_EFFECT_REV`, and run the comms vector suite plus `durable_log_laws` on `ToronLog`.

## 6. Consequences

- Better: one spelling of the journal states, `plan_attempt`, the L16 append rule and replay across elm-effect-rs, beads and toron; the F2 class of bug has a structural home (one interpreter, outcomes as messages, `Failure::NotAppended`).
- Harder: toron now builds the elm-effect carrier (≈1.2 MB of source) and tracks a pre-1.0, single-maintainer framework. The vendored copy must be kept honest by the gate, Upstream revs must be on elm-effect-rs `main` before toron pins them.
- Two kernel styles coexist (hand-written `*_core.rs` and `Program`) until touched cores migrate.

## 7. Verification

- [x] `bash scripts/verify-vendor-elm-effect.sh` exits 0 (integrity, pin, provenance on mikasa).
- [x] `mbx test -p toron-core --test kernel_port --test kernel_fence` green, including both negative controls.
- [x] `kernel::durable_log_laws::stale_epoch_is_fenced` passes through the port (`stale_epoch_is_fenced_through_the_kernel_port`).
- [x] `bash scripts/verify-gates.sh` passes.
- [x] Phase 1: `ToronLog: DurableLog` passes `durable_log_laws::all`; loop-inside-Tokio test has no nested-runtime panic (toron#11).
- [x] elm-effect-rs is public (https://github.com/bangedorrunt/elm-effect-rs); the vendored rev `49f01a0` is on its `main` (#2, merge `6d8e95d`); provenance is fetched from the public URL when no local checkout holds the rev. The copy stays vendored (§5).

## 8. Status log

- **2026-10-11 — accepted.** Captain word (relayed). Phase 0 of the stack-comms build: upstream fixes at `604688c`, vendored pin, `toron_core::kernel` seam, fence and pin tests.
