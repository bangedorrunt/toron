<!-- governed-by: ADR-0003, ADR-0005, ADR-0014, ADR-0019, ADR-0020, ADR-0022 -->
---
status: proposed
date: 2026-08-27
decision-makers: Captain (bangedorrunt)
consulted: bastani-inc/atomic live tree (cloned 2026-08-27, `main` @ `/tmp/atomic-fresh`; packages/workflows loop-until-done, adversarial-verification, workflow-failures, durable DBOS backend, keepContext, ctx.tool failureMode=return); 2026-08-25 Buffy comparison (partially superseded by the tree study, see §3.3); Round-1 in-file debate (§12); control_flow.rs DAG scheduler + ActionRunner shape; flaky-test incidents (bd-8xca, bd-cvv9)
informed: flywheel maintainers; toron-workflow engine owners; skill owners (toron, flywheel, runtime-adaptation)
---

# ADR-0024: Make the DAG Canonical, Type Runtime Failure, and Unroll Bounded Repair as Distinct Graph Nodes

**Status:** Proposed (2026-08-27, governing revision after Atomic-tree study + Round-1 debate)
**Date:** 2026-08-27
**Decision-makers:** Captain (bangedorrunt)
**Extends:** [ADR-0005](0005-orchestration-layer.md) (workflow engine, DAG scheduler, control-flow actions), [ADR-0019](0019-bead-verified-execution-and-principle-playbooks.md) (ledger-gated close, VERIFY fence, 21 engineering principles), [ADR-0020](0020-nostr-two-plane-cli-and-crew-steering.md) (two-plane surface, crew steering), [ADR-0022](0022-phase-native-orchestration.md) (typed `DispatchSpec`, `Phase` enum, one durable execution rail)
**Amends:** ADR-0005 Spec 4b only (`graph:` becomes the canonical internal shape; `steps:` desugars to a chain graph at parse time; graph nodes park `request_approval`/`delay`/`spawn`; failure objects carry a classified `kind`; a repair *pattern* unrolls as named nodes).
**Does not amend:** ADR-0019 Spec 1 / beads-fork legal-close table (`VerdictKind` unchanged; repair is an event log, never a close-legal verdict). ADR-0022 §4 (Phase vocabulary stays five values; repair is not a sixth phase). Frozen MCP tool surface (40 tools). Kind registry (ADR-0014). One-way dep (flywheel → toron-workflow → toron-core). Worktree ban (ADR-0019 Spec 3). Identity grammar (ADR-0018). Toron plane-surface noun freeze (ADR-0020: `channel|job|react|presence|watch`). Atomic reservation conflict-check (ADR-0011).
**Tracker:** beads. Never pull requests.
**Companion overlay:** [ADR-0025](0025-adopt-graph-dispatch-artifact-reduce-pin-supervision.md) (proposed 2026-08-27). Lease reclaim, typed artifacts, pin supervision, plane routing table, ingress unification. Does not change Wave 1 specs. [ADR-0026](0026-failure-taxonomy-funded-preflight-fallback-chains-mail-health-escalation.md) (proposed 2026-08-28) amends Spec 1's runtime kind set with `billing` and `provider`.

> **Scope (governing):** make `run_graph` the only sequencing path. Classify *runtime* failure at the ActionRunner boundary. Encode work-quality repair as an unrolled graph (worker → independent evaluator → VERIFY), each cycle a distinct named node with a durable ledger artifact. Approvals carry artifact paths and node states, not a bare string. A bead stays the unit of work; a workflow run is one durable attempt that may *prove* a close, never *be* the close.
>
> **Out of scope:** eleven new `flywheel workflow *` nouns, `br create --auto-workflow`, engine shell / `br close`, default `on_error` flip, `phase_chain.rs` retirement, worktrees, hashline, compaction `keepContext`, LLM mean-and-veto verifier. Those are follow-up ADRs. Provenance: 2026-08-25 draft + §12 debate + 2026-08-27 Atomic clone. Where they conflict, **§6 below governs**.

---

## 1. TL;DR

The DAG scheduler exists (`control_flow.rs` `run_graph`). It is opt-in; `steps:` is the primary shape. `Action::Retry` retries the same step on the same model. `on_error` is `Continue | FailRun`. Approval parks on a plain `message` string. The engine cannot express "work, then an independent evaluator, then VERIFY, bounded, each cycle a visible node."

Atomic (`bastani-inc/atomic`, cloned 2026-08-27) does **not** hide that loop inside `on_error`. `loop-until-done` (`packages/workflows/builtin/loop-until-done-runner.ts`) unrolls `iteration-N` + `evaluate-N` as distinct stages, each `context: "fresh"`, with a durable JSON ledger and a structured evaluator `{done, remaining_work, validation_evidence, failures}`. Progress scores are advisory and **never a stop decision**. `ctx.tool(..., { failureMode: "return" })` lets a check fail as typed data; the engine **never injects** that evidence into the next prompt. Runtime failures are a different type (`auth | rate_limit | provider | cancelled | unknown` with `retryable`/`resumable`/`disposition`). Durability is DBOS + hashed tool-output cache, not "JSONL only."

We take those *invariants* onto our spine (Resonate DAG, file reservations, `br` legal-close). We do not take Atomic's process (TypeScript runtime, worktrees, DBOS, engine-as-shell).

Five ships, in this order:

1. **The graph is canonical.** `steps:` desugars. `run_workflow` always calls `run_graph`. Graph nodes park `request_approval` / `delay` / `spawn` the same way linear steps do (today they do not — that would be a WF-08 regression).
2. **Runtime failure is typed.** Embedding classifies `{ok:false, kind, retryable, resumable, disposition}`. Default `on_error` stays `Continue` until every runner site classifies. Work-quality failure is **not** an `ErrorKind`.
3. **Repair is an unrolled pattern.** Worker (fresh context) → independent evaluator (fresh context, structured remaining_work) → VerifySink (embedding runs the command) → next named cycle or escalate. Max N. Ledger JSON on disk. Engine does not shell out and does not call `br`.
4. **Approvals carry evidence.** Promise payload includes node states, artifact *paths*, repair-ledger path. Question is a template; evidence lives in files (`reads`), not dumped into the message.
5. **A bead is not a workflow run.** `--work` stays claim → brief → close-detect → **loop-run VERIFY** (`CommandVerified` is loop-only, anti-forgery). Unrolled repair lives inside workflow YAML. Engine never writes `command-verified` and never calls `br close`.

The DAG scheduler is the spine. We do not rebuild it. We do not promote it into a shell runtime. We do not merge the bead ledger into the run.

---

## 2. The Execution Graph (governing picture)

This is the target **after §6**. Repair is not a hidden `on_error` on review/verify. It is named nodes. The loop still owns `command-verified`. There is no Wave-1 `workflow tree` CLI.

```
  compose recipe repair-until-verified
  (or --from-yaml graph; NOT on_error: repair)
                 │
    ┌────────────┼────────────┐
    ▼            ▼            ▼
 implement    review       (optional)
 @coder       @reviewer    judgment: ReviewerSigned
 context:inherit           do NOT repair
    │
    ▼
 ┌──────────────────────────────────────────┐
 │  unrolled cycle N = 1..max_iterations    │
 │                                          │
 │   repair-N     evaluate-N     verify-N   │
 │   spawn-await  spawn-await    VerifySink │
 │   context:fresh context:fresh  embedding │
 │   @coder       @reviewer      cargo slot │
 │        │             │             │     │
 │        └──────reads ledger path────┘     │
 │              (never prompt-stuff stdout) │
 └──────────────────────────────────────────┘
          │ done && exit 0          │ else N+1
          ▼                         ▼
     gate (paths +              exhaust →
     evidence_hash)             kind-14 + remaining_work
          │                     NO br gate report
          ▼
     worker may br close
          │
          ▼
     LOOP loop_run_verify → command-verified
     (LOOP_ARMED only; never the graph)
```

**Two planes, do not collapse:**

```
  workflow run (ship / compose / bug-fix)
       │  in-graph green  = run may continue
       ▼
  bead (--work loop SM; do not replace)
       │  loop-run VERIFY = bead may close
       ▼
  br close --commit-sha   ← only closer
```

**Every node is:**
- **Typed** with a `Phase` (five values, ADR-0022). Repair is not a sixth phase.
- **Gated** with optional `guard` over prior node JSON (`check_phase` stays structured; not a shell `verify:`).
- **Output-producing** — artifact **paths** (`receipt_path`, `ledger_path`, `commit_sha`). Downstream `reads` the file. Engine does not dump stdout into the next prompt.
- **Durable** — Resonate checkpoint per node. `Action::Spawn { await_: true }` waits Idle (`drain_spawn_sink`). `request_approval` parks only if the node goes through `run_step` (Spec 0 park fix).
- **Not auto-repairable.** Work-quality repair is an unrolled subgraph (recipe). Runtime failure is `{ok:false, kind?}`. `Action::Retry` is same-step. Judgment nodes escalate.

**The entire graph is:**
- **Park-safe** after Spec 0 — including linear `steps:` desugared to a chain. Compose stays `graph: None` until that fix is green.
- **Resumable** — kill -9 → same `engine.invoke` run_id; SpawnSink `seen` dedups promise ids.
- **Not forkable/connectable in Wave 1.** Those CLI nouns are a later flywheel ADR.
- **Not the bead closer.** Receipt path in; `br gate report` never out.

---

## 3. Context and Problem Statement

### 3.1 What already exists (do not rebuild)

| Piece | Location | Job |
| :--- | :--- | :--- |
| DAG scheduler | `crates/toron-workflow/src/engine/control_flow.rs` `run_graph` | Wave-based concurrent node dispatch; `needs:` edges; `ctx.run(run_graph_node).spawn()` per node; deterministic replay across crash-resume |
| `on_error: fail_run` | `control_flow.rs` `run_step` (line 226) | A step with `on_error: fail_run` whose action returns `{ok:false}` aborts the run with `{status:"failed", error, failed_step}` |
| `Action::Retry` | `control_flow.rs` `run_retry` | Retries the SAME step up to `max_attempts` with `backoff_secs` — same model, same action, same context. Useful for transient failures (network, relay); useless for logic bugs. |
| `request_approval` | `control_flow.rs` `run_step` (line 139) | Creates a Resonate promise and parks. Engine comments say the embedding publishes kind-46011; **flywheel does not** (no `ApprovalSink` drain). Live SIGNOFF hangs until `resolve_approval`. Wave 1 drains that sink. |
| VERIFY fence + `br gate` | ADR-0019 Spec 1 | Beads carry a `## VERIFY` command; `br gate report` records verdicts; the loop enforces legal-close by reopening unverified beads |
| Bead lifecycle | `flywheel/src/durable.rs` swarm_loop | Poll `br ready`, dispatch to agents, claim-watch in-progress beads, detect `br close`, verify gates, reopen unverified |
| `Phase` enum | `toron-workflow/src/schema.rs` (ADR-0022 §3.1) | Five closed-vocabulary phases: resolve/plan/implement/review/verify; typed `DispatchSpec` with lint rules R-P1..P4 |
| `flywheel runs` | `flywheel/src/runs.rs` | Run board: status, phase column, per-step cost, `runs list --json` |

### 3.2 Measured gaps (evidence, not vibes)

| Gap | Evidence | Real-world consequence |
| :--- | :--- | :--- |
| `steps:` is the primary shape | `run_workflow` dispatches `graph:` only when the field is present (line 43). Every built-in (`builtin_workflows.rs`), every example (`triage.yaml`), and every compose output (`crew.rs`) uses `steps:`. | The mental model is linear. DAG power is hidden. Captains never reach for parallelism. The `ship` built-in (ADR-0022 §3.3) is five sequential compose parts — no concurrent review+verify wave. |
| No repair-after-failure | `on_error` is binary (`Continue` / `FailRun`). `Action::Retry` retries the SAME step on the SAME model. | A failing `cargo test` with `fail_run` aborts the whole ship run. No attempt to fix the test. The captain must see the failure, compose a new run manually, and re-dispatch. This happened twice in production (bd-8xca compose failure, bd-cvv9 chain-gate false positive) — both resolved by captain re-dispatch, not self-healing. |
| Approval carries no evidence | `request_approval` sends a plain `message: String` (control_flow.rs:139). The promise payload is `{ "message": "<the string>" }` — no run state. | The operator sees "Release toron v0.4.0?" — not which nodes passed, what artifacts exist, whether verify ran. A release decision made without knowing verify passed is a \_prove-it-works\_ violation. |
| Bead and workflow are separate | `br close` writes gate rows to SQLite. `build_workflow_run_terminal` emits kind-46012 to the Nostr bus. Neither knows about the other. The loop manually bridges: poll `br list --status closed`, check `br gate report`, reopen if missing. | Two closing ceremonies. A bead can be "closed" with no workflow run at all (manual `br close`). A workflow run can "succeed" without closing its bead. The loop's bridge code is a source of drift (ADR-0007's bug ledger catalogs several bridge mismatches). |
| No live-run observation | Run output is a `serde_json::Value` returned by `run_workflow`. No CLI to inspect an in-flight run, stream node-level events, or fork at a checkpoint. | When a run is parked on approval, the captain cannot see the run tree. When a node fails and the run is still alive (repairing), the captain cannot connect to it. The only signal is the kind-46012 terminal event — which hasn't fired yet. |
| No auto-parallelization | Graph edges are manual `needs:` declarations in YAML. | Two nodes that reserve disjoint files COULD run concurrently, but the captain must know and declare that. The engine could compute it from reservation data. |
| Two sequencing engines | `run_graph` (`control_flow.rs`) owns the DAG; `run_ship_chain` (`phase_chain.rs`) owns `ship`'s `call_workflow` links with a separate `check_phase` gate between links. | One `Workflow` struct, two semantics. Phase contracts fire only on `ship`; a hand-authored graph with the same resolve→plan→implement→review→verify shape gets no contract enforcement. |
| Binary `on_error`, no error taxonomy | `StepOnError` is `Continue \| FailRun`. `ActionRunner` returns `anyhow::Result<Value>`; every failure collapses to `{ok:false, error:"<string>"}`. | The engine can't route failure. "Retry" (same step, transient) vs "repair" (different model, logic bug) vs "escalate" (human) is decided by the captain, not the engine. Self-healing is impossible without an error kind. |
| Phases are string tags | `DispatchSpec.phase` is `Option<String>`. The engine can't tell a plan node from a build node. | The engine can't gate a build on a plan artifact; only the chain runner can, and only for `ship`. |
| Stages are prose | A stage's body is a prompt template; it declares phase/role-affinity/principles but no input/output contract. | Nothing fails loudly if an agent skips the claim→reserve→close sequence inside `systematic-execute`. The stage is advice, not a contract. |
| No run lifecycle beyond checkpoint | `pause` (checkpoint + handoff) and `pickup` (resume) exist. No cancel/quit, no per-action timeout (only `spawn.no_claim_timeout`). | A run can be suspended and resumed but not cancelled or forked at an arbitrary node. A hung `call_webhook` or `dispatch_job` blocks the run forever. |

### 3.3 What Atomic actually does (live tree, 2026-08-27)

Cloned `bastani-inc/atomic` `main` to `/tmp/atomic-fresh` (LFS binaries skipped). The 2026-08-25 Buffy note is **partially wrong**. Cite the tree.

| Atomic mechanism | Where | Buffy/ADR-2026-08-25 said | Tree says | Take? |
| :--- | :--- | :--- | :--- | :--- |
| **Unrolled repair loop** | `packages/workflows/builtin/loop-until-done-runner.ts:175–258` | Failed node → different-model repairer → re-verify as `on_error` | `iteration-N` worker + `evaluate-N` independent stage, both `context: "fresh"`. Structured evaluator `{done, remaining_work, validation_evidence, failures}`. Durable `progress-ledger.json`. Progress scores **never** stop the run (`PROGRESS_DISCLAIMER`; LLM-verifier spec §3.2). | **Yes.** Pattern, not `on_error`. |
| **Adversarial verify + bounded repairs** | `builtin/adversarial-verification.ts:6–16` | (not named) | Produce candidate, N independent per-criterion verifiers, mean-and-veto gate, `max_repairs` 0–5, parse failure is re-ask not silent fail-vote. | Follow-up (ReviewerSigned). Not Wave 1. |
| **`failureMode: "return"`** | `packages/workflows/README.md:108` | (missing) | Check may fail as `{ok:false, error, attempts, cached}`. Node stays failed. Author continues. Engine **never injects** stdout into the next prompt. Distinct serializable args per rerun (identity). Replay does not re-run the callback. | **Yes.** Our Continue swallows; we need failed-visible + explicit `reads`. |
| **Runtime failure taxonomy** | `workflow-failures-contract.ts:38–73` | Logic/Transient/Blocked/Escalate as one enum | `auth \| rate_limit \| provider \| cancelled \| unknown` plus `retryable`, `resumable`, `recoverability`, `disposition` (`active_blocked` / `terminal_killed` / `terminal_failed`), optional `retryAfterMs`. This is *runtime* recoverability, not "the model got the test wrong." | **Yes, split layers.** Work-quality ≠ runtime. |
| **Durability** | `src/durable/backend.ts:51–78` | "Atomic checkpointing is coarser; no durable child-spawn" | **DBOS is the sole persistent backend.** Tool outputs cached by content hash so resume does not re-execute side effects. In-memory is tests/fallback. Compaction hook (`persistence-compaction-policy.ts:41–71`) re-emits `run.start`/`stage.start` for in-flight runs so restore still sees them. | Keep Resonate. Do not port DBOS. Correct the "coarser" claim. |
| **Graph shape** | `packages/workflows/README.md:78–85` | Linear is sugar over a static DAG | **Imperative TypeScript** `workflow({ run })`. Topology materializes at runtime. Cycles forbidden, checked at execution/replay/DBOS hydration. Not YAML `needs:` desugar. | Take inspectable DAG + unrolled loops. Do not take TS-as-YAML. |
| **HIL** | `README.md` Example 3 `:199–216` | Approval carries the run tree | `ctx.ui.confirm` at the callsite. Best practice: write a **plan file**, pass `reads: [planPath]`. Human input is runtime-only; no declaration-time HIL marker. Durability of prompts is DBOS prompt-reservation tokens (`prompt-reservation-state.ts`). | Take artifact-path evidence. Our Resonate promise already outlasts process death. |
| **`keepContext`** | `src/authoring/keep-context.ts:1–41` | "Verbatim compaction deletes low-value lines" | A pin tag. Compaction must never compress tagged constraints (acceptance criteria, prohibitions, run id). Tags on their own line. Tool-result tags are inert. Protected lines count against the keep target. The line-deletion planner is the coding-agent, not the workflow engine. | Follow-up (CONTEXT_LIMIT ADR). Not this record. |
| **Hashline edits** | `packages/coding-agent/docs/tools.md:7–27` | Deferred | Session-scoped `[path#TAG]` snapshots. Stale tag fails closed unless non-overlapping recovery. Parallel edits sharing a tag batch as one snapshot. | Follow-up. Reservations remain isolation. |
| **Worktrees** | `src/runs/shared/worktree-*.ts`, README `:34–42` | Banned here, correctly | Linked worktrees + symlink dirs + `.worktreeinclude`. Isolation for PR-shaped work. | **Reject.** ADR-0019 Spec 3 + `.beads/` single-writer. |
| **Budgets** | LLM-verifier spec B1–B3 | (missing) | `maxDurationMs` / `maxTokens` / `maxCost`; `budget_exceeded` is a **resumable blocked** rail, not a fail. | Follow-up. |
| **Stop door** | `ralph-review-gate.ts:6–26` | (missing) | `stop_review_loop` boolean is the sole approval door. Recomputing approval from findings arrays deadlocks. Parse failure never approves. Reviewer execution failure never approves. | Take for ReviewerSigned. Do not recompute close from repair-cycle rows. |
| **Prompt reservations** | `durable/prompt-reservation-state.ts` | (missing) | Generation-tombstoned tokens: reserve/release of in-flight HIL prompts. | We already have file reservations. Do not confuse the two. |

### 3.4 The problem statement

> How do we make `run_graph` the only sequencing path, classify runtime failure at the embedding boundary, and express bounded work-quality repair as an unrolled graph (fresh-context worker, independent evaluator, embedding VERIFY, durable ledger, distinct node per cycle) — without new MCP tools, without new Nostr kinds, without worktrees, without the engine running a shell or calling `br close`, and without making a bead identical to a workflow run?

---

## 4. Decision Drivers

1. **Build on the existing DAG scheduler.** `run_graph` already does wave-based concurrent dispatch with deterministic replay. Do not rebuild it. Do not hide a second sequencer inside `on_error`.
2. **No new MCP tools, no new Nostr kinds.** Schema + embedding DI (`VerifySink`) + `DispatchSpec.context`. No 40th tool. No new kind. No new toron CLI noun. No new `br` flag in this record.
3. **One-way dependency.** Flywheel → toron-workflow → toron-core. The engine never imports flywheel, never shells out, never calls `br`. VERIFY and spawn-await live in the embedding (`ActionRunner` is sync mailbox-shaped today; job-await is `Action::Spawn { await_: true }` + `SpawnSink`).
4. **Two failure layers.** Runtime recoverability (rate limit, auth, reservation, cancel, timeout) is classified at the runner boundary. Work-quality (test red, review REJECT) is a graph pattern. One enum for both is how the 2026-08-25 draft overfitted Atomic.
5. **Repair unrolls.** Each cycle is a distinct named node (`repair-2`, `evaluate-2`) with stable identity, matching Atomic `iteration-N` / `evaluate-N`. A recursive `on_error: Repair` on one node is the rejected Option D in disguise.
6. **Fresh context, same tree, one writer.** Evaluator and repairer get `DispatchSpec.context: fresh` (new session, no transcript). They inherit the bead's reservation (extend TTL) or the original pane is interrupted. Never two holders (ADR-0011 HARD). No worktree.
7. **Beads stay the tracker.** A workflow run is an attempt. `br close` is the only closer (ADR-0019 lock 4 / beads ADR-0001). Repair cycles are kind-9 / `br comments`, never a `VerdictKind`.
8. **Evidence over claims, and over prompt-stuffing.** Pass artifact *paths*. Engine never auto-injects check stdout into the next prompt (Atomic README:108). `keepContext`-grade constraints (VERIFY command, bead id, non-goals) stay pinned in the dispatch spec, not hoped-for in a transcript.
9. **TDD.** Failing test before the implementation. First proof: desugared `request_approval` still parks; a 2-cycle unrolled repair succeeds on cycle 2 without `br` or a shell in `toron-workflow`.
10. **`phase_chain.rs` stays.** `check_phase` is structured JSON. Do not encode it as a shell `verify:` string. Retirement is a later flywheel bead.

---

## 5. Considered Options

### Option A′ — Graph-canonical + split failure + unrolled repair pattern + evidence-by-path (CHOSEN, 2026-08-27)

The 2026-08-25 Option A is demoted. A′ keeps the DAG substrate. Runtime failure is classified at the embedding. Work-quality repair is a flywheel-authored unrolled subgraph (or a builtin YAML that compose emits): worker → evaluate → VerifySink, each cycle a new named node, ledger JSON, `context: fresh`, one reservation holder. Approvals carry paths. Beads remain the unit of work.

### Option A — Graph-first + one ErrorKind + `on_error: repair` + bead=workflow + CLI surface (2026-08-25, superseded)

Four waves that hid a loop inside `StepOnError::Repair`, collapsed work-quality into `ErrorKind::Logic`, ran VERIFY in the engine, and made `br close` an engine effect. Rejected after Round-1 debate (§12) and the Atomic tree study: Atomic itself does not implement repair that way.

### Option B — Status quo (graph stays opt-in, no repair, plain approvals, separate lifecycles)

Rejected. The DAG scheduler is unused in production. `fail_run` aborts without trying to fix — every failure becomes a captain interrupt. Approvals are blind — every approval decision is made without evidence. Beads and runs diverge — every close is a dual ceremony the loop bridges by hand. The four gaps in §3.2 remain open.

### Option C — Full Atomic clone (TypeScript runtime, worktrees, session JSONL)

Rejected. Different identity model (session files vs Schnorr keys — Atomic has no cryptographic identity). Worktrees conflict with ADR-0019 Spec 3 and the `.beads/` single-writer invariant. TypeScript runtime conflicts with the Resonate/Rust spine and the one-way dependency. We take the invariants (repair loop, graph-first UX, evidence approvals), not the implementation.

### Option D — Repair as a new `Action::Repair` (not an `on_error` mode)

A new action type that a captain composes into the graph manually: "if build fails, run repair." Rejected. Repair is a **failure mode** of a node, not a standalone action a captain pre-wires. A captain shouldn't have to predict which nodes will fail and wire repair edges by hand for every workflow. The engine routes into repair when a node fails — same as `fail_run` routes into abort. The captain declares the policy; the engine executes it.

### Option E — Repair without a taxonomy (route on error-string matching)

Keep the binary `on_error` and add repair by matching `error` substrings ("flaky", "timeout", "assertion failed") to decide retry vs repair vs escalate. Rejected. String matching is fragile. Runtime `kind` on `{ok:false}` is the substrate for retry/block/cancel. Work-quality repair is the unrolled recipe, not a kind and not substring match.

---

## 6. Decision Outcome

Chosen option: **A′**. The 2026-08-25 Spec 0–6 text is **superseded**. Git history and [§12](#12-counter-argument-round-1--2026-08-27--git-diff-in-file) keep the draft. Implement **only** what follows.

### Spec 0 — Graph-canonical, park-safe (Wave 1)

`graph:` is the internal shape. `steps:` desugars to a chain (`needs: [previous]`). `run_workflow` always calls `run_graph`.

`to_graph()` copies only fields `GraphNode` has today (`action`, `needs`, `guard`, `on_error`). If playbook lint still needs `build_slot` on graph nodes, add that field to `GraphNode` in the **same cut** (`schema.rs:135–149` vs `Step.build_slot` at `:302`). The 2026-08-25 snippet does not compile.

**Same-cut park fix (non-negotiable):** `run_graph_node` currently expands only Parallel / Foreach / Branch / Retry / CallWorkflow (`control_flow.rs:673–680`). `RequestApproval`, `Delay`, and `Spawn` go to the sync `ActionRunner` and do **not** park. Linear `run_step` (`:132–180`) is the park path. After always-`run_graph`, existing `steps:` YAML with `request_approval` (triage, wave-swarm SIGNOFF) would complete without a decision (WF-08 regression). Route those three actions through `run_step` (or run every graph node through `run_step`) **before** deleting the linear path.

**Same-cut `fail_run` predicate:** linear `run_step` fails the run on `ok == false` (`control_flow.rs:235–238`). Graph `run_graph` fails on **any** `error` string field (`:626–635`) — so a successful node that carries an `error` key (or a repair ledger object) would abort. Wave 1 uses **one** predicate on both paths:

```text
ok == false  →  apply on_error
```

Presence of `error` without `ok: false` is not a failure.

**Compat gate:** named `steps.<name>` keys preserved. Unnamed linear steps are omitted from `step_outputs` today; desugar may name them `step_N` and the output map may grow. Document that. Do **not** require byte-identical output.

No Wave-1 CLI noun. Tests may dump `to_graph()` as JSON.

### Spec 1 — Runtime failure at the ActionRunner boundary (Wave 1)

Do **not** replace `ActionRunner` (`Arc<dyn Fn(&Action) -> anyhow::Result<Value> + Send + Sync>`, `engine/mod.rs:198`). Carry classification on the existing error object:

```json
{ "ok": false, "error": "relay timeout", "kind": "rate_limit", "retryable": true, "resumable": true, "disposition": "active_blocked", "retry_after_ms": 5000 }
```

`kind` is optional until every flywheel runner site classifies (`main.rs`, `pane.rs`, `headless.rs`, `crew.rs`, `smart_cron.rs`). Until then, routing uses explicit `on_error` only. Default stays `Continue`. The default-flip is a **later one-line bead**, not Wave 1.

**Runtime kinds** (Atomic `workflow-failures-contract.ts:38–73`, adapted; this is recoverability, not "the model was wrong"):

| kind | means | default |
| :--- | :--- | :--- |
| `timeout` | hung action / deadline | retry (bounded), then escalate |
| `rate_limit` | 429 / quota, may carry `retry_after_ms` | retry |
| `auth` | missing/invalid provider creds | escalate (resumable blocked) |
| `blocked` | reservation held, build slot busy, dep missing | escalate (repair cannot unblock HARD) |
| `cancelled` | operator / abort | terminal, no repair |
| `unknown` | unclassified | explicit `on_error` |

Work-quality (test red, review REJECT, empty artifact) is **not** a kind. It is Spec 2.

**`fail_run` is kind-aware (Wave 1, not a later flip).** `on_error: fail_run` aborts only for non-retryable failure: missing kind, `unknown`, `auth`, `blocked`, `cancelled`, or work-quality `{ok:false}` with no runtime kind. If `kind` is `rate_limit` or `timeout`, **do not abort** even when YAML says `fail_run`. The engine retries that **same node** (same action, same args) with backoff:

- honor `retry_after_ms` when present, else `backoff_secs` on an enclosing `Action::Retry`, else 1s × 2^(attempt-1), cap 60s
- bound: enclosing `Retry.max_attempts` if present, else **3**
- still failing after the bound → escalate (kind-14, `disposition: active_blocked` for rate_limit, terminal for timeout). **Do not** mint `repair-(N+1)`

Lint: R-E1 retry-on-`blocked`/`auth` is an error. R-E2 `kind` missing is a warning until every runner site classifies. R-E3: a `spawn` node with `fail_run` and no enclosing `Retry` is a **warning** (429s still retry via the rule above; the warning tells authors to wrap spawn in `retry` so the bound is explicit).

**Pane vs headless 429 (live gap).** Headless already retries **once** with the registry fallback model (`bd-42fs`, `main.rs:23461–23541`) and does **not** surface `kind`. Wave 1: after that one fallback, if still 429, return `{ok:false, kind:"rate_limit", retryable:true}`. Copy the same one-shot fallback into the **pane** spawn path (today: none). Then the engine retry rule above can fire. Do not treat the fallback retry as a repair cycle.

### Spec 2 — Unrolled repair pattern (Wave 1–2)

Atomic's loop (`loop-until-done-runner.ts:175–258`) is the model: each cycle is two **named** stages, both `context: "fresh"`, plus a ledger file. We do not hide this inside `StepOnError::Repair`.

**Composer / builtin emits this subgraph** (YAML, not engine magic):

```yaml
# cycle identity is in the node name. Do not reuse `verify` across cycles.
# Spawn is wrapped in Retry: a 429 retries THIS node, it does not unroll repair-2.
repair-1:
  action: retry
  max_attempts: 3
  backoff_secs: 5
  step:
    action: spawn
    await: true
    spec:
      profile: "@coder"
      phase: implement
      context: fresh          # new DispatchSpec field: inherit | fresh
      model: "{{inputs.repair_model}}"   # optional different family
      task: |
        Fix remaining work. Ledger: {{inputs.ledger_path}}
        Previous failures: {{inputs.ledger_path}} (read the file; do not trust this prompt dump).
      plan: "{{inputs.bead_id}}"
evaluate-1:
  needs: [repair-1]
  action: retry
  max_attempts: 3
  backoff_secs: 5
  step:
    action: spawn
    await: true
    spec:
      profile: "@reviewer"
      phase: verify
      context: fresh
      task: "Structured evaluation against the VERIFY command and ledger."
verify-1:
  needs: [evaluate-1]
  action: verify
  command: "{{inputs.verify}}"
  on_error: fail_run
  # Action::Verify { command }. fail_run here is work-quality (exit ≠ 0, no kind)
  # or missing sink. kind:rate_limit on Verify is not expected (cargo is not an LLM).
  # If a kind is retryable, Spec 1 still retries despite fail_run.
```

Wave 1 engine support for that pattern:

1. **`DispatchSpec.context: Option<ContextMode>`** (`inherit` default, `fresh` = new session, empty transcript). Fresh is the Atomic default for both worker and evaluator. Different-model (`repair_model`) is optional and still useful; **fresh context is the load-bearing independence**, not the model family.
2. **`Action::Verify { command: String }` + `VerifySink`.** New action variant in `schema.rs` `Action` (serde tag `verify`). Engine calls `VerifySink` (`Fn(&VerifyRequest) -> Result<VerifyResult>`): command, cwd, optional bead id → `{ok, exit, stdout_path, stderr_path}` (paths, bounded, redacted). Missing sink → `{ok:false, error:"verify sink missing"}`. Flywheel implementation reuses `__loop-run-verify` under `mail slot acquire --slot cargo`, writes a **workflow** receipt via `write_receipt` (non-`CommandVerified`). Never `std::process::Command` in toron-workflow. Never `CallWebhook`.
3. **Ledger writer in the embedding** (flywheel), not the engine. JSON at `.flywheel/receipts/<run-id>-progress-ledger.json` (same tree as ADR-0019 receipts; do not grow a second directory). Cycle files use `write_receipt` for non-`CommandVerified` kinds. **Never** `write_command_verified_receipt` from a workflow runner (`verdict.rs:746–776`).

```json
{
  "bead_id": "bd-123",
  "verify": "cargo test -p toron-core reservation_conflict_detection",
  "max_iterations": 3,
  "status": "active",
  "entries": [
    {
      "iteration": 1,
      "node": "repair-1",
      "evaluate_node": "evaluate-1",
      "done": false,
      "remaining_work": "conflict check still compares holder_name",
      "failures": ["test reservation_conflict_detection ... FAILED"],
      "validation_evidence": [".flywheel/receipts/.../verify-1.txt"],
      "receipt_path": ".flywheel/receipts/.../verify-1.txt"
    }
  ]
}
```

4. **Evaluator schema** (structured, fail loud if missing — Atomic `evaluationFrom` throws). Minimum fields: `done: bool`, `remaining_work: string`, `failures: [string]`, `validation_evidence: [string]`. `done: true` without a VerifySink exit 0 is illegal (`prove-it-works`).
5. **No prompt injection.** Next cycle's task names the ledger **path**. The engine does not splice stdout into the prompt (Atomic README:108).
6. **Exhaustion.** After `max_iterations` (recipe input, lint 1..10, Atomic-style; do not invent `FLYWHEEL_REPAIR_MAX_CYCLES` until something reads it), emit kind-14 to captain with the ledger path. Terminal `{ok:false, kind:"unknown", disposition:"terminal_failed", remaining_work: "..."}`. Do not write a pass-shaped `br gate` row.
7. **One writer.** Repairer inherits the failed node's reservation (extend TTL, same `reason=<bead-id>`) **or** the original pane is interrupted first. Two holders is illegal (ADR-0011).
8. **Spawn-await, not DispatchJob.** `Action::DispatchJob` does not await 43001→06. The await primitive is `Action::Spawn { await_: true, no_claim_timeout }` + `SpawnSink` (`control_flow.rs:218–231`, `sinks.rs:31–34`). **Headless refuses spawn** (`main.rs:23756–23762`). Default `workflow run --mode` is headless. **Wave 1 lock:** `repair-until-verified` runs as `--execute --mode pane` (same as `steer`). Headless must **fail loud** naming that flag, never record `{action:"spawn"}` and continue. Headless spawn-await is a follow-up (Spec 5), not a Wave 1 silent path.
9. **`failureMode: return` analogue.** A check node may return `{ok:false}` with the node **visible as failed** in `step_outputs` while the graph continues to the evaluator. Today's `Continue` captures the error but the graph UX treats the run as proceeding-clean. Preserve `ok:false` on that node.
10. **429 / timeout is not a repair cycle.** Mid-work rate limit on `repair-N` or `evaluate-N` retries that spawn (Spec 1 + enclosing `Retry`). It does **not** increment `N`, does **not** write a ledger `remaining_work` row, does **not** spawn a different-model fixer to "fix" HTTP 429. VerifySink `ok:false` with **no** runtime kind (test red) **does** unroll `repair-(N+1)`. After Retry bound on `rate_limit`: escalate, recipe status failed, ledger `status: "blocked"`. Same reservation holder until escalate.

`StepOnError::Repair { ... }` as a hidden engine loop is **rejected** (Option A). Serde today rejects the unknown variant (`schema.rs` tests `:1188–1194`). Compose **always** emits `graph: None` and linear `steps` (`crew.rs:387`) with `on_error: Continue` — that is why live SIGNOFF/plan `request_approval` actually parks. Parts grammar cannot emit an unrolled repair graph (`retry(n):` is `Action::Retry`). Authoring path: a compiled-in recipe token like `wave-swarm` (`crew.rs:357–373`) named `repair-until-verified`, or `compose --from-yaml` with an explicit `graph:`. Optional later: parse-only `on_error: repair` alias that desugars to that recipe. Do not add a third sequencer inside `run_graph_node`. Do not flip compose to `graph:` until Spec 0's park fix is green (otherwise SIGNOFF becomes a silent `{action:"request_approval"}` record).

**Repair IS:** fresh-context worker, independent fresh-context evaluator, embedding VERIFY, bounded, named nodes, ledger path, one reservation, escalate on exhaustion.

**Repair IS NOT:** retry-of-same, engine shell, `br close`, a VerdictKind, a sixth phase, a worktree, silent, prompt-stuffed stdout, a stop decision driven by a progress score, **a 429 / timeout** (that retries the same spawn; it does not unroll `repair-(N+1)`).

**Judgment nodes** (review with no command): do not repair. Use `ReviewerSigned`. Atomic's `ralph-review-gate.ts` stop door is a boolean the reviewer sets; parse failure never approves. Same here.

### Spec 3 — Evidence-bearing approvals (Wave 2)

`request_approval` promise data becomes:

```json
{
  "run_id": "wf-…",
  "workflow": "ship-toron",
  "suspended_at": { "node": "gate", "wave": 5 },
  "question": "Release toron v0.4.0?",
  "context": {
    "nodes_completed": ["…"],
    "nodes_failed": ["repair-1"],
    "nodes_pending": ["merge"],
    "artifacts": [
      { "node": "implement", "kind": "commit", "sha": "abc1234" },
      { "node": "verify-2", "kind": "receipt", "path": ".flywheel/receipts/…/verify-2.txt", "exit": 0 }
    ],
    "ledger_path": ".flywheel/receipts/…/progress-ledger.json",
    "blast_radius": "…",
    "timeout": "PT24H"
  }
}
```

Built from `step_outputs` + graph topology. **Paths, not blobs.** Atomic's HIL is a string (`PendingPrompt.message` in `store-types.ts:112–126`); the README example dumps `plan.text`. The *better* authoring practice is a file + `reads`. We store paths on the promise. The operator CLI (when it exists) `cat`s the ledger; the promise does not carry 50KB of stdout.

**Evidence identity (Wave 2, crash-stable).** Hash `{run_id, node, artifacts[].path, artifacts[].sha, ledger_path}` into the promise payload as `evidence_hash`. Resume that re-hits `request_approval` with a *different* hash is a new question, not a cached yes. Atomic durable UI replays by hash of kind+message+callsite (`durable/ui-primitive.ts`); we hash the *evidence*, so a later commit SHA cannot inherit an earlier approve.

**There is no kind-46011 listener in flywheel.** Zero `ApprovalSink` drains, zero 46011 publishes, zero `resolve_approval` outside tests. Engine comments say "embedding publishes kind-46011"; the embedding does not. Live park is the Resonate promise. `plan` / `wave-swarm` SIGNOFF hang until something calls `WorkflowEngine::resolve_approval` (cron dry-run already documents this and caps at 25s). Wave 1: drain `ApprovalSink` the same way pane drains `SpawnSink`. Wave 2: richer promise data. CLI nouns later. Do not wrap a listener that does not exist.

Reject still records `status: "rejected"` (WF-08: rejection is a decision, not a crash).

### Spec 4 — Bead remains the unit of work (loop SM is not replaced)

`flywheel loop-mode run --work` stays the bead runtime (flywheel skill §3: do not replace). Today: `br ready` → claim → brief → claim-watch → worker `br close` → **loop** `loop_run_verify` (`durable.rs:660–711`) writes `.flywheel/receipts/<id>-<sha>.command-verified.txt` then `br gate report`. That is the legal-close path.

Unrolled repair (Spec 2) lives **inside workflow YAML** (`ship`, `bug-fix`, compose graphs). It is not a second bead dispatcher.

**Anti-forgery, live code (`flywheel/src/verdict.rs`):**

- `CommandVerified` means **the loop executed VERIFY**. `write_receipt` **refuses** that kind (`:746–751`). `write_command_verified_receipt` requires `LOOP_ARMED` (`:767–776`). A worker-invoked `br gate report` on a loop-owned bead is rejected (`:339–341`).
- `legal_close` is exhaustive over `VerdictKind` (`:293–327`). Cheap band (P≥2, Normal, loop-runnable VERIFY) closes on `CommandVerified` **only**. Judgment AC closes on `ReviewerSigned` only. Adding `RepairCycle` would not compile without amending this table — and we do not amend it.

So Spec 2's VerifySink **must not** call `br gate report` at all (not `command-verified`, not `reviewer-signed`). `br gate` is an open append with **no actor ACL** (`beads` `GateCommands`; `--provider` is a free string). Flywheel's `LOOP_ARMED` is process-local and does not bind `br`. A workflow evaluator in the same graph writing `reviewer-signed` is the same class of lie as a worker writing `command-verified`.

VerifySink writes a receipt **path** into the ledger JSON. After the worker closes, the **armed loop** still runs VERIFY and records `command-verified`. In-graph green is how the ship run continues. Loop green is how the bead closes. That duplication is the anti-forgery.

**Time bomb:** `legal_close_input_for_issue` in `beads/src/close_policy.rs` still ignores `issue.verify` / `blast` / `ac_shape` (every bead looks Normal + checkable + empty VERIFY → non-runnable band). Today a forged `command-verified` is **noise**. When schema-v18 wiring lands, cheap-band closes become `command-verified` **only** — and a workflow-written row becomes a legal forged close. Do not ship workflow gate writes "for later." Never write them.

**Reservation inherit-or-interrupt.** `Mailbox::transfer_reservation` exists in toron-core; MCP `file_reservation_paths` accepts `transfer_from`. Flywheel `mail locks` is only reserve/list/show — **no transfer subcommand**. `flywheel interrupt --force` is **wrong**: `--force` allows ctrl+c, which **kills the agent process** (`main.rs:314–328`). Default interrupt is Escape. Repair handoff: wait Idle or Escape-interrupt, then `toron reserve transfer`. Never `--force`. Never commit-on-behalf.

**Receipt paths.** Reuse `.flywheel/receipts/` (`write_receipt` / `write_command_verified_receipt`). A `progress-ledger.json` may sit beside them. Do not invent a second receipt tree.

Drop from this ADR: `br create --auto-workflow`, `workflow_yaml` on the beads schema, "kind-46012 IS the bead close", engine-side close, **loop-dispatch-via-`engine.invoke` as the primary `--work` path**.

A captain may still `workflow run` a graph that cites a bead id. That is opt-in compose/steer, not the loop.

### Spec 5 — Explicitly not this ADR

| Item | Why later |
| :--- | :--- |
| `flywheel workflow tree/connect/fork/cancel` | CLI surface, flywheel ADR. `connect` ≈ Atomic steer-a-live-stage. Valuable, not a type seam. |
| Default `on_error` routed-by-kind flip | Needs every runner site classified. |
| `phase_chain.rs` retirement | Keep `check_phase` JSON contracts. |
| Auto-parallel lint from BLAST_RADIUS | Title was "reservation data"; engine never sees live leases. Lint-only, later. |
| Worktrees, hashline, `keepContext` | Isolation / edit / compaction follow-ups. `keepContext` is the right pin for VERIFY + non-goals once CONTEXT_LIMIT is an ADR. |
| LLM mean-and-veto / Bradley-Terry | Atomic `adversarial-verification` + verifier program. Maps onto ReviewerSigned. Separate. |
| Run budgets (`maxTokens` / `maxCost` / `budget_exceeded` blocked rail) | Atomic B1–B3. We have `FLYWHEEL_COMPOSE_MAX_SECONDS`. Token/cost budgets later. |
| Heartbeats | Atomic 15 min product default. Resonate already durable-sleeps. |
| Natural-language → TypeScript authoring | Atomic writes `workflow({run})`. We have `workflow compose --agent`. Stay YAML. |
| Headless spawn-await | Wave 1 repair is `--mode pane`. Teaching headless to await spawn is a follow-up so default `workflow run` can run the recipe. |

### Spec 6 — Wave 1 files (target zone)

| Path | Change |
| :--- | :--- |
| `crates/toron-workflow/src/schema.rs` | `Workflow::to_graph()` (compile-correct). `DispatchSpec.context`. `Action::Verify { command }`. `GraphNode.build_slot` only if lint needs it. Failure object fields documented on the Value shape, not a new `ActionRunner` type. |
| `crates/toron-workflow/src/engine/control_flow.rs` | always `run_graph`; park RequestApproval/Delay/Spawn; graph `fail_run` uses `ok == false` (not mere `error` key); `Action::Verify` → VerifySink; **no** `Command` / `record_br_gate` / `br close`. |
| `crates/toron-workflow/src/engine/sinks.rs` | `VerifySink` newtype (unique `TypeId`, same pattern as `ApprovalSink`). |
| `crates/toron-workflow/src/engine/mod.rs` | register VerifySink; `Decision` unchanged (`outcome` + `note`). Optional `context` on approval `promise.data` is Wave 2. |
| `crates/toron-workflow/src/lint.rs` | R-E1; desugar-or-reject `on_error: repair`; unnamed-step `step_N` documented. Repair lints do **not** live only in `playbook_lint.rs`. |
| `flywheel/src/pane.rs` + `main.rs` `headless_dispatch` + `cron.rs` | Classify `{kind?}` on `{ok:false}`. Production runners are these three, **not** `crew.rs` / `smart_cron.rs` (test noops). 429 retry already lives *inside* `__run-headless` (`main.rs:23382+`) and does not currently surface `kind: rate_limit`. |
| VerifySink impl | Reuse `__loop-run-verify` / `cmd_internal_loop_run_verify` (`main.rs:25238+`) under `mail slot acquire --slot cargo`. Write a **workflow** receipt, never `command-verified`. `headless.rs` is DispatchSpec helpers, not the VERIFY leaf. |
| `DispatchSpec.context` | Add the field **and** honor it in the same commit: map `fresh` onto `__respawn` / new spawn pane (`pane.rs:755–771` splits if no live registry entry). A field that lands in `extra` unread is dead. |
| `flywheel/src/crew.rs` / `builtin_workflows.rs` | optional: emit unrolled repair subgraph for verify nodes. No required YAML migration. |
| Skills | Layer-3 `workflow-engine.md` only in Wave 1. No Layer-2 dump. No tool-count change. |

`governed-by: ADR-0024` on those Target files.

**Wave 1 ship order (do not skip):**

1. Park-fix + `fail_run`=`ok==false` + `to_graph` (toron-workflow). SIGNOFF still linear until this is green.
2. Drain `ApprovalSink` (flywheel, copy SpawnSink). SIGNOFF stops hanging.
3. `{ok:false, kind?}` at pane / `headless_dispatch` / cron. Pane spawn gets bd-42fs 429→fallback then `kind: rate_limit`. `fail_run` does not abort retryable kinds. Unclassified default Continue.
4. `Action::Verify` + `VerifySink` + `__loop-run-verify` reused, workflow receipt only.
5. Recipe `repair-until-verified` (`--mode pane`; headless fails loud).
6. Tests in §8 Wave 1. No `br gate report`. No new MCP tool. No new kind.

## 7. Implementation Blueprint

### 7.1 Waves

Target files: **§6 Spec 6**. Do not use the 2026-08-25 path table in 7.2 as a landing list.

| Wave | Ships | Gate |
| :--- | :--- | :--- |
| **1** (this ADR) | Compile-correct `to_graph` + always `run_graph` + park RequestApproval/Delay/Spawn. `{ok:false, kind?}` classified at embedding (default Continue). `DispatchSpec.context`. `VerifySink` DI. Unrolled repair pattern (named nodes + ledger JSON + Spawn-await). Lint: desugar-or-reject `on_error: repair`. | §8 Wave 1. No new MCP tool. No new kind. No `br close` in engine. Named-step keys preserved. Approval still parks. |
| **2** | Drain `ApprovalSink` (copy SpawnSink). Then richer promise `context` with artifact **paths** + `evidence_hash`. No new CLI nouns. | SIGNOFF/plan gates resolve. Payload has `nodes_completed`, `artifacts[].path`, `ledger_path`, `evidence_hash`. Reject is a decision. |
| **3** | Loop may `engine.invoke` a composed graph for a ready bead. Terminal may write `command-verified`. Engine still does not close. | Coexistence with bare `br close` green. |
| **later** | tree/connect/fork/cancel CLI, default-kind flip, `phase_chain` retirement, budgets, `keepContext`, hashline, LLM verifier. | Own ADRs. |

### 7.2 Affected paths

See **§6 Spec 6**. The 2026-08-25 table (engine `run_repair_cycle`, `br create --auto-workflow`, eleven CLI nouns) is not the landing list.

### 7.3 Patterns to follow

- `run_graph` / `run_graph_node` / `Action::Spawn { await_: true }` / `SpawnSink` / `ApprovalSink` TypeId newtype. `VerifySink` copies that newtype pattern.
- Unrolled named nodes (`repair-N`, `evaluate-N`, `verify-N`), Atomic `iteration-N` / `evaluate-N`.
- Ledger JSON on disk; next prompt gets a **path**.
- Reservation inherit-or-interrupt, one writer.
- `compose_workflow` as the vehicle for a ready bead (ADR-0022 §3.6). Close stays `br`.

### 7.4 Patterns to avoid

- Engine `std::process::Command`, engine `br gate` / `br close`.
- `StepOnError::Repair` as a hidden loop inside `run_graph_node`.
- `ErrorKind::Logic` (work-quality is a pattern).
- Auto-injecting VERIFY stdout into the next prompt.
- Two reservation holders during repair.
- `RepairCycle` VerdictKind; repair-cycle-succeeded as a legal-close row.
- Worktrees. New MCP tools. New kinds. New toron plane nouns. New `br` flags in this record.
- Progress scores as a stop decision (Atomic forbids this by construction).

### 7.5 Skill & Documentation Drift (token-friendly plan)

Wave 1 skill drift is Layer 3 only (`toron/references/workflow-engine.md`:
graph-canonical, `DispatchSpec.context`, VerifySink, unrolled repair-until-verified
shape, runtime `kind` on `{ok:false}`). No Layer-2 command-token dump (no new
CLI nouns in Wave 1). No `br` skill change. Tool-count stays 40. The 2026-08-25
map below that still mentions `on_error: repair` / eleven CLI verbs / `--auto-workflow`
is **not** Wave 1; ignore it until a later ADR.

#### 7.5.1 Skill Sync Contract applied

| Step | Contract item | ADR-0024 impact |
| :--- | :--- | :--- |
| 1 | `Command` enum (`cli.rs`) — ground truth | `flywheel/src/main.rs` gains `workflow graph/tree/status/connect/resume/fork/cancel/approvals/approve/reject`. `toron-cli` unchanged (standalone fabric). |
| 2 | MCP `#[tool(name)]` | **No change.** 40 tools frozen (38 parity + 2 additive). All new surface is CLI. |
| 3 | AGENTS.md STRUCTURE + CONVENTIONS | CONVENTIONS notes graph-first + `on_error: repair` + `ErrorKind` taxonomy + "no new tools/kinds" (already stated). STRUCTURE unchanged. |
| 4 | Skill files (toron, flywheel, br) | Drift map in §7.5.2. |
| 5 | Tool-count constant | **Unchanged** — 40 stays 40 (no MCP tool added). |
| 6 | `toron catalog --json` + `catalog_freshness` | **Unchanged** — toron CLI catalog does not grow; the new surface is flywheel CLI. Flywheel's *builtin* catalog grows in Wave 1 (compose/recipe tokens), gated by `flywheel workflow lint`, not the toron catalog gate. |

#### 7.5.2 Drift map — exact file × section × change

| Skill file | Section | Change | Layer |
| :--- | :--- | :--- | :--- |
| `toron/references/workflow-engine.md` | `### Step policies`, `### Actions`, `### Composition patterns`, `## Approval gate` | `on_error: repair` variant (`max_cycles`, `repair_model`, `verify`); `graph:` canonical + `steps:` desugar; `ErrorKind` (Transient/Logic/Blocked/Fatal/Escalate) → routed defaults; evidence-bearing approval payload fields; typed stage contracts (`inputs`/`outputs`/`phase`); `timeout_secs`. **Single source of truth for engine semantics.** | Layer 3 (on-demand) |
| `flywheel/references/workflow-authoring.md` | `## Schema`, `### Step policies`, `## Composition patterns`, `## Lint` | Same engine surface, orchestration view: compose emits `graph:`; auto-parallelization lint (disjoint vs overlapping reservation globs). Points to `workflow-engine.md` for semantics rather than duplicating. | Layer 3 |
| `flywheel/SKILL.md` | `## 5. Commands (ops)` — the `workflow {…}` token; `## 1. Classify` table | Append `graph,tree,status,connect,resume,fork,cancel,approvals,approve,reject` to the `workflow {…}` list. One line in §1 noting compose emits `graph:` shape. **~15-20 tokens total.** | Layer 2 (always loaded) |
| `flywheel/references/QUICK-REFERENCE.md` | command crib | Add the new `workflow *` subcommands verbatim. | Layer 3 |
| `toron/SKILL.md` | `## Workflow + @mention` (action list), `## CLI Surface` | One line: graph-first + `on_error: repair` + `ErrorKind` routing exist; point to `workflow-engine.md`. No toron CLI change (engine semantics only). **~10 tokens.** | Layer 2 |
| `~/.agents/skills/br/SKILL.md` | (the `br` skill) | `br create --auto-workflow` flag (Wave 3): generates `workflow_yaml` from VERIFY fence + PRINCIPLES. `br close` triggers the terminal gate. | Layer 2 |
| `toron/references/crew-steering.md` | (unchanged) | Channel/job steering is orthogonal to graph execution. No change. | — |
| `AGENTS.md` (toron) | CONVENTIONS | One clause: graph-first canonical shape, `on_error: repair`, ErrorKind routing. No structural edit. | — |

#### 7.5.3 Token budget (why this stays lean)

The two skills are already near-optimal by the skill-judge rubric: **toron 218
lines, flywheel 274 lines** — both well under the 500-line soft cap, both using
a `Load` table with explicit *read / skip* and *"Do not load every reference"*
guidance (progressive disclosure at its cleanest). The drift therefore obeys
three rules:

1. **Heavy content → Layer 3 only.** The repair state machine, `ErrorKind`
routing table, and evidence-payload field list go into the two on-demand
references (`workflow-engine.md`, `workflow-authoring.md`). They are loaded
already *only when authoring or debugging YAML*, so they absorb the new
tokens without taxing every invocation.
2. **Layer 2 delta is one line per skill.** The always-loaded `SKILL.md` bodies
(and the br skill) gain only the new command tokens — on the order of 15–20
tokens each. No new top-level sections, no restated concepts.
3. **Single source, no duplication.** Engine semantics live in
toron's `workflow-engine.md` (the engine is toron-workflow). Flywheel's
`workflow-authoring.md` adds only the orchestration delta (compose → `graph:`,
CLI commands) and links back. The ASCII DAG and repair flowchart stay in this
ADR — the skills need the *shape*, not the picture.

**Anti-drift guards (what NOT to add):**
- Do not restate "what a repair loop is" (retry → fix → verify is already in
  the model's training; only the `on_error: repair` YAML shape + field names
  are expert knowledge).
- Do not copy the full DAG diagram or repair state machine into either skill
  — that is redundant knowledge delta and pure token waste.
- Do not duplicate the `ErrorKind` routing decision table in both skills.
- Do not touch the 39-tool count or the catalog gate — they are unchanged.

#### 7.5.4 Pre-existing token debt to fix while we are in there (skill-judge finding)

Both skills' frontmatter `description` uses a folded YAML scalar (`>-`). The
skill-maker validator rejects multiline descriptions (`>`/`|`) — minimal YAML
parsers only accept a single-line value. This is a one-line, token-neutral
fix (collapse to a single line) and should land in the same Wave-1 touch. The
extra frontmatter fields (`license`, `domain`, `role`, `scope`, `triggers`,
`metadata`) are non-standard but inert — leave them unless a validator rejects
them; deleting them saves a handful of Layer-1 tokens but risks breaking
harnesses that read `triggers:`.

---

## 8. Verification (acceptance)

### Wave 1

- [ ] `to_graph()` desugars a 3-step linear workflow into a 3-node chain with `needs:` step_0→step_1→step_2. The function compiles (`GraphNode` fields only, or `build_slot` added in the same cut).
- [ ] `run_workflow` always calls `run_graph`. Linear `run_steps` path deleted after the park gate is green.
- [ ] A `steps:` YAML with `request_approval` **parks** (Resonate `Suspended`) when run through `run_graph`. Witness: existing triage / wave-swarm SIGNOFF shape. Without this, do not delete the linear path.
- [ ] Named `step_outputs` keys preserved. Unnamed keys may appear as `step_N` and are documented. No byte-identical requirement.
- [ ] Fake ActionRunner returning `{ok:false, kind:"blocked"}` does not enter the repair subgraph. Lint R-E1 fires on retry-on-blocked.
- [ ] Unrolled 2-cycle graph: `repair-1`/`evaluate-1`/`verify-1` then `repair-2`/… VerifySink fails cycle 1, passes cycle 2. Run succeeded. Ledger JSON has two entries. **No `br` invoked. No `Command` in `toron-workflow`.** `DispatchSpec.context` is `fresh` on those nodes.
- [ ] Cycle 2 task prompt contains the ledger **path**, not the cycle-1 stdout blob.
- [ ] Exhaustion after N: kind-14 (or test sink) names the ledger path; terminal has `remaining_work`; no pass-shaped gate row.
- [ ] Evaluator result missing `done` / `remaining_work` fails loud (not a silent continue).
- [ ] `done: true` with VerifySink exit ≠ 0 is illegal (run does not succeed).
- [ ] `on_error: repair` YAML is rejected at parse or lint (no hidden engine loop; recipe token is the authoring path).
- [ ] Graph `fail_run` uses `ok == false`, same as linear. A node Value that contains `"error"` but `ok: true` does **not** fail the run.
- [ ] `action: verify` with no VerifySink returns `{ok:false, error:"verify sink missing"}`. With a sink, exit 0 → `{ok:true, stdout_path}`; never hits `std::process::Command` in `toron-workflow`.
- [ ] `action: call_webhook` is not a verify alias.
- [ ] Headless `workflow run --execute` of a graph containing `action: spawn` **fails loud** naming `--mode pane`. Does not record `{action:"spawn"}` and continue.
- [ ] Pane-mode 2-cycle repair recipe: cycle 1 VerifySink fail, cycle 2 pass; no `br gate report`; no `command-verified` file from the workflow runner.
- [ ] Spawn on `repair-1` returns `{ok:false, kind:"rate_limit", retryable:true}`: **same node** is retried (Retry / Spec 1 bound). `repair-2` is **not** created. Ledger has no new `remaining_work` row for the 429.
- [ ] Same spawn still 429 after the retry bound: run escalates (kind-14); status blocked/failed; **no** further unroll.
- [ ] VerifySink `{ok:false}` with **no** `kind` (exit ≠ 0): **does** unroll `repair-2`. That is work-quality.
- [ ] `on_error: fail_run` on a spawn that returns `kind: rate_limit` does **not** abort the graph on the first 429.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` on touched crates.
- [ ] `#[tool(name` count == 40. No new kinds in `kinds.rs`.
- [ ] `governed-by: ADR-0024` on §6 Spec 6 Target files.

### Wave 2

- [ ] `request_approval` promise data includes `question`, `context.nodes_completed`, `context.artifacts[].path`, `context.ledger_path`, `evidence_hash`.
- [ ] Resume that re-hits approval with a different `evidence_hash` is a new question, not a cached yes.
- [ ] Reject → `status: "rejected"` (decision, not crash). Approve resumes.

### Wave 3

- [ ] A workflow VerifySink / repair ledger **cannot** write `command-verified` (the worker `write_receipt` helper already refuses it; keep that).
- [ ] After a repaired workflow run, `loop_run_verify` still records CommandVerified (loop-armed). Cheap-band close still requires that row.
- [ ] `legal_close` still has no `RepairCycle` kind (exhaustive match still compiles).
- [ ] Bare `br close` + loop VERIFY path still works. Loop `--work` is not replaced by `engine.invoke` per bead.

### Later (not this ADR's acceptance)

tree/connect/fork/cancel CLI, default-kind flip, `phase_chain` deletion, auto-parallel lint, `--auto-workflow`.

---

## 9. Consequences

### Good

- **The graph is canonical and park-safe.** Captains still write `steps:`; the engine always sequences a DAG; `request_approval` still suspends.
- **Runtime failure is typed** without a flag-day ActionRunner rename. Rate limits retry; HARD reservation blocks escalate; cancel does not "repair."
- **Work-quality repair is visible.** `repair-1` / `evaluate-1` / `verify-1` show up as nodes. The ledger path is the evidence. Fresh context is the independence. VERIFY still runs in the embedding under the build slot.
- **Approvals carry paths.** The operator decides on artifacts, not "ok?"
- **Close legality stays in `br`.** A successful graph can prove a gate row. It cannot close.

### Bad

- **Unrolled repair makes graphs larger.** A 3-cycle repair is 9 nodes, not one node with a hidden loop. That is the point (inspectable) and the cost (verbose YAML). Compose/builtin must generate it; humans should not hand-write `repair-7`.
- **Fresh-context sessions cost tokens.** Bounded by `max_iterations` (1..10) and `FLYWHEEL_REPAIR_MAX_CYCLES` (default 10).
- **`context: fresh` plus same-tree edits needs reservation inherit-or-interrupt.** Getting this wrong is two writers (ADR-0011). Spec 2.7 is load-bearing.
- **Default `on_error` does not flip in Wave 1.** Self-healing is opt-in via the unrolled subgraph (or lint-desugar). Overnight "everything repairs" is not this cut.

### Neutral

- **The `steps:` YAML surface is unchanged.** Captains can still write linear workflows. The engine sees them as chain graphs. No migration needed.
- **Fork is not free.** A forked run is a new Resonate durable function with its own promise store entries and its own kind-46012 terminal. The cost is proportional to the run's remaining depth. Fork is a deliberate operator action, not an automatic behavior.
- **Federation is unaffected.** All graph execution, repair cycles, and approvals run on the local bus. Kind-9 status posts and kind-14 escalation DMs use the existing plane-S and plane-P surfaces.

### Risks

| Risk | Severity | Mitigation |
| :--- | :--- | :--- |
| Repairer makes things worse (introduces a new bug while fixing the first) | Medium | Each cycle re-runs VERIFY. A repairer that fails verify `max_cycles` times stops and escalates. The repairer cannot "succeed" without the verify command passing. |
| Graph desugar breaks a workflow that relied on undocumented step ordering | Low | The chain graph preserves exact sequential order. Named steps with no `needs:` get a lint warning. No auto-parallelization without captain confirmation. |
| Bead-workflow bridge diverges from bare `br close` path | Medium | The bridge is one code path. Both paths produce `br gate report` rows. The workflow path additionally emits kind-46012. The loop consumes both. Coexistence is tested (Wave 3 verification). |
| Fork breaks Resonate replay determinism | Low | Fork copies the serialized `RunInput` up to the fork point. A new run starts from that state with a new `run_id`. The original run's replay is unaffected. Deterministic replay is per-run, not cross-run. |
| Recipe `repair-until-verified` on every verify node, masking systemic failures | Medium | Cap is recipe `max_iterations` (1..10). Exhaustion is kind-14 + `remaining_work`. Frequent exhaustions are a captain signal, not a quieter default. |

---

## 10. More Information

### Intention

A year from now a captain opens the run, sees `repair-1 ✗` / `repair-2 ✓` / `verify-2 ✓`, reads `.flywheel/receipts/…/progress-ledger.json` (`remaining_work` empty, VERIFY exit 0), and approves. `br close` still happens through the legal-close table, with a `command-verified` row the loop wrote. Nobody pasted a 50KB log into a prompt. Nobody called `br close` from `control_flow.rs`.

### Principles cited in this ADR

| Principle | Where it applies |
| :--- | :--- |
| `prove-it-works` | Repair cycles re-run VERIFY. Approvals carry evidence. The graph shows node states, not agent claims. |
| `sequence-verifiable-units` | Each named cycle (`repair-N` / `evaluate-N` / `verify-N`) is a unit. Evidence is a receipt **path** + ledger JSON, not a `br gate` row. |
| `fix-root-causes` | Repair fixes the root cause, not the symptom. Retry-of-same is `Action::Retry` (transient failures only). |
| `encode-lessons-in-structure` | The unrolled recipe is the structure. `on_error: repair` as an engine loop is rejected. Compose `retry(n):` stays same-step retry. |
| `never-block-on-the-human` | Recipe `max_iterations` then kind-14. Judgment nodes escalate, they do not auto-repair. |
| `build-the-lever` | The lever in Wave 1 is VerifySink + ledger path + `loop_run_verify`. `workflow tree/connect/fork` is a later CLI ADR. |
| `type-system-discipline` | `StepOnError` stays `Continue \| FailRun`. `Action::Verify` is a new action, not a sixth phase. `Phase` stays five values. |
| `laziness-protocol` | The DAG scheduler already exists. This ADR extends it — no new engine, no new kinds, no new MCP tools. Delete the old `steps:` path after migration. |
| `migrate-callers-then-delete-legacy-apis` | `run_workflow`'s old `steps:` path is deleted in Wave 1 after backward-compat tests pass. |

### Related

- `docs/notes/2026-08-25-atomic-analysis.md` — Buffy's full-stack comparison of Atomic vs our stack (what to adopt, what to keep)
- ADR-0005 Spec 4b — the existing DAG scheduler (`run_graph`) this ADR promotes to canonical
- ADR-0019 Spec 1 — VERIFY fence, `br gate`, legal-close table. Repair does **not** add a VerdictKind.
- ADR-0022 §3.1 — `Phase` enum (graph nodes carry phase)
- ADR-0022 §3.4 — one durable execution rail (`engine.invoke` for compose + steer). Beads stay on `--work`; they do not join this rail as runs.
- ADR-0023 — NIP-29 39000/39002 (channel create/close; run channels use these for status posts)
- bastani-inc/atomic — `loop-until-done` (bounded repair inspiration), `fan-out-and-synthesize` (parallel fan-out with evidence), `/workflow connect` (live run streaming)

### Revisit

- **Verbatim compaction** (Atomic's context management): the repairer's context window is bounded by `CONTEXT_LIMIT`. Smarter compaction — deleting low-value transcript lines without rewriting surviving text — is a general improvement, not repair-specific. Deferred to a separate ADR.
- **Permission gate hooks** (Atomic's tool-call interception, blocking dangerous shell commands before execution): our reservation system + pre-commit guards already prevent most dangerous writes. Runtime command interception is defense-in-depth. Deferred.
- **Hashline-style content verification** (Atomic's hashline edits anchored to file hashes): file reservations already prevent concurrent writes. Hash verification on commit would catch a repairer who violated the reservation. Deferred for defense-in-depth.
- **Event-driven loop promotion** (ADR-0019 wave 4): when the loop is event-driven (herdr reactor primary, no poll), kind-46012 is a *run* wake, not a bead close. `--work` still closes through `br` + loop VERIFY.
- **Headless spawn-await:** so default `workflow run` can run `repair-until-verified` without `--mode pane`.

---

### Wave 1 ship proof (recorded 2026-08-29 — satisfies the ADR-0025 Spec 0 cross-repo gate)

**bd-adr24-w1-graph-canonical-f0z9 CLOSED.** Commits: `32ebae8` (toron-workflow: `to_graph`, always-`run_graph`, park path, `ok==false` predicate, `Action::Verify` + `VerifySink`, `DispatchSpec.context`, lint R-E1/E2/E3) and `542cffe` (flywheel: `fresh` context honored at the spawn split, `src/pane.rs`).

Park-proof witness: `engine::tests::park_desugared_request_approval_still_parks` — linear `steps:` SIGNOFF desugars to a chain (`needs` asserted), runs through Resonate, parks (handle not done), resumes on `resolve_approval` with the decision projected into `step_outputs`. Predicate witness: `failure_kind_ok_false_is_failure_error_without_ok_is_not`. Sink witness: `verify_sink_missing_returns_error`. Approval decision/reject: `approval_suspends_and_resumes`, `approval_rejection_is_a_decision_not_a_failure`.

Captain verification (TealDune, 2026-08-29): full `cargo test -p toron-workflow` 203/203 green; `cargo clippy -p toron-workflow --all-targets` 0 warnings; `cargo fmt --check` clean; `cargo test -p toron-cli catalog` green (surface frozen; no new tools/kinds); no `std::process::Command` and no `br` calls under `crates/toron-workflow/src/`. Known deviation: the bead's original VERIFY filters (`graph_desugar`, `park`) matched zero test names — the substance landed under the names above; bead record amended.

## 11. Appendix: Governing scenarios

These replace the 2026-08-25 before/after YAML. They match §6. `--execute --mode pane` unless noted.

### A — `goal`: do not abort the ship on a red test

**Today:** `on_error: fail_run` on build. Flaky `cargo test` kills the run. Captain re-dispatches.

**Wave 1:** after `build`, compose recipe `repair-until-verified` (unrolled nodes). Review is judgment: no repair. Gate is `request_approval` with paths (parks only after Spec 0). SIGNOFF still needs ApprovalSink drain.

```yaml
name: goal
steps:
  - name: ideate
    action: call_workflow
    workflow: brainstorm
  - name: plan
    action: call_workflow
    workflow: phase-plan
  - name: build
    action: dispatch_job
    spec: { profile: "@coder", phase: implement, stage: systematic-execute }
    on_error: fail_run
  - name: repair
    action: call_workflow
    workflow: repair-until-verified
    inputs:
      bead_id: "{{inputs.bead_id}}"
      verify: "cargo test --workspace"
      max_iterations: 3
      repair_model: "{{inputs.repair_model}}"
    # Inner spawn is Retry-wrapped (Spec 2). Outer fail_run is only if the
    # recipe exhausts work-quality cycles or retryable-kind bounds.
  - name: review
    action: dispatch_job
    spec: { profile: "@reviewer", phase: review, stage: code-review }
    on_error: fail_run
  - name: gate
    action: request_approval
    message: "Goal complete. Ledger {{steps.repair.ledger_path}}. Approve ship?"
```

`--mode pane`. Headless of this graph fails loud on `spawn` inside the recipe.

After a worker `br close` on a cited bead, **loop** `loop_run_verify` still writes `command-verified`. The recipe never `br gate report`.

### B — `bug-fix`: repro stays fail_run; the fix loop unrolls

**Today:** graph already; every node `fail_run`. Prove REJECT aborts.

**Wave 1:** `repro` stays `fail_run` (no repair without a repro). `tdd`/`fix` go through `repair-until-verified` with the bead VERIFY command. `prove` is ReviewerSigned territory: one `dispatch_job` `@reviewer`, `fail_run`, no inner repair.

```yaml
name: bug-fix
graph:
  nodes:
    repro:
      action: dispatch_job
      spec: { profile: "@scout", phase: resolve, task: "Write the repro." }
      on_error: fail_run
    fix:
      action: call_workflow
      workflow: repair-until-verified
      needs: [repro]
      inputs:
        verify: "cargo test {{inputs.test_filter}}"
        max_iterations: 3
    prove:
      action: dispatch_job
      needs: [fix]
      spec: { profile: "@reviewer", harness: opencode, phase: review }
      on_error: fail_run
```

Park-safe: this `graph:` only ships after Spec 0 (else `dispatch_job` is fine as graph leaves; `call_workflow` already expands). Do not put `request_approval` on a graph node until park-fix is green.

### C — `ship`: concurrent review+verify is a later DAG; Wave 1 does not retire `phase_chain`

**Today:** `run_ship_chain` + `check_phase` JSON contracts. Review then verify, sequential.

**Wave 1:** `ship` still goes through `invoke_or_ship_chain`. Do **not** encode §4.5 as `action: verify` shell strings.

**Later (after Spec 0 park-fix):** a graph-authored `ship` may run review ∥ verify (`needs: [implement]` both) and `gate` with `needs: [review, verify]`. Until then, compose stays linear so SIGNOFF parks.

### D — a bead is not a workflow run (the rejected story)

**Does not hold.** `--work` stays claim → brief → close-detect → `loop_run_verify`. No `br create --auto-workflow`. No `workflow_yaml` on the issue. No engine `br close`.

A captain **may** `workflow run bug-fix --inputs bead_id=bd-123` as opt-in. That run's VerifySink writes a workflow receipt. The bead still closes only after the worker `br close --commit-sha` and the **armed loop** records `command-verified`.

### E — `weekly-review` cron: retry transients; do not hang on approval

**Today:** `on_failure: bead` on first failure. Graph already has parallel scout fan-out.

**Wave 1:** scout arm timeout / 429 is `kind: timeout` / `rate_limit` → same-node `Action::Retry` (Spec 1), not a repair cycle. Oracle empty artifact (work-quality): `repair-until-verified` with `verify: test -s <artifact>` (existence, not `/tmp/jq`). **No** `request_approval` on the cron path until ApprovalSink is drained (cron dry-run already hangs 25s). Exhaustion of work-quality cycles: existing cron `on_failure: bead`. Exhaustion of 429 retries: same, plus kind-14.

### F — migration contract

Existing `steps:` YAML keeps parsing. Compose keeps emitting linear `steps:` + `graph: None` until Spec 0 park-fix is green. Then optional `repair-until-verified` recipe. No silent `on_error` default flip. No `to_graph` byte-identical output gate.

---

## 12. Counter-Argument (Round 1 — 2026-08-27 — git-diff in-file)

> Historical. Captain then asked for a live Atomic-tree study. **§6 A′ is the governing decision.** This section is the Round-1 seam critique that A′ accepted.

> Captain directive: carefully review and debate, edit in same file, do
> better. Counter verified against the **live working trees** of `toron`
> (`control_flow.rs`, `schema.rs`, `engine/mod.rs`, `engine/sinks.rs`) and
> `flywheel` (`phase_chain.rs`, `phase_contracts.rs`, `builtin_workflows.rs`)
> on 2026-08-27. Skills: adr-skill checklist, adversarial-reviewer, accretive
> + dominance, glaze (one escalation), latent-diver.

### Verdict

**AGREE-WITH-CHANGES.** The Atomic lesson this ADR should take is the
**loop invariant**: fail → different-model fix → re-verify → bound →
escalate. Graph-canonical desugar, evidence-bearing approvals, and an
`ErrorKind` at the ActionRunner boundary all serve that invariant.

The Atomic lesson this ADR must **not** take is the **process shape**.
Atomic is one runtime. We are a three-crate spine. "The engine is not yet
an execution runtime" and "a bead IS a workflow run" port the process
shape and fight ADR-0005 / ADR-0019 / the one-way dep this record claims
not to amend.

| Claim in this ADR | Counter |
| :--- | :--- |
| Engine must become an execution runtime | **Reject.** Substrate is the product. Repair/VERIFY/close belong in the embedding. |
| Bead IS a workflow run | **Reject.** Bead = unit of work (ADR-0019). Workflow = one durable attempt. |
| Wave 1 ships graph + taxonomy + repair + phase_chain retirement + viz | **Split.** Three type-seam items, not five subsystems. |
| `to_graph` + always `run_graph` is a no-behavior change | **False.** Graph nodes do not park `request_approval`. Unnamed steps gain output keys. Snippet does not compile (`build_slot`). |
| `Verdict` gains `RepairCycle` | **Reject.** Legal-close table lives in the beads fork. Repair is an event log. |
| ADR-0022 §4 gains a repair *transition* | **Reject.** Repair is `StepOnError`, not a sixth phase. |
| `run_repair_cycle` in `control_flow.rs` shells out + `br gate` + awaits DispatchJob | **Unsound.** ActionRunner is sync mailbox-shaped; job-await is `Action::Spawn { await_: true }`; VERIFY/build-slot live in flywheel; `br` is another repo. |
| `check_phase` → `on_error: repair { verify: "<shell>" }` | **Precision loss.** §4.5 is structured JSON. Keep `check_phase`. |
| Default `on_error` flips to routed-by-kind | **Defer** until every runner site classifies. Warn-first is not self-healing. |
| Spec 5 reads reservation data | **Title lies.** Mechanism is BLAST_RADIUS globs. Lint-only, later wave. |

### C0 — Spec 0 as written is a silent approval-park regression

- **recommended_change:** Before deleting the linear `run_steps` path, route
  `RequestApproval`, `Delay`, and `Spawn` through `run_step` from
  `run_graph_node` (extend the `is_control_flow` match at
  `control_flow.rs:673–680`, or run every graph node through `run_step`).
  Drop the byte-identical output gate; require named-step key preservation
  plus "RequestApproval still parks" as the compat gate.
- **evidence_of_defect:** `run_graph_node` only expands Parallel / Foreach /
  Branch / Retry / CallWorkflow. `RequestApproval` is a leaf dispatched to
  the sync `ActionRunner`. Linear `run_step` (`control_flow.rs:132–180`) is
  the path that creates the Resonate promise and parks. `triage.yaml` and
  flywheel `wave-swarm` SIGNOFF use `request_approval` on `steps:`. After
  "always `run_graph`", those runs no longer suspend. `GraphNode` has no
  `build_slot` (`schema.rs:135–149`); the Spec 0 snippet does not compile.
  Unnamed linear steps are omitted from `step_outputs`; desugar names them
  `step_N` and the output map grows.
- **confidence:** High (first-hand tree).
- **minimum_acceptable_fix:** graph-node control-flow match includes
  RequestApproval/Delay/Spawn; `to_graph` only copies fields `GraphNode`
  has (or add `build_slot` in the same cut); compat gate rewritten.
- **do_not_broaden_into:** a new engine, new kinds, observability CLI.
- **remediation_posture:** Narrow. Same-cut fix on the Spec 0 landing.

### C1 — Layering: `run_repair_cycle` as written inverts the one-way dep

- **recommended_change:** Engine loops. Embedding executes. Repairer spawn
  is `Action::Spawn { await_: true, no_claim_timeout }` (the await
  primitive that already exists, `schema.rs:570`, `control_flow.rs:218–231`,
  `SpawnSink` in `sinks.rs:31–34`). VERIFY is an injected `VerifySink`
  (`Fn(&str) -> Result<VerifyResult>`), same DI idiom as `ActionRunner` /
  `ProfileResolver` / `WorkflowResolver` (`engine/mod.rs:198–209`: "The
  ENGINE never imports flywheel"). Cycle log is kind-9 and/or `br comments`.
  No `std::process::Command`, no `br gate`, no `br close` in
  `toron-workflow`.
- **evidence_of_defect:** `ActionRunner` is
  `Arc<dyn Fn(&Action) -> anyhow::Result<Value> + Send + Sync>` — sync,
  mailbox-shaped. `DispatchJob` does not await 43001→06. Schema says
  `build_slot` is "a static contract marker. Execution semantics live in
  the embedding" (`schema.rs:296–300`). `record_br_gate` /
  `escalate_to_captain` / `run_verify` in `control_flow.rs` would make
  toron-workflow a br client and a shell runner, contradicting Driver 3
  and the "Does not amend the one-way dependency" header.
- **confidence:** High.
- **minimum_acceptable_fix:** Spec 1 rewritten onto Spawn-await + VerifySink;
  engine-side br/shell deleted from the pseudocode.
- **do_not_broaden_into:** async ActionRunner, a new job-await action, a
  toron-workflow br crate.
- **remediation_posture:** Structural-small. The DI seam already exists.

### C2 — "A bead IS a workflow run" reopens ADR-0019 lock 4

- **recommended_change:** Rewrite Spec 3. A bead remains the unit of work.
  A workflow run is one durable attempt *for* a bead. The terminal event
  may carry evidence the loop uses to write `command-verified` /
  `reviewer-signed`. The engine does not call `br close`. Drop
  `br create --auto-workflow` and `workflow_yaml` on the beads schema
  from this ADR (other repo; beads ADR-0001 owns schema v18). Keep:
  loop may `compose_workflow` + `engine.invoke` when a ready bead is
  dispatched (vehicle, not identity).
- **evidence_of_defect:** ADR-0019: "The unit of work is a **bead**. Proof
  is a **ledger row**." Lock 4 (2026-08-22, bd-54ui) moved legal-close
  into the beads fork so an illegal close is unrepresentable. Spec 3 rule 3
  ("On approval, the engine … calls `br close`") creates a second close
  authority. Spec 3 rule 5 then admits two paths and calls drift a
  residual risk in §9. `RepairCycle` is not a `VerdictKind`
  (`CommandVerified` | `ReviewerSigned` | unit-test-verified |
  live-verified | worker-receipt). A `repair-cycle-succeeded` gate row
  that could legal-close is a false verdict.
- **confidence:** High.
- **minimum_acceptable_fix:** Spec 3 identity sentence deleted; engine
  `br close` deleted; `RepairCycle` dropped from Amends; Wave 3 demoted
  to a follow-up ADR or to "loop invokes a graph for a bead."
- **do_not_broaden_into:** a BeadRuntime noun collapse (ADR-0019 wave 5,
  evidence-gated, not due).
- **remediation_posture:** Spec amendment. Narrower than "do not integrate";
  the dispatch-vehicle half is keepable.

### C3 — Wave 1 is five decisions; Spec 6 is foundational but sequenced last

- **recommended_change:** Wave 1 (this ADR) is exactly: (1) compile-correct
  `to_graph` + always `run_graph` + C0 park fix, (2) `{ok:false, error, kind}`
  at the runner boundary with embedding classification, (3)
  `StepOnError::Repair` over Spawn-await + VerifySink. Everything else
  (phase_chain retirement, default `on_error` flip, bead identity, eleven
  CLI nouns, auto-parallel lint, fork/cancel, engine shell) leaves this
  record.
- **evidence_of_defect:** Agent-readiness fails: an implementer cannot start
  without asking which of five Wave-1 ships lands first. Spec 6 says it
  feeds Spec 1, then Wave 1 ships Spec 0+1+6+phase_chain+viz together.
  Default-flip is "warn-first until a one-line later bead" — so the
  self-healing-by-default claim is not in Wave 1. Title is four ANDs
  (graph-first, repair loops, evidence approvals, bead-workflow).
- **confidence:** High.
- **minimum_acceptable_fix:** retitle to a verb phrase about repair-on-DAG;
  Wave table cut to the three type-seam items; Spec 6 moved above Spec 1
  without the default flip.
- **do_not_broaden_into:** rewriting the appendix scenarios (they can stay
  as *later* illustrations once the seam is right).
- **remediation_posture:** De-scope.

### C4 — `ErrorKind` cannot be recovered from `anyhow`; the DI rename is a flag day

- **recommended_change:** Do not replace `ActionRunner`. Carry `kind` on the
  existing `{ok:false, error}` object. Embedding classifies at each runner
  site (mailbox/relay timeout/429 → Transient; completed-agent `{ok:false}`
  → Logic; reservation conflict / slot busy → Blocked; approval reject /
  repair exhausted → Escalate). Until every site classifies, `kind` is
  optional and routing uses explicit `on_error` only. Reconcile §7.5.2
  `Fatal` with Spec 6.6.1's four-variant enum (pick four).
- **evidence_of_defect:** `ActionRunner` is used from flywheel `main.rs`,
  `pane.rs`, `headless.rs`, `crew.rs`, `smart_cron.rs`. A new `ActionResult`
  type is a flag-day. `anyhow::Error` has no kind. The ADR's own migration
  gate admits the default flip is a behavior change; "warn-first" means
  Wave 1 does not route.
- **confidence:** High.
- **minimum_acceptable_fix:** kind-on-value, not a new runner type; default
  stays Continue; Fatal dropped or defined.
- **do_not_broaden_into:** a new error crate, colored errors, thiserror in
  the engine.
- **remediation_posture:** Narrow.

### C5 — Retiring `phase_chain` via shell `verify:` loses the §4.5 contract

- **recommended_change:** Keep `flywheel/src/phase_contracts.rs` `check_phase`
  as the structured completion contract (JSON four-fields / verdict set /
  gate exit). Optionally *call* it as a graph guard over node output JSON.
  Do not encode `br dep cycles --json | count == 0` or
  `git log -1 --grep=<bead-id>` as `repair.verify`. Those are different
  predicates, and `verify:` is a shell string (injection + PATH + cwd).
  `run_ship_chain` retirement is a flywheel bead, not an engine bead, and
  not Wave 1.
- **evidence_of_defect:** `check_phase` (`phase_contracts.rs:272`) inspects
  structured step output. Spec 6.6.2 table replaces it with shell one-liners
  that do not parse the four resolve fields or the verdict enum. `ship`
  YAML today is `steps:` of `call_workflow` links; the gated runner is
  flywheel-side (`builtin_workflows.rs:178`, `phase_chain.rs:214`).
- **confidence:** High.
- **minimum_acceptable_fix:** delete Spec 6.6.2 from Wave 1; keep
  `check_phase`; document that ship concurrency (review ∥ verify) is a
  later graph-authored `ship`, not a chain-runner rewrite.
- **do_not_broaden_into:** deleting `phase_chain.rs` in the same PR as
  `StepOnError::Repair`.
- **remediation_posture:** De-scope.

### C6 — Same-tree repair has no reservation-transfer protocol

- **recommended_change:** State the isolation rule: the repairer *inherits*
  the failed node's reservation (extend TTL, same `reason=<bead-id>`); the
  original pane is interrupted or the repair is the same job continuation
  (`Spawn` await on the same assignee, different model). Two writers on
  one tree is the failure mode ADR-0019 Spec 3 exists to prevent. R-R5
  (different model family) does not address file conflict.
- **evidence_of_defect:** Spec 1.6 "the repairer works in the same tree,
  under the same reservations" plus DispatchJob to a *new* agent. Held
  reservations are HARD (ADR-0011). A second agent on the same paths
  either conflicts (progress stuck, `ErrorKind::Blocked` — which Spec 6
  then *escalates*, skipping repair) or the original holder is assumed
  gone with no interrupt. Unspecified.
- **confidence:** High (protocol gap, not a line-number bug).
- **minimum_acceptable_fix:** one paragraph in Spec 1: inherit-or-interrupt;
  never two holders.
- **do_not_broaden_into:** lifting the worktree ban.
- **remediation_posture:** Spec amendment (small).

### C7 — Examples violate the lint this ADR writes (R-R2)

- **recommended_change:** Every `on_error: repair` example carries a
  non-empty `verify:`. Judgment nodes (review, scout synthesis) do not
  use `repair`; they use `ReviewerSigned` / escalate. Cron
  `verify: "test -n '$(cat /tmp/weekly-review-result.json | jq …)'"` is
  not `prove-it-works` (tmp path, no receipt).
- **evidence_of_defect:** Scenario A `review` repair has no `verify:`.
  Scenario C `review` repair has no `verify:`. Scenario E `scout-fanout`
  repair has no `verify:`. R-R2 is Error on empty verify.
- **confidence:** High.
- **minimum_acceptable_fix:** patch the appendix YAML, or mark those nodes
  `on_error: fail_run` / escalate.
- **do_not_broaden_into:** rewriting the captain-experience transcripts.
- **remediation_posture:** Documentation correction.

### C8 — Wrong seams in the Amends line

- **recommended_change:** Amends ADR-0005 Spec 4b only (graph canonical +
  `StepOnError::Repair`). Does not amend ADR-0019 Spec 1. Does not amend
  ADR-0022 §4. CLI freeze note scoped to toron plane-surface nouns; new
  `flywheel workflow *` nouns and `br create --auto-workflow` named as
  follow-up surface in their own records.
- **evidence_of_defect:** ADR-0022 §4: "The vocabulary is closed (5 values).
  New phases require an ADR amendment, not a string." A "repair
  transition" is either a sixth phase or a confusion of phase with
  `on_error`. ADR-0019 `VerdictKind` is the legal-close table in the
  beads fork; this repo does not own it. Header "Does not amend … freeze
  on new CLI plane-surface nouns (ADR-0020)" is true for toron and false
  for the eleven flywheel nouns in Spec 4 plus a beads CLI flag in Spec 3.
- **confidence:** High.
- **minimum_acceptable_fix:** rewrite the Amends / Does-not-amend lines.
- **do_not_broaden_into:** a new ADR-0025 for CLI observability (file it
  separately if Wave 4 still wanted).
- **remediation_posture:** Spec amendment (small).

### Soundness ledger

| claim_id | claim_or_obligation | kind | witness_required | witness_status | preservation | progress | inhabitance | evidence | minimum_acceptable_fix |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| S-park | Graph `request_approval` still parks after always-`run_graph` | preservation | test: steps YAML with approval suspends via `run_graph` | **unwitnessed** (code routes the other way) | broken if Spec 0 lands as written | stuck (run completes without a decision) | RequestApproval-as-leaf is an illegal inhabitant of the graph path | `control_flow.rs:673–680` vs `:132–180` | C0 |
| S-await | Repairer result is awaited, not fire-and-forget | progress | Spawn-await (or equivalent) in the cycle | **unwitnessed** (pseudocode uses DispatchJob) | n/a | stuck (cycle cannot observe the fix) | DispatchJob-as-await is uninhabitable today | `Action::Spawn { await_ }` exists; DispatchJob does not | C1 |
| S-verify | Engine does not run shell; VERIFY is embedding-side | refinement | VerifySink DI; no `Command` in toron-workflow | **contradicted** by `run_verify` in Spec 1 | would break Driver 3 | n/a | shell-in-engine is an illegal inhabitant of this crate | `schema.rs:296–300`; `ActionRunner` shape | C1 |
| S-close | Only `br` legal-close closes a bead | preservation | engine never calls `br close` | **contradicted** by Spec 3 rule 3 | broken (second closer) | n/a | engine-close is an illegal inhabitant of lock 4 | ADR-0019 lock 4; beads ADR-0001 | C2 |
| S-kind | Failure kind is classified, not parsed from anyhow | refinement | `{ok:false, kind}` at every runner site | **partial** (enum proposed, runner type flag-day) | default-flip would not preserve Continue | n/a | unclassified anyhow has no kind | `engine/mod.rs:198` | C4 |
| S-phase | Phase vocabulary stays five values | inhabitance | no `repair` phase, no §4 transition | **strained** by Amends line | would reopen ADR-0022 closure | n/a | repair-as-phase is illegal | ADR-0022 §4 | C8 |
| S-tree | Same-tree repair has one writer | preservation | inherit-or-interrupt reservation rule | **unwitnessed** | two holders break ADR-0011 HARD | Blocked→escalate skips repair | two-holder is illegal | Spec 1.6 vs ADR-0011 | C6 |

### Complexity delta

Overall: **increases** as written (incidental). **Neutral-to-reduces** under
A′ (below).

| vector | as written | under A′ |
| :--- | :--- | :--- |
| control-flow | two runners + repair SM + default-flip + phase_chain retirement | one DAG path + one repair loop on existing Spawn-await |
| state | bead metadata `workflow_yaml` + dual close | none (bead schema untouched) |
| coupling | engine→br, engine→shell, engine→job-await | engine→VerifySink (one DI, existing idiom) |
| config-surface | `FLYWHEEL_REPAIR_MAX_CYCLES`, default flip, ship-gate-mode remapping, eleven CLI nouns | max_cycles + env cap only |
| API-surface | ActionRunner type break + br flag + flywheel verbs | `{ok:false, kind}` additive; `StepOnError::Repair` additive |
| operational-surface | fork/cancel/connect, crash-resume of bead-runs, two close paths | repair exhaust → existing kind-14 escalate |
| cognitive-load | four-AND title, Spec 6 listed last, "bead IS run" | one invariant, two objects (bead, run) |

Essential: typed failure, bounded different-model repair, graph-canonical,
evidence on approval. Incidental: identity merge, engine-as-runtime,
shell-encoded phase contracts, default-flip, observability CLI in the same
record.

### Invariant ledger

| invariant | tier | status as written | blast |
| :--- | :--- | :--- | :--- |
| flywheel → toron-workflow → toron-core, engine never imports flywheel | critical | **strained** (C1) | cross-cutting |
| ADR-0011 held reservation is HARD; one writer | critical | **strained** (C6) | module |
| ADR-0019 lock 4: illegal close unrepresentable in `br` | critical | **broken** if Spec 3 rule 3 lands (C2) | cross-cutting |
| ADR-0022 closed Phase vocabulary (5) | major | **strained** (C8) | module |
| `request_approval` parks (WF-08) | critical | **broken** if Spec 0 lands without C0 | module |
| MCP 40 / kind registry frozen | major | preserved | local |
| worktree ban | major | preserved (correctly rejected Option C) | local |

### Foot-gun register

| foot-gun | ease | detectability | note |
| :--- | :--- | :--- | :--- |
| always-`run_graph` without parking RequestApproval | high | subtle (run "succeeds" with no decision) | C0 |
| `on_error: repair` on a judgment node with no `verify:` | high | obvious once R-R2 ships; silent until then | C7 |
| default `on_error` flip after warn-first | high | silent (continue → retry/repair overnight) | C4 |
| `repair-cycle-succeeded` gate row legal-closing a bead | medium | subtle | C2 |
| two agents in one tree during repair | high | subtle (reservation conflict or clobber) | C6 |
| `verify:` shell string as phase contract | medium | subtle (wrong predicate still exit 0) | C5 |
| Spec 5 titled "from reservation data" | medium | subtle (lint on prose globs) | title |

### Non-material

- ASCII tree glyphs (◌✓✗⏸↻) are fine for a later CLI ADR.
- Skill token-budget §7.5 is well-aimed; it is not Wave 1.
- Folded YAML `description: >-` on skills is real pre-existing debt; do not
  couple it to this decision.
- Verbatim compaction / hashline / permission gates correctly deferred.
- Option C (Atomic clone) and Option D (Repair as Action) rejections stand.
- Option E (string-match routing) rejection stands; that is why `ErrorKind`
  is the right substrate, classified at the embedding, not parsed from
  `e.to_string()`.
- Evidence-bearing approval payload (Spec 2) is small and correct; it can
  ride with Wave 1 once C0 lands (the payload is just richer
  `promise.data`).

### Verification gaps

- No test named that proves graph `request_approval` parks.
- No test that a repairer DispatchJob is *awaited* (because it cannot be).
- "Output byte-matches" is unsatisfiable (unnamed-step keys).
- "One full bead → workflow → repair → gate → `br close`" e2e is a beads-repo
  + flywheel + engine test; it is not a Wave 1 gate.
- `phase_chain.rs` retirement gate ("same §4.5 outcomes") cannot hold if
  contracts become shell strings.

### Residual uncertainty

- Whether flywheel `ActionRunner` sites can classify Transient vs Logic vs
  Blocked without a small shared helper. Fastest check: grep `{ok: false`
  in `flywheel/src/pane.rs` and `headless.rs` and list the error strings
  they already produce. That list is the first `ErrorKind` map.
- Whether `VerifySink` should take the bead VERIFY fence or the node's
  `repair.verify` string. Lean: the node's string, which the composer
  copies from the fence. The engine never parses markdown.
- Whether review-node failure is Logic (repair) or Escalate (human). Lean:
  Escalate. Review is judgment; `ReviewerSigned` already exists.

### Change Agenda (changes to this ADR, low → high leverage)

1. **change:** Patch appendix YAML so every `repair` has `verify:`; judgment
   nodes escalate (C7). **why:** R-R2. **posture:** doc
2. **change:** Rewrite Amends: only ADR-0005 Spec 4b. Drop RepairCycle
   VerdictKind and ADR-0022 repair transition. Scope the CLI freeze (C8).
   **why:** wrong seams. **posture:** spec amendment
3. **change:** Compile-correct `to_graph`; graph nodes park
   RequestApproval/Delay/Spawn; drop byte-identical gate (C0). **why:**
   Spec 0 as written is a WF-08 regression. **posture:** narrow
4. **change:** `ErrorKind` as `{ok:false, kind}` on the existing runner
   value; no ActionRunner rename; no default flip in Wave 1; drop Fatal
   or define it (C4). **why:** flag-day + unclassifiable anyhow. **posture:**
   narrow
5. **change:** Keep `check_phase`; move phase_chain retirement out of Wave 1;
   do not encode §4.5 as shell (C5). **why:** precision. **posture:** de-scope
6. **change:** Spec 1 isolation paragraph: inherit-or-interrupt, never two
   holders (C6). **why:** ADR-0011. **posture:** spec amendment
7. **change:** Spec 1 onto Spawn-await + VerifySink; delete engine shell /
   `br gate` / `br close` (C1). **why:** one-way dep. **posture:** structural-small
8. **change:** **Rewrite Spec 3: workflow proves the bead; the bead remains
   the unit of work. Engine never closes. Drop `--auto-workflow` /
   `workflow_yaml` from this record (C2).** **why:** lock 4. **posture:**
   spec amendment; strongest next move. Without it this ADR reopens the
   legal-close program ADR-0019 just moved into `br`.

### Option A′ — substrate-preserving repair (the better decision)

Chosen if the captain ACCEPT-MODIFIED. Four ships, each independently
useful, none inverting the spine.

**Title:** Adopt bounded different-model repair on the canonical DAG, with
typed failure at the ActionRunner boundary.

**Wave 1 (this ADR, toron-workflow + flywheel runner sites):**

1. `Workflow::to_graph()` copies `action`, `needs`, `guard`, `on_error`
   (add `build_slot` to `GraphNode` in the same cut if playbook lint
   still needs it). `run_workflow` always calls `run_graph`. Graph nodes
   run through `run_step` so RequestApproval parks, Delay sleeps, Spawn
   awaits. Compat gate: named `steps.<name>` keys preserved; approval
   still suspends; unnamed keys may appear as `step_N` and are documented.
2. Failure object is `{ok:false, error, kind?}`. Embedding classifies.
   `StepOnError` stays Continue | FailRun until every runner site sets
   `kind`. Lint R-E1..E3 may warn. Default does not flip.
3. `StepOnError::Repair { max_cycles, repair_profile, repair_model, verify }`.
   On `{ok:false, kind: logic}` (or explicit `on_error: repair`), the
   engine runs a bounded loop: `Action::Spawn { await_: true }` with the
   model override, then `VerifySink(verify)`. Success → node output is
   the repair result. Exhaustion → `{ok:false, kind: escalate}` and the
   embedding mails the captain (existing kind-14 path). Cycle log: kind-9
   in the run channel and/or `br comments`. Not a VerdictKind.
4. `request_approval` promise data includes `nodes_completed`, artifacts
   already in `step_outputs`, and any repair cycle log. No new kind. No
   new MCP tool.

**Non-goals of this ADR (follow-ups, not this record):**

- Bead identity merge, `br create --auto-workflow`, `workflow_yaml`
- `flywheel workflow tree/connect/fork/cancel/approvals` CLI
- Auto-parallelization lint
- Default `on_error` routed-by-kind flip
- `phase_chain.rs` retirement
- Engine shell, engine `br close`
- ADR-0022 phase vocabulary change
- ADR-0019 VerdictKind change

**Repair IS / IS NOT, corrected:**

```diff
  Repair IS:
    a different model, fresh context, bounded, re-verified, tracked, escalated on exhaustion
-   tracked as a br gate VerdictKind
+   tracked as a cycle log (kind-9 / br comments). Close legality stays br's table.
  Repair IS NOT:
    retry-of-same, unbounded, a worktree, a self-report, silent
+   a shell the engine runs, a br close, a sixth phase, a second unit of work
```

**First proof signal (Wave 1, one test):** a 2-node graph whose first node
returns `{ok:false, kind:"logic"}` with `on_error: repair { max_cycles: 2,
verify: "true" }`; a fake SpawnSink succeeds on cycle 2; VerifySink exit 0
on cycle 2; run status succeeded; no `br` invoked; RequestApproval on a
desugared `steps:` YAML still parks.

### Bottom line

Take Atomic's *loop*. Do not take Atomic's *process*. The DAG scheduler,
Spawn-await, Verify-as-DI, `br` legal-close, and file reservations are
already the right spine. This draft's load-bearing defect is treating
those as gaps to close by collapsing layers. Cut Spec 3's identity claim,
move VERIFY/`br` out of `control_flow.rs`, fix Spec 0 so approvals still
park, and Wave 1 becomes a decision an agent can implement without asking
which of five ships is the ship.

Disagree with the *direction* (graph-first, typed failure, bounded repair,
evidence on approval)? No. Disagree with the *seam*? Yes. The seam is the
decision.

---

## 13. Atomic tree study (2026-08-27) — what we took, what we left

Clone: `/tmp/atomic-fresh` from `https://github.com/bastani-inc/atomic` (`main`, LFS skipped). Package of record: `packages/workflows`.

### Taken (now in §6)

1. **Unroll the loop.** `loop-until-done-runner.ts:175–258` names `iteration-N` and `evaluate-N`. Both `context: "fresh"`. Structured evaluator. Ledger JSON. `remaining_work` on exhaustion. Progress scores are advisory (`PROGRESS_DISCLAIMER`; verifier spec forbids them as a kill switch).
2. **Do not inject check output into the next prompt.** `packages/workflows/README.md:108` — `failureMode: "return"` keeps the node failed, author continues, evidence is passed explicitly, distinct args per rerun, replay does not re-execute.
3. **Split runtime failure from work-quality.** `workflow-failures-contract.ts` kinds are `auth|rate_limit|provider|cancelled|unknown` plus `retryable/resumable/disposition`. That is recoverability. Test-red is a pattern, not a kind.
4. **Artifact paths in HIL.** Example 3 writes `plan.md` and `reads: [planPath]`. Our promise carries paths. Resonate still outlasts the process (Atomic HIL uses DBOS prompt-reservation tokens).
5. **Stop doors stay boolean and fail-closed.** `ralph-review-gate.ts:6–26` — `stop_review_loop` is the sole approval; parse failure never approves. Maps to `done: true` requiring VerifySink exit 0, and to ReviewerSigned not `on_error: repair`.

### Left (follow-up ADRs, not this record)

| Atomic | Why not now |
| :--- | :--- |
| Imperative TypeScript `workflow({ run })` | We stay YAML. Compose can unroll. |
| DBOS durable backend | We have Resonate. Buffy "coarser checkpointing" was **wrong** (DBOS + hashed tool cache); we still do not port DBOS. |
| Worktrees | ADR-0019 Spec 3, `.beads/` single-writer. |
| Hashline `[path#TAG]` | Reservations isolate writers; hashline is edit-staleness. Later. |
| `keepContext` pin tags | Right idea for VERIFY/non-goals vs CONTEXT_LIMIT. Own ADR. |
| Mean-and-veto LLM verifier, Bradley-Terry tournament | ReviewerSigned follow-up. |
| `budget_exceeded` resumable blocked rail | We have compose wall-clock. Token/cost later. |
| Steer a live stage / `/workflow connect` | flywheel CLI ADR. |
| Heartbeats (15 min product default) | Resonate already durable-sleeps. |
| NL → authored workflow file | `workflow compose --agent` exists. |
| Prompt-reservation tokens | File reservations are a different object. Do not conflate. |

### Buffy note corrections (so the next agent does not re-import them)

- Repair is **not** `on_error` of a node. It is an unrolled graph.
- Atomic **does** have durable functions (DBOS). "JSONL session files only" is false for workflows.
- "Verbatim compaction" in the workflow package is `keepContext` + re-emit `run.start` on compact. Line-deletion is the coding-agent.
- Approval does **not** magically serialize the run tree; authors pass files.
- "Linear is sugar" in Atomic means imperative TS, not YAML `needs:` desugar.
- `max_repairs` default is **2** (adversarial-verification), loop-until-done default **5** (cap 20). Our cap stays 10 via env; YAML 1..10.

---

## 14. Flywheel × toron × beads optimality pass (2026-08-27)

Round-1 + Atomic study were **not** enough. This pass reads live `flywheel/src/{verdict,durable,pane,crew}.rs` and `br close`/`br gate`. **A′ as previously written was not optimal.** Spec 4 is patched above. The rest of §6 stands.

| A′ claim | Live stack | Patch |
| :--- | :--- | :--- |
| Workflow terminal may write `command-verified` | **False verdict.** Loop-only, `LOOP_ARMED`, `write_receipt` refuses the kind (`verdict.rs:339–341, 746–776`). Cheap band `legal_close` is CommandVerified exclusive (`:298–304`). | VerifySink writes a workflow receipt. Loop still runs `loop_run_verify` after close-detect (`durable.rs:660–711`). |
| Loop may `engine.invoke` a graph per ready bead | **Would replace the loop SM.** Skill §3: do not replace. `--work` is claim → brief → claim-watch → close-detect → loop VERIFY. | Repair unrolls inside **workflow YAML** (`ship` / compose / `bug-fix`). Not a second dispatcher. |
| New receipt tree under `progress-ledger.json` | Receipts already live at `.flywheel/receipts/<id>-<sha>.<kind>.txt`. | Same directory. Ledger file is additive. |
| Inherit-or-interrupt as a new protocol | Toron `transfer_reservation` exists; flywheel mail locks do **not** wrap it. `interrupt --force` kills the process. | Escape/Idle + `toron reserve transfer`. |
| `compose retry(n):` as repair | That grammar is `Action::Retry` (same step, same model) (`crew.rs:161, 260`). | Do not desugar repair into `retry(n):`. |
| Spawn-await as a new primitive | **Already production.** `drain_spawn_sink` (`pane.rs:649–663`): `await: true` waits for Idle then settles; `await: false` fire-and-forget. | Use it. |
| `DispatchSpec.context: fresh` | Loop CONTEXT_LIMIT respawn is a **different** fresh (whole pane). No per-node empty-transcript flag. | Still add the field; do not confuse it with loop respawn. |
| Workflow may `br gate report` (any name) | `br gate report` is an open append. No loop-only ACL in `br`. `--provider` is a free string. `LOOP_ARMED` is flywheel-process only. | Workflow **never** `br gate report`. Receipt path + comments/kind-9 only. |
| `reviewer-signed` from the graph evaluator | Same-graph "independent" review is a false `ReviewerSigned`. Legal only for judgment AC from a different-family verifier. | Evaluator output is ledger JSON. Loop/captain records `reviewer-signed` if the legal-close table says so. |
| `legal_close` already consumes VERIFY/blast | **Stub.** `beads/src/close_policy.rs` `legal_close_input_for_issue` ignores those fields. Today forged `command-verified` is noise; post v18 it is the only cheap-band close. | Do not write workflow gate rows "to get ready for v18." |

**What was already optimal and stays:** park-safe `to_graph`; runtime `kind` on `{ok:false}` without renaming ActionRunner; unrolled named repair nodes; engine never shells out; engine never `br close`; no RepairCycle VerdictKind; no worktrees; no eleven CLI nouns in Wave 1.

| Wrap existing 46011 listener | **No such listener.** Flywheel never drains `ApprovalSink`, never publishes 46011, never `resolve_approval` outside tests. SIGNOFF parks on the Resonate promise until something settles it. | Wave 1: drain ApprovalSink (copy SpawnSink). Wave 2: richer `.data`. CLI later. |
| `interrupt --force` as repair handoff | `--force` = ctrl+c = **kill agent process** (`main.rs:314–328`). | Escape or wait Idle, then `toron reserve transfer`. |
| Compose emits unrolled repair / `graph:` | Compose always `graph: None`, linear `steps`, `on_error: Continue` (`crew.rs:387`). That is why SIGNOFF parks. `retry(n):` is `Action::Retry`. | Recipe token `repair-until-verified` (wave-swarm pattern) or `--from-yaml`. Do not force `graph:` until park-fix is green. |
| Classifier sites include `crew.rs` / `smart_cron.rs` / `headless.rs` | Production: `pane.rs`, `main.rs` `headless_dispatch`, `cron.rs`. `headless.rs` is phase helpers. | Fix the file list. |
| `FLYWHEEL_REPAIR_MAX_CYCLES` | **Does not exist.** Related: `FLYWHEEL_COMPOSE_MAX_SECONDS` (3600), `FLYWHEEL_SHIP_GATE_MODE`. | Cap in recipe inputs (Atomic-style `max_repairs`) plus a new env only if we actually read it. |
| Wave-swarm VERIFY = loop VERIFY | Wave-swarm VERIFY is `@reviewer` `dispatch_job` (`builtin_workflows.rs:1187–1194`). Loop VERIFY is close-detect `__loop-run-verify`. | Do not collapse them. |

**What was already optimal and stays:** park-safe `to_graph`; runtime `kind` on `{ok:false}` without renaming ActionRunner; unrolled named repair nodes; engine never shells out; engine never `br close`; no RepairCycle VerdictKind; no worktrees; no eleven CLI nouns in Wave 1; Spawn-await via existing `drain_spawn_sink`.

**Verdict:** A′ **with Spec 2–4 patched from this flywheel pass**. Ship graph-park + classified `{ok:false}` + ApprovalSink drain first. Repair YAML is a `wave-swarm`-style recipe, not a new sequencer. Without the park fix, making the graph canonical **breaks live SIGNOFF**.

### Extra, from the isolation/compaction pass (same clone)

- **Worktrees never merge.** Parallel `worktree: true` creates `<repo>/.atomic/worktrees/<runId>-<i>`, diffs against the base commit into `worktree-diffs/task-<i>-<agent>.patch`, then `git worktree remove --force` + branch delete (`packages/subagents/src/runs/shared/worktree.ts`). Requires a **clean** tree (`status --porcelain --untracked-files=no`). No `.beads` handling exists in that repo. Another reason Spec 5 rejects them: we need commits on the live branch under one reservation, not a patch pile that is thrown away.
- **Verbatim compaction is mechanical deletion.** Planner returns `start,end` line ranges only. Reconstruction walks original lines; `keepContext` lines are split *out* of the deletion set after the planner, so the model cannot rewrite a survivor (`packages/coding-agent` compaction docs + `deleted-ranges.ts`). Last-rung fallback is `"fresh"` (drop the compactable region, no rewrite). Workflow-side hook still only re-emits `run.start` so restore sees in-flight runs.
- **Permission gates are fail-open.** Tools run as the user. Optional `pi.on("tool_call")` example regex-blocks `rm -rf` / `sudo`. Builtin bash interceptor is *routing* (`cat`/`sed -i` → `read`/`edit`), not a sandbox. We already have reservations + pre-commit guards; do not import a fail-open regex as a security story.
- **Identity is a uuidv7 JSONL session file.** Intercom is a length-prefixed Unix-socket broker (`~/.atomic/agent/intercom/broker.sock`), group isolation is a string compare. Not Schnorr, not NIP-17, not cross-machine.
- **Later ideas, still not Wave 1:** warm-first prefix-cache fan-out before parallel judges; parent-ask that *terminates* the child (parent must spawn a new run, not keep a mailbox open); Bradley-Terry `select_best` instead of knockout.

### Extra, from the DAG-core pass (same clone)

- **No declarative `needs:` / `graph()` authoring.** Topology is inferred at runtime by `GraphFrontierTracker` (`engine/graph-inference.ts:17–82`): spawn snapshots the unsettled frontier as parents; settle replaces those parents with this node. Linear is `ctx.chain`, fan-out is `ctx.parallel`. We keep explicit YAML `needs:`. Do not port frontier inference; it is a TypeScript-await affordance and it is un-lintable as a static DAG.
- **Three failure layers, matching §6's split.** (A) `ctx.tool` retries are **same node, same args** (default throw after 1; opt-in `maxAttempts` 3) — this is our `Action::Retry`, not repair. (B) Engine classifies provider/auth into `retryable/resumable/disposition`; resume is **explicit** `/workflow resume`, not automatic. (C) Repair is author `for` loops minting new names. There is **no** engine `on_error: repair` and **no** engine `escalate()` API.
- **Ralph/Goal stop door is a boolean, exhaustion is human.** Ralph default 10 loops; both reviewers must `stop_review_loop === true`. Exhaustion + `create_pr` opens a **draft PR** and stops. Goal consecutive identical blocker → `blocked`. Maps to our escalate-with-ledger-path, not to engine `br close`.
- **Heartbeats are not durable.** Default 15 min; process exit does **not** backfill missed beats. Do not sell heartbeats as a Resonate substitute.
- **`ctx.tool` replay is content-hash of name+args**, not Resonate yield identity for arbitrary JS. Bare `await fs.writeFile` is **not** checkpointed. Only `ctx.*` writes durable checkpoints. Our equivalent: ActionRunner + VerifySink + Spawn-await. Do not checkpoint random embedding side effects.
- **Connect/steer is live Store snapshots + JSONL `workflow.*` entries**, not a structured evidence approval. `/workflow connect` opens a graph overlay. Valuable; still a flywheel CLI ADR.