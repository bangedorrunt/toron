# Atomic × Toron/Flywheel/Beads: Deep Analysis

**Date:** 2026-08-25
**Agent:** Buffy (Codebuff)
**Context:** Full-stack analysis of bastani-inc/atomic against the toron/flywheel/beads automation stack. Findings inform ADR-0024.

## 1. What Atomic Gets Right

### Repair Loops (P0 lesson)

Atomic's killer feature: a stage that fails routes into a repair loop — fix → verify → fix → verify up to N iterations, different model for repair, escalate to human on exhaustion. Our `on_error` is binary (continue/fail_run). `Action::Retry` retries the SAME step on the SAME model. We cannot self-heal a failing build without captain intervention.

**ADR-0024 Spec 1** adopts this as `on_error: repair`.

### Graph-First UX (P0 lesson)

Atomic's `workflow({...})` is a DAG from day one. Linear is sugar. Runtime renders the graph, shows node states, lets you connect to a live run, fork at a checkpoint. Our DAG scheduler (`run_graph`) exists but is opt-in; `steps:` is the primary mental model and every built-in uses it.

**ADR-0024 Spec 0** makes `graph:` the canonical shape; `steps:` desugars to a chain graph at parse time. Spec 4 builds the observability surface.

### Evidence-Bearing Approvals (P1 lesson)

Atomic's approval gates carry the run state: which nodes passed, which artifacts exist, what the specific question is. Our `request_approval` sends a plain `message` string. The operator sees "ok?" with no context.

**ADR-0024 Spec 2** makes approvals carry structured context built from the run's accumulated state.

### Verbatim Compaction

Atomic deletes low-value transcript lines without rewriting what survives. Our context management is coarse — `CONTEXT_LIMIT` at 80% triggers a full respawn. Deferred to a separate ADR (general improvement, not repair-specific).

### Worktree-Isolated Parallelism

Atomic uses git worktrees per parallel agent so concurrent edits cannot collide. We ban worktrees (ADR-0019 Spec 3) and use file reservations (hard/soft conflict detection). Different philosophies, same goal. Our reservation system is the right answer for a system with `.beads/` and `Cargo.lock` as single-writer invariants. Atomic's worktrees work for a PR-based model where each PR IS the isolation boundary.

### Safety (Hashline Edits, Permission Gates)

Atomic's hashline edits anchor to file hashes so a file changed behind the model's back fails loudly. Their permission gate hooks intercept dangerous shell commands before execution. We have file reservations (prevent concurrent writes) and pre-commit guards (prevent dangerous patterns). Deferred for defense-in-depth; our existing mechanisms cover the common cases.

## 2. What We Get Right

| Our strength | Why Atomic doesn't have it | Why it matters |
| :--- | :--- | :--- |
| **Nostr-native identity** | Session files, not cryptographic identity | Every event is signed; audit trail is cryptographically verifiable |
| **Resonate durability** | Checkpointing, not durable functions | Crash → resume from last checkpoint; promise-based suspension/approval |
| **Bead dependency graphs** | File-based todos, not a graph | `br dep` + `br ready` + `br plan` is a real work graph with cycle detection |
| **21 engineering principles as structure** | None | Principles are lint-checked, typed, and encoded in profiles/lints/preflight |
| **File reservations** | Worktrees (separate copies) | Hard/soft conflict detection without cloning the repo |
| **Coalition orchestration** | Intercom (session messaging) | Multi-crew coordination with intent/held protocols |
| **Cron as distinct type** | None | Scheduled maintenance with failure policies, distinct from ad-hoc workflows |
| **Closed phase vocabulary** | None | 5 phases (resolve/plan/implement/review/verify), lint-enforced, harness-mapped |

## 3. The Gap We're Closing

The DAG scheduler exists (`control_flow.rs` `run_graph` — wave-based concurrent dispatch, `needs:` edges, deterministic replay). It is the second-class citizen. The graph should be the primary shape. Failure should route into repair. Approvals should carry evidence. Beads and runs should be one lifecycle.

ADR-0024 closes these four gaps on the existing spine. No new engine. No new Nostr kinds. No new MCP tools. The DAG scheduler is already there — we make it the canonical path and add the three missing capabilities around it.