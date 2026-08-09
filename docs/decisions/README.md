# Architecture Decision Records (ADR)

This directory contains records of the major architectural decisions made for the `toron` project.

| ID | Title | Status |
| :--- | :--- | :--- |
| [0001](0001-project-foundation.md) | Project Foundation and Core Stack | **Superseded** by [ADR-0003](0003-nostr-mailbox-fabric.md) |
| [0002](adopt-agent-adapter-architecture.md) | Adopt Agent Adapter Architecture | **Partially superseded** by [ADR-0003](0003-nostr-mailbox-fabric.md) (MCP adapter retained; Sidecar → `Backend` trait) |
| [0003](0003-nostr-mailbox-fabric.md) | Pivot to a Nostr-Native Multi-Backend Agent Mailbox | **Accepted** |
| [0004](0004-operational-hardening.md) | Operational Hardening & Agent-Automation Efficiency (Post-Parity) | **Accepted** |
| [0005](0005-orchestration-layer.md) | Orchestration Tier — Profiles, Workflows, Stages, Smart Cron, and a Flexible Workflow Engine | **Accepted** (2-round adversarial review + 3 captain amendments) |
| [0006](0006-blossom-blob-federation.md) | Blossom / NIP-B7 Federated Blob Serving | **Accepted** (bd-esh.5) |
| [0007](0007-post-implementation-hardening-and-api-redesign.md) | Post-Implementation Hardening, Bug Ledger, and Backend/API Redesign | **Accepted** (Rev 3; impl e23b + residual epic `bd-adr7-residual-vxnv` closed 2026-08-08 — captain gate `.4`; flywheel se2.3/c3b.1 remain out-of-epic; see [Verification](0007-post-implementation-hardening-and-api-redesign.md#verification-acceptance-for-this-adr)) |
| [0008](0008-comprehensive-codebase-review-charter.md) | Comprehensive Codebase Review Charter — Tracks, Verification Gates, and Debate Protocol | **Accepted** (Rev 1, 2026-08-09; opencode debate crew AGREE-WITH-CHANGES — both counters in `docs/review/0008-debate-round1.md`; captain resolution `docs/review/0008-captain-resolution.md`; 20 items resolved 19-ACCEPT/1-REJECT; all 11 Soundness Ledger gates ran GREEN; NIP-40 ingest probe added as P0-candidate) |

## Parity catalogs

| Catalog | Scope |
| :--- | :--- |
| [`docs/parity/am-rust-tool-catalog.md`](../parity/am-rust-tool-catalog.md) | AM-Rust 38-tool / 25-resource surface (names, args, counts — the frozen contract) |
| [`docs/parity/known-divergences.md`](../parity/known-divergences.md) | Intentional response-shape divergences (npub vs id, ts_us vs ISO, event-id returns) |

## ADR-0005 phase status

Executable plan lives in the `bd-*` beads under `bd-adr5-orchestration-tier-qez`; the
ADR is the spec, the beads are the plan. Phase completion is recorded here as the
epics close.

| Phase | Scope | Status |
| :--- | :--- | :--- |
| **P0** | Failure propagation (ActionRunner `Result<Value>`, `on_error: fail_run`, kind-46012 `status=failed`) | **Complete** (bd-p0-* epics closed) |
| **P1** | Backend trait unification (ONE trait, call-site migration) | **Complete** (bd-p1-* epics closed) |
| **P2** | Profiles + built-in library (6 roles, 30013 content-map, `Profile::resolve`) | **Complete** (bd-p2-* epics closed) |
| **P3** | Unified workflow engine (one vocab, one engine, stages, auto-crew, workflow CLI) | **Complete** (bd-p3-* epics closed) |
| **P4** | Smart cron (self-reporting + defined fixer; durable-sleep manager) | **Complete** (bd-p4-smart-cron-xvbj closed — all 7 children: schedule-ext, spawn-await+promise↔job binding, cron-mgr durable-sleep, bead-on-fail, sagas→workflows, cron CLI, tests) |
| **P5** | Engine flexibility (graph DAG, control-flow, lint-graph) | **Complete** (bd-p5-* epics closed; spike GATE passed) |
| **P6** | Squads + observability (squad/recipe unify, runs view + per-run cost, robot-snapshot runs) | **Complete** (bd-p6-squads-observability-61td closed) |

**ADR-0005 beads P0–P6 closed** (umbrella rollup 37/37 descendants). Unit/
scenario suites for those beads are green (`verify-gates.sh`; workflow lib;
e2e p1/p2/p4 matrix sc18–sc24). **However:** a full-scope post-implementation
review (2026-08-08) found **material integration gaps** that overclaim
"production complete" — NIP-17 `message_posted` trigger path closed by
construction, flywheel mail trait still AM-shaped (`u64` ids), per-pid
ephemeral in-process mailbox, `foreach` item drop, `call_workflow` stub,
workflow-emitted stamp not applied in production, coalescer flush no-op.
See **[ADR-0007](0007-post-implementation-hardening-and-api-redesign.md)**
(proposed) for the bug ledger, soundness ledger, and redesign (MailSurface +
MessageId + TriggerIngress + durable roots). Do not treat P0–P6 bead closure
as fixed-point until ADR-0007 Change Agenda is green or features are demoted.

// governed-by: ADR-0003, ADR-0007 (proposed)
