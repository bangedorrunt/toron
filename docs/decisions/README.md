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
| **P4** | Smart cron (self-reporting + defined fixer; durable-sleep manager) | **In progress** (P4.1 schedule-ext ✅; P4.2 spawn-await + promise↔job binding ✅; P4.3 cron-mgr ✅ (BlueOak); P4.4 bead-on-fail ✅ decision layer (BlueOak) — flywheel side-effect wiring open; P4.5 sagas open; P4.6 cron CLI ✅ (EmeraldSwan); P4.7 tests: SC rows ✅ green, FW rows blocked on P4.4 wiring) |
| **P5** | Engine flexibility (graph DAG, control-flow, lint-graph) | **Complete** (bd-p5-* epics closed; spike GATE passed) |
| **P6** | Squads + observability (squad/recipe unify, runs view + per-run cost, robot-snapshot runs) | **Complete** (bd-p6-squads-observability-61td closed) |

P4 (smart cron) is the remaining open phase under ADR-0005. Cross-phase
verification (mine, 2026-08-08): ADR-0003 `verify-gates.sh` passes; toron-workflow
lib suite 110/110 green; ADR-0004 e2e suite (p1/p2) fully green — the
`request_approval` suspend regression from the 0.6 migration is fixed
(ApprovalSink/SpawnSink TypeId collision, BlueOak P4.2 lane). P4.7 SC rows green.
**P4.7 harness (2026-08-08):** shared cron fixtures (`tests/common/mod.rs`) +
scenario skeletons for the remaining P4 rows landed — `sc22_cron_failure_bead`,
`sc23_cron_escalate`, `sc24_cron_catchup` are now GREEN (P4.4 decision layer +
P4.3 catch-up, BlueOak). `sc21_cron_durable_loop` (BlueOak) covers
`cron_success` + `cron_restart`. sc22/sc23 assert the POLICY DECISIONS (sink
events); the flywheel side-effect execution (`br create` / kind-14 DM) is the
embedding's wiring.

// governed-by: ADR-0003
