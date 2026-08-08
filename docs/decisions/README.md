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
| **P4** | Smart cron (self-reporting + defined fixer; durable-sleep manager) | **In progress** (bd-p4-* epics open) |
| **P5** | Engine flexibility (graph DAG, control-flow, lint-graph) | **Complete** (bd-p5-* epics closed; spike GATE passed) |
| **P6** | Squads + observability (squad/recipe unify, runs view + per-run cost, robot-snapshot runs) | **Complete** (bd-p6-squads-observability-61td closed) |

P4 (smart cron) is the remaining open phase under ADR-0005.

// governed-by: ADR-0003
