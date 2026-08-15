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
| [0009](0009-daemon-lifecycle-and-resonate-selection.md) | Daemon Lifecycle, Autostart, and Resonate Server Selection | **Accepted** (2026-08-09; `toron daemon` subcommand group; passphrase-file secret handling; remote→local→embedded Resonate selection; supersedes ad-hoc `com.flywheel.toron`) |
| [0010](0010-execution-wiring-model-registry-headless.md) | Execution Wiring, Model Registry, Role Conditioning, and Headless Mode | **Accepted** (2026-08-13; implemented + live) |
| [0011](0011-coalition-orchestration.md) | Coalition Orchestration — Steer + Collab as Layers, Intent-Based Coordination | **Accepted** (2026-08-13; implemented + live) |
| [0012](0012-standalone-cron-type.md) | Standalone Cron Type — a scheduled primitive distinct from workflow | **Accepted** (2026-08-13; implemented + live; amends ADR-0005 Spec 3) |
| [0013](https://github.com/bangedorrunt/toron.dev/blob/main/docs/decisions/0001-website-architecture-and-feature-presentation.md) | toron.dev Website Architecture and Feature Presentation | **Moved** (2026-08-14) to the toron.dev repo, renumbered **ADR-0001** — site decisions live in the site repo |
| [0014](0014-kind-registry-specification.md) | Kind Registry Specification — formal spec of every custom Nostr kind | **Accepted** (2026-08-14; amends ADR-0003) |
| [0015](0015-flywheel-toron-integration-contract.md) | Flywheel ↔ Toron Integration Contract — the boundary guarantees | **Accepted** (2026-08-14) |
| [0016](https://github.com/bangedorrunt/toron.dev/blob/main/docs/decisions/0002-site-repo-spec-sync-and-hosting.md) | toron.dev Site Repository, Spec-Sync Pipeline, and Hosting Tier | **Moved** (2026-08-14) to the toron.dev repo, renumbered **ADR-0002** (catalog sync pipeline + Vercel Hobby→Pro triggers govern the `toron catalog` extractor) |
| [0017](0017-project-root-canonicalization.md) | Project Root Canonicalization and Identity Resolution | **Accepted** (2026-08-15; S2 keystone — canonical short slug + alias table, one root per project, aliased read-only migration, ack authorization via pane binding / thread participant; supersedes the bd-cmgu key-basename fallback as the primary mechanism) |
| [0018](0018-agent-identity-naming-grammar.md) | Agent Identity Naming Grammar and Routing Rules | **Accepted** (2026-08-15; naming triangle keystone — grammar `flywheel-<project-slug>-<role><seq>` as THE routable identity per pane, `agent-names.json` as pane-to-identity authority, per-project `identity_mode` (swarm/open) with canonical-name-or-npub routing, whois/send/register hardening, one-time legacy reconciliation via `agent_name_aliases` folds) |
| [0019](0019-bead-verified-execution-and-principle-playbooks.md) | Bead-Verified Execution, Baked Principles, and Playbook Workflows | **Proposed** (2026-08-15; pstack invariants on beads: ledger-gated close, 21 principles as structure, playbook builtins; wave 1 locked by grilling; waves 2–6 specified) |
| [0020](0020-nostr-two-plane-cli-and-crew-steering.md) | Nostr Two-Plane Surface (CLI-first) and Crew-to-Crew Steering | **Accepted** (2026-08-15; SC-27..33 green; CLI `channel`/`job`/`react`/`presence`/`watch`; MCP stays 39) |

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
