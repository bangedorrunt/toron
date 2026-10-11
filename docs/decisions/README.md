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
| [0019](0019-bead-verified-execution-and-principle-playbooks.md) | Bead-Verified Execution, Baked Principles, and Playbook Workflows | **Accepted** (2026-08-15; pstack invariants on beads: ledger-gated close, 21 principles as structure, playbook builtins; wave 1 locked by grilling; body `status: accepted` — README row corrected 2026-09-04, GS-016) |
| [0020](0020-nostr-two-plane-cli-and-crew-steering.md) | Nostr Two-Plane Surface (CLI-first) and Crew-to-Crew Steering | **Accepted** (2026-08-15; SC-27..33 green; CLI `channel`/`job`/`react`/`presence`/`watch`; MCP stays 39) |
| [0021](0021-nip06-hierarchical-keys.md) | NIP-06 Hierarchical Agent Keys — One scrypt Total? | **Rejected** (2026-08-18; identity continuity unmaintainable for 119 production keys, residual cost bounded by lazy-unwrap + KDF cache, single-master compromise is structural; revisit trigger recorded) |
| [0022](0022-phase-native-orchestration.md) | Phase-Native Orchestration — typed JobSpec, harness phase mechanics, one durable execution rail | **Accepted** (2026-08-21; W1–W7 waves incl. compose versatility ladder (§3.6) + stage registry (§3.8) + persist/delegation (§3.7); extends ADR-0005 Spec 2 + ADR-0010; closes bd-dispatch-spec-untyped-0iud + bd-compose-not-durable-oxjy; compose + steer route through `engine.invoke`) |
| [0023](0023-nip29-group-metadata-kinds.md) | Adopt NIP-29 39000/39002; retire 40/41 to ingest-only | **Accepted** (2026-08-24; ADR-0020 wave 2; ChannelStore stays the gate; no 9000–9008 admin) |
| [0024](0024-graph-first-repair-loops-bead-workflow.md) | Make the DAG Canonical, Type Runtime Failure, and Unroll Bounded Repair as Distinct Graph Nodes | **Accepted** (2026-08-31; Spec 1 kind-aware fail_run + Spec 2 bounded repair loop with `continue` on verify-1/2 so c2 can run after c1 failure, final verify gates; `Unrolled` claim removed, Branch early-exit is future optimization; VerifySink attached) |
| [0025](0025-adopt-graph-dispatch-artifact-reduce-pin-supervision.md) | Adopt Graph-Only Dispatch, Artifact Reduce, and Pin Supervision | **Accepted** (2026-08-31; acceptance record with deviations — all 7 specs have shipped surface as of `c96a7ae` doorbell-tick bind; deviations: Spec 6 typed seam `bd-0or2` not enum merge, Spec 3 wave-2 remainders `heal`/`route_event` fold `bd-nsb3`, `InvokeGraph` deliberately not restored per `GS-005` §9) |
| [0026](0026-failure-taxonomy-funded-preflight-fallback-chains-mail-health-escalation.md) | Failure Taxonomy, Funded Preflight, Fallback Chains, and Bounded Mail-Health Escalation | **Accepted** (2026-08-31; both waves verified file:line — `FailureKind` frozen spelling, funded preflight, fallback chains, bounded mail-health + `mail_degraded` + loop-identity fail-loud; one deviation: escalation rides eprintln+Blocked per this ADR's own note) |
| [0027](0027-one-workspace-tick-control-plane.md) | One Workspace Tick — Control Plane as Elm/Redux on Doorbell + 30013 | **Accepted** (2026-09-01, captain word ordered full implementation; `WorkspaceModel`/`Msg`/`tick` + `Cmd`, `corr`/`extra_tags`, receipts, 30013 role, `why --corr/--seq` replay; slices ship as green) |
| [0028](0028-jido-behavior-first-on-wheelhouse-nouns.md) | Behavior-First Contracts on Wheelhouse Nouns — Cmd Honesty, WaitCorr Parity, Assignment Ring Proof | **Accepted** (2026-09-03, captain word ordered full implementation; GS-015 STEER + open-mode review; Cmd apply-or-delete, bus-wake WaitCorr, corr authority + receipt ownership, Human corr plumbing, presence trust, S-proofs; beads `bd-adr-0028-implementation-zfnx` dispatch as waves) |
| [0029](0029-rot-supervision-handoff-pacing-and-config-trust-boundary.md) | Deterministic Rot Supervision, Respawn Handoff, Window Pacing, and the Repo-Config Trust Boundary | **Proposed** (2026-09-08; zirv-cli pattern study — pure rot scorer on the 0027 tick, distilled handoff into bead threads, park-until-reset for window-exhaustion, mail caps + shared memory bank + repo-config-may-only-narrow; extends ADR-0024/0025/0026/0027; flywheel-local, toron unchanged) |
| [0030](0030-presence-gated-chokepoints.md) | Presence-Gated Chokepoints — Session Entry Enforced at Tool Boundaries | **Proposed** (2026-09-09; the solo-lane incident — live fleet on toron, unarrived pin, legitimate-looking exit ceremony; presence freshness gates br claim/gate/close + reserve warn→strict behind operator-owned `TORON_PROTOCOL_STRICT`, gates.jsonl presence/briefed metadata, doctor + doorbell suspect flags, one-command `flywheel session-start`; extends ADR-0011/0019/0028; acceptance harness `docs/decisions/_scripts/verify-presence-gates.sh` RED today by design) |
| [0031](0031-herdr-runtime-study-elm-cores-superlogical-proof-seams.md) | Herdr Runtime Study — Pane/Agent Split, Elm Decision Cores, Superlogical-Proof Seams | **Accepted** 2026-09-10 (all slices S1–S5 plus xstate steals landed green; toron-side companion to flywheel ADR-0012) |
| [0032](0032-workflow-engine-honesty-jido-contracts.md) | Workflow-Engine Honesty Gate and Jido Action Contracts — Per-Step Turn Receipts, Closed-Enum Schema Tables, Catalog-Driven Decide | **Proposed** (2026-09-11, rev 2 review pass 2026-09-12; wave-1 beads `bd-i6kc` toron + `bd-himk`/`bd-40d1` flywheel, wave-2 `bd-htoo`/`bd-3pti` toron; extends ADR-0024/0026/0028) |
| [0033](0033-additive-tools-kinds-discipline.md) | Additive MCP Tools and Kinds — Replacement Discipline for the Frozen Surface | **Proposed** (2026-09-19, bead bd-ncpt; amends ADR-0003 freeze clause + ADR-0032 count lock: additive tools/kinds with ADR + catalog regen + tests + versioning; renumber/parity drift still forbidden) |
| [0034](0034-effect-oriented-toron-companion.md) | Effect-oriented discipline for toron (companion to flywheel ADR-0015 §4) | **Proposed** (bead bd-lzo6, crew B; acceptance is a captain word on the file) |
| [0035](0035-elm-kernel-law-and-judgment-plane.md) | Elm Kernel Law and Judgment Plane Across toron, flywheel, and beads | **Accepted** (operator word 2026-09-22; research bead bd-fp4u closed, implementation epic bd-vhmn closing) |
| [0036](0036-shared-bus-substrate.md) | Daemon Push Sidecar (Wake Bus) for Cross-Process Observation | **Accepted** (operator word 2026-09-23; serve wiring landed bd-ewsi, design bead bd-42tn + transport bead bd-un5s closed) |
| [0037](0037-one-key-per-pin-project-scoped-visibility.md) | One Key per Pin — Project-Scoped Visibility, Not Project-Scoped Keys | **Accepted** (captain word 2026-10-11, stack-comms build Phase 0 prerequisite; W1–W5 not yet implemented, bd-j5sk stays open until they land and the §5 migration runs) |
| [0038](0038-operator-key-lives-in-the-config-home.md) | The Operator Key Lives in the Config Home — One Resolution for Everyone | **Accepted** 2026-09-30 (operator choice; the two paths held different keys on the machine that prompted it, so `doctor` reported a red that read as unconfigured while the daemon signed with a second identity; the implicit data-root mint now refuses and names the `mv`) |
| [0040](0040-adopt-elm-effect-for-the-comms-kernel.md) | Adopt elm-effect-elm and elm-effect-journal for the Comms Kernel | **Accepted** (captain word 2026-10-11; amends ADR-0035 for the comms kernel only; one port `toron_core::kernel`; vendored at a hash-checked rev until elm-effect-rs has a remote) |
| [0041](0041-comms-envelope-log-and-inbox-verbs.md) | Comms Envelope, Per-Recipient Log, and the `next`/`ack`/`done` Verbs | **Accepted** (captain word 2026-10-11; L16 log per recipient, delivery facts written only by the reader's verbs; CLI-only behind `TORON_COMMS_INBOX`, no MCP tool) |

Rows 0034–0036 were missing from this index when ADR-0037 was drafted; they are backfilled here. An index is the directory's only discovery surface, and ADR-0035/0036 are **accepted** — an accepted decision nobody can find is a gap, not a cosmetic one.

Flywheel [ADR-0017](../../../flywheel/docs/decisions/0017-shared-foldcore-crate.md) (proposed 2026-09-22) would have placed a zero-dependency `foldcore` crate here. It was never created and is **rejected** (2026-10-11), superseded by elm-effect-rs ADR-0002 + [ADR-0040](0040-adopt-elm-effect-for-the-comms-kernel.md). Flywheel must not become a dependency of this repo.

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

// governed-by: ADR-0003, ADR-0007
