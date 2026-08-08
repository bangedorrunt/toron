# Swarm Plan — toron ADR-0005 Orchestration Tier (3 opencode lanes)

Updated: 3 opencode agents (OPCODE ONLY, per captain). All deepseek-v4-flash.

## Lanes (workspace w1V)

- **oc  = BlueOak   (w1V:p1)** — P0 engine + P5 spike GATE + P3 unified engine (LINCHPIN)
- **oc2 = EmeraldSwan (w1V:p2)** — P1 trait unify + P2 profiles + P4 smart cron
- **oc3 = BlackMarsh (w1V:p3)** — parity catalogs (bd-96a/3mp) + bd-pkc ADR-0004 wire + P6 squads/obs + docs

## Wave Plan (dependency-aware, lanes run in parallel where no shared file contention)

Wave 0 (parallel, no overlap):
- oc:  bd-p5-spike-lqdl (GATE, run EARLY) + bd-p0-sig-ke9 (P0.1)
- oc2: bd-p1-design-gn0 (P1.1) + bd-p2-catalog-9by (P2.1)
- oc3: bd-96a + bd-3mp (parity catalogs) + bd-pkc (ADR-0004 wire)

Wave 1:
- oc:  bd-p0-on-error-rqk (P0.2)
- oc2: bd-p2-content-map-msp (P2.2 AUDIT FIX) + bd-p1-unify-y9j (after design)
- oc3: bd-nyg + bd-9o9 (research epics)

Wave 2:
- oc:  bd-p0-46012-on1 (P0.3) + bd-p0-tests-tqr (P0.4)
- oc2: bd-p2-resolve-ntt + bd-p2-builtins-3zl + bd-p1-verify-16v
- oc3: (docs current)

Wave 3 — P3 LINCHPIN (oc owns):
- oc:  bd-p3-edge-wkm (P3.1 cargo edge flywheel->toron-workflow + WorkflowEngine::invoke)
- oc2: bd-p2-cli-wn8 + bd-p2-tests-4bd
- oc3: P6 prep when deps land

Wave 4:
- oc:  bd-p3-catalog-com, bd-p3-stages-iya, bd-p3-trigger-opt-kzr
- oc2: bd-p4-schedule-ext-5v2s
- oc3: bd-p6-squad-eiv0, bd-p6-squad-cli-flvr

Wave 5:
- oc:  bd-p3-auto-crew-27c, bd-p3-cli-llsh, bd-p3-builtins-uab
- oc2: bd-p4-spawn-await-pxw6
- oc3: bd-p6-runs-trye, bd-p6-robot-rd8y

Wave 6:
- oc:  bd-p3-zero-config-m11b, bd-p3-tests-hw29
- oc2: bd-p4-cron-mgr-ur33, bd-p4-bead-on-fail-0uaf
- oc3: bd-p6-tests-3zq7

Wave 7:
- oc:  bd-p5-graph-ac26, bd-p5-actions-rwgu (IF spike passed)
- oc2: bd-p4-cli-b99j, bd-p4-migrate-sagas-7rmo, bd-p4-tests-4cl7
- oc3: verification sweep + docs

Wave 8: remaining stragglers + cross-phase verification + commit .beads/

## File Reservations (avoid oc/oc2/oc3 collisions on shared files)

- toron-workflow/{engine,schema,lint}.rs: oc owns (P0/P3/P5 engine). oc2/oc3 DON'T edit.
- flywheel/src/main.rs + backend/*.rs: oc2 owns (P1/P2/P4). oc DON'T migrate these.
- crates/toron-core/src/backend/: oc2 owns (P1 trait).
- crates/toron-{cli,e2e}/: oc owns tests; oc2 owns cli.
- crates/toron-mcp/tools.rs: NOBODY (38-tool surface frozen). oc3 catalogs only reference doc/only.
- docs/decisions + CHANGELOG: oc3 owns.
- Build-slot: shared workspace -> acquire 'cargo' slot, one build at a time.

## Launch

- flywheel loop toron --work --on-blocked mail  (durable; report invocation id)
