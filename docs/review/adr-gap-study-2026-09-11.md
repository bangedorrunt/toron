# ADR Gap Study — remaining features across the stack (2026-09-11)

Vehicle: open (current pane), pin QuietAnvil, bead bd-qfai. Scope: toron ADRs
0007/0021/0024–0031, flywheel ADRs 0001–0012, both `_scripts/` harnesses, and
the open-bead boards of both repos. Every claim below was cross-checked against
the trackers (`br show`) and the tree on 2026-09-11; several ADR-body claims
were stale and are corrected here.

## Bottom line

Two whole ADRs are proposed-but-unaccepted and unimplemented (toron ADR-0029
rot supervision, toron ADR-0030 presence-gated chokepoints). Flywheel ADR-0012
Phases 1–4 are accepted but have **no beads filed** (the index points at
bd-asr2.1–.19, which were never created). Flywheel ADR-0010 delivery semantics
is mostly unlanded beyond Wave A.0 and D8. Everything else named in recent ADR
bodies as "open" turned out to be closed and the docs are stale.

## 1. Proposed-unaccepted — the real remaining feature surface

### toron ADR-0029 (2026-09-08) — rot supervision, handoff, pacing, config trust
Awaiting captain word. All five waves unimplemented, flywheel-local:
- W1 pure rot scorer (`src/rot.rs`, Healthy/Advise/Compact/Restart) on the 0027 tick.
- W2 respawn with distilled handoff (cheap-model distill ≤4096 B, 30 s timeout, structural fallback; delivered to prompt + bead thread).
- W3 window pacing: park-until-reset + per-provider reservation ledger, budget-exempt.
- W4 mail caps + shared memory bank (repo-committed, capped, 30-day staleness) + repo-config-may-only-narrow trust boundary (~100 forbidden keys).
- W5 risk ratchet (never-downgrade on REVIEW/VERIFY entry), registry sweep-on-read, nudge budget (3 interrupts then mail-only), supervised exit-code contract.

### toron ADR-0030 (2026-09-09) — presence-gated chokepoints
Harness `docs/decisions/_scripts/verify-presence-gates.sh` is RED by design
(header lines 5–8) and stays red until the waves ship. Surfaces confirmed
absent in the tree today (probed 2026-09-11):
- W1 `toron whois --fresh` (900 s freshness boundary, per-project) — no top-level `whois` subcommand yet.
- W2 chokepoint gating on `br claim/gate/close` + `toron reserve` behind operator-owned `TORON_PROTOCOL_STRICT` — zero occurrences in source.
- W3 `gates.jsonl` additive `presence:`/`briefed:` fields + `br doctor protocol.unarrived_closes`.
- W4 doorbell suspect banner on done-from-absent-pin + captain digest suspect section.
- W5 one-command `flywheel session-start` — subcommand does not exist.

## 2. Accepted-but-unimplemented

### flywheel ADR-0012 — doorbell plane, Phases 1–4 (rev 3, accepted 2026-09-10)
Only Phase 0 landed (bd-asr2, closed). The index says Phases 1–4 are "tracked
in bd-asr2.1–.19" but **those beads do not exist** — the plan is untracked:
- Phase 1: `doorbell-rt` + daemon beside flywheel, journal-diff parity gate, crash harness, D9 fence.
- Phase 2: subscriptions + human digest + durable budgets (journal v2) + webhook/ncrypt-DM sinks + SSE replay + stats.
- Phase 3: cut spawn-ownership, default host flip.
- Phase 4 (default-off): arrival_triple, beads_poller, cron_doorbell_sink, emit identities, cross-workspace scopes.

### flywheel ADR-0010 — delivery semantics / attention triage
§13 verification boxes unchecked. Landed pieces: Wave A.0 (one WorkspaceModel,
`poll_mail_digests` producer deleted — only a guard assert remains) and D8
(receipts `degraded` + corr-preserving resend, bd-d8e4 closed). Remaining:
- Wave A: delivery seam D1/D2/D3/D4/D14.
- Wave B: triage D5/D6/D15 (AFK digest, open-decisions fold).
- Wave C remainder: control plane D7/D9/D13, `--resolve-bead`, subagent-guard.
- Wave D: snapshot/helm + tiered decay D10/D12 — and D11 is blocked on a
  **beads successor ADR** (typed deliverable `diff|report`, `--promotes`,
  `hold --kind captain`), which does not exist yet; flywheel must not ship a
  flywheel-only column. No `verify-0010.sh` exists at all.

### flywheel ADR-0008 — one open e2e box
`beads-lifecycle` durable `--run` e2e (preview + guard-eval + defaults are
covered; the full durable invoke path is not tested).

### flywheel ADR-0006 — decide-routing follow-up, due
Wave-swarm graduated 2026-08-23; the recorded follow-up (route wave-graphs to
compose in `decide`) is unclaimed. Cosmetic sweep bead q0g0 is closed.

## 3. Stale docs — ADR bodies/index say open, tree says done

Verified closed/landed; the doc rows should be corrected when convenient:
- toron ADR-0025: bd-nsb3 (heal/route_event/watch_sends fold into tick) and bd-0or2 (IngressEvent→LoopMsg seam) both **closed**; index still lists them as deviations/remainders.
- toron ADR-0027: slices green — `toron wait --corr` exists, WaitCorr bus-drain live; body checkboxes stale.
- toron ADR-0028: implementation epic bd-adr-0028-implementation-zfnx + both named beads (kfo6, sdi1) **closed**; body gates mostly unchecked.
- toron ADR-0007: F11 honesty gap **fixed** — `file_reservation_ids` is gone from the shared surface (`toron-core/src/mailbox/surface.rs:425-427` states paths-only is the truthful superset) and `no_integer_id_fields_in_tool_surface` guards it; the se2.3 index/body disagreement resolves as closed (flywheel epic closed 2026-08-09).
- flywheel ADR-0011: §1 says Waves B–D are "code follow-up", but §6 boxes are checked, `src/briefing.rs` exists, and briefing.v1 answered this very session — the §1 caveat is stale.

## 4. Open beads not named in any ADR body (flywheel board)

- p1 bd-red-test-repair-ptqu — 16 red flywheel-bin tests predating bd-ene2.
- p1 bd-cfgv — stage override replaces whole record instead of field-level merge.
- p1 bd-qd87 — compose token expansion ignores user/project workflow overrides.
- p1 bd-9tbb — loop never re-reads the reservation board (memoized leaf + resume-replay serialize forever).
- p2 bd-0zcr — doorbell cutover: default daemon host + spawn-API registration + launchd `--host` (overlaps ADR-0012 Phases 2–3).
- p2 bd-dify (model routing review), bd-uan9 (workflow/stage/compose audit), bd-xmq5 (centralize catalog resolution across seven call sites), bd-lifecycle-occupied-pane-wedge-07tx (beads-lifecycle packing wedge).

## 5. Explicit deferrals — backlog with recorded triggers (not owed now)

- toron ADR-0024 §10: verbatim transcript compaction (needs its own ADR), permission-gate hooks, hashline commit verification, event-driven loop promotion (ADR-0019 wave 4), headless spawn-await, `workflow tree/connect/fork` live-run CLI ADR, Branch early-exit optimization.
- toron ADR-0025/0026: liveness heartbeat + delivery-verdict ADR (carrier = ADR-0025 W3 LoopSnapshot), cheapest-sufficient model routing, engine↔flywheel `FailureKind` unification (move, not merge).
- toron ADR-0021 D6: revisit NIP-06 hierarchical keys only if cold-start unlock cost becomes significant (≥50 distinct agents per serve process or KDF ≥1 s).
- flywheel ADR-0009 §3: Stage.refs, compound R-P lint gate, principles-registry ADR, cross-project corpus.
- flywheel ADR-0008 Option B: ComposePart::Recipe → ComposeToken rename wave.
- flywheel ADR-0007 D5: Wave 7 auto-dispatch (revisit on P0–P1 model spend/quality).
- flywheel ADR-0005 D3: programmatic registerCommand extensions; sub-second plane-P wake as a later ADR.
- flywheel ADR-0002: R1.5 (adopt toron-core Backend trait, delete src/herdr.rs), R2 (real SuperlogicalBackend when SDK ships).
- toron ADR-0030 revisit triggers: activity-coupled freshness if the 900 s claim-time check proves too coarse; raise the operator window rather than special-casing verbs.

## Harness states

- toron `verify-presence-gates.sh`: RED by design until ADR-0030 ships.
- toron `verify-adr-links.sh`: enforcing gate (governed-by refs + forbid(unsafe) markers), expected green.
- flywheel `_scripts/`: verify-0001/0002/0005/0006/0007/0008/0009/0011/0012 exist and were green at their landing; **no verify-0010.sh exists** despite ADR-0010 defining per-wave scripts (0010a0 named in the ADR but not present).

// governed-by: ADR-0008, ADR-0019
