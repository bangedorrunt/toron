---
status: accepted
date: 2026-08-09
revision: 1
decision-makers: bangedorrunt (captain)
consulted: opencode adversarial debate crew (w29:p1 Adversary 1 "oc" + w29:p2 Adversary 2 "oc2", DeepSeek V4 Flash); Round-1 counters in docs/review/0008-debate-round1.md; captain resolution in docs/review/0008-captain-resolution.md
informed: ""
supersedes: []
extends: [ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007]
related: [flywheel, herdr, opencode, bd-review-hardening-mail-backend-c3b]
---

// governed-by: ADR-0003

# ADR-0008: Comprehensive Codebase Review Charter — Tracks, Verification Gates, and Debate Protocol

> **Self-contained review-charter ADR.** ADRs 0003–0007 are marked
> implementation-complete (beads closed, phase tables green, ADR-0007 Rev 3
> accepted with captain gate `.4`). This ADR does **not** reopen any prior
> decision. It charters a **de novo, full-scope, adversarial code review** of
> the *actual toron tree* (5 crates, 45,609 LOC) against the contracts those
> ADRs fixed — to (a) verify the ADR-0007 Soundness Ledger claims S1–S9 are
> genuinely green in code now (not on paper), (b) surface material bugs,
> refactor debt, and correctness risks across concurrency / crypto / error /
> id-discipline / API-seam layers, (c) validate performance SLOs where they
> exist, and (d) batch-fix in one governed pass. The review is itself governed
> by a flywheel-dispatched opencode adversarial debate crew that must
> AGREE-WITH-CHANGES (or reject) this charter before execution begins.
>
> **Status: ACCEPTED (Revision 1, 2026-08-09).** Captain locked scope (all
> tracks P0–P6), backend posture (Nostr native = production; agent-mail =
> compiled fallback), repo scope (toron only — flywheel boundary documented,
> not edited), fix policy (batch), API posture (reshape allowed if it
> materially future-proofs), validation posture (correctness **and**
> performance), and **debate-governance** (opencode crew must
> AGREE-WITH-CHANGES before execution). Round-1 debate COMPLETE: both
> adversaries returned AGREE-WITH-CHANGES (no REJECT, no RED P0 claim — all
> 11 Soundness Ledger gates ran GREEN). 20 Change Agenda items resolved (19
> ACCEPT/ACCEPT-MODIFIED, 1 REJECT) — see `docs/review/0008-captain-resolution.md`.
> Rev 1 applies: tightened S3/S8/bd-XXX gates, NIP-40 ingest probe (P0-candidate),
> P1 Lagged + std-guard probes, P2 silent-drop + count-conservation, P3
> unsafe-checker dropped, P4 dead-code reclassified as fixtures, P5
> all-features + D8.2 redefinition, P6 verify-adr-links.sh authored in batch,
> captain decision 7 (debate), bounded second loop + cross-check, risks R8/R9.

---

## Context and Problem Statement

### What the prior ADRs claim is done

| ADR | Claim | Confidence the claim survives adversarial review |
|-----|-------|--------------------------------------------------|
| **0003** | Nostr-native mailbox fabric; 5 layers (nostr / bus / mailbox / backend trait / mcp); NIP-17 gift-wrap; `#![forbid(unsafe_code)]` workspace-wide; 38 tools + 25 resources; InProcessBus default + RelayBus federation | **Verified baseline:** `#![forbid(unsafe_code)]` present in every crate root + 23+ files (the 80 `unsafe` grep hits are the forbid attrs + comments — zero real unsafe); `cargo check --workspace` clean; tools.rs declares ~39 `#[tool(` |
| **0004** | 5 hardening ships: mention coalescing, loop prevention, `reconstruct`, guard bound, overseer reachability | **Partially verified:** `MentionCoalescer` + `should_evaluate_trigger_event` exist in workflow_host.rs; `stamp` + `trigger_filter` exist; **`reconstruct` doctor command existence + guard bounds unverified** |
| **0005** | Orchestration tier: profiles, workflows, stages, smart cron, flexible engine; `flywheel → toron-workflow` cargo edge | **Partially verified:** engine.rs 2401 LOC with `run_foreach`/`run_call_workflow`/`WorkflowResolver` DI present; cron.rs + coalesce.rs + lint.rs exist; **smart-cron catch-up + profile wiring unverified** |
| **0006** | Blossom / NIP-B7 federated blob serving (bd-esh.5) | **Partially verified:** blob_serve.rs exists in toron-mcp; archive/blob.rs in toron-core; **NIP-B7 auth + federation path unverified** |
| **0007** | Post-impl hardening + bug ledger F1–F9 + API redesign D1–D8 + Soundness Ledger S1–S9 | **UNVERIFIED post-acceptance.** ADR-0007's own Soundness Ledger (accepted 2026-08-08) records S3/S5/S6/S7/S9 as FAIL/PARTIAL/stuck at acceptance. Captain now reports "fully implemented 0003–0007" — this review's first job is to prove that claim in code |

### The captain's review requirements (this charter's north star)

1. **All tracks P0–P6.** No fast-path shortcut; every layer audited.
2. **Backend posture:** Nostr/toron native is the production mail path; agent-mail
   remains a **compiled opt-in fallback** (must build + pass D8.2 parity).
3. **Repo scope:** **toron only.** The flywheel `MailBackend` boundary (ADR-0007
   S9 / F3) is documented as a known seam but **not edited in this review** —
   flywheel-side fixes are a follow-on charter if the debate identifies them as
   blocking. toron's own surface must be correct on its own terms.
4. **Fix policy:** **Batch.** Collect all findings across P0–P6 into a ranked
   bug ledger, then fix in one governed pass (RED→GREEN per AGENTS.md TDD).
5. **API posture:** **Reshape allowed** if it materially improves the codebase
   and future-proofs/extensibility — not for aesthetics. Type breaks are
   permitted pre-1.0 with a `docs/parity/known-divergences.md` entry + dual-accept
   window where practical (ADR-0007 C3 discipline retained).
6. **Validation posture:** **Correctness AND performance.** Concurrency
   backpressure + KDF bounds + projection latency validated where SLOs exist.
7. **Debate-governance:** an opencode adversarial crew must return
   AGREE-WITH-CHANGES (not REJECT) before execution; every Change Agenda item
   is resolved ACCEPT / ACCEPT-MODIFIED / REJECT with rationale; a bounded
   second loop lets the crew flag rulings that materially change a P0 gate
   (two-line captain justification recorded); adversaries cross-check each
   other's file:line cites before the captain rules. (Rev 1 — promoted from
   §Decision Outcome per oc2 A2.5.)

### Problem statement

> Given ADRs 0003–0007 are reported implementation-complete, what **material
> defects, incomplete seams, concurrency/crypto/id correctness risks, refactor
> debt, and performance gaps** remain in the actual toron tree — and what
> **batched, debate-governed change agenda** (if any) is required to make the
> codebase trustworthy as the production Nostr-native agent mailbox, with
> agent-mail as a real compiled fallback?

---

## Measured Baseline (2026-08-09, this charter's evidence floor)

| Signal | Value | Verdict |
|--------|-------|---------|
| Total LOC (src + tests) | 45,609 | sized |
| Crates | 5 (core 22,152 / mcp 6,574 / workflow 7,618 / cli 4,340 / e2e 4,925) | sized |
| `#![forbid(unsafe_code)]` | every crate root + 23+ files | **verified sound** |
| Real `unsafe` blocks in src | 0 — **(Rev 1, oc2 A2.2)** reproducible: `rg -n 'unsafe' crates/*/src \| rg -v 'forbid\\(unsafe_code\\)'` = 5 hits, all doc comments; 0 real `unsafe {}` blocks | **verified sound** |
| Real-src `panic!`/`todo!`/`unreachable!` | 0 (35 grep hits = all in `#[cfg(test)]`) | **verified sound** |
| `unwrap()` in src | 219 | **to-audit (P2)** |
| `expect()` in src | 984 | **to-audit (P2)** |
| `unwrap_or(0)`/`unwrap_or_default` in src | 67 | **to-audit (P2 — ADR-0007 F3 residual target)** |
| `Mutex`/`RwLock` mentions (incl. imports/docs) | 102 — **(Rev 1, oc2 A2.1)** real `Mutex<`/`RwLock<` instantiations = 52 (the P1 working number) | **to-audit (P1)** |
| `.lock()` call sites | 122 | **to-audit (P1 poison/deadlock)** |
| `.await` call sites | 1,324 | **to-audit (P1 cancel-safety)** |
| `clone()` in src | 548 | **to-sniff (P4 — some necessary, some smell)** |
| Tests | 574 (335 `#[test]` + 239 `#[tokio::test]`); 2 `#[ignore]` | sized |
| clippy `--workspace --all-targets` lints | 16 (5 auto-fixable; dead code in workflow: `noop_runner`, `next_tick`, `housekeeper_workflow`, `fixture_registration`, `cron_registry`) | **to-clean (P4)** |
| Mega-files (>1000 LOC, **src only — tests excluded**) | 9 listed; **(Rev 1, oc2 A2.3)** `crates/toron-mcp/tests/am_parity_e2e.rs` (1059 LOC) is a 10th file if tests included | **to-map (P4 refactor candidates)** |
| Inline `bd-XXX` parity-bug comments | 3 (build_slots.rs:117, identity.rs:73, reservations.rs:239) | **to-verify-fixed-or-file (P0)** |

---

## Decision Drivers

1. **ADR-0007's own Soundness Ledger is the highest-leverage audit target.**
   S3/S5/S6/S7/S9 were FAIL/PARTIAL/stuck at ADR acceptance. "Fully implemented"
   is an unverified claim until each is proven green in code with a passing
   gate test.
2. **Concurrency = silent overnight-killer risk.** 122 `.lock()` + 102
   Mutex/RwLock + 1,324 `.await` is a large surface for poison-on-panic,
   deadlock-via-lock-ordering, and cancel-unsafety-while-locked bugs. The
   hachiko thread_local→OnceLock pattern (silent cross-thread intent loss,
   ADR-0040 D1) is the canonical cautionary tale — must be ruled out here.
3. **Crypto soundness is load-bearing for the entire value proposition.**
   ADR-0003's "Parity Superiority" table stakes identity/E2E/federation on
   Schnorr + NIP-44 + NIP-59. A verification skip on ANY ingest path, or a
   replay/expiry hole, voids the superset claim.
4. **Id discipline across the MCP↔core↔flywheel boundary was the ADR-0007 F3
   kill target.** Hex event id ↔ u64 ↔ String coercion must be audited
   end-to-end; the `unwrap_or(0)` residual was the named defect.
5. **Agent-automation context budget is the scarce resource** (ADR-0004 driver
   retained). Projection latency, search landing, and KDF startup bounds
   directly affect whether the mailbox is usable in the field — not just correct.
6. **No backwards-compat tax pre-1.0** (AGENTS.md). API reshape is permitted
   when it future-proofs; the cost is a `known-divergences.md` entry, not a
   compat shim.

---

## The Seven Review Tracks

Each track is independently executable, has a named skill mapping, a concrete
verification gate, and produces a ranked findings table that feeds the batch
fix. **No code edits in any track until the full P0–P6 findings table is
reviewed and the captain approves the batched change agenda.**

### P0 — Soundness Ledger Verification (highest leverage)

**Goal:** prove ADR-0007 Soundness Ledger S1–S9 + F2.KDF + the 3 inline
`bd-XXX` parity bugs are genuinely green in code now.

**Claims to verify (with evidence path + gate):**

| Claim | Evidence path | Gate |
|-------|---------------|------|
| **S1** NIP-17 DM triggers `message_posted` | `unwrap.rs:213` `ProjectionSink` trait + `TriggerNotifySink:226` + `server.rs:463–469` host wiring | **named witness (Rev 1):** `serve_wire_gift_wrap_fires_message_posted_once` (`server.rs:446`) — gift-wrapped `Mailbox::send_message`, asserts exactly 2 kind-1059 wraps, stamp present on reply (`:565`), no 3rd wrap in 4s grace. (oc ran it: 50.3s PASS.) Raw kind-14 `bus.publish` tests e.g. `workflow_host.rs:926` do NOT satisfy S1 (ADR-0007 C7 trap) |
| **S2** Workflow cannot re-trigger itself | `trigger_filter.rs` stamp filter + stamp applied on **every** emit path (not test-only) | audit every `bus.publish`/`emit` call site in workflow_host.rs + engine.rs for the `toron:workflow-emitted` stamp; gate test: workflow emit → no re-eval |
| **S3** Shared durable mail state across processes | `server.rs:283` `TORON_MCP_DATA_DIR` + `:292–304` XDG stable durable root + `operator key 0600` | **(Rev 1 reworded):** claim = "shared durable **agent state (identity/keys)** across processes". `cli_robot.rs` proves identity sharing (subprocess sees registered agent; isolated root → exit 1 fail-closed). **Batch adds RED test:** send-in-process → inbox-visible-in-subprocess (mail-content sharing, currently untested) |
| **S4** toron mail ids round-trip through flywheel | MCP tool arg types: `AcknowledgeMessageArgs.message_id: String` (tools.rs:1512), `MarkMessageReadArgs.message_id: String` (tools.rs:1523), `ForceReleaseFileReservationArgs.file_reservation_id: String` (tools.rs:1607 — **not** `message_id`); `ids.rs:17` `MessageId(String)` newtype | assert all are `String` (hex event id / kind-30031 hex), not `i64`/`u64`; ADR-0007 F3 — flywheel boundary documented, not edited |
| **S5** `foreach` binds loop variable | `engine.rs:650` `run_foreach` + `:1139–1146` `{{item}}` injection | gate test `foreach_over_list_collects` (engine.rs:2041) passes; body uses item, no `let _ = item;` |
| **S6** `call_workflow` composes | `engine.rs:738` `run_call_workflow` + `:266` `WorkflowResolver` DI + `:2134` nest test | gate test: nested durable child run; not a fixed `{ok:false}` stub |
| **S7** Mention burst → one job (at **host**, not lib-only) | `workflow_host.rs:81` `Arc<std::sync::Mutex<MentionCoalescer>>` + `:149–164` flush path | gate test: 3 mentions in burst window → exactly 1 coalesced kind-43001 published; **also audit the `.lock().expect("coalescer lock")` poison risk (P1)** |
| **S8** 25 resources readable (no placeholder stubs) | `resources.rs:11–129` exactly 25 RESOURCES entries; `:177` `metrics_core` honest stub (`"database": "not wired", "storage": "not wired"`); 11 data-projection arms, 7 static arms, 2 generic `live_via_tools` stubs | **(Rev 1 tightened):** each of 25 resolves to non-empty JSON (extend `static_resources_serve_json`/`data_backed_resources_project_mailbox_data` to cover all 25 URIs). Honest stubs allowed ONLY with honest field values + a `status` field naming the live tool (`metrics_core` qualifies). Placeholder strings without a `status` field = RED |
| **S9** Nostr is AM superset at the **flywheel trait** boundary | (flywheel repo — **out of scope per captain decision 3**) | document the gap in `known-divergences.md`; do not edit flywheel |
| **F2.KDF** Mailbox startup bound | `mailbox/mod.rs:175` `cached_load_agent_key` + `:197` `kdf_load_count` + `:540` 50-key gate test | gate test `unwrap_loop_kdf_cached_between_rebuilds_many_keys` passes; KDF count grows O(Δkeys) not O(N) |
| **bd-XXX ×3** build_slots under-extension / identity upsert / reservations 2x expiry | `crates/toron-core/src/mailbox/build_slots.rs:117` (renew base = existing lease expiry `:120–121`), `identity.rs:73` (create fails closed on existing key `:80–84`), `reservations.rs:239` (renewal = old absolute expiry + extend `:240–247`) — **all 3 document FIXED bugs** | **(Rev 1):** verify each fix is **regression-pinned**. identity pinned (`test_create_always_different_npub_or_refuses_collision`). build_slots (`renew_and_release_lifecycle:271`) + reservations (`test_renew:735`) currently assert only `Some`/count — **batch adds RED regression tests asserting absolute-expiry renewal semantics** (under-extension / 2x-expiry would otherwise pass silently) |
| **F2.NIP40** (Rev 1 — NEW P0-candidate, oc A1.10) NIP-40 `expiration` honored on mail unwrap ingest | grep shows NIP-40 only in `reservations.rs` (leases) + slots; the unwrap pipeline (`unwrap.rs` / `mailbox/mod.rs:223–244`) has **no TTL check** found | **(Rev 1):** if P3 confirms no TTL check on the unwrap path → **escalates to P0 crypto finding** (voids ADR-0003 "Parity Superiority" replay/expiry claim — an expired-but-never-seen gift wrap would be ACCEPTED). Batch adds the TTL check + RED test |

**Skill mapping:** codebase-archaeologist + adversarial-reviewer.
**Method:** per claim — read evidence path, run named gate test, record GREEN/RED/STUB with file:line proof.
**Deliverable:** Soundness Ledger table (claim × status × evidence × gate-result).

### P1 — Concurrency Correctness

**Goal:** rule out poison-on-panic, deadlock-via-lock-ordering, and
cancel-unsafety-while-locked across the 122 `.lock()` / 102 Mutex-RwLock /
1,324 `.await` surface.

**Targets:**
- Every `.lock().unwrap()` / `.lock().expect(...)` — poison-on-panic survivability (does a single panic brick the whole mailbox? ADR-0007 D7 recovery posture).
- Functions acquiring ≥2 locks — lock ordering (deadlock).
- `.await` while holding a `std::sync::Mutex` or `tokio::sync::Mutex` guard — cancel-safety + reentrancy.
- `Arc<Mutex<…>>` / `Arc<RwLock<…>>` shared-state invariants (cf hachiko thread_local→OnceLock silent-loss bug pattern — ADR-0040 D1).
- `broadcast` lag errors — are `RecvError::Lagged` swallowed or surfaced? Bus backpressure under burst.
- `OnceLock` initialization races.
- **(Rev 1, oc A1.6)** `RecvError::Lagged` swallow — `relay_bus.rs:196` `Err(RecvError::Lagged(_)) => continue` drops lag silently on the federation bus (in-process default is per-subscriber mpsc, so relay-only risk, but real).
- **(Rev 1, oc A1.6)** std-Mutex-guard held across `block_on_async`/thread-blocking publish — `workflow_host.rs:441–461` holds `self.coalescer.lock().expect("coalescer lock")` while `publish_coalesced_job` blocks on a scoped thread + `rt.block_on(bus.publish)`. The `.lock().await` ast-grep CANNOT see this class.

**Skill mapping:** m07-concurrency + m12-lifecycle + rust-async-patterns.
**Method:** `ast-grep` for `.lock().await`, `.lock().unwrap()`, `.lock().expect(`; **(Rev 1)** add `RecvError::Lagged` grep + `lock().unwrap()/lock().expect(` within ±10 lines of `.await`/`block_on`/thread-scope probe; multi-lock function enumeration; per-finding classify stall-vs-cancel + invariant-true vs real-risk.
**Deliverable:** concurrency findings table (site × pattern × risk × fix-class).

### P2 — Error-Handling + Id Discipline

**Goal:** eliminate silent-failure masking from the 219 `unwrap()` / 67
`unwrap_or(0)`/`default` / 984 `expect()` population; verify id-type discipline
end-to-end.

**Targets:**
- `unwrap()` on I/O / external input / lock / parse / serde — real bug class.
- `unwrap_or(0)` numeric coercion — ADR-0007 F3 residual kill target.
- `expect(msg)` on non-invariant paths — silent masking.
- Id type: hex event id ↔ u64 ↔ String across MCP tools ↔ core ↔ (flywheel boundary, documented).
- `Result`/`Option` propagation; `thiserror` error enums vs ad-hoc string errors; `?` discipline.
- Error message quality (actionable vs opaque).
- **(Rev 1, oc A1.5)** silent Result/Option drops: `let _ = $X`, `.ok();`, ignored `Result`/`Option` — invisible to the `.unwrap()` ast-grep. Canonical deliberate case: `unwrap.rs:251` `let _ = self.tx.send(mail.clone())` (best-effort); accidental siblings would not surface.
- **(Rev 1, oc A1.5)** count-conservation rule: every one of the 219 `unwrap()` / 984 `expect()` / 67 `unwrap_or(0)` sites classified invariant-true or risk — no cherry-picking. Findings table must show "classified N = audited N = baseline N".

**Skill mapping:** m06-error-handling + m13-domain-error.
**Method:** `ast-grep` `$X.unwrap()`, `$X.unwrap_or(0)`, `$X.expect($MSG)`, **(Rev 1)** `let _ = $X`, `.ok()`; classify each invariant-true (keep, maybe document with SAFETY-style comment) vs real-risk (fix).
**Deliverable:** error-handling findings table (site × class × invariant-true-or-risk × fix).

### P3 — Crypto / NIP-17 / Gift-Wrap Soundness

**Goal:** prove the ADR-0003 "Parity Superiority" claims (unforgeable identity,
E2E DMs, federatable threads, non-repudiable acks) hold on every ingest path.

**Targets:**
- `nostr/proof.rs` (688 LOC) — Schnorr verify on EVERY ingest path (forge resistance); proof gate (operator key, ADR-0003 "Proof gate" superiority).
- `nostr/gift_wrap.rs` — one-time random key per wrap; no metadata leak; seal re-sign; sender-mismatch detection.
- Replay protection — event id dedup, NIP-40 `expiration` TTL honored.
- Argon2 KDF params (memory/time) — DoS bound; align with F2.KDF gate.
- `unwrap.rs:1114` pipeline — projection ordering (sqlite before git? git before sqlite? atomicity?).
- NIP-09 deletion handling on the projection sinks.

**Skill mapping:** **(Rev 1, oc A1.10)** ~~unsafe-checker~~ (dropped — tree is `#![forbid(unsafe_code)]`, 0 real unsafe; skill mismatch) + m02-resource (key lifecycle) + m13-domain-error.
**Method:** read `proof.rs` + `gift_wrap.rs` + `unwrap.rs` end-to-end; trace every ingest path; verify verify-before-project invariant. **(Rev 1)** probes: (1) verify-before-project per ingest path (named fn + line), (2) **NIP-40 `expiration` honored on unwrap ingest** — grep shows NIP-40 only in `reservations.rs` (leases) + slots; if the unwrap pipeline has no TTL check → **file as P0-level finding + RED test in batch** (see P0 table F2.NIP40), (3) replay = `is_projected` dedup confirmed per sink.
**Deliverable:** crypto findings table (path × invariant × verified × risk).

### P4 — Mega-File Refactor Candidates

**Goal:** map responsibility boundaries in the 9 mega-files (>1000 LOC) and
propose behavior-preserving (or API-reshape, per captain decision 5) splits +
dead-code removal.

**Targets:**
- `engine.rs` (2401) — holds dispatch + cron + lint + jobs + foreach + call_workflow + template interpolation? Split candidate.
- `tools.rs` (1896) — 38 tool defs; split by domain (mail / reservation / product / slot / contact / workflow / system).
- `surface.rs` (1561) + `messaging.rs` (1341) — overlap? duplication?
- `unwrap.rs` (1114) — ProjectionSink hierarchy (already clean per P0); verify no responsibility creep.
- `operator.rs` (1104) + `herdr.rs` (1083) + `workflow_host.rs` (1076) + `reservations.rs` (1007).
- Dead code (clippy): `noop_runner`, `next_tick`, `housekeeper_workflow`, `fixture_registration`, `cron_registry` — **(Rev 1, oc A1.8)** these are LIVE test fixtures (`noop_runner` used by `engine.rs:2211` + `toron-cli/src/workflow.rs` + sc11/sc18–sc20; `next_tick`/`housekeeper_workflow`/`fixture_registration`/`cron_registry` in `tests/common/mod.rs` + sc18–sc24). **Reclassify as test-support; wire behind `#[cfg(any(test, feature = "e2e-fixtures"))]` or move to `toron-e2e` common. Removal FORBIDDEN unless zero test references** (literal "remove" breaks sc11/sc18–sc20/sc24).
- Duplication: `in_process.rs` vs `relay_bus.rs`; `surface.rs` vs `messaging.rs`.
- Repeated patterns → extract (cf hachiko cvd_signals proliferation lesson — extend in place, don't proliferate).

**Skill mapping:** complexity-mitigator + simplify-and-refactor-code-isomorphically + m15-anti-pattern.
**Method:** per mega-file produce a responsibility map (sections × concerns); propose splits; NO edits in this track — proposals feed the batch.
**Deliverable:** refactor proposals table (file × split × behavior-preserving-or-reshape × blast-radius × estimated-LOC-delta).

### P5 — Test Coverage + e2e Parity Gaps

**Goal:** verify the test suite proves what the ADRs claim, and surface gaps.

**Targets:**
- ADR-0007 D8.2: per-backend send→inbox→ack→reply — **(Rev 1, oc A1.9)** the agent-mail backend does NOT exist in this tree (`crates/toron-core/src/backend/` = herdr/tmux/mock/error/types only). D8.2 parity surface = `crates/toron-mcp/tests/am_parity_e2e.rs` (38-tool functional parity vs `docs/parity/am-rust-tool-catalog.md`) + `cli_robot.rs`. External agent-mail build = flywheel CI scope (documented, not this repo). Captain decision 2 (agent-mail fallback) is honored by keeping `am_parity_e2e` green against the catalog.
- sc1–sc25 e2e suite — **(Rev 1, oc2 A2.6)** scenarios are **feature-gated** (`p1`=sc1–8, `p2`=sc9–13, `p5`=sc16, `p4`=sc18–25; no default feature). A bare `cargo test --workspace` compiles sc1–sc8 and **silently skips sc9–sc25**. Run `cargo test --workspace --all-features` (or per-gate matrix `--features p1,p2,p4,p5`); capture failures, classify flaky vs real.
- `#[ignore]` `sc_fw_integrated` — real flywheel binary integration; still deferred? document why.
- Property tests for crypto round-trips (seal/unseal, gift-wrap/unwrap), id coercion (hex↔string round-trip).
- Differential tests (cf hachiko incremental-indicator pattern) for projection parity: bus projection vs git projection vs sqlite projection must agree bar-for-bar.
- Concurrency stress tests (burst coalesce, broadcast lag, multi-process durable root).

**Skill mapping:** tdd + testing-conformance-harnesses.
**Method:** **(Rev 1)** run `cargo test --workspace --all-features` (NOT bare `--workspace` — feature gates silently skip sc9–sc25), capture per-test result; list coverage gaps per ADR claim; propose missing tests (added in batch fix, RED first).
**Deliverable:** test-coverage findings table (ADR-claim × existing-test × pass/fail × gap × proposed-test).

### P6 — Doc / ADR ↔ Code Drift

**Goal:** ensure docs match reality; no "Complete" lies in the index.

**Targets:**
- `// governed-by: ADR-XXXX` markers present on all files implementing an ADR constraint. **(Rev 1, oc A1.7)** `docs/decisions/_scripts/verify-adr-links.sh` does NOT exist (`_scripts/` absent) — enumeration is the named gate; the batch ADDS the script + wires it (AGENTS.md mandates it).
- `docs/parity/known-divergences.md §7` (KDF) current with F2.KDF gate result.
- README claims vs actual surface (38 tools + 25 resources — verify count).
- ADR-0007 Soundness Ledger — update S1–S9 status column to reflect this review's verdicts.
- CHANGELOG entries for any batch fix.

**Skill mapping:** changelog-md-workmanship + de-slopify (docs prose only).
**Method:** enumerate governed-by markers; diff README counts vs code; update ledger.
**Deliverable:** doc-drift findings table (doc-claim × code-reality × drift × fix).

---

## Options Considered (review approach)

### Option A — Captain + single-agent sequential review (REJECTED)
One agent reads everything top-to-bottom. Pro: coherent mental model. Con: 45k
LOC exceeds one context window's effective audit depth; no adversarial check;
single point of failure on missed bugs. **Rejected** — ADR-0007's own
Round-1 opencode counter caught material defects the architect missed; the
adversarial pattern is load-bearing.

### Option B — Fan-out parallel review, no charter (REJECTED)
Spawn N opencode agents each on a crate, free-form. Pro: fast. Con: no shared
verification gates → findings not comparable → no batch fix possible → violates
captain decision 4. **Rejected.**

### Option C — Charter-gated, debate-governed, batched (CHOSEN)
This ADR. A self-contained charter fixes (a) the verification gates per track,
(b) the evidence-floor baseline, (c) the captain's 6 decisions, (d) the debate
protocol. An opencode crew debates the charter BEFORE execution (catch charter
errors early — missing claims, wrong gates, bad skill mapping). Execution is
sequential P0→P6 (findings feed forward). All findings batch into one ranked
ledger. Captain approves the batch. Fix pass is RED→GREEN TDD.

**Why chosen:** matches ADR-0007's proven Round-1 pattern; satisfies captain
decisions 1 (all tracks), 4 (batch fix), 7 (debate); gates execution on
charter quality, not just execution speed.

---

## Decision Outcome

**Adopt Option C: Charter-gated, debate-governed, batched review.**

### The debate protocol (mandatory before execution)

1. **Captain finalizes this ADR (Revision 0 → accepted) AFTER the opencode
   crew's counter is resolved.** No execution until both agree.
2. **Crew composition:** 2 opencode agents on herdr (reuse `flywheel-toron`
   workspace w26 — 2 idle `oc`/`oc2` present, or spawn fresh via
   `flywheel spawn toron-review --oc 2`). Roles:
   - **Adversary 1 (Round-1 counter):** code-grounded challenge — for each
     P0 claim, verify the evidence path exists and the gate is real; for each
     P1–P6 track, challenge whether the skill mapping + method will actually
     surface the named risk class. Cite file:line. Output: AGREE-WITH-CHANGES
     or REJECT, with a Change Agenda (low→high leverage).
   - **Adversary 2 (charter-coherence):** does the charter omit a load-bearing
     track? Are the captain's 6 decisions faithfully encoded? Is the baseline
     honest? Are the gates falsifiable (not tautologies)? Output: same shape.
3. **Captain resolution:** each Change Agenda item is ACCEPT / ACCEPT-MODIFIED /
   REJECT with a one-line rationale. ADR bumps to Revision 1.
4. **(Rev 1, oc2 A2.8) Bounded second loop:** after captain rulings, the crew
   gets ONE round to flag any ACCEPT-MODIFIED/REJECT ruling that materially
   changes a P0 gate; flagged rulings require a two-line captain
   justification recorded in the ADR. REJECT verdicts honored unless the
   captain publishes a written override. Bounded — no infinite ping-pong.
   (Round 1 cross-check already executed: oc verified oc2's cites and
   vice versa; resolution in `docs/review/0008-captain-resolution.md`.)
5. **Execution gate:** `flywheel loop toron-review --work --on-blocked mail`
   drives the crew through P0→P6; the captain monitors via
   `flywheel status`/`flywheel view`. Findings accumulate in
   `docs/review/0008-findings.md` (created at execution start).
6. **Batch approval:** when P0–P6 findings tables are complete, captain reviews
   the ranked ledger, approves a batched change agenda (bug fixes first, then
   refactors, then doc), and the fix pass executes RED→GREEN.

### Sequencing

```
P0 (soundness ledger) ──► P1 (concurrency) ──► P2 (error/id) ──► P3 (crypto)
                                                                        │
   P6 (doc drift) ◄── P5 (test gaps) ◄── P4 (refactor proposals) ◄──────┘
                                  │
                                  ▼
                  batched change agenda → captain approval → RED→GREEN fix pass
```

P0 first (highest leverage — ADR self-tracks risk). P1 before P2/P3 (concurrency
bugs are silent overnight-killers). P4 after P0–P3 (refactor only on a correct
base). P5/P6 last (polish + drift fix reflect the final state).

---

## Verification / Acceptance for this ADR

This ADR is **accepted** when ALL hold:

1. **Debate gate:** the opencode crew returns AGREE-WITH-CHANGES (not REJECT);
   every Change Agenda item is resolved ACCEPT / ACCEPT-MODIFIED / REJECT with
   rationale; ADR bumps to Revision 1.
2. **Baseline honesty:** the Measured Baseline table's numbers are reproducible
   by the crew from the live tree (not stale).
3. **Gate falsifiability:** every P0 claim's gate is a real test or audit step
   that can FAIL (not a tautology). The crew signs off on this.
4. **Skill mapping adequacy:** the crew confirms each P1–P6 track's named
   skills + method will actually surface the named risk class (or amends).
5. **Captain sign-off:** bangedorrunt marks `status: accepted` after the above.

This ADR is **implemented** (not just accepted) when the batched fix pass lands
and:
- All P0 claims are GREEN with passing gate tests.
- P1–P6 findings tables are complete and either fixed, filed as follow-on
  beads, or explicitly accepted-as-wontfix with rationale.
- ADR-0007 Soundness Ledger S1–S9 status column updated to reflect this
  review's verdicts.
- `docs/parity/known-divergences.md` updated with any type-break entries.
- CHANGELOG entry added.

---

## Risks / Counter-Arguments (pre-seeded for the debate crew)

These are the charter author's own anticipated challenges — the crew should
probe whether they are complete and correctly framed.

- **R1 (scope creep):** "All tracks P0–P6" may overflow the review window. **Mitigation:** each track produces a findings table independently; the batch fix is gated, not the discovery. If discovery overflows, tracks split into Review-1 (P0–P3 correctness) and Review-2 (P4–P6 quality) — captain decides at the P3 checkpoint.
- **R2 (debate theater):** the opencode crew could rubber-stamp. **Mitigation:** the Change Agenda must be code-grounded (file:line cites); an empty/tautological counter = REJECT and respawn with a sharper prompt.
- **R3 (flywheel boundary blindness):** captain decision 3 excludes flywheel edits, but ADR-0007 S9 + F3 live at that boundary. **Mitigation:** toron's own surface must be correct on its own terms; flywheel-side gaps are documented in `known-divergences.md` and filed as a follow-on charter (ADR-0009 candidate), not silently dropped.
- **R4 (API-reshape runaway):** captain decision 5 permits reshape, risking a refactor that's churn-not-improvement. **Mitigation:** P4 refactor proposals must state blast-radius + future-proof justification + LOC-delta; captain approves per-proposal, not blanket.
- **R5 (performance SLO undefined):** captain decision 6 asks for performance validation but no SLO is fixed. **Mitigation:** P1 establishes backpressure bounds (broadcast lag ceiling); P3 establishes KDF bound (already F2.KDF); projection latency benchmarked against the F1 e2e path. If no explicit SLO, the review establishes the **measured** baseline + proposes SLOs for ADR-0009.
- **R6 (agent-mail fallback rot):** if agent-mail is fallback-only, its build + D8.2 parity could silently break. **Mitigation:** P5 explicitly runs the agent-mail path; a CI gate (proposed in batch) compiles it on every change.
- **R7 (batch-fix size):** one batched fix pass could be unreviewably large. **Mitigation:** batch splits into logical commits (one per track's fixes) with conventional-commit messages; captain reviews per-commit.
- **R8 (baseline/gate staleness mid-execution) — Rev 1, oc2 A2.9:** the Measured Baseline is dated 2026-08-09; if any P0–P6 work lands between baseline measurement and the P6 doc-drift pass, the numbers + file:line cites drift. Precedent: ADR-0007 :851–855 stale-citation failure. **Mitigation:** re-run the 5 cheap greps (LOC, unwrap, expect, lock, await) immediately before the batch-fix pass and annotate any delta in the findings file; file:line cites in findings re-verified at P6.
- **R9 (counter independence / cross-check) — Rev 1, oc2 A2.10:** the captain is charter author AND counter judge; the crew has one shot, no appeal. **Mitigation:** cross-check requirement — each adversary's file:line cites verified by the OTHER adversary before the captain rules (proven in Round 1: oc caught oc2's stale S1 cite → A2.7 REJECT; oc2 reproduced oc's baseline exactly). Lighter than a full independence mechanism; sufficient at this charter's rigor.

---

## More Information

- **Relationship to ADR-0003:** verifies the 5-layer architecture + Parity Superiority claims in code. Does not reopen layer/bus/runtime decisions.
- **Relationship to ADR-0004:** verifies the 5 hardening ships are production-wired (not lib-only).
- **Relationship to ADR-0005:** verifies engine flexibility claims (foreach/call_workflow/cron) are real (not stubs).
- **Relationship to ADR-0006:** verifies NIP-B7 blob path (auth + federation).
- **Relationship to ADR-0007:** this ADR's P0 is a direct verification of ADR-0007's Soundness Ledger S1–S9 + F2.KDF. ADR-0007 Rev 3 accepted with several ledger cells at FAIL/PARTIAL/stuck; this review is the post-acceptance proof.
- **Revisit triggers:** (a) the debate crew REJECTS this charter → respawn or revise; (b) P0 discovers a Soundness Ledger claim is still RED after "fully implemented" report → escalate to captain immediately (blocks P1+); (c) a track surfaces a defect class outside the charter's scope → amendment; (d) batch-fix size exceeds review window → split into Review-1/Review-2.

---

## Appendix A — Skill mapping summary

| Track | Primary skills | Method anchor |
|-------|----------------|---------------|
| P0 | codebase-archaeologist, adversarial-reviewer | per-claim evidence + gate test |
| P1 | m07-concurrency, m12-lifecycle, rust-async-patterns | ast-grep lock/await patterns |
| P2 | m06-error-handling, m13-domain-error | ast-grep unwrap/expect/unlock_or(0) |
| P3 | unsafe-checker, m02-resource, m13-domain-error | end-to-end ingest trace |
| P4 | complexity-mitigator, simplify-and-refactor-code-isomorphically, m15-anti-pattern | responsibility map per mega-file |
| P5 | tdd, testing-conformance-harnesses | run suite + gap analysis |
| P6 | changelog-md-workmanship, de-slopify | governed-by marker enumeration |

## Appendix B — Crew dispatch commands (reference)

```bash
# Reuse existing idle crew (preferred — w26 has 2 idle oc agents):
flywheel send toron-review oc  "<debate prompt for Adversary 1>"
flywheel send toron-review oc2 "<debate prompt for Adversary 2>"
flywheel loop toron-review --work --on-blocked mail   # durable monitor

# Or spawn fresh if w26 is contaminated:
flywheel spawn toron-review --oc 2 --label "ADR-0008 debate"
```

The debate prompt (per adversary role) is finalized at execution; the charter
above is the prompt's authoritative source.
