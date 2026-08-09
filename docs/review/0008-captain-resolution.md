# ADR-0008 Captain Resolution — Round 1 Change Agenda

**Captain:** bangedorrunt
**Date:** 2026-08-09
**Crew:** Adversary 1 = oc (w29:p1, DeepSeek V4 Flash) — code-grounded P0 verifier; Adversary 2 = oc2 (w29:p2) — charter-coherence.
**Counter file:** `docs/review/0008-debate-round1.md` (oc's section present; oc2's section restored post clobber — see §Coordination note).

Both adversaries returned **AGREE-WITH-CHANGES**. No REJECT. No RED P0 claim. 20 Change Agenda items resolved below (19 ACCEPT/ACCEPT-MODIFIED, 1 REJECT). ADR bumps to **Revision 1**.

This document IS the second-loop artifact (oc2 A2.C8 demand): every ruling is evidence-grounded; the crew cross-checked each other's cites before the captain ruled (oc verified oc2's S1 cite stale; oc2 verified oc's baseline reproduces).

---

## Adversary 1 (oc) — 10 items

| # | Change | Ruling | Rationale |
|---|--------|--------|-----------|
| A1.1 | bd-XXX full paths + S4 arg name (`file_reservation_id`) | **ACCEPT** | Charter text must point at real paths; bare names mislocate. Fix: `crates/toron-core/src/mailbox/{build_slots,identity,reservations}.rs`; S4 arg = `file_reservation_id: String` (tools.rs:1607). |
| A1.2 | S8 honest-stub rule | **ACCEPT** | Converges with A2.4. Gate: each of 25 resources resolves to non-empty JSON; stubs allowed ONLY with honest field values + a `status` field naming the live tool. `metrics_core` "not wired" = honest (allowed); placeholder strings = RED. |
| A1.3 | S3 gate: identity vs mail-content | **ACCEPT** | Reword claim to "shared durable agent state (identity/keys)"; add RED test in batch: send-in-process → inbox-visible-in-subprocess. Current `cli_robot.rs` proves identity sharing only. |
| A1.4 | bd-XXX regression tests unpinned | **ACCEPT (high value)** | Code is GREEN but 2/3 fixes (build_slots, reservations renewals) have no regression pin — a refactor silently re-introduces the ADR-0007 FAIL cells. Batch adds RED-first regression tests asserting absolute-expiry renewal semantics. identity already pinned. |
| A1.5 | P2 silent-Result-drop + count-conservation | **ACCEPT** | Add ast-grep `let _ = $X` + `.ok()` patterns (silent-drop class invisible to `.unwrap()` grep); require "classified N = audited N = baseline N" in findings table. |
| A1.6 | P1 Lagged-swallow + std-guard-across-block_on probes | **ACCEPT (high value)** | The `.lock().await` ast-grep CANNOT see: (a) `relay_bus.rs:196` `Err(RecvError::Lagged(_)) => continue` (federation bus drops lag silently); (b) `workflow_host.rs:441–461` std-Mutex-guard held while `publish_coalesced_job` does `rt.block_on(bus.publish)`. Add probes: `Lagged` grep + `lock().unwrap/expect(` within ±10 lines of `.await`/`block_on`/thread-scope. Classify stall-vs-cancel per site. |
| A1.7 | P6 verify-adr-links.sh missing | **ACCEPT** | `docs/decisions/_scripts/` absent. Enumeration is the named gate; batch ADDS the script + wires it (AGENTS.md mandates it). |
| A1.8 | P4 dead-code = live fixtures | **ACCEPT (prevents batch breakage)** | `noop_runner`/`next_tick`/`housekeeper_workflow`/`fixture_registration`/`cron_registry` are used by sc11/sc18–sc20/sc24 + `tests/common/mod.rs`. Literal "remove" breaks the e2e suite. Reclassify as test-support; wire behind `#[cfg(any(test, feature = "e2e-fixtures"))]` or move to `toron-e2e` common. Removal FORBIDDEN unless zero test references. |
| A1.9 | P5 D8.2 names non-existent agent-mail backend | **ACCEPT** | `crates/toron-core/src/backend/` has no agent-mail impl (herdr/tmux/mock/error/types only). D8.2 parity surface = `crates/toron-mcp/tests/am_parity_e2e.rs` (38-tool functional parity vs catalog) + `cli_robot.rs`. External agent-mail build = flywheel CI scope (documented, not this repo). |
| A1.10 | P3 drop unsafe-checker + add NIP-40 ingest probe | **ACCEPT (HIGHEST LEVERAGE)** | (a) unsafe-checker reviews unsafe blocks; tree is `#![forbid(unsafe_code)]` with 0 real unsafe — skill mismatch, drop it. (b) **NIP-40 `expiration` has NO probe on the mail unwrap ingest path** — grep shows NIP-40 only in `reservations.rs` (leases) + slots; the unwrap pipeline (`unwrap.rs`/`mailbox/mod.rs:223–244`) has no TTL check. An expired-but-never-seen gift wrap would be ACCEPTED, voiding the ADR-0003 "Parity Superiority" replay/expiry claim. P3 method probes: (1) verify-before-project per ingest path (named fn + line), (2) NIP-40 expiration honored on unwrap → if absent, file as **P0-level finding** + RED test in batch, (3) replay = `is_projected` dedup confirmed per sink. |

## Adversary 2 (oc2) — 10 items

| # | Change | Ruling | Rationale |
|---|--------|--------|-----------|
| A2.1 | Mutex/RwLock label drift (102 mentions vs 52 instantiations) | **ACCEPT** | Relabel baseline row: "Mutex/RwLock mentions (incl. imports/docs)" = 102; add "Mutex</RwLock< instantiations" = 52 as the P1 working number. |
| A2.2 | 80-unsafe reproducibility (pin exact grep) | **ACCEPT** | Replace "80 grep hits" with reproducible command: `rg -n 'unsafe' crates/*/src \| rg -v 'forbid\(unsafe_code\)'` = 5 hits, all comments; assert 0 real `unsafe {}` blocks. |
| A2.3 | Mega-file table test exclusion | **ACCEPT** | Add scope column "src (tests excluded)"; note `crates/toron-mcp/tests/am_parity_e2e.rs` (1059 LOC) is the 10th file if tests included. |
| A2.4 | S8 escape-hatch theater | **ACCEPT** | Converges with A1.2. Stub must carry explicit marker; "not wired" alone = RED unless it's an honest field value with a `status` naming the live tool. |
| A2.5 | Phantom "decision 7" | **ACCEPT** | Promote debate-governance to enumerated captain decision 7 in §"captain's review requirements". Cleaner than rewriting citations. |
| A2.6 | P5 `cargo test --workspace` skips feature-gated e2e | **ACCEPT** | sc9–sc25 are feature-gated (`p1`/`p2`/`p4`/`p5`, no default). P5 method = `cargo test --workspace --all-features` (or per-gate matrix `--features p1,p2,p4,p5`) + note that sc1–sc25 coverage requires the feature gates. |
| A2.7 | S1 gate witness missing | **REJECT (cross-checked stale)** | oc (Adversary 1) RAN `serve_wire_gift_wrap_fires_message_posted_once` (`server.rs:446`) — real gift-wrapped `Mailbox::send_message` e2e, 50.3s PASS, asserts exactly 2 kind-1059 wraps + stamp on reply + no 3rd wrap in 4s grace. oc2's cite of `workflow_host.rs:926` was the raw-kind-14 trap, but a SEPARATE correct witness exists at `server.rs:446`. **Cross-check mechanism worked: oc caught oc2's incomplete cite.** Amendment kept: charter S1 row NAMES `server.rs:446` as the witness explicitly. |
| A2.8 | Debate REJECT single-blind | **ACCEPT-MODIFIED** | Add bounded second loop: after captain rulings, crew gets ONE round to flag ACCEPT-MODIFIED/REJECT rulings that materially change a P0 gate; flagged rulings require a two-line captain justification recorded in the ADR. Bounded — no infinite ping-pong. (This document partially satisfies it; the crew may flag A1.10 NIP-40 escalation if they disagree with P0 framing.) |
| A2.9 | R8 baseline staleness mid-execution | **ACCEPT** | Add R8: re-run 5 cheap greps (LOC, unwrap, expect, lock, await) immediately before the batch-fix pass; annotate any delta in findings; file:line cites re-verified at P6. Precedent: ADR-0007 :851–855 stale-citation failure. |
| A2.10 | R9 counter independence / cross-check | **ACCEPT-MODIFIED** | Add cross-check requirement: each adversary's file:line cites verified by the OTHER adversary before captain rules. Already proven: oc verified oc2's S1 cite (found stale → A2.7 REJECT); oc2 verified oc's baseline (reproduced exactly). Lighter than a full independence mechanism; sufficient at this charter's rigor. |

---

## Net effect on the charter (Rev 0 → Rev 1)

**P0 verdict:** All 11 claims GREEN in code (oc ran every gate). "Fully implemented 0003–0007" SURVIVES adversarial verification for the Soundness Ledger. The Change Agenda does NOT reopen any P0 claim — it (a) fixes charter text precision (paths/arg names), (b) tightens 3 gates (S3 wording, S8 stub rule, bd-XXX regression pinning), (c) names the S1 witness.

**New P0 candidate surfaced:** A1.10 NIP-40 expiration ingest absence. If P3 confirms no TTL check on the unwrap path, this escalates to a P0 crypto finding (voids replay/expiry superiority). The batch fix must add the TTL check + RED test.

**Method gaps closed:** P1 (Lagged + std-guard probes), P2 (silent-drop + count conservation), P5 (all-features), P3 (drop unsafe-checker, add NIP-40 probe), P6 (verify-adr-links.sh authored in batch), P4 (dead-code → fixtures, never remove).

**Governance strengthened:** debate-governance = captain decision 7 (A2.5); bounded second loop (A2.8); cross-check requirement (A2.10); baseline-staleness risk R8 (A2.9).

---

## Coordination note (process bug, not charter bug)

oc (Adversary 1) was instructed "Write your verdict to `0008-debate-round1.md`"; oc2 (Adversary 2) was instructed "Append ... under a clearly separated section." oc finished AFTER oc2 and its "Write" clobbered oc2's appended section (file went 209→98 lines). oc2's section is restored in the merged file from the captain's earlier full read. **Lesson for the execution dispatch:** distinct output files per adversary (`0008-debate-round1-oc.md` / `-oc2.md`), merged by the captain — never "Write" + "Append" to the same path.

---

## Captain sign-off

All 20 items resolved. ADR-0008 → **Revision 1, status: accepted**. Execution (P0→P6) may proceed under the §Decision Outcome protocol, with the Rev 1 amendments applied.
