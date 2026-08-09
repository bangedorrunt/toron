#!/usr/bin/env bash
# governed-by: ADR-0008
# ADR-0008 Comprehensive Codebase Review - bead DAG generator (definitive).
# Creates the full umbrella + 9 epics + ~40 leaves with rich self-documenting
# descriptions, then wires hierarchy (parent) + meaningful cross-track deps.
#
# DEP-MODEL NOTE (post-creation fix applied 2026-08-09):
# br epic/child hierarchy uses `--parent` (sets the `parent` field; non-blocking;
# registers children for `br epic status` + `br epic close-eligible`). Blocking
# prerequisites use `br dep add A B --type blocks` = A depends-on B = B blocks A.
# This script ORIGINALLY wired hierarchy as `br dep add leaf epic --type blocks`
# (WRONG: leaf blocked-by-epic -> nothing ready). Post-creation fix reversed
# those to `br dep add epic leaf --type blocks` (epic gated by leaves -> leaves
# ready) AND set `--parent` on every leaf/epic for hierarchy display. Re-running
# this script creates DUPLICATE beads (not idempotent); it is an ARTIFACT.
# Inspect: br list --json | jq length ; bv --robot-insights ; br dep cycles
set -uo pipefail
cd /Users/bangedorrunt/workspace/toron
ACTOR="${BR_ACTOR:-captain}"
DESC_DIR=$(mktemp -d)
trap 'rm -rf "$DESC_DIR"' EXIT
declare -A IDS

PREAMBLE='## Project context (ADR-0008 review charter)

toron = Nostr-native Tokio multi-backend agent mailbox (5 crates, 45,609 LOC:
toron-core 22,152 / toron-mcp 6,574 / toron-workflow 7,618 / toron-cli 4,340 /
toron-e2e 4,925). ADRs 0003-0007 marked implementation-complete. ADR-0008
(`docs/decisions/0008-comprehensive-codebase-review-charter.md`, Rev 1,
accepted 2026-08-09) charters a de novo adversarial review to verify the
ADR-0007 Soundness Ledger S1-S9 are genuinely green in code (not paper) and
surface material bugs / refactor debt / perf gaps across all layers.

Measured baseline (2026-08-09, reproducible): `#![forbid(unsafe_code)]` in
every crate root + 23+ files (0 real unsafe; `rg -n "unsafe" crates/*/src |
rg -v "forbid\(unsafe_code\)"` = 5 hits all doc comments); 0 real-src panic!
(all 35 hits in #[cfg(test)]); 219 unwrap() + 984 expect() + 67
unwrap_or(0)/default + 548 clone() in src; 102 Mutex/RwLock mentions (52 real
instantiations), 122 .lock() sites, 1324 .await sites; 574 tests (335 #[test]
+ 239 #[tokio::test]), 2 #[ignore]; cargo check clean; clippy 16 lints.

Round-1 opencode debate (Adversary 1 "oc" + Adversary 2 "oc2", DeepSeek V4
Flash): BOTH AGREE-WITH-CHANGES. oc RAN all 11 Soundness Ledger gate tests -
ALL GREEN. "Fully implemented 0003-0007" SURVIVES for the ledger. 20 Change
Agenda items resolved 19 ACCEPT / 1 REJECT in
`docs/review/0008-captain-resolution.md`.

Captain decisions: (1) all tracks P0-P6; (2) Nostr native = production,
agent-mail = compiled fallback (must pass D8.2); (3) toron only - flywheel
boundary documented NOT edited; (4) BATCH fix (RED->GREEN in one pass);
(5) API reshape allowed if future-proofs; (6) correctness AND performance;
(7) debate-governed. Sequencing: P0 -> P1 -> P2 -> P3 -> P4 -> P5 -> P6 ->
batch-fix-pass.'

# mk <slug> <title> <type> <priority> <body-markdown-file>
mk() {
  local slug="$1" title="$2" type="$3" pri="$4" body="$5"
  local desc="$DESC_DIR/$slug.md"
  { printf '%s\n\n' "$PREAMBLE"; cat "$body"; } > "$desc"
  local id
  id=$(br create --actor "$ACTOR" --slug "$slug" --title "$title" -t "$type" -p "$pri" --description-file "$desc" --json 2>/dev/null \
       | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('id') or (d.get('issue') or {}).get('id') or '')" 2>/dev/null)
  if [ -z "$id" ]; then
    echo "FAIL $slug:" >&2; br create --actor "$ACTOR" --slug "$slug" --title "$title" -t "$type" -p "$pri" --description-file "$desc" --json 2>&1 | tail -3 >&2
    return 1
  fi
  IDS[$slug]="$id"
  echo "  $slug -> $id"
}
# body <file>  (reads heredoc from stdin into DESC_DIR/file)
body() { cat > "$DESC_DIR/bodies/$1"; }
mkdir -p "$DESC_DIR/bodies"

############################################################ UMBRELLA
cat > "$DESC_DIR/bodies/umbrella" <<'EOF'
## This epic

Single epic aggregating the ENTIRE ADR-0008 review execution: 7 tracks
(P0 soundness-ledger -> P1 concurrency -> P2 error/id -> P3 crypto ->
P4 refactor -> P5 test-coverage -> P6 doc-drift) plus cross-cutting
batch-fix-pass + baseline-rerun + process-followups.

## Goal
Turn the accepted ADR-0008 charter into a verified, batched, RED->GREEN
hardened toron tree with an honest ADR-0007 Soundness Ledger.

## Why this matters (over-arching project goal)
toron's value proposition (ADR-0003 "Parity Superiority") stakes identity /
E2E DMs / federation / non-repudiable acks on Schnorr + NIP-44 + NIP-59.
A single unverified ingest path, replay hole, or poison-on-panic lock voids
the superset claim. This review is the post-acceptance PROOF the marked-
complete ADRs are real in code, plus a batched fix of whatever adversarial
pass surfaced. Captain rejected "huge work"/"complexity" framing - this is
diligent, meticulous verification + fix.

## Closure criteria (umbrella)
- All P0-P6 track epics closed; findings tables in docs/review/0008-findings.md
- ADR-0007 Soundness Ledger S1-S9 status column updated per verdicts
- docs/parity/known-divergences.md current (incl. any type-break entries)
- CHANGELOG entry added
- `br dep cycles` empty; `bv --robot-insights | jq .Cycles` empty
- `br sync --flush-only` + git commit of .beads/

## Sequencing (charter Decision Outcome)
P0 -> P1 -> P2 -> P3 -> P4 (refactor ONLY after correctness) ->
P5 -> P6 (reflects final state) -> batch-fix-pass (RED->GREEN) ->
process-followups.
EOF
mk adr8-review-umbrella "ADR-0008 Comprehensive Codebase Review Execution" epic 0 "$DESC_DIR/bodies/umbrella"
UMB=${IDS[adr8-review-umbrella]}

############################################################ TRACK EPICS
cat > "$DESC_DIR/bodies/p0" <<'EOF'
## This track (P0 - HIGHEST LEVERAGE)
Verify each ADR-0007 Soundness Ledger claim S1-S9 + F2.KDF + bd-XXX x3 is
genuinely GREEN in code now. The ADR's own ledger recorded S3/S5/S6/S7/S9
as FAIL/PARTIAL/stuck at acceptance; captain reports "fully implemented" -
P0 PROVES it.

## Verified-green at debate time (oc ran the gates)
S1 serve_wire_gift_wrap_fires_message_posted_once (server.rs:446, 50.3s);
S2 trigger_filter 19/19; S3 cli_robot 5/5; S5 foreach_over_list_collects;
S6 call_workflow_nested_subrun_returns_child_output;
S7 coalesced_burst_publishes_one_stamped_43001; S8 resources 7/7;
F2.KDF unwrap_loop_kdf_cached_between_rebuilds_many_keys (7.7s);
bd-XXX x3 (all FIXED in crates/toron-core/src/mailbox/).

## NEW P0-candidate (oc A1.10, HIGHEST EV)
NIP-40 expiration has NO probe on mail unwrap ingest path - grep shows
NIP-40 only in reservations.rs (leases) + slots; unwrap pipeline
(unwrap.rs / mailbox/mod.rs:223-244) has no TTL check. If confirmed ->
VOIDS ADR-0003 replay/expiry "Parity Superiority" claim. Escalates to P0
crypto bug (fix under P3, gated by p0-nip40-ingest-probe leaf).

## Leaves under this epic
p0-nip40-ingest-probe (HIGHEST), p0-bdxxx-regression-tests (A1.4),
p0-s8-resources-stub-gate (A1.2/A2.4), p0-s3-shared-state-reword (A1.3),
p0-verify-green-claims (S1/S2/S4/S5/S6/S7/F2KDF doc), p0-s9-flywheel-boundary-doc.

## Closure
Every claim has file:line evidence + gate verdict (GREEN/RED) in
docs/review/0008-findings.md. Any RED -> filed as fix bead + wired into
batch-fix-pass.
EOF

cat > "$DESC_DIR/bodies/p1" <<'EOF'
## This track (P1)
Rule out poison-on-panic, deadlock-via-lock-ordering, cancel-unsafety-while-
locked across 122 .lock() / 52 real Mutex+RwLock instantiations / 1324 .await.

## Why P1 before P2/P3
Concurrency bugs are silent overnight-killers (cf hachiko thread_local->OnceLock
silent cross-thread intent loss, ADR-0040 D1 - canonical cautionary tale). A
panic on one tokio worker can brick the whole mailbox if locks aren't
poison-recoverable.

## oc A1.6 method gap (CRITICAL - .lock().await ast-grep is BLIND to these)
1. relay_bus.rs:196 `Err(RecvError::Lagged(_)) => continue` - federation bus
   silently drops lag (in-process default is per-subscriber mpsc; relay-only
   risk but real).
2. workflow_host.rs:441-461 holds `self.coalescer.lock().expect("coalescer lock")`
   (std Mutex) while publish_coalesced_job blocks on scoped thread +
   rt.block_on(bus.publish). std-guard-across-blocking-publish stall
   candidate; .lock().await ast-grep NEVER matches either site.

## Method (charter P1 + Rev 1)
ast-grep `.lock().await`, `.lock().unwrap()`, `.lock().expect(`; PLUS
`RecvError::Lagged` grep + `lock().unwrap()/expect(` within +/-10 lines of
`.await`/`block_on`/thread::scope. Multi-lock function enumeration. Per-
finding classify stall-vs-cancel + invariant-true vs real-risk.

## Closure
Concurrency findings table (site x pattern x risk x fix-class) in findings.md.
EOF

cat > "$DESC_DIR/bodies/p2" <<'EOF'
## This track (P2)
Eliminate silent-failure masking from 219 unwrap() + 984 expect() + 67
unwrap_or(0)/default. Verify id-type discipline hex<->u64<->String across
MCP<->core<->(flywheel boundary, documented).

## F3 residual kill target
unwrap_or(0) numeric coercion was ADR-0007 F3's named defect. The 67 sites
are the residual - classify invariant-true (keep, SAFETY-comment) or
real-risk (fix).

## oc A1.5 method (Rev 1)
Add ast-grep `let _ = $X` + `.ok()` (silent Result/Option drop - invisible to
.unwrap() grep; canonical deliberate case unwrap.rs:251
`let _ = self.tx.send(mail.clone())` best-effort). COUNT-CONSERVATION rule:
classified N = audited N = baseline N (219 unwrap, 984 expect, 67
unwrap_or(0)) - no cherry-picking.

## Closure
Error findings table (site x class x invariant-true-or-risk x fix) with
count-conservation row. Id-type end-to-end trace documented.
EOF

cat > "$DESC_DIR/bodies/p3" <<'EOF'
## This track (P3)
Prove ADR-0003 "Parity Superiority" claims (unforgeable identity, E2E DMs,
federatable threads, non-repudiable acks) hold on EVERY ingest path.

## oc A1.10 skill correction (Rev 1)
DROP unsafe-checker (tree is #![forbid(unsafe_code)], 0 real unsafe - skill
mismatch). Anchor on named crypto invariants.

## NIP-40 = HIGHEST (gated by p0-nip40-ingest-probe)
If P0 confirms no TTL check on unwrap path -> this track delivers the FIX
(TTL check on unwrap + RED test). Voids replay/expiry claim until fixed.

## Probes
1. verify-before-project per ingest path (named fn + line).
2. NIP-40 expiration honored on unwrap -> if absent, P0 finding + RED test.
3. replay = is_projected dedup confirmed PER sink (sqlite + git).
4. Argon2 KDF params DoS-bound (align F2.KDF).
5. Projection sink ordering atomicity (sqlite before git? git before sqlite?).
6. NIP-09 deletion handling on projection sinks.

## Closure
Crypto findings table (path x invariant x verified x risk). NIP-40 either
GREEN-with-test or RED-with-fix-merged.
EOF

cat > "$DESC_DIR/bodies/p4" <<'EOF'
## This track (P4 - AFTER P0-P3 correctness)
Map responsibility boundaries in 9 src mega-files (>1000 LOC) + propose
behavior-preserving (or API-reshape per captain decision 5) splits.

## Mega-files (src only; tests excluded - am_parity_e2e.rs 1059 is 10th if counted)
engine.rs 2401 / tools.rs 1896 / surface.rs 1561 / messaging.rs 1341 /
unwrap.rs 1114 / operator.rs 1104 / herdr.rs 1083 / workflow_host.rs 1076 /
reservations.rs 1007.

## oc A1.8 CRITICAL (Rev 1) - dead code = LIVE fixtures
clippy "dead code" (noop_runner, next_tick, housekeeper_workflow,
fixture_registration, cron_registry) are used by sc11/sc18-20/sc24 +
tests/common/mod.rs. Literal "remove" BREAKS e2e. RECLASSIFY as test-support:
wire behind #[cfg(any(test, feature = "e2e-fixtures"))] or move to
toron-e2e common. Removal FORBIDDEN unless zero test references.

## Closure
Refactor proposals table (file x split x behavior-preserving-or-reshape x
blast-radius x LOC-delta). Captain approves per-proposal (not blanket).
NO edits in this track - proposals feed batch-fix-pass.
EOF

cat > "$DESC_DIR/bodies/p5" <<'EOF'
## This track (P5)
Verify the suite proves what ADRs claim; surface gaps; add missing tests
(RED first in batch).

## oc2 A2.6 CRITICAL (Rev 1) - feature-gate silent skip
toron-e2e scenarios are feature-gated: p1=sc1-8, p2=sc9-13, p5=sc16,
p4=sc18-25 (NO default feature). Bare `cargo test --workspace` compiles
sc1-8 and SILENTLY SKIPS sc9-25. MUST run `cargo test --workspace
--all-features` (or per-gate matrix --features p1,p2,p4,p5).

## oc A1.9 D8.2 redefinition (Rev 1)
agent-mail backend does NOT exist in this tree (crates/toron-core/src/backend/
= herdr/tmux/mock/error/types only). D8.2 parity surface =
crates/toron-mcp/tests/am_parity_e2e.rs (38-tool functional parity vs
docs/parity/am-rust-tool-catalog.md) + cli_robot.rs. External agent-mail build
= flywheel CI scope (documented, not this repo). Captain decision 2 honored by
keeping am_parity_e2e GREEN against catalog.

## Targets
- sc1-sc25 full run (--all-features), classify flaky vs real.
- #[ignore] sc_fw_integrated (real flywheel binary) - still deferred? document.
- Property tests: seal/unseal, gift-wrap/unwrap round-trips, hex<->string id.
- Differential tests: bus vs git vs sqlite projection bar-for-bar agreement.
- Concurrency stress: burst coalesce, broadcast lag, multi-process durable root.

## Closure
Test-coverage findings table (ADR-claim x existing-test x pass/fail x gap x
proposed-test). Missing tests added RED-first in batch.
EOF

cat > "$DESC_DIR/bodies/p6" <<'EOF'
## This track (P6 - LAST, reflects final state)
Ensure docs match reality; no "Complete" lies in the index.

## oc A1.7 (Rev 1) - verify-adr-links.sh MISSING
docs/decisions/_scripts/ does not exist. AGENTS.md mandates the script.
Enumeration is the named gate; the batch AUTHORS the script + wires it.

## Targets
- // governed-by: ADR-XXXX markers on all files implementing an ADR constraint.
- docs/parity/known-divergences.md section 7 (KDF) current with F2.KDF gate.
- README claims vs actual surface (38 tools + 25 resources - verify count).
- ADR-0007 Soundness Ledger S1-S9 status column updated per THIS review's verdicts.
- CHANGELOG entries for batch fixes.

## Closure
Doc-drift findings table (doc-claim x code-reality x drift x fix). ADR-0007
ledger + known-divergences + README all current.
EOF

cat > "$DESC_DIR/bodies/batch" <<'EOF'
## This epic (cross-cutting)
The single governed RED->GREEN fix pass that consumes ALL P0-P6 findings.
Captain decision 4 = BATCH (collect everything, fix in one pass). Per
AGENTS.md TDD: RED (failing test) -> GREEN (minimal fix) -> REFACTOR.

## Gating
- DEPENDS on all P0-P6 track epics being closed (findings complete).
- DEPENDS on baseline-rerun (R8: re-run 5 cheap greps before fixing so
  numbers/file:line cites are not stale - precedent ADR-0007 :851-855).

## Commit discipline (charter R7)
Batch splits into logical commits (one per track's fixes) with conventional-
commit messages; captain reviews per-commit. No unreviewably-large single commit.

## Closure
All findings either fixed (GREEN test), filed as follow-on bead (with rationale),
or explicitly accepted-as-wontfix (with rationale). br sync --flush-only +
git commit + git push; git status "up to date with origin".
EOF

cat > "$DESC_DIR/bodies/baseline" <<'EOF'
## This task (R8 baseline-staleness gate, oc2 A2.9)
Re-run the 5 cheap greps (LOC, unwrap, expect, lock, await) immediately
before the batch-fix pass; annotate any delta vs the 2026-08-09 Measured
Baseline in docs/review/0008-findings.md. file:line cites in findings
re-verified at P6.

## Why
Precedent: ADR-0007 :851-855 records "several citations in the audit are
stale relative to what this ADR audits". This charter must not repeat that
failure - if any P0-P6 work (or unrelated commit) lands between baseline
measurement and the batch fix, the numbers + cites drift and the batch could
"fix" against a tree that changed under it.

## Gating
GATES batch-fix-pass (batch-fix-pass depends on this).

## Closure
Findings.md has a "Baseline re-run (pre-batch)" section with the 5 greps +
delta annotation. Any cite drift flagged.
EOF

cat > "$DESC_DIR/bodies/process" <<'EOF'
## This epic (process follow-ups, NON-BLOCKING)
File the process/tooling defects surfaced during the ADR-0008 debate
dispatch as follow-ups. These do NOT block the review but must not be lost.

## Known items
1. flywheel swarm registry BROKEN - `flywheel list`/`status`/`send` return
   empty despite `flywheel spawn` creating herdr panes + agents. Workaround
   used: drive opencode panes via `herdr pane run w29:pN 'prompt'`. File
   against flywheel repo (out of toron scope per captain decision 3).
2. flywheel spawn toron agent-name collision - `flywheel-toron-oc` bound to
   dead workspace panes; must kill stale herdr workspace before respawn.
3. Coordination dispatch pattern - "Write to file X" + "Append to file X"
   across two agents = clobber risk (oc overwrote oc2, 209->98 lines).
   Document: distinct output files per adversary, captain merges.
4. DeepSeek V4 Flash latency - ~12 min for full read+debate of 45k LOC
   charter; budget time or use a faster model for large-context debate.

## Closure
Each item filed (flywheel repo issue OR toron docs/ops note). None block
the review execution.
EOF

for spec in \
  "p0-soundness-ledger|P0 Soundness Ledger Verification|1|p0" \
  "p1-concurrency|P1 Concurrency Correctness|1|p1" \
  "p2-error-id|P2 Error-Handling + Id Discipline|1|p2" \
  "p3-crypto-giftwrap|P3 Crypto / NIP-17 / Gift-Wrap Soundness|1|p3" \
  "p4-refactor|P4 Mega-File Refactor Candidates|2|p4" \
  "p5-test-coverage|P5 Test Coverage + e2e Parity Gaps|2|p5" \
  "p6-doc-drift|P6 Doc / ADR <-> Code Drift|2|p6" \
  "batch-fix-pass|Batch Fix Pass (RED->GREEN)|1|batch" \
  "baseline-rerun|Baseline Re-run Before Batch (R8)|0|baseline" \
  "process-followups|Process / Tooling Follow-ups|3|process" \
; do
  IFS='|' read -r slug title pri body <<< "$spec"
  mk "$slug" "$title" epic "$pri" "$DESC_DIR/bodies/$body"
  br dep add "${IDS[$slug]}" "$UMB" --type blocks >/dev/null 2>&1
done

############################################################ LEAVES
# P0 leaves
cat > "$DESC_DIR/bodies/l-nip40-probe" <<'EOF'
## This task (HIGHEST EV of the entire review, oc A1.10)
Probe whether NIP-40 `expiration` is honored on the MAIL UNWRAP ingest path.

## Evidence of suspected defect
grep shows NIP-40 only in reservations.rs (leases) + build_slots; the unwrap
pipeline (crates/toron-core/src/mailbox/unwrap.rs +
mailbox/mod.rs:223-244 unwrap_loop) has NO TTL check found. An expired-but-
never-seen gift wrap would be ACCEPTED, voiding ADR-0003 "Parity Superiority"
replay/expiry claim.

## Method
1. Trace every ingest path into unwrap_loop / ProjectionSink.
2. For each: is there an expiration check (NIP-40 tag parse + reject-if-past)
   BEFORE the event is projected to sqlite/git?
3. If absent -> file as P0 crypto finding + escalate: spawns
   p3-nip40-expiration-fix (RED test first, then the TTL check).

## Success criteria
- Verdict GREEN: TTL check exists at named fn+line; RED test
  `expired_gift_wrap_rejected_on_unwrap` authored + passes.
- Verdict RED: no TTL check found -> p3-nip40-expiration-fix unblocked;
  findings.md records the P0 crypto finding with file:line.

## Test plan (RED first if RED)
Property test: gift wrap with expiration in the past -> unwrap rejects.
Regression: gift wrap with expiration in future -> unwrap accepts.
EOF

cat > "$DESC_DIR/bodies/l-bdxxx-regr" <<'EOF'
## This task (oc A1.4 - regression pins for already-fixed bd-XXX bugs)
The 3 inline bd-XXX parity bugs are FIXED in code but 2 of 3 are UNPINNED by
regression tests. A future refactor silently re-introduces the ADR-0007 FAIL
cells.

## Sites (crates/toron-core/src/mailbox/)
- build_slots.rs:117 - renew base = existing lease expiry (:120-121), not now.
  Current test renew_and_release_lifecycle (:271) asserts only
  `renewed.is_some()` - under-extension bug would PASS.
- identity.rs:73 - create fails closed on existing key (:80-84). PINNED by
  test_create_always_different_npub_or_refuses_collision (no work needed).
- reservations.rs:239 - renewal = old absolute expiry + extend (:240-247),
  no 2x. Current test_renew (:735) asserts only `renewed == 1` - 2x-expiry
  bug would PASS.

## Method (TDD RED->GREEN)
For build_slots + reservations: write a failing test that asserts the FIXED
absolute-expiry renewal semantics (renewed expiry = old + extend, NOT now +
extend, NOT 2x). Tests should FAIL if the fix regresses. Run RED first
(confirms test bites), then the existing fix keeps them GREEN.

## Success criteria
- build_slots: regression test asserts renewed expiry == old_expiry + extend.
- reservations: regression test asserts renewed expiry == old_expiry + extend (not 2x).
- identity: already pinned, verify the existing test still bites.

## Considerations
These are guardrails, not features. The comments at the 3 sites document
WHY the fix matters - keep the comments; the tests make them enforceable.
EOF

cat > "$DESC_DIR/bodies/l-s8-stub" <<'EOF'
## This task (oc A1.2 + oc2 A2.4 - S8 honest-stub gate)
Tighten the S8 gate so the "honest structured stub" escape hatch cannot wave
through a placeholder.

## Evidence
resources.rs:11-129 = exactly 25 RESOURCES entries. resources.rs:177
metrics_core stub fields `"database": "not wired", "storage": "not wired"`.
The charter's original gate banned "not yet wired" but allowed "honest
structured stub" - ambiguous, lets metrics_core pass without a rule.

## Method (Rev 1 gate wording)
Each of 25 resources resolves to non-empty JSON. Extend
static_resources_serve_json / data_backed_resources_project_mailbox_data to
cover ALL 25 URIs. Honest stubs allowed ONLY with honest field values + a
`status` field naming the live tool (metrics_core qualifies - its fields are
honest "not wired" + it names the live tools path). Placeholder strings
WITHOUT a status field = RED.

## Success criteria
- Test asserts all 25 URIs return non-empty JSON.
- metrics_core documented as honest stub (allowed); any placeholder lacking
  status field flagged RED in findings.

## Considerations
Do NOT fake metrics to make the stub "real" - honesty over cosmetics (oc A1.2
explicit). The stub naming the live tool is the contract.
EOF

cat > "$DESC_DIR/bodies/l-s3-reword" <<'EOF'
## This task (oc A1.3 - S3 shared-state wording + mail-content RED test)
Reword the S3 claim to "shared durable AGENT STATE (identity/keys)" (what
cli_robot.rs actually proves) AND add a RED test for mail-content sharing
(currently untested).

## Evidence
cli_robot.rs:71-94 inbox_parity_emits_valid_json asserts exit 0 + array shape
only. The empirical fail-closed (isolated root -> exit 1 "agent has no local
key") proves IDENTITY sharing, not mail-content sharing.

## Method
1. Reword the S3 ledger cell to "shared durable agent state (identity/keys)".
2. RED test in batch: register agent in-process -> send message -> shell out
   to toron binary on same TORON_MCP_DATA_DIR -> subprocess inbox contains
   the message. Currently this would FAIL (mail-content sharing untested) ->
   GREEN when the durable root actually shares projected mail.

## Success criteria
- Ledger wording honest.
- RED test authored; passes GREEN after the durable root shares mail content
  (or documents the gap if it doesn't).

## Considerations
If the subprocess CANNOT see in-process-sent mail, that's a real S3 gap ->
file as P0 finding (re-opens the "fully implemented" claim for S3 specifically).
EOF

cat > "$DESC_DIR/bodies/l-verify-green" <<'EOF'
## This task (verify + document the already-green claims)
oc RAN these gates GREEN at debate time - this task confirms they STAY green
at execution time and records the file:line evidence in findings.md (no code
change unless a gate has regressed).

## Claims to re-verify
- S1 serve_wire_gift_wrap_fires_message_posted_once (server.rs:446) - run, confirm PASS.
- S2 trigger_filter 19/19 (trigger_filter.rs) - run, confirm.
- S4 id args all String (tools.rs:1512/1523/1607) - audit, confirm no i64/u64.
- S5 foreach_over_list_collects (engine.rs:2041) - run, confirm.
- S6 call_workflow_nested_subrun_returns_child_output (engine.rs:2136) - run, confirm.
- S7 coalesced_burst_publishes_one_stamped_43001 (workflow_host.rs:824) - run, confirm.
- F2.KDF unwrap_loop_kdf_cached_between_rebuilds_many_keys (mailbox/mod.rs:540) - run, confirm.

## Method
Run each gate test; record actual output line in findings.md. If any
regressed -> file as P0 finding immediately.

## Success criteria
All 7 re-verify GREEN with output lines in findings.md. No regression.

## Considerations
This is the cheap confirmation pass - high confidence (oc ran them ~12h ago)
but the tree may have moved. Re-running is the honest verification.
EOF

cat > "$DESC_DIR/bodies/l-s9-doc" <<'EOF'
## This task (docs, oc - S9 flywheel boundary, captain decision 3)
Document the ADR-0007 S9 gap (Nostr is AM superset at the FLYWHEEL trait
boundary) in docs/parity/known-divergences.md. Do NOT edit flywheel (captain
decision 3 - toron only).

## Evidence
flywheel MailBackend trait is still AM-shaped (u64 ids at the boundary per
ADR-0007 S9 FAIL). toron's own surface is correct on its own terms (S4 GREEN
- all String). The gap is at the integration seam, not toron's fault.

## Method
Add a known-divergences entry: "flywheel MailBackend trait remains AM-shaped
(u64 ids); toron MailSurface is String-native; coercion at the flywheel edge
is a follow-on charter (ADR-0009 candidate), out of scope for ADR-0008."

## Success criteria
known-divergences.md has the S9 entry; no flywheel edits.

## Considerations
Filing this honestly prevents the gap being silently lost. A future ADR-0009
can charter the flywheel-side fix.
EOF

# P1 leaves
cat > "$DESC_DIR/bodies/l-std-guard" <<'EOF'
## This task (oc A1.6 - the ast-grep-INVISIBLE concurrency class)
Audit std-Mutex-guard-held-across-blocking-publish. The `.lock().await`
ast-grep CANNOT see this class because the lock is std (not async) + the
block happens via rt.block_on / scoped thread.

## Prime suspect
workflow_host.rs:441-461 holds `self.coalescer.lock().expect("coalescer lock")`
(std::sync::Mutex) while publish_coalesced_job (workflow_host.rs:207-218)
blocks on a scoped thread + rt.block_on(bus.publish). If the publish blocks
slowly (relay latency, backpressure), the coalescer lock is held the whole
time -> any other tick flush on a different thread stalls.

## Method
1. ast-grep / grep: `lock().unwrap()` + `lock().expect(` within +/-10 lines
   of `.await` / `block_on` / `thread::scope` / `spawn_blocking`.
2. For each hit: is the guard held across the blocking call? Classify
   stall-vs-cancel.
3. If workflow_host.rs:441-461 confirmed -> fix: drop the guard before the
   blocking publish (clone the coalesced job out under the lock, release,
   then publish).

## Success criteria
- Findings table row for the suspect + any siblings.
- If real: RED test (concurrent tick flush stalls while publish blocks) +
  fix in batch.

## Considerations
Same site flagged in charter S7 row (poison risk) - this task covers the
STALL risk; p1-poison-on-panic-audit covers the POISON risk. Both real.
EOF

cat > "$DESC_DIR/bodies/l-lagged" <<'EOF'
## This task (oc A1.6 - broadcast lag swallow)
relay_bus.rs:196 `Err(RecvError::Lagged(_)) => continue` silently drops lag
events on the federation bus.

## Evidence
The default in-process bus is per-subscriber mpsc (in_process.rs:13 comment)
so lag is impossible there. RelayBus (nostr-sdk Client over WS) uses tokio
broadcast -> lag possible under burst. Silent `continue` = the subscriber
misses events with no signal.

## Method
1. Confirm relay_bus.rs:196 still swallows.
2. Decide: surface (log + metric + re-sync) or keep silent (documented
   best-effort). ADR-0004 driver = "agent-automation context budget is
   scarce" -> a missed event is worse than a loud log IF the event was a
   trigger; but lag on a federation bus is often acceptable.
3. If surface: log at warn with the lag count + the missed event range;
   add a metric for ops.

## Success criteria
Decision recorded in findings.md. If surface -> RED test (inject lag, assert
log/metric) + fix in batch.

## Considerations
Flywheel boundary (relay_bus is toron-core, consumed by flywheel daemon mode)
- but the fix is in toron, so in scope.
EOF

cat > "$DESC_DIR/bodies/l-poison" <<'EOF'
## This task (poison-on-panic survivability)
Every `.lock().unwrap()` / `.lock().expect(...)` - does a single panic on
one tokio worker brick the whole mailbox? ADR-0007 D7 recovery posture.

## Method
1. Enumerate all 122 .lock() sites; for each std::sync::Mutex, classify:
   is the guard recovered on poison (PoisonError -> re-init) or does it
   propagate (panic-in-panic = dead service)?
2. tokio::sync::Mutex does NOT poison (no PoisonError) - those are safe;
   focus on std::sync::Mutex.
3. Charter S7 row specifically flags workflow_host.rs:156
   `.lock().expect("coalescer lock")` - if the coalescer panics, every
   subsequent flush panics too.

## Success criteria
Per-site verdict in findings table. Any poison-propagating std Mutex holding
shared state -> fix (recover from PoisonError or switch to parking_lot::Mutex
which doesn't poison, or re-architect to avoid the shared mutable).

## Considerations
parking_lot::Mutex doesn't poison but changes the dep tree - weigh against
just handling PoisonError. Pre-1.0 so either is fine (captain decision 5).
EOF

cat > "$DESC_DIR/bodies/l-lock-order" <<'EOF'
## This task (deadlock via lock ordering)
Functions acquiring >=2 locks - is there a consistent global lock order?

## Method
1. Enumerate functions that acquire >=2 locks (ast-grep for sequential
   `.lock()` calls in one fn).
2. Build the lock-order graph; check for cycles (A->B in one fn, B->A in another).
3. Common toron suspects: mailbox holding bus + index lock; workflow_host
   holding coalescer + bus; reservations holding map + expiry.

## Success criteria
Lock-order graph in findings.md. Any cycle -> fix (impose a documented global
order + enforce with a lint or a wrapper type).

## Considerations
Most multi-lock sites are fine (acquire-and-release in sequence). The risk is
nested holds. Document the order even if no cycle today.
EOF

cat > "$DESC_DIR/bodies/l-await-locked" <<'EOF'
## This task (cancel-safety / reentrancy - guard held across .await)
`.await` while holding a std::sync::Mutex (impossible - std guard isn't Send
across await in tokio) or tokio::sync::Mutex guard.

## Method
1. ast-grep `.lock().await` - each is a tokio guard held across await.
2. For each: is the held-across-await intentional (serialized section) or
   accidental (long-held lock blocking other tasks)? Cancel-safety: if the
   task is cancelled mid-await, is the guard dropped cleanly (RAII) or leaked?
3. tokio::sync::Mutex is cancel-safe (guard Drop on cancel). std is not held
   across await (compiler rejects). So focus on tokio guard contention.

## Success criteria
Findings table: each `.lock().await` site classified intentional-vs-accidental
+ contention note.

## Considerations
Related to p1-std-guard-across-blockon (the ast-grep-invisible sibling) -
together they cover the full held-guard-during-blocking surface.
EOF

cat > "$DESC_DIR/bodies/l-backpressure" <<'EOF'
## This task (performance deliverable, oc2 coherence finding)
Captain decision 6 = correctness AND performance. This task owns the
performance MEASURED BASELINE (the charter encoded performance only as a risk
mitigation R5; oc2 flagged it needs a table row).

## Method
1. Establish measured baseline for: broadcast bus throughput + lag ceiling
   under burst; projection latency (sqlite sink p50/p95/p99 per 1000 events);
   KDF cold-start (already F2.KDF gate, 50-key); search landing latency.
2. Propose SLOs for ADR-0009 (no explicit SLO today - this review establishes
   the measured numbers).

## Success criteria
Performance baseline table in findings.md (metric x measured x proposed-SLO).
Any pathological number (e.g. projection p99 > 100ms) -> filed as perf bead.

## Considerations
This is the "both if possible" answer to captain decision 6. No SLO means
no pass/fail, but the measured numbers are the input to a future SLO.
EOF

# P2 leaves
cat > "$DESC_DIR/bodies/l-silent-drop" <<'EOF'
## This task (oc A1.5 - silent Result/Option drop)
`let _ = $X`, `.ok();`, ignored Result/Option - invisible to the .unwrap()
ast-grep but a real silent-failure class.

## Evidence
Canonical deliberate case: unwrap.rs:251 `let _ = self.tx.send(mail.clone())`
(best-effort trigger notify - drop on purpose). Accidental siblings would
not surface under the unwrap/expect audit.

## Method
1. ast-grep `let _ = $X` + `.ok()` + bare `fn_call()` returning Result/Option
   not bound.
2. For each: classify deliberate (best-effort, document with reason) vs
   accidental (silent failure - fix to ? or handle).

## Success criteria
Findings table: each silent-drop site classified deliberate-vs-accidental.
Accidental ones -> fix in batch.

## Considerations
Feeds p2-count-conservation (the total audited must equal baseline).
EOF

cat > "$DESC_DIR/bodies/l-coerce" <<'EOF'
## This task (F3 residual kill target)
67 `unwrap_or(0)` / `unwrap_or_default` sites in src. ADR-0007 F3 named
numeric coercion as the defect (u64 id fallback to 0 masked missing ids).

## Method
1. Enumerate all 67 sites.
2. For each: is the 0/default a sane invariant fallback (e.g. counter init)
   or a silent failure mask (e.g. id parse -> 0 -> wrong agent)?
3. The id-coercion sites specifically: trace hex-event-id -> String parse;
   any unwrap_or(0) on an id path = the F3 residual -> fix to error.

## Success criteria
All 67 classified. id-path coercion sites fixed (error, not 0). Count-
conservation row in findings.

## Considerations
This is the named ADR-0007 defect - high signal even if most of the 67 are
benign counter inits.
EOF

cat > "$DESC_DIR/bodies/l-count" <<'EOF'
## This task (oc A1.5 - count-conservation gate)
The P2 audit must show "classified N = audited N = baseline N" for the 219
unwrap + 984 expect + 67 unwrap_or(0) populations. Prevents a partial pass
being rubber-stamped.

## Method
1. After p2-unwrap-expect-classify + p2-silent-result-drop + p2-unwrap-or-zero-coercion
   run, sum the classified counts.
2. Assert classified == baseline (219, 984, 67). Any gap = uncategorized site
   -> must be categorized before P2 closes.

## Success criteria
findings.md P2 section has the count-conservation row, all three populations
reconcile.

## Considerations
This is a gate, not new findings. It certifies the other P2 leaves are complete.
EOF

cat > "$DESC_DIR/bodies/l-classify" <<'EOF'
## This task (the bulk classification)
Classify the 219 unwrap() + 984 expect() populations invariant-true vs real-risk.

## Method
1. ast-grep `$X.unwrap()` + `$X.expect($MSG)`.
2. For each: invariant-true (the value is guaranteed by construction - keep,
   maybe add SAFETY-style comment) vs real-risk (I/O / external input / lock /
   parse / serde - fix to ? or handle).
3. expect() messages: are they on non-invariant paths (silent masking via a
   confident message)? Flag those.

## Success criteria
Findings table: each site classified. real-risk sites -> fix in batch.

## Considerations
984 expect() is large; many are in tests (cfg(test)) - exclude those from the
prod classification. The 219 unwrap() are the higher-signal population.
EOF

cat > "$DESC_DIR/bodies/l-id-e2e" <<'EOF'
## This task (id-type discipline end-to-end)
hex event id <-> u64 <-> String across MCP tools <-> core <-> (flywheel
boundary, documented).

## Evidence (oc verified GREEN at debate)
S4 confirmed: AcknowledgeMessageArgs.message_id: String (tools.rs:1512),
MarkMessageReadArgs.message_id: String (tools.rs:1523),
ForceReleaseFileReservationArgs.file_reservation_id: String (tools.rs:1607),
ids.rs:17 MessageId(String) newtype with FromStr rejecting empty/whitespace.

## Method
1. Trace every id-bearing type through MCP -> core -> projection sinks.
2. Confirm no i64/u64 leak in the toron surface (S4 GREEN says no).
3. The flywheel boundary (u64 at MailBackend) = documented in S9, not edited.

## Success criteria
End-to-end id trace in findings.md. toron surface confirmed all-String.

## Considerations
This closes the ADR-0007 F3 loop for toron's own surface. Low risk (oc verified)
but the trace documents it for future maintainers.
EOF

cat > "$DESC_DIR/bodies/l-error-enum" <<'EOF'
## This task (error-enum discipline)
thiserror error enums vs ad-hoc string errors; ? discipline; error message
quality.

## Method
1. Enumerate error types (grep `enum.*Error` + `#[derive(.*Error.*)]`).
2. Are there ad-hoc `Box<dyn Error>` / String errors? Flag.
3. ? propagation discipline: any `.unwrap()`/`expect()` on a Result that
   SHOULD propagate?
4. Message quality: actionable ("failed to open {path}: {src}") vs opaque
   ("error").

## Success criteria
Findings table: error-enum inventory + ad-hoc instances + message-quality
notes.

## Considerations
Lower priority (P3 within P2) - toron likely already uses thiserror consistently
(verify). This is polish, not a bug hunt.
EOF

# P3 leaves
cat > "$DESC_DIR/bodies/l-nip40-fix" <<'EOF'
## This task (the FIX, gated by p0-nip40-ingest-probe)
If P0 confirms NIP-40 expiration is NOT honored on the unwrap ingest path ->
this bead delivers the fix. TDD RED->GREEN.

## Dependencies
DEPENDS on p0-nip40-ingest-probe verdict. If GREEN (TTL exists) -> close
this bead as wontfix-with-evidence. If RED -> execute.

## Method (RED first)
1. RED: `expired_gift_wrap_rejected_on_unwrap` - construct a gift wrap with
   NIP-40 expiration tag in the past; assert unwrap_loop rejects it (does
   NOT project to sqlite/git). Confirm test FAILS today.
2. GREEN: add expiration check in unwrap pipeline - parse NIP-40 tag, compare
   to now, reject-if-past BEFORE projection. Minimal fix.
3. REFACTOR: extract the TTL check into a named helper for clarity.

## Success criteria
RED test passes GREEN. Unwrap pipeline rejects expired events. findings.md
records the P0 crypto finding + fix.

## Considerations
This is the single highest-EV fix of the review if the probe is RED. It
restores the ADR-0003 replay/expiry "Parity Superiority" claim.
EOF

cat > "$DESC_DIR/bodies/l-schnorr" <<'EOF'
## This task (verify-before-project per ingest path)
Schnorr signature verification on EVERY ingest path (forge resistance).

## Method
1. Trace every path an event enters the system (InProcessBus publish,
   RelayBus ingest, unwrap pipeline input).
2. For each: is there a verify_event (nostr/event.rs) call BEFORE the event
   is projected/indexed/acted-on?
3. Name the fn + line for each ingest path's verify.

## Success criteria
Findings table: ingest-path x verify-fn x line. Any path missing verify ->
P0 finding (forge vulnerability).

## Considerations
High likelihood GREEN (nostr-sdk verify is standard) but the trace proves it.
EOF

cat > "$DESC_DIR/bodies/l-giftwrap-meta" <<'EOF'
## This task (gift-wrap metadata leak)
One-time random key per wrap; no metadata leak; seal re-sign; sender-mismatch
detection.

## Method
1. gift_wrap.rs: confirm each wrap uses a FRESH one-time key (not the
   sender's identity key - that would leak sender via the wrap's pubkey).
2. Seal (kind-13) is sender-SIGNED (not just encrypted) - verify.
3. SenderMismatch detection (gift_wrap.rs:100 verify_event(&seal),
   :114-117) - confirm it fires on tampered seal.

## Success criteria
Findings table: each invariant verified with fn+line. Property test:
wrap->unwrap round-trip + tampered-seal-rejected.

## Considerations
This is the E2E privacy claim. NIP-59 compliance is load-bearing for
"federated relays/bus observers can't read DMs".
EOF

cat > "$DESC_DIR/bodies/l-replay" <<'EOF'
## This task (replay protection - is_projected per sink)
Event id dedup + replay rejection on EACH projection sink (sqlite + git).

## Method
1. For SqliteProjectionSink + GitProjectionSink: is there an is_projected
   check before write? (idempotent re-projection must not duplicate.)
2. Replay: same event published twice -> projected once?
3. NIP-40 expiration (cross-ref p0-nip40-ingest-probe) - expired events
   rejected pre-projection.

## Success criteria
Findings table: sink x dedup-mechanism x line. Replay test: double-publish
-> single projection.

## Considerations
Sqlite has UNIQUE(event_id) constraint likely; git append needs explicit
dedup. Verify both.
EOF

cat > "$DESC_DIR/bodies/l-kdf-dos" <<'EOF'
## This task (Argon2 KDF params DoS-bound)
Align with F2.KDF gate - KDF params (memory/time) must be DoS-bound.

## Evidence
F2.KDF GREEN: cached_load_agent_key (mailbox/mod.rs:175) + kdf_load_count
counter + 50-key gate test. The CACHE bounds rebuild cost; the PARAMS bound
per-key cost.

## Method
1. Find the Argon2 param config (memory_kb, time_cost, parallelism).
2. Are they sane (e.g. OWASP min: 19 MiB / 2 / 1) or weak?
3. Per-key cost ~1s (per ADR-0007 C2). Is that acceptable for a DoS attacker
   who forces key scans? The cache mitigates RE-scans but cold = N x ~1s.

## Success criteria
Findings table: KDF params + assessment. If weak -> fix bead (but F2.KDF gate
already bounds the practical impact).

## Considerations
F2.KDF cache is the real protection; params are defense-in-depth. Low risk
but document.
EOF

cat > "$DESC_DIR/bodies/l-projection-atom" <<'EOF'
## This task (projection sink ordering + atomicity)
sqlite vs git sink ordering - what happens if one succeeds and the other
fails mid-projection?

## Method
1. unwrap.rs: what's the sink invocation order (sqlite then git? git then
   sqlite? both in TriggerNotifySink wrapper?)?
2. If sqlite succeeds + git fails: is the event marked projected (sqlite) or
   rolled back? A partial projection = inconsistency between the two stores.
3. Is there a transaction/compensation, or is eventual-consistency acceptable
   (ADR-0003 says "two sources (Git + relay replay) either alone rebuilds")?

## Success criteria
Findings table: ordering + failure semantics. If partial-projection
inconsistent -> document acceptable (rebuild-from-either) OR fix.

## Considerations
ADR-0003's disaster-recovery posture ("either alone rebuilds") may make
partial projection acceptable IF a reconcile/heal exists. Check
bus/reconcile.rs.
EOF

# P4 leaves
cat > "$DESC_DIR/bodies/l-deadcode" <<'EOF'
## This task (oc A1.8 - dead code = LIVE fixtures, reclassify NEVER remove)
clippy flags noop_runner, next_tick, housekeeper_workflow, fixture_registration,
cron_registry as "dead code" but they are LIVE test fixtures.

## Evidence
noop_runner: engine.rs:2211 (call_workflow test) + toron-cli/src/workflow.rs
(2 sites) + sc11/sc18/sc19/sc20 e2e.
next_tick: cron.rs + tests/common/mod.rs.
housekeeper_workflow: tests/common/mod.rs (via housekeeper_resolver, sc18-20).
fixture_registration: sc24_cron_catchup.rs + common/mod.rs.
cron_registry: common/mod.rs.

## Method
1. Verify each is referenced (the evidence above).
2. Reclassify: wire behind #[cfg(any(test, feature = "e2e-fixtures"))] OR
   move to toron-e2e common module.
3. Removal FORBIDDEN unless zero test references (none qualify).

## Success criteria
clippy --all-targets no longer flags these (cfg-gated or moved). e2e suite
still compiles + passes.

## Considerations
Literal "remove" would break sc11/sc18-20/sc24 silently. This is the
highest-risk P4 task (batch could break the suite if misread).
EOF

cat > "$DESC_DIR/bodies/l-engine-map" <<'EOF'
## This task (engine.rs 2401 LOC responsibility map + split proposal)
Map responsibility boundaries; propose behavior-preserving split.

## Method
1. Section engine.rs by concern: dispatch, cron, lint, jobs, foreach,
   call_workflow, template interpolation, WorkflowResolver DI, tests.
2. Propose split: e.g. engine/mod.rs (dispatch + run) + engine/control_flow.rs
   (foreach/call_workflow/branch/retry) + engine/template.rs (interpolation)
   + engine/tests.rs.
3. NO edits - proposal only. Captain approves per-proposal.

## Success criteria
Responsibility map + split proposal in findings.md. Blast-radius +
LOC-delta per split.

## Considerations
engine.rs is the workflow engine core - high blast radius. Behavior-
preserving move-only first; reshape only if captain approves.
EOF

cat > "$DESC_DIR/bodies/l-tools-split" <<'EOF'
## This task (tools.rs 1896 LOC - 38 tool defs, split by domain)
Propose split by domain: mail / reservation / product / slot / contact /
workflow / system.

## Method
1. Inventory the ~39 #[tool( defs by domain.
2. Propose: tools/mail.rs, tools/reservation.rs, tools/product.rs,
   tools/slot.rs, tools/contact.rs, tools/workflow.rs, tools/system.rs
   + tools/mod.rs dispatcher.
3. NO edits - proposal only.

## Success criteria
Domain inventory + split proposal in findings.md.

## Considerations
38 tool names are FROZEN (ADR-0007 C3) - the split is internal organization,
the MCP surface must not change. Behavior-preserving.
EOF

cat > "$DESC_DIR/bodies/l-surface-messaging" <<'EOF'
## This task (surface.rs 1561 + messaging.rs 1341 - overlap/dedup audit)
Are these two modules duplicating responsibility?

## Method
1. Map each module's public API + responsibility.
2. surface.rs likely = MailSurface trait + ToronMailSurface impl (ADR-0007 D1-D2).
3. messaging.rs likely = the send/reply/inbox business logic.
4. If overlap: propose extraction (shared helper module) or clearer boundary.

## Success criteria
Overlap audit in findings.md. Proposal if duplication found.

## Considerations
May be clean separation already - verify before proposing churn.
EOF

cat > "$DESC_DIR/bodies/l-bus-dedup" <<'EOF'
## This task (in_process.rs vs relay_bus.rs - duplication audit)
Both implement the Bus trait; check for copy-paste logic that should be
shared.

## Method
1. Map shared logic (event serialization, filter matching, subscription
   management) across the two impls.
2. If duplicated: propose a shared helper (bus/common.rs) or trait extension.

## Success criteria
Dedup audit in findings.md. Proposal if duplication found.

## Considerations
in_process = tokio broadcast + sqlite; relay_bus = nostr-sdk Client over WS.
Different transports but shared Nostr event semantics - some dedup likely.
EOF

# P5 leaves
cat > "$DESC_DIR/bodies/l-allfeatures" <<'EOF'
## This task (oc2 A2.6 - CRITICAL baseline test run)
`cargo test --workspace --all-features` to actually run sc9-sc25 (feature-gated).

## Evidence
toron-e2e Cargo.toml:6-13 defines p1 (sc1-8), p2 (sc9-13), p5 (sc16),
p4 (sc18-25) with NO default feature. Bare `cargo test --workspace` compiles
sc1-8 and SILENTLY SKIPS sc9-25.

## Method
1. Run `cargo test --workspace --all-features` (or per-gate matrix
   `--features p1,p2,p4,p5`).
2. Capture per-test result. Classify flaky vs real failure.
3. Record the FULL sc1-sc25 status (not the partial sc1-8).

## Success criteria
Full test matrix result in findings.md. Any failure classified.

## Considerations
This should run EARLY (it's the baseline health check). Feeds P5 gap analysis.
EOF

cat > "$DESC_DIR/bodies/l-d82" <<'EOF'
## This task (oc A1.9 - D8.2 parity surface verification)
D8.2 = am_parity_e2e.rs + cli_robot.rs pass against docs/parity/am-rust-tool-catalog.md
+ known-divergences.md. agent-mail backend does NOT exist in this tree.

## Method
1. Run crates/toron-mcp/tests/am_parity_e2e.rs (38-tool functional parity
   vs catalog). This boots the REAL toron MCP daemon + drives all 38 tools
   over Streamable HTTP.
2. Run cli_robot.rs (subprocess toron binary on shared durable root).
3. Both must pass against the catalog.

## Success criteria
Both green. Captain decision 2 (agent-mail fallback) honored by am_parity_e2e
passing (it tests the Nostr surface that agent-mail would integrate with).

## Considerations
External agent-mail build = flywheel CI scope, NOT this repo. Document that.
EOF

cat > "$DESC_DIR/bodies/l-prop-crypto" <<'EOF'
## This task (crypto property tests)
seal/unseal round-trip, gift-wrap/unwrap round-trip property tests.

## Method
proptest or quickcheck:
1. For random rumor content: seal(unseal(seal(content))) == content.
2. gift-wrap: unwrap(wrap(rumor, recipient), recipient_key) == rumor.
3. Tampered seal -> SenderMismatch error.
4. Wrong-recipient unwrap -> failure.

## Success criteria
Property tests authored (RED if missing, GREEN in batch).

## Considerations
These are the regression nets for p3-gift-wrap-metadata-leak + p3-schnorr.
EOF

cat > "$DESC_DIR/bodies/l-prop-projection" <<'EOF'
## This task (differential projection tests)
bus projection vs git projection vs sqlite projection must agree bar-for-bar.

## Method (cf hachiko incremental-indicator differential pattern)
1. Publish N events through the bus.
2. Read projections from all 3 sinks (bus event_store, git archive, sqlite
   messages table).
3. Assert bar-for-bar agreement (same event ids, same order or documented
   ordering difference).

## Success criteria
Differential test authored. Any disagreement -> P0 finding (projection
inconsistency).

## Considerations
Relates to p3-projection-atomicity (the failure-semantics question). Together
they certify projection soundness.
EOF

cat > "$DESC_DIR/bodies/l-stress" <<'EOF'
## This task (concurrency stress tests)
burst coalesce, broadcast lag, multi-process durable root.

## Method
1. Burst: fire 100 mentions in 1s -> assert coalescer produces bounded jobs
   (not 100).
2. Broadcast lag: saturate the bus -> assert lag handling (cf p1-broadcast-lag-handling).
3. Multi-process: 2 toron binaries on one TORON_MCP_DATA_DIR -> assert
   cross-process visibility (cf p0-s3-shared-state-reword).

## Success criteria
Stress tests authored. Find any race/deadlock/durability gap.

## Considerations
These are the integration-level nets above unit tests. High signal for
overnight reliability.
EOF

cat > "$DESC_DIR/bodies/l-scfw" <<'EOF'
## This task (#[ignore] sc_fw_integrated resolution)
sc_fw_integrated.rs is #[ignore]d - it boots the real flywheel binary. Still
deferred? Document why.

## Method
1. Read sc_fw_integrated.rs:23 + :197 - the ignore reason.
2. Is the flywheel-binary dependency still blocking? (captain decision 3 =
   toron only, so a flywheel-binary e2e may be permanently out of scope.)
3. Decision: keep ignored + document, OR convert to a toron-only integration
   test, OR delete if obsolete.

## Success criteria
Decision recorded in findings.md. No silent-ignored tests without a reason.

## Considerations
Captain decision 3 (toron only) likely means this stays ignored with a clear
"requires flywheel binary, out of toron scope" note.
EOF

# P6 leaves
cat > "$DESC_DIR/bodies/l-verify-script" <<'EOF'
## This task (oc A1.7 - author the missing verify-adr-links.sh + wire it)
docs/decisions/_scripts/ does not exist. AGENTS.md mandates the script. The
batch AUTHORS it.

## Method
1. Write docs/decisions/_scripts/verify-adr-links.sh: enumerate src + tests
   for `// governed-by: ADR-XXXX` markers; cross-ref against
   docs/decisions/*.md; report files implementing an ADR constraint that
   lack the marker (and markers referencing non-existent ADRs).
2. Wire into the repo (pre-commit hook or CI step - decide).
3. Run it; fix any missing markers in batch.

## Success criteria
Script exists, executable, wired. Output clean (all governed files marked).

## Considerations
This is the ADR-code-linkage enforcement. AGENTS.md says "verified by
docs/decisions/_scripts/verify-adr-links.sh" - make it real.
EOF

cat > "$DESC_DIR/bodies/l-governed" <<'EOF'
## This task (oc A1.7 - enumerate governed-by markers)
Companion to p6-verify-adr-links-script: the enumeration that the script will
automate, done once by hand to seed it.

## Method
1. grep `// governed-by: ADR-` across crates/*/src + crates/*/tests.
2. Cross-ref: every file implementing an ADR constraint has a marker?
3. Every marker references an existing ADR?

## Success criteria
Marker inventory in findings.md. Missing markers filed for batch fix.

## Considerations
Feeds p6-verify-adr-links-script (the script automates this going forward).
EOF

cat > "$DESC_DIR/bodies/l-ledger-update" <<'EOF'
## This task (docs - update ADR-0007 Soundness Ledger per this review's verdicts)
ADR-0007 §Soundness Ledger S1-S9 status column must reflect ADR-0008's findings.

## Method
After all P0-P6 findings land, update the ledger cells:
- S1-S9: status from this review (GREEN/RED/STUB) + evidence ref to findings.md.
- F2.KDF: confirm GREEN with the 50-key gate.
- bd-XXX x3: confirm GREEN + note the new regression pins (p0-bdxxx-regression-tests).
- F2.NIP40: NEW row from ADR-0008 if the probe was RED.

## Success criteria
ADR-0007 ledger current. No "FAIL"/"stuck" cells that this review proved GREEN.

## Considerations
This is the honesty anchor - the ledger must not lie about status.
EOF

cat > "$DESC_DIR/bodies/l-divergences" <<'EOF'
## This task (docs - known-divergences.md update)
section 7 (KDF) current with F2.KDF gate; add any type-break entries from
the batch fix; add the S9 flywheel-boundary entry (p0-s9-flywheel-boundary-doc).

## Method
1. Confirm section 7 KDF entry matches F2.KDF gate result (cached_load_agent_key,
   50-key, O(delta) rebuild).
2. For each type-break in the batch (if any API reshape per captain decision 5):
   add an entry per ADR-0007 C3 discipline.
3. Add S9 flywheel MailBackend gap entry.

## Success criteria
known-divergences.md current + complete.

## Considerations
The frozen surfaces (38 tool names, kind names/numbers) must NOT be in here
as breaks - they're frozen. Only arg-type changes (pre-1.0 allowed with entry).
EOF

cat > "$DESC_DIR/bodies/l-readme" <<'EOF'
## This task (README claims vs actual surface)
Verify "38 tools + 25 resources" count matches code.

## Method
1. Count #[tool( in tools.rs - confirm 38 (or ~39 per debate).
2. Count RESOURCES entries in resources.rs - confirm 25.
3. README + docs/decisions/README.md index counts match.

## Success criteria
Counts reconcile. Any drift fixed in README.

## Considerations
Low effort but catches doc rot. P6 polish.
EOF

############################################################ CREATE LEAVES
echo "creating leaves..."
# P0
mk p0-nip40-ingest-probe "P0: Probe NIP-40 expiration on unwrap ingest path" bug 0 "$DESC_DIR/bodies/l-nip40-probe"
mk p0-bdxxx-regression-tests "P0: RED regression tests for bd-XXX renewal semantics" task 0 "$DESC_DIR/bodies/l-bdxxx-regr"
mk p0-s8-resources-stub-gate "P0: Tighten S8 honest-stub gate + extend to 25 URIs" task 1 "$DESC_DIR/bodies/l-s8-stub"
mk p0-s3-shared-state-reword "P0: Reword S3 + RED mail-content sharing test" task 1 "$DESC_DIR/bodies/l-s3-reword"
mk p0-verify-green-claims "P0: Re-verify S1/S2/S4/S5/S6/S7/F2.KDF GREEN" task 1 "$DESC_DIR/bodies/l-verify-green"
mk p0-s9-flywheel-boundary-doc "P0: Document S9 flywheel boundary in known-divergences" docs 3 "$DESC_DIR/bodies/l-s9-doc"
# P1
mk p1-std-guard-across-blockon "P1: Audit std-Mutex-guard across block_on publish" bug 0 "$DESC_DIR/bodies/l-std-guard"
mk p1-broadcast-lag-handling "P1: RecvError::Lagged swallow (relay_bus.rs:196)" bug 1 "$DESC_DIR/bodies/l-lagged"
mk p1-poison-on-panic-audit "P1: Poison-on-panic survivability audit" task 1 "$DESC_DIR/bodies/l-poison"
mk p1-lock-ordering-deadlock "P1: Multi-lock deadlock ordering audit" task 1 "$DESC_DIR/bodies/l-lock-order"
mk p1-await-while-locked "P1: tokio guard held across .await audit" task 1 "$DESC_DIR/bodies/l-await-locked"
mk p1-backpressure-baseline "P1: Performance backpressure/latency baseline" task 2 "$DESC_DIR/bodies/l-backpressure"
# P2
mk p2-silent-result-drop "P2: Silent Result/Option drop (let _ = / .ok())" task 1 "$DESC_DIR/bodies/l-silent-drop"
mk p2-unwrap-or-zero-coercion "P2: unwrap_or(0) coercion - F3 residual kill" bug 1 "$DESC_DIR/bodies/l-coerce"
mk p2-count-conservation "P2: Count-conservation gate (classified=audited=baseline)" task 1 "$DESC_DIR/bodies/l-count"
mk p2-unwrap-expect-classify "P2: Classify 219 unwrap + 984 expect" task 2 "$DESC_DIR/bodies/l-classify"
mk p2-id-type-end-to-end "P2: Id-type discipline hex<->u64<->String trace" task 1 "$DESC_DIR/bodies/l-id-e2e"
mk p2-error-enum-discipline "P2: thiserror enum + message quality audit" task 3 "$DESC_DIR/bodies/l-error-enum"
# P3
mk p3-nip40-expiration-fix "P3: NIP-40 expiration TTL fix (gated by probe)" bug 0 "$DESC_DIR/bodies/l-nip40-fix"
mk p3-schnorr-verify-every-ingest "P3: Schnorr verify-before-project per ingest" task 1 "$DESC_DIR/bodies/l-schnorr"
mk p3-gift-wrap-metadata-leak "P3: Gift-wrap onetime key + seal resign + mismatch" task 1 "$DESC_DIR/bodies/l-giftwrap-meta"
mk p3-replay-dedup-per-sink "P3: Replay dedup per projection sink" task 1 "$DESC_DIR/bodies/l-replay"
mk p3-argon2-kdf-dos-bound "P3: Argon2 KDF params DoS-bound" task 2 "$DESC_DIR/bodies/l-kdf-dos"
mk p3-projection-atomicity "P3: Projection sink ordering + atomicity" task 2 "$DESC_DIR/bodies/l-projection-atom"
# P4
mk p4-deadcode-reclassify-fixtures "P4: Reclassify dead code as test fixtures (NEVER remove)" task 0 "$DESC_DIR/bodies/l-deadcode"
mk p4-engine-responsibility-map "P4: engine.rs 2401 responsibility map + split" task 2 "$DESC_DIR/bodies/l-engine-map"
mk p4-tools-domain-split "P4: tools.rs 1896 domain split proposal" task 2 "$DESC_DIR/bodies/l-tools-split"
mk p4-surface-messaging-dedup "P4: surface.rs/messaging.rs overlap audit" task 3 "$DESC_DIR/bodies/l-surface-messaging"
mk p4-bus-impl-dedup "P4: in_process/relay_bus dedup audit" task 3 "$DESC_DIR/bodies/l-bus-dedup"
# P5
mk p5-all-features-test-run "P5: cargo test --workspace --all-features (sc1-25)" task 0 "$DESC_DIR/bodies/l-allfeatures"
mk p5-d82-parity-am-parity-e2e "P5: D8.2 parity via am_parity_e2e + cli_robot" task 1 "$DESC_DIR/bodies/l-d82"
mk p5-crypto-property-tests "P5: Crypto property tests (seal/gift-wrap round-trips)" task 2 "$DESC_DIR/bodies/l-prop-crypto"
mk p5-projection-differential-tests "P5: Differential projection tests (bus/git/sqlite)" task 2 "$DESC_DIR/bodies/l-prop-projection"
mk p5-concurrency-stress-tests "P5: Concurrency stress (burst/lag/multi-process)" task 2 "$DESC_DIR/bodies/l-stress"
mk p5-sc-fw-integrated-resolution "P5: #[ignore] sc_fw_integrated resolution" task 3 "$DESC_DIR/bodies/l-scfw"
# P6
mk p6-verify-adr-links-script "P6: Author verify-adr-links.sh + wire it" task 1 "$DESC_DIR/bodies/l-verify-script"
mk p6-governed-by-enumeration "P6: Enumerate governed-by markers" task 2 "$DESC_DIR/bodies/l-governed"
mk p6-soundness-ledger-update "P6: Update ADR-0007 ledger per verdicts" docs 1 "$DESC_DIR/bodies/l-ledger-update"
mk p6-known-divergences-update "P6: Update known-divergences.md" docs 2 "$DESC_DIR/bodies/l-divergences"
mk p6-readme-counts-verify "P6: README counts vs actual surface" task 3 "$DESC_DIR/bodies/l-readme"

############################################################ WIRE HIERARCHY (parent) + DEPS
echo "wiring parents + deps..."
# parents: leaves -> track epics
for pair in \
  "p0-nip40-ingest-probe:p0-soundness-ledger" "p0-bdxxx-regression-tests:p0-soundness-ledger" \
  "p0-s8-resources-stub-gate:p0-soundness-ledger" "p0-s3-shared-state-reword:p0-soundness-ledger" \
  "p0-verify-green-claims:p0-soundness-ledger" "p0-s9-flywheel-boundary-doc:p0-soundness-ledger" \
  "p1-std-guard-across-blockon:p1-concurrency" "p1-broadcast-lag-handling:p1-concurrency" \
  "p1-poison-on-panic-audit:p1-concurrency" "p1-lock-ordering-deadlock:p1-concurrency" \
  "p1-await-while-locked:p1-concurrency" "p1-backpressure-baseline:p1-concurrency" \
  "p2-silent-result-drop:p2-error-id" "p2-unwrap-or-zero-coercion:p2-error-id" \
  "p2-count-conservation:p2-error-id" "p2-unwrap-expect-classify:p2-error-id" \
  "p2-id-type-end-to-end:p2-error-id" "p2-error-enum-discipline:p2-error-id" \
  "p3-nip40-expiration-fix:p3-crypto-giftwrap" "p3-schnorr-verify-every-ingest:p3-crypto-giftwrap" \
  "p3-gift-wrap-metadata-leak:p3-crypto-giftwrap" "p3-replay-dedup-per-sink:p3-crypto-giftwrap" \
  "p3-argon2-kdf-dos-bound:p3-crypto-giftwrap" "p3-projection-atomicity:p3-crypto-giftwrap" \
  "p4-deadcode-reclassify-fixtures:p4-refactor" "p4-engine-responsibility-map:p4-refactor" \
  "p4-tools-domain-split:p4-refactor" "p4-surface-messaging-dedup:p4-refactor" \
  "p4-bus-impl-dedup:p4-refactor" \
  "p5-all-features-test-run:p5-test-coverage" "p5-d82-parity-am-parity-e2e:p5-test-coverage" \
  "p5-crypto-property-tests:p5-test-coverage" "p5-projection-differential-tests:p5-test-coverage" \
  "p5-concurrency-stress-tests:p5-test-coverage" "p5-sc-fw-integrated-resolution:p5-test-coverage" \
  "p6-verify-adr-links-script:p6-doc-drift" "p6-governed-by-enumeration:p6-doc-drift" \
  "p6-soundness-ledger-update:p6-doc-drift" "p6-known-divergences-update:p6-doc-drift" \
  "p6-readme-counts-verify:p6-doc-drift" \
; do
  IFS=':' read -r child parent <<< "$pair"
  br dep add "${IDS[$child]}" "${IDS[$parent]}" --type blocks >/dev/null 2>&1 || echo "  dep-fail $child->$parent"
done

# meaningful cross-track deps (probe gates fix; refactor after correctness; batch after all + baseline)
echo "cross-track deps..."
# p3 NIP-40 fix DEPENDS ON p0 probe
br dep add "${IDS[p3-nip40-expiration-fix]}" "${IDS[p0-nip40-ingest-probe]}" --type blocks >/dev/null 2>&1
# P4 refactor epics DEPEND on P0-P3 correctness epics (refactor only on a correct base)
for p in p0-soundness-ledger p1-concurrency p2-error-id p3-crypto-giftwrap; do
  br dep add "${IDS[p4-refactor]}" "${IDS[$p]}" --type blocks >/dev/null 2>&1
done
# P6 doc-drift DEPENDS on P0-P5 (reflects final state)
for p in p0-soundness-ledger p1-concurrency p2-error-id p3-crypto-giftwrap p4-refactor p5-test-coverage; do
  br dep add "${IDS[p6-doc-drift]}" "${IDS[$p]}" --type blocks >/dev/null 2>&1
done
# batch-fix-pass DEPENDS on all track epics + baseline-rerun
for p in p0-soundness-ledger p1-concurrency p2-error-id p3-crypto-giftwrap p4-refactor p5-test-coverage p6-doc-drift; do
  br dep add "${IDS[batch-fix-pass]}" "${IDS[$p]}" --type blocks >/dev/null 2>&1
done
br dep add "${IDS[batch-fix-pass]}" "${IDS[baseline-rerun]}" --type blocks >/dev/null 2>&1
# baseline-rerun DEPENDS on findings (P0-P6) being done
for p in p0-soundness-ledger p1-concurrency p2-error-id p3-crypto-giftwrap p4-refactor p5-test-coverage p6-doc-drift; do
  br dep add "${IDS[baseline-rerun]}" "${IDS[$p]}" --type blocks >/dev/null 2>&1
done
# p2-count-conservation DEPENDS on the other P2 classification leaves
for p in p2-silent-result-drop p2-unwrap-or-zero-coercion p2-unwrap-expect-classify; do
  br dep add "${IDS[p2-count-conservation]}" "${IDS[$p]}" --type blocks >/dev/null 2>&1
done
# p1-backpressure-baseline + p5-concurrency-stress loosely relate but no hard dep

############################################################ VERIFY
echo ""
echo "=== SUMMARY ==="
echo "total beads created: ${#IDS[@]}"
br count --by status --json
echo "--- cycles check (MUST be empty) ---"
br dep cycles --json
echo "--- ready (should be the unblocked entry points) ---"
br ready --format toon 2>&1 | head -20

############################################################ SYNC
echo "--- sync ---"
br sync --flush-only 2>&1 | tail -3
echo "DONE. Next: git add .beads/ && git commit -m 'feat(beads): ADR-0008 review DAG (<n> beads)'"
