# ADR-0008 Debate Prompt — Opencode Adversarial Crew

You are the **opencode adversarial debate crew** chartered by ADR-0008
(`docs/decisions/0008-comprehensive-codebase-review-charter.md`). The captain
(bangedorrunt) has PROPOSED this ADR and requires your code-grounded counter
BEFORE execution begins. This is not a rubber stamp — an empty or tautological
counter = REJECT.

## Your two roles

### Adversary 1 — Round-1 code-grounded counter (assignee: oc)

For EACH P0 Soundness Ledger claim (S1–S9, F2.KDF, bd-XXX ×3):
1. Read the evidence path cited in the ADR. Confirm the code exists where
   claimed (cite file:line).
2. Confirm the named gate test exists and is a REAL test that can FAIL (not a
   tautology). Run it if fast (`cargo test -p <crate> <test_name>`); report
   pass/fail with the actual output line.
3. If the claim's evidence is missing, the gate is fake, or the test fails →
   mark the claim RED and propose the minimum fix.

For EACH P1–P6 track:
1. Challenge whether the named skill mapping + method will ACTUALLY surface the
   named risk class. Cite a concrete example file:line where the risk could hide
   if the method is wrong.
2. If the method is inadequate, propose an amendment.

**Output shape:** AGREE-WITH-CHANGES or REJECT, then a Change Agenda
(low→high leverage). Each Change Agenda item:
- `change:` one-line description
- `posture:` narrow / narrow-structural / structural-small / structural
- `evidence_of_defect:` file:line + why current charter is wrong
- `recommended_change:` concrete fix to the CHARTER (not the code — code fixes
  happen in the batch pass AFTER the charter is accepted)
- `confidence:` Low / Medium / High

### Adversary 2 — Charter-coherence counter (assignee: oc2)

Audit the charter ITSELF, not the code:
1. Does it omit a load-bearing track? (Name it + why it's load-bearing.)
2. Are the captain's 6 decisions faithfully encoded? (All 6 — list each + ✓/✗.)
3. Is the Measured Baseline honest? Re-run 3 of the baseline greps yourself and
   confirm the numbers reproduce.
4. Are the gates falsifiable? (A gate that cannot fail is theater — flag any.)
5. Is the debate protocol (§Decision Outcome) sound? Can it deadlock? Can the
   crew rubber-stamp undetected?
6. Are the Risks/Counter-Arguments (R1–R7) complete? What did the author miss?

**Output shape:** same AGREE-WITH-CHANGES / REJECT + Change Agenda.

## Hard rules

- **Code-grounded or it doesn't count.** Every claim cites file:line. "Looks
  fine" without evidence = REJECT.
- **No code edits.** You are debating the CHARTER. The codebase is read-only
  for this pass. Edits happen in the batch fix after acceptance.
- **Toron only.** The flywheel repo is OUT OF SCOPE (captain decision 3). If
  you find a flywheel-side gap, document it for a follow-on charter — do not
  propose editing flywheel.
- **No "huge work" / "complexity" framing.** The captain rejects that framing.
  Be precise about leverage, not size.
- **Honest negatives are wins.** A claim you prove RED is more valuable than a
  rubber-stamp GREEN.

## Deliverable

Write your counter to `docs/review/0008-debate-round1.md` (create the dir).
Structure:
```
# ADR-0008 Round-1 Counter — <your role>

## Verdict: AGREE-WITH-CHANGES | REJECT

## Baseline reproduction (Adversary 2 only)
<3 grep re-runs + match/drift>

## P0 claim verdicts (Adversary 1 only)
| Claim | Evidence verified | Gate real | Gate result | Verdict |

## Change Agenda (low → high leverage)
1. change: ...
2. change: ...

## Charter-coherence findings (Adversary 2 only)
<omitted tracks / decision fidelity / falsifiability / protocol soundness / missing risks>
```

When both counters are written, send a single consolidated message to the
captain via `flywheel mail` (project `toron`, agent name = your toron mail
identity, to captain) summarizing the merged Change Agenda. The captain will
resolve each item ACCEPT / ACCEPT-MODIFIED / REJECT and bump the ADR to Rev 1.

## Context pointers

- Charter: `docs/decisions/0008-comprehensive-codebase-review-charter.md`
- Precedent (the pattern to match): `docs/decisions/0007-post-implementation-hardening-and-api-redesign.md` §Counter-Argument Round 1
- Soundness Ledger source: ADR-0007 §Soundness Ledger
- Baseline greps: ADR-0008 §Measured Baseline
- Captain's 6 decisions: ADR-0008 §"captain's review requirements"

## Ops notes (bd-process-followups-4lqt)

1. **Model latency budget (DeepSeek V4 Flash):** a full charter read + debate
   of the ~45k LOC tree took ~12 minutes. For large-context debate rounds,
   budget ≥15 min per round or dispatch to a faster model; do not treat a
   slow first response as a stall.
2. **Adversary output files:** NEVER instruct "Write to file X" and "Append to
   file X" for two adversaries — the later "Write" clobbers the earlier
   "Append" (209→98 lines in round 1). Always use distinct output files per
   adversary (`0008-debate-round1-oc.md` / `-oc2.md`), merged by the captain
   (see `0008-captain-resolution.md` §Coordination note).

Begin. Read the charter fully before any grep.
