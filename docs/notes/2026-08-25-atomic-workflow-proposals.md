# Atomic-Inspired Built-In Workflow Proposals

**Date:** 2026-08-25
**Agent:** Buffy (Codebuff)
**Context:** Cross-referenced bastani-inc/atomic's workflow catalog against our 29 built-ins (`flywheel/src/builtin_workflows.rs`). Six genuine gaps found. Each proposal below follows the exact YAML shape and conventions of the existing catalog — `crew: auto`, `assignee: x`, `on_error: fail_run`, `graph:` where parallelism matters. Compose-in-flight notes show how each new workflow combines with existing ones.

---

## 1. `fan-out-and-synthesize` — General-Purpose Partition → Fan-Out → Synthesize

**Inspiration:** Atomic `fan-out-and-synthesize`
**Our closest:** `weekly-review` (cron-specific, repo-health scouting) + `brainstorm` (divergent ideation)
**Gap:** No **general-purpose** partition fan-out. The captain says "partition the rate-limiter by subsystem, fan out scouts, synthesize findings." Today that's `figure-it-out` (planner authors custom YAML). This is the reusable shape.

**Shape:** N scouts (default 3) each take one partition slice. An oracle synthesizes the N reports into a single cited evidence document. Read-only — no beads, no edits.

```yaml
name: fan-out-and-synthesize
description: "General-purpose partition fan-out (Atomic-inspired). Partition a scope into N independent slices, fan out scouts in parallel, an oracle synthesizes the N reports into a single cited evidence document. READONLY — no beads, no repo edits, no worktree. Pick N based on the partition count; 2 for small scopes, 5 for large."
trigger:
  type: webhook
crew: auto
inputs:
  - name: question
  - name: scope
  - name: partitions
    default: "3"
  - name: report_path
    default: ".flywheel/reports/fan-out-synthesize.md"
principles:
  - guard-the-context-window
  - prove-it-works
  - separate-before-serializing-shared-state
graph:
  nodes:
    scout-fanout:
      action: parallel
      over: "{{inputs.partitions}}"
      each:
        action: dispatch_job
        assignee: x
        spec:
          profile: "@scout"
          phase: resolve
          task: "READONLY — no beads, no edits, no worktree. You are ONE of {{inputs.partitions}} scouts. Partition the scope into {{inputs.partitions}} independent slices; take your assigned slice. Question: {{inputs.question}}. Scope: {{inputs.scope}}. Map your slice: what exists, what's relevant, what's missing. Return COMPRESSED findings JSON: [{area, finding, evidence, ev: VERIFIED|INFERRED|ASSUMED}]. No synthesis — that's the oracle's job."
    synthesize:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@oracle"
        phase: plan
        task: "READONLY — no beads, no edits. Synthesize the {{inputs.partitions}} scout reports into a single cited evidence document. Write the full report markdown to: {{inputs.report_path}}. Scout findings: {{nodes.scout-fanout}}. Question: {{inputs.question}}. Structure: (1) Executive summary — the answer in one paragraph, (2) Evidence by slice with citations to the scout who found it, (3) Gaps — what no scout covered, (4) Recommended action. Every claim cites a scout finding; no new facts not in the scout reports."
      needs: [scout-fanout]
      on_error: fail_run
```

**Compose-in-flight:**

```bash
# Partition a codebase audit across 5 subsystems, synthesize into a report
flywheel workflow run fan-out-and-synthesize \
  --inputs '{question:"Audit toron-core for ADR-0007 gap resolution", scope:"crates/toron-core/", partitions:"5"}'

# Then route the findings into captain-audit for adversarial challenge
flywheel workflow compose "challenge the fan-out report" \
  --parts "workflow:fan-out-and-synthesize, workflow:captain-audit"

# Or feed the report into plan for bead creation
flywheel workflow compose "bead the findings" \
  --parts "workflow:fan-out-and-synthesize, workflow:plan"
```

**Compare with existing:**
- `weekly-review`: same fan-out→synthesize shape, but hardcoded to "scout toron+flywheel repos for improvement ideas" and cron-triggered. This is general-purpose.
- `brainstorm`: fan-out→reduce for *ideation* (oracles diverge, planner reduces to ranked options). This is fan-out→synthesize for *evidence gathering* (scouts map, oracle synthesizes).

---

## 2. `adversarial-verify` — Adversarial Review WITH Bounded Repair

**Inspiration:** Atomic `adversarial-verification` (challenge → repair → re-verify, bounded)
**Our closest:** `interrogate` (N-model adversarial review, read-only, no repair) + `debate` (2-reviewer synthesis)
**Gap:** No review workflow that **includes repair**. `interrogate` produces Act-on/Consider/Noted/Dismissed — the captain manually routes Act-on into `bug-fix` or `refactoring`. This workflow closes the loop: review, fix the findings, re-review.

**Shape:** 2 reviewers on different model families. Parent synthesizes findings into a fix list. Coder applies the fixes. Reviewers re-verify. Bounded to 2 repair cycles. Gate at the end.

```yaml
name: adversarial-verify
description: "Adversarial review WITH bounded repair (Atomic-inspired). Two reviewers on DIFFERENT model families challenge the artifact. A coder applies the Act-on findings. Reviewers re-verify. Bounded to 2 repair cycles; on exhaustion, escalate to captain. Not a substitute for interrogate (read-only) — use this when you want the findings FIXED."
trigger:
  type: webhook
crew: auto
inputs:
  - name: target
  - name: artifact
  - name: verify_cmd
principles:
  - prove-it-works
  - fix-root-causes
  - guard-the-context-window
graph:
  nodes:
    challenge:
      action: parallel
      over: "2"
      each:
        action: dispatch_job
        assignee: x
        spec:
          profile: "@reviewer"
          harness: "opencode"
          phase: review
          task: "ADVERSARIAL REVIEW (you are ONE of 2 reviewers, DIFFERENT model families). READ the artifact FIRST — ground every finding in the real code. Target: {{inputs.target}}. Artifact: {{inputs.artifact}}. Per finding give: severity (BLOCKER|MAJOR|MINOR), citation (file:line or evidence), classification (Act-on|Consider|Dismissed), and the EXACT fix proposal. DO NOT apply fixes — the repairer does that. Return JSON: [{severity, citation, classification, fix_proposal}]."
    synthesize:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@oracle"
        phase: plan
        task: "Synthesize the 2 reviewer findings for: {{inputs.target}}. De-duplicate overlapping findings. Merge the Act-on items into a single FIX LIST — ordered by severity (BLOCKER first), one fix per line with the exact fix proposal from the reviewer. Consider and Dismissed items go into an appendix (no action). Reviewer findings: {{nodes.challenge}}. Return JSON: {fix_list: [{finding, fix_proposal, severity}], appendix: [{finding, classification, reason}]}."
      needs: [challenge]
      on_error: fail_run
    repair:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@coder"
        phase: implement
        task: "Apply the fix list for: {{inputs.target}}. Fix list: {{nodes.synthesize.fix_list}}. Apply EVERY fix in order. Do NOT change anything not in the fix list. Run: {{inputs.verify_cmd}} after applying all fixes. If verify passes, commit with message 'fix(review): address adversarial review findings'. If verify fails, report which fix broke it."
      needs: [synthesize]
      on_error:
        repair:
          max_cycles: 2
          repair_profile: "@coder"
          repair_model: "claude-opus-4"
          verify: "{{inputs.verify_cmd}}"
    re-verify:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@reviewer"
        harness: "opencode"
        phase: verify
        task: "Re-verify the repaired artifact for: {{inputs.target}}. Did the coder address all Act-on findings? Did the verify command pass? Are there NEW issues introduced by the fix? Run: {{inputs.verify_cmd}}. Return JSON: {all_acton_addressed: bool, verify_passed: bool, new_issues: [{finding}]}."
      needs: [repair]
      on_error:
        repair:
          max_cycles: 2
          verify: "{{inputs.verify_cmd}}"
    gate:
      action: request_approval
      message: "Adversarial verify complete for {{inputs.target}}. Fix list applied. Re-verified. Approve?"
      needs: [re-verify]
```

**Compose-in-flight:**

```bash
# After a feature build, run adversarial verify on the implementation
flywheel workflow compose "adversarial verify the feature build" \
  --parts "workflow:feature, workflow:adversarial-verify"

# After interrogate produces Act-on items, feed into adversarial-verify
# (interrogate finds, adversarial-verify fixes)
flywheel workflow compose "review then fix" \
  --parts "workflow:interrogate, workflow:adversarial-verify"

# On a critical P0 bead, replace the single-reviewer prove step with adversarial-verify
flywheel workflow compose "ship with adversarial review" \
  --parts "workflow:ship, each:phase-review:workflow:adversarial-verify"
```

**Compare with existing:**
- `interrogate`: N reviewers, same rubric, read-only, produces Act-on/Consider/Noted/Dismissed. This ADDS the repair loop.
- `bug-fix`: repro→tdd→fix→prove. This is *review-driven* repair (reviewers find, coder fixes, reviewers re-verify).

---

## 3. `loop-until` — Bounded Predicate Meta-Workflow

**Inspiration:** Atomic `loop-until-done` (iterate with durable ledger until completion evidence or bound exhaustion)
**Our closest:** `hillclimb` (measurement loop: try→measure→keep-or-discard) + `autonomous-work` (runs the real `loop run --work`)
**Gap:** No general **"run sub-workflow X until predicate Y is true, bounded by N iterations"** meta. `hillclimb` is measurement-specific. `autonomous-work` runs the real bead loop, not an arbitrary sub-workflow.

**Shape:** Accept a sub-workflow name and a predicate (a shell command that exits 0 when done). Run the sub-workflow, evaluate the predicate, repeat up to `max_iters`. On each iteration, a ledger row records the attempt. On exhaustion, escalate.

```yaml
name: loop-until
description: "Bounded predicate meta-workflow (Atomic-inspired). Run a sub-workflow repeatedly until a predicate passes, bounded by max_iters. Each iteration is a tracked ledger row. On exhaustion, escalate to captain. Use for flaky-until-green, converge-until-stable, or any goal that needs iteration. NOT a replacement for hillclimb (measurement loop) or autonomous-work (runs the real bead loop)."
trigger:
  type: webhook
crew: auto
inputs:
  - name: objective
  - name: sub_workflow
    default: "bug-fix"
  - name: predicate
  - name: max_iters
    default: "5"
  - name: sub_inputs
    default: "{}"
principles:
  - prove-it-works
  - sequence-verifiable-units
  - never-block-on-the-human
steps:
  - name: iterate
    action: retry
    max_attempts: 10
    step:
      name: iter
      action: dispatch_job
      assignee: x
      spec:
        profile: "@housekeeper"
        harness: "opencode"
        task: "LOOP-UNTIL ITERATION (budget {{inputs.max_iters}} total; objective: {{inputs.objective}}). (1) Run the sub-workflow: `flywheel workflow run {{inputs.sub_workflow}} --inputs '{{inputs.sub_inputs}}'`. (2) If the sub-workflow FAILED, return an ERROR with the failure reason — the loop will retry. (3) If the sub-workflow SUCCEEDED, evaluate the predicate: `{{inputs.predicate}}`. (4) If the predicate passes (exit 0), return ok: the loop stops. (5) If the predicate fails (exit ≠ 0), record the attempt in the ledger (`br gate report` with the iteration number and predicate output), then return an ERROR so the loop retries with the next iteration. NEVER loop past {{inputs.max_iters}} iterations — the retry cap enforces the bound. Report: iteration number, sub-workflow outcome, predicate output."
```

**Compose-in-flight:**

```bash
# Run bug-fix up to 5 times until cargo test passes
flywheel workflow run loop-until \
  --inputs '{
    objective:"Fix flaky reservation test",
    sub_workflow:"bug-fix",
    predicate:"cargo test -p toron-core reservation_conflict_detection -- --test-threads=1",
    max_iters:"5",
    sub_inputs:"{\"symptom\":\"reservation test flakes under load\",\"repro\":\"cargo test -p toron-core -- --test-threads=4\"}"
  }'

# Run refactoring until the invariant stays green 3 times in a row
flywheel workflow run loop-until \
  --inputs '{
    objective:"Stable refactoring",
    sub_workflow:"refactoring",
    predicate:"cargo test --workspace && cargo clippy --workspace -- -D warnings",
    max_iters:"3",
    sub_inputs:"{\"target\":\"crates/toron-core/src/mailbox/\",\"invariant_cmd\":\"cargo test -p toron-core\"}"
  }'

# Compose: keep running goal until ship-beads predicate passes
flywheel workflow compose "overnight until ship" \
  --parts "each:workflow:goal retry(5):workflow:ship-beads"
```

**Compare with existing:**
- `hillclimb`: iteration with measurement, keep-or-discard per attempt. This is general — any sub-workflow, any predicate.
- `autonomous-work`: runs `flywheel loop run --work`, the real bead dispatch loop. This runs an arbitrary sub-workflow.
- `Action::Retry`: retries a single step on failure. This retries an entire *workflow* and checks a predicate, not just "did it succeed."

---

## 4. `research-then-act` — Research → Decide → Act

**Inspiration:** Atomic `ralph` (research-first delegated implementation with bounded multi-model review)
**Our closest:** `forensics` (diagnosis-only, no fix) + `feature` (type-shape-first, not research-first)
**Gap:** No workflow that **researches the codebase first**, then decides what action to take based on findings, then acts. `forensics` stops at diagnosis. `feature` starts from a declared data shape. This fills the space between "just diagnose" and "just build from a spec."

**Shape:** Scout researches the codebase area (what exists, what's coupled, what patterns apply). Planner decides: is this a bug-fix, a feature, a refactoring, or "no action needed"? Coder executes the chosen action. Reviewer proves the outcome.

```yaml
name: research-then-act
description: "Research-first pipeline (Atomic ralph-inspired). A scout researches the codebase area FIRST (what exists, coupling, patterns, blast radius). A planner decides the action type based on findings (bug-fix / feature / refactoring / no-action). A coder executes. A reviewer proves. Use when the captain has a symptom or goal but doesn't know the shape of the solution — let the research decide."
trigger:
  type: webhook
crew: auto
inputs:
  - name: goal
  - name: area
principles:
  - foundational-thinking
  - fix-root-causes
  - prove-it-works
graph:
  nodes:
    research:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@scout"
        phase: resolve
        task: "RESEARCH the codebase area for: {{inputs.goal}}. Area: {{inputs.area}}. Map: (1) what code exists in this area, (2) what it couples to, (3) existing patterns and conventions, (4) blast radius of any change here, (5) tests that cover this area. Return COMPRESSED findings JSON: {area_map: [{path, purpose, coupled_to}], patterns: [{name, where_used}], blast_radius: [paths], tests: [{path, what_it_covers}]}. NO action recommendation — the planner decides."
      on_error: fail_run
    decide:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@planner"
        phase: plan
        task: "DECIDE the action type for: {{inputs.goal}}. Based on the scout's research: {{nodes.research}}. Classify into exactly ONE: {action: bug-fix|feature|refactoring|no-action, rationale: '<one sentence>', spec: '<the concrete task for the coder>', verify: '<the command that proves done>'}. If no-action: the spec is 'no work needed, research is the deliverable'. If the research reveals the goal is impossible or out-of-scope: action=no-action, rationale=why. Do not invent work when the research says none is needed."
      needs: [research]
      on_error: fail_run
    act:
      action: branch
      if: "str_contains({{nodes.decide.action}}, \"no-action\") == false"
      needs: [decide]
      then:
        - name: build
          action: dispatch_job
          assignee: x
          spec:
            profile: "@coder"
            phase: implement
            task: "Execute the decided action for: {{inputs.goal}}. Action: {{nodes.decide.action}}. Spec: {{nodes.decide.spec}}. Verify: {{nodes.decide.verify}}. Research (for context): {{nodes.research}}. Commit with a message citing the research findings."
          on_error:
            repair:
              max_cycles: 3
              verify: "{{nodes.decide.verify}}"
        - name: prove
          action: dispatch_job
          assignee: x
          spec:
            profile: "@reviewer"
            harness: "opencode"
            phase: review
            task: "PROVE the action for: {{inputs.goal}}. Action taken: {{nodes.decide.action}}. Verify command: {{nodes.decide.verify}}. Research: {{nodes.research}}. Was the right action chosen? Did the implementation match the spec? Does verify pass? Return verdict JSON: {action_correct: bool, verify_passed: bool, findings: [{finding}]}."
          needs: [build]
          on_error:
            repair:
              max_cycles: 2
              verify: "{{nodes.decide.verify}}"
      else:
        - name: report
          action: dispatch_job
          assignee: x
          spec:
            profile: "@planner"
            task: "No action needed for: {{inputs.goal}}. Research: {{nodes.research}}. Decision: {{nodes.decide}}. Write a no-action report to .flywheel/reports/no-action-{{inputs.goal}}.md summarizing the research findings and why no action was decided. This is a valid, honest outcome — not a failure."
    gate:
      action: request_approval
      message: "Research-then-act complete for {{inputs.goal}}. Action: {{nodes.decide.action}}. Approve?"
      needs: [act]
```

**Compose-in-flight:**

```bash
# Captain has a symptom but doesn't know if it's a bug or a missing feature
flywheel workflow run research-then-act \
  --inputs '{goal:"rate limiter doesn't work under burst traffic", area:"crates/toron-core/src/rate_limit/"}'

# Research-then-act feeds into bug-fix if the planner decides it's a bug
flywheel workflow compose "investigate then fix" \
  --parts "workflow:research-then-act, when:{{nodes.decide.action}}==bug-fix:workflow:bug-fix"

# Research-first version of goal
flywheel workflow compose "research-first goal" \
  --parts "workflow:research-then-act, workflow:goal"
```

**Compare with existing:**
- `forensics`: diagnosis only. `research-then-act` researches then acts.
- `feature`: starts from a declared `data_shape`. This starts from an `area` and lets the research determine the shape.
- `figure-it-out`: planner authors custom YAML. This is the reusable "research first" shape.

---

## 5. `incident-response` — Diagnose → Contain → Fix → Verify → Postmortem

**Inspiration:** Atomic's bounded repair loop applied at incident scale. Not in Atomic's catalog directly but follows from their repair loop + evidence primitives.
**Our closest:** `bug-fix` (repro→tdd→fix→prove) + `forensics` (diagnosis-only)
**Gap:** No incident-scale workflow with containment before fix, postmortem after verify. `bug-fix` assumes you can just fix it. Incidents need containment first (stop the bleeding), then diagnosis, then fix, then a postmortem that becomes structure.

**Shape:** Scout diagnoses. Housekeeper contains (rollback, feature-flag-off, scale-down). Coder fixes. Reviewer verifies. Oracle writes the postmortem. The postmortem is a bead — every incident becomes structure.

```yaml
name: incident-response
description: "Incident response at scale. Diagnose the root cause -> contain the blast radius -> fix the root cause -> verify the fix -> write a postmortem bead. Containment happens BEFORE the fix (stop the bleeding first). The postmortem is a bead (every incident becomes structure). Different from bug-fix: bug-fix assumes you can just fix it; incidents need containment and a postmortem."
trigger:
  type: webhook
crew: auto
inputs:
  - name: symptom
  - name: blast_radius
  - name: verify_cmd
principles:
  - fix-root-causes
  - prove-it-works
  - encode-lessons-in-structure
  - never-block-on-the-human
graph:
  nodes:
    diagnose:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@scout"
        phase: resolve
        task: "INCIDENT DIAGNOSIS. Symptom: {{inputs.symptom}}. Blast radius: {{inputs.blast_radius}}. (1) What is the ROOT CAUSE? Not the symptom — the underlying bug or state that triggered it. (2) What is the CURRENT IMPACT? Who is affected, what is degraded? (3) What is the CONTAINMENT? The smallest action that stops the bleeding — rollback, feature-flag-off, scale-down, circuit-breaker. Return JSON: {root_cause: '<one sentence>', impact: '<who/what>', containment: {action: '<exact command or step>', reversible: bool}}."
      on_error: fail_run
    contain:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@housekeeper"
        phase: implement
        task: "INCIDENT CONTAINMENT. Symptom: {{inputs.symptom}}. Diagnosis: {{nodes.diagnose}}. Apply the containment action: {{nodes.diagnose.containment.action}}. STOP THE BLEEDING FIRST. Do not fix the root cause yet — that comes after containment. Verify the containment worked: the symptom should stop or degrade. Run: {{inputs.verify_cmd}} to check that the system is stable (even if degraded). Return JSON: {contained: bool, action_taken: '<what you did>', current_state: '<stable|degraded|still_broken>'}."
      needs: [diagnose]
      on_error: fail_run
    fix:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@coder"
        phase: implement
        task: "INCIDENT FIX. Symptom: {{inputs.symptom}}. Root cause: {{nodes.diagnose.root_cause}}. The system is contained ({{nodes.contain.current_state}}). Now fix the ROOT CAUSE — not the symptom, not a workaround. The containment is temporary; the fix is permanent. Run: {{inputs.verify_cmd}} after the fix. Commit with the incident symptom in the message."
      needs: [contain]
      on_error:
        repair:
          max_cycles: 3
          verify: "{{inputs.verify_cmd}}"
    verify:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@reviewer"
        harness: "opencode"
        phase: verify
        task: "INCIDENT VERIFICATION. Symptom: {{inputs.symptom}}. Fix applied: {{nodes.fix}}. (1) Run: {{inputs.verify_cmd}}. Does it pass? (2) Is the root cause ({{nodes.diagnose.root_cause}}) actually fixed, or just the symptom? (3) Could this incident recur? What would prevent it? Return JSON: {verify_passed: bool, root_cause_fixed: bool, recurrence_risk: '<none|low|medium|high>', prevention: '<one sentence>'}."
      needs: [fix]
      on_error:
        repair:
          max_cycles: 2
          verify: "{{inputs.verify_cmd}}"
    postmortem:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@oracle"
        phase: plan
        task: "INCIDENT POSTMORTEM. Symptom: {{inputs.symptom}}. Full timeline: diagnose={{nodes.diagnose}} -> contain={{nodes.contain}} -> fix={{nodes.fix}} -> verify={{nodes.verify}}. Write a postmortem bead (br create --auto-workflow with type=bug, priority=0 or 1). The bead MUST answer: (1) What happened? (2) Why did it happen? (3) How was it fixed? (4) What STRUCTURAL change prevents recurrence? — a lint, a preflight, a guard, a test. The postmortem bead IS the structural change (encode-lessons-in-structure)."
      needs: [verify]
      on_error: fail_run
```

**Compose-in-flight:**

```bash
# Incident response on a production regression
flywheel workflow run incident-response \
  --inputs '{
    symptom:"reservation conflicts reported as successes under load",
    blast_radius:"crates/toron-core/src/reservations.rs, all projects using toron",
    verify_cmd:"cargo test -p toron-core reservations -- --test-threads=4"
  }'

# Weekly review finds an incident → incident response handles it
flywheel workflow compose "review-then-respond" \
  --parts "workflow:weekly-review, when:{{nodes.oracle.idea.severity}}==incident:workflow:incident-response"
```

---

## 6. `dependency-upgrade` — Changelog → Upgrade → Verify → Rollback-If-Broken

**Inspiration:** Generalization of Atomic's bounded repair loop + our `perf` keep-or-discard pattern. Not in Atomic's catalog, but a common engineering task.
**Our closest:** `housekeeper` profile's default workflow `docker-auto-upgrade` (not a built-in workflow, it's a profile default) + `perf` (keep-or-discard branch)
**Gap:** No structured dependency upgrade workflow with changelog review, verify gate, and automatic rollback.

**Shape:** Scout reviews the changelog and assesses risk. Housekeeper applies the upgrade. Scout verifies (tests pass, no regressions). Branch: if verify passes, keep (commit). If verify fails, rollback (revert + report). Postmortem bead if the upgrade is rejected.

```yaml
name: dependency-upgrade
description: "Structured dependency upgrade. Scout reviews the changelog and assesses breaking changes + risk. Housekeeper applies the upgrade. Scout verifies (tests pass, no regression). If verify passes: keep (commit). If verify fails: rollback (revert exactly, no leftover) and create a postmortem bead. The verify_cmd is the gate; a failed upgrade is an incident that becomes a bead."
trigger:
  type: webhook
crew: auto
inputs:
  - name: dependency
  - name: from_version
  - name: to_version
  - name: verify_cmd
    default: "cargo test --workspace"
  - name: changelog_url
    default: ""
principles:
  - prove-it-works
  - make-operations-idempotent
  - laziness-protocol
graph:
  nodes:
    assess:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@scout"
        phase: resolve
        task: "DEPENDENCY ASSESSMENT. Dependency: {{inputs.dependency}} ({{inputs.from_version}} → {{inputs.to_version}}). Changelog: {{inputs.changelog_url}}. Review the changelog and assess: (1) Breaking changes that affect us. (2) New features or fixes we benefit from. (3) Risk level (low|medium|high) based on our usage. (4) Migration steps if any. (5) Rollback plan: the exact command to revert. Return JSON: {breaking_changes: [{change, affects_us: bool}], benefits: [{change, why_useful}], risk: 'low|medium|high', migration_steps: ['...'], rollback_cmd: '<exact revert command>'}."
      on_error: fail_run
    upgrade:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@housekeeper"
        phase: implement
        task: "DEPENDENCY UPGRADE. Dependency: {{inputs.dependency}} ({{inputs.from_version}} → {{inputs.to_version}}). Assessment: {{nodes.assess}}. Apply the upgrade: update Cargo.toml/package.json/requirements.txt and run the package manager update. Apply migration steps if any: {{nodes.assess.migration_steps}}. After upgrading, DO NOT verify yet — just report the upgrade was applied. Return JSON: {upgraded: bool, new_version: '<actual>', migration_applied: ['...']}."
      needs: [assess]
      on_error: fail_run
    verify:
      action: dispatch_job
      assignee: x
      spec:
        profile: "@scout"
        phase: verify
        task: "DEPENDENCY VERIFICATION. Dependency: {{inputs.dependency}} (now at {{nodes.upgrade.new_version}}). Run: {{inputs.verify_cmd}}. Check for: (1) Test failures (new or existing). (2) Compilation errors. (3) Deprecation warnings that will become errors. Return JSON: {verify_passed: bool, failures: ['...'], warnings: ['...']}."
      needs: [upgrade]
      on_error:
        repair:
          max_cycles: 2
          verify: "{{inputs.verify_cmd}}"
    keep-or-rollback:
      action: branch
      if: "{{nodes.verify.verify_passed}} == true"
      needs: [verify]
      then:
        - name: keep
          action: dispatch_job
          assignee: x
          spec:
            profile: "@housekeeper"
            phase: verify
            task: "KEEP the upgrade of {{inputs.dependency}} to {{nodes.upgrade.new_version}}. Verify passed: {{nodes.verify}}. Commit the upgrade with a message citing the changelog and the assessment. The upgrade is permanent."
          on_error: fail_run
      else:
        - name: rollback
          action: dispatch_job
          assignee: x
          spec:
            profile: "@housekeeper"
            phase: verify
            task: "ROLLBACK the upgrade of {{inputs.dependency}} ({{nodes.upgrade.new_version}}). Verify FAILED: {{nodes.verify}}. Rollback command: {{nodes.assess.rollback_cmd}}. Execute it exactly. Verify the rollback by running: {{inputs.verify_cmd}} — it should PASS after rollback. Then create a postmortem bead (br create --auto-workflow, type=bug, priority=1): what upgrade failed, why, what needs to happen before the next attempt."
          on_error: fail_run
```

**Compose-in-flight:**

```bash
# Weekly cron: check for dependency upgrades
flywheel workflow run dependency-upgrade \
  --inputs '{
    dependency:"nostr-sdk",
    from_version:"0.45.2",
    to_version:"0.46.0",
    changelog_url:"https://github.com/rust-nostr/nostr/releases/tag/v0.46.0"
  }'

# Chain: dependency-upgrade → if kept, run ship to verify the whole chain
flywheel workflow compose "upgrade then ship" \
  --parts "workflow:dependency-upgrade, when:{{nodes.keep-or-rollback.branch}}==then:workflow:ship"
```

---

## Summary: Six Built-Ins, How They Compose

| New built-in | Composes with | Captain says |
| :--- | :--- | :--- |
| `fan-out-and-synthesize` | `captain-audit`, `plan`, `weekly-review`, `brainstorm` | "Partition the rate-limiter by subsystem, fan out 5 scouts, synthesize into a report, then plan the beads." |
| `adversarial-verify` | `feature`, `bug-fix`, `goal`, `ship`, `interrogate` | "Build the feature, then run 2 reviewers on different models — fix whatever they find, re-verify, and gate." |
| `loop-until` | `bug-fix`, `refactoring`, `perf`, `dependency-upgrade` | "Keep running bug-fix until cargo test passes, but stop after 5 attempts." |
| `research-then-act` | `bug-fix`, `feature`, `refactoring`, `goal` | "Scout the reservation module, then decide if it's a bug fix or a feature — and do it." |
| `incident-response` | `forensics`, `bug-fix`, `weekly-review` | "The reservation system is broken under load. Contain it first, then fix it, then write the postmortem bead." |
| `dependency-upgrade` | `ship`, `loop-until`, `weekly-review` | "Upgrade nostr-sdk to 0.46, run the workspace tests, rollback if anything breaks." |

Each follows the exact conventions of `builtin_workflows.rs`: `crew: auto`, `assignee: x`, `on_error: fail_run`, `graph:` for parallelism, `inputs:` with defaults, `principles:` citing the 21-name registry. No new stages, no new profiles, no new kinds — all use the existing profile catalog (`@scout`, `@planner`, `@coder`, `@reviewer`, `@oracle`, `@housekeeper`) and existing stages.