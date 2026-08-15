<!-- governed-by: ADR-0003, ADR-0005, ADR-0010, ADR-0011, ADR-0015 -->
---
status: proposed
date: 2026-08-15
decision-makers: Captain (bangedorrunt)
consulted: grilling session 2026-08-15 (wave-1 locks); pstack / poteto-mode (cursor/plugins) as prior art
informed: flywheel maintainers; toron-workflow / toron-core (no surface change)
---

# ADR-0019: Adopt Bead-Verified Execution, Bake Engineering Principles into Structure, and Ship Playbook Workflows

**Status:** Proposed (2026-08-15)
**Date:** 2026-08-15
**Decision-makers:** Captain (bangedorrunt) — grilling session locked wave 1; this record is the full program
**Extends:** [ADR-0005](0005-orchestration-layer.md) (profiles, stages, built-in workflows), [ADR-0010](0010-execution-wiring-model-registry-headless.md) (real runner, role conditioning, headless), [ADR-0011](0011-coalition-orchestration.md) (intents + reservations)
**Amends:** [ADR-0015](0015-flywheel-toron-integration-contract.md) (adds one flywheel-side guarantee: a close is not done without a ledger verdict). Does **not** amend the frozen 38-tool MCP surface or the kind registry.
**Companion (flywheel):** flywheel ADR-0001 (event-driven loop leftover = this ADR's wave 4)
**Tracker:** beads (`br` / `bv`). Never pull requests, never Graphite.

> **Scope:** make overnight swarm work *trustable* and make poteto's engineering principles *unskippable* for every future captain and crew. The unit of work is a **bead**. Proof is a **ledger row**. Isolation is a **reservation**. Contention **serializes or splits**, it does not spawn a worktree. Worktrees are **forbidden** in the loop and in playbook dispatch (token hunger + shared-state lie). Playbooks become **built-in flywheel workflows**. Principles become **types, lints, preflight, briefs, and profile rules**, not a markdown sermon.
>
> This ADR is the whole program. Wave 1 is the first cut. Waves 2–6 are specified here so later agents do not invent a smaller vision.

---

## 1. TL;DR

pstack (poteto-mode) is a Cursor plugin whose real invention is not 22 playbooks. It is:

1. a checkable work unit
2. an isolated write target
3. an independent verdict
4. principles that *steer by name* and are *encoded as structure*

We already have the orchestrator (flywheel), the transport (toron), and the tracker (beads). What we do not have is a **verdict**. `br close` is a self-report. The loop treats that as done. The lifecycle protocol is a 2KB prompt string (`flywheel/src/durable.rs` `bead_prompt`). Agents forget it. The loop cannot parse a receipt out of it.

**Decision:** transplant pstack's *invariants* onto beads. Do not transplant its PR/Graphite stack. Do not fork pstack into this repo. Do not add a parallel skill router that re-derives `flywheel decide`.

| pstack object | Our object |
|---|---|
| PR + head SHA | bead id + commit SHA |
| Graphite stack | `br dep` + `bv --robot-plan` tracks |
| babysit PR / land stack | claim-watch + T0–T3 + ledger-gated close |
| `units.tsv` / `ledger.tsv` | `.flywheel/ledger.tsv` |
| poteto-mode router | `flywheel decide` (extended, still one table) |
| standing orders | generated file from the principle registry |
| `/loop until predicate` | `flywheel loop run --work` until ready empty **and** claim-watch empty **and** ledger green |
| playbooks | built-in flywheel workflows + stages |

**Wave 1 (grilled, then amended 2026-08-15):** ledger + scaled verifier + short brief + reservation floor. Contention **serializes or path-splits**. Worktrees forbidden in the loop. Delete `bead_prompt` / `resume_prompt` in the same cut.

**Wave 2:** bake all 21 principles into registry, standing orders, profiles, stages, lint, preflight, and the brief `PRINCIPLES` field.

**Wave 3:** ship the playbook workflows as compiled-in builtins and extend `decide`.

**Wave 4:** event-driven loop (flywheel ADR-0001 A2) and delete the dual loop body.

**Wave 5:** only if waves 1–3 still leave noun sprawl, collapse steer/loop/workflow into one BeadRuntime.

**Wave 6:** `reflect` workflow. A correction seen twice becomes a lint, a preflight, or a principle amendment bead.

---

## 2. Context and Problem Statement

### 2.1 What already exists (do not rebuild)

Named so a future agent does not "add pstack" on top of a working spine.

| Piece | Where | Job |
|---|---|---|
| Captain classifier | `flywheel/src/decide.rs` | first-match table → one argv |
| Durable loop | `flywheel/src/durable.rs` `swarm_loop` | poll, dispatch, claim-watch, T0–T3, CONTEXT_LIMIT respawn, mail wake |
| Sync loop (duplicate) | `flywheel/src/main.rs` `cmd_loop` | same job, second body |
| Steer | `cmd_steer` → builtin `goal` / `goal-direct` | plan-gate vs direct |
| Builtins today | `flywheel/src/builtin_workflows.rs` | brainstorm, plan, plan-direct, beads, debate, goal, goal-direct, triage, plan-review, onboard, triage-deep, captain-audit, weekly-review |
| Stages today | `flywheel/src/stages.rs` | p1–p11 semantic aliases, including `systematic-execute` which restates the lifecycle in prose |
| Profiles today | `flywheel/src/profile.rs` | oracle / planner / reviewer / coder / scout / housekeeper, `behavior` prepended (ADR-0010 Spec C) |
| Coalitions | ADR-0011 | intent=SOFT, held=HARD, never remove the conflict check |
| Mail | ADR-0015 `MailSurface` | threads, ack, reservations, slots. `unread_only` is a documented NO-OP |
| Jobs | kinds 43001–43006 | first-claimer, bounded dispatch |
| Identity | ADR-0018 | `flywheel-<slug>-<role><seq>`, swarm vs open |
| One-way dep | ADR-0003 D3A / ADR-0015 | flywheel → toron-workflow → toron-core |

The spine is real. The *proof* and the *principle enforcement* are not.

### 2.2 What pstack is, stripped of Cursor/PR clothing

Source studied: `https://github.com/cursor/plugins/tree/main/pstack`, especially `skills/poteto-mode/SKILL.md` and the 21 `principle-*` leaves.

**Playbooks** are routing. They pick a sequence. We already route with `decide` + workflows + stages.

**Principles** are the load-bearing part. Each one names a *when* and a *rule*. poteto-mode requires the agent to cite the principle *and the decision it changed*. A citation with no decision is a skip.

**Orchestrate / autonomous-run / babysit / shipping** assume PRs, Graphite, `gt`, merge-when-ready, and a `ledger.tsv` keyed by PR+SHA. Those objects are forbidden here. The *invariants* underneath them are not:

- Completions are queue events, not interrupts.
- The brief is the product.
- A unit is not done until its output is externalized (push / ledger row / receipt).
- CI green is an input to a verdict, not a verdict.
- Independent verifier on a different model family when the unit is expensive, judgment-laden, or high blast.
- Separate write targets before adding locks.
- Encode a repeated instruction as a lint or type, then delete the instruction.

### 2.3 What the 2026-08-15 grilling locked (wave 1)

Four forks, captain answers:

1. **First increment** = ledger + independent verifier on the *current* loop. Do not collapse nouns. Do not event-drive the loop first. Do not skill-overlay only.
2. **Isolation** = exclusive reservation is the floor. Overlap **serializes or splits**. Worktrees are not a loop tool. (Amended same day: the original "worktree on contention" lock is revoked. See Spec 3.)
3. **Close legality** = every bead has a VERIFY command. Worker always runs it and records stdout. A different-model verifier is required for priority 0–1, contention / high blast, or judgment acceptance criteria. Cheap P2–P4 command-only beads may close on worker receipt plus loop spot-check.
4. **Enforcement** = `.flywheel/ledger.tsv`. Loop reopens a close that has no matching legal verdict and mails the owner on `thread_id=<bead-id>`. No `br` fork. No new Nostr kind.
5. **Same cut** = replace `bead_prompt` / `resume_prompt` with GOAL / SCOPE / ACCEPTANCE / VERIFY / REPORT. Lifecycle moves to one standing-orders file.

Lock 2 was **reopened and revoked the same day** (captain: worktrees are token-hungry and get abused). Spec 3 is the replacement. Locks 1, 3, 4, 5 stand.

### 2.4 Evidence of the gaps (not vibes)

- **Protocol is a prompt.** `flywheel/src/durable.rs` `bead_prompt` is one interpolating string. The loop cannot tell claim from close from "I forgot."
- **Close is trusted.** Nothing in preflight, types, or the loop requires proof. A dishonest or hurried `br close --reason "done"` drains the ready set.
- **Loop still polls.** `flywheel/docs/agent-instructions/loop.md`. flywheel ADR-0001 A2 (subscribe to herdr events) is still open. Latency hole. Separate root cause. Wave 4.
- **`unread_only` is a NO-OP** (ADR-0015). Mail-wake already keys on message id. Do not build read-receipts in this program.
- **Three nouns, one job.** `steer` is `goal`. `loop --work` drains ready beads. `workflow` is the generic engine. Wave 1 keeps all three. Wave 5 revisits only with evidence.
- **Dual loop bodies.** `durable.rs` `swarm_loop` and `main.rs` sync `cmd_loop`. Wave 4 deletes one.
- **Principles exist as profile prose** (ADR-0010 Spec C) but they are *unnamed*, not the poteto set, and nothing fails if a crew ignores them.
- **Playbooks do not exist as workflows.** `captain-audit` ≈ investigation. `debate` ≈ a thin interrogate. `goal` ≈ a generic feature factory. There is no bug-fix, no TDD-gated repro, no prototype/arena, no hillclimb, no forensics-without-fix, no bead-babysit, no pickup/pause, no ship-beads that means "ledger-green then commit."

### 2.5 The problem statement

> How do we make bead-driven swarm work *fail-closed on proof*, give every captain and crew the poteto principles as *named, enforced engineering reflexes* when they hit real bugs and design forks, and give them *playbook workflows* for the jobs they actually run — without new Nostr kinds, without new MCP tools, without Graphite, without a second orchestrator, and without bolting a skill overlay onto a prompt-as-protocol root cause?

---

## 3. Decision Drivers

1. **Beads are the tracker.** Every pstack PR-shaped object maps to a bead-shaped object or is rejected.
2. **ADR-0003 kind freeze + ADR-0007 C3 tool freeze.** No new kinds. No new MCP tools. Ledger is a flywheel file + CLI.
3. **ADR-0015 one-way dependency and one mail surface.** Toron does not learn about `.flywheel/ledger.tsv`. Flywheel does not grow a second mail backend.
4. **Atomic reservation conflict-check is NEVER removed** (ADR-0011). Intents stay advisory. Held stays HARD.
5. **Headless must not fake 43001/43004** (ADR-0010 Spec E). Cheap VERIFY is headless. Live-UI VERIFY is pane.
6. **Encode lessons in structure.** A second textual reminder is a failed implementation of this ADR.
7. **Wave 1 locks are load-bearing.** Later waves may not undo them.
8. **Live loop must not break.** Ledger and briefs land behind tests on the existing `--work` path. No flag that preserves the 2KB prompt.
9. **Identity is ADR-0018.** Briefs and ledger `actor` use the canonical `flywheel-<slug>-<role><seq>` name (or registered alias). Never a free string.
10. **TDD for new features and bug fixes** (repo convention). The `bug-fix` and `feature` workflows make that a step, not a hope.

---

## 4. Considered Options

### Option A — Skill overlay only

Copy poteto playbooks into `~/.agents/skills`. No engine change.

Rejected. The root cause is prompt-as-protocol and self-reported close. More text on the same string is the failure mode poteto names in **encode-lessons-in-structure**.

### Option B — Ledger + verifier + briefs on the current loop, then bake principles, then playbook workflows (CHOSEN)

Keep `decide` / `steer` / `loop` / `workflow` as nouns. Add `BeadUnit` + `Verdict` + `.flywheel/ledger.tsv`. Delete the lifecycle novel. Then encode the 21 principles as registry + lint + profiles. Then compile playbooks as builtins.

Chosen because it is the smallest change that makes close fail-closed, it respects the grilling locks, and it gives later waves a type to hang principles and playbooks on.

### Option C — Collapse steer / loop / workflow into one BeadRuntime first

Highest long-term simplicity. Biggest first cut. Blocks overnight proof until the collapse lands. Deferred to wave 5, and only with evidence that the three nouns still cost.

### Option D — Event-driven loop first (flywheel ADR-0001 A2)

Fixes dispatch latency. Does not fix self-reported close, shared-tree collisions, or the 2KB prompt. Wave 4.

### Option E — Fork pstack / poteto-mode into the repo

Rejected. Different tracker, different multiplexer, different mail. Dual vocabulary. We take the invariants and the principle *names*, not the TypeScript orchestrator, not Graphite, not Cursor `/loop`.

---

## 5. Decision Outcome

Chosen option: **B**, because it matches the grilled locks, fails closed on the real artifact, and leaves a type (`Verdict`) that waves 2–3 can enforce principles and playbooks against.

### Spec 0 — Vocabulary (beads, not PRs)

These substitutions are mandatory in every workflow, skill, stage, and brief this ADR adds. Using the left column in new text is a spec bug.

| Forbidden (pstack) | Required (ours) |
|---|---|
| pull request, PR number, `gh pr` | bead id (`br show`) |
| Graphite, `gt`, stack, merge-when-ready | `br dep`, `bv --robot-plan` tracks, `br close` |
| "land the stack" | all tracked beads ledger-green, then housekeeper commit |
| "open a PR" as a playbook last step | `br sync --flush-only` + commit `.beads/` (human or housekeeper) |
| babysit CI comments | babysit a blocked / unclaimed / unverified bead |
| `orch.ts` as orchestrator | `flywheel loop` + `flywheel ledger` + `br` |

Mail `thread_id` stays the bead id. File reservation `reason` stays the bead id. Commit messages include the bead id.

### Spec 1 — BeadUnit, Verdict, ledger

Core types (Rust, flywheel). This is the data shape. Get it right before more loop branches.

```text
BeadUnit
  id:           String            # br id
  title:        String
  priority:     u8                # 0-4
  verify:       String            # argv or shell command; required
  blast:        Normal | High     # High if overlapping reserved paths, or AC is judgment
  owner:        Option<AgentName> # ADR-0018 canonical name
  head_sha:     Option<String>    # git SHA of the work being verified

VerdictKind
  UnitTestVerified
  LiveVerified
  WorkerReceipt
  VerifierBlocked
  VerifierFailed

Verdict
  kind:         VerdictKind
  bead_id:      String
  sha:          String
  at:           DateTime<Utc>
  actor:        AgentName
  receipt_path: PathBuf           # stdout / log the check produced
  verify_cmd:   String
```

**Illegal, must not compile as a success path:** `{ status: closed, verdict: none }`.

**Ledger file:** `<project>/.flywheel/ledger.tsv`

Columns, tab-separated, one header row:

```
bead_id	sha	kind	actor	at	receipt_path	verify_cmd
```

`flywheel ledger record` upserts on `(bead_id, sha)`. Re-record on the same key is idempotent (last writer wins, previous row stays in an optional `ledger.jsonl` append-only log if we want audit; TSV is the live view).

`flywheel ledger check <bead-id> [--sha HEAD]` exits 0 only when a *legal* verdict exists for that key.

**Legal close**

| Condition | Legal kinds |
|---|---|
| priority ≥ 2 **and** blast = Normal **and** VERIFY is a single cheap command | `WorkerReceipt`, `UnitTestVerified`, `LiveVerified` |
| priority ≤ 1 **or** blast = High **or** AC is judgment (review, design, "does this feel right") | `UnitTestVerified`, `LiveVerified` only |
| any | never `VerifierFailed`, never `VerifierBlocked`, never missing |

`VerifierBlocked` is not a pass. Respawn the verifier when the environment heals. `VerifierFailed` spawns a fix unit (or reopens). It does not get a re-verify of the same broken head.

**Single ledger writer.** The loop is the only process that mutates `.flywheel/ledger.tsv`. A worker writes a receipt file only:

```
.flywheel/receipts/<bead-id>-<sha>.<kind>.txt
```

The loop upserts the TSV from that file after spot-checking it is non-empty and contains the VERIFY command line. Two workers must never append the TSV. `flywheel ledger record` used by a worker is a helper that writes the receipt file; the loop drains receipts each tick. This is `separate-before-serializing` applied to the ledger itself.

**Loop enforcement (fail closed)**

The loop already knows the assigned set (claim-watch keys). Each tick it `br show --json` those ids (not a full `br list` scan, not a comment grep). For each id whose status became `closed` since last checkpoint:

1. SHA = receipt filename sha, else `git rev-parse HEAD` in the **primary** tree.
2. `ledger check`.
3. If illegal or missing: `br reopen <id>` + mail on `thread_id=<id>` subject `[<id>] unverified close reopened`. The bead is ready again. One mail per `(bead, sha)`.
4. If legal: leave closed. Claim-watch releases.

A worker who closes without a receipt *cannot* drain the queue. That is the red test of wave 1.

**VERIFY lives on the bead.** Required block in the description, exact shape:

```markdown
## VERIFY
```
<one command, one line>
```
```

`load_ready_beads` parses that fence. Missing, multi-line, or empty → refuse-to-dispatch + mail the planner. **No fallback to `scripts/verify-gates.sh`.** That script is the whole workspace matrix. Using it as a default is a time and token bomb, and it lies about a docs bead. If the planner cannot name a cheaper command, the bead is not ready.

### Spec 2 — Brief + standing orders (delete the lifecycle novel)

Delete `bead_prompt` and `resume_prompt` as prompt bodies. Replace with a filled brief. The lifecycle is not restated in the send.

```
GOAL         one sentence, the outcome
SCOPE        paths this unit may write; paths it may not
CONTEXT      pointers only (paths, parent bead id, receipt path). Never file dumps.
ACCEPTANCE   checkable criteria, one per line
VERIFY       the exact command
TIMEBOX      rough cap; on expiry return partial findings
FORBIDDEN    no new beads, no force-unlock of a peer, no scope leak, **no worktree**, plus unit bans
REPORT       status, sha, verdict kind, what you actually ran, deviations, PRINCIPLES cited
PRINCIPLES   each line: `<principle-name> — <the decision it changed>`  (names only; do not paste leaves)
STANDING     **path only.** Never verbatim paste the standing-orders file into the send.
```

Resume uses the same template with `GOAL` prefixed `Resume <id>:`.

`systematic-execute` (p6) is rewritten to *point at* the brief contract and the receipt path. It must not re-paste the claim→reserve→close novel.

Standing orders file:

- Generated path: `<project>/.flywheel/standing-orders.md` (project fences) layered on `~/.config/flywheel/standing-orders.md` (global, generated from the principle registry).
- Numbered lines, one constraint each.
- **Path-referenced only.** ADR-0010 `Profile.behavior` stays the first prepend. Standing orders are **not** a second prepend of the 21 leaves. The send names the path. The agent reads the file if it needs a rule.
- When a captain restates an instruction, wave 2/6 appends a line here and deletes the restatement.

Hard cap: the dispatched prompt (behavior + brief) ≤ 4 KiB. Over cap is a loop bug, not an agent problem.

### Spec 3 — Isolation (worktrees forbidden)

**Amendment 2026-08-15.** The grilled "worktree on contention" lock is revoked. It treated a worktree as isolation. In this stack a worktree is a **second full context** that still shares the objects that must be single-writer.

**Why a worktree does not isolate us**

These are real single-writer invariants. A second checkout does not split them. It duplicates them, then a merge fights:

| Shared object | Why a worktree lies |
|---|---|
| `.beads/` (`br` db + `issues.jsonl`) | Two trees, one tracker. Dual close/claim. |
| `.flywheel/ledger.tsv` + receipts | Proof store is per-project, not per-tree. |
| `Cargo.lock` / workspace `target/` | Two compiles, or a corrupted shared target. Build slots already serialize cargo. |
| `docs/catalog/` | Catalog freshness is one artifact. |
| ADR-0018 `agent-names.json` | Pane identity is per workspace, not per worktree. |

**Why it is token-hungry**

A herdr/git worktree is a second repo the new pane will index. That pane re-reads `AGENTS.md`, crate maps, and skills. `CONTEXT_LIMIT` at 80% fires twice as often. The brief's CONTEXT can no longer be pointers (the sibling's receipts live in the other tree), so we start inlining dumps. Merge is a third agent session. pstack can afford this because a PR *is* the isolated unit. Our unit is a bead in one tracker. Worktrees fight the tracker.

**Rule**

1. Every in-flight bead **must** hold an exclusive reservation on the paths it will write, `reason=<bead-id>`, TTL renewed by the worker. Acquire vs held = HARD. Acquire vs in-TTL intent = SOFT (ADR-0011, unchanged).
2. **The loop never calls `worktree create` / `worktree open`.** Playbook dispatch never does either. `FORBIDDEN` on every brief includes `no worktree`.
3. **`flywheel worktree` stays an operator CLI.** Captain may create one by hand for a human experiment. The loop will not adopt that tree, will not dispatch `--work` into it, and will not ledger-check a SHA from it. Preflight on `loop run` / `workflow run --execute` fails if the project's workspace has an extra linked worktree the run did not expect (opt-out: `--allow-worktree`, operator only, logged).
4. **Contention resolution, in this order (stop at the first that applies):**
   1. **Split** by directory / bead-dep. **Only when a coalition is already formed** (ADR-0011 proposer). Loop `--work` with no coalition does **not** invent a split and does **not** call an LLM planner.
   2. **Serialize.** Default for `loop --work`. The overlapping second bead stays `ready` and is not dispatched until the holder releases. Claim-watch does not treat that wait as a stuck owner (new wait reason: `overlap`, distinct from agent `blocked`).
   3. **Soft-conflict freeze + propose** (ADR-0011). Crews negotiate. Still one tree.
   4. **Never** "spawn a worktree for the loser."
5. **Always-serialize paths** (overlap here is never "split into parallel"): `.beads/**`, `.flywheel/**`, `Cargo.lock`, workspace-root `Cargo.toml`, `docs/catalog/**`, `target/**`. Two beads that both need `Cargo.lock` run one after the other. Use the existing **build slot** (`acquire_build_slot`) for `cargo test` / `cargo build`.
6. **Throwaway sketches** (`prototype` / `arena`) write under `/tmp/flywheel-proto-<run>-<arm>/` copies of *only the files in SCOPE*. They do not get a herdr worktree of the repo. They do not run `br`. They do not touch `.beads/`. The oracle picks; the winner is applied back in the primary tree as a later bead.
7. An agent that creates a git/herdr worktree during a loop-owned bead is out of scope. The loop mails a nudge and does not accept a ledger SHA from that tree.

This is `separate-before-serializing` read correctly: eliminate *false* sharing (two writers on `src/foo.rs`) with reservations. Do **not** clone the world to avoid a queue. One writer of `.beads/` is a real invariant. Serialize it.

### Spec 4 — Scaled verifier

- Worker always runs VERIFY and writes the receipt file (Spec 1). It does not edit the TSV.
- Loop spot-checks the receipt exists, is non-empty, and contains the VERIFY command line before accepting `WorkerReceipt`.
- Independent verifier:
  - profile `@reviewer`
  - **different model family** than the worker (model registry: if worker is grok, reviewer is not grok)
  - headless when VERIFY is a command
  - pane when VERIFY needs a live TUI/UI
  - records `unit-test-verified` or `live-verified` (or `verifier-failed` / `verifier-blocked`)
- Required when priority ≤ 1, blast = High, or the bead type is `docs` with judgment AC / any `review` stage.
- A verifier whose entire product would be rerunning one cheap P3 command is ceremony. The worker receipt plus spot-check is enough (pstack's own scale-to-unit rule).

### Spec 5 — The 21 principles (canonical names)

These names are the steering vocabulary. They are copied from pstack's principle leaves (MIT). The *text of the rule* is compiled into flywheel so a crew can `flywheel principles show <name>` and get the full leaf, not a gist.

**Core**

| Name | When | Rule |
|---|---|---|
| `laziness-protocol` | Refactor, sizing a diff, tempted to add a layer | Prefer deletion. Smallest change that solves it. Flat call hierarchy. |
| `foundational-thinking` | Before writing logic | Data structures first. Scaffold before features. Ask what concurrent actors share. |
| `redesign-from-first-principles` | Integrating a new requirement | Redesign as if it had been there on day one. Do not bolt on. |
| `subtract-before-you-add` | Sequencing an addition or rewrite | Remove dead weight first, then build on the simpler base. |
| `minimize-reader-load` | Shaping hard-to-trace code | Count layers and hidden state. Collapse one-caller wrappers. |
| `outcome-oriented-execution` | Planned migrations with phase boundaries | Converge on the target. Do not keep throwaway compatibility. |
| `experience-first` | Product / UX / scope tradeoffs | User delight over implementation convenience. Ship less, ship better. |
| `exhaust-the-design-space` | Novel interaction or architecture, no precedent | Build 2–3 competing prototypes. Compare. Then commit. |
| `build-the-lever` | Any non-trivial work | Build the tool that does or proves it. The tool is what a reviewer reruns. |

**Architecture**

| Name | When | Rule |
|---|---|---|
| `model-the-domain` | Stateful logic, branching, repeated shape | Encode the domain in a structure, not scattered ifs. |
| `boundary-discipline` | Validation, errors, adapters | Guards at the boundary. Trust internal types. Pure core. |
| `type-system-discipline` | Types and signatures | Illegal states unrepresentable. Brand primitives. Parse at the boundary. |
| `make-operations-idempotent` | Commands, loops, crashes, retries | Converge to the same end state from any partial prior run. |
| `migrate-callers-then-delete-legacy-apis` | New internal API, old callers exist | Migrate and delete in one wave. No compatibility layer. |
| `separate-before-serializing-shared-state` | Concurrent writers | Eliminate sharing first. Lock only when one writer is a real invariant. |

**Verification**

| Name | When | Rule |
|---|---|---|
| `prove-it-works` | After a task, before "done" | Check the real artifact. Not a proxy, not "it compiles", not a self-report. |
| `fix-root-causes` | Debugging | Reproduce first. Ask why until the root. No nil-check silencers. |
| `sequence-verifiable-units` | Multi-step work, stacking commits | Each unit ends in a check. Verify before the next. |

**Delegation**

| Name | When | Rule |
|---|---|---|
| `guard-the-context-window` | Large outputs, fan-out, long files | Bulk to subagents. Summaries in the main thread. |
| `never-block-on-the-human` | Tempted to ask "should I?" on reversible work | Proceed. Present the result. Reserve asks for irreversible or genuine preference. |

**Meta**

| Name | When | Rule |
|---|---|---|
| `encode-lessons-in-structure` | Writing the same instruction a second time | Lint, type, preflight, or script. Then delete the instruction. |

A captain steers with the name: "apply prove-it-works. show the ledger row, not the close reason." That only works if the leaf is in the system (wave 2) and the ledger exists (wave 1).

### Spec 6 — How principles are baked (the point of wave 2)

Text that an agent may skip is **not** an implementation of this spec. Each principle must have at least one *mechanism* from the list below. The table is the acceptance test for wave 2.

| Principle | Structural encoding |
|---|---|
| `laziness-protocol` | `refactoring` workflow first step is a deletion inventory. Reviewer BLOCKER if a playbook-authored PR adds a one-caller wrapper without deleting the old path. |
| `foundational-thinking` | `feature` workflow requires input `data_shape`. Lint fails if missing. |
| `redesign-from-first-principles` | `feature` reviewer stage asks "would this be the design if the requirement had been day-one?" Finding without that answer is incomplete. |
| `subtract-before-you-add` | `goal` / `feature` / `refactoring` may not add files named `*V2*` / `*_improved*` / `*_enhanced*` (preflight + reviewer). Matches repo convention. |
| `minimize-reader-load` | Dispatched prompt ≤ 4 KiB. Standing orders by path, never pasted. `bead_prompt` gone. No worktree second index. |
| `outcome-oriented-execution` | No `--legacy-prompt` flag. Dual template is a compile error (the old fns deleted). |
| `experience-first` | `decide` already prefers `loop-work` when beads are ready. Do not add a "just spawn more agents" primitive. |
| `exhaust-the-design-space` | `prototype` / `arena` require `n >= 2` arms. Lint fails on `n=1`. |
| `build-the-lever` | VERIFY is a command. `flywheel ledger` is the lever. A playbook step that says "verify by inspection" without a command is a lint error except `forensics` (diagnosis is the artifact). |
| `model-the-domain` | `BeadUnit` / `Verdict` / `Blast` enums. Feature `data_shape` is a named type in the brief, not a paragraph. |
| `boundary-discipline` | `br` JSON parsed at `__ready` / work.rs. Loop core takes `Vec<ReadyBead>`, never a raw string. |
| `type-system-discipline` | `VerdictKind` is exhaustive. New kind without a `legal_close` case does not compile. |
| `make-operations-idempotent` | `ledger record` upsert. Reopen is convergent (already closed + missing row → reopen; already open + missing row → no-op). |
| `migrate-callers-then-delete-legacy-apis` | Wave 1 deletes `bead_prompt` / `resume_prompt`. Tests that snapshot the old string are rewritten, not dual-pathed. |
| `separate-before-serializing-shared-state` | Spec 3. False sharing → reservation + split. Real single-writer (`.beads/`, ledger, `Cargo.lock`) → serialize. Never clone the repo to dodge a queue. |
| `prove-it-works` | Spec 1. Close without legal verdict reopens. |
| `fix-root-causes` | `bug-fix` workflow: repro step must produce a failing command *before* the fix step is dispatched (`needs: [repro]`). A "fix" workflow with no repro node fails lint. |
| `sequence-verifiable-units` | One in-flight bead per owner. Next ready bead only after a legal ledger row or an honest reopen. |
| `guard-the-context-window` | Brief small. Standing orders by path. No worktree. No `verify-gates.sh` default. Scout compresses. Interrogate default N=2. |
| `never-block-on-the-human` | Reopen + T0–T3 mail is automatic. `request_approval` only for irreversible or plan-gate (`goal`, not `goal-direct`). No "should I keep going" in standing orders. |
| `encode-lessons-in-structure` | Wave 6 `reflect`. Also: this table itself. A principle with only a skill paragraph and no row above is not baked. |

**Delivery surfaces for the names (so captains and crews actually meet them)**

1. `flywheel principles list|show <name>` — compiled-in full leaf.
2. `Profile.behavior` rewritten to *cite the names* that role owns (coder: `prove-it-works`, `fix-root-causes`, `sequence-verifiable-units`, `laziness-protocol`, `type-system-discipline`; reviewer: `prove-it-works`, `fix-root-causes`, `minimize-reader-load`; planner: `foundational-thinking`, `sequence-verifiable-units`, `model-the-domain`; oracle: `exhaust-the-design-space`, `redesign-from-first-principles`; scout: `guard-the-context-window`, `prove-it-works`; housekeeper: `make-operations-idempotent`, `encode-lessons-in-structure`).
3. Brief `PRINCIPLES` field. Reviewer marks a report incomplete if the field is empty on a non-trivial bead (priority ≤ 2).
4. Workflow YAML optional `principles: [prove-it-works, …]`. Lint checks the name is in the registry.
5. Skills: flywheel, swarm, beads-execute, beads-workflow get a **Principles** index (names + one line), not a second copy of the leaves. Full text is `flywheel principles show`.
6. AGENTS.md (toron + flywheel) gets a short pointer: "engineering principles: `flywheel principles list`. Cite the name that changed the decision."
7. Stage `systematic-execute` and `code-review` mention the three verification names by name.

This is how a future captain debugging a flaky reservation does not get "try harder." They get `fix-root-causes` (repro first) and `prove-it-works` (show the conflict check, not the log line that says ok).

### Spec 7 — Playbook workflows (wave 3)

pstack playbooks become compiled-in flywheel workflows (or an explicit "already exists / skip" row). All are invoke-only unless noted. All that edit the repo are bead-tracked except `goal-direct` (already the no-tracker arm).

#### 7.1 Keep (already builtins)

| Workflow | pstack analogue | Change in this program |
|---|---|---|
| `captain-audit` | investigation | Prepend principle index. Report must cite `prove-it-works` (evidence) and stay read-only. |
| `debate` | interrogate (thin) | Wave 3 adds `interrogate` as the multi-model form. `debate` stays 2 reviewers + oracle. |
| `plan` / `plan-direct` / `beads` / `plan-review` | multi-phase plan (partial) | Bead descriptions must include `## VERIFY`. |
| `goal` / `goal-direct` | feature factory + autonomous-run-ish | `goal` stays the meta. New `feature` is the *named data shape* path. |
| `onboard` / `triage` / `triage-deep` / `weekly-review` | no pstack twin | Unchanged except standing-orders prepend. |

#### 7.2 New builtins (required)

Each YAML lives in `flywheel/src/builtin_workflows.rs` (compiled-in, same as today). Lint must pass. `--execute --mode pane` unless noted.

**`bug-fix`**

Inputs: `symptom`, `repro` (command or steps).

```
repro      @scout     run/write the repro. ACCEPTANCE = command exits non-zero (or shows the bug).
tdd        @coder     failing test first. VERIFY = that test fails.
fix        @coder     minimal fix. needs: [tdd]. VERIFY = the test passes AND the original repro is gone.
prove      @reviewer  different model. record unit-test-verified. needs: [fix].
```

Lint: no `fix` node without a `repro` predecessor. Principles: `fix-root-causes`, `prove-it-works`, `sequence-verifiable-units`.

**`feature`**

Inputs: `objective`, `data_shape` (required).

```
shape      @planner   name the type / state machine. refuse if data_shape empty.
build      @coder     implement against that shape. TDD.
prove      @reviewer  VERIFY from the bead. scaled per Spec 4.
```

Principles: `foundational-thinking`, `model-the-domain`, `prove-it-works`.

**`refactoring`**

Inputs: `target`, `invariant_cmd` (the test that must stay green).

```
baseline   @scout     run invariant_cmd, store receipt.
subtract   @coder     deletion inventory + apply deletions.
reshape    @coder     behavior-preserving reshape.
prove      @reviewer  invariant_cmd still green; no V2 files.
```

Principles: `laziness-protocol`, `subtract-before-you-add`, `prove-it-works`.

**`prototype`**

Inputs: `question`, `n` (default 2, min 2, max 3).

```
arms       parallel over n   @coder   throwaway sketch in /tmp/flywheel-proto-<run>-<arm>/ (SCOPE files only; no herdr worktree; no br)
judge      @oracle           compare on named criteria. pick one. others discarded.
```

No merge of "a bit of each" unless the oracle writes a graft bead. Principles: `exhaust-the-design-space`, `separate-before-serializing-shared-state`.

**`arena`**

Same shape as `prototype` but the arms may be the *same* brief on different models (model race). Selection rule declared up front: `first-pass` | `rank-all` | `best-of`. Principles: `exhaust-the-design-space`, `guard-the-context-window` (parent keeps the table, not raw dumps).

**`perf`**

Inputs: `metric`, `baseline_cmd`, `budget`.

```
baseline   @scout     run baseline_cmd, record number.
change     @coder     one change aimed at the metric.
measure    @scout     same command. keep only if metric improved and budget held.
```

Discard is a revert, not a leftover. Principles: `prove-it-works`, `sequence-verifiable-units`, `laziness-protocol`.

**`hillclimb`**

Inputs: `metric`, `target`, `max_iters`.

Loop (workflow `retry` / foreach with cap): hypothesize → smallest change → measure → ledger row (keep or discard). One commit per accepted win. Principles: `prove-it-works`, `sequence-verifiable-units`, `build-the-lever`.

**`forensics`**

Inputs: `symptom`, `artifact` (optional path to a profile/trace).

```
diagnose   @scout     instrumentation or artifact read. Deliverable is a diagnosis.
```

**No fix step.** A fix is a separate `bug-fix` after the captain accepts the diagnosis. Principles: `fix-root-causes`, `prove-it-works`. This is the "I said repro, I did not ask for a fix" playbook.

**`babysit-bead`**

Inputs: `bead_id`.

Drive one in_progress / blocked / unverified-closed bead to a legal ledger row: claim-watch nudge, reservation renew, T0–T3 mail, verifier spawn if required. Does not start new beads. Principles: `never-block-on-the-human`, `prove-it-works`.

**`ship-beads`**

Inputs: `project`.

Predicate: no ready beads, claim-watch empty, every newly closed bead legal on the ledger. Then `@housekeeper` `commit-and-push` (existing p8). This is pstack "shipping" without merge-when-ready. Principles: `prove-it-works`, `sequence-verifiable-units`.

**`autonomous-work`**

Inputs: `project`, `predicate` (default = ship-beads predicate).

Does **not** reimplement the loop. It is `flywheel loop run --work --on-blocked mail` plus a terminal `ship-beads` check. Documented so `decide` can point at it. Principles: `never-block-on-the-human`, `make-operations-idempotent`.

**`pickup`**

Inputs: `project`.

Read `.flywheel/agent-state/`, `br list --status in_progress`, ledger gaps, last-sent registry. Respawn exhausted panes. Resume briefs for stranded in_progress. Principles: `make-operations-idempotent`, `guard-the-context-window`.

**`pause`**

Inputs: `project`, `msg`.

`flywheel checkpoint save` + write standing handoff (what is in flight, ledger gaps, exact resume command). Kill nothing that is working unless `--force-working` is explicit. Principles: `make-operations-idempotent`, `never-block-on-the-human`.

**`interrogate`**

Inputs: `target` (diff, bead id, or path set).

N reviewers (default **2**, max 3) on **different model families**, same rubric, `readonly`. Parent synthesizes Act-on / Consider / Noted / Dismissed. **Does not apply fixes.** A follow-up `bug-fix` or `refactoring` consumes Act-on. Principles: `prove-it-works`, `fix-root-causes`, `guard-the-context-window`. Extends, does not delete, `debate`.

**`figure-it-out`**

Inputs: `objective`.

Uses existing `workflow compose --agent` (planner authors YAML, lint is the gate) then `--execute`. For work that matches no bundled playbook. Principles: `foundational-thinking`, `encode-lessons-in-structure`.

**`reflect`** (wave 6, listed here so the catalog is complete)

Inputs: `run_id` or `project`.

Read ledger + standing-orders + human corrections in mail threads. If the same instruction appears twice, emit a bead: add a lint / preflight / principle-registry line, then delete the instruction from standing orders. Principles: `encode-lessons-in-structure`.

#### 7.3 Explicitly not shipped as builtins

| pstack playbook | Why not |
|---|---|
| visual-parity | We do not ship a pixel UI product. |
| authoring-a-skill | Existing skill-creator / authoring skills. Not a flywheel workflow. |
| eval | Optional later. Blind eval of skill changes is a research tool, not daily driver. |
| worktree-cleanup | Housekeeper stage, not a goal workflow. |
| opening-a-pr, shipping (Graphite), autopilot-full, autopilot-stack | PR-shaped. Replaced by `ship-beads` + `autonomous-work`. |
| babysit (PR) | Replaced by `babysit-bead`. |

### Spec 8 — `decide` table extensions (wave 3)

`flywheel/src/decide.rs` stays one pure table, first match wins. Insert **after** row 1 (audit) and **before** the implement+tracker steer rows, so a "fix the repro" does not fall through to generic `steer`.

New rows (names are the primitive ids):

| When | Primitive | argv |
|---|---|---|
| `repro`, `regression`, `bug-fix` (and `fix`/`implement` with `bug`/`repro`) | `bug-fix` | `workflow run bug-fix --execute --mode pane` |
| `refactor` | `refactoring` | `workflow run refactoring --execute --mode pane` |
| `prototype`, `sketch`, `mock it up` | `prototype` | `workflow run prototype --execute --mode pane` |
| `arena` | `arena` | `workflow run arena --execute --mode pane` |
| `perf`, `p95`, `slow` (not `fix` alone) | `perf` | `workflow run perf --execute --mode pane` |
| `hillclimb` | `hillclimb` | `workflow run hillclimb --execute --mode pane` |
| `forensic`, `leak`, `idle cpu`, `spindump` | `forensics` | `workflow run forensics --execute --mode headless` |
| `babysit` + bead-ish | `babysit-bead` | `workflow run babysit-bead --execute --mode pane` |
| `pickup`, `resume session` | `pickup` | `workflow run pickup --execute --mode pane` |
| `pause`, `going offline` | `pause` | `workflow run pause --execute` |
| `interrogate`, `tear this apart`, `adversarial review` | `interrogate` | `workflow run interrogate --execute --mode headless` |
| `figure it out`, `no playbook` | `figure-it-out` | `workflow compose --agent` |
| `ship beads`, `land the beads` | `ship-beads` | `workflow run ship-beads --execute --mode pane` |

Existing rows stay: audit → `captain-audit`; ready beads + work/loop/beads → `loop-work`; implement+tracker → `steer`; implement+direct → `steer-direct`; compose; coalition; spawn-send; cron; fallback `steer`.

Every new primitive gets a `decide.rs` unit test, same style as `row1_audit_is_captain_audit`.

### Spec 9 — Waves

#### Wave 1 — Bead verdict (locked, implement first)

- Types `BeadUnit`, `Verdict`, `VerdictKind`, `Blast`.
- Receipt files + loop-only TSV writer. `flywheel ledger check|status`. Worker helper writes receipts only.
- Loop reopen-on-unverified-close (assigned-set `br show`, not a full list scan).
- Parse exact `## VERIFY` fence. Missing → refuse-to-dispatch. No `verify-gates.sh` default.
- Reservation floor. Contention → split or serialize. Loop must not call `worktree create`.
- Preflight fails on an unexpected linked worktree unless `--allow-worktree`.
- Scaled verifier (`@reviewer`, other model family).
- Delete `bead_prompt` / `resume_prompt`. Ship the brief template. Standing orders path-only.
- Tests: the validation paths in §7 of this ADR.

**Out of wave 1:** new playbook builtins (except the brief/ledger enable them), event-driven loop, noun collapse, read receipts, dual-loop deletion, `reflect`.

#### Wave 2 — Principles as structure

- Compiled-in principle registry + `flywheel principles list|show`.
- Generate `~/.config/flywheel/standing-orders.md` from the registry.
- Rewrite six `Profile.behavior` texts to **cite principle names only** (Spec 6). Do not paste leaves. `flywheel principles show` is on-demand.
- Rewrite stages `systematic-execute` and `code-review` to cite verification principles and the ledger CLI.
- Brief `PRINCIPLES` field required for priority ≤ 2.
- Workflow lint: known `principles:` names; `feature` requires `data_shape`; `bug-fix` requires `repro` predecessor; `prototype`/`arena` require `n>=2`.
- Preflight: standing-orders exists; reject `*V2*` / `*_improved*` / `*_enhanced*` new files in a playbook run.
- Skill + AGENTS.md pointers (toron, flywheel, beads-execute, swarm).
- Reviewer incomplete if PRINCIPLES empty on a P0–P2 bead.

#### Wave 3 — Playbook workflows + decide

- All Spec 7.2 builtins compiled in, lint green, e2e smoke each (`workflow lint` + a dry control-flow run; `--execute` tests where cheap).
- Spec 8 decide rows + tests.
- Catalog freshness if the CLI catalog lists workflows (`toron catalog` / flywheel `workflow list`).
- Skill sync contract: Command enum, AGENTS.md, flywheel skill §5, robot-capabilities.

#### Wave 4 — Event-driven loop + subtract the duplicate

- Implement flywheel ADR-0001 A2: `events.subscribe` is primary. Poll is heartbeat/fallback only.
- Delete the unused loop body (sync *or* durable, whichever is proven redundant after A2). One `plan_actions`.
- Keep claim-watch, T0–T3, ledger check, mail wake.

#### Wave 5 — BeadRuntime (conditional)

Revisit only if, after waves 1–3, captains still cannot answer "where does X come from?" in 30 seconds (`minimize-reader-load`). If yes: one runtime object that `steer`, `loop --work`, and `workflow run goal` all construct. If no: accept the three nouns and close this wave as "won't do."

#### Wave 6 — Reflect

- `reflect` builtin.
- Cron-optional: weekly-review may `call_workflow: reflect`.
- Output is a bead that adds a lint/preflight, not another paragraph in AGENTS.md.

### Spec 10 — Ownership (ADR-0015 addendum)

**flywheel guarantees (new)**

- A `br close` observed by the loop is not terminal without a legal ledger row.
- `flywheel ledger` is the rerunnable proof.
- Playbook workflows and the principle registry live in flywheel (compiled-in).
- Dispatch still goes through herdr panes / headless leaves, not toron events (ADR-0015 unchanged).

**toron guarantees (unchanged)**

- `MailSurface`, atomic reservations, crash-survivable `request_approval`, durable XDG root, 43001–43006, ADR-0018 naming.
- No new tools. No new kinds. No dependency on flywheel. No knowledge of `.flywheel/ledger.tsv`.

**beads**

- System of record for work. VERIFY text lives in the bead. Dep graph is the stack.

### Spec 11 — Non-goals

- No Graphite, no `gh pr` as the unit, no merge queue.
- No pstack submodule, no `orch.ts`, no Cursor `/loop` as the wake mechanism.
- No new Nostr kinds, no 39th MCP tool.
- No `unread_only` implementation.
- No `--legacy-prompt` compatibility shim.
- No visual-parity / skill-authoring / eval builtins in this program.
- No making cron a `trigger: schedule` on a normal workflow (ADR-0012 stands).

---

## 6. Implementation Plan

An agent implements **one wave at a time**, TDD, beads with deps. Do not start wave N+1 until that wave's verification checkboxes are green.

### Wave 1 — affected paths

| Path | Change |
|---|---|
| `flywheel/src/ledger.rs` | **new.** types, TSV read/write, `legal_close`, upsert |
| `flywheel/src/main.rs` | `Ledger` subcommand; loop hook after close-detect; brief builder |
| `flywheel/src/durable.rs` | delete `bead_prompt` / `resume_prompt` bodies; brief builder; receipt drain; reopen leaf; serialize-on-overlap; never `worktree create` |
| `flywheel/src/work.rs` / `__ready` | parse `## VERIFY` into `ReadyBead.verify` |
| `flywheel/src/preflight.rs` | `.flywheel/` writable; `br` present (already) |
| `flywheel/src/stages.rs` | `systematic-execute` points at brief + `flywheel ledger` |
| `flywheel/tests/` | unit: legal_close matrix; reopen idempotence; brief fill; TSV upsert |
| `~/.agents/skills/flywheel/SKILL.md` | ledger CLI, brief contract |
| this ADR | status stays proposed until wave 1 green + captain accept |

**Patterns to follow:** `ReadyBead` parsing style in `durable.rs`; `on_blocked_tier_exit` purity (legal_close is pure); subprocess leaves for `br reopen` / mail (no nested `block_on`).

**Patterns to avoid:** string-contains "done" as proof; storing verdict only in a br comment; teaching the ledger via a longer prompt; new toron types.

**Migration:** existing in_progress beads get a resume *brief*, not the old resume string. Existing closed beads are not retro-verified.

### Wave 2 — affected paths

| Path | Change |
|---|---|
| `flywheel/src/principles.rs` | **new.** 21 leaves as `const` + `list`/`show` |
| `flywheel/src/profile.rs` | behavior texts cite names |
| `flywheel/src/stages.rs` | p2/p6 cite verification names + ledger |
| `toron-workflow` lint | `principles:` known names; feature `data_shape`; prototype `n>=2`; bug-fix graph shape |
| `flywheel` skill, swarm skill, beads-execute, both AGENTS.md | index, not a second bible |
| `docs/principles/` (flywheel) | optional human-readable copies of the leaves; `show` is canonical |

### Wave 3 — affected paths

| Path | Change |
|---|---|
| `flywheel/src/builtin_workflows.rs` | all Spec 7.2 YAML |
| `flywheel/src/decide.rs` | Spec 8 rows + tests |
| `flywheel/src/stages.rs` | new stages if a playbook step is reused (`repro`, `tdd-red`, `baseline`) |
| skill sync | Commands, robot-capabilities, AGENTS.md, flywheel skill |
| catalog | `workflow list` includes new names |

### Waves 4–6

Wave 4: `durable.rs` event subscribe (see flywheel ADR-0001 §4); delete duplicate loop. Wave 5: only after a written "reader-load still fails" note in this ADR's More Information. Wave 6: `reflect` YAML + optional weekly-review edge.

---

## 7. Verification

### Wave 1

- [ ] `BeadUnit` / `VerdictKind` exist. `legal_close` is table-tested for every kind × priority × blast cell.
- [ ] `flywheel ledger record` then `check` exits 0 on the same `(id, sha)`. Second record is idempotent.
- [ ] Fixture: worker closes with no row → loop reopens → bead is in `br ready`. **This is the red-then-green of the program.**
- [ ] Fixture: P3 cheap VERIFY + `worker-receipt` + non-empty receipt → stays closed.
- [ ] Fixture: P0 + `worker-receipt` only → reopened until `@reviewer` records `unit-test-verified` or `live-verified`.
- [ ] Two overlapping ready beads on a normal path → split **or** the second stays undispatched until release. Loop logs contain no `worktree.create`.
- [ ] Two beads both touching `.beads/` or `Cargo.lock` → serialized. Never parallel.
- [ ] `loop run` in a workspace with an extra linked worktree exits non-zero without `--allow-worktree`.
- [ ] `prototype` / `arena` arms write under `/tmp/flywheel-proto-*` only. No herdr worktree. No `br` in the arm.
- [ ] `rg 'Execute bead \{id\}'` in flywheel/src is empty. `bead_prompt` / `resume_prompt` do not exist as the old templates.
- [ ] `cargo test -p flywheel ledger::` and loop reopen tests green.
- [ ] No new `use flywheel` in toron crates. No new MCP tool. No new kind.

### Wave 2

- [ ] `flywheel principles list` prints 21 names. `show prove-it-works` prints the full rule, not a gist.
- [ ] Each Spec 6 row has a *code* or *lint* or *preflight* hook. A grep for the principle name in `profile.rs` / lint / preflight / stages hits.
- [ ] `feature` YAML without `data_shape` fails `workflow lint`.
- [ ] `bug-fix` YAML without a `repro` predecessor fails `workflow lint`.
- [ ] `prototype` with `n=1` fails lint.
- [ ] P2 bead report with empty PRINCIPLES is incomplete per reviewer contract (unit test on the checker, or a fixture).
- [ ] Skills and AGENTS.md point at `flywheel principles`. They do not duplicate the 21 leaves.

### Wave 3

- [ ] `flywheel workflow list` includes: bug-fix, feature, refactoring, prototype, arena, perf, hillclimb, forensics, babysit-bead, ship-beads, autonomous-work, pickup, pause, interrogate, figure-it-out.
- [ ] `flywheel workflow lint` over builtins is clean.
- [ ] Each new `decide` primitive has a unit test. `decide("repro the scroll drift", …).primitive == "bug-fix"`.
- [ ] Skill sync contract holds (Commands + robot-capabilities + AGENTS.md + flywheel skill).
- [ ] No builtin YAML mentions pull requests, Graphite, or `gh pr`.

### Wave 4+

- [ ] Loop reacts to `pane_agent_status_changed` without waiting the full settled interval (test or timed e2e).
- [ ] Exactly one loop body remains (`rg 'fn plan_actions'` has one definition that matters, or the sync path is a thin wrapper).
- [ ] Wave 5 either lands BeadRuntime or this ADR's More Information records "won't do" with the reader-load evidence.
- [ ] `reflect` produces a bead that adds a hook, not an AGENTS.md paragraph, on a fixture of two identical standing-order restatements.

---

## 8. Consequences

### Good

- Overnight `loop --work` becomes auditable. Morning artifact is `.flywheel/ledger.tsv`, not a transcript.
- One tree, one tracker, one cargo slot. No second context window pretending to be isolation.
- Captains steer with principle *names* that resolve to leaves and to lints.
- Crews get a playbook for the job in front of them (`bug-fix` vs `feature` vs `forensics`) instead of one `goal` sausage.
- Close lies stop draining the ready set.
- Toron surface stays frozen. Reservations stay the isolation floor.

### Bad

- Loop gains a close-detect + reopen path. More `br` subprocesses per tick.
- Independent verifier doubles model spend on P0/P1.
- Serialized overlap makes wall-clock longer than a fantasy of two worktrees. That cost is real. Tokens and merge tax were larger. We take the queue.
- Profile.behavior rewrites will churn prompts. Agents mid-flight during wave 2 need a respawn.
- `decide` table grows. First-match order becomes easier to get wrong. Tests are mandatory per row.

### Neutral

- steer / loop / workflow remain three nouns until wave 5.
- `debate` and `interrogate` both exist. Different jobs (2-brief synthesis vs N-model rubric).
- Standing orders are generated. Hand edits belong in the project layer (`<project>/.flywheel/standing-orders.md`) so a regenerate does not clobber fences.

### Risks and mitigations

| Risk | Mitigation |
|---|---|
| Agents learn to write a fake receipt | Spot-check: receipt must contain the VERIFY command's stdout markers; empty file is illegal. P0 still needs a second model. |
| Someone "just this once" worktrees a bead | Preflight fail on extra linked worktree. Brief FORBIDDEN. Loop will not ledger a foreign SHA. |
| Serialized overlap looks idle | Claim-watch must not nudge a waiting-on-overlap bead as if the owner is stuck. |
| Reopen storms | Idempotent reopen. Mail once per `(bead, sha)` missing-verdict. |
| VERIFY missing on old beads | Default refuse-to-dispatch + mail planner. Optional fallback to `scripts/verify-gates.sh` only for `docs`/`chore`. |
| Contention detector false positives on README | Reuse ADR-0011 non-trivial path rule (source, not docs/config). |
| Principle names become cargo-cult citations | Reviewer rejects a PRINCIPLES line with no decision. Same rule poteto-mode uses. |

---

## 9. Pros and Cons of the Options

### A — Skill overlay

- Good, because zero Rust.
- Bad, because it leaves self-reported close and the 2KB prompt. Fails `encode-lessons-in-structure` and `prove-it-works`.

### B — Ledger first, then principles, then playbooks (chosen)

- Good, because wave 1 is fail-closed on the real artifact.
- Good, because later waves hang on `Verdict` and the brief, not on more prose.
- Bad, because the program is multi-wave. Temptation to stop after wave 1 and never bake principles.

**Mitigation:** this ADR specifies waves 2–6 now. Wave 2 is not optional garnish. A captain who accepts this ADR accepts the bake.

### C — Collapse nouns first

- Good, because reader load drops.
- Bad, because it mixes a rewrite with the proof cut. Grilling rejected it as first increment.

### D — Events first

- Good, because latency.
- Bad, because a faster loop that trusts `br close` just lies faster.

### E — Fork pstack

- Good, because playbooks already written.
- Bad, because PR-shaped, Cursor-shaped, Graphite-shaped. Violates driver 1.

---

## 10. More Information

### Intention (the vision, in one place)

The system we want a year from now:

A captain types a target. `flywheel decide` returns one argv. If the target is a bug, the crew is forced through repro → red test → fix → ledger row. If it is a novel design, they are forced through 2–3 *scratch* prototypes, not repo clones. If it is ready beads, the loop drains them **in one tree**. A close that cannot show VERIFY stdout does not count. When two beads share a path, the second waits or the proposer splits the paths. When a captain says "apply prove-it-works," every agent already has that leaf and the receipt path. When the same human correction happens twice, `reflect` turns it into a lint and deletes the reminder.

That is pstack's fearless parallelism, on beads, on toron mail, on the loop we already run.

### Prior art

- pstack README + `skills/poteto-mode/SKILL.md` + 21 `principle-*` leaves (MIT, cursor/plugins).
- pstack playbooks: investigation, bug-fix, perf, hillclimb, forensics, feature, refactoring, prototype, babysit, shipping, autonomous-run, orchestrate, pickup, pause, multi-phase, figure-it-out, interrogate, arena, swarm.
- Grilling 2026-08-15 (this conversation): four locks in §2.3.

### Related ADRs

- flywheel [0001](../../flywheel/docs/decisions/0001-event-driven-agent-collaboration-spine.md) — event spine; wave 4 of this ADR.
- [0005](0005-orchestration-layer.md) — engine + builtins this ADR extends.
- [0010](0010-execution-wiring-model-registry-headless.md) — runner, role conditioning, headless.
- [0011](0011-coalition-orchestration.md) — isolation floor this ADR reuses.
- [0012](0012-standalone-cron-type.md) — cron stays a distinct type.
- [0015](0015-flywheel-toron-integration-contract.md) — boundary; this ADR adds a flywheel-side close guarantee.
- [0018](0018-agent-identity-naming-grammar.md) — ledger `actor` and brief identity.

### Revisit triggers

- Wave 5: if after wave 3 a new captain cannot find the work unit in 30 seconds.
- If `br` grows native custom fields, consider moving the ledger row into the bead. Until then the TSV stays.
- If we ever have external API users of flywheel's close semantics, the "no compatibility shim" driver changes. We do not today.

### Acceptance of this ADR

Status stays **proposed** until the captain flips it. Implementation of wave 1 may start under a proposed ADR as beads, but merge-to-main of the ledger path should wait for **accepted** or an explicit captain override.

When accepted, create the bead graph: one epic per wave, children matching the verification checkboxes. Beads, not PRs.
