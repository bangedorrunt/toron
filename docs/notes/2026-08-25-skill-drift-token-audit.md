# Skill Drift & Token-Friendliness Audit (ADR-0024)

> Evaluates the two canonical agent-facing skills — `toron` and `flywheel` —
> against the skill-judge rubric, then maps what drifts when ADR-0024 is
> accepted. Companion to `docs/decisions/0024-graph-first-repair-loops-bead-workflow.md`
> §7.5. Source of truth for the skill files is `~/.agents/skills/`.

## 1. Baseline scores (pre-ADR-0024)

Skill-judge grades eight dimensions over 120 points. Both skills were read in
full (`toron/SKILL.md` 218 lines + 6 references; `flywheel/SKILL.md` 274 lines
+ 8 references).

### toron (Nostr-native mailbox)

| Dimension | Score | Max | Notes |
| :--- | :--- | :--- | :--- |
| D1 Knowledge Delta | 18 | 20 | Kind registry, two-plane semantics, gift-wrap vs room vs doorbell, pin-vs-herdr-name, reservation intent/held/released, bd-* gotchas. Almost all expert-only. |
| D2 Mindset + Procedures | 13 | 15 | ELI5 room/doorbell/sealed-letter model; "never commit on behalf". Strong frameworks. |
| D3 Anti-Pattern Quality | 13 | 15 | "Do not gift-wrap a room post", "do not write kind 42", "never the username fallback" — each with reason. |
| D4 Spec Compliance | 11 | 15 | Description has WHAT/WHEN/keywords, but uses folded `>-` scalar (multiline). See §3. |
| D5 Progressive Disclosure | 14 | 15 | `Load` table with read/skip + "Do not load every reference". Clean. |
| D6 Freedom Calibration | 13 | 15 | Fragile ops (close/purge/force-release) are low-freedom with exact commands; steering is medium. |
| D7 Pattern Recognition | 8 | 10 | Tool pattern (tables, decision trees). |
| D8 Practical Usability | 13 | 15 | Exact commands, edge cases, error codes. |
| **Total** | **103** | **120** | **86% → B+** |

### flywheel (orchestrator)

| Dimension | Score | Max | Notes |
| :--- | :--- | :--- | :--- |
| D1 Knowledge Delta | 17 | 20 | `decide` table, wave-gate, "ready means both" VERIFY/PRINCIPLES, STRICT model registry, spawn/spawn-send semantics. |
| D2 Mindset + Procedures | 13 | 15 | "classify then run", "runtime SM — do not replace". |
| D3 Anti-Pattern Quality | 13 | 15 | "never bare `--to oc`", "no worktree", "never `--to coder1`". |
| D4 Spec Compliance | 11 | 15 | Same folded `>-` scalar issue. |
| D5 Progressive Disclosure | 14 | 15 | `Load` table with skip column; "Do not load CHANGELOG.md". |
| D6 Freedom Calibration | 13 | 15 | Dispatch/spawn are low-freedom (exact flags); decide/steer medium. |
| D7 Pattern Recognition | 8 | 10 | Process + Tool hybrid. |
| D8 Practical Usability | 13 | 15 | Dispatch/monitor/intervene tables with exact commands. |
| **Total** | **102** | **120** | **85% → B** |

**Verdict:** both are production-grade, near-optimal skills. The only
systematic deduction is D4 (frontmatter `description` scalar form). Neither
suffers the classic failures — no tutorials, no orphan references, no vague
warnings, no over-engineering (toron keeps CHANGELOG-free; flywheel keeps a
CHANGELOG but explicitly says "Do not load CHANGELOG.md").

## 2. Post-ADR-0024 drift (what moves, by layer)

The skill-judge three-layer model is the token-friendliness frame:

| Layer | Loads when | ADR-0024 impact |
| :--- | :--- | :--- |
| 1 metadata (name+description) | always | **0 tokens.** No new kind/tool/domain. Descriptions stay valid. |
| 2 SKILL.md body | after trigger | **~15–20 tokens per skill.** One-line command additions only. |
| 3 references/ | on demand | **~60–120 tokens** in `workflow-engine.md` + `workflow-authoring.md`, loaded only when authoring/debugging YAML. |

Full file × section map is in the ADR §7.5.2. The invariant to preserve: **the
new semantic weight (`on_error: repair`, `ErrorKind` routing, evidence payloads,
typed stages) lands entirely in Layer 3.** The Layer-2 bodies only gain the
command tokens so `flywheel workflow graph …` is visible to an agent that
triggered the skill, without loading the repair state machine it may not need.

## 3. The one concrete spec fix (do it in Wave 1)

Both descriptions use:

```yaml
description: >-
  Nostr-native agent mailbox (toron) — durable, …
```

The skill-maker validator (`scripts/validate-skill.ts`) rejects multiline
scalars — minimal YAML parsers only accept a single-line `description`. Collapse
to one line (token-neutral, content unchanged):

```yaml
description: Nostr-native agent mailbox (toron) — durable, cryptographically-signed coordination under agent swarms. 39 MCP tools + 25 resources on Nostr (Schnorr, NIP-17 gift-wrap, NIP-29 channels, @mention dispatch). Use when agents need peer mail, encrypted threads, file reservations, proofs, contacts, build slots, workflow automation, multi-agent debate, or captain-to-crew steering. Pairs with the flywheel skill (orchestration). Binary: ~/workspace/toron/target/debug/toron.
```

Same for flywheel. This is the only change that *must* happen for spec
compliance; the rest of the frontmatter (`license`, `domain`, `role`, `scope`,
`output-format`, `triggers`, `metadata`) is non-standard but inert — leave it
unless a validator rejects it (some harnesses read `triggers:`).

## 4. Token-waste guards (what NOT to add during drift)

1. **No repair state-machine copy.** The full state machine + ASCII flowchart
   live in the ADR §6.1. Skills need the YAML *shape* (`on_error: repair`,
   `max_cycles`, `repair_model`, `verify`) and the routing *rule*
   (Transient→retry, Logic→repair, Blocked/Fatal→escalate) — nothing else.
2. **No DAG diagram.** `flywheel workflow graph` renders it at runtime; a
   static copy in the skill is redundant and drifts.
3. **No duplicated routing table.** Engine semantics live once, in toron's
   `workflow-engine.md`. Flywheel's `workflow-authoring.md` links, doesn't copy.
4. **Don't restate "what is a repair loop".** Retry→fix→verify is in the model;
   only the toron-specific YAML fields and lint rules (R-R1..R5) are knowledge
   delta.
5. **Don't touch the 39-tool count or the catalog gate.** ADR-0024 adds no MCP
   tools and no toron CLI surface; `cargo test catalog_freshness` stays green.

## 5. Acceptance checklist for the skill edits (when ADR is accepted)

- [ ] `toron/references/workflow-engine.md`: `on_error: repair` + `ErrorKind` + typed stages + evidence payload, in the existing sections (no new sections).
- [ ] `flywheel/references/workflow-authoring.md`: orchestration delta only; links to `workflow-engine.md` for semantics.
- [ ] `flywheel/SKILL.md` §5: append `graph,tree,status,connect,resume,fork,cancel,approvals,approve,reject` to the `workflow {…}` token; §1 one-line note.
- [ ] `toron/SKILL.md` `## Workflow + @mention`: one line (graph-first + repair + taxonomy), pointing at the reference.
- [ ] `flywheel/references/QUICK-REFERENCE.md`: new `workflow *` verbs.
- [ ] `br` skill: `br create --auto-workflow` + `br close`-as-terminal-gate (Wave 3).
- [ ] Both `description:` frontmatter fields collapsed to single-line (§3).
- [ ] `AGENTS.md` CONVENTIONS: graph-first + `on_error: repair` clause.
- [ ] Layer-2 token delta ≤ 20 tokens per skill (sanity: diff the `wc -l`; no new top-level sections).
