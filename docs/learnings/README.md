# Learnings corpus (ADR-0009)

One learning per file, git-tracked, `rg`-able. This is the compound-engineering **return
arrow**: `ce-brainstorm` / the wave GATE / the brief's LEARNINGS field read it before work;
the `ce-compound` gate writes to it after verified work; `ce-sweep` maintains it.

## The gate (all three must hold — completion alone never qualifies)

1. **Non-obvious** — the reasoning is not recoverable from the final code, tests, types, or
   comments.
2. **Durable** — an invariant, root cause, rejected approach, or convention useful beyond the
   exact diff.
3. **Material** — forgetting it would plausibly cause recurrence or substantial rediscovery.

Counterfactual: if the doc vanished, would the next crew redo this investigation? If not,
`clean_skip`.

## Schema

```markdown
---
track: bug            # bug | knowledge — decides the sections
module: src/crew.rs   # open vocabulary, reuse what the corpus already uses
problem_type: integration_issue
tags: [dispatch]
bead: bd-xxxx         # provenance
created: 2026-09-06
---
## Symptoms            # bug track: Symptoms / What Didn't Work / Solution /
## What didn't work    #         Why This Works / Prevention
## Solution
## Prevention
# knowledge track instead: Context / Guidance / When to Apply
```

- High overlap with an existing entry ⇒ UPDATE it (stamp `last_updated`), never duplicate.
- A learning contradicting standing-orders/AGENTS.md guidance **outranks ordinary staleness** —
  resolve by which side current code follows; report a wrong guidance file, never edit it here
  (`ce-sweep` owns that).
- The learning ships in the SAME commit as the work that produced it.

## Maintenance

`flywheel workflow run ce-sweep --execute --inputs '{"project":"<slug>"}'` —
Keep / Update / Consolidate / Replace / Delete, evidence per verdict.
