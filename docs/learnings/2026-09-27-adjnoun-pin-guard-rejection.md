---
track: bug
module: toron-cli/src/identity.rs
problem_type: integration_issue
tags: [identity, guard, naming-grammar, commit]
bead: bd-l2cb
created: 2026-09-27
---
# A hyphenated pin is rejected by the guard, and the other three repos accept it

## Symptoms

Committing a docs fix in `toron` failed twice, then succeeded only after the pin
was respelled:

```
toron: strict guard: unattributed bead-tagged hunks — ["bd-l2cb"] appear in the
staged diff but not in the commit message
toron: strict guard blocked staged paths: docs/guides/check.mjs (no active reservation)
toron: error: agent name swift-otter is not routable for project toron — swarm
projects use flywheel-toron-<role><seq> or a REGISTERED adjective+noun pin
(ADR-0018 D1a/D3)
```

The same `swift-otter` pin, the same guard, the same command, committed without
complaint in `flywheel`, `beads`, `chiebukuro`, and `toron.dev` in the same
session. Only `toron` refused it.

## What didn't work

**Treating the first block as a reservation problem.** The guard said
`no active reservation`, so the obvious read was that `toron reserve acquire`
had not been run for these paths. It had not, and running it under the same pin
failed with `agent swift-otter has no local key in project toron`. The missing
reservation was a symptom; the missing *identity* was the cause.

**Reserving per-path before diagnosing.** The second block listed four files,
which reads like a glob problem. It was not: the guard checks each staged path
against the lease table, and the lease table was empty because the acquire had
never succeeded. Reserving more paths would have produced the same error.

**Assuming one pin works everywhere.** `toron agent bootstrap --as swift-otter`
succeeded in three repos and was rejected in the fourth. Nothing in the error
said "wrong shape", it said "not routable", which sends you looking for a
missing registration rather than a malformed name.

## Solution

**The routable pin is CamelCase adjective+noun.** ADR-0018 D1a makes the
registered adj+noun pin (`DustySparrow`, `TealDune`, `SwiftOtter`) the
canonical mail identity, and D1a.3 reverses the earlier demotion so pins *are*
routing targets while unregistered free strings stay rejected. `swift-otter` is
a correct adjective and a correct noun, joined by a hyphen, so the grammar does
not match and registration is refused.

`toron agent bootstrap --project toron --as SwiftOtter` registers it, and the
commit then passes the guard on the first attempt.

The failure is loud and the remedy is one command, so the sharp edge is not the
rejection. It is that three of the four sibling projects accept the hyphenated
spelling, so a pin that has been working all session fails on the fourth repo
for a reason that has nothing to do with the work.

## Why this works

`toron` is the project where ADR-0018 lives, so it is the project that enforces
the grammar on the pin that ADR-0018 governs. The other repos resolve a pin by
key file and never consult the naming grammar, which is why they accept both
spellings. Enforcement follows the decision, not the other way round.

This compounds `F1/F4` in the same file, which recorded that register and whois
resolve by different paths. The shape is the same from the other side: two
resolution paths, one strict, and the strict one is the one that decides
whether a commit lands.

## Prevention

- **Pick the CamelCase adj+noun spelling for a new pin, in every project.** The
  hyphenated form is not a portable pin; it works until the project that owns
  the naming grammar sees it.
- **Read a `not routable` error as a name-shape problem first**, before looking
  for a missing registration. `unknown agent name` means register; `not
  routable` means the name does not satisfy the grammar.
- **When a guard blocks in one repo but not its siblings, suspect the repo's own
  ADRs before the work.** The stricter repo is usually the one that owns the
  rule.
- One pin, one key, still holds: `bootstrap` upserted onto the existing key
  rather than minting a second one, which is the semantics Sev2/F1 recommends.
