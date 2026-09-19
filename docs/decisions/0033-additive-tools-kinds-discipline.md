<!-- governed-by: ADR-0003, ADR-0032 -->
---
status: proposed
date: 2026-09-19
decision-makers: Captain (bangedorrunt)
consulted: Crew B (MistyCreek, bead bd-ncpt)
informed: flywheel x toron x beads maintainers
---

# ADR-0033: Additive MCP Tools and Kinds — Replacement Discipline for the Frozen Surface

**Status:** Proposed (bead bd-ncpt). Acceptance is a captain word on this file.

**Decision in one sentence:** The frozen surface stays frozen by default, but additive
MCP tools and Nostr kinds are allowed under a replacement discipline (ADR +
catalog regen + tests + versioning) instead of the current lock that only a
freeze-clause violation can open.

## Context

- ADR-0003 §"Nostr kind allocation — FROZEN REGISTRY" locks kinds (AGREEMENT
  LOCKED 2026-08-04): any renumber requires a new ADR revision.
- ADR-0032 "Does not amend" locks the MCP tool count, and its Forbidden zones
  list "kind registry, MCP tool names/count".
- The code already practices half the discipline: `tool_catalog()` rejects
  tools outside frozen-38 + `ADDITIVE_TOOLS` with "a new tool needs an ADR
  (ADR-0007 C3) and a catalog group", and `catalog_shapes_frozen` freezes all
  40 shapes + 25 resources in a versioned fixture. What is missing is the
  constitutional permission: today an additive tool is a freeze violation
  with good manners. bd-ncpt (judgment tool) needs a door, not anarchy.

## Decision

1. **Additive tools allowed with an ADR.** A new MCP tool lands iff (a) an ADR
   names it, (b) it is registered in `ADDITIVE_TOOLS` + a catalog group,
   (c) `docs/catalog/toron-mcp.json` is regenerated via `toron catalog --json`,
   (d) the shapes fixture is version-bumped (`tool-method-shapes-vN.json`,
   never edited in place), with `TOOL_COUNT`/`PARITY_TOOL_COUNT` updated to
   match, and (e) `catalog_freshness` + `catalog_shapes_frozen` + flag
   conformance stay green.
2. **Additive kinds allowed with an ADR.** A new Nostr kind lands iff (a) an ADR
   names it with allocation rationale, (b) it occupies a free block (never a
   renumber of a live value), and (c) `test_frozen_registry_values` +
   `test_standard_kinds_are_frozen` are extended to pin the new value.
3. **Renumber stays forbidden.** No live kind value ever changes; no parity
   (38) tool is renamed, reshaped, or removed under this discipline. Parity
   drift is still a freeze violation, full stop.
4. **Versioning.** Every addition bumps the shapes fixture version and records
   a CHANGELOG entry. The fixture history (`v1`, `v2`, …) is the audit trail
   of what the surface was at each step.

## Amends

- ADR-0003 frozen-registry clause: "any renumber requires a new ADR revision"
  is narrowed to renumber; additive occupancy of free blocks requires an ADR
  under §2 instead.
- ADR-0032 "Does not amend: … MCP tool count" and Forbidden zones "MCP tool
  names/count": names/count may grow additively under §1. The runtime
  `FailureKind` spelling freeze and every other forbidden zone are untouched.

## Does not amend

The one-way dependency `flywheel → toron-workflow → toron-core`; the 38-tool
parity surface (names, shapes, count); live kind values; the frozen runtime
`FailureKind` spelling; beads as the work graph; `thread_id ≠ corr`;
`workspace::tick` and the Tick Law; `br gate report`/`br close` ownership;
the worktree prohibition.

## Consequences

- bd-ncpt lane 2 lands the first disciplined addition: `ask_judgment` (3rd
  additive; 38 parity + 3 additive = 41 total), fixture
  `tool-method-shapes-v2.json`, `TOOL_COUNT` 40 → 41.
- The next addition copies this file's §1 checklist; reviewers check the
  checklist, not the freeze.

## Tracker

Bead bd-ncpt (mail `thread_id`, reservation `reason`). Verification:
`mbx test catalog_freshness`, `mbx test -p toron-mcp catalog_shapes`,
`cargo test -p toron-core --lib mailbox::guard`, kind-registry tests.
