// governed-by: ADR-0003, ADR-0007, ADR-0023

# ADR-0014: Kind Registry Specification

**Status:** Accepted (2026-08-14)
**Amends:** ADR-0003 (§ kind registry)
**Amended by:** [ADR-0023](0023-nip29-group-metadata-kinds.md) (39000/39002 emit; 40/41/42 ingest-only)

## Context

ADR-0003 froze the kind registry ("no new kinds/tools without an ADR") but
never formally specified each kind's fields, lifecycle, d-tag semantics, or
deprecation policy. The registry has since grown to 10+ custom kinds across
4 functional areas. Without a formal spec:

- There is no validation target (a conformance test can only check names, not
  field/lifecycle correctness).
- Adding a kind requires reverse-engineering the codebase to understand
  conventions (d-tag = project slug; `status:` for reservations; etc.).
- Deprecation/removal has no documented migration path.

This ADR formalizes the registry as a specification, not just a frozen list.

## Decision

### Registry table

| Kind | Name | Area | d-tag | Status field | Lifecycle |
|------|------|------|-------|-------------|-----------|
| 30010 | Project manifest | Identity | project slug | — | static (kind-30010 create; immutable) |
| 30011 | Product manifest | Identity | product key | — | static; a-tag links member projects |
| 30013 | Agent profile | Identity | `project-slug:name` | — | addressable; latest per d-tag (code; not npub) |
| 30014 | Contact policy | Identity | agent npub | `open\|auto\|contacts_only\|block_all` | upsert (latest wins) |
| 30031 | File reservation | Reservation | `project:path-glob` (code) | `intent\|held\|released\|revoked` | addressable; latest wins; dedup per (kind, author, d-tag) |
| 7 | Reaction | Shared plane | — | — | NIP-25; `e`+`p`+`k`+`h` |
| 9 | Channel post | Shared plane | — | — | NIP-29 chat; `h`=channel, `t`=project, `p`=mentions |
| 39000 | Group metadata | Shared plane | channel id | `closed` | NIP-29; create/close; addressable (ADR-0023) |
| 39002 | Group members | Shared plane | channel id | — | NIP-29; `p`=members snapshot (ADR-0023) |
| 40 | Channel create (legacy) | Shared plane | channel id | — | NIP-28; ingest only |
| 41 | Channel metadata (legacy) | Shared plane | — | — | NIP-28 join/leave/close; ingest only |
| 42 | Channel post (legacy) | Shared plane | — | — | Pre-ADR-0020; ingest only |
| 43001 | Job request | Job | job id | — | request → 43002 (claim) → 43003 (progress) → 43004 (result) |
| 43002 | Job accepted | Job | job id | — | signed claim; first-claimer wins |
| 43003 | Job progress | Job | job id | — | optional; informational |
| 43004 | Job result | Job | job id | — | terminal success |
| 43005 | Job cancel | Job | job id | — | terminal cancel |
| 43006 | Job error | Job | job id | — | terminal error |
| 46011 | Workflow approval | Workflow | run id | — | operator approve (resumes suspended run) |
| 46012 | Workflow rejection | Workflow | run id | `status=failed` | operator reject OR step `on_error: fail_run` |

### Conventions (binding)

1. **`t` tag = project slug only.** Never harness/model/job tags. Violating
   this pollutes the project-scoped bus filter.
2. **`d` tag = the addressable coordinate.** For addressable kinds (30031),
   `d` = `<kind>:<author>` so dedup collapses renewals to the latest event.
3. **`status:` field on 30031** = the lifecycle state machine:
   `intent` (TTL-bounded ~5min) → `held` (live lease) → `released` (clean) |
   `revoked` (operator force-release). The conflict check splits two tiers:
   held=HARD conflict (blocked), intent=SOFT conflict (freeze+propose).
4. **`p` tag = mentioned npub.** On kind-14 DMs and kind-9 channel messages,
   the `@name` marker in content MUST be accompanied by a `p` tag with the
   mentioned npub — this is what the @mention dispatcher matches.
5. **Job lifecycle (43001-43006) is Buzz-verbatim.** First-claimer wins (43002
   is a signed claim). The kind is the only switch; frozen.
6. **No NIP-09 cross-key delete.** Force-release = operator `status:revoked`
   on a new 30031 event (not a delete of the holder's event).

### Adding a new kind (the process)

1. Write an ADR naming the kind number, its fields, d-tag semantics, lifecycle,
   and the `t`-tag convention.
2. Add it to the registry table above.
3. Add the kind constant to `crates/toron-core/src/nostr/kinds.rs`.
4. Add a conformance test verifying the kind's fields/lifecycle.
5. Update the skill (`~/.agents/skills/toron/references/mcp-surface.md`) and
   AGENTS.md.

### Deprecation

A deprecated kind stays in the registry table (marked **Deprecated**) with a
migration note. The kind constant stays in `kinds.rs` (never removed — Nostr
events are permanent). New events MUST NOT use deprecated kinds; the bus
filter MAY warn on ingest.

## Consequences

- The registry is now a validation target: a conformance test can verify every
  kind's fields and lifecycle against this spec.
- Adding a kind has a clear, documented process (no more reverse-engineering).
- The `status:` lifecycle on 30031 is formally specified (was implicit in code).
- Federation (currently off) has a contract to interop against when enabled.

## Verification

```bash
# Kind constants match the registry table
grep 'pub fn toron_kind' crates/toron-core/src/nostr/kinds.rs

# 30031 status field appears in conflict-check code
grep -r 'status.*intent\|status.*held\|status.*released\|status.*revoked' crates/toron-core/src/

# @mention dispatcher checks p tag
grep 'p.*tag\|mentioned' crates/toron-workflow/src/mention.rs
```
