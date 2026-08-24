---
status: accepted
date: 2026-08-24
decision-makers: Captain (bangedorrunt)
consulted: nostr NIP-29 kinds 39000/39002; ADR-0020 Spec 1 wave-2 note
informed: flywheel maintainers; toron skill owners
---

# ADR-0023: Adopt NIP-29 Group Metadata (39000/39002); Retire 40/41 to Ingest-Only

**Status:** Accepted (2026-08-24)
**Extends:** [ADR-0020](0020-nostr-two-plane-cli-and-crew-steering.md) wave 2, [ADR-0014](0014-kind-registry-specification.md)
**Amends:** ADR-0020 Spec 1 (create/join wire), ADR-0014 registry table
**Does not amend:** MCP tool names/count, kind 9 posts, kind 7 reactions, plane-P gift wrap, federation default
**Tracker:** `bd-zb0y`

## Context and Problem Statement

ADR-0020 restored kind **9** for channel posts and deferred group metadata:

> A later ADR may emit 39000/39002 so `nak req -k 39000` lists rooms.

Create/join/leave/close still emit NIP-28 kinds **40/41**. Comments call them NIP-29. Kind 40 is regular (relays keep every create); `d` is ignored. Real NIP-29 metadata is addressable **39000** (`d`=group id) and member list **39002**.

How do we emit spec-correct group metadata without a NIP-29 relay and without the 9000–9008 admin set?

## Decision Drivers

* Kind freeze: adopt standard NIPs the same way 7 and 9 were adopted. No invented 4xxxx kinds.
* ChannelStore stays the membership gate (ADR-0020).
* Cross-process rebuild must keep working (`rebuild_store`).
* No 40th MCP tool. No full NIP-29 admin (9000–9008, 9021, 39001, 44100).
* Legacy 40/41/42 ingest so existing rooms rebuild.

## Considered Options

* **A — Emit 39000 (create/close) + 39002 (member snapshot); 40/41 ingest-only (CHOSEN)**
* **B — Keep 40/41 forever, docs-only “NIP-29”** — rejected; wire would stay NIP-28.
* **C — Full NIP-29 admin (9000–9008) + embedded group relay** — rejected this ADR; needs a real NIP-29 relay.

## Decision Outcome

Chosen option: **A**.

### Emit

| Action | Kind | Shape |
|---|---|---|
| create / metadata / close | **39000** | addressable; `d`=`h`=channel id; `name`; `private`; `t`=project; close adds `closed`/`true` |
| join / leave | **39002** | addressable; `d`=`h`; `p`=each current member; `t`=project. Signed by the actor after ChannelStore mutates. Rebuild takes the newest 39002 (any author, NIP-01 order) as the member set. |
| post | **9** | unchanged |
| react | **7** | `e`+`p`+`k`+`h` |

### Ingest

Canonical: 39000, 39002. **Legacy:** 40 (create), 41 (join/leave/close content), 42 (posts). `rebuild_store` applies 40/39000 first, then 41 deltas, then overlays the newest 39002.

### Purge

Unchanged: local bus deletion + audit log. Not NIP-09, not kind 9008.

### Non-goals

9000–9008, 9021, 39001, 44100, federation of plane S, sender self-wrap, NIP-46.

## Implementation Plan

* **Affected paths:** `crates/toron-core/src/nostr/kinds.rs`, `crates/toron-workflow/src/channels.rs`, `crates/toron-cli/src/plane.rs`, `crates/toron-e2e/tests/sc13_channel_membership.rs`, `docs/decisions/0014-kind-registry-specification.md`, `docs/decisions/README.md`, AGENTS.md, `~/.agents/skills/toron/SKILL.md`
* **Patterns:** same CLI nouns; ChannelStore gate unchanged; `build_event` for wire events.
* **Migration:** no live rewrite. Next `channel list`/`rebuild_store` reads legacy 40/41.

### Verification

- [x] `CHANNEL_CREATE` / group metadata emit == 39000
- [x] join/leave emit == 39002 (`p` tags + JSON member list in content)
- [x] close emit == 39000 with `closed`
- [x] rebuild from 40/41 still constructs the room
- [x] SC-13 green; `cargo test -p toron-cli --test cli_channels_lifecycle` green
- [x] no new MCP tools; catalog CLI nouns unchanged

## Consequences

* Good: `nak req -k 39000` lists rooms; 39000 is addressable so create/close latest-wins.
* Good: 40/41 comments stop lying about NIP-29.
* Neutral: 39002 signed by the joiner (not a group relay) is a toron projection snapshot, not a hosted NIP-29 relay.
* Bad: two ingest eras until 40/41 are deleted in a later ADR.

## More Information

Captain confirmed 2026-08-24: hygiene pack + this ADR; sender self-wrap waits for federation.
