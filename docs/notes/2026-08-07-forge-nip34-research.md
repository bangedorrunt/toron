# Forge — NIP-34 Git Events as Channel-Scoped PRs (Research Note, bd-9o9)

// governed-by: ADR-0003, ADR-0005

> **Status:** RESEARCH EPIC (priority 3, Phase 3, deferred). The single biggest
> agent-collaboration feature toron lacks vs Buzz. Canonical bead: **bd-9o9**.
> Source: study doc item L3/12 + ADR-0003 §Forge (kind reservation).

## What this is

Buzz's VISION_PROJECTS: push → channel created for branch → CI agent picks up → posts
results → reviewer approves (signed event) → merge → channel archives. Every git
push/CI/approval/merge becomes a **signed event in a channel**. toron reserved the
kind numbers in ADR-0003 §Phase 3 but shipped nothing.

## CRITICAL correction — the kind map in bd-9o9 / ADR-0003 is STALE

The bead and ADR-0003 §Forge say:

> push (`30618`) → CI-passed (`1630`) → approval (`46011`) → merge (`1631`);
> repo = `30617`, project = `30621` (groups repos via `a`).

The **canonical NIP-34** (fetched from `nostr-protocol/nips/34.md`, 2026-08-07) assigns
these kinds DIFFERENTLY:

| Kind | bd-9o9 / ADR-0003 claim | **Canonical NIP-34** |
|---|---|---|
| `30617` | repo announcement | **Repository announcements** ✓ (matches) |
| `30618` | push | **Repository state announcements** (branch/tag refs, `refs/<heads\|tags>/...`, `HEAD`) |
| `30621` | project (groups repos) | **Git commit** (not a project/grouping kind) |
| `1630` | CI-passed | **Status: Open** |
| `1631` | merge | **Status: Applied/Merged** (patches) / Resolved (issues) |
| `46011` | approval | toron custom (ADR-0003, not NIP-34) |
| `1617` | (not mentioned) | **Patches** (git format-patch content) |
| `1618` | (not mentioned) | **Pull Requests** |
| `1619` | (not mentioned) | **PR Updates** |
| `1621` | (not mentioned) | **Issues** |
| `1632` | (not mentioned) | **Status: Closed** |
| `1633` | (not mentioned) | **Status: Draft** |

There is **no "CI-passed" kind in NIP-34**. `1630` is the generic "Open" status. A CI
result would be a toron custom kind (or reuse `46001–46012`), NOT `1630`. The bead's
map conflated Buzz's proprietary semantics with NIP-34's.

### What this means for the build

1. **Verify-before-reserve is done:** `30617`/`30618`/`30621` are standard NIP-34
   kinds (already assigned by the protocol — "free" in the sense that toron has not
   shipped them, but they are NOT toron-custom). `1617`/`1618`/`1619`/`1621`/
   `1630`–`1633` are also standard. **Only `46011` (approval) is toron-custom** and it
   is already in the `46001..=46012` registry range (`crates/toron-core/src/nostr/kinds.rs`).
2. **No new kind needed for the core flow** — NIP-34 gives patches/PRs/issues/status
   for free. toron would add at most a CI-result kind (CI status lives outside NIP-34).
3. **Correct channel flow:** push detection via kind-30618 (branch ref change) →
   auto-create NIP-29 channel → PR = kind-1618 (`a`-tagged to the repo 30617) →
   CI result posted to channel (toron kind) → reviewer approves via kind-46011 →
   merge via kind-1631 status (with `merge-commit` tag) → channel archive.
4. `30621` is NOT a project-grouping kind — grouping repos "via `a`" is done by the
   repo-owner pubkey in the `a` tag address, not by 30621.

## Scope (corrected)

- NIP-34 kind adoption: `30617` (repo announce), `30618` (state), `1618` (PR),
  `1619` (PR update), `1630–1633` (status), `1617` (patch), `1621` (issue).
  Add to `kinds.rs` as `pub const` + registry (ADR-0003 invariant 1: no NEW kind —
  these are adopting standard kinds, which is allowed; the registry gains entries).
- **`git post-receive` hook** → publish kind-30618 state event (branch moved).
- **Channel-per-branch auto-creation** on 30618 (NIP-29 channel).
- **CI-result ingestion** → toron custom kind in `46001–46012` (or a CI-status event)
  → posted to the channel.
- **Approval** → kind-46011 → triggers **merge** (kind-1631 status event).
- **Channel archive** on merge/close.

## Why deferred (unchanged from ADR-0003)

- flywheel does git orchestration today via filesystem + herdr, not signed events.
- The Nostr value = cross-machine visibility + channel-as-PR UX + federated audit.
- git is already signed/auditable; the incremental value is the collaboration UX.
- Large build. Decide ATC vs Forge as the next long bet (study doc item 11/12) —
  not both at once.

## Open questions

1. **CI-result kind:** no NIP-34 standard. Use a toron custom kind in the
   `46001–46012` workflow range, or a fresh reservation (needs ADR-0003 change since
   "no new kind" freezes the registry)? Recommendation: reuse `46001–46012` (a
   workflow/CI run is conceptually a workflow run), avoiding a registry change.
2. **Patch vs PR for the 60KB threshold** (NIP-34: patches < 60KB, PRs otherwise):
   support both or PR-only? Recommendation: PR-only initially (matches Buzz UX;
   patches are a format-patch subtlety).
3. **Merge execution:** does toron merge (git plumbing on the host) or does the
   approval just emit kind-1631 and leave merge to a CI/host agent? The latter is
   safer and more honest about blast radius.
4. **Channel archive semantics:** NIP-29 has no formal archive; "archive" = channel
   closure event + thread freeze. Confirm the exact mechanism when building.

## Recommendation

Keep as a Phase 3 research epic. The single most valuable correction from this pass:
**the kind map must be rebuilt against canonical NIP-34 before any code.** Do NOT
reuse the bd-9o9 / ADR-0003 §Forge kind map verbatim — `1630` is Open-status, not
CI-passed; `30621` is a git commit, not a project.

---

*Lane OC-C (BlackMarsh), 2026-08-07. Research only — no code shipped.*
