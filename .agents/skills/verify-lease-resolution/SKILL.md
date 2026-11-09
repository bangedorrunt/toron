---
name: verify-lease-resolution
description: Formally verify Toron's file-reservation coordinate resolution, fencing epochs, and SQLite liveness-extension mirror in crates/toron-core/src/mailbox/reservations.rs and reservations_core.rs. Use whenever changing lease acquisition, release, force-release, NIP-40 expiry, inbox-cursor liveness extension, or claim epoch fencing.
---

# Verify Lease Resolution & Fencing Epochs (`toron`)

Synced to commit: `515e12819832fd979628dcb166b47e865b50294c`

This skill verifies the state machine that merges Nostr kind-`30031` coordinate replay with the SQLite `file_reservations` mirror and `inbox_cursors` liveness oracle in `crates/toron-core/src/mailbox/reservations.rs`.

## Properties Verified

### 1. `SingleExclusiveOwnerPerPath`
- **Plain statement**: At most one agent can hold an active exclusive lease on a `(project_slug, path_pattern)` coordinate at any time.
- **TLA+ invariant**: `Cardinality(ActiveOwners) <= 1`, where `ActiveOwners == BusActiveOwners \cup LivenessExtendedOwners`.
- **Rust correspondence**: `Mailbox::resolved_leases` (`crates/toron-core/src/mailbox/reservations.rs:1361-1425`) and `Mailbox::active_reservations` (`reservations.rs:1558-1582`) must never emit two `ReservationStatus::Held` rows with `exclusive == true` for the same `path_pattern`.
- **Current status**: **Verified & Passing** on `ToronLeaseResolution.fixed.cfg`. In unpatched code at `515e1281` it was violated in 5 steps (`AcquireA -> ExpireBusLease -> LapseActivityA -> AcquireB -> RecordInboxPollA`) because:
  1. `project_lease_upsert` (`reservations.rs:2582-2616`) keys `file_reservations` on `(project_slug, path_pattern, agent_pubkey)` and never marks a superseded holder's expired row inactive when another agent acquires the path.
  2. `resolved_leases` (`reservations.rs:1401-1409`) checks `let dup = out.iter().any(|l| l.agent_pubkey == pubkey && l.path_pattern == path && !l.expired);` (filtering by `l.agent_pubkey == pubkey` instead of path/coordinate ownership), so when `A` later polls its inbox (`record_inbox_poll`), `A`'s expired SQLite mirror row is re-materialized as `liveness_extended: true` alongside `B`'s live bus lease.
  *Fix*: Added `conflict` check against existing `out` leases in `resolved_leases` and marked superseded rows inactive on exclusive upsert in `project_lease_upsert`. Regression witness: `mailbox::reservations::tests::liveness_extension_never_supersedes_live_exclusive_holder`.

### 2. `LivenessExtensionPreservesEpoch`
- **Plain statement**: Extending an expired lease via `durable_live` (`inbox_cursors`) must preserve the fencing epoch the holder claimed at `d = <project_slug>:<path>`.
- **TLA+ invariant**: `(bus_status = "Held" /\ bus_expired /\ bus_holder \in LivenessExtendedOwners) => (ResolvedEpoch(bus_holder) = bus_epoch)`.
- **Rust correspondence**: `Mailbox::resolved_leases` (`reservations.rs:1410-1422`) and `Mailbox::check_claim_epoch` (`reservations.rs:1660-1688`).
- **Current status**: **Verified & Passing** on `ToronLeaseResolution.fixed.cfg`. In unpatched code at `515e1281` it was violated in 4 steps (`AcquireA -> ForceRevokeHolder -> AcquireA -> ExpireBusLease`) because `resolved_leases` (`reservations.rs:1419`) hardcodes `epoch: 0` on `liveness_extended` rows, causing `check_claim_epoch` / `release_fenced` / `renew_fenced` to reject a live holder presenting `epoch >= 1` with `stale claim epoch: presented 1, current 0`.
  *Fix*: Resolved event from bus replay or directly from `bus_events WHERE event_id = ?1` to restore the original fencing epoch. Regression witness: `mailbox::reservations::tests::liveness_extended_lease_preserves_fencing_epoch`.

## Coverage Record

| Code Symbol / Step | File & Lines | TLA+ Action / Operator |
|---|---|---|
| `Mailbox::file_reservation_paths` | `crates/toron-core/src/mailbox/reservations.rs:489-634` | `AcquireA`, `AcquireB` |
| `Mailbox::release_file_reservations` | `crates/toron-core/src/mailbox/reservations.rs:923-985` | `ReleaseHolder` |
| `Mailbox::force_release_file_reservation` | `crates/toron-core/src/mailbox/reservations.rs:992-1102` | `ForceRevokeHolder` |
| `Mailbox::resolve_lease_state` & `lease_event_order` | `crates/toron-core/src/mailbox/reservations.rs:1324-1355, 6235-6246` | `BusActiveOwners` |
| `Mailbox::resolve_leases_from_events` | `crates/toron-core/src/mailbox/reservations.rs:1430-1491` | `ExpireBusLease`, `BusActiveOwners` |
| `Mailbox::resolved_leases` | `crates/toron-core/src/mailbox/reservations.rs:1361-1425` | `LivenessExtendedOwners`, `ActiveOwners`, `ResolvedEpoch` |
| `Mailbox::check_claim_epoch` | `crates/toron-core/src/mailbox/reservations.rs:1660-1688` | `LivenessExtensionPreservesEpoch` |
| `Mailbox::project_lease_upsert` & `project_lease_status` | `crates/toron-core/src/mailbox/reservations.rs:2582-2646` | `mirror_expired_active`, `mirror_epoch` |
| `read_activity_snapshot` | `crates/toron-core/src/mailbox/lease_liveness.rs:55-88` | `LapseActivityA`, `RecordInboxPollA`, `durable_live` |

## How to Run the Verification

### 1. Model Check the TLA+ Specification with `/statechart`

Copy `.agents/skills/verify-lease-resolution/models/ToronLeaseResolution.tla`, `ToronLeaseResolution.cfg` (or `ToronLeaseResolution.fixed.cfg` when verifying a candidate fix), and `ToronLeaseResolution.view.ts` to a scratch directory outside the repo (never inside the workspace tree, which `scripts/verify-workspace-clean.sh` forbids), run the `/statechart` skill on `ToronLeaseResolution.tla`, and read every result with the `/statechart` skill:

- With `ToronLeaseResolution.cfg` (`FixSplitBrainAndEpoch = FALSE`, matching the unpatched code at `515e1281`), TLC explores 33 states and reports the 4-step `LivenessExtensionPreservesEpoch` counterexample and the 5-step `SingleExclusiveOwnerPerPath` counterexample.
- With `ToronLeaseResolution.fixed.cfg` (`FixSplitBrainAndEpoch = TRUE`), TLC explores all reachable states with 0 invariant violations and `NoDeadlock` holding.

### 2. Run the Hermetic Rust Test & CLI Replay

Always obey the `AGENTS.md` Test Sandbox rules (deny-shims for `launchctl` and `systemctl` first on `PATH`, temporary `HOME`/`XDG_CONFIG_HOME`/`XDG_DATA_HOME`, explicit `TORON_MCP_DATA_DIR` distinct from `XDG_DATA_HOME/toron`, `unset TORON_MCP_URL`, and `mbx` prefix):

```bash
mbx test -p toron-core --lib mailbox::reservations
```

To replay the 5-step `SingleExclusiveOwnerPerPath` counterexample against the compiled `toron` binary in an isolated temporary directory:

```bash
SBX="$(mktemp -d)"
mkdir -p "$SBX/bin" "$SBX/home" "$SBX/data" "$SBX/cfg" "$SBX/isolated"
printf '#!/bin/sh\nexit 1\n' > "$SBX/bin/launchctl" && chmod +x "$SBX/bin/launchctl"
printf '#!/bin/sh\nexit 1\n' > "$SBX/bin/systemctl" && chmod +x "$SBX/bin/systemctl"
unset TORON_MCP_URL
export PATH="$SBX/bin:$PWD/target/debug:$PATH"
export HOME="$SBX/home" XDG_CONFIG_HOME="$SBX/cfg" XDG_DATA_HOME="$SBX/data"
export TORON_MCP_DATA_DIR="$SBX/isolated" TORON_DATA_DIR="$SBX/isolated"

toron agent bootstrap --project p --as RedFox --program claude-code --model opus --json >/dev/null
toron agent bootstrap --project p --as BlueLake --program claude-code --model opus --json >/dev/null
toron reserve acquire --project p --as RedFox --paths src/lib.rs --exclusive --ttl-secs 1 --json 2>/dev/null
sleep 2
sqlite3 "$SBX/isolated/index.sqlite" "DELETE FROM inbox_cursors;"
toron reserve acquire --project p --as BlueLake --paths src/lib.rs --exclusive --ttl-secs 60 --json 2>/dev/null
toron mail inbox --project p --as RedFox --json >/dev/null
toron reserve list --project p --json | jq -c '.reservations'
rm -rf "$SBX"
```

## Assumptions, Bounds, and Limitations

- **Bounds**: Models 2 agents (`"A"`, `"B"`), 1 coordinate (`d`), and fencing epochs `0..MaxEpoch` (`MaxEpoch = 1`).
- **Abstractions**: Symmetric-glob overlap (`pattern_matches`) is abstracted to a single canonical coordinate `d`. Same-second event ordering is folded into `BusActiveOwners` (since `lease_event_order` was fixed in `af22c21d` to compare `(created_at, lease_epoch, KeyPriority, id)`).
- **What a passing check does NOT prove**: It does not verify SQLite migration syntax, Nostr Schnorr signature verification, or cross-project `../<repo>/...` path translation (`active_reservations_escaping_into`).

## When to Update This Skill

Update `models/ToronLeaseResolution.tla` and the coverage table above whenever:
- `Mailbox::resolved_leases`, `resolve_leases_from_events`, or `resolve_lease_state` changes how expired `file_reservations` rows merge with kind-`30031` events.
- `file_reservations` schema or `project_lease_upsert` / `project_lease_status` changes how `epoch` or `status` is persisted.
- `read_activity_snapshot` (`lease_liveness.rs`) changes the SQLite liveness predicate.

## Not Covered & Related Skills

- **Not covered**: Workflow effect journal lease claiming (`crates/toron-core/src/index/workflow_effects.rs`), multiplexer backend fallback latch (`crates/toron-plane/src/backend/fallback.rs`), and Lean 4 delivery/wake proofs (`proto/Proto/`).
- **Related skills**: `/statechart` (for running TLC and rendering statecharts), `/maintain-formal-verification-skill` (for updating this skill when `reservations.rs` evolves).
