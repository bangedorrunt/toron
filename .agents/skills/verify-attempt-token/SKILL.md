---
name: verify-attempt-token
description: Formally verify cross-stack AttemptToken(bead_id, status_revision, pin) coordination, fencing epoch bounds, and clean reopen revocation across beads, flywheel, and toron. Use whenever changing attempt token construction, status revision bumps, reservation reason stamping, or release-by-reason epoch fencing.
---

# Verify AttemptToken & Cross-Stack Fencing (`toron` x `flywheel` x `beads`)

This skill formally verifies the cross-stack coordination invariant connecting:
1. `beads`: issue `status_revision: u64` monotonically increasing with each status change and reopen.
2. `flywheel`: dispatching attempts stamped with `AttemptToken(bead_id, status_revision, pin)` and deriving observations via `BeadObservation`.
3. `toron`: file coordinate reservations stamped with `reason = bead_id` and fenced by `epoch = status_revision`, revoked on reopen via `release_by_reason_up_to_epoch`.

## Properties Verified

### 1. `SingleExclusiveLease`
- **Plain statement**: At most one agent holds an active exclusive lease on a file coordinate at any time.
- **TLA+ invariant**: `Cardinality(active_leases) <= 1`.
- **Rust correspondence**: `Mailbox::file_reservation_with_token` and `reservations_core::tick` enforce symmetric conflict checks.

### 2. `NoStaleAttemptHoldingLease`
- **Plain statement**: No active lease can outlive a reopen. When a bead is reopened ($R \to R+1$), prior leases with epoch $\le R$ are revoked.
- **TLA+ invariant**: `\A l \in active_leases : l.epoch >= status_revision`.
- **Rust correspondence**: `Mailbox::release_by_reason_up_to_epoch` in `crates/toron-core/src/mailbox/reservations.rs`.

### 3. `ActiveLeaseMatchesAttempt`
- **Plain statement**: Any active lease held during in-progress execution strictly matches the currently dispatched attempt agent and current status revision.
- **TLA+ invariant**: `(bead_status = "InProgress" /\ active_leases # {}) => (\A l \in active_leases : l.agent = current_attempt /\ l.epoch = status_revision)`.
- **Rust correspondence**: `AttemptToken::parse_stamp`, `file_reservation_with_token`.

## Verification Instructions

Run TLC model checker on the TLA+ specification:

```bash
cd .agents/skills/verify-attempt-token/models
tlc AttemptTokenFencing.tla -config AttemptTokenFencing.cfg
```

Run Rust regression tests:
```bash
mbx test -p toron-core --lib attempt_token_roundtrip_and_fencing
```
