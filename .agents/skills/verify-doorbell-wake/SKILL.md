---
name: verify-doorbell-wake
description: Formally verify Toron's level-triggered doorbell wake decision, backoff, suppression, and escalation state machine in crates/toron-core/src/wake.rs. Use whenever changing wake logic, ring count bounds, pane state checks, or escalation thresholds.
---

# Verify Toron Doorbell Wake Level-Triggered State Machine

This skill formally verifies the level-triggered wake protocol governed by ADR-0041 and implemented in `crates/toron-core/src/wake.rs`.

## Properties Verified

### 1. `BoundedWake` (ADR-0041 P6)
- **Plain statement**: The recipient cannot be rung indefinitely without cursor progress; rings are capped at `MAX_RINGS`.
- **TLA+ invariant**: `rings <= MaxRings`.

### 2. `NoRingWhenCaughtUp`
- **Plain statement**: If the recipient's cursor has caught up to the inbox head watermark (`head <= cursor`), no wake line is ever emitted.
- **TLA+ invariant**: `cursor >= head => decision = "NothingUnread"`.

### 3. `NoRingWhenUnbound` & `UnboundNeverRings`
- **Plain statement**: If binding is unverified, stale generation, or pane is gone, ringing is inhibited and the decision is `Unbound`.
- **TLA+ invariant**: `(~binding_ok /\ head > cursor) => decision = "Unbound"` and `~binding_ok => decision # "Ring"`.

### 4. `NoRingWhenBusy`
- **Plain statement**: A pane mid-turn (`Working` or `Blocked`) is never typed into for normal urgency, avoiding terminal input collisions.
- **TLA+ invariant**: `(binding_ok /\ head > cursor /\ rings < MaxRings /\ pane_state = "Working") => decision = "Busy"`.

### 5. `DeafAgentEscalates` & `NoRingWhenEscalated`
- **Plain statement**: When `rings >= MaxRings` without cursor advancement, the decision transitions strictly to `Escalate` (emitting `deaf{r, wm}` and notifying supervisor).
- **TLA+ invariant**: `(binding_ok /\ head > cursor /\ rings >= MaxRings) => decision = "Escalate"`.

## Verification Instructions

Run the statechart verification CLI:

```bash
cd .agents/skills/verify-doorbell-wake/models
~/.gemini/skills/statechart/scripts/statechart DoorbellWake.tla --config DoorbellWake.cfg --view DoorbellWake.view.ts --out out
```

Run Rust unit and property tests:
```bash
mbx test -p toron-core --lib wake::tests
```
