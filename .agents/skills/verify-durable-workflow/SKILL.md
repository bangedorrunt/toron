---
name: verify-durable-workflow
description: Formally verify Toron durable workflow execution, dependency resolution (needs DAG), park/delay resumption, and operator approval lifecycle in crates/toron-workflow and crates/toron-core/src/index/workflow_effects.rs. Use whenever changing DAG scheduling, node state transitions, park/resume semantics, or approval gates.
---

# Verify Toron Durable Workflow DAG & Approval Lifecycle

This skill formally verifies the durable workflow execution semantics governed by ADR-0032, ADR-0036, and implemented in `crates/toron-workflow` and `crates/toron-core::index::workflow_effects`.

## Properties Verified

### 1. `NeedsPrecedeExecution` (DAG Topological Soundness)
- **Plain statement**: No workflow node can be dispatched, parked, or settled unless all upstream dependencies declared in `needs` have reached `Settled`.
- **TLA+ invariant**:
  ```tla
  \A n \in Nodes :
      (node_state[n] \in {"Executing", "Parked", "AwaitingApproval", "Settled"}) =>
          (\A d \in Needs(n) : node_state[d] = "Settled")
  ```

### 2. `ParkedRequiresWakeBeforeSettled` (Durable Park Invariant)
- **Plain statement**: A parked node (`ParkDecision::Park`) represents an interruption, not completion or failure. It must wake and re-enter execution before settling.
- **TLA+ invariant**: `\A n \in Nodes : node_state[n] = "Parked" => n = "fetch"`.

### 3. `AwaitingApprovalRequiresGate` (Human-in-the-Loop Gate)
- **Plain statement**: Operator approval is required for gated steps; unapproved steps cannot settle.
- **TLA+ invariant**: `\A n \in Nodes : node_state[n] = "AwaitingApproval" => n = "approve"`.

### 4. `TerminalSuccessRequiresAllSettled` & `TerminalFailureHasFailedNode`
- **Plain statement**: A workflow transitions to `Succeeded` if and only if all nodes settled, and to `Failed` only upon a node failure.
- **TLA+ invariant**: `phase = "Succeeded" => (\A n \in Nodes : node_state[n] = "Settled")`.

## Verification Instructions

Run the statechart verification CLI:

```bash
cd .agents/skills/verify-durable-workflow/models
~/.gemini/skills/statechart/scripts/statechart DurableWorkflow.tla --config DurableWorkflow.cfg --view DurableWorkflow.view.ts --out out
```

Run Rust regression tests:
```bash
mbx test -p toron-workflow --test graph_lifecycle
mbx test -p toron-core --lib workflow_effects::tests
```
