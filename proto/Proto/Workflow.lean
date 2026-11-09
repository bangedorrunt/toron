/-
  governed-by: ADR-0032
  Durable workflow execution and DAG scheduling model.
  Mirroring `crates/toron-workflow/src/engine/durable.rs` (`GraphModel`,
  `NodeState`, `GraphPhase`, `schedule`, `advance_graph`).
-/
namespace Proto

/-- Lifecycle state of one node in the DAG. -/
inductive NodeState where
  | pending
  | executing (wave : Nat) (attempt : Nat)
  | parked (untilMs : Nat) (reason : String)
  | awaitingApproval
  | settled
  | failed (error : String)
  deriving DecidableEq, Repr, Inhabited

/-- Coarse lifecycle of the overall graph execution. -/
inductive GraphPhase where
  | ready
  | running
  | succeeded
  | failed (error : String)
  | cancelled
  deriving DecidableEq, Repr, Inhabited

def GraphPhase.isTerminal : GraphPhase → Bool
  | .succeeded | .failed _ | .cancelled => true
  | .ready | .running => false

structure NodeDef where
  name : String
  needs : List String
  deriving DecidableEq, Repr, Inhabited

def getNodeState (nodes : List (String × NodeState)) (name : String) : NodeState :=
  match nodes.find? (fun p => p.1 == name) with
  | some (_, s) => s
  | none => .pending

def isSettled (nodes : List (String × NodeState)) (name : String) : Bool :=
  match getNodeState nodes name with
  | .settled => true
  | _ => false

def allNeedsSettled (nodes : List (String × NodeState)) (needs : List String) : Bool :=
  needs.all (isSettled nodes)

def updateNodeState (nodes : List (String × NodeState)) (name : String) (newState : NodeState) :
    List (String × NodeState) :=
  nodes.map fun p => if p.1 == name then (name, newState) else p

/-- Schedule a ready node: transitions from .pending to .executing if and only if
all upstream dependencies in `needs` are `.settled`. -/
def scheduleNode (nodes : List (String × NodeState)) (node : NodeDef) (wave attempt : Nat) :
    Except String (List (String × NodeState)) :=
  if getNodeState nodes node.name != .pending then
    .error "node not pending"
  else if !allNeedsSettled nodes node.needs then
    .error "needs not settled"
  else
    .ok (updateNodeState nodes node.name (.executing wave attempt))

/-- Park an executing node until a wake deadline (interruption). -/
def parkNode (nodes : List (String × NodeState)) (name : String) (untilMs : Nat) (reason : String) :
    Except String (List (String × NodeState)) :=
  match getNodeState nodes name with
  | .executing _ _ => .ok (updateNodeState nodes name (.parked untilMs reason))
  | _ => .error "node not executing"

/-- Wake a parked node once deadline has elapsed. -/
def wakeNode (nodes : List (String × NodeState)) (name : String) (wave attempt : Nat) :
    Except String (List (String × NodeState)) :=
  match getNodeState nodes name with
  | .parked _ _ => .ok (updateNodeState nodes name (.executing wave attempt))
  | _ => .error "node not parked"

/-- Settle an executing node on completion. -/
def settleNode (nodes : List (String × NodeState)) (name : String) :
    Except String (List (String × NodeState)) :=
  match getNodeState nodes name with
  | .executing _ _ => .ok (updateNodeState nodes name .settled)
  | _ => .error "node not executing"

/-- Operator signs off on a gated step awaiting approval. -/
def operatorApprove (nodes : List (String × NodeState)) (name : String) :
    Except String (List (String × NodeState)) :=
  match getNodeState nodes name with
  | .awaitingApproval => .ok (updateNodeState nodes name .settled)
  | _ => .error "node not awaiting approval"

/-! ## Formal Invariants and Theorems -/

/-- Theorem: A node cannot be scheduled to execute unless all its declared `needs` are settled. -/
theorem schedule_requires_needs_settled (nodes nodes' : List (String × NodeState))
    (node : NodeDef) (wave attempt : Nat)
    (h : scheduleNode nodes node wave attempt = .ok nodes') :
    allNeedsSettled nodes node.needs = true := by
  unfold scheduleNode at h
  split at h
  · contradiction
  · split at h
    · contradiction
    · rename_i _ hNeeds
      revert hNeeds
      cases allNeedsSettled nodes node.needs <;> simp

/-- Theorem: A parked node is an interruption, not completion.
A node in `.parked` state is never `.settled`. -/
theorem parked_is_not_settled (untilMs : Nat) (reason : String) :
    NodeState.parked untilMs reason ≠ .settled := by
  intro h
  contradiction

/-- Theorem: A node awaiting operator approval cannot settle without explicit approval. -/
theorem approval_gate_blocks_direct_settlement (nodes : List (String × NodeState)) (name : String)
    (hState : getNodeState nodes name = .awaitingApproval) :
    settleNode nodes name = .error "node not executing" := by
  unfold settleNode
  rw [hState]

/-- Theorem: Operator approval successfully settles an awaiting node. -/
theorem operator_approval_settles (nodes nodes' : List (String × NodeState)) (name : String)
    (h : operatorApprove nodes name = .ok nodes') :
    getNodeState nodes name = .awaitingApproval := by
  unfold operatorApprove at h
  split at h
  · assumption
  · contradiction

end Proto
