/-
  governed-by: ADR-0041
  P7. `project` against a literal table, on a finite universe: state 0 and
  state 1..branchCount+1 (one past the end) for swarm (4), loop (5), and open
  (3). Supervisor has no branches; state 0 and state 1 are both `notYet`.

  This is not `DeliveryFsm::step`. DeliveryFsm has no roles. P3 and P5 stay
  on `step` in `Fencing.lean`. `sendOk` is membership in `project`; the table
  is what makes that non-vacuous.
-/
import Choreo
import Choreo.Swarm
import Choreo.Loop
import Choreo.Open

namespace Choreo

def project (t : Topology) (r : Role) (s : Nat) : Proj :=
  match t with
  | .supervisor => .notYet
  | .swarm => .verbs (sortUniq (collect gSwarm r (some s)))
  | .loop => .verbs (sortUniq (collect gLoop r (some s)))
  | .«open» => .verbs (sortUniq (collect gOpen r (some s)))

/-- Literal spec. Cells not listed are empty (default deny), except supervisor,
which is `notYet` at every state. -/
def expectedProj : Topology → Role → Nat → Proj
  | .supervisor, _, _ => .notYet
  | .swarm, .captain, 0 => .verbs ["approval", "task"]
  | .swarm, .captain, 1 => .verbs ["task"]
  | .swarm, .captain, 4 => .verbs ["approval"]
  | .swarm, .crew, 0 => .verbs ["done", "progress", "reject"]
  | .swarm, .crew, 2 => .verbs ["done", "progress", "reject"]
  | .swarm, .verifier, 0 => .verbs ["verdict"]
  | .swarm, .verifier, 3 => .verbs ["verdict"]
  | .loop, .loopSelf, 0 => .verbs ["not_ready", "notify", "task", "tick"]
  | .loop, .loopSelf, 1 => .verbs ["tick"]
  | .loop, .loopSelf, 2 => .verbs ["task"]
  | .loop, .loopSelf, 4 => .verbs ["notify"]
  | .loop, .loopSelf, 5 => .verbs ["not_ready"]
  | .loop, .crew, 0 => .verbs ["done"]
  | .loop, .crew, 3 => .verbs ["done"]
  | .«open», .peer, 0 => .verbs ["notify", "reply", "request"]
  | .«open», .peer, 1 => .verbs ["request"]
  | .«open», .peer, 2 => .verbs ["reply"]
  | .«open», .peer, 3 => .verbs ["notify"]
  | _, _, _ => .verbs []

def bound : Topology → Nat
  | .swarm => 5
  | .loop => 6
  | .«open» => 4
  | .supervisor => 1

def statesOf (t : Topology) : List Nat := List.range (bound t + 1)

def roles : List Role := [.captain, .supervisor, .crew, .peer, .loopSelf, .verifier]

/-- Wire order matches `Topology::ALL`: swarm, supervisor, loop, open. -/
def topologies : List Topology := [.swarm, .supervisor, .loop, .«open»]

def allCells : List (Topology × Role × Nat) :=
  topologies.flatMap fun t =>
    roles.flatMap fun r =>
      (statesOf t).map fun s => (t, r, s)

def sendOk (t : Topology) (r : Role) (s : Nat) (c : String) : Bool :=
  (project t r s).verbsOf.elem c

def sameProj (t : Topology) (r : Role) (s : Nat) : Bool :=
  project t r s == expectedProj t r s

theorem cells_same :
    (allCells.all fun c => sameProj c.1 c.2.1 c.2.2) = true := by
  native_decide

theorem cell_eq (t : Topology) (r : Role) (s : Nat) (h : (t, r, s) ∈ allCells) :
    project t r s = expectedProj t r s := by
  have hAll := cells_same
  rw [List.all_eq_true] at hAll
  have hb : sameProj t r s = true := hAll (t, r, s) h
  simp only [sameProj] at hb
  exact eq_of_beq hb

/-- Both directions: `sendOk` accepts a contract exactly when the literal table lists it.
Supervisor cells list nothing, and `project` there is `notYet`, not an empty allow that
`sendOk` could confuse with "no sends". -/
theorem projection_sound (t : Topology) (r : Role) (s : Nat) (c : String)
    (h : (t, r, s) ∈ allCells) :
    sendOk t r s c = true ↔ c ∈ (expectedProj t r s).verbsOf := by
  have eqp := cell_eq t r s h
  unfold sendOk
  rw [eqp]
  cases expectedProj t r s with
  | verbs vs =>
    simp only [Proj.verbsOf]
    exact List.elem_iff
  | notYet =>
    simp [Proj.verbsOf]

theorem supervisor_is_not_yet (r : Role) (s : Nat) :
    project .supervisor r s = .notYet := rfl

theorem allCells_count : allCells.length = 120 := rfl

end Choreo
