/-
  governed-by: ADR-0041
  Choreography projection, the Lean twin of `toron_core::topology::{project,collect}`.
  Supervisor is refused by the topology name (`not_yet`), not by walking `End`.
  A copied `End` is an empty verb list; that is a different answer, and Rust
  only gets it when the pointer check is skipped.
-/
namespace Choreo

inductive Role where
  | captain | supervisor | crew | peer | loopSelf | verifier
  deriving DecidableEq, Repr, Inhabited

def Role.wire : Role → String
  | .captain => "captain"
  | .supervisor => "supervisor"
  | .crew => "crew"
  | .peer => "peer"
  | .loopSelf => "loop_self"
  | .verifier => "verifier"

inductive Topology where
  | swarm | supervisor | loop | «open»
  deriving DecidableEq, Repr, Inhabited

def Topology.wire : Topology → String
  | .swarm => "swarm"
  | .supervisor => "supervisor"
  | .loop => "loop"
  | .«open» => "open"

/-- Five combinators, matching `topology::Global`. `empty` is Rust's `End`. -/
inductive Global where
  | send (fromRole toRole : Role) (contract : String)
  | choice (arms : List Global)
  | par (branches : List Global)
  | loop (body : Global)
  | empty
  deriving Repr

/-- BTreeSet order: lexicographic, duplicates collapsed. -/
def insertVerb (x : String) : List String → List String
  | [] => [x]
  | y :: ys =>
    match Ord.compare x y with
    | .lt => x :: y :: ys
    | .eq => y :: ys
    | .gt => y :: insertVerb x ys

def sortUniq (xs : List String) : List String :=
  xs.foldl (fun acc x => insertVerb x acc) []

/- `state = none` or `some 0` keeps every send this role owns.
`some n` with `n > 0` keeps only the n-th arm (1-based) of this Par or Choice,
then reads that arm as state 0. A cursor past the end adds nothing.
Head/tail is the same pick as indexing: the n = 1 arm is the head, and a
longer cursor walks the tail. -/
mutual
def collect (g : Global) (role : Role) (state : Option Nat) : List String :=
  match g with
  | .send fr _ contract =>
    if fr == role && (state.isNone || state == some 0) then [contract] else []
  | .choice arms => collectAt arms role state
  | .par arms => collectAt arms role state
  | .loop body => collect body role state
  | .empty => []
termination_by sizeOf g

def collectAt (arms : List Global) (role : Role) (state : Option Nat) : List String :=
  match state with
  | none | some 0 =>
    match arms with
    | [] => []
    | a :: rest => collect a role (some 0) ++ collectAt rest role (some 0)
  | some n =>
    match arms with
    | [] => []
    | a :: rest =>
      if n = 1 then collect a role (some 0) else collectAt rest role (some (n - 1))
termination_by sizeOf arms
end

/-- What a projection returns. `notYet` is the supervisor refusal, not `verbs []`. -/
inductive Proj where
  | verbs (vs : List String)
  | notYet
  deriving DecidableEq, Repr

def Proj.verbsOf : Proj → List String
  | .verbs vs => vs
  | .notYet => []

end Choreo
