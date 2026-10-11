/-
  governed-by: ADR-0041
  The delivery state machine (redesign §3), the Lean twin of
  `toron_core::delivery::DeliveryFsm::step`. Rust is the implementation;
  this is the reference. `tests/proto_vectors.rs` replays vectors emitted
  from `step` below against the Rust function.
-/
namespace Proto

inductive Phase where
  | intent | unknown | sent | delivered | acknowledged | acted | proved
  | rejected | expired | superseded | cancelled | failed
  deriving DecidableEq, Repr, Inhabited

inductive Class where
  | notify | request | reply | task | control | receipt
  deriving DecidableEq, Repr, Inhabited

def Phase.rank : Phase → Nat
  | .intent | .unknown => 0
  | .sent => 1
  | .delivered => 2
  | .acknowledged => 3
  | .acted => 4
  | .proved => 5
  | .rejected | .expired | .superseded | .cancelled | .failed => 6

def successTerminal : Class → Phase
  | .notify | .receipt => .delivered
  | .request | .reply | .control => .acknowledged
  | .task => .proved

def Class.obligation : Class → Bool
  | .notify | .receipt => false
  | _ => true

structure State where
  phase : Phase
  cls : Class
  recipient : String
  offset : Option Nat
  gen : Nat
  holder : Option String
  doer : Option String
  deriving DecidableEq, Repr, Inhabited

def State.isTerminal (s : State) : Bool :=
  s.phase.rank == 6 || s.phase == successTerminal s.cls

inductive Event where
  | appended (offset : Nat)
  | appendUncertain
  | reconcileFound (offset : Nat)
  | reconcileAbsent
  | read (reader : String) (cursor : Nat)
  | deadlinePassed
  | accept (by_ : String) (gen : Nat)
  | refuse (by_ : String)
  | progress (by_ : String) (gen : Nat)
  | done (by_ : String) (gen : Nat)
  | verdict (by_ : String) (pass : Bool) (rework : Bool)
  | supersede (newGen : Nat)
  | cancel
  deriving DecidableEq, Repr

def Event.name : Event → String
  | .appended _ => "appended"
  | .appendUncertain => "append_uncertain"
  | .reconcileFound _ => "reconcile_found"
  | .reconcileAbsent => "reconcile_absent"
  | .read _ _ => "read"
  | .deadlinePassed => "deadline_passed"
  | .accept _ _ => "accept"
  | .refuse _ => "refuse"
  | .progress _ _ => "progress"
  | .done _ _ => "done"
  | .verdict _ _ _ => "verdict"
  | .supersede _ => "supersede"
  | .cancel => "cancel"

/-- Reject kinds, spelled as the Rust `Reject` serde tags. -/
inductive Reject where
  | notRecipient | cursorBehind | staleGen | notHolder | selfVerdict
  | notApplicable | terminal | illegalTransition
  deriving DecidableEq, Repr

def Reject.tag : Reject → String
  | .notRecipient => "not_recipient"
  | .cursorBehind => "cursor_behind"
  | .staleGen => "stale_gen"
  | .notHolder => "not_holder"
  | .selfVerdict => "self_verdict"
  | .notApplicable => "not_applicable"
  | .terminal => "terminal"
  | .illegalTransition => "illegal_transition"

/-- One step; the check order mirrors the Rust function exactly. -/
def step (s : State) (e : Event) : Except Reject State :=
  if s.isTerminal then .error .terminal else
  match s.phase, e with
  | .intent, .appended o => .ok { s with phase := .sent, offset := some o }
  | .intent, .appendUncertain => .ok { s with phase := .unknown }
  | .unknown, .reconcileFound o => .ok { s with phase := .sent, offset := some o }
  | .unknown, .reconcileAbsent => .ok { s with phase := .intent }
  | .sent, .read reader cursor =>
      if reader ≠ s.recipient then .error .notRecipient else
      match s.offset with
      | none => .error .illegalTransition
      | some off => if cursor < off then .error .cursorBehind else .ok { s with phase := .delivered }
  | .sent, .deadlinePassed => .ok { s with phase := .expired }
  | .delivered, .deadlinePassed => .ok { s with phase := .expired }
  | .delivered, .accept b g =>
      if !s.cls.obligation then .error .notApplicable
      else if b ≠ s.recipient then .error .notRecipient
      else if g ≠ s.gen then .error .staleGen
      else .ok { s with phase := .acknowledged, holder := some b }
  | .delivered, .refuse b =>
      if !s.cls.obligation then .error .notApplicable
      else if b ≠ s.recipient then .error .notRecipient
      else .ok { s with phase := .rejected }
  | .acknowledged, .progress b g =>
      if s.holder ≠ some b then .error .notHolder
      else if g ≠ s.gen then .error .staleGen
      else .ok s
  | .acknowledged, .done b g =>
      if s.holder ≠ some b then .error .notHolder
      else if g ≠ s.gen then .error .staleGen
      else if s.cls ≠ .task then .error .notApplicable
      else .ok { s with phase := .acted, doer := some b }
  | .acted, .verdict b pass rework =>
      if s.doer = some b then .error .selfVerdict
      else if pass then .ok { s with phase := .proved }
      else if rework then .ok { s with phase := .acknowledged, doer := none }
      else .ok { s with phase := .failed }
  | .acknowledged, .supersede ng =>
      if ng ≤ s.gen then .error .staleGen else .ok { s with phase := .superseded, gen := ng }
  | .acted, .supersede ng =>
      if ng ≤ s.gen then .error .staleGen else .ok { s with phase := .superseded, gen := ng }
  | .acknowledged, .cancel => .ok { s with phase := .cancelled }
  | _, _ => .error .illegalTransition

/-! ## P2: monotonicity -/

/-- The one edge allowed to lower the rank: verdict-gated rework. -/
def reworkEdge (a b : Phase) : Prop := a = .acted ∧ b = .acknowledged

/-- P2. Every accepted step keeps or raises the rank, except the
verdict-gated rework edge `acted → acknowledged`. -/
theorem step_monotone (s s' : State) (e : Event) (h : step s e = .ok s') :
    s.phase.rank ≤ s'.phase.rank ∨ reworkEdge s.phase s'.phase := by
  unfold step at h
  split at h
  · cases h
  · split at h <;> (repeat' split at h) <;> (first | cases h | (injection h with h; subst h)) <;>
      simp_all [Phase.rank, reworkEdge]

/-- P2'. Terminals absorb: no event moves a terminal delivery. -/
theorem terminal_absorbs (s : State) (e : Event) (h : s.isTerminal = true) :
    step s e = .error .terminal := by
  unfold step; simp [h]

/-- The rework edge needs a verdict event (P2's exception is gated). -/
theorem rework_needs_a_verdict (s s' : State) (e : Event) (h : step s e = .ok s')
    (hr : reworkEdge s.phase s'.phase) : ∃ b, e = .verdict b false true := by
  unfold step at h
  obtain ⟨h1, h2⟩ := hr
  split at h
  · cases h
  · split at h <;> (repeat' split at h) <;> (first | cases h | (injection h with h; subst h)) <;>
      simp_all

end Proto
