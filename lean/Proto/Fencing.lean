/-
  governed-by: ADR-0041
  P3 and P5 beside `step`. The machine is not rewritten: a stale generation
  never becomes `acted`, and the doer of a task cannot be the verifier who
  marks it proved. Terminal classes at `acknowledged` (request, reply,
  control) refuse earlier with `terminal`, so a second accept is "not ok"
  rather than one fixed reject tag.
-/
import Proto.Delivery

namespace Proto

/-! ## P3: no double act -/

/-- P3. A `done` that lands on `acted` was the holder, on this generation,
from `acknowledged`, and the class was `task`. -/
theorem no_double_act (s s' : State) (b : String) (g : Nat)
    (h : step s (.done b g) = .ok s') (hp : s'.phase = .acted) :
    g = s.gen ∧ s.holder = some b ∧ s.phase = .acknowledged ∧ s.cls = .task := by
  unfold step at h
  split at h
  · cases h
  · split at h <;> (repeat' split at h) <;> (first | cases h | (injection h with h; subst h)) <;>
      simp_all

/-- A stale generation never produces `acted`. An earlier check (holder,
terminal, wrong phase) may refuse first, so the reject tag is not always
`staleGen`; the phase result is. -/
theorem stale_gen_never_acted (s s' : State) (b : String) (g : Nat)
    (hg : g ≠ s.gen) (h : step s (.done b g) = .ok s') :
    s'.phase ≠ .acted := by
  intro hp
  exact hg (no_double_act s s' b g h hp).1

/-- An accept that succeeds binds `holder` to the recipient at this generation. -/
theorem accept_sets_holder (s s' : State) (b : String) (g : Nat)
    (h : step s (.accept b g) = .ok s') :
    s'.holder = some b ∧ b = s.recipient ∧ g = s.gen ∧ s'.phase = .acknowledged := by
  unfold step at h
  split at h
  · cases h
  · split at h <;> (repeat' split at h) <;> (first | cases h | (injection h with h; subst h)) <;>
      simp_all

/-- No accept arm exists once the phase is `acknowledged`, so a second accept
is not `.ok`. -/
theorem accept_acknowledged_not_ok (s : State) (b : String) (g : Nat)
    (hp : s.phase = .acknowledged) :
    ∀ s', step s (.accept b g) ≠ .ok s' := by
  intro s' h
  unfold step at h
  rw [hp] at h
  split at h <;> cases h

/-! ## P5: proof separation -/

/-- P5. A passing verdict that reaches `proved` started from `acted`, and the
verifier is not the doer. -/
theorem proof_separation (s s' : State) (b : String) (rework : Bool)
    (h : step s (.verdict b true rework) = .ok s') (hp : s'.phase = .proved) :
    s.phase = .acted ∧ s.doer ≠ some b := by
  unfold step at h
  split at h
  · cases h
  · split at h <;> (repeat' split at h) <;> (first | cases h | (injection h with h; subst h)) <;>
      simp_all

/-- The doer-equals-verifier case. `acted` is never terminal, so the match
reaches the self-verdict check. Mirrors the `selfVerdict` arm. -/
theorem self_verdict_refused (s : State) (b : String) (pass rework : Bool)
    (hp : s.phase = .acted) (hd : s.doer = some b) :
    step s (.verdict b pass rework) = .error .selfVerdict := by
  have ht : s.isTerminal = false := by
    cases hc : s.cls <;> simp [State.isTerminal, hp, Phase.rank, successTerminal, hc]
  unfold step
  simp [ht, hp, hd]

end Proto
