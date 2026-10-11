/-
  governed-by: ADR-0041
  The L16 append rule (redesign §2.2), the Lean twin of
  `toron_core::inbox::InboxStore::append_json`: the same triple appends once
  and returns its original offset; a lower epoch is fenced (before dedupe);
  a gap is rejected; a new epoch starts at seq 0. Epoch state never expires
  because entries are never removed.
-/
namespace Proto

structure Triple where
  producer : String
  epoch : Nat
  seq : Nat
  deriving DecidableEq, Repr

structure Log where
  entries : List (Triple × Nat)
  next : Nat
  deriving Repr

def Log.empty : Log := { entries := [], next := 1 }

inductive AppendErr where
  | staleEpoch | seqGap
  deriving DecidableEq, Repr

/-- The producer's current (highest accepted) epoch; 0 if none. -/
def curE : List (Triple × Nat) → String → Nat
  | [], _ => 0
  | x :: xs, p => if x.1.producer = p then max x.1.epoch (curE xs p) else curE xs p

/-- Is this producer known at all? -/
def known : List (Triple × Nat) → String → Bool
  | [], _ => false
  | x :: xs, p => x.1.producer = p || known xs p

/-- Last accepted seq for (producer, epoch). -/
def lastSeq : List (Triple × Nat) → String → Nat → Option Nat
  | [], _, _ => none
  | x :: xs, p, e =>
    let rest := lastSeq xs p e
    if x.1.producer = p ∧ x.1.epoch = e then
      some (match rest with | some r => max r x.1.seq | none => x.1.seq)
    else rest

def findOff : List (Triple × Nat) → Triple → Option Nat
  | [], _ => none
  | x :: xs, t => if x.1 = t then some x.2 else findOff xs t

def expected (l : Log) (t : Triple) : Nat :=
  match lastSeq l.entries t.producer t.epoch with
  | some s => s + 1
  | none => 0

/-- L16 append. Fence first, then dedupe, then the gap rule. -/
def append (l : Log) (t : Triple) : Except AppendErr (Log × Nat) :=
  if known l.entries t.producer ∧ t.epoch < curE l.entries t.producer then .error .staleEpoch else
  match findOff l.entries t with
  | some off => .ok (l, off)
  | none =>
    if t.seq ≠ expected l t then .error .seqGap
    else .ok ({ entries := l.entries ++ [(t, l.next)], next := l.next + 1 }, l.next)

/-! ## Lemmas -/

theorem curE_append (xs ys : List (Triple × Nat)) (p : String) :
    curE (xs ++ ys) p = max (curE xs p) (curE ys p) := by
  induction xs with
  | nil => simp [curE]
  | cons x xs ih => simp only [List.cons_append, curE]; split <;> simp [ih] <;> omega

theorem known_append (xs ys : List (Triple × Nat)) (p : String) :
    known (xs ++ ys) p = (known xs p || known ys p) := by
  induction xs with
  | nil => simp [known]
  | cons x xs ih => simp [known, ih, Bool.or_assoc]

theorem curE_unknown (xs : List (Triple × Nat)) (p : String)
    (h : known xs p = false) : curE xs p = 0 := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
    simp only [known, Bool.or_eq_false_iff, decide_eq_false_iff_not] at h
    simp [curE, h.1, ih h.2]

theorem findOff_append_none (xs : List (Triple × Nat)) (t : Triple) (n : Nat)
    (h : findOff xs t = none) : findOff (xs ++ [(t, n)]) t = some n := by
  induction xs with
  | nil => simp [findOff]
  | cons x xs ih =>
    simp only [findOff] at h
    simp only [List.cons_append, findOff]
    split at h
    · cases h
    · simp_all

/-! ## P1: exactly-once append -/

/-- P1a. Appending a triple the log accepted returns the same offset and
leaves the log unchanged (one row, same offset). -/
theorem append_idempotent (l l' : Log) (t : Triple) (o : Nat)
    (h : append l t = .ok (l', o)) : append l' t = .ok (l', o) := by
  unfold append at h
  split at h
  · cases h
  · rename_i hfence
    split at h
    · rename_i off hf
      cases h
      unfold append
      simp [hfence, hf]
    · rename_i hf
      split at h
      · cases h
      · cases h
        unfold append
        have hfind := findOff_append_none l.entries t l.next hf
        have hc : curE l.entries t.producer ≤ t.epoch := by
          by_cases hk : known l.entries t.producer = true
          · have := hfence
            simp only [hk, true_and, Nat.not_lt] at this
            exact this
          · rw [curE_unknown _ _ (by simpa using hk)]
            exact Nat.zero_le _
        have hnot : ¬ (known (l.entries ++ [(t, l.next)]) t.producer = true ∧
            t.epoch < curE (l.entries ++ [(t, l.next)]) t.producer) := by
          rw [curE_append]
          simp only [curE, ite_true, Nat.max_zero]
          omega
        simp [hnot, hfind]

/-- P1b. A lower epoch is fenced, even for a triple accepted earlier. -/
theorem stale_epoch_fenced (l : Log) (t : Triple)
    (hk : known l.entries t.producer = true) (hlt : t.epoch < curE l.entries t.producer) :
    append l t = .error .staleEpoch := by
  unfold append; simp [hk, hlt]

/-- P1c. A new triple whose seq is not the next expected one is a gap. -/
theorem gap_rejected (l : Log) (t : Triple)
    (hfresh : ¬ (known l.entries t.producer = true ∧ t.epoch < curE l.entries t.producer))
    (hnew : findOff l.entries t = none) (hgap : t.seq ≠ expected l t) :
    append l t = .error .seqGap := by
  unfold append; simp [hfresh, hnew, hgap]

/-- P1d. Appending grows the log by at most one row. -/
theorem append_at_most_one_row (l l' : Log) (t : Triple) (o : Nat)
    (h : append l t = .ok (l', o)) : l'.entries.length ≤ l.entries.length + 1 := by
  unfold append at h
  split at h
  · cases h
  · split at h
    · cases h; omega
    · split at h
      · cases h
      · cases h; simp

end Proto
