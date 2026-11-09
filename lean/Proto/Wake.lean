/-
  governed-by: ADR-0041
  The wake decision (redesign §4.1, P6), the Lean twin of
  `toron_core::wake::should_ring`. Rust is the implementation; this is the
  reference. `tests/proto_vectors.rs` replays vectors emitted from
  `shouldRing` below against the Rust function.
-/
namespace Proto

inductive PaneState where
  | idle | done | working | blocked | unknown | gone
  deriving DecidableEq, Repr, Inhabited

inductive Urgency where
  | normal | urgent
  deriving DecidableEq, Repr, Inhabited

def maxRings : Urgency → Nat
  | .normal => 5
  | .urgent => 3

def backoff (rings : Nat) (u : Urgency) : Nat :=
  match u with
  | .normal => min 900000 (30000 * 2 ^ (min rings 20))
  | .urgent => min 60000 (5000 * 2 ^ (min rings 20))

structure RingInput where
  head : Nat
  cursor : Nat
  bindingVerified : Bool
  bindingGen : Nat
  currentGen : Nat
  pane : PaneState
  lastRingWm : Option Nat
  lastRingAt : Option Nat
  rings : Nat
  now : Nat
  urgency : Urgency
  deriving DecidableEq, Repr, Inhabited

inductive Decision where
  | nothingUnread
  | unbound
  | busy
  | hold (untilMs : Nat)
  | ring (wm : Nat)
  | escalate (wm : Nat)
  deriving DecidableEq, Repr

def ringable (p : PaneState) (u : Urgency) : Bool :=
  match p with
  | .idle | .done => true
  | .working => u == .urgent
  | .blocked | .unknown | .gone => false

/-- Mail that rings at once: only when nothing is outstanding (the cursor
advanced since the last ring) and the head is past the last ring. More mail
after an unanswered ring waits for the backoff, so a flood cannot spend the
ring budget. -/
def newMail (i : RingInput) : Bool :=
  i.rings == 0 &&
  (match i.lastRingWm with
   | none => true
   | some wm => wm < i.head)

/-- The rule; the check order mirrors the Rust function exactly. -/
def shouldRing (i : RingInput) : Decision :=
  if i.head ≤ i.cursor then .nothingUnread else
  if !i.bindingVerified || i.bindingGen != i.currentGen || i.pane == .gone then .unbound else
  if maxRings i.urgency ≤ i.rings then .escalate i.head else
  if !ringable i.pane i.urgency then .busy else
  if newMail i then .ring i.head else
  let due := i.lastRingAt.getD 0 + backoff i.rings i.urgency
  if due ≤ i.now then .ring i.head else .hold due

/-! ## P6: bounded wake -/

/-- One host tick against a recipient that never reads: a ring is
recorded, a hold moves the clock to the due time, anything else is left. -/
def tick (i : RingInput) : RingInput :=
  match shouldRing i with
  | .ring wm => { i with lastRingWm := some wm, lastRingAt := some i.now, rings := i.rings + 1 }
  | .hold u => { i with now := u }
  | _ => i

def tickN : Nat → RingInput → RingInput
  | 0, i => i
  | n + 1, i => tickN n (tick i)

/-- The facts a tick never changes. -/
theorem tick_keeps_inputs (i : RingInput) :
    (tick i).head = i.head ∧ (tick i).cursor = i.cursor ∧
    (tick i).bindingVerified = i.bindingVerified ∧ (tick i).bindingGen = i.bindingGen ∧
    (tick i).currentGen = i.currentGen ∧ (tick i).pane = i.pane ∧
    (tick i).urgency = i.urgency := by
  unfold tick
  split <;> simp_all

/-- Once the budget is spent the decision is Escalate: never a ring. -/
theorem escalates_at_budget (i : RingInput) (hu : i.cursor < i.head)
    (hb : i.bindingVerified = true) (hg : i.bindingGen = i.currentGen) (hp : i.pane ≠ .gone)
    (hr : maxRings i.urgency ≤ i.rings) : shouldRing i = .escalate i.head := by
  unfold shouldRing
  have h1 : ¬ i.head ≤ i.cursor := by omega
  simp [h1, hb, hg, hp, hr]

/-- No ring is ever issued past the budget. -/
theorem ring_stays_in_budget (i : RingInput) (wm : Nat) (h : shouldRing i = .ring wm) :
    i.rings < maxRings i.urgency := by
  unfold shouldRing at h
  by_cases h1 : i.head ≤ i.cursor
  · simp [h1] at h
  by_cases h2 : (!i.bindingVerified || i.bindingGen != i.currentGen || i.pane == .gone) = true
  · simp [h1, h2] at h
  by_cases h3 : maxRings i.urgency ≤ i.rings
  · simp [h1, h2, h3] at h
  omega

/-- A ring needs a verified, current binding on a live pane. -/
theorem ring_needs_a_verified_binding (i : RingInput) (wm : Nat) (h : shouldRing i = .ring wm) :
    i.bindingVerified = true ∧ i.bindingGen = i.currentGen ∧ i.pane ≠ .gone := by
  unfold shouldRing at h
  by_cases h1 : i.head ≤ i.cursor
  · simp [h1] at h
  by_cases h2 : (!i.bindingVerified || i.bindingGen != i.currentGen || i.pane == .gone) = true
  · simp [h1, h2] at h
  simp at h2
  exact ⟨h2.1.1, h2.1.2, h2.2⟩

/-- A ring needs unread mail. -/
theorem ring_needs_unread_mail (i : RingInput) (wm : Nat) (h : shouldRing i = .ring wm) :
    i.cursor < i.head := by
  unfold shouldRing at h
  by_cases h1 : i.head ≤ i.cursor
  · simp [h1] at h
  omega

/-- When caught up (head ≤ cursor), the decision is strictly nothingUnread. -/
theorem caught_up_is_nothing_unread (i : RingInput) (h : i.head ≤ i.cursor) :
    shouldRing i = .nothingUnread := by
  unfold shouldRing
  simp [h]

/-- An unverified binding never rings. -/
theorem unverified_binding_never_rings (i : RingInput) (wm : Nat)
    (hb : i.bindingVerified = false) (h : shouldRing i = .ring wm) : False := by
  have ⟨hvb, _, _⟩ := ring_needs_a_verified_binding i wm h
  simp [hb] at hvb

/-- A stale binding generation never rings. -/
theorem stale_gen_never_rings (i : RingInput) (wm : Nat)
    (hg : i.bindingGen ≠ i.currentGen) (h : shouldRing i = .ring wm) : False := by
  have ⟨_, hgen, _⟩ := ring_needs_a_verified_binding i wm h
  exact hg hgen

/-- A working pane for normal mail is always suppressed as Busy. -/
theorem working_pane_normal_mail_is_busy (i : RingInput) (hu : i.cursor < i.head)
    (hb : i.bindingVerified = true) (hg : i.bindingGen = i.currentGen) (hp : i.pane = .working)
    (hurg : i.urgency = .normal) (hr : i.rings < maxRings i.urgency) :
    shouldRing i = .busy := by
  unfold shouldRing
  have h1 : ¬ i.head ≤ i.cursor := by omega
  have h2 : ¬ ((!i.bindingVerified || i.bindingGen != i.currentGen || i.pane == .gone) = true) := by
    simp [hb, hg, hp]
  have h3 : ¬ maxRings i.urgency ≤ i.rings := by omega
  have h4 : (!ringable i.pane i.urgency) = true := by
    simp [hp, hurg, ringable]
  simp [h1, h2, h3, h4]

/-- After a Hold, the clock sits at the due time and the very next
decision is a ring. -/
theorem hold_then_ring (i : RingInput) (u : Nat) (h : shouldRing i = .hold u) :
    shouldRing { i with now := u } = .ring i.head := by
  unfold shouldRing at h ⊢
  by_cases h1 : i.head ≤ i.cursor
  · simp [h1] at h
  by_cases h2 : (!i.bindingVerified || i.bindingGen != i.currentGen || i.pane == .gone) = true
  · simp [h1, h2] at h
  by_cases h3 : maxRings i.urgency ≤ i.rings
  · simp [h1, h2, h3] at h
  by_cases h4 : (!ringable i.pane i.urgency) = true
  · simp [h1, h2, h3, h4] at h
  by_cases h5 : newMail i = true
  · simp [h1, h2, h3, h4, h5] at h
  by_cases h6 : i.lastRingAt.getD 0 + backoff i.rings i.urgency ≤ i.now
  · simp [h1, h2, h3, h4, h5, h6] at h
  simp [h1, h2, h3, h4, h5, h6] at h
  subst h
  simp [h1, h2, h3, h4]

/-- The standing conditions of P6: unread mail, a verified current binding,
a pane that can be typed into. -/
def Live (i : RingInput) : Prop :=
  i.cursor < i.head ∧ i.bindingVerified = true ∧ i.bindingGen = i.currentGen ∧
    (i.pane = .idle ∨ i.pane = .done)

/-- Under the standing conditions and with budget left, the decision is a
ring or a hold; nothing else. -/
theorem live_rings_or_holds (i : RingInput) (hl : Live i) (hr : i.rings < maxRings i.urgency) :
    shouldRing i = .ring i.head ∨ ∃ u, shouldRing i = .hold u := by
  obtain ⟨hu, hb, hg, hp⟩ := hl
  have h1 : ¬ i.head ≤ i.cursor := by omega
  have h2 : ¬ ((!i.bindingVerified || i.bindingGen != i.currentGen || i.pane == .gone) = true) := by
    rcases hp with hp | hp <;> simp [hb, hg, hp]
  have h3 : ¬ maxRings i.urgency ≤ i.rings := by omega
  have h4 : ¬ ((!ringable i.pane i.urgency) = true) := by
    rcases hp with hp | hp <;> simp [hp, ringable]
  unfold shouldRing
  simp only [h1, h2, h3, h4]
  by_cases h5 : newMail i = true
  · left; simp [h5]
  · by_cases h6 : i.lastRingAt.getD 0 + backoff i.rings i.urgency ≤ i.now
    · left; simp [h5, h6]
    · right; exact ⟨i.lastRingAt.getD 0 + backoff i.rings i.urgency, by simp [h5, h6]⟩

theorem tick_of_ring (i : RingInput) (wm : Nat) (h : shouldRing i = .ring wm) :
    tick i = { i with lastRingWm := some wm, lastRingAt := some i.now, rings := i.rings + 1 } := by
  simp [tick, h]

theorem tick_of_hold (i : RingInput) (u : Nat) (h : shouldRing i = .hold u) :
    tick i = { i with now := u } := by
  simp [tick, h]

theorem live_after_ring (i : RingInput) (hl : Live i) (wm : Nat) (h : shouldRing i = .ring wm) :
    Live (tick i) ∧ maxRings (tick i).urgency - (tick i).rings < maxRings i.urgency - i.rings := by
  have hr := ring_stays_in_budget i wm h
  rw [tick_of_ring i wm h]
  obtain ⟨hu, hb, hg, hp⟩ := hl
  refine ⟨⟨hu, hb, hg, hp⟩, ?_⟩
  simp only []
  omega

theorem live_after_hold (i : RingInput) (hl : Live i) (u : Nat) (h : shouldRing i = .hold u) :
    Live (tick i) := by
  rw [tick_of_hold i u h]
  exact hl

theorem reach (d : Nat) : ∀ i : RingInput, Live i → maxRings i.urgency - i.rings ≤ d →
    ∃ k, k ≤ 2 * d + 1 ∧ shouldRing (tickN k i) = .escalate i.head := by
  induction d with
  | zero =>
    intro i hl hd
    refine ⟨0, by omega, ?_⟩
    obtain ⟨hu, hb, hg, hp⟩ := hl
    have : maxRings i.urgency ≤ i.rings := by omega
    exact escalates_at_budget i hu hb hg (by rcases hp with hp | hp <;> simp [hp]) this
  | succ d ih =>
    intro i hl hd
    by_cases hb : maxRings i.urgency ≤ i.rings
    · refine ⟨0, by omega, ?_⟩
      obtain ⟨hu, hb', hg, hp⟩ := hl
      exact escalates_at_budget i hu hb' hg (by rcases hp with hp | hp <;> simp [hp]) hb
    · have hr : i.rings < maxRings i.urgency := by omega
      rcases live_rings_or_holds i hl hr with h | ⟨u, h⟩
      · obtain ⟨hl', hlt⟩ := live_after_ring i hl _ h
        obtain ⟨k, hk, hs⟩ := ih (tick i) hl' (by omega)
        refine ⟨k + 1, by omega, ?_⟩
        have hhead := (tick_keeps_inputs i).1
        show shouldRing (tickN k (tick i)) = _
        rw [hs, hhead]
      · have hl1 := live_after_hold i hl u h
        have hring : shouldRing (tick i) = .ring i.head := by
          rw [tick_of_hold i u h]; exact hold_then_ring i u h
        obtain ⟨hl2, hlt⟩ := live_after_ring (tick i) hl1 _ hring
        obtain ⟨k, hk, hs⟩ := ih (tick (tick i)) hl2 (by
          have h1 := (tick_keeps_inputs i).2.2.2.2.2.2
          have h2 := (tick_keeps_inputs (tick i)).2.2.2.2.2.2
          have h3 : (tick i).rings = i.rings := by rw [tick_of_hold i u h]
          rw [h2, h1, h3] at hlt
          rw [h2, h1]
          omega)
        refine ⟨k + 2, by omega, ?_⟩
        have e1 := (tick_keeps_inputs i).1
        have e2 := (tick_keeps_inputs (tick i)).1
        show shouldRing (tickN k (tick (tick i))) = _
        rw [hs, e2, e1]

/-- P6, bounded wake. A verified binding on a ringable pane with mail that
is never read: within `2 * maxRings + 1` ticks (a hold plus a ring per
ring) the decision is Escalate. With `ring_stays_in_budget`, no more than
`maxRings` rings are issued first. No silent stall. -/
theorem p6_bounded_wake (i : RingInput) (hl : Live i) :
    ∃ k, k ≤ 2 * maxRings i.urgency + 1 ∧ shouldRing (tickN k i) = .escalate i.head :=
  reach (maxRings i.urgency) i hl (by omega)

end Proto
