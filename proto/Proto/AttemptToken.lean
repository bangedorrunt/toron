/-
  governed-by: ADR-0037
  AttemptToken coordination and lease fencing model.
  Mirroring `crates/toron-core/src/mailbox/reservations.rs` (`AttemptToken`,
  `file_reservation_with_token`, `release_by_reason_up_to_epoch`) and
  `flywheel/src/durable.rs` (`BeadSlotState`).
-/
namespace Proto

/-- Attempt token tying bead attempt revision and agent pin to a coordinate lease. -/
structure AttemptToken where
  beadId : String
  statusRevision : Nat
  pin : String
  deriving DecidableEq, Repr, Inhabited

/-- One coordinate lease held in the mailbox. -/
structure Reservation where
  id : String
  pathPattern : String
  agent : String
  epoch : Nat
  reason : String
  deriving DecidableEq, Repr, Inhabited

/-- Check if an active lease exists on a given path pattern. -/
def pathConflict (leases : List Reservation) (path : String) : Bool :=
  leases.any (fun r => r.pathPattern == path)

/-- Acquire a lease stamped with an AttemptToken. Fails if coordinate is in conflict. -/
def acquireWithToken (leases : List Reservation) (tok : AttemptToken) (id path agent : String) :
    Except String (List Reservation) :=
  if pathConflict leases path then
    .error "conflict"
  else
    let res : Reservation := {
      id := id
      pathPattern := path
      agent := agent
      epoch := tok.statusRevision
      reason := tok.beadId
    }
    .ok (res :: leases)

/-- Operator-signed release of all reservations whose reason matches `beadId`
up to `maxEpoch` (release_by_reason_up_to_epoch). -/
def releaseByReasonUpToEpoch (leases : List Reservation) (beadId : String) (maxEpoch : Nat) :
    List Reservation :=
  leases.filter fun r =>
    let matchesReason := r.reason == beadId
    !(matchesReason && r.epoch ≤ maxEpoch)

/-- Reopen ceremony: bump status revision and revoke prior leases with epoch <= current rev. -/
def reopenBead (statusRev : Nat) (leases : List Reservation) (beadId : String) :
    Nat × List Reservation :=
  (statusRev + 1, releaseByReasonUpToEpoch leases beadId statusRev)

/-! ## Formal Invariants and Theorems -/

/-- Theorem: Any lease acquired with an AttemptToken is stamped with the token's revision and beadId. -/
theorem acquire_stamps_token_revision (leases leases' : List Reservation) (tok : AttemptToken)
    (id path agent : String) (h : acquireWithToken leases tok id path agent = .ok leases') :
    ∃ r, r ∈ leases' ∧ r.epoch = tok.statusRevision ∧ r.reason = tok.beadId := by
  unfold acquireWithToken at h
  split at h
  · contradiction
  · injection h with hLeases
    subst hLeases
    refine ⟨_, List.Mem.head _, rfl, rfl⟩

/-- Theorem: No stale attempt outlives release_by_reason_up_to_epoch.
Any surviving lease matching beadId has strictly higher epoch than maxEpoch. -/
theorem stale_epoch_revoked (leases : List Reservation) (beadId : String) (maxEpoch : Nat)
    (r : Reservation) (hr : r ∈ releaseByReasonUpToEpoch leases beadId maxEpoch)
    (hm : r.reason = beadId) :
    maxEpoch < r.epoch := by
  unfold releaseByReasonUpToEpoch at hr
  rw [List.mem_filter] at hr
  obtain ⟨_, hCond⟩ := hr
  simp only [hm, beq_self_eq_true] at hCond
  have hNotLe : ¬ (r.epoch ≤ maxEpoch) := by
    intro hLe
    simp [hLe] at hCond
  omega

/-- Theorem: Reopening a bead fences all prior leases.
After reopenBead (R -> R+1), no surviving lease with reason=beadId has epoch <= R. -/
theorem reopen_fences_prior_leases (statusRev : Nat) (leases : List Reservation) (beadId : String)
    (r : Reservation) (hr : r ∈ (reopenBead statusRev leases beadId).2)
    (hm : r.reason = beadId) :
    statusRev < r.epoch := by
  unfold reopenBead at hr
  exact stale_epoch_revoked leases beadId statusRev r hr hm

/-- Theorem: Exclusive lease acquisition preserves single ownership per coordinate path. -/
theorem single_exclusive_lease (leases : List Reservation) (tok : AttemptToken)
    (id path agent : String) (r : Reservation)
    (h : acquireWithToken leases tok id path agent = .ok (r :: leases)) :
    ¬ pathConflict leases path := by
  unfold acquireWithToken at h
  split at h
  · contradiction
  · intro hc
    rename_i hConflict
    have hTrue : (pathConflict leases path = true) := hc
    have hFalse : (pathConflict leases path = false) := by
      revert hConflict
      cases (pathConflict leases path) <;> simp
    rw [hTrue] at hFalse
    contradiction

end Proto
