---- MODULE AttemptTokenFencing ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxRevisions

VARIABLES
    bead_status,       \* "Open", "InProgress", "Closed"
    status_revision,   \* Nat
    current_attempt,   \* Agent or "None"
    active_leases      \* Set of [agent: Agents, epoch: Nat]

vars == <<bead_status, status_revision, current_attempt, active_leases>>

Agents == {"AgentA", "AgentB"}

Init ==
    /\ bead_status = "Open"
    /\ status_revision = 0
    /\ current_attempt = "None"
    /\ active_leases = {}

Dispatch(a) ==
    /\ bead_status = "Open"
    /\ status_revision <= MaxRevisions
    /\ bead_status' = "InProgress"
    /\ current_attempt' = a
    /\ UNCHANGED <<status_revision, active_leases>>

AcquireLease(a) ==
    /\ bead_status = "InProgress"
    /\ current_attempt = a
    /\ active_leases = {}
    /\ active_leases' = {[agent |-> a, epoch |-> status_revision]}
    /\ UNCHANGED <<bead_status, status_revision, current_attempt>>

ReleaseLease(a) ==
    /\ \E l \in active_leases : l.agent = a
    /\ active_leases' = {l \in active_leases : l.agent # a}
    /\ UNCHANGED <<bead_status, status_revision, current_attempt>>

Close ==
    /\ bead_status = "InProgress"
    /\ bead_status' = "Closed"
    /\ active_leases' = {}  \* release_by_reason clears all leases on close
    /\ current_attempt' = "None"
    /\ UNCHANGED <<status_revision>>

Reopen ==
    /\ status_revision < MaxRevisions
    /\ (bead_status = "Closed" \/ bead_status = "InProgress")
    /\ bead_status' = "Open"
    /\ status_revision' = status_revision + 1
    /\ current_attempt' = "None"
    \* release_by_reason_up_to_epoch revokes all leases with epoch <= current revision
    /\ active_leases' = {l \in active_leases : l.epoch > status_revision}

Next ==
    \/ \E a \in Agents : Dispatch(a)
    \/ \E a \in Agents : AcquireLease(a)
    \/ \E a \in Agents : ReleaseLease(a)
    \/ Close
    \/ Reopen

Spec == Init /\ [][Next]_vars

\* Invariants
SingleExclusiveLease ==
    Cardinality(active_leases) <= 1

NoStaleAttemptHoldingLease ==
    \A l \in active_leases : l.epoch >= status_revision

ActiveLeaseMatchesAttempt ==
    (bead_status = "InProgress" /\ active_leases # {}) =>
        (\A l \in active_leases : l.agent = current_attempt /\ l.epoch = status_revision)

====
