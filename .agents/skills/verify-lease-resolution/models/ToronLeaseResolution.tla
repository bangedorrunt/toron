---- MODULE ToronLeaseResolution ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxEpoch, FixSplitBrainAndEpoch, Bug, CapEnabled, MaxHold
\* bd-ll7bs: Bug \in {"none", "double-holder", "lifo", "drop-waiter", "no-ttl"}.
\* bd-lk0kt: also "no-edit-lease", "no-edit-queue", "double-acquire".
\* Each non-"none" value removes exactly one fence.

VARIABLES
    bus_holder,
    bus_status,
    bus_epoch,
    next_epoch,
    bus_expired,
    mirror_expired_active,
    mirror_epoch,
    durable_live,
    waitq,
    waitqLen,
    arrq,
    arrLen,
    servedq,
    servedLen,
    arrived,
    served,
    asked,
    badRelease,
    ghost,
    hold_ticks,
    dirty,
    stolen,
    clean_stalled,
    edits_leased,
    edits_blocked,
    edits_waiters,
    edits_writes

vars == <<
    bus_holder,
    bus_status,
    bus_epoch,
    next_epoch,
    bus_expired,
    mirror_expired_active,
    mirror_epoch,
    durable_live,
    waitq,
    waitqLen,
    arrq,
    arrLen,
    servedq,
    servedLen,
    arrived,
    served,
    asked,
    badRelease,
    ghost,
    hold_ticks,
    dirty,
    stolen,
    clean_stalled,
    edits_leased,
    edits_blocked,
    edits_waiters,
    edits_writes
>>

\* bd-ll7bs: the waitlist half of the lease board. Kept in the SAME model so
\* the existing split-brain/epoch invariants still constrain a handoff.
LeaseVars == <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired,
    mirror_expired_active, mirror_epoch, durable_live, waitq, waitqLen,
    arrq, arrLen, servedq, servedLen, arrived, served, asked, badRelease,
    ghost, hold_ticks, dirty, stolen, clean_stalled>>

EditVars == <<edits_leased, edits_blocked, edits_waiters, edits_writes>>

Agents == {"A", "B", "C"}
Sources == {"commit", "ship", "release", "ttl", "retire"}
MaxQ == 2
QIdx == 1..MaxQ
baseNew == <<waitq, waitqLen, arrq, arrLen, servedq, servedLen, arrived, served, asked, badRelease, ghost, dirty, stolen, clean_stalled>>
newVars == <<baseNew, hold_ticks>>

BusActiveOwners ==
    IF bus_status = "Held" /\ ~bus_expired /\ bus_holder \in Agents
    THEN {bus_holder}
    ELSE {}

LivenessExtendedOwners ==
    IF FixSplitBrainAndEpoch THEN
        {a \in Agents :
            /\ a \in mirror_expired_active
            /\ a \in durable_live
            /\ bus_status = "Held"
            /\ bus_expired
            /\ bus_holder = a
            /\ BusActiveOwners = {}
            /\ (Bug = "clean-idle-extends" \/ a \in dirty)}
    ELSE
        {a \in Agents :
            /\ a \in mirror_expired_active
            /\ a \in durable_live
            /\ ~(a \in BusActiveOwners)
            /\ (Bug = "clean-idle-extends" \/ a \in dirty)}

ActiveOwners == BusActiveOwners \cup LivenessExtendedOwners

ResolvedEpoch(a) ==
    IF a \in BusActiveOwners THEN bus_epoch
    ELSE IF FixSplitBrainAndEpoch THEN mirror_epoch[a]
    ELSE 0

Init ==
    /\ bus_holder = "None"
    /\ bus_status = "None"
    /\ bus_epoch = 0
    /\ next_epoch = 0
    /\ bus_expired = FALSE
    /\ mirror_expired_active = {}
    /\ mirror_epoch = [a \in Agents |-> 0]
    /\ durable_live = {}
    /\ waitq = [i \in QIdx |-> "None"]
    /\ waitqLen = 0
    /\ arrq = [i \in QIdx |-> "None"]
    /\ arrLen = 0
    /\ servedq = [i \in QIdx |-> "None"]
    /\ servedLen = 0
    /\ arrived = {}
    /\ served = {}
    /\ asked = {}
    /\ badRelease = {}
    /\ ghost = {}
    /\ hold_ticks = 0
    /\ dirty = {}
    /\ stolen = FALSE
    /\ clean_stalled = FALSE
    /\ edits_leased = {}
    /\ edits_blocked = {}
    /\ edits_waiters = [a \in Agents |-> 0]
    /\ edits_writes = {}

AcquireA ==
    /\ UNCHANGED EditVars
    /\ next_epoch <= MaxEpoch
    /\ ActiveOwners = {}
    /\ bus_holder' = "A"
    /\ bus_status' = "Held"
    /\ bus_epoch' = next_epoch
    /\ next_epoch' = next_epoch + 1
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' =
        IF FixSplitBrainAndEpoch THEN {} ELSE mirror_expired_active \ {"A"}
    /\ mirror_epoch' = [mirror_epoch EXCEPT !["A"] = next_epoch]
    /\ durable_live' = durable_live \cup {"A"}
    /\ UNCHANGED baseNew
    /\ hold_ticks' = 0

AcquireB ==
    /\ UNCHANGED EditVars
    /\ next_epoch >= 1
    /\ next_epoch <= MaxEpoch
    /\ ActiveOwners = {}
    /\ bus_holder' = "B"
    /\ bus_status' = "Held"
    /\ bus_epoch' = next_epoch
    /\ next_epoch' = next_epoch + 1
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' =
        IF FixSplitBrainAndEpoch THEN {} ELSE mirror_expired_active \ {"B"}
    /\ mirror_epoch' = [mirror_epoch EXCEPT !["B"] = next_epoch]
    /\ durable_live' = durable_live \cup {"B"}
    /\ UNCHANGED baseNew
    /\ hold_ticks' = 0

AcquireC ==
    /\ UNCHANGED EditVars
    /\ next_epoch >= 2
    /\ next_epoch <= MaxEpoch
    /\ ActiveOwners = {}
    /\ bus_holder' = "C"
    /\ bus_status' = "Held"
    /\ bus_epoch' = next_epoch
    /\ next_epoch' = next_epoch + 1
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = IF FixSplitBrainAndEpoch THEN {} ELSE mirror_expired_active \ {"C"}
    /\ mirror_epoch' = [mirror_epoch EXCEPT !["C"] = next_epoch]
    /\ durable_live' = durable_live \cup {"C"}
    /\ UNCHANGED baseNew
    /\ hold_ticks' = 0

HoldTick ==
    /\ UNCHANGED EditVars
    /\ (IF CapEnabled THEN hold_ticks < MaxHold ELSE hold_ticks < MaxHold + 1)
    /\ hold_ticks' = hold_ticks + 1
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired,
                   mirror_expired_active, mirror_epoch, durable_live, baseNew>>

\* bd-nmilo: a live hold survives its TTL only while the holder still has
\* uncommitted changes in the path. Commit clears the dirty flag; after the
\* grace window (modelled by CleanCommit) the lease is no longer extended.
EditDirty(a) ==
    /\ UNCHANGED EditVars
    /\ a \in Agents
    /\ dirty' = dirty \cup {a}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired,
                   mirror_expired_active, mirror_epoch, durable_live,
                   waitq, waitqLen, arrq, arrLen, servedq, servedLen,
                   arrived, served, asked, badRelease, ghost, hold_ticks,
                   stolen, clean_stalled>>

CleanCommit(a) ==
    /\ UNCHANGED EditVars
    /\ a \in Agents
    /\ a \in dirty
    /\ dirty' = dirty \ {a}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired,
                   mirror_expired_active, mirror_epoch, durable_live,
                   waitq, waitqLen, arrq, arrLen, servedq, servedLen,
                   arrived, served, asked, badRelease, ghost, hold_ticks,
                   stolen, clean_stalled>>

\* Negative fixture: handing a dirty path away while its holder is live.
StealDirty(a, b) ==
    /\ UNCHANGED EditVars
    /\ Bug = "steal-dirty"
    /\ a \in Agents /\ b \in Agents /\ a /= b
    /\ a \in dirty /\ a \in durable_live
    /\ bus_holder = a
    /\ bus_holder' = b
    /\ stolen' = TRUE
    /\ UNCHANGED <<bus_status, bus_epoch, next_epoch, bus_expired,
                   mirror_expired_active, mirror_epoch, durable_live,
                   waitq, waitqLen, arrq, arrLen, servedq, servedLen,
                   arrived, served, asked, badRelease, ghost, hold_ticks,
                   dirty, clean_stalled>>

\* A clean holder's expired lease must hand off to the head waiter.
HandoffClean ==
    /\ UNCHANGED EditVars
    /\ bus_status = "Held"
    /\ bus_expired
    /\ bus_holder \notin dirty
    /\ waitqLen > 0
    /\ IF Bug = "no-clean-handoff"
       THEN /\ clean_stalled' = TRUE
            /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired,
                           mirror_expired_active, mirror_epoch, durable_live,
                           waitq, waitqLen, arrq, arrLen, servedq, servedLen,
                           arrived, served, asked, badRelease, ghost, dirty, hold_ticks, stolen>>
       ELSE /\ bus_holder' = waitq[1]
            /\ bus_status' = "Held"
            /\ bus_expired' = FALSE
            /\ bus_epoch' = next_epoch
            /\ next_epoch' = next_epoch + 1
            /\ waitq' = [i \in QIdx |-> IF i < waitqLen THEN waitq[i + 1] ELSE "None"]
            /\ waitqLen' = waitqLen - 1
            /\ served' = served \cup {waitq[1]}
            /\ mirror_epoch' =
                 [mirror_epoch EXCEPT ![bus_holder] = bus_epoch, ![waitq[1]] = next_epoch]
            /\ clean_stalled' = FALSE
            /\ UNCHANGED <<mirror_expired_active, durable_live,
                           arrq, arrLen, servedq, servedLen, arrived, asked,
                           badRelease, ghost, dirty, hold_ticks, stolen>>

ReleaseHolder ==
    /\ UNCHANGED EditVars
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ bus_status' = "Released"
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = mirror_expired_active \ {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_epoch, next_epoch, mirror_epoch, durable_live>>
    /\ UNCHANGED newVars

ForceRevokeHolder ==
    /\ UNCHANGED EditVars
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ bus_status' = "Revoked"
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = mirror_expired_active \ {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_epoch, next_epoch, mirror_epoch, durable_live>>
    /\ UNCHANGED newVars

ExpireBusLease ==
    /\ UNCHANGED EditVars
    /\ bus_status = "Held"
    /\ ~bus_expired
    /\ bus_holder \in Agents
    /\ bus_expired' = TRUE
    /\ mirror_expired_active' = mirror_expired_active \cup {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, mirror_epoch, durable_live>>
    /\ UNCHANGED newVars

LapseActivityA ==
    /\ UNCHANGED EditVars
    /\ "A" \in durable_live
    /\ durable_live' = durable_live \ {"A"}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired, mirror_expired_active, mirror_epoch>>
    /\ UNCHANGED newVars

RecordInboxPollA ==
    /\ UNCHANGED EditVars
    /\ ~("A" \in durable_live)
    /\ "A" \in mirror_expired_active
    /\ durable_live' = durable_live \cup {"A"}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired, mirror_expired_active, mirror_epoch>>
    /\ UNCHANGED newVars

Done ==
    /\ UNCHANGED EditVars
    /\ next_epoch > MaxEpoch
    /\ UNCHANGED vars

\* bd-ll7bs: a blocked acquire joins ONE FIFO queue and asks the holder once.
QueueHold(a) ==
    /\ UNCHANGED EditVars
    /\ a \in Agents
    /\ ActiveOwners /= {}
    /\ a \notin ActiveOwners
    /\ a \notin arrived
    /\ waitqLen < MaxQ
    /\ waitq' = [waitq EXCEPT ![waitqLen + 1] = a]
    /\ waitqLen' = waitqLen + 1
    /\ arrq' = [arrq EXCEPT ![arrLen + 1] = a]
    /\ arrLen' = arrLen + 1
    /\ arrived' = arrived \cup {a}
    /\ asked' = asked \cup {a}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired,
                   mirror_expired_active, mirror_epoch, durable_live, served, servedq, servedLen, badRelease, ghost, hold_ticks, dirty, stolen, clean_stalled>>

\* The one handoff helper: every release source transfers to the head waiter,
\* through the existing epoch-safe grant, never a second grant path.
HandoffRelease(src) ==
    /\ UNCHANGED EditVars
    /\ src \in Sources
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ next_epoch <= MaxEpoch
    /\ LET head == IF Bug = "lifo" /\ waitqLen > 0 THEN waitq[waitqLen]
                   ELSE IF waitqLen > 0 THEN waitq[1] ELSE "None"
           skipped == Bug = "no-ttl" /\ src = "ttl"
           grant == head /= "None" /\ ~skipped
           old == bus_holder
       IN /\ bus_holder' = IF grant THEN head ELSE "None"
          /\ bus_status' = IF grant THEN "Held" ELSE "Released"
          /\ bus_epoch' = IF grant THEN next_epoch ELSE bus_epoch
          /\ next_epoch' = IF grant THEN next_epoch + 1 ELSE next_epoch
          /\ bus_expired' = FALSE
          /\ mirror_expired_active' = mirror_expired_active \ {old}
          /\ mirror_epoch' =
                IF grant
                THEN [mirror_epoch EXCEPT ![old] = bus_epoch, ![head] = next_epoch]
                ELSE [mirror_epoch EXCEPT ![old] = bus_epoch]
          /\ durable_live' = (durable_live \ {old}) \cup IF grant THEN {head} ELSE {}
          /\ ghost' = IF Bug = "double-holder" /\ grant THEN ghost \cup {old} ELSE ghost
          /\ IF grant
             THEN IF Bug = "drop-waiter"
                  THEN /\ waitq' = [i \in QIdx |-> "None"]
                       /\ waitqLen' = 0
                       /\ UNCHANGED <<servedq, servedLen, served>>
                  ELSE /\ waitq' = [i \in QIdx |-> IF i < waitqLen THEN waitq[i + 1] ELSE "None"]
                       /\ waitqLen' = waitqLen - 1
                       /\ servedq' = [servedq EXCEPT ![servedLen + 1] = head]
                       /\ servedLen' = servedLen + 1
                       /\ served' = served \cup {head}
             ELSE /\ UNCHANGED <<waitq, waitqLen, servedq, servedLen, served>>
          /\ badRelease' = IF waitqLen > 0 /\ ~grant
                           THEN badRelease \cup {src}
                           ELSE badRelease
    /\ UNCHANGED <<arrq, arrLen, arrived, asked, hold_ticks, dirty, stolen, clean_stalled>>

\* bd-lk0kt: pre-edit hook. A free path is leased on first edit; a path held
\* by another pin blocks the edit and enqueues exactly one waiter; a repeat
\* pre-edit by the holder is a no-op.
PreEditGranted ==
    \E a \in Agents:
        /\ a \notin edits_leased
        /\ a \notin edits_blocked
        /\ ActiveOwners = {} \/ a \in ActiveOwners
        /\ edits_leased' = edits_leased \cup {a}
        /\ UNCHANGED LeaseVars
        /\ UNCHANGED <<edits_blocked, edits_waiters, edits_writes>>

PreEditBlocked ==
    \E a, b \in Agents:
        /\ a /= b
        /\ b \in ActiveOwners
        /\ a \notin edits_leased
        /\ a \notin edits_blocked
        /\ edits_blocked' = edits_blocked \cup {a}
        /\ edits_waiters' = IF Bug = "no-edit-queue" THEN edits_waiters
                            ELSE [edits_waiters EXCEPT ![a] = @ + 1]
        /\ UNCHANGED LeaseVars
        /\ UNCHANGED <<edits_leased, edits_writes>>

PreEditRepeat ==
    \E a \in edits_leased:
        /\ edits_waiters' = IF Bug = "double-acquire"
                            THEN [edits_waiters EXCEPT ![a] = @ + 1]
                            ELSE edits_waiters
        /\ UNCHANGED LeaseVars
        /\ UNCHANGED <<edits_leased, edits_blocked, edits_writes>>

WriteEdit ==
    \E a \in Agents:
        /\ a \in edits_leased \/ Bug = "no-edit-lease"
        /\ edits_writes' = edits_writes \cup {a}
        /\ UNCHANGED LeaseVars
        /\ UNCHANGED <<edits_leased, edits_blocked, edits_waiters>>

Next ==
    \/ PreEditGranted
    \/ PreEditBlocked
    \/ PreEditRepeat
    \/ WriteEdit
    \/ AcquireA
    \/ AcquireC
    \/ AcquireB
    \/ ReleaseHolder
    \/ ForceRevokeHolder
    \/ ExpireBusLease
    \/ LapseActivityA
    \/ RecordInboxPollA
    \/ HoldTick
    \/ \E a \in Agents : EditDirty(a)
    \/ \E a \in Agents : CleanCommit(a)
    \/ \E a, b \in Agents : StealDirty(a, b)
    \/ HandoffClean
    \/ \E a \in Agents : QueueHold(a)
    \/ \E s \in Sources : HandoffRelease(s)
    \/ Done

Spec == Init /\ [][Next]_vars

SingleExclusiveOwnerPerPath ==
    Cardinality(ActiveOwners) <= 1

LivenessExtensionPreservesEpoch ==
    (bus_status = "Held" /\ bus_expired /\ bus_holder \in LivenessExtendedOwners)
        => (ResolvedEpoch(bus_holder) = bus_epoch)

\* bd-ll7bs invariants.
OneHolder == Cardinality(ActiveOwners \cup ghost) <= 1

Queued == {waitq[i] : i \in 1..waitqLen}
FifoHandoff == \A i \in 1..servedLen : servedq[i] = arrq[i]
NoLostWaiter == arrived \subseteq Queued \cup ActiveOwners \cup served
HandoffOnEveryRelease == badRelease = {}

\* bd-kyute: a hold is a heartbeat, not a deed. One tick maps to the 15-minute
\* MAX_RESERVATION_TTL_SECS; MaxHold is the operator's bounded-hold cap.
BoundedHold == hold_ticks <= MaxHold

\* bd-nmilo: a hold ends when the edit ends, not when the agent dies.
LeaseOnlyWhileDirty == \A a \in LivenessExtendedOwners : a \in dirty
DirtyNeverStolen == stolen = FALSE
HandoffOnClean == clean_stalled = FALSE

\* bd-lk0kt: every edit happens under its writer's lease; every blocked edit
\* has exactly one waiter; re-acquiring a held path adds no waiter.
NoEditWithoutLease == edits_writes \subseteq edits_leased
BlockedEditQueued == \A a \in edits_blocked : edits_waiters[a] = 1
IdempotentAcquire == \A a \in Agents : edits_waiters[a] <= 1

====
