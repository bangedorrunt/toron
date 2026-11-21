---- MODULE ToronLeaseResolution ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxEpoch, FixSplitBrainAndEpoch, Bug
\* bd-ll7bs: Bug \in {"none", "double-holder", "lifo", "drop-waiter", "no-ttl"}.
\* Each non-"none" value removes exactly one waitlist fence.

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
    ghost

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
    ghost
>>

\* bd-ll7bs: the waitlist half of the lease board. Kept in the SAME model so
\* the existing split-brain/epoch invariants still constrain a handoff.
Agents == {"A", "B", "C"}
Sources == {"commit", "ship", "release", "ttl", "retire"}
MaxQ == 2
QIdx == 1..MaxQ
newVars == <<waitq, waitqLen, arrq, arrLen, servedq, servedLen, arrived, served, asked, badRelease, ghost>>

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
            /\ BusActiveOwners = {}}
    ELSE
        {a \in Agents :
            /\ a \in mirror_expired_active
            /\ a \in durable_live
            /\ ~(a \in BusActiveOwners)}

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

AcquireA ==
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
    /\ UNCHANGED newVars

AcquireB ==
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
    /\ UNCHANGED newVars

AcquireC ==
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
    /\ UNCHANGED newVars

ReleaseHolder ==
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ bus_status' = "Released"
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = mirror_expired_active \ {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_epoch, next_epoch, mirror_epoch, durable_live>>
    /\ UNCHANGED newVars

ForceRevokeHolder ==
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ bus_status' = "Revoked"
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = mirror_expired_active \ {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_epoch, next_epoch, mirror_epoch, durable_live>>
    /\ UNCHANGED newVars

ExpireBusLease ==
    /\ bus_status = "Held"
    /\ ~bus_expired
    /\ bus_holder \in Agents
    /\ bus_expired' = TRUE
    /\ mirror_expired_active' = mirror_expired_active \cup {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, mirror_epoch, durable_live>>
    /\ UNCHANGED newVars

LapseActivityA ==
    /\ "A" \in durable_live
    /\ durable_live' = durable_live \ {"A"}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired, mirror_expired_active, mirror_epoch>>
    /\ UNCHANGED newVars

RecordInboxPollA ==
    /\ ~("A" \in durable_live)
    /\ "A" \in mirror_expired_active
    /\ durable_live' = durable_live \cup {"A"}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired, mirror_expired_active, mirror_epoch>>
    /\ UNCHANGED newVars

Done ==
    /\ next_epoch > MaxEpoch
    /\ UNCHANGED vars

\* bd-ll7bs: a blocked acquire joins ONE FIFO queue and asks the holder once.
QueueHold(a) ==
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
                   mirror_expired_active, mirror_epoch, durable_live, served, servedq, servedLen, badRelease, ghost>>

\* The one handoff helper: every release source transfers to the head waiter,
\* through the existing epoch-safe grant, never a second grant path.
HandoffRelease(src) ==
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
    /\ UNCHANGED <<arrq, arrLen, arrived, asked>>

Next ==
    \/ AcquireA
    \/ AcquireC
    \/ AcquireB
    \/ ReleaseHolder
    \/ ForceRevokeHolder
    \/ ExpireBusLease
    \/ LapseActivityA
    \/ RecordInboxPollA
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

====
