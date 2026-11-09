---- MODULE ToronLeaseResolution ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS MaxEpoch, FixSplitBrainAndEpoch

VARIABLES
    bus_holder,
    bus_status,
    bus_epoch,
    next_epoch,
    bus_expired,
    mirror_expired_active,
    mirror_epoch,
    durable_live

vars == <<
    bus_holder,
    bus_status,
    bus_epoch,
    next_epoch,
    bus_expired,
    mirror_expired_active,
    mirror_epoch,
    durable_live
>>

Agents == {"A", "B"}

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

ReleaseHolder ==
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ bus_status' = "Released"
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = mirror_expired_active \ {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_epoch, next_epoch, mirror_epoch, durable_live>>

ForceRevokeHolder ==
    /\ bus_status = "Held"
    /\ bus_holder \in ActiveOwners
    /\ bus_status' = "Revoked"
    /\ bus_expired' = FALSE
    /\ mirror_expired_active' = mirror_expired_active \ {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_epoch, next_epoch, mirror_epoch, durable_live>>

ExpireBusLease ==
    /\ bus_status = "Held"
    /\ ~bus_expired
    /\ bus_holder \in Agents
    /\ bus_expired' = TRUE
    /\ mirror_expired_active' = mirror_expired_active \cup {bus_holder}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, mirror_epoch, durable_live>>

LapseActivityA ==
    /\ "A" \in durable_live
    /\ durable_live' = durable_live \ {"A"}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired, mirror_expired_active, mirror_epoch>>

RecordInboxPollA ==
    /\ ~("A" \in durable_live)
    /\ "A" \in mirror_expired_active
    /\ durable_live' = durable_live \cup {"A"}
    /\ UNCHANGED <<bus_holder, bus_status, bus_epoch, next_epoch, bus_expired, mirror_expired_active, mirror_epoch>>

Done ==
    /\ next_epoch > MaxEpoch
    /\ UNCHANGED vars

Next ==
    \/ AcquireA
    \/ AcquireB
    \/ ReleaseHolder
    \/ ForceRevokeHolder
    \/ ExpireBusLease
    \/ LapseActivityA
    \/ RecordInboxPollA
    \/ Done

Spec == Init /\ [][Next]_vars

SingleExclusiveOwnerPerPath ==
    Cardinality(ActiveOwners) <= 1

LivenessExtensionPreservesEpoch ==
    (bus_status = "Held" /\ bus_expired /\ bus_holder \in LivenessExtendedOwners)
        => (ResolvedEpoch(bus_holder) = bus_epoch)

====
