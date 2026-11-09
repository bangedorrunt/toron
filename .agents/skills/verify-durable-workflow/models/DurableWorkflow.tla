---- MODULE DurableWorkflow ----
EXTENDS Naturals, FiniteSets, TLC

VARIABLES
    phase,       \* "Ready", "Running", "Succeeded", "Failed", "Cancelled"
    node_state   \* Function: Nodes -> {"Pending", "Executing", "Parked", "AwaitingApproval", "Settled", "Failed"}

vars == <<phase, node_state>>

Nodes == {"fetch", "approve", "publish"}

Needs(n) ==
    IF n = "fetch" THEN {}
    ELSE IF n = "approve" THEN {"fetch"}
    ELSE IF n = "publish" THEN {"approve"}
    ELSE {}

Init ==
    /\ phase = "Ready"
    /\ node_state = [n \in Nodes |-> "Pending"]

StartWorkflow ==
    /\ phase = "Ready"
    /\ phase' = "Running"
    /\ UNCHANGED node_state

ScheduleNode(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "Pending"
    /\ \A d \in Needs(n) : node_state[d] = "Settled"
    /\ node_state' = [node_state EXCEPT ![n] = "Executing"]
    /\ UNCHANGED phase

NodeSucceeds(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "Executing"
    /\ n # "approve"  \* approve must go through approval gate
    /\ node_state' = [node_state EXCEPT ![n] = "Settled"]
    /\ UNCHANGED phase

NodeParks(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "Executing"
    /\ n = "fetch"    \* e.g. rate limit backoff or delay
    /\ node_state' = [node_state EXCEPT ![n] = "Parked"]
    /\ UNCHANGED phase

NodeWakes(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "Parked"
    /\ node_state' = [node_state EXCEPT ![n] = "Executing"]
    /\ UNCHANGED phase

NodeRequestsApproval(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "Executing"
    /\ n = "approve"
    /\ node_state' = [node_state EXCEPT ![n] = "AwaitingApproval"]
    /\ UNCHANGED phase

OperatorApproves(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "AwaitingApproval"
    /\ node_state' = [node_state EXCEPT ![n] = "Settled"]
    /\ UNCHANGED phase

OperatorRejects(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "AwaitingApproval"
    /\ node_state' = [node_state EXCEPT ![n] = "Failed"]
    /\ phase' = "Failed"

NodeFails(n) ==
    /\ phase = "Running"
    /\ node_state[n] = "Executing"
    /\ node_state' = [node_state EXCEPT ![n] = "Failed"]
    /\ phase' = "Failed"

WorkflowSucceeds ==
    /\ phase = "Running"
    /\ \A n \in Nodes : node_state[n] = "Settled"
    /\ phase' = "Succeeded"
    /\ UNCHANGED node_state

CancelWorkflow ==
    /\ phase \in {"Ready", "Running"}
    /\ phase' = "Cancelled"
    /\ UNCHANGED node_state

Done ==
    /\ phase \in {"Succeeded", "Failed", "Cancelled"}
    /\ UNCHANGED vars

Next ==
    \/ StartWorkflow
    \/ \E n \in Nodes : ScheduleNode(n)
    \/ \E n \in Nodes : NodeSucceeds(n)
    \/ \E n \in Nodes : NodeParks(n)
    \/ \E n \in Nodes : NodeWakes(n)
    \/ \E n \in Nodes : NodeRequestsApproval(n)
    \/ \E n \in Nodes : OperatorApproves(n)
    \/ \E n \in Nodes : OperatorRejects(n)
    \/ \E n \in Nodes : NodeFails(n)
    \/ WorkflowSucceeds
    \/ CancelWorkflow
    \/ Done

Spec == Init /\ [][Next]_vars

\* Invariants
NeedsPrecedeExecution ==
    \A n \in Nodes :
        (node_state[n] \in {"Executing", "Parked", "AwaitingApproval", "Settled"}) =>
            (\A d \in Needs(n) : node_state[d] = "Settled")

TerminalSuccessRequiresAllSettled ==
    phase = "Succeeded" => (\A n \in Nodes : node_state[n] = "Settled")

TerminalFailureHasFailedNode ==
    phase = "Failed" => (\E n \in Nodes : node_state[n] = "Failed")

AwaitingApprovalRequiresGate ==
    \A n \in Nodes : node_state[n] = "AwaitingApproval" => n = "approve"

ParkedRequiresWakeBeforeSettled ==
    \A n \in Nodes : node_state[n] = "Parked" => n = "fetch"

====
