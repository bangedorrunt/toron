---- MODULE DoorbellWake ----
EXTENDS Naturals, TLC

CONSTANTS MaxRings, MaxHead

VARIABLES
    head,            \* Nat: newest mail watermark
    cursor,          \* Nat: recipient's committed cursor
    binding_ok,      \* BOOLEAN: verified binding & matching generation
    pane_state,      \* {"Idle", "Working"}
    rings,           \* Nat: rings since last cursor advance
    backoff_elapsed, \* BOOLEAN: whether backoff delay has passed
    decision         \* {"NothingUnread", "Unbound", "Busy", "Hold", "Ring", "Escalate"}

vars == <<head, cursor, binding_ok, pane_state, rings, backoff_elapsed, decision>>

ComputeDecision(h, c, b_ok, p_state, r, b_elapsed) ==
    IF h <= c THEN "NothingUnread"
    ELSE IF ~b_ok THEN "Unbound"
    ELSE IF r >= MaxRings THEN "Escalate"
    ELSE IF p_state # "Idle" THEN "Busy"
    ELSE IF r = 0 \/ b_elapsed THEN "Ring"
    ELSE "Hold"

Init ==
    /\ head = 0
    /\ cursor = 0
    /\ binding_ok = TRUE
    /\ pane_state = "Idle"
    /\ rings = 0
    /\ backoff_elapsed = TRUE
    /\ decision = "NothingUnread"

NewMail ==
    /\ head < MaxHead
    /\ head' = head + 1
    /\ decision' = ComputeDecision(head + 1, cursor, binding_ok, pane_state, rings, backoff_elapsed)
    /\ UNCHANGED <<cursor, binding_ok, pane_state, rings, backoff_elapsed>>

RingSent ==
    /\ decision = "Ring"
    /\ rings < MaxRings
    /\ rings' = rings + 1
    /\ backoff_elapsed' = FALSE
    /\ decision' = ComputeDecision(head, cursor, binding_ok, pane_state, rings + 1, FALSE)
    /\ UNCHANGED <<head, cursor, binding_ok, pane_state>>

BackoffElapsed ==
    /\ ~backoff_elapsed
    /\ rings > 0
    /\ rings < MaxRings
    /\ backoff_elapsed' = TRUE
    /\ decision' = ComputeDecision(head, cursor, binding_ok, pane_state, rings, TRUE)
    /\ UNCHANGED <<head, cursor, binding_ok, pane_state, rings>>

AdvanceCursor ==
    /\ cursor < head
    /\ cursor' = head
    /\ rings' = 0
    /\ backoff_elapsed' = TRUE
    /\ decision' = ComputeDecision(head, head, binding_ok, pane_state, 0, TRUE)
    /\ UNCHANGED <<head, binding_ok, pane_state>>

PaneStartsWorking ==
    /\ pane_state = "Idle"
    /\ pane_state' = "Working"
    /\ decision' = ComputeDecision(head, cursor, binding_ok, "Working", rings, backoff_elapsed)
    /\ UNCHANGED <<head, cursor, binding_ok, rings, backoff_elapsed>>

PaneFinishesTurn ==
    /\ pane_state = "Working"
    /\ pane_state' = "Idle"
    /\ decision' = ComputeDecision(head, cursor, binding_ok, "Idle", rings, backoff_elapsed)
    /\ UNCHANGED <<head, cursor, binding_ok, rings, backoff_elapsed>>

BindingDrops ==
    /\ binding_ok
    /\ binding_ok' = FALSE
    /\ decision' = ComputeDecision(head, cursor, FALSE, pane_state, rings, backoff_elapsed)
    /\ UNCHANGED <<head, cursor, pane_state, rings, backoff_elapsed>>

BindingRestores ==
    /\ ~binding_ok
    /\ binding_ok' = TRUE
    /\ decision' = ComputeDecision(head, cursor, TRUE, pane_state, rings, backoff_elapsed)
    /\ UNCHANGED <<head, cursor, pane_state, rings, backoff_elapsed>>

Done ==
    /\ (cursor = MaxHead \/ decision = "Escalate")
    /\ UNCHANGED vars

Next ==
    \/ NewMail
    \/ RingSent
    \/ BackoffElapsed
    \/ AdvanceCursor
    \/ PaneStartsWorking
    \/ PaneFinishesTurn
    \/ BindingDrops
    \/ BindingRestores
    \/ Done

Spec == Init /\ [][Next]_vars

\* Invariants
BoundedWake ==
    rings <= MaxRings

NoRingWhenCaughtUp ==
    cursor >= head => decision = "NothingUnread"

NoRingWhenUnbound ==
    (~binding_ok /\ head > cursor) => decision = "Unbound"

UnboundNeverRings ==
    ~binding_ok => decision # "Ring"

NoRingWhenBusy ==
    (binding_ok /\ head > cursor /\ rings < MaxRings /\ pane_state = "Working") => decision = "Busy"

DeafAgentEscalates ==
    (binding_ok /\ head > cursor /\ rings >= MaxRings) => decision = "Escalate"

NoRingWhenEscalated ==
    decision = "Escalate" => rings >= MaxRings

====
