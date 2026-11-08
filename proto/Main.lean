import Proto
open Proto

/-! `lake exe proto vectors [--check FILE]`: every (state, event) pair over
a small fixed universe, as JSONL `{"state":…,"event":…,"expect":…}`. The
universe is exhaustive, so there is no seed: the vectors are a golden table. -/

def jstr (s : String) : String := "\"" ++ s ++ "\""
def jopt (o : Option String) : String := match o with | some s => jstr s | none => "null"
def joptN (o : Option Nat) : String := match o with | some n => toString n | none => "null"

def phaseName : Phase → String
  | .intent => "intent" | .unknown => "unknown" | .sent => "sent" | .delivered => "delivered"
  | .acknowledged => "acknowledged" | .acted => "acted" | .proved => "proved"
  | .rejected => "rejected" | .expired => "expired" | .superseded => "superseded"
  | .cancelled => "cancelled" | .failed => "failed"

def className : Class → String
  | .notify => "notify" | .request => "request" | .reply => "reply" | .task => "task"
  | .control => "control" | .receipt => "receipt"

def stateJson (s : State) : String :=
  "{\"phase\":" ++ jstr (phaseName s.phase) ++ ",\"class\":" ++ jstr (className s.cls) ++
  ",\"recipient\":" ++ jstr s.recipient ++ ",\"offset\":" ++ joptN s.offset ++
  ",\"gen\":" ++ toString s.gen ++ ",\"holder\":" ++ jopt s.holder ++ ",\"doer\":" ++ jopt s.doer ++ "}"

def eventJson : Event → String
  | .appended o => "{\"event\":\"appended\",\"offset\":" ++ toString o ++ "}"
  | .appendUncertain => "{\"event\":\"append_uncertain\"}"
  | .reconcileFound o => "{\"event\":\"reconcile_found\",\"offset\":" ++ toString o ++ "}"
  | .reconcileAbsent => "{\"event\":\"reconcile_absent\"}"
  | .read r c => "{\"event\":\"read\",\"reader\":" ++ jstr r ++ ",\"cursor\":" ++ toString c ++ "}"
  | .deadlinePassed => "{\"event\":\"deadline_passed\"}"
  | .accept b g => "{\"event\":\"accept\",\"by\":" ++ jstr b ++ ",\"gen\":" ++ toString g ++ "}"
  | .refuse b => "{\"event\":\"refuse\",\"by\":" ++ jstr b ++ ",\"reason\":\"no\"}"
  | .progress b g => "{\"event\":\"progress\",\"by\":" ++ jstr b ++ ",\"gen\":" ++ toString g ++ "}"
  | .done b g => "{\"event\":\"done\",\"by\":" ++ jstr b ++ ",\"gen\":" ++ toString g ++ "}"
  | .verdict b p r => "{\"event\":\"verdict\",\"by\":" ++ jstr b ++ ",\"pass\":" ++ toString p ++ ",\"rework\":" ++ toString r ++ "}"
  | .supersede g => "{\"event\":\"supersede\",\"new_gen\":" ++ toString g ++ "}"
  | .rebase g => "{\"event\":\"rebase\",\"new_gen\":" ++ toString g ++ "}"
  | .cancel => "{\"event\":\"cancel\"}"

def phases : List Phase := [.intent, .unknown, .sent, .delivered, .acknowledged, .acted, .proved,
  .rejected, .expired, .superseded, .cancelled, .failed]
def classes : List Class := [.notify, .request, .task, .control]

/-- States: only field combinations a phase can carry. -/
def states : List State := Id.run do
  let mut out := []
  for c in classes do
    for p in phases do
      let offs : List (Option Nat) := if p.rank == 0 then [none] else [some 5]
      let holders : List (Option String) :=
        if p.rank ≥ 3 && p != .rejected && p != .expired then [some "r", some "x"] else [none]
      let doers : List (Option String) := if p == .acted then [some "r", some "x"] else [none]
      for o in offs do
        for h in holders do
          for d in doers do
            out := out ++ [{ phase := p, cls := c, recipient := "r", offset := o, gen := 1, holder := h, doer := d }]
  return out

def events : List Event := [
  .appended 5, .appendUncertain, .reconcileFound 5, .reconcileAbsent,
  .read "r" 5, .read "r" 4, .read "r" 9, .read "x" 5, .deadlinePassed,
  .accept "r" 1, .accept "r" 0, .accept "x" 1, .refuse "r", .refuse "x",
  .progress "r" 1, .progress "r" 0, .progress "x" 1,
  .done "r" 1, .done "r" 0, .done "x" 1,
  .verdict "v" true false, .verdict "v" false true, .verdict "v" false false, .verdict "r" true false,
  .supersede 2, .supersede 1, .cancel, .rebase 2, .rebase 1 ]

def vectorLine (s : State) (e : Event) : String :=
  let expect := match step s e with
    | .ok s' => "{\"ok\":" ++ stateJson s' ++ "}"
    | .error r => "{\"reject\":" ++ jstr r.tag ++ "}"
  "{\"state\":" ++ stateJson s ++ ",\"event\":" ++ eventJson e ++ ",\"expect\":" ++ expect ++ "}"

def allVectors : List String := states.flatMap fun s => events.map fun e => vectorLine s e

/-! Log traces: every length-3 sequence over a few triples, each step's
expected outcome (`{"ok":offset}` or `{"err":"stale_epoch"|"seq_gap"}`)
from `Proto.append` on a fresh log (offsets start at 1). -/

def triples : List Triple := [⟨"p", 0, 0⟩, ⟨"p", 0, 1⟩, ⟨"p", 1, 0⟩, ⟨"p", 0, 2⟩, ⟨"q", 0, 0⟩]

def tripleJson (t : Triple) : String :=
  "{\"producer\":" ++ jstr t.producer ++ ",\"epoch\":" ++ toString t.epoch ++ ",\"seq\":" ++ toString t.seq ++ "}"

def traceLine (ts : List Triple) : String := Id.run do
  let mut l := Log.empty
  let mut steps : List String := []
  for t in ts do
    match append l t with
    | .ok (l', o) =>
      l := l'
      steps := steps ++ ["{\"append\":" ++ tripleJson t ++ ",\"expect\":{\"ok\":" ++ toString o ++ "}}"]
    | .error e =>
      let tag := match e with | .staleEpoch => "stale_epoch" | .seqGap => "seq_gap"
      steps := steps ++ ["{\"append\":" ++ tripleJson t ++ ",\"expect\":{\"err\":" ++ jstr tag ++ "}}"]
  return "[" ++ String.intercalate "," steps ++ "]"

def allTraces : List String :=
  triples.flatMap fun a => triples.flatMap fun b => triples.map fun c => traceLine [a, b, c]

/-! Wake vectors: a grid over the inputs of `shouldRing` (head 5; cursor
caught up or behind; binding good, unverified or stale; every pane state;
last ring never / for an older / for this watermark; last ring recent or
long ago; ring counts around both budgets; both urgencies). Golden table. -/

def paneName : PaneState → String
  | .idle => "idle" | .done => "done" | .working => "working"
  | .blocked => "blocked" | .unknown => "unknown" | .gone => "gone"

def urgencyName : Urgency → String
  | .normal => "normal" | .urgent => "urgent"

def ringInputJson (i : RingInput) : String :=
  "{\"head\":" ++ toString i.head ++ ",\"cursor\":" ++ toString i.cursor ++
  ",\"binding_verified\":" ++ toString i.bindingVerified ++
  ",\"binding_gen\":" ++ toString i.bindingGen ++ ",\"current_gen\":" ++ toString i.currentGen ++
  ",\"pane_state\":" ++ jstr (paneName i.pane) ++ ",\"last_ring_wm\":" ++ joptN i.lastRingWm ++
  ",\"last_ring_at_ms\":" ++ joptN i.lastRingAt ++ ",\"rings_since_advance\":" ++ toString i.rings ++
  ",\"now_ms\":" ++ toString i.now ++ ",\"urgency\":" ++ jstr (urgencyName i.urgency) ++ "}"

def decisionJson : Decision → String
  | .nothingUnread => "{\"decision\":\"nothing_unread\"}"
  | .unbound => "{\"decision\":\"unbound\"}"
  | .busy => "{\"decision\":\"busy\"}"
  | .hold u => "{\"decision\":\"hold\",\"until_ms\":" ++ toString u ++ "}"
  | .ring w => "{\"decision\":\"ring\",\"wm\":" ++ toString w ++ "}"
  | .escalate w => "{\"decision\":\"escalate\",\"wm\":" ++ toString w ++ "}"

def wakeInputs : List RingInput := Id.run do
  let mut out := []
  for cursor in [5, 4] do
    for (verified, bgen) in [(true, 2), (false, 2), (true, 1)] do
      for pane in [PaneState.idle, .done, .working, .blocked, .unknown, .gone] do
        for lastWm in [none, some 4, some 5] do
          for lastAt in [none, some 999000, some 0] do
            for rings in [0, 1, 2, 3, 4, 5, 6] do
              for u in [Urgency.normal, .urgent] do
                out := out ++ [RingInput.mk 5 cursor verified bgen 2 pane lastWm lastAt rings 1000000 u]
  return out

def allWakeVectors : List String := wakeInputs.map fun i =>
  "{\"input\":" ++ ringInputJson i ++ ",\"expect\":" ++ decisionJson (shouldRing i) ++ "}"

/-! Choreography vectors: one line per cell of `allCells` (state 0 through one
past the last branch, plus supervisor `not_yet`). Verbs are sorted. -/

def verbsJson (vs : List String) : String :=
  "[" ++ String.intercalate "," (vs.map jstr) ++ "]"

def choreoLine (t : Topology) (r : Role) (s : Nat) : String :=
  let head := "{\"topology\":" ++ jstr t.wire ++ ",\"role\":" ++ jstr r.wire ++
    ",\"state\":" ++ toString s
  match project t r s with
  | .verbs vs => head ++ ",\"verbs\":" ++ verbsJson vs ++ "}"
  | .notYet => head ++ ",\"error\":\"not_yet\"}"

def allChoreo : List String :=
  allCells.map fun c => choreoLine c.1 c.2.1 c.2.2

def checkFile (file : String) (lines : List String) (what : String) : IO UInt32 := do
  let committed ← IO.FS.readFile file
  let fresh := String.intercalate "\n" lines ++ "\n"
  if committed == fresh then
    IO.println s!"ok: {lines.length} {what} match {file}"
    return 0
  else
    IO.eprintln s!"STALE: {file} differs from `lake exe proto` ({lines.length} {what}); regenerate it"
    return 1

def main (args : List String) : IO UInt32 := do
  match args with
  | ["log-traces"] =>
    for l in allTraces do IO.println l
    return 0
  | ["log-traces", "--check", file] => checkFile file allTraces "log traces"
  | ["vectors"] =>
    for l in allVectors do IO.println l
    return 0
  | ["vectors", "--check", file] => checkFile file allVectors "delivery vectors"
  | ["wake-vectors"] =>
    for l in allWakeVectors do IO.println l
    return 0
  | ["wake-vectors", "--check", file] => checkFile file allWakeVectors "wake vectors"
  | ["choreo-vectors"] =>
    for l in allChoreo do IO.println l
    return 0
  | ["choreo-vectors", "--check", file] => checkFile file allChoreo "choreo vectors"
  | _ =>
    IO.eprintln "usage: proto (vectors | log-traces | wake-vectors | choreo-vectors) [--check FILE]"
    return 2
