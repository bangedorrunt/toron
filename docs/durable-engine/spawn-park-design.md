# Durable spawn park: the last Resonate-shaped action

Bead: `bd-spawn-durable-park-nncl`. Flywheel step 5 / inventory step 4.
Status: design for captain review. No code until approved.

## Why this exists

`Action::Spawn` is the only shipped action whose park still lives in the
vendor store. `run_spawn` (`control_flow.rs:2166`) resolves a profile,
builds a spec, creates a Resonate promise, side-channels the promise id to
`SpawnSink`, and awaits it. Flywheel's spawn drain reads the promise data
for the spec and resolves it when the pane finishes (`pane.rs`). Deleting
the server means moving that spec and that completion into the in-house
model; a notification event is not enough because the event never carried
the profile/resolved/harness payload today.

## (a) Model

```rust
RunPhase::AwaitingSpawn {
    node: String,
    spec: SpawnSpec,       // serialized into the model: restart-safe
    deadline_ms: Option<u64>,
}

RunEffect::StartSpawn { node: String, spec: SpawnSpec }
RunEffect::SpawnDetached { node: String, spec: SpawnSpec } // await=false, no park

RunMsg::SpawnSettled { node: String, value: serde_json::Value, now_ms: u64 }
RunMsg::SpawnFailed  { node: String, error: String, now_ms: u64 }
```

`SpawnSpec` is the old promise data, made serializable and self-contained:

```rust
struct SpawnSpec {
    spawn_key: String,          // run_id + node + attempt; the correlation key
    run_id: String,
    node: String,
    workflow: String,
    project: Option<String>,
    generation: u64,
    profile: String,
    resolved: serde_json::Value, // ProfileResolver output
    harness: Option<String>,
    workflow_ref: Option<String>,
    await_: bool,
    no_claim_timeout: u64,
    context: Option<ContextMode>,
}
```

- `spawn_key` replaces the promise id everywhere: pending event, claim,
  completion, resolution validation.
- `await_ == false` emits `SpawnDetached` and lands `{ok:true, spawned:true}`
  immediately (same shape as today); no phase change.
- `await_ == true` persists `AwaitingSpawn` **before** `StartSpawn` is handed
  to the host (the model-first rule).
- `SpawnSettled` lands `{ok:true, spawn_key, result}`; `SpawnFailed` lands
  `{ok:false, error}` and applies `on_error`.
- A settlement for any other node/key is ignored (stale generation).

## (b) Host start seam

Extract the Context-free part of `run_spawn` into a seam, the same move as
`LeafSeams`/`linear_step`:

```rust
pub struct SpawnSeams { pub resolver: ProfileResolver }
pub fn prepare_spawn(seams: &SpawnSeams, input: SpawnInput) -> Result<SpawnSpec, SpawnError>;
```

- `prepare_spawn` does profile resolution + spec construction only. No
  promise, no sink, no await.
- The host publishes kind-43001 with the **whole spec** in `content.spec`
  (today's `publish_spawn_pending` sends only run/node/promise; that is the
  piece flywheel's promise read was compensating for).
- The host tracks `(spawn_key -> node/run)` in memory and marks the run
  `Suspended`.
- The resolution loop parses kind-43003..43006 terminal job events (or a
  dedicated completion event) carrying `spawn_key`; it calls
  `DurableRun::spawn_settled` / `spawn_failed` through the state store.
- Restart: the sweep re-opens the model, re-emits `StartSpawn`, and
  republishes the same `spawn_key`; the bus dedupes by event id, and the
  job claim makes a second start impossible (see (d)).

## (c) Correlation and restart

- Key: `spawn_key = "{run_id}:{node}:{attempt}"`, hashed if it must fit a
  tag. The run id is already the approval correlation key; spawn uses the
  node too because one run may spawn several times.
- The pending event is idempotent by `spawn_key`: a restarted host that
  republishes produces either the same event id (same timestamp/body) or a
  duplicate the consumer ignores by key.
- Completion before the park is durable is the same posture as approval:
  ignored + debug-logged; the operator/consumer re-settles after the
  republished pending. No queue.
- The model never stores a promise id; `await` is a phase, not a store
  lookup.

## (d) Exactly-once child start

The bus already has the job lifecycle (`toron-workflow/src/jobs.rs`):
43001 request, 43002 accepted/claim, 43003..43006 terminal. Reuse it:

1. Host publishes 43001 with the spec.
2. The spawn consumer claims with 43002 carrying `spawn_key`; a second
   claim for the same key is refused (the claim is the mutual exclusion).
3. The consumer starts the pane/child and publishes the terminal job event
   with `spawn_key` and the result.
4. The host settles the model from the terminal event.

No claim → no start. A crashed consumer's claim can be reclaimed by the
existing job-claim timeout; the child key (pane/profile) is the same, so a
reclaim adopts rather than duplicates. This is stronger than the promise
store: the claim is durable on the bus, not in a vendor process.

## (e) Timeout

`no_claim_timeout` was a promise timeout. Model it as a park deadline:

- `AwaitingSpawn { deadline_ms }` with a `ParkUntil` effect at the deadline.
- `Woke` while still `AwaitingSpawn` and no completion row → `SpawnFailed
  { error: "spawn-await {key}: timed out after {n}s" }`, the same error
  text as today.
- A completion that arrives at the same instant wins if it is durably
  recorded first; the fold is deterministic on the messages it receives.

## (f) Refusal and cutover

- Until this lands, `unsupported_reason` keeps refusing `spawn`; the whole
  workflow stays on `WorkflowEngine::run`.
- After: `spawn` is admitted when the spec can be prepared (`ProfileResolver`
  present). A missing profile still lands `{ok:false, error: unknown
  profile}` as today, not a run failure.
- `SpawnSink`/`SpawnSettled` promise paths are deleted with the old engine;
  the host's `durable_spawns` map replaces the sink registry.

## Witnesses

1. `spawn_parks_then_settles_across_restart` — host1 starts, parks; drop;
   host2 sweeps, republishes the same key, receives the terminal event,
   settles; the consumer saw exactly one claim and one start.
2. `detached_spawn_does_not_park` — `await: false` lands `{ok:true,
   spawned:true}` with no `AwaitingSpawn`.
3. `duplicate_spawn_completion_is_ignored` — two terminal events, one
   settlement.
4. `spawn_timeout_fails_the_node` — no completion, wake at the deadline,
   `{ok:false,error:"…timed out…"}`, `on_error` applies.
5. `unknown_profile_lands_a_value` — no park, `{ok:false,error:unknown
   profile}`, the run continues.
6. Flywheel side: `pane.rs` spawn drain starts from the 43001 spec, claims
   before start, publishes the terminal; no `promises.get` call.

## Cannot preserve (named)

- The vendor promise's server-side TTL is replaced by the model deadline;
  the error string is kept. A consumer that never claims is failed at the
  deadline instead of being left pending on the server.
- The promise's data payload is replaced by the event spec. Anything that
  read the promise directly (only flywheel's drain, measured) must switch to
  the event.
- In-flight promises at the flip are drained/resolved once through the old
  server, then the port lands (same operational rule as ADR-0043).

## Captain addendum — 2026-11-02 (the transport is not the bus)

The implementation landed the model and the consumer functions, then stopped at
the cutover. Checking the live wiring before flipping the router found the
design's transport assumption is false:

- `InProcessBus` is tokio channels over a SQLite event store. It fans out
  **in-process only**; a second process cannot subscribe to the host's bus.
- The durable `WorkflowHost` lives in the toron MCP server process. The pane
  launcher (`pane::launch_spawn_job`) lives in the flywheel process.
- Flywheel's live drainer still drains `engine.spawn_sink()` (the old promise
  sink). `consume_spawn_job` / `spawn_spec_from_job` are `#[allow(dead_code)]`
  and called by tests only.
- `SpawnClaims` is an in-memory `HashMap`, so a restarted host would re-emit
  `StartSpawn` and launch a second pane for the same `spawn_key`.

So "publish 43001, let the consumer claim, settle from 43004" cannot be the
transport across that process boundary, and the claim cannot be the
exactly-once gate while it is memory-only.

**Decision**

1. `WorkflowHost` gains an optional `SpawnLauncher` seam — the same shape as
   `LeafSeams`: `Arc<dyn Fn(SpawnSpec) -> BoxFuture<Result<Value>> + Send + Sync>`.
   On `RunEffect::StartSpawn` the linear effect loop awaits the launcher and
   feeds the outcome back through `spawn_settled` / `spawn_failed`, exactly like
   a step outcome. The park phase is still persisted first.
2. The 43001 publish stays as operator evidence, and the 43004/43006 terminal
   stays as the job record, but the host does not wait for them. A host with no
   consumer still settles.
3. Admission is capability-based, not shape-based. `unsupported_reason` becomes
   shape-only (spawn is a shape the model can sequence); `durable_route` takes a
   `spawn_capable` flag and refuses a spawn workflow when the caller has no
   launcher (cron and the bare server pass false). `open_with_seams` does the
   same check for direct callers. A refused spawn still leaves no row.
4. Exactly-once is durable and flywheel-side: claims keyed by `spawn_key` in the
   existing atomic `.flywheel` state file, taken before the pane starts. A
   restarted host re-emits `StartSpawn`, the consumer finds the claim, and
   adopts the running job instead of launching a second pane. The in-memory
   `SpawnClaims` remains the unit-test double.
5. The deadline still fails the node with the old timeout text when a launch
   outlives it.

**Why not a server-side consumer:** the server has no pane backend, and teaching
toron-mcp to shell into flywheel would move the launch policy out of the one
process that owns profiles, panes, and workspace identity. The seam keeps the
cut in the process that already owns spawn.

**Witnesses for the flip**

1. A spawn step on a host with a launcher launches once, settles the full value,
   and completes; kind-43001 is still published.
2. A spawn body on a host without a launcher (`durable_route(..., false)`) is
   refused before any state or engine row.
3. Kill the host after the claim but before the terminal; the restarted host
   re-emits `StartSpawn`, the durable claim adopts, and the consumer starts no
   second child.
4. A launcher that never resolves fails the node at the park deadline with the
   same error text as the old promise timeout.
