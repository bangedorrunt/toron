# Flywheel off Resonate: per-surface port design

Bead: `bd-flywheel-resonate-port-1d1v`. Steps 3–4 of
`resonate-removal-inventory.md` §7. Design only; **no flywheel source
edited**, no toron code touched.

Principle (bead): `one-substrate` — the in-house engine replaces Resonate
everywhere, not only in toron.

## What flywheel actually uses (measured, not assumed)

`resonate-sdk = "0.6.0"` (`Cargo.toml:40`). 14 files mention it, but most
are comments, tests, or config plumbing. The live surfaces:

| # | Surface | File | Real dependency |
|---|---|---|---|
| 1 | `swarm_loop` durable fn + ~30 `#[resonate_sdk::function]` leaves, per-project-group worker, invocation promise as liveness record | `durable.rs` (`run_durable_loop`, `swarm_loop`, `clear_stale_loop_invocations`, `promise_pending`, `loop_start_decision`, `fresh_loop_id`) | Invocation promise + stale sweep + sleeps |
| 2 | Doorbell `heal_tick` + server-side every-minute schedule | `doorbell.rs` (`supervisor_function_registered`, `SUPERVISOR_CRON`), driven by `ensure_doorbell_supervisor` (`main.rs`) | Server schedule + worker |
| 3 | Cron tick driving via toron's Resonate `CronManager` | `cron.rs` (`start_loops`, `registrations`, `cron_runner`, `drain_policy_sink`) | Durable-sleep loop + `WorkflowEngine::run` bodies |
| 4 | Spawn-await promise reads/resolves | `pane.rs` (`engine.resonate().promises.get`, `resolve_spawn`, `reject_spawn`, engine constructors) | Promise store + spawn parks |
| 5 | Health/probes/config | `main.rs` (`resonate_health`, doctor check, `ensure_resonate` call sites, tests), `daemon.rs` (`ensure_resonate`, `resonate_healthy`, `which_resonate`), `preflight.rs` (`check_resonate`), `config.rs` (`resonate_url`), `cmd/engine.rs` (store CLI + probe), `cmd/captain.rs:1028` | Server process + `/health` + URL config |

Already off Resonate (no work): `effect_journal.rs` (over toron-mcp's
`EffectDispatcher` + `WorkflowEffectStore`), `run_leaf_journaled`
(subprocess + file journal — the loop's leaves execute here, not in
`ctx.run` checkpoints), `elm.rs` adapter (comment only).

## (c) The swarm loop, function by function

This is the core finding: the loop body is **already Resonate-independent
in practice**. Leaves run via `run_leaf_journaled`; the idle cadence is a
blocking `poll_events` leaf with no trailing `ctx.sleep`; `swarm_loop` is
inline Rust, not a replayed composition. Resonate supplies exactly four
things:

1. **Invocation promise (pending-for-lifetime) as the liveness record.**
   Crash/stall detection = promise state × pid registry (`state.rs`
   ActiveLoop, `ensure_loop_owner`). Replacement: a local sqlite liveness
   row — `DurableRun::open` at loop start (model holds invocation id,
   project, params digest), settle at end. `promise_pending` becomes a
   row-state read; `clear_stale_loop_invocations` becomes a sweep over rows
   whose owner pid is dead (the pid registry already exists — reuse it,
   do not reinvent it). Fresh/resume/force semantics (`loop_start_decision`)
   survive verbatim, minus the server round-trips.
2. **`ctx.sleep`.** Only inside leaves and the (removed) trailing wait.
   Replacement: plain `tokio::sleep` for in-process waits; the supervise
   `--once` cadence already paces passes. Nothing durable needed.
3. **`ctx.run` checkpoints.** Vestigial: `run_leaf_journaled` owns
   crash behavior (file journal, at-least-once re-run). Replacement: keep
   the leaves byte-identical, delete the macro. The ~30
   `#[resonate_sdk::function]` annotations become plain `async fn`
   (mechanical; same bead or cleanup bead — open question 5).
4. **Stop.** Today: natural return (`complete` / degrade / abort-Err) or
   process kill; the next start rejects the stale promise. Replacement:
   identical minus the server — process exit + pid registry, next start
   sweeps dead-owner rows. No promise to settle because none exists.

Two things VANISH rather than port (simplification wins, not losses):

- The stale-checkpoint degrade machinery (`STALE_CHECKPOINT_DEGRADE`,
  blind-complete counters, "checkpoints are not replayed" paths): an
  explicit model has no replay, so a resumed run cannot deserialize stale
  leaves. Delete with the substrate.
- The worker/group anycast (`flywheel-{base}` groups, child-id collision
  storms): no workers, no collisions. Delete.

Kept verbatim (no server needed): invocation-id scheme
(`fresh_loop_id`-equivalent over local ids, persisted in the liveness row)
because resume-dedup (`should_resend`, `record_sent`) keys off it; the
`--resume`/`--force` CLI contract; `ensure_loop_owner` exclusivity.

**Not proposed:** rewriting the 2000-line loop body as a pure `RunModel`.
The port is substrate deletion, not a model rewrite — the body never
depended on replay, only on the four items above. (Open question 2 asks to
confirm inline-body + liveness row over a full rewrite.)

Witnesses: no `resonate` process (`resonate --version` absent, no
`:8001` listener); kill -9 mid-poll → next start sweeps the dead-owner row
and starts fresh with no duplicate sends (invocation dedup holds);
`stale_loop_reject_ids` selection logic re-pointed at rows, covered by its
existing unit tests.

## (a) Cron cadence via `ElmCron::tick`

Replace `start_loops`' toron `CronManager` (Resonate durable-sleep loop)
with a host loop over `ElmCron`:

- Registrations from the same merged view (`RegistryCronManager::
  registrations` unchanged — catalog surface untouched).
- Host calls `tick(now_ms)` at a named interval (**30s**, matching the
  loop's active cadence; open question 1). One schedule ⇒ at most one
  `CronFire` per call; the idempotent `fire_key(name, tick_ms)` makes a
  retry safe (`Already` re-attaches).
- Durable last-fired checkpoint: persist `last_fired_ms` per cron in local
  sqlite (new tiny table vs reuse — open question 1). Without it a restart
  resets to 0 and the `Latest` policy fires one catch-up tick per schedule
  (safe but noisy); with it, exactly-once across restarts.
- Tick bodies run through the durable host (`WorkflowHost::
  spawn_run_and_drain` — flywheel already depends on toron-mcp), not
  `WorkflowEngine::run`. Linear bodies now; graph bodies once
  `bd-durable-graph-impl-r2x1` lands (router refuses them to the old path
  until then — no action needed here).
- `on_failure: bead` policy: keep `drain_policy_sink`'s input shape —
  the tick driver maps a `Fail`/failed-terminal outcome to the same
  `br create` leaf the sink drain feeds today.
- `cron_runner` per-mode arms (headless/pane dispatch, webhook probes)
  survive unchanged: they are `EffectRunner` closures, and the durable
  path consumes them as `LeafSeams.async_runner` (the exact seam
  `bd-host-cut-pqkg` built for this).
- Doorbell supervisor: the same `ElmCron` host gains one `* * * * *`
  schedule whose body calls `heal_tick` as a plain async fn. The
  worker-for-life leaked runtime and the server-side schedule go away.

Witnesses: two `tick()` calls at the same instant ⇒ one fire; kill -9
between ticks ⇒ restart fires exactly the missed `Latest` tick once
(checkpoint + idempotent key); paused crons excluded; `list`/`status`/
`history` outputs byte-identical (same merged view).

## (b) Workflow runs through the durable host

Flywheel invokes workflows in three places: cron bodies (§(a)), `pane.rs`
spawn flows (§cannot-port), and tests. The cut for the first and third:

- Cron bodies and CLI `workflow run/invoke` route to `WorkflowHost`
  (already a dependency via `effect_journal.rs`) instead of constructing
  `WorkflowEngine`. The host's router admits linear workflows today and
  graph workflows once step 2 lands; anything else stays on the old engine
  by refusal — flywheel changes no call shape, only which constructor it
  uses.
- `main.rs` tests that drive `engine.resonate().run(...).spawn()` +
  `approval_sink` + `resolve_approval` port to `run_durable_loop` +
  `resolve_parked` (the toron-mcp surface `bd-host-cut-pqkg` proved with
  `durable_sweep_resumes_a_parked_run_after_restart`).
- The per-cron engine-per-mode construction (`start_loops` building one
  engine per cron because `EffectRunner` receives only the action) maps 1:1
  onto per-cron `LeafSeams` — same closure, new struct.

Witnesses: `serve_wire_gift_wrap`-class test green with no server (already
demonstrated on the toron side); `rg 'resonate\(' src/` returns only
historical comments; `cargo metadata` shows no `resonate-sdk`.

## (d) Health, probes, config

| Today | Replacement |
|---|---|
| `resonate_health` (`GET {url}/health`, 2xx) | Parked-run capability probe: open + park + `drain_due` a scratch `DurableRun` against the engine store (mirrors toron's inventory-step-6 doctor decision — the only detector of a run that suspends forever) |
| `ensure_resonate` (autostart `resonate serve`, loopback guard, 10s `/health` wait) | Deleted. Nothing to autostart; the store is a sqlite file whose health is a file+`PRAGMA integrity_check` read |
| Doctor `resonate server` check + `check_resonate` preflight gate | Same capability probe, same FAIL-closed posture (steer/loop/workflow/cron keep their gates, renamed) |
| `cmd/engine.rs` store CLI (`resonate.db` path, probe) | Retarget at the engine-store sqlite (path, size, checkpoint state); drop the `/health` shape |
| `config.rs` `resonate_url` + `RESONATE_URL` + `cmd/config.rs` display | Deprecate-warn this release, remove next (hard removal breaks user configs for zero benefit — open question 4) |
| Pane store-flavor probe test + engine-constructor tests | Rewritten onto the capability probe + `durable_route` unit tests |
| `daemon.rs` `which_resonate`, `resonate_healthy`, loopback classifier | Deleted with the binary; the loopback-classifier tests go with them |

Witnesses: `flywheel doctor` green with no Resonate binary on `PATH`;
preflight refuses honestly when the store file is missing/unwritable;
`rg -i resonate Cargo.toml src/` shows config-deprecation + historical
comments only.

## Cannot port in this removal

1. **Spawn-await** (`pane.rs` spawn drain: promise reads,
   `resolve_spawn`/`reject_spawn`, `SpawnSink`). toron's model refuses
   `spawn` — there is no durable spawn park to resolve through. Keeping a
   Resonate server *only* for spawn flows would preserve the thing being
   removed to serve one action; deferring the spawn-await port to a
   follow-up bead keeps the removal complete for everything else.
   Recommendation: defer, and until then spawn workflows stay on the old
   engine behind the same refusal (no partial execution).
2. **In-flight server promises at flip time** (pending loop invocations,
   parked approval/spawn promises on the Resonate server). Same bridge as
   toron ADR-0043: drain before the flip or resolve once through the old
   server; a run that suspends forever with no error is the failure to
   avoid. Operational checklist, not code.
3. **`WorkflowEngine::remote_*` deletion** stays blocked until this port
   lands — this design is the flywheel side of that coordination; toron
   cannot delete first.

## Cut order (proposed)

1. Health/probes/config (§(d)) — observable, no behavior change.
2. Cron cadence (§(a)) + doorbell supervisor onto `ElmCron`.
3. Workflow bodies onto `WorkflowHost` (§(b)); tests ported.
4. Swarm-loop substrate (§(c)): liveness row + stale sweep, macro
   deletion, replay-machinery deletion.
5. Spawn-await follow-up bead (separate); `resonate-sdk` leaves
   `Cargo.toml`; `resonate_url` removal next release.

## Open questions

1. Cron host interval (30s proposed?) and checkpoint store (new table vs reuse)?
2. Swarm loop: inline-body + liveness row (recommended) vs full `RunModel` rewrite?
3. Spawn-await: defer to follow-up (recommended) vs Resonate-for-spawn-only interim?
4. `resonate_url`: deprecate-warn now, remove next release (recommended) vs hard remove?
5. `#[resonate_sdk::function]` mechanical deletion: this bead or cleanup bead?
6. Doorbell supervisor: fold into the `ElmCron` host (recommended) vs separate minute-tick?

## Captain review — 2026-11-02

Approved as the port map, with one reorder and one scope correction.

**Answers**

1. Cron interval **30s**. Checkpoint: reuse flywheel's existing atomic
   `.flywheel` state file, not a new sqlite table. A lost checkpoint write
   costs one safe `Latest` catch-up tick; a schema migration costs a toron
   release for no correctness gain. Record the file path in the port commit.
2. Swarm loop: **inline body + liveness row**, as recommended. The body is
   not a replay composition and must not be rewritten as a `RunModel` to
   remove a substrate it does not use.
3. Spawn-await: **required, not deferrable past the removal**. Keep it on the
   old engine until the port lands, but `resonate-sdk` cannot leave
   `Cargo.toml` (and toron cannot delete `remote_*`/`resolve_spawn`) while a
   shipped action still needs the server. Open a dedicated toron bead for a
   durable spawn park (model phase + kind-43001 correlation + host start
   seam) as part of this removal, not a follow-up release.
4. `resonate_url`: deprecate-warn now, remove next release. Correct.
5. `#[resonate_sdk::function]` deletion: **same port bead** as the loop
   substrate. A mechanical deletion that compiles is not a separate review
   unit.
6. Doorbell supervisor: fold into the `ElmCron` host. Correct.

**Corrected cut order**

The proposed step 1 cannot delete `ensure_resonate`/`resonate_healthy` while
the swarm loop still reads the server. Split it:

1. **Add** the parked-run capability probe, rename the doctor/preflight
   check, deprecate `resonate_url`. Keep `ensure_resonate` and the health
   read alive.
2. Cron cadence onto `ElmCron` (+ doorbell minute schedule).
3. Workflow bodies onto `WorkflowHost`, tests ported.
4. Swarm-loop substrate (liveness row, stale sweep, macro deletion, replay
   machinery).
5. **Delete** `ensure_resonate`, `resonate_healthy`, the server doctor arm,
   `resonate_url`, and `resonate-sdk` from `Cargo.toml` — only after 2-4
   land and spawn's durable park (question 3) exists.

Witness for the final deletion: no `resonate` process or `:8001` listener,
`cargo metadata` has none, and a kill -9 mid-poll resumes with no duplicate
send. `resonate --version` absent is not the witness; the host may keep a
stale binary on `PATH`.
