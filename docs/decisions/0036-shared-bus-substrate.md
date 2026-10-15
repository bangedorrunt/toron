<!-- governed-by: ADR-0003, ADR-0014, ADR-0019, ADR-0027, ADR-0032, ADR-0035 -->
---
status: proposed
date: 2026-09-22
decision-makers: bangedorrunt
consulted: bd-un5s no-go record (RoseCitadel, sqlite-replay load-bearing proof); wake-transport map + substrate survey 2026-09-22 (bd-42tn); effect-oriented skill (rust.md, patterns.md)
informed: flywheel x toron x beads maintainers
---

# ADR-0036: Daemon Push Sidecar (Wake Bus) for Cross-Process Observation

**Status:** Accepted — operator word 2026-09-23; serve wiring landed (bd-ewsi), see status log. Design bead bd-42tn and transports bead bd-un5s closed with the wake-unity waves.

**Decision in one sentence:** The toron daemon holds one `subscribe_trusted` over the wake kinds and fans wake frames out over a Unix-domain socket (`wake.sock`, JSONL, herdr-sock pattern), so every cross-process consumer (doorbell `FileHost`, `toron watch --live`, `wait --corr`) subscribes instead of polling SQLite or spawning a `toron watch` child — while SQLite replay stays the catch-up/repair path and kind-9 stays the wire vocabulary.

**Extends:**

- [ADR-0003](0003-nostr-mailbox-fabric.md): bus + projection + archive layering; the sidecar is a transport in front of the existing store, not a second store.
- [ADR-0014](0014-kind-registry-specification.md): no new kinds; kind-9 + `p`/`t`/`h` tags stay the wake protocol on the wire.
- [ADR-0027](0027-one-workspace-tick-control-plane.md): sensors stay Msg producers; the sidecar is a sensor transport, never a decision maker.
- [ADR-0032](0032-workflow-engine-honesty-jido-contracts.md): replay-then-live with EOSE keeps the honesty contract (no claimed delivery without observation).
- [ADR-0035](0035-elm-kernel-law-and-judgment-plane.md): Spec 5 transports half (bd-un5s); the WakeTable/GateDecision work stands unchanged — this feeds it faster, it does not redesign it.

**Amends:** nothing. ADR-0020 (two-plane CLI) keeps `watch` semantics; only the transport underneath changes.

**Does not amend:** the frozen kind registry; MCP parity names/shapes; `InProcessBus` semantics; the 100k store cap and durable-kind exemptions; Resonate as park-only durability.

**Does not wire:** federated/embedded-relay wake (Phase-1 policy gate stays); Resonate streaming (does not exist); presence unification (20001 stays file-projected); a new network surface by default.

**Tracker:** bd-42tn (design) → implementation beads on acceptance. bd-un5s closes when its VERIFY (no `WatchChild`, no snapshot pump) passes against this substrate.

---

## 1. Scope and fixed point

Goals:

1. **One push path across processes.** A daemon-held subscription fans out; consumers stop polling SQLite (1s pump) and stop spawning per-consumer `toron watch` children.
2. **Same contract as in-process.** `SubscriptionEvent::{Event, Eose, Closed}` shape, replay-since-cursor then live tail, per-subscriber dedup window — the sidecar speaks the `Bus` subscription language over a socket.
3. **Replay stays the repair path.** Slow/disconnected consumers resync via the existing store replay (cross-instance invalidation already proven); the socket never claims durability it does not have.
4. **Fail-closed liveness.** Watchbeat heartbeats distinguish quiet-bus from deaf-watcher; idle-exit semantics preserved; unresolved socket refuses hosting visibility loudly instead of degrading silently.

The fixed point is **one held subscription per daemon, N cheap subscribers, zero polls**. It is not a message broker, not federated delivery, not exactly-once across processes.

### 1.1 Non-goals

- No federated wake (relays stay Phase-1 policy-gated; lag-drop mode unconfronted here).
- No Resonate streaming (no such primitive; promises stay park-only, resolved from sidecar events).
- No presence unification (20001 ephemeral stays on the `presence.jsonl` projection with its TTLs).
- No new Nostr kinds, no MCP tools, no default TCP surface.

---

## 2. Context (evidence, with file pointers)

bd-un5s no-go proof (design record on the bead): in embedded mode publisher and observer are different OS processes with no shared tokio runtime, so no `mpsc` crosses the boundary — SQLite replay is the only shared substrate (`plane.rs:1428-1437`, was a 24GB/5h RSS bomb at 100ms per bd-741i, tamed to 1s + memos). The memos that keep the poll cheap live in `event_store.rs:149-170` (64-slot snapshot memo with cross-process invalidation via `bus_meta`, 10k hot parse memo).

What exists to build on (surveyed 2026-09-22):

| Piece | Verdict | Evidence |
|---|---|---|
| `InProcessBus` live fanout | DEAD-END across processes | per-process `mpsc` map (`in_process.rs:138-149,527-541`); docstring says not a relay (`:4-10`) |
| `BusEventStore` + `Filter` + `SubscriptionEvent` | BUILD-ON | multi-process-safe SQLite, cross-instance invalidation (`event_store.rs:666-687`), replay/EOSE shape transport-agnostic (`subscription.rs:4-9`, `bus/mod.rs:95-130`) |
| `RelayBus` (NIP-01 WS) | BUILD-ON with caveats | only proven cross-process push, but kind-9 policy-blocked (`relay_bus.rs:135-143,250-255`) + lag-drop (`:82-87,210-221`) |
| Resonate | DEAD-END as transport | promises + durable sleep only (`engine/mod.rs:11-21`, `cron.rs:4-11`); something must still resolve the promise |
| kind-9 + `p`/`t`/`h` + `WAIT_WAKE_KINDS` | BUILD-ON | already the wake protocol (`messaging.rs:598-637`, `plane.rs:1467`, doorbell `WATCH_KINDS`) |
| herdr.sock JSONL-line UDS RPC | BUILD-ON (pattern) | `herdr.rs:14,43-62,282-349,1321-1448`; no `doorbell.sock` exists — generalize the pattern, not a socket |

Copies of the problem to delete: `WatchChild` + `restart_watch` (`file_host.rs:469-529,1328-1411`, plus the `main.rs:9363` and `doorbell_host.rs:87-90` twins), the 1s snapshot pump (`plane.rs:1427-1453`), per-pin CLI inbox forks per sweep (`doorbell_host.rs:515-560`).

---

## 3. Decision drivers

- **Kill the poll, keep the contract.** 1s fleet-wide SQLite polls + per-workspace child processes are the steady-state tax; replay/EOSE/dedup semantics must survive the move.
- **One holder, N subscribers.** Exactly one `subscribe_trusted` per daemon (bounded fan-out, slow-consumer shed with resync), never one subscription per consumer.
- **Liveness must stay explicit.** Quiet bus vs deaf watcher is today's `/health` distinction — the sidecar carries heartbeats or it is a regression.
- **No second store, no second truth.** The socket is a transport in front of `BusEventStore`, journaled nowhere, claiming nothing durable.
- **Shippable cutover.** Pump stays until the sidecar proves green per consumer behind a flag; tripwire tests fail the build if both paths run.

---

## 4. Considered options

- **Option A — daemon UDS push sidecar (chosen).** `wake.sock` beside the daemon: one held subscription, JSONL `Event/Eose/Heartbeat` frames, per-subscriber kind filter + since-cursor, bounded queues with shed-and-resync, watchbeat liveness, filesystem-permission security.
- **Option B — TCP loopback push sidecar.** Rejected: new network surface (however local) for a same-host problem; UDS gives peer credentials + filesystem ACLs for free. TCP stays opt-in for exotic layouts, never default.
- **Option C — lift kind-9 over relays / embed a relay.** Rejected: fights the Phase-1 policy gate, inherits lag-drop, and needs an embedded relay that ADR-0003 deferred. Revisit when the gate lifts.
- **Option D — keep polling, optimize memos.** Rejected: memos already cut the cost; the floor is still N processes × 1s wakeups + per-consumer children. Diminishing returns, problem retained.

---

## 5. Decision outcome

Chosen option: **Option A**, because the socket already has a proven pattern (herdr.sock), the store already has the contract (replay/EOSE/invalidation), and every alternative either adds network surface, fights frozen policy, or keeps the tax.

### Consequences

- Good, because per-workspace `toron watch` children, the 1s pump, and per-sweep CLI forks all delete (CPU + RSS + fork storms go away).
- Good, because wake latency drops from poll-granularity (~1s + replay) to socket-speed, which the prompt-hop budget can actually use.
- Good, because the `Bus` trait shape is unchanged — in-process consumers never notice; only cross-process transports swap.
- Bad, because a new daemon-owned socket is a new failure domain (stale socket, slow-consumer shed storms) — mitigated by shed-and-resync + watchbeat + tripwires.
- Bad, because cutover spans toron + flywheel with a duality period — mitigated per-consumer flags, never flag-day.
- Neutral, because SQLite replay code stays (repair path), presence files stay, Resonate stays park-only.

---

## 6. Spec 1 — wire protocol (JSONL frames)

```
// subscribe request (client → daemon, one line)
{"subscribe": {"kinds": [9,14,20001,30013,43001,30031,39002], "since_cursor": "<opaque>" | null, "project": "<slug>" | null}}
// frames (daemon → client, one per line)
{"event": {...bus event JSON...}}
{"eose": {"cursor": "<opaque>"}}
{"heartbeat": {"ts_ms": <u64>}}
{"shed": {"resync_from": "<opaque cursor>"}}   // slow consumer evicted: replay, then re-subscribe
```

1. `since_cursor` null = replay from store head, then live (same as `subscribe` today); cursor = resume without replay.
2. Frames carry the existing bus event JSON unchanged (kind-9 + tags untouched).
3. Heartbeat every N seconds of quiet (watchbeat cadence); client-side quiet-vs-deaf uses the same rule as today.
4. Presence (20001) frames are sourced from the existing file projection, marked `{"projected": true}` — no fake liveness.

**Amendment 2026-10-11 (no silent loss; bd-wake-live-burst-loss-j86v, bd-wake-replay-cap-qk8n).** flywheel's live run lost about 46% of a same-second burst with no frame, and a resume more than 1000 rows behind replayed only the newest 1000. Two causes: the cursor was `(created_at seconds, event id)`, which is not arrival order, so live events that sorted below their predecessor were suppressed as overlap; and replay went through a newest-1000 store window. Fixed as follows:

- The store keeps an arrival log (`bus_arrivals`, `AUTOINCREMENT` seq written in the insert's transaction, so seq order is commit order and a number is never reused). The server mints `seq:<n>` cursors. The older `<created_at>:<event_id>` cursor is still accepted and placed by looking its event up. Cursors stay opaque to clients.
- A subscriber is served from the store: replay and live delivery both page the arrival log from the subscriber's own cursor until it is exhausted. The fan-out broadcast is only a doorbell, so a slow reader gets backpressure, not a shed.
- New frame `{"events_lost": {"from": "<cursor>", "through": "<cursor>|unknown"}}`. It is sent before replay when cap eviction removed arrivals after the subscriber's cursor (`through` = the highest evicted seq), or when a legacy cursor's event is no longer stored (`through` = `unknown`; replay then restarts from that cursor's second, preferring duplicates to a skip). A subscriber never misses a stored event without this frame.
- Backends without an arrival log (relay buses) keep the previous path.

---

## 7. Spec 2 — socket surface

1. Path: `$XDG_STATE_HOME/toron/wake.sock` (daemon data dir), mode 0600, single-writer lease reused from the `serve.pid` pattern (stale-socket takeover with identity check, never blind delete).
2. One `subscribe_trusted(WAKE_KINDS)` held by the daemon; fan-out loop per subscriber with bounded queue (default 1024, structure mirrors `subscription.rs:75-83` capacity).
3. Slow consumer: shed with `shed` frame + cursor; client replays via store, resubscribes. Shed counts exported (today's `note_full_drop` honesty, extended across the socket). *(Amended 2026-10-11: on the arrival-log path a slow consumer is backpressured and never shed; `shed` remains only on the legacy path. See the §6 amendment.)*
4. UDS only by default. TCP loopback (`127.0.0.1`, opt-in flag, allowlisted) only for layouts without shared filesystem — never on by default.
5. `serve` without the sidecar flag still works (embedded consumers keep today's paths); the flag is per-daemon, consumers degrade to poll with a loud reason when the socket is absent (fail-closed visibility, never silent deafness).

---

## 8. Spec 3 — consumer cutover (per consumer, flagged, tripwired)

| Consumer | Today | Target | Flag |
|---|---|---|---|
| doorbell `FileHost` watch | `toron watch --live` child + respawn loop | `wake.sock` subscription, same `BrainMsg::Watch/Died` mapping | removed (wave 4, bd-ud7k) |
| toron `watch --live` | in-process tail or pump | socket tail when daemon present, else today's paths | removed (wave 4, bd-ud7k) |
| `wait --corr` unpark | any-bus-event wake + store re-check | socket wake + same store re-check (store stays the truth) | removed (wave 4, bd-ud7k) |
| per-pin inbox sweep | CLI fork per pin per sweep | unchanged initially (correctness first; socket inbox later, explicitly out of scope) | — |

1. Migrate in the table's order (daemon consumer first — biggest win).
2. Each cutover deletes its old path in the same change (migrate-then-delete); a tripwire test fails the build if both the child-spawn and the socket consumer are live.
3. `wait --corr` keeps the receipts-store fast path and per-wake re-check — the socket changes only what wakes it.

---

## 9. Implementation plan (crew lanes)

**Affected paths.** toron: `crates/toron-core/src/bus/` (socket publisher beside `InProcessBus`, reusing `Filter`/`SubscriptionEvent`), `crates/toron-mcp/src/server.rs` (serve alongside `/mcp`), `crates/toron-cli/src/{plane.rs (socket tail + pump deletion), mailbox_source.rs (socket discovery)}`. flywheel: `crates/doorbell-rt/src/file_host.rs` (socket consumer, delete `WatchChild`), `src/doorbell_host.rs` (embedded twin), `src/main.rs` (watch child deletion).

**Patterns to follow.** herdr.sock JSONL-line RPC + tests (`herdr.rs:1321-1448`); `serve.pid` stale-takeover identity check; `SubscriptionEvent` shape; watchbeat heartbeat; `note_full_drop` exact shed counts.

**Patterns to avoid.** A second store; a new Nostr kind; TCP by default; claiming durability for socket delivery; deleting the pump before the socket proves green per consumer; unbounded subscriber queues.

**Migration steps.** Wave 1: sidecar + socket tail behind flags (duality, tripwires). Wave 2: cut `FileHost` over, delete `WatchChild`. Wave 3: cut `watch --live` + `wait --corr` wake, delete pump. Wave 4: metrics + docs + remove flags.

### Verification

- [ ] Socket tail delivers a kind-9 post to a subscriber with no SQLite poll in the path (strace-level: no `read` on the db file between publish and delivery)
- [ ] Slow consumer gets `shed` + resyncs via replay with zero loss and zero duplicates (dedup window test)
- [ ] Kill -9 the subscriber mid-stream: resubscribe with cursor loses nothing past the last delivered frame
- [ ] Stale socket takeover: new daemon adopts after identity-checked takeover, old one refuses
- [ ] Tripwire: child-spawn code + socket consumer cannot both be live (build fails)
- [ ] `/health` deaf-vs-quiet distinction preserved (quiet bus ≠ dead watcher, asserted)
- [ ] No new kinds, no new MCP tools, no TCP listener by default (negative checks)

---

## 10. Risks

- Shed storms under flood (kind-9 floods now evict socket consumers instead of slowing polls): mitigated by cursor resync + shed counters + the existing cap/DURABLE_KINDS debate unchanged.
- Stale-socket rollout on upgrade: mitigated by identity-checked takeover (same pattern as `serve.pid`).
- Duality-period confusion (two observation paths): mitigated per-consumer flags + tripwires, short-lived by wave plan.
- Daemon as single point of observation failure: unchanged from today (daemon already owns presence/registry); embedded mode keeps poll fallback with loud reason.

---

## 11. More information

- No-go record it answers: bd-un5s (design record on the bead).
- Revisit triggers: Phase-1 relay policy lift (revisit §4 option C); Resonate streaming primitive appearing (revisit park-only assumption); socket shed rate above tuned baseline.
- Status changes append here with date stamps.
- 2026-09-22 (bd-ud7k, wave 4): cutover flags removed on both sides
  (`toron.watch_sock`, `doorbell.watch_sock`); socket tail is the
  unconditional path with LOUD in-process fallback. Shed counters exported
  (`WakeHandle::aggregate_stats`: exact delivered/shed sums + live gauge;
  disconnects reap on the heartbeat write so error paths keep their counts).
  Deaf-vs-quiet asserted (`wake_bus_quiet_gets_heartbeat_deaf_gets_eof`:
  quiet bus heartbeats on the injected clock, dead server reads EOF).
- 2026-09-23 (bd-ewsi, OPERATOR WORD): serve wiring landed —
  `serve_stdio`/`serve_http` start `WakeServer` after the one-writer serve
  lock and hold the handle across the serve await (RAII drop removes the
  socket). Bind path and every discovery now share
  `toron_core::config::state_dir()/wake.sock` — one derivation by
  construction, closing the consumers-first gap (waves 2-4 shipped dialers
  while no process ever bound the socket: doorbell looped
  `wake_sock_absent`/`Died` ~16s apart, no wake delivered for hours).
  Spec 2.5 best-effort holds: `AlreadyServing`/bind failure warns loud and
  serve continues. Process-boundary pin:
  `cli_robot::serve_binds_wake_sock_for_consumers` (RED before wiring,
  GREEN after). Live verification: all six doorbell wake consumers
  reconnected, watch heartbeats fresh (`last_event_age_s: 12`),
  `degraded: []`.
