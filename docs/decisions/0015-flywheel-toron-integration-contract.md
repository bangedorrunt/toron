// governed-by: ADR-0003, ADR-0007

# ADR-0015: Flywheel ↔ Toron Integration Contract

**Status:** Accepted (2026-08-14)
**Related:** ADR-0003 (Decision 3A — dependency direction), ADR-0007 (MailSurface)

## Context

Flywheel and toron compose: flywheel is the orchestrator (spawn/send/watch/
loop/steer/coalition), toron is the transport/mailbox/workflow-substrate. The
boundary between them has been implicit — enforced by the one-way dependency
(`flywheel → toron-workflow → toron-core`) and code review, but never
formalized as a versioned contract.

The risk: either side can silently violate the contract (e.g., flywheel adding
a second mail backend, toron changing the `MailSurface` trait without updating
flywheel's `ToronMailBackend`). This ADR makes the contract explicit.

## Decision

### What flywheel guarantees to toron

1. **One-way dependency.** `flywheel → toron-workflow → toron-core`. Toron
   NEVER depends on flywheel. No `use flywheel::` anywhere in toron crates.
   Verified by `sc-fw` test asserting exactly one `fn agent_prompt`.

2. **One mail surface.** Flywheel's local `MailBackend` trait is DELETED (not
   dual-lived). Flywheel consumes the unified `MailSurface` trait in
   toron-core (`toron_core::mailbox::surface::MailSurface`). Selection:
   `FLYWHEEL_MAIL=toron` → `ToronMailBackend`.

3. **`MessageId` is String/hex end-to-end.** Flywheel never converts toron's
   hex event ids to `u64` or `i64` in shared request/response types. The
   `acknowledge_message` / `mark_message_read` / `force_release_file_reservation`
   args are `String` (toron hex ids), not integers.

4. **One shared herdr client.** The herdr client lives in `toron-core::backend`.
   Flywheel uses it via re-export (`toron_core::backend::herdr::Client`).
   Exactly ONE `fn agent_prompt` definition in the entire dependency tree.

5. **Profile/crew dispatch is flywheel-internal.** Steer, loop, spawn, and
   workflow dispatch go through herdr panes, NOT through toron events. Toron
   carries the metadata (mail threads, reservations, workflow projections) but
   not the dispatch. The captain's dispatch IS flywheel's job.

### What toron guarantees to flywheel

1. **`MailSurface` is the AM-intent superset.** send/reply/inbox/ack/mark_read/
   search, contacts, agents, reservations, build slots, products,
   summarize_thread(+product), pane identity helpers. All response DTOs derive
   serde. Inbox supports `urgent_only`/`since_ts`. Send receipts carry the rumor
   id.

2. **Atomic file reservations.** The conflict check is NEVER removed. Two tiers:
   held=HARD conflict (blocked — the corruption-prevention floor), intent=SOFT
   conflict (freeze+propose). Two distinct agents can never hold the same
   exclusive path.

3. **Crash-survivable workflows.** A workflow run that hits `request_approval`
   suspends as a Resonate durable promise. Process crash → run stays PENDING →
   resumes on restart (never "failed"). The Resonate server selection is:
   `RESONATE_URL` pin > remote Tailscale > local `com.flywheel.resonate` >
   embedded.

4. **Durable mailbox root.** The mailbox root is XDG (`$XDG_DATA_HOME/toron`),
   never per-pid temp. Both flywheel's `mail_shim.rs` and `coalition.rs` share
   this root.

5. **Addressable event dedup.** Kind-30031 events dedup per `(kind, author,
   d_tag)` on publish. Renewals collapse to the latest instead of storming the
   store. Replay keeps the NEWEST `limit` events (ASC tail).

6. **@mention dispatch is bounded.** `dispatch_job` is bounded by a configurable
   semaphore + one-in-flight-per-agent — no swarm thrash on a 20-mention burst.

### What each side MUST NOT assume

1. **Flywheel MUST NOT** create a second mail backend (the old `MailBackend`
   trait is deleted; `agentmail` is a real peer backend, not a shim).
2. **Flywheel MUST NOT** assume toron's projection applies read state
   (`unread_only` is a documented NO-OP).
3. **Flywheel MUST NOT** fake pane 43001/43004 lifecycle in headless mode
   (ADR-0010 Spec E — a spawn step cannot run headless).
4. **Toron MUST NOT** add a dependency on flywheel (breaks the one-way rule).
5. **Toron MUST NOT** change the 38 AM-Rust parity tool names/args/returns
   without an ADR (frozen contract — ADR-0007 C3).
6. **Neither side** uses `std::env::set_var` outside of integration tests that
   omit `forbid(unsafe_code)`.

### Versioning

The contract version is implicit in the crate versions. A breaking change to
any guarantee above requires:
1. An ADR in toron (the lower layer owns the contract spec).
2. A flywheel ADR or bead updating the consumer side.
3. Both crates' versions bumped (breaking = minor bump in 0.x).

## Consequences

- The boundary is now a validation target: tests can assert the one-way dep,
  the single mail surface, the single herdr client, and the MessageId type.
- Future changes to the boundary have a clear process (ADR on both sides).
- Federation has a contract to interop against when enabled.
- The contract is the substrate for the preflight checks (ADR-Q3):
  preflight verifies the guarantees hold before dispatching.

## Verification

```bash
# One-way dependency: no flywheel imports in toron crates
! grep -r 'use flywheel' crates/ 2>/dev/null

# Single mail surface: MailBackend trait deleted from flywheel
! grep 'trait MailBackend' ../flywheel/src/ 2>/dev/null

# Single agent_prompt definition
grep -rn 'fn agent_prompt' crates/ ../flywheel/src/ | wc -l  # == 1

# MessageId is String
grep 'type MessageId' crates/toron-core/src/mailbox/surface.rs
```
