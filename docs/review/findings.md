# ADR-0008 Review Findings Table

Ranked findings across P0–P6 tracks. Fed into the batched fix pass (captain decision 4:
collect all findings, then batch-fix RED→GREEN). **No code edits until the full table is
reviewed and the captain approves.**

| ID | Track | Site (file:line) | Invariant | Verified | Risk | Fix-class |
|----|-------|------------------|-----------|----------|------|-----------|
| F2.NIP40 | P0 crypto | `crates/toron-core/src/mailbox/unwrap.rs:602-656` (`UnwrapPipeline::project`) | NIP-40 `expiration` honored on unwrap ingest — expired gift wrap rejected BEFORE sqlite/git projection | **RED** — no TTL check exists | **P0 crypto** — expired-but-never-seen gift wrap is ACCEPTED + projected, voiding ADR-0003 "Parity Superiority" replay/expiry claim | TTL check in unwrap pipeline (parse `expiration` tag, reject-if-past pre-write). RED test `expired_gift_wrap_rejected_on_unwrap` first (see `bd-p3-nip40-expiration-fix-6ezx`) |

## F2.NIP40 — NIP-40 expiration NOT honored on unwrap ingest path (P0)

**Probe:** `bd-p0-nip40-ingest-probe-oyaa` (2026-08-09). Verdict: **RED**.

### Evidence (every ingest layer traced, no TTL check found)

1. **Unwrap pipeline** — `UnwrapPipeline::project`, crates/toron-core/src/mailbox/unwrap.rs:602-656.
   Flow: seen_wraps guard → `try_unwrap` → dedup check → G14 policy gate → ack cache →
   `sink.write`. No NIP-40 parse, no reject-if-past, no `expiration` mention anywhere in
   the module.
2. **Unwrap crypto** — `gift_wrap::unwrap`, crates/toron-core/src/nostr/gift_wrap.rs:88-118.
   Kind check → decrypt seal → verify seal → decrypt rumor → NIP-59 sender-mismatch.
   No TTL check.
3. **Bus live publish** — `InProcessBus::publish`, crates/toron-core/src/bus/in_process.rs:203-255.
   Pipeline: Verify → Auth → Dedup → Fanout. No expiration stage.
4. **Bus live fanout** — `InProcessBus::fanout`, crates/toron-core/src/bus/in_process.rs:342-374.
   Access-gated fanout only; no TTL filter.
5. **Only enforcement that exists: bus REPLAY + GC** —
   `BusEventStore::replay`, crates/toron-core/src/bus/event_store.rs:102-108
   (`WHERE (expires_at IS NULL OR expires_at > ?1)`), prune at :139. Covers the
   subscribe-time snapshot only; a LIVE-published expired wrap bypasses it and is
   delivered to the unwrap loop.

### Consequence

An expired gift wrap published to the bus while the unwrap loop is subscribed is
delivered → unwrapped → projected to SQLite + Git. ADR-0003 rev6's "NIP-40 expiration
TTL honored" (replay/expiry Parity Superiority row) holds for replay but not for the
live ingest path.

### Fix (deferred to `bd-p3-nip40-expiration-fix-6ezx`)

1. RED: `expired_gift_wrap_rejected_on_unwrap` — gift wrap with `expiration` tag in the
   past → pipeline rejects (no sqlite/git write). Confirmed to fail today.
2. GREEN: TTL check in unwrap pipeline before `sink.write` (parse `expiration` tag,
   compare vs now, reject-if-past).
3. REFACTOR: extract named helper.
