<!-- governed-by: ADR-0003, ADR-0039 -->
---
status: accepted
date: 2026-11-21
decision-maker: SlateHarbor
consulted: ADR-0039 (MCP Events protocol methods), bd-2ghz8
---

# ADR-0046: Count-only signal links for hooks

**Decision in one sentence:** a hook that only needs to know how busy a seat is
holds a 24-hour, hash-at-rest signal secret — not a read token — and can learn
only `{pending, urgent, waking, cursor, expiresAt}` for one participant through
`Authorization: Bearer`; it never sees ids, bodies, hints, or a secret in a URL.

## Context

ADR-0039 principals carry read tokens: anyone holding one can `events/poll` and
see payloads. A wake hook (for example, a shell script in a terminal harness)
only needs a "should I wake?" signal. Handing it a read token violates
least-privilege: a leaked hook token becomes a full events reader.

## Decision

1. **Surface.** `signal/create` and `signal/revoke` are custom JSON-RPC methods
   routed beside `events/*` on `on_custom_request`, under the existing
   `io.modelcontextprotocol/events` capability — no new tools, kinds or
   extension entry. `GET /signal` is a new HTTP route on the MCP server.
2. **Principal.** Create/revoke authenticate the caller with its events token
   (`crate::events::authorize`) and take the participant from that principal,
   never from arguments. GET resolves the signal secret itself.
3. **Secret.** One 32-byte hex secret, shown once at mint, stored only as
   `SHA-256(secret)` in a 0600 `signal_links.json` beside `events_tokens.json`.
   It travels only in `Authorization: Bearer`, never in a path or query, never
   in a journal or log line.
4. **Lifecycle.** 24h expiry (checked on every resolve, unlike read tokens);
   re-create rotates under one file lock (old secret dies as the new one is
   born); revoke ends every live link for the participant.
5. **Counts.** `count_for` derives pending/urgent/waking/cursor from live
   subscriptions and undelivered deliveries without returning any payload, id
   or hint. `urgent` counts undeliveries on urgent catalogue names; `waking`
   counts live subscriptions.
6. **Refusal.** Missing, unknown, expired or revoked secret → HTTP 401.

## Consequences

- A leaked hook secret grants counts for one participant for at most 24h; it
  grants no reads and cannot be replayed after rotate/revoke.
- The third surface grows by two methods and one GET route; the frozen 41-tool
  and 25-resource surfaces and the kind registry are untouched.
- `counts_for` is a default trait method so every `SubscriptionStore` gets the
  count-only view; no payload leaks through the response type.

## Alternatives rejected

- **Give hooks a scoped events token:** a leak exposes payloads; violates the
  bead's least-privilege principle.
- **Put the secret in the URL:** leaks into logs, proxies and shell history.
- **Reuse `principal::resolve`:** it deliberately ignores expiry; links must
  expire, so a parallel resolver is required.
