<!-- governed-by: ADR-0003, ADR-0027, ADR-0033, ADR-0035, ADR-0036, ADR-0037, ADR-0038 -->
---
status: accepted  # 2026-11-20: accepted by the decision-maker, amended with D7 (events/stream) and D8 (harness bridges)
date: 2026-10-08
decision-makers: bangedorrunt
consulted: MCP Events extension SEP-3415 (final, protocol 2026-07-28) and richardanaya/pi-mcp-events (MIT); opencode 2.3.2 native events client (binary inspection 2026-11-20); Claude Code 2.1.328 channels (`claude/channel`); rmcp 3.5.1 release notes (#1322, #1308, #1281, #1295, #1274); rmcp 3.5.1 release notes (#1322, #1308, #1281, #1295, #1274); flywheel ADR-0012 D6; executor.sh v2 MCP endpoint docs
informed: flywheel x toron x beads x chiebukuro maintainers
---

# ADR-0039: MCP Events as Protocol Methods over Existing Bus Facts

**Status:** Accepted (2026-11-20), amended with D7 and D8. Plan: `docs/plans/2026-10-08-1854-feat-toron-mcp-events-plan.md`.

**Decision in one sentence:** toron serves the MCP Events extension (`events/list`, `events/poll`, `events/stream`, `events/subscribe`, `events/unsubscribe`) as protocol methods behind an advertised `extensions` capability, not as tools. Its catalogue is derived only from facts already on the bus, subscriptions are durable and bearer-authorised per D6, and push delivery goes only to operator-allowlisted HTTPS callbacks, Standard Webhooks-signed, through one SSRF-safe egress guard.

**Extends:**

- [ADR-0033](0033-additive-tools-kinds-discipline.md): the frozen 41-tool surface and the kind registry are untouched. Protocol methods are a third surface, and this ADR is their discipline (catalog entry, tests, versioning under the capability key).
- [ADR-0035](0035-elm-kernel-law-and-judgment-plane.md): delivery is one Model/Msg/`tick` with effects journaled before execution. Retry timing enters as clock Msgs.
- [ADR-0036](0036-shared-bus-substrate.md): one `subscribe_trusted` dispatcher reuses the wake substrate's cursor/`eose`/`shed` semantics. There is no second bus and no SQLite polling.
- [ADR-0037](0037-one-key-per-pin-project-scoped-visibility.md) and [ADR-0038](0038-operator-key-lives-in-the-config-home.md): a principal is one pin or the operator. Token minting is operator-signed with the ADR-0038 key.
- flywheel ADR-0012 D6: the verb authz matrix is applied to events (table below). The doorbell becomes a subscriber and keeps its single 5/30/3 pane path.

## Context

- Consumers outside the daemon's process tree cannot get toron facts pushed with authentication, durability or resumption. `wake.sock` is host-local and unauthenticated by design (file permissions).
- Harnesses reach toron only through executor, which exposes `execute`/`skills`/`resume` and does not forward custom protocol methods. Events are therefore for direct clients (doorbell, chie, beads, operator tooling), and harness wake-ups stay on the doorbell.
- The only outbound-HTTP primitive (`call_webhook`) is `Unsupported` pending an SSRF contract (bd-i6kc). `http_guard::is_private_ip` exists but has no caller, and it refuses the tailnet ranges (100.64.0.0/10 and fc00::/7, which contains Tailscale's fd7a:115c:a1e0::/48) that our receivers live on.
- Under rmcp 3.4.0, a handler `-32602` became HTTP 400, which clients read as a transport failure. rmcp 3.5.1 restricts 400 to a missing `_meta`, which is the error contract events need.

## Decision

1. **Surface.** Advertise `capabilities.extensions["io.modelcontextprotocol/events"]` (key per the draft, versioned) and route `events/*` through rmcp `on_custom_request`. `tools/list` stays at 41, and no Nostr kinds are added. Handler errors return HTTP 200 JSON-RPC errors. A missing `_meta` returns 400 `-32602`.
2. **Catalogue.** The catalogue is limited to bus facts:
   - `mail.addressed`
   - `reservation.conflict`
   - `ack_sla_expired`
   - `role.changed`
   - `policy.changed`
   - the synthetic `gap`
   
   Payloads default to hint plus counts. Full payload is an operator opt-in. The `beads.*` and `chie.*` namespaces are reserved for later publishers.
3. **Subscriptions.** Subscriptions are keyed by (principal, delivery, name, canonical-args hash) and stored durably in the data root. An identical re-subscribe is a refresh. TTL is at most 24h, and no-expiry is operator-only.
4. **Egress.** Use one guard:
   - https only, with no userinfo;
   - host allowlist (default `*.example-tailnet.ts.net`) and CIDR allowlist (default `100.64.0.0/10`, `fd7a:115c:a1e0::/48`), with everything else private refused by `is_private_ip`;
   - resolve once and connect to the checked address;
   - no redirects;
   - bounded body and time.

   The allowlist is a carve-out. The guard is never disabled.
5. **Trust.** A verification challenge is required before activation. Standard Webhooks HMAC signing uses a per-subscription `whsec_` secret. Retries are capped exponential with jitter. 410 terminates the subscription, 413 is not retried, and exhaustion or overflow emits one `gap` carrying a resume cursor.
6. **Principals.** Operator-minted, hashed-at-rest per-pin bearer tokens. In this ADR's scope they are required on `events/*` only, so executor's token-less tool calls keep working.
7. **Push stream (amendment, 2026-11-20).** `events/stream` is served over Streamable HTTP SSE, one long-lived request per subscription, sharing the poll cursor and the D5 `gap` semantics. It sends `notifications/events/active`, `event`, `heartbeat` and `terminated`, routed by `_meta["io.modelcontextprotocol/subscriptionId"]`. The reason is that opencode 2.3.2 ships a native SEP-3415 client that streams, and a Claude channel bridge must push. Poll stays the floor every client can fall back to.
8. **Harness bridges until native support (amendment, 2026-11-20).** A harness with no native events client gets a thin bridge. The bridge is an SEP-3415 client (poll or stream, cursor persisted, deduped by `eventId`) talking directly to toron with its pin's token, for `events/*` only. Tools keep going through executor. The bridge turns each event into the harness's own inbound door:
   - Claude Code: a channel MCP server (`notifications/claude/channel`).
   - pi/omp: the pi-mcp-events extension (`pi.sendMessage`, `deliverAs` follow-up or steer).
   - opencode: its native client, so no bridge.
   - Harnesses with no inbound door (gemini, antigravity, grok, codex): the doorbell as subscriber (U8) on the existing typed-wake path.

   The payload is the D2 hint ("mail pending, run `toron next`"), never the body. A bridge is retired when its harness ships native support. The doorbell dedupes by bus event id, so a seat reached by both a bridge and the doorbell rings once.

**D6 mapping:**

| Events verb | Pin token | Operator token |
|---|---|---|
| `events/list` (filtered to what the caller may subscribe to) | ✔ | ✔ |
| subscribe/poll, own pin and project, TTL ≤24h | ✔ | ✔ |
| subscribe webhook to an operator-allowlisted callback, own scope, hint payload | ✔ (the allowlist is the D6 operator approval) | ✔ |
| cross-pin or cross-project scope, full payload, no-expiry | ✖ | ✔ |
| token mint/revoke, allowlist edit | ✖ | ✔ |

## Consequences

- **Positive:** Off-host consumers get authenticated, resumable push without new tools or kinds. bd-i6kc gains a reusable guard. Loss becomes explicit (`gap`). The doorbell keeps a single budgeted path.
- **Negative:** It adds a third public surface (protocol methods) that the catalog and skill sync must track. The webhook egress is new attack surface, bounded by the allowlist and signing. The draft spec may still rename things, and versioning under the capability key absorbs that.
- **Neutral:** Executor users see no change. Harness-side MCP Events clients cannot reach toron through executor. If wanted, that is executor's feature to add.

## Alternatives rejected

- Subscribe/unsubscribe as tools, because it breaks the 41 freeze and makes client plumbing model-invocable.
- Exposing `wake.sock` over the network, because it has no auth or signing and is not MCP.
- Disabling `is_private_ip` for tailnet callbacks, because the operator explicitly chose an allowlist over disabling the guard.

## Status log

- 2026-10-08: proposed alongside the plan. Not committed. Pending operator review.
- 2026-11-20: accepted by the decision-maker with D7 (`events/stream`) and D8 (harness bridges). The executor gap in Context still holds for tools. D8 has bridges reach toron directly for `events/*` only.
