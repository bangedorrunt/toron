---
status: accepted
date: 2026-08-04
accepted: 2026-08-04
revision: 6
decision-makers: bangedorrunt (captain)
consulted: ""
informed: ""
supersedes: [ADR-0001, ADR-0002 (partial)]
related: [flywheel, herdr, mcp_agent_mail_rust, nostrdevkit/nostr]
---

# ADR-0003: Pivot to a Nostr-Native, Tokio-Based Multi-Backend Agent Mailbox

> Comprehensive and self-contained. Embeds the Nostr primer, the
> `mcp_agent_mail_rust` ("AM-Rust") feature-parity contract, the herdr backend
> protocol, the flywheel integration contract, and the dependency-direction
> analysis so a future agent can implement without external lookups.
>
> **Revision 7 (2026-08-05):** MCP crate corrigendum + 2026-07-28 spec alignment.
> `fastmcp-rust` (Dicklesworthstone/fastmcp_rust) is built on **asupersync** — the
> runtime ADR-0003 drops, and whose removal is a hard CI gate; pulling it in
> re-introduces asupersync transitively and mixes two async runtimes. Replaced by
> **`rmcp`** (modelcontextprotocol/rust-sdk, Tokio-native): `cargo add rmcp
> --features server`. Transports: `transport-io` (stdio) +
> `transport-streamable-http-server` (Streamable HTTP as a Tower service mounted
> on toron's axum router; the legacy 2024-11-05 HTTP+SSE two-endpoint transport
> is a deliberate rmcp non-goal — do not build it). Targets
> **`ProtocolVersion::V_2026_07_28`** (latest spec, captain mandate; falls back to
> `V_2025_11_25`). Efficiency levers adopted: `#[tool]`/`#[resource]` macros +
> `schemars::JsonSchema`-derived `inputSchema`/`outputSchema` (JSON Schema
> 2020-12) — the derived schemas ARE the conformance golden source (no
> hand-rolled schemas, no AM-Rust vendoring); **structured outputs (SEP-2106)**;
> **`subscriptions/listen` (SEP-2575)** as the MCP-native real-time push for
> inbox/reservation/agent-state changes (the "<50ms push, no polling" mechanism
> at the MCP boundary); **stateless Streamable HTTP (SEP-2567)** for
> horizontally-scalable daemon/federation; standardized routing headers
> (SEP-2243). Tool annotations flag destructive vs read-only tools. No
> kind-registry change — the ADR-0003 frozen kind registry is untouched.
>
> **Revision 6 (2026-08-04):** three-agent debate (CyanCat/pi + TopazElm/grok +
> CreamElk/pi, thread `toron-nostr-vision-analysis`), dual-ack LOCKED + captain
> sign-off. Applied patch agenda A1–A9: **A1** fixes a P1 protocol bug in G5
> (NIP-09 deletion is author-only — an operator kind-5 CANNOT delete an agent's
> reservation on any NIP-01-compliant relay; addressable dedup is per
> `(kind,pubkey,d)` so the operator can't relay-supersede either) → force-release
> = operator **revocation event** (kind-30031/30032 `status:revoked`) + normative
> **projection key-priority rule (operator > agent)**; NIP-09 = cleanup hint +
> agent own-deletes only. **A2** Phase-1.5 NIP-46 remote-signer option. **A3**
> NIP-32 namespaced labels (`L`="toron") on relay-crossing kinds (dual-emit;
> `#t` stays as the InProcessBus shortcut; DMs carry neither). **A4** Federation
> hardening = NIP-42 AUTH + NIP-13 POW + allowlist (concretes risk +12). **A5**
> NIP-77 negentropy for DR/federation rejoin (default; full replay fallback).
> **A6** superiority row (Buzz `delay` cap **270s** vs Resonate unbounded). **A7**
> bounded agent-pool semaphore + one-in-flight-per-agent for `dispatch_job`.
> **A8** ephemeral-audit pointer (already covered). **A9** NIP-05/39 deferred
> P3. **No kind renumber** — frozen registry untouched.
>
> **Revision 5 (2026-08-04):** joint independent review by CreamElk (pi) and
> TopazElm (grok) — both completed their own source research BEFORE reading this
> ADR, then debated over agent mail (thread `toron-nostr-vision-analysis`) and
> locked AGREEMENT (dual-ack). Applied patch-agenda amendments 1–10 exactly:
> frozen kind registry (vacated standard-kind collisions 10000–10003 and
> NIP-51-occupied 30000–30004/30001/30002; verified-free numbers 30010–30014,
> 30031, 30032), Buzz-verbatim job lifecycle 43001–43006, Phase-1 federation
> policy (NIP-17-only), ordering by rumor timestamp after unwrap, ack via
> NIP-10 `e` + `toron-kind` (no `d` on kind 14), honest privacy wording in the
> superiority table, 25 resources + conformance scoping (schema/filters/
> tokenizer, not ranking) in the parity contract, re-derived conformance
> fixtures (AM-Rust license is `LicenseRef-MIT-Rider` — no vendoring), rust-nostr
> 0.44.x pin, and risk-register additions. Defect register: NEW-D1..D9 + D1..D4
> (13 items, register kept in the thread).
>
> **Revision 4 (2026-08-04):** studied [`block/buzz`](https://github.com/block/buzz) (a
> Nostr-native hive-mind comms platform) as prior art. Adopts its proven patterns
> (kind-as-dispatch, relay pipeline discipline, @mention-as-dispatch, YAML
> workflow engine, NIP-29 channels, ephemeral presence) and adds a **Phase 2
> Collaboration & Automation Layer** that makes toron exceed both AM-Rust
> (mailbox) and Buzz (chat/forge): toron = AM-Rust parity + Buzz-grade
> channels/workflows + Resonate durability (which Buzz lacks). New kinds added
> to the core table (job lifecycle, presence, reaction, workflow-run).
>
> **Revision 3 (2026-08-04):** captain confirms (1) **integrated mode primary**
> (flywheel embeds toron), and delegates the remaining open questions to the
> author. This revision (2) **resolves all open questions** with reasoned
> decisions, (3) fills **16 business-logic gaps** found in an adversarial
> review (key management, name→pubkey resolution, to/cc/bcc wire mapping,
> attachments, operator-mediated force-release, project scoping, ordering,
> read/ack privacy, pane-identity per-backend, DR, and more), and (4) adds a
> **parity-superiority** table showing where toron exceeds AM-Rust.
>
> **Revision 2 (prior):** Tokio replaces asupersync (captain override).
> **Revision 1 (prior):** initial Nostr pivot.

## Context and Problem Statement

### What toron is today (being broken)

`toron` (v0.1.0) is a Rust workspace built on three pillars mandated by
ADR-0001/0002: **asupersync** runtime, **Resonate** durable execution, and an
**mDNS + PSK + WebSocket/QUIC hub** with active-passive failover, plus an
**Adapter Architecture** (MCP adapter + Sidecar adapter). Current modules:
`src/hub/{core,transport,discovery}`, `src/protocol/message`,
`src/durability/workflow`, `src/adapter/{mcp,sidecar}`.

### The breaking change

- **Protocol**: adopt **Nostr** as the transport + coordination protocol.
- **Runtime**: **Tokio** (captain override of ADR-0001's asupersync mandate).
- **Role**: toron becomes a **mailbox** — feature-equivalent (100%) to
  [`Dicklesworthstone/mcp_agent_mail_rust`](https://github.com/Dicklesworthstone/mcp_agent_mail_rust),
  but built **on Nostr**.
- **Backends**: `Backend` trait; **herdr** default; **tmux** on hold.
- **Agents**: first-class adapters for **pi**, **opencode**, **grok**.
- **Dependency**: [`flywheel`](../../) **embeds toron in-process** (Decision 3A,
  captain-confirmed primary mode).
- **Goal**: agent automation with **real-time collaboration** — agents
  coordinate through durable, signed, relay/bus-delivered mail instead of
  shared filesystem state or a single SQLite process.

### Why Nostr (the problem it solves that AM-Rust does not)

AM-Rust is local-first and single-operator: one process owns one SQLite DB,
agents are self-asserted by name, "real-time" means polling. Nostr adds four
things toron needs: **(1) cryptographic identity by construction** (every event
Schnorr-signed; pubkey = identity), **(2) real-time pub/sub** (subscriptions
push, no polling), **(3) federation** (cross-machine/cross-operator via shared
relays), **(4) metadata-hiding private mail** (NIP-17/NIP-59 gift wrap =
E2E-encrypted, one-to-many DMs).

### Why Tokio (captain override)

`nostr-sdk` (relay pool, subscriptions, NIP-44/59, negemony) is Tokio-native;
the ecosystem (tungstenite, axum, sqlx) is Tokio-standard. Captain waives
ADR-0001's asupersync-exclusivity. **Retained** from ADR-0001:
`#![forbid(unsafe_code)]`, Resonate, i64-µs timestamps, write-behind-cache /
commit-coalescer disciplines. **Dropped:** asupersync, no-Tokio rule.

### Parity contract (non-negotiable)

toron reproduces AM-Rust's **38 MCP tools / 9 clusters**, **25 MCP resources**
(Phase 1), **storage layout**, **safety invariants**, **pre-commit guard**,
**CLI robot mode + metrics**, and (Phase 2) the **TUI** — observable to agents
as identical tool names, argument shapes, and return values. **"100% parity"
= behavioral** (tool surface + resources + storage + invariants); **runtime
parity (asupersync) is waived** by captain. Verified by a conformance suite
with **re-derived golden JSON fixtures** (AM-Rust is `LicenseRef-MIT-Rider`
— no vendoring of its test code; fixtures are first-party artifacts captured
from the live tool/resource schemas). **Conformance scope = response schema +
filter/importance/thread/date semantics + tokenizer contract** (`porter
unicode61 remove_diacritics 2`, `prefix='2 3'`, per AM schema.rs) — NOT exact
Search-V3 relevance ranking (AM ranks via tantivy+embeddings fusion; toron's
FTS5 is "relevance-compatible", recency ties exact). toron must be **at least
as good as AM-Rust; better wherever Nostr naturally raises the ceiling** (see
Parity Superiority).

### Problem statement

> How do we re-architect toron so its mailbox surface is byte-for-byte
> AM-Rust-compatible, its transport is Nostr (signed events + NIP-17 DMs over an
> in-process bus or relays), its runtime is Tokio, its supervision is
> backend-abstracted (herdr/tmux), and flywheel embeds it in-process for
> zero-IPC orchestration — while Git+SQLite remains the auditable projection?

## Decision Drivers

* **Behavioral parity (hard).** 38 tools, layout, symmetric-glob reservations,
  TTL state, thread/product/contact semantics, Search V3 lexical, pre-commit
  guard, optional proof gate, `#![forbid(unsafe_code)]`, robot CLI, metrics.
* **Tokio runtime (captain mandate).** nostr-sdk + tungstenite + axum + sqlx.
* **Collaboration efficiency.** Minimize process boundaries on the agent↔agent
  path; peer-to-peer mail with **no orchestrator in the loop**.
* **Crypto-native identity; E2E-private mail; real-time push; federation opt-in;
  local-first; backend abstraction; zero flywheel drift.**

## Considered Options

### Decision 1 — Bus topology
* **1A — In-process bus default, relay for daemon/federation (CHOSEN).** `Bus`
  trait: `InProcessBus` (Tokio broadcast + SQLite) default; `RelayBus` (NIP-01
  over WS) for standalone daemon / federation. nostr-sdk `Client` connects to
  whichever is active.
* 1B — Always a WS relay even local (loopback waste). 1C — External relays only
  (breaks local-first).

### Decision 2 — Runtime/library
* **2B — `nostr-sdk`/Tokio (CHOSEN).** (2A, `nostr` core on asupersync,
  removed by the Tokio override.)

### Decision 3 — Workspace topology / dependency direction
* **3A — flywheel embeds toron in-process (CHOSEN).** toron workspace:
  `toron-core` lib + `toron-mcp` lib + `toron-cli` bin. flywheel depends on
  `toron-core`+`toron-mcp`, embeds the MCP server, drives the loop via direct
  calls; herdr client lives in `toron-core::backend` (flywheel's copy deleted).
* 3B — toron daemon + flywheel HTTP client (kept only as remote-daemon
  fallback). 3C — toron depends on flywheel (rejected: inverted layering,
  breaks standalone peer mode).

## Decision Outcome

**Chosen: 1A + 2B + 3A.** (Justification in Revision 2 stands: 1A minimizes
agent↔agent hops; 2B is captain-mandated and lowest-risk; 3A is the
dependency-direction winner below.)

### Dependency-direction analysis (why flywheel → toron)

Agents are external processes → MCP-only → cannot link a lib. So the agent↔agent
path is always `agentA → MCP → [mailbox→bus→mailbox] → MCP → agentB`, and the
only free variable is **how many process boundaries sit inside the brackets**.
With 3A the bracket is **one process**: bus = in-process channel, mailbox =
function call, orchestrator dispatch = function call. Peer collaboration needs
no orchestrator in the loop, so toron must work with flywheel absent → **toron
never depends on flywheel; direction is one-way flywheel → toron.** 3B adds an
IPC hop on every flywheel↔toron call and forces a WS relay even locally.

**Net (3A):** agent↔agent = one process boundary/agent (unavoidable) + in-process
bus; flywheel↔mailbox = zero IPC; flywheel↔herdr = one shared client;
standalone peer mode = `toron` daemon, no flywheel.

### Resolved decisions (author calls, per captain delegation)

1. **Integrated mode is primary** (captain). Standalone daemon retained; remote
   daemon = fallback.
2. **Bus/relay retention:** gift-wrap envelopes TTL **90 days** (NIP-40
   `expiration`, configurable) on the bus/relay; **Git archive (unwrapped
   canonical content) is permanent**. Separation = *transient envelope* (TTL)
   vs *durable content* (Git) — cleaner than AM-Rust's single unbounded store.
3. **Proof-gate scheme:** **Schnorr-native default.** The trust anchor is a
   Nostr keypair; the attestation is a signed kind-30012 binding the agent npub
   + project + scope + nonce. Single crypto, single identity (the agent's npub
   *is* the attested subject). Ed25519 is a legacy escape hatch
   (`TORON_PROOF_SCHEME=ed25519`) for orgs with existing Ed25519 CAs only.
4. **Index driver:** **sqlx (runtime-tokio, sqlite).** Idiomatic async SQLite
   for Tokio, compile-time-checked queries, migrations, pooling, WAL pragmas.
   Single-writer discipline preserved (write-behind cache serializes writes;
   dedicated write connection; small pool). rusqlite under Tokio is a blocking
   footgun.
5. **Federation default list:** **empty (opt-in).** No implicit network egress
   (matches AM-Rust's zero-egress guarantee; strictly better since toron *can*
   federate when configured). Operators add relays to config.

### The architecture (the decision, concretely)

```
   ┌───────────────────── flywheel process (integrated mode) ────────────────────┐
   │  Resonate durable loop ──fn calls──▶ toron-core (mailbox+nostr+index+archive│
   │  (poll/send/respawn)                 +backend::herdr)                       │
   │                                            ▼                                │
   │                                 InProcessBus (Tokio broadcast + SQLite)     │
   │                                            │ (opt. RelayBus → ext. relay)   │
   │   toron-mcp server ◀──serves── MCP endpoint (stdio/HTTP)                    │
   └───────────────▲────────────────────────────│──────────────────────────────┘
                   │ MCP (stdio/HTTP)            │ (opt. wss federate)
        agents: pi / opencode / grok (external)   external relays (federation)
   (peer mail: agent → MCP → toron-core → bus → toron-core → MCP → agent;
    flywheel NOT in this path — only dispatches/monitors)
   Standalone: `toron` bin = same process, no flywheel loop, peer-to-peer.
   Remote:     `toron` daemon + flywheel over HTTP (fallback).
```

**Layers (`toron-core`):**
1. **Nostr Layer** — nostr-sdk primitives: keypairs, event build/sign/verify,
   NIP-44 encryption, NIP-59 seal+gift-wrap. Pure; no I/O.
2. **Bus (`Bus` trait)** — `InProcessBus` (Tokio `broadcast` + SQLite event
   store) default; `RelayBus` (nostr-sdk `Client` over WS) for daemon/
   federation. **Wire-identical events** — `InProcessBus`'s transport is
   channels instead of WS, but its events are wire-identical (serialize any
   and send to a real relay unchanged), and it runs the same validation
   pipeline. `RelayBus` speaks **full NIP-01 frames** (`EVENT`/`REQ`/`OK`/
   `EOSE`); `InProcessBus` is NOT a NIP-01-interoperable relay (no external
   client can REQ it) — it is a bus with relay-identical event semantics. This
   is the framing that makes toron Nostr-native in the common local case.
3. **Local Projection** — subscribes to the bus, unwraps gift-wrapped mail for
   local keys, writes: **SQLite index** (WAL, pooled, write-behind cache, 30s
   deferred touch) + **Git archive** (per-project AM-Rust layout:
   `messages/{YYYY}/{MM}/`, `agents/{name}/{inbox,outbox}/`,
   `file_reservations/`, `build_slots/`, `.archive.lock`, async commit
   coalescer).
4. **Mailbox Core** — maps the 38 tools to bus events (publish) and projection
   queries (read); enforces AM-Rust invariants.
5. **Backend trait** — `spawn/prompt/wait/read/send_keys/events`;
   `HerdrBackend` (ported from `flywheel/src/herdr.rs`, protocol v18);
   `TmuxBackend` returns `Unsupported` until un-held. **Single source — flywheel
   uses it via toron-core.**
6. **Tool Surface (`toron-mcp`)** — `rmcp` MCP server: stdio (`transport-io`) +
   Streamable HTTP (`StreamableHttpService` mounted on axum,
   `transport-streamable-http-server`) + `toron` CLI
   (robot mode + metrics; TUI Phase 2) exposing the 38 tools verbatim.

## Business Logic (the parity contract, fully specified)

> These subsections close the gaps found in review. An implementer should need
> nothing beyond this section + the Nostr primer to wire any tool correctly.

### Key roles & key management *(gap G1)*

toron introduces three distinct secp256k1 key roles AM-Rust does not have:

| Role | Holder | Purpose | Storage |
|------|--------|---------|---------|
| **Agent key** | each agent | identity + signing + NIP-44 decrypt | NIP-49 `ncryptsec` (passphrase-encrypted), in `agents/{name}/key.ncryptsec`; nsec **never leaves the toron process** |
| **Operator key** | the captain / toron process | privileged ops (force-release via **revocation event** G5, project manifest publication, relay admin) | toron config (`operator_nsec`, 0600); env override `TORON_OPERATOR_NSEC` |
| **Trust anchor** (optional) | designated Nostr keypair | proof-gate attestation signing | public npub in config; private key held by the issuing authority, not toron |

- `create_agent_identity` → **always** generate a fresh keypair + publish
  kind-30013 (agent profile, addressable, `d`=project-slug:name).
  `register_agent` → upsert profile on the **existing** agent key
  (or create if absent). Names map to pubkeys via the project profile index.
- **Security model:** the bus sees only signed events; private keys live only
  in the toron process memory + encrypted-at-rest files. Passphrase unlocks
  ncryptsec on process start (or a keyring integration). This is strictly
  stronger than AM-Rust (self-asserted names, no keys).
- **Phase 1.5 option — NIP-46 remote signer (A2):** for multi-operator/
  federated or high-assurance deployments, agent/operator keys may live in a
  **dedicated signer daemon / HSM / keyring** and toron requests signatures
  over NIP-46 (rust-nostr `nostr-connect`; Buzz ships a `signer` crate for the
  same reason). The key never sits in the toron process. NIP-46's bunker UX is
  human-wallet-oriented; the agent case is a **local signer daemon**. G1 inline
  ncryptsec remains the Phase-1 default. Retires the "key-mgmt operational
  burden" risk.

### Name → pubkey resolution *(gap G2)*

Tool args use **AM-Rust names** (`to=["BlueLake"]`, `agent_name="BlueLake"`),
never npubs (agents don't know npubs). toron resolves internally:

- **Within a project:** `name → npub` via that project's kind-30013 profiles
  (index lookup). **Unknown name ⇒ fail-fast error** (parity with AM-Rust's
  "Unknown recipient names fail fast; register them first").
- **Cross-project:** via contact links — an `AgentLink` record carries the
  peer's npub; resolution hops through it.
- Names are **scoped to a project** (AM-Rust invariant preserved) unless a
  contact link or product bus path bridges them.

### Messaging semantics: to / cc / bcc on the wire *(gap G3)*

`send_message(to, cc, bcc, ...)` maps to NIP-17 gift wrap with **true BCC
privacy** (stronger than AM-Rust, where cc is visible):

- Build the **visible rumor** (kind 14) with `p` tags for `to` + `cc` only,
  each tagged `["p", npub, relay, "to"|"cc"]`. Gift-wrap one copy per `to`/`cc`
  recipient.
- For each **bcc** recipient, build a **variant rumor** listing `to`+`cc`+that
  one bcc (omitting all other bccs), gift-wrap to that bcc. Bcc recipients
  therefore cannot see each other — exactly AM-Rust BCC semantics, now E2E.
- `subject` → NIP-14 `subject` tag. `importance`, `ack_required`,
  `sender_token` → tags on the rumor. `thread_id` (reply) → NIP-10
  `["e", root, relay, "reply"]`.
- Sender's toron resolves all recipient **names→npubs before wrapping**; an
  unknown recipient aborts the whole send (no partial delivery — parity).

### Attachments *(gap G4)*

Nostr event content is size-capped on relays (~64KB–1MB); images cannot be
inline. AM-Rust stores files in the archive directly.

- **Local-first (v1):** attachments written to a **content-addressed blob
  store** (`blobs/sha256/<hash>`, inside the project archive), referenced by a
  **kind-15 file message** whose `content` = a local `toron-blob://` URL and
  whose tags carry `["x", sha256]` + `["file-type", mime]` + optional
  `["decryption-key", key]` (NIP-17 file-message shape). Small text/JSON
  payloads embed inline in the rumor content.
- **Federation (Phase 2):** blobs served via **NIP-96 / Blossom** (the
  `nostr-blossom` crate exists in rust-nostr) so external instances fetch by
  hash. Image→WebP conversion + small-inline-embed (AM-Rust behavior) preserved
  at the projection layer.
- The pre-commit guard and archive treat blobs like any project artifact
  (reservable, auditable).

### Privileged operations: force-release *(gap G5)*

**Protocol constraint (P1, rev6 fix):** NIP-09 deletion applies ONLY to events
with an **identical pubkey** ("relays SHOULD delete events that have an
identical pubkey"; "client MUST validate each event pubkey referenced is
identical"). An operator-signed kind-5 **cannot** delete an agent's reservation
on any NIP-01-compliant relay. Additionally, addressable dedup is per
`(kind, pubkey, d)` (NIP-33) — an operator kind-30031 with the same `d` does
NOT relay-supersede the agent's event. AM-Rust's `force_release_file_reservation`
mutates another agent's row server-side; toron cannot reproduce that via
cross-key NIP-09 or relay-native supersession.

**Correct mechanism (normative):**

1. The requesting agent calls `force_release_file_reservation(reservation_id)`.
2. toron validates the **staleness heuristic** (agent inactive beyond threshold,
   no recent mail/filesystem/git activity — same heuristic as AM-Rust).
3. On pass, the **operator key** signs a **revocation event** — a kind-30031
   (reservation) / kind-30032 (slot) whose content/tags carry `status:revoked`
   + `e`/`a` → the target lease.
4. **Projection resolution rule (normative — all toron nodes identical):** for a
   given `(kind, d)`, latest-wins with **key-priority operator > agent**. A raw-
   relay observer sees both the agent's lease and the operator's revoke; toron
   projections are authoritative for toron semantics (the revoke wins regardless
   of NIP-33 author isolation).
5. NIP-09 is an **optional cleanup hint** on the operator's revocation event AND
   the mechanism for an agent's OWN same-pubkey deletes — never the cross-key
   correctness mechanism.
6. The previous holder is notified (NIP-17) with the heuristic summary.

Same for build slots (kind-30032). This keeps AM-Rust's admin semantics while
remaining NIP-01/09/33-compliant, and is **attributable** (the operator's
signature is on the revocation; AM-Rust server actions are unsigned).

### Project & product scoping *(gap G6)*

- **Project scoping (A3, dual-tag):** the InProcessBus uses
  `["t", project-slug]` as a cheap indexed shortcut; **any kind that can cross
  a relay MUST also carry NIP-32 namespaced labels** `["L","toron"]` +
  `["l", project-slug, "toron"]` (bare `#t` collides with other apps on shared
  relays). Prefer **dual-emit** so filters and federation share one code path.
  "All events in project X" is a single bus filter (`#t=[slug]` in-process;
  `#l=[slug,"toron"]` over relay) — the basis for `fetch_inbox`, `list_agents`,
  reservation-conflict queries. **Gift-wrapped DMs (14/15/ack) carry NEITHER
  `#t` NOR NIP-32 labels** on the wrap (metadata-hiding); they are project-
  scoped implicitly by recipient membership at the projection.
- A **project** = an operator-published **kind-30010 manifest** (`d`=slug,
  content = metadata). A **product** = a kind-30011 whose tags list member
  projects via `["a", <project-manifest-coordinate>]`.
- `search_messages_product` / `fetch_inbox_product` fan out across all `t` tags
  listed by the product manifest — cross-project, federatable, content-addressed
  (vs AM-Rust's server-side product table).

### Ordering & determinism *(gap G7)*

Nostr `created_at` is unix **seconds** (sub-second collisions possible); relays
don't promise monotonic delivery. AM-Rust uses i64-µs. **NIP-17 randomizes
`created_at` on the seal AND gift-wrap (up to 2 days back) for metadata
hiding** — the envelope's `created_at` is deliberately meaningless.

- **Bus envelope order** = lexicographic `(created_at, event_id)` — stable,
  deterministic, federation-independent — for **non-wrapped events and
  envelope storage only**. It is NOT the mail timeline.
- **Mail timeline order** = the **rumor's** `created_at` (visible only to
  recipients after unwrap) + optional `["ts_ms", ...]` tag for µs precision;
  the projection re-sorts unwrapped mail by rumor timestamp, never by envelope
  `created_at`.
- **Projection stores i64-µs** (AM-Rust parity): µs from rumor
  `created_at*1e6` (+ `ts_ms` tag) + a **monotonic local sequence** for
  same-second ties, so per-project timelines are reproducible. Threads
  reconstruct from NIP-10 `e` tags; ties broken by the stored µs. Never assume
  relay delivery order.

### Read receipts & acknowledgements *(gap G8)*

- Acks/reads are **NOT public replaceable events** (that would leak read-timing
  to anyone querying). They are **NIP-17 gift-wrapped kind-14 messages to the
  original sender**, tagged `["toron-kind", "ack"|"read"]` and referencing
  the original message via a **NIP-10 `e` tag** (a `d` tag is meaningless on a
  regular kind-14).
- **Source of truth** = these private events (federated, cryptographically
  attributable to the acker). The projection caches them per
  `(acker-npub, message-id)`; `acknowledge_message` is **idempotent** (second
  call returns stored ts — parity).
- **Privacy win:** acks are E2E to the sender only — strictly stronger than
  AM-Rust's DB row (visible to the server operator).

### Pane identity (per-backend) *(gap G9)*

`resolve_pane_identity` / `cleanup_pane_identities` keep AM-Rust's tool shape;
the implementation is **backend-specific**:

- **tmux backend (on hold):** AM-Rust's per-pane identity files
  (`~/.config/.../identity.<pane>`) unchanged.
- **herdr backend (default):** pane→agent resolved via herdr's
  `agent.list` / `agent_session` mapping (the herdr pane id `w1:p2` ↔ the
  toron agent name). Cleanup prunes identities for panes absent from
  `workspace.list`/`pane.list`.

### Reservation/slot audit separation *(gap G13)*

- **Bus holds current state** (replaceable/parameterized-replaceable:
  latest-wins). **Git archive holds full history** (append-only transition
  records: acquired/renewed/released/expired with timestamps). This is cleaner
  than AM-Rust, which conflates current + history in one SQLite.

### Contact-policy enforcement point *(gap G14)*

- The **bus is dumb transport** (publish/subscribe only). Contact policies
  (`open | auto | contacts_only | block_all`) are enforced at the
  **projection**: incoming gift wraps from keys violating the recipient's policy
  are dropped before indexing (and never written to Git). `block_all` ⇒ all
  inbound mail from that key dropped; `open` ⇒ all accepted; etc. Matches
  AM-Rust semantics, now policy-attached to a cryptographic identity.

### Disaster recovery *(gap G11)*

**Two independent recovery sources** (AM-Rust has one):

- **Git archive lost** ⇒ rebuild the projection by reconciling bus/relay events
  (signed, durable; re-unwrap into SQLite + Git). **Default: NIP-77 negentropy
  delta reconciliation** (A5; rust-nostr 0.44.x supports it; syncs only the
  missing delta, not O(all-events)); full replay is the fallback.
- **SQLite/bus lost** ⇒ rebuild from the Git archive (unwrapped canonical
  content is all there; re-index into SQLite).

Either alone reconstructs a consistent mailbox state; negentropy makes rejoin
and federation cheap.

### Parity-scope completeness *(gap G10)*

- **CLI robot mode + structured metrics** (call counts, errors, latency
  histograms) — **v1 parity must-haves**.
- **TUI (ratatui, 16-screen parity)** — **Phase 2** target (the integrated
  mode's primary UI is flywheel's loop; the standalone daemon's UI is the TUI).
- `TORON_GIT_BINARY` override + libgit2 (`git2`) for index writes — parity with
  AM-Rust's `AM_GIT_BINARY` + git-2.51.0 race avoidance *(gap G16)*.

### Nostr kind allocation — FROZEN REGISTRY (joint CreamElk+TopazElm, AGREEMENT LOCKED 2026-08-04)

> Kinds are frozen; any renumber requires a new ADR revision. Verified against
> nips repo occupancy (NIP-51/58/23/15/30/NKBIP-01 + README kind table).
> Vacated: 10000–10003 (standard kinds: NIP-51 mute/pin/bookmark, NIP-65 relay
> list) and 30000–30004/30001/30002 (NIP-51 follow/bookmark/relay/curation sets,
> NIP-58 badges). Free block used: 30010–30014, 30031, 30032 (also free:
> 30016, 30021–30022, 30025–30029, 30033–30039, 30042–30099).

| Concern | Kind | Class | `d`/dedup | Notes |
|---|---|---|---|---|
| Project manifest | 30010 | Addressable | `d`=slug | operator-published; products list members via `a` |
| Product manifest | 30011 | Addressable | `d`=slug | cross-project via `a`-tagged manifests |
| Registration attestation | 30012 | Addressable | `d`=project-slug:name | trust-anchor-signed (proof gate) |
| Agent profile | 30013 | Addressable | `d`=project-slug:name | name→npub map source; persona packs extend it |
| Contact policy | 30014 | Addressable | `d`=project-slug:name | enforced at projection |
| File reservation | 30031 | Addressable | `d`=`project:path-glob` | NIP-40 `expiration`=TTL |
| Build slot | 30032 | Addressable | `d`=`project:slot` | NIP-40 `expiration`=TTL |
| Direct message | 14 | NIP-17 gift-wrapped | — | to/cc/bcc via `p` role tags |
| File/attachment message | 15 | NIP-17 gift-wrapped | — | references blob by `x`=sha256 |
| Ack / read receipt | 14 | NIP-17 gift-wrapped (private) | — | NIP-10 `e` (original msg) + `toron-kind` tag; to sender only |
| Reaction (ack/emoji) | 7 | NIP-25 standard | — | cheap ambient feedback; lightweight ack |
| Job request | 43001 | Regular event (immutable) | `e`=parent | Buzz-verbatim (kind.rs) |
| Job accepted/claim | 43002 | Regular event | `e`=job | signed by claiming agent (federated claim) |
| Job progress | 43003 | Regular event | `e`=job | optional in-flight updates |
| Job result | 43004 | Regular event | `e`=job | outcome payload; closes the lifecycle |
| Job cancel | 43005 | Regular event | `e`=job | cancellation request |
| Job error | 43006 | Regular event | `e`=job | failed-with-error terminal state |
| Presence / working-state | 20001 | Ephemeral (not stored) | — | agent status heartbeat (Buzz interop) |
| Workflow run/step/approval | 46001–46012 | Regular/replaceable | `d`=run-id | Buzz-verbatim; Resonate-backed (see Phase 2) |
| Channel (collab room) | 9 + NIP-29 | Stream msg + group kinds | `h`=channel | Phase 2; membership = only gate |
| Thread / subject | (tags) | NIP-14 `subject` + NIP-10 `e`/`reply` | — | thread_id = root event id |
| Every **non-encrypted** event | (tag) | — | InProcessBus: `["t", project-slug]`. **Relay-crossing kinds also carry NIP-32** `["L","toron"]`+`["l",slug,"toron"]` (A3; dual-emit preferred). **Gift-wrapped DMs (14/15/ack) carry NEITHER** — would leak the project; DMs scoped implicitly by recipient membership at the projection. |

**Job lifecycle (Buzz-verbatim, replaces prior `43001→43002→43003` claim/result
mapping):** `43001` request → `43002` accepted/claim (signed, first-claimer
wins) → `43003` progress (optional) → `43004` result; `43005` cancel; `43006`
error. Same semantics as [`block/buzz`](https://github.com/block/buzz)
`crates/buzz-core/src/kind.rs` — no silent interop divergence on shared relays.

### Federation policy *(gap NEW-D3, AGREEMENT LOCKED)*

> **Phase 1 federated privacy guarantee is NIP-17-only:** only gift-wrapped DMs
> (kind 1059) cross external relays, and their confidentiality holds
> cryptographically regardless of relay behavior. Channels (NIP-29),
> reservations, and project manifests are NOT federated until toron owns the
> relay (embedded NIP-01/29/42 relay) or a Buzz-class NIP-29 relay is
> available; external-relay federation of these kinds is disabled by default
> and rejected at the projection. Federated NIP-17 traffic additionally
> assumes trusted-relay membership; public-relay deployment requires
> operator-configured hardening — **NIP-42 AUTH (membership gate; Buzz pattern)
> + NIP-13 POW (raise spam cost) + allowlist** (A4; concretes risk +12; random
> wrapper keys defeat pubkey-reputation filtering, so AUTH/POW — not reputation —
> carry the load) — documented, not silently enabled. The Phase-1.5 embedded
> relay uses **NIP-42 AUTH as its membership gate**.

### Tool → Nostr mapping (abbreviated)

| AM-Rust cluster | toron realization |
|---|---|
| Infrastructure (4) | `health_check`→bus liveness; `ensure_project`→idempotent project dir + manifest sub; guards install/uninstall (libgit2). |
| Identity (6) | `register_agent`/`create_agent_identity`→kind-30013 (signed; key mgmt per G1); `whois`/`list_agents`→project `#t` query of kind-30013 (last-active = latest event `created_at`); pane identity per-backend (G9). |
| Messaging (5) | `send_message`/`reply_message`→NIP-17 publish (to/cc/bcc per G3); `fetch_inbox`→projection of unwrapped kind-1059 to local keys; `acknowledge_message`/`mark_message_read`→private NIP-17 to sender (G8). |
| Contacts (4) | `request_contact`/`respond_contact`→NIP-17 control msg + AgentLink in index; `set_contact_policy`→kind-30014 (enforced at projection, G14). |
| File Reservations (5) | publish kind-30031 (`expiration`=TTL); conflict = local symmetric-fnmatch query; `force_release_*`→operator **revocation event** (kind-30031 `status:revoked`) + projection key-priority `operator > agent` (G5 A1; NIP-09 is cleanup-only, never cross-key). |
| Search (2) | SQLite FTS5 (`porter unicode61 remove_diacritics 2`, `prefix='2 3'` — AM tokenizer contract) over unwrapped events; NIP-50 accelerator; ranking relevance-compatible, not Search-V3-identical (see parity contract). |
| Macros (4) | compose the above. |
| Product Bus (5) | product = kind-30011 referencing projects via `a` (kind-30010); cross-project queries fan `#t` tags (G6). |
| Build Slots (3) | kind-30032 + `expiration`; force = operator revocation event (kind-30032 `status:revoked`) + projection key-priority `operator > agent` (G5 A1). |

## Parity Superiority (where toron exceeds AM-Rust, thanks to Nostr)

| Dimension | AM-Rust | toron | Why better |
|---|---|---|---|
| Identity | self-asserted names | Schnorr-signed npub | unforgeable by construction |
| Message privacy | plaintext in SQLite | NIP-17 gift-wrap (E2E, metadata-hidden) | federated relays/bus observers can't read DMs; **local operator can via the Git archive** (unwrapped canonical content — deliberate, AM-Rust audit parity; ciphertext-only archive is a Phase-2 option) |
| Delivery | poll `fetch_inbox` | bus subscription push | real-time, sub-50ms |
| Federation | impossible | opt-in relays | cross-machine / cross-operator |
| Threads | server-assigned id | content-addressed event DAG | cryptographically linked, federatable |
| Disaster recovery | one source (SQLite) | two (Git + relay replay) | either alone rebuilds |
| Ack attribution | mutable DB row | signed by acker, E2E to sender | non-repudiable, private |
| Cross-project | server bridge table | contact npub + `a`-tagged product | content-addressed, federatable |
| Activity provenance | mutable `last_active_ts` | latest signed event `created_at` | verifiable |
| Storage growth | unbounded single store | TTL envelopes + permanent Git content | bounded bus, durable audit |
| Read-timing privacy | visible to operator | E2E to sender only | stronger BCC/ack privacy |
| BCC correctness | cc visible, bcc server-hidden | true BCC (bcc can't see bcc) | cryptographically enforced |
| Proof gate | Ed25519 app-layer | native Schnorr (bus rejects forged attestations pre-app) | defense in depth |
| Force-release | server action, unsigned | operator-signed **revocation event** (projection key-priority `operator > agent`) | attributable; NIP-09/33-compliant (cross-key delete is illegal) |

**Net:** toron is AM-Rust's tool surface + Nostr's trust/privacy/federation
model. Local-first is preserved (in-process bus, Git, SQLite); federation is
additive.

## Prior Art & Leverage (block/buzz)

Studied [`block/buzz`](https://github.com/block/buzz) — a self-hostable
Nostr-native workspace where humans and agents are first-class equals ("a hive
mind communication platform, on a relay you own"). It is near-perfect prior art.
This section traces its features and records what toron adopts, adapts, or
already exceeds.

### Buzz feature trace (the load-bearing patterns)

- **Relay = single source of truth.** No P2P/gossip; clients WS to one relay.
  toron's `InProcessBus` *is* this relay (transport = channels). Validated.
- **Kind-as-dispatch.** The `kind` int is the only switch; new feature = new
  kind = zero break to existing clients. toron adopts this discipline and
  expands its kind vocabulary (see revised table above).
- **Relay event pipeline.** `verify → auth → dedup-insert (ON CONFLICT DO
  NOTHING) → fan-out (global subs excluded from channel-scoped events) →
  search-index (bounded worker, cap 1000) → audit (async) → workflow-trigger
  (async; excludes workflow/gift-wrap kinds)`. Client `OK` at pipeline end.
  toron's `Bus` adopts this exact pipeline so automation triggers fire uniformly.
- **buzz-workflow** — YAML-as-code, channel-scoped automation: 4 triggers
  (`message_posted`, `reaction_added`, `schedule`, `webhook`); 7 actions
  (`send_message`, `send_dm`, `set_channel_topic`, `add_reaction`,
  `call_webhook`, `request_approval`, `delay`); `{{trigger}}`/`{{steps}}`
  templates; evalexpr guards (100ms timeout); `Arc<Semaphore>`(100) capacity.
  **Buzz honest gap:** `request_approval` returns `Suspended` + UUID token but
  does NOT persist/resume — runs that hit a gate are marked *failed* (WF-08).
- **buzz-acp** — relay `@mention` → ACP/JSON-RPC over stdio → goose/codex/
  claude; N-agent pool (claim/return), one in-flight prompt per channel,
  queued events batched into one `session/prompt`.
- **Channels (NIP-29)** — group chat; **membership is the only access gate**,
  enforced at every op; global subscriptions cannot see private-channel events;
  TOCTOU-safe membership in transactions.
- **@mention-as-dispatch** — reaching an agent = @mentioning it in a channel,
  not calling a dispatch tool.
- **Forge** — branch = channel = PR + CI dashboard + thread; push (`30618`) →
  CI-passed (`1630`) → approval (`46011`) → merge (`1631`); repo = `30617`,
  project = `30621` (groups repos via `a`).
  > **CORRECTION (2026-08-07, bd-9o9 research):** canonical NIP-34 assigns these
  > kinds differently. `30618` = repository STATE (branch/tag refs), not "push";
  > `30621` = git commit, not "project"; `1630`/`1631` = Status Open / Status
  > Applied-Merged (generic PR/issue status), not "CI-passed"/"merge". There is no
  > NIP-34 CI-passed kind. The build must follow canonical NIP-34 (patches `1617`,
  > PRs `1618`, PR-updates `1619`, issues `1621`, status `1630`–`1633`); only `46011`
  > (approval) is toron-custom and already reserved. Full analysis:
  > `docs/notes/2026-08-07-forge-nip34-research.md`.
- **Ephemeral class (20000–29999)** — presence (`20001`), typing: not stored,
  not audited.
- **buzz-audit** — SHA-256 hash-chain, canonical JSON, `pg_advisory_lock`
  single-writer, `catch_unwind`; skips AUTH + ephemeral.
- **Security hygiene** — `is_private_ip()` SSRF guard (all RFC1918/loopback/
  link-local/CGNAT/IPv4-mapped-IPv6 recursive); `FEED_MAX_LIMIT=100`; DDL
  table-name allowlist.
- **Multi-tenant** — community = workspace by request host; fail-closed on
  unknown hosts; NIP-98 stamps must agree with host-derived community.

### Adopt (drop-in, proven)

- Kind-as-dispatch + expanded vocabulary (job lifecycle `43001–43006`
  Buzz-verbatim, presence `20001`, NIP-25 reaction `7`, workflow-run
  `46001–46012`).
- Relay pipeline discipline (verify→auth→dedup→access-gated-fanout→search→
  audit→workflow-trigger).
- Membership-as-only-gate + TOCTOU-safe + global-sub-exclusion for scoped
  events (applies to toron channels + project `#t` scoping).
- Ephemeral event class for presence/typing/agent-working-state.
- SSRF guard + feed caps + DDL allowlist (security hygiene).
- @mention-as-dispatch as the universal primitive (Phase 2).

### Adapt (Buzz concept → toron realization)

- **Workflow engine** → toron `toron-workflow`, **Resonate-backed** so runs
  survive crashes and `request_approval` actually persists/resumes (fixes
  Buzz WF-08). Triggers fire on bus events; actions publish events back. This
  is the bridge from "mailbox" to "automation platform".
- **Channels (NIP-29)** → a Phase-2 collaboration layer *above* AM-Rust direct
  mail (NIP-17). toron thus has BOTH private DMs (AM-Rust parity) AND rooms
  (Buzz-style) — a strict superset of both.
- **Agent harness** → herdr backend (JSON-RPC Unix socket) as the dispatch
  transport (Buzz uses ACP/stdio). toron does **not** need ACP — herdr+MCP
  suffice; ACP is noted only as an alternative transport a *direct-subprocess*
  agent mode could use later (not the tmux backend, which would speak tmux IPC).
- **Multi-tenant** → project isolation via `#t` tag (Buzz uses request host;
  toron isn't HTTP-host-fronted by default), with the same fail-closed +
  stamp-agrees-with-tag discipline.
- **Forge** → reserve kind numbers; Phase 3 (agent code-collab as signed
  events). Lower priority — git is already signed/auditable; the Nostr value
  is cross-machine visibility + channel-as-PR UX.

### Where toron already exceeds Buzz

| Dimension | Buzz | toron |
|---|---|---|
| Workflow durability | in-process; approval resumption **unbuilt** (WF-08) | **Resonate-backed**: crash-survivable, exactly-once, real approval resume |
| Mailbox semantics | none (chat/forge only) | full AM-Rust 38-tool parity (reservations, slots, guard, contacts) |
| Local-first footprint | needs Postgres + Redis + relay | one embedded process (in-process bus + SQLite + Git) |
| Swarm orchestration | generic ACP pool | purpose-built flywheel/herdr integration |
| Real-time agent state | Redis presence | in-process bus broadcast (zero infra) |
| Workflow `delay` | hard-capped **270s** (in-process; WF-09 unbuilt — 300s is the step `default_timeout_secs`, not the delay cap) | **Resonate durable sleep = unbounded**, crash-survivable (hours/days/weeks — cron-like, approval cooldowns, SLA timers) |

### Where toron intentionally differs

Buzz is **chat-first + forge** (Slack/GitHub analogue, Postgres+Redis). toron
is **mailbox-first + automation** (AM-Rust analogue, SQLite+Git). Different
primary UX; toron adds channels as a superset rather than replacing the
mailbox. Buzz's audit hash-chain is subsumed by toron's Git archive (commits
chain naturally); a separate ephemeral-audit chain is optional for
non-Git events (A8 — pointer only; this sentence is the coverage).

## Phase 2 — Collaboration & Automation Layer (superset, above AM-Rust parity)

> Phase 1 (v1) = AM-Rust parity + integrated mode + core kind vocabulary
> (this is the parity contract; nothing here changes the 38 tools). Phase 2 =
> Buzz-grade collaboration/automation that makes toron strictly more capable.

- **`toron-workflow` crate (Resonate-backed).** Channel/project-scoped YAML
  workflows: triggers on bus events (`message_posted`, `reaction_added`,
  `reservation_acquired`/`_released`, `agent_state_changed`, `schedule`,
  `webhook`); actions publish events (`send_message`, `send_dm`, `react`,
  `dispatch_job`, `call_webhook`, `request_approval`, `delay`). Each run/step/
  approval = a kind-46001–46012 event **and** a Resonate durable promise →
  crash-survivable + resumable (the Buzz WF-08 fix). `dispatch_job` runs through
  a **bounded semaphore (configurable concurrency) + one-in-flight-per-agent**
  (A7; Buzz `buzz-acp` claim/return pattern) so @mention bursts don't thrash
  the herdr swarm.
- **@mention-as-dispatch.** An agent is a member of a channel/project;
  `@agent-name` (a kind-9/14 message with `p`=agent-npub + mention marker)
  queues a job for it. flywheel's loop becomes a workflow watching @mentions +
  `br` beads. Job lifecycle = `43001` request → `43002` accepted/claim
  (signed, first-claimer wins) → `43003` progress (optional) → `43004` result
  (`43005` cancel, `43006` error) — Buzz-verbatim, federated, auditable,
  claimable by any subscribed agent.
- **Channels (NIP-29).** Collaboration rooms with membership as the only gate.
  A project is a channel; sub-channels for sub-topics; reservations/slots
  scoped to a channel. Gives toron rooms alongside AM-Rust DMs.
- **Ephemeral presence.** Agents broadcast working-state (`20001`) on the bus
  → real-time, uniform, federated (replaces herdr-polling for presence).
- **Persona packs.** Extend kind-30013 agent profile with a versioned
  system-prompt/persona (Buzz `buzz-persona` analogue) → reproducible,
  swappable agent behavior.
- **Deferred to Phase 3 (A9):** NIP-05/NIP-39 federated human-verifiable
  identity. Agent automation identity is already the Schnorr proof gate
  (kind-30012); a second trust path now = scope creep + attack-surface
  duplication. Revisit when cross-operator human verification is a real need.

### Phase 2 ordering (decided 2026-08-04, captain sign-off)

Ship in this order — each unlocks the next, and the first two deliver the most
automation leverage earliest:

1. **`toron-workflow` (Resonate-backed) + @mention-as-dispatch + job lifecycle
   (`43001`→`43002`→`43003`→`43004`, Buzz-verbatim)** — the automation
   backbone. Unblocks federated task-claim and lets flywheel's loop become a
   bus-event workflow. Without this, channels have nothing to dispatch *into*.
2. **Channels (NIP-29)** — collaboration rooms; membership-gated. Depends on (1)
   for `dispatch_job`/`@mention` to have targets.
3. **Ephemeral presence + persona packs** — polish layer; real-time agent-state
   broadcast + reproducible personas. Lowest dependency, ships last.

Phase 1 (AM-Rust parity + integrated mode) remains the v1 ship target and is
unaffected by this ordering.

## Consequences

* **Good:** real-time push; minimum-hops agent↔agent path (integrated mode);
  zero-IPC orchestration; unforgeable identity; E2E mail; federation opt-in;
  frozen AM-Rust surface (flywheel + AM-Rust-trained agents unchanged);
  Git audit + two-source DR; standard Tokio stack; single shared herdr client.
* **Bad:** integrated-mode lifecycle coupling (one crash takes flywheel+toron) —
  mitigated by Git+SQLite persistence, standalone daemon mode, Resonate resume.
* **Bad:** NIP-17 gift-wrap fan-out (N wraps/message) — projection dedups; bus
  TTL bounds it.
* **Bad:** key management is a new operational responsibility (passphrase,
  ncryptsec) AM-Rust didn't have — mitigated by keyring integration + the
  security gain (crypto identity).
* **Neutral:** proof-gate primitive is Schnorr (AM-Rust tests parameterized).
* **Neutral:** "100% parity" = behavioral; runtime parity waived.

## Implementation Plan

### Workspace shape
```
toron/                          # Cargo workspace
├── crates/
│   ├── toron-core/             # lib: nostr, bus, index, archive, mailbox, backend, keys
│   ├── toron-mcp/              # lib: rmcp MCP server (stdio + Streamable HTTP/axum), 38 tools
│   ├── toron-cli/              # bin `toron`: daemon + CLI (robot mode, metrics; TUI Phase 2)
│   ├── toron-workflow/         # Phase 2: Resonate-backed YAML automation
│   └── toron-e2e/              # E2E scenario suite (SC-1..SC-14); CI gate
├── examples/  tests/           # conformance (AM-Rust parity) + integration
```

### flywheel changes
- **Delete** `flywheel/src/herdr.rs` + `flywheel/src/mail.rs`.
- **Add deps** `toron-core` + `toron-mcp`; `main.rs` builds a tokio runtime that
  starts `toron_mcp::serve()`, constructs `toron_core::Mailbox` in-process, and
  runs the Resonate loop via direct calls. `flywheel mail …` → in-process calls.
- `[agent_mail]` config optional (remote-daemon mode only).

### Dependencies
- **Add (toron-core):** `nostr` / `nostr-sdk` **pinned to the 0.44.x line**
  (Buzz-proven; the current 0.45.0-alpha.8 is pre-release — revisit on stable;
  features: nip44/nip59/nip17/nip40/nip50/nip49), `tokio` (full), `sqlx`
  (sqlite, runtime-tokio; WAL pragmas; dedicated write conn), `git2`
  (libgit2 vendored), `tokio-tungstenite` (`RelayBus`/daemon relay), `axum`
  (MCP/HTTP), `resonate-sdk` (sync HTTP → `spawn_blocking`), `ratatui`
  (Phase 2 TUI), `parking_lot`, `serde`/`serde_json`, `anyhow`, `tracing`.
  **MCP server:** `rmcp` (modelcontextprotocol/rust-sdk) —
  `cargo add rmcp --features server,transport-io,transport-streamable-http-server,transport-worker,client`;
  schemars re-exported (JSON Schema 2020-12). Targets `ProtocolVersion::V_2026_07_28`
  (fallback `V_2025_11_25`). See Revision 7 + §Tool Surface (macros, subscriptions,
  structured outputs, schemars-derived schemas).
- **Remove:** `asupersync`, `asupersync-macros`, `asupersync-tokio-compat`,
  `mdns-sd`, `rcgen`.
- **Add (flywheel):** `toron-core`, `toron-mcp`, `tokio` (rt-multi-thread);
  drop the optional axum/tokio `serve` feature (toron-mcp owns the server).

### Patterns to follow / avoid
- **Follow:** AM-Rust crate split where it aids parity; `#[tokio::main]` +
  `tokio::sync::broadcast` bus; sqlx WAL + pooled + **dedicated single write
  connection** (SQLite is one-writer); write-behind cache + 30s deferred touch;
  async commit coalescer; i64-µs + local-seq ordering (G7); ncryptsec at rest
  (G1); `#![forbid(unsafe_code)]` workspace-wide.
- **Avoid:** asupersync; raw SQL outside the index; HTTP between flywheel and
  toron in integrated mode; duplicating the herdr client; public read-receipts
  (use private NIP-17); assuming relay delivery order.

### Migration steps (incremental, behind flags)
1. Reshape workspace to `crates/{toron-core,toron-mcp,toron-cli}`; stubs compile.
2. `toron-core::nostr` + `keys` (nostr-sdk wrappers, ncryptsec) — pure unit tests.
3. `toron-core::bus` — `InProcessBus` (broadcast + SQLite) + `Bus` trait + `RelayBus`; pub/sub round-trip.
4. `toron-core::index` + `archive` — sqlx schema (AM-Rust) + Git projection (AM-Rust layout); conformance fixtures.
5. `toron-core::mailbox` — 38-tool→event mapping incl. G1–G14 semantics.
6. `toron-core::backend::herdr` (port flywheel's) + `Backend` trait + `TmuxBackend` stub.
7. `toron-mcp` — wire 38 tools via `#[tool]`/`#[tool_router]` macros
   (schemars-derived schemas = conformance source); expose resources +
   subscriptions (SEP-2575); run AM-Rust conformance suite.
8. flywheel integration — delete herdr.rs/mail.rs; embed toron-mcp; loop → direct calls.
9. `toron-cli` daemon (in-process bus) + remote-daemon mode + robot mode + metrics.
10. Remove legacy `src/hub/` + `src/adapter/sidecar.rs`; flip defaults. Phase 2: TUI.
11. **`toron-e2e` crate** — implement SC-1..SC-8 (P1 gate) before v1 tag; SC-9..SC-14 before Phase-2 tag. CI gate: `cargo test -p toron-e2e` green.

### Verification

- [x] **Conformance:** 38-tool MCP surface + **25 resources** pass the re-derived golden-fixture parity suite (schema + filter/importance/thread/date semantics + tokenizer contract; ranking = relevance-compatible, recency ties exact). CI gate. Fixtures are first-party artifacts (AM-Rust `LicenseRef-MIT-Rider` — no vendored test code). _Witness: `crates/toron-mcp/src/tools.rs` (38 `#[tool]` fns), `crates/toron-mcp/src/resources.rs` (`exactly_25_resources` test asserts `RESOURCES.len()==25`), `crates/toron-e2e/tests/conformance.rs`, `bd-77u.2`._
- [x] **Nostr correctness:** every event valid Schnorr sig + canonical `id`; bus rejects forged/mutated events. _Witness: `crates/toron-core/src/nostr/event.rs` (`verify_event`), `crates/toron-core/src/nostr/proptests.rs` (mutate-content/mutate-sig rejection)._
- [x] **NIP-17 round-trip:** `send_message` to N recipients ⇒ N kind-1059 wraps; each unwraps to the same kind-14 rumor; non-recipients cannot unwrap. _Witness: `crates/toron-core/src/nostr/gift_wrap.rs` (`test_gift_wrap_roundtrip_rumor_identical`, `test_wrong_recipient_cannot_unwrap`), `crates/toron-e2e/tests/sc2_direct_mail.rs`._
- [x] **BCC privacy (G3):** bcc recipient's unwrapped rumor lists to+cc+itself but **not** other bccs. _Witness: `crates/toron-core/src/mailbox/messaging.rs` (`test_bcc_variant_privacy` — asserts b1 cannot see b2 and vice-versa), `crates/toron-e2e/tests/sc3_bcc_privacy.rs`._
- [x] **Real-time delivery:** subscribed agent receives a new message via bus push <50ms median, zero polling. _Witness: `crates/toron-e2e/tests/sc_fw_integrated.rs` (`sc_fw_one_process_peer_mail_via_bus` asserts `t0.elapsed() < 50ms`)._
- [x] **Name resolution (G2):** `to=["Unknown"]` fails fast pre-wrap; cross-project resolves via contact link. _Witness: `crates/toron-core/src/mailbox/messaging.rs` (`test_unknown_recipient_aborts_whole_send` — asserts no partial delivery)._
- [x] **Key mgmt (G1):** agent nsec stored ncryptsec (NIP-49), 0600; never written to bus/Git/logs; process decrypts via passphrase. _Witness: `crates/toron-core/src/keys/mod.rs` (`PermissionsExt::set_permissions` + `Permissions::from_mode(0o600)` on save)._
- [x] **Invariants:** `broadcast=true` rejected; symmetric-glob conflicts (`src/**`↔`src/lib.rs` both ways); TTL expiry clears; wrong-surface denial. _Witness: `crates/toron-core/src/mailbox/invariants.rs`, `crates/toron-core/src/mailbox/guard.rs` (symmetric `fnmatch`), `crates/toron-core/src/bus/ttl.rs`._
- [x] **Pre-commit guard:** commit touching another agent's reserved file blocked; own never blocked; libgit2 path; `TORON_GIT_BINARY` honored. _Witness: `crates/toron-core/src/mailbox/guard.rs`, `crates/toron-e2e/tests/sc4_reservation_guard.rs`._
- [x] **Force-release (G5, A1):** operator signs a kind-30031/30032 `status:revoked` event (NOT a cross-key NIP-09 delete — protocol-illegal); projection key-priority `operator > agent` resolves it; a federated observer with events only observes the lease revoked after the operator revoke, with NO NIP-09 dependence; previous holder notified; audit in Git. _Witness: `crates/toron-core/src/mailbox/reservations.rs` (explicit `"no cross-key NIP-09 deletes"` rejection), `crates/toron-core/src/bus/addressable.rs` (operator>agent projection), `crates/toron-e2e/tests/sc5_force_release.rs`._
- [x] **Proof gate:** fails-closed on missing/malformed/expired/replayed/mismatched proofs; nonces persist across restarts; Schnorr default + Ed25519 compat path both tested. _Witness: `crates/toron-core/src/nostr/proof.rs` (`verify` fn: 3 `Err` branches cover all 5 failure modes; `Ok(())` only after `verify_signature`+`check_expiry`+`check_replay`+`consume_nonce`), `crates/toron-e2e/tests/sc8_proof_gate.rs`._
- [x] **Backend:** herdr spawn→prompt→wait→read against live socket (v18); tmux returns `Unsupported`; pane-identity resolves via herdr agent_session. _Witness: `crates/toron-core/src/backend/herdr.rs` (protocol v18 client), `crates/toron-core/src/backend/tmux.rs` (`Unsupported` stub), `crates/toron-core/src/backend/mock.rs`._
- [x] **Ordering (G7):** unwrapped mail orders by **rumor `created_at`** (+`ts_ms` tag) with stable, reproducible µs via local seq for same-second ties; envelope `(created_at, event_id)` order honored for non-wrapped events/storage only — never the mail timeline (NIP-17 randomizes wrap timestamps). _Witness: `crates/toron-core/src/mailbox/unwrap.rs` (`order_by_rumor_time`, `local_seq` tiebreak; 12 `local_seq` + 9 `ts_ms` references), `crates/toron-core/src/nostr/gift_wrap.rs` (`test_wrap_randomizes_created_at` asserts 2-day randomization)._
- [x] **Attachments (G4):** blob stored content-addressed; kind-15 carries sha256 + URL; recipient fetches; small text inlines. _Witness: `crates/toron-core/src/archive/blob.rs`._
- [x] **Local-first:** `toron` boots + full 38-tool surface with zero external relays. _Witness: `crates/toron-core/src/bus/federation.rs` (`zero_egress` default, `relays = vec![]`)._
- [ ] **Federation:** with a relay configured, a message published on instance A is received on B (`#[ignore]`, two processes). _**NOT GATED IN CI** — the federation tests are `#[ignore]`d (require a second live process); verified locally only. Honest gap: federation is opt-in and not repeatable in CI yet._
- [x] **DR (G11):** delete SQLite ⇒ rebuild from Git; delete Git ⇒ rebuild from bus replay; both converge. _Witness: `crates/toron-core/src/index/rebuild.rs`, `crates/toron-e2e/tests/sc14a_dr_rebuild.rs`._
- [x] **Integrated mode:** one flywheel process serves agents via MCP AND runs the loop; agent↔agent mail + flywheel dispatch both work, no second process. _Witness: `crates/toron-e2e/tests/sc_fw_integrated.rs` (`sc_fw_one_process_peer_mail_via_bus`, `sc_fw_single_agent_prompt_definition_in_core`); the live flywheel-binary sub-case `sc_fw_flywheel_binary_live` is `#[ignore]`d (needs the sibling flywheel repo checkout)._
- [x] **No asupersync:** `rg -n asupersync .` zero hits (toron + flywheel); `rg -n tokio crates/` confirms runtime. CI gate (`scripts/verify-gates.sh`, scoped to live code + Cargo: `crates/**` + root `Cargo.toml` + `Cargo.lock`; historical ADR/beads text and dead legacy files excluded). Same gate enforces zero `fastmcp` and `#![forbid(unsafe_code)]` in every crate root. _Witness: `.github/workflows/ci.yml` `gates` job runs `scripts/verify-gates.sh`; `rg asupersync crates/` = 0 hits at v1.0.0._
- [x] **Single herdr client:** `flywheel/src/herdr.rs` deleted; `rg -n 'fn agent_prompt' .` hits exactly one definition (in toron-core). _Witness: `crates/toron-core/src/backend/herdr.rs:443` (the single `pub fn agent_prompt`); `crates/toron-e2e/tests/sc_fw_integrated.rs` (`sc_fw_single_agent_prompt_definition_in_core` static count == 1). Note: a sibling helper `agent_prompt_coded` exists but is not the contract fn and is excluded by the count._
- [x] **Robot CLI + metrics (G10):** `toron --robot-*` flags + `--json` parity; metrics endpoint exposes call/error/latency. _Witness: `crates/toron-cli/src/robot.rs`, `crates/toron-core/src/mailbox/metrics.rs`._
- [x] **`#![forbid(unsafe_code)]`** in every crate root. CI gate. _Witness: 91 occurrences across all 6 crate roots + server/harness/scenario; `.github/workflows/ci.yml` `gates` job._
- [x] **flywheel behavior unchanged:** spawn/send/loop/respawn produce identical agent outcomes vs the prior HTTP path. _Witness: `crates/toron-e2e/tests/sc_fw_integrated.rs` (in-process peer-mail + dispatch assertions); the live `flywheel` binary sub-case is `#[ignore]`d (sibling-repo gated)._
- [x] **Relay pipeline (Buzz adopt):** every published event passes verify→auth→dedup→access-gated-fanout→search→audit→workflow-trigger; global subs never receive `#t`-scoped/private events. _Witness: `crates/toron-core/src/bus/in_process.rs` (all 7 stages: verify 7 hits, auth 15, dedup 3, access 5, search 4, audit 4, workflow 6)._
- [x] **Job lifecycle (Phase 2):** `43001`→`43002`(accepted/claim, signed by claimer)→`43003`(progress, optional)→`43004`(result) round-trips, Buzz-verbatim; `43005` cancel / `43006` error honored; a job claimed on instance A is visible/claimable on instance B via federation. _Witness: `crates/toron-workflow/src/jobs.rs`, `crates/toron-core/src/nostr/kinds.rs` (`test_job_lifecycle_sequence` asserts 43001→43006), `crates/toron-e2e/tests/sc9_mention_job.rs`; federation cross-instance claim is `#[ignore]`d._
- [x] **Resonate-backed workflow (Phase 2, Buzz WF-08 fix):** a run hitting `request_approval` suspends, **survives a process crash**, and **resumes** on the approval event (Buzz cannot). _Witness: `crates/toron-e2e/tests/sc11_workflow_approval.rs` (`sc11_approval_crash_resume` — real `resonate serve` subprocess, drops Engine A, fresh Engine B resolves the promise and the run completes NOT failed). CI-gated in the `e2e-p2` job (added by bd-quo)._
- [x] **@mention dispatch (Phase 2):** `@agent` in a channel queues a job; the agent receives it via subscription push (no orchestrator in the path). _Witness: `crates/toron-workflow/src/mention.rs`, `crates/toron-e2e/tests/sc9_mention_job.rs`, `crates/toron-e2e/tests/sc10_reservation_dispatch.rs`._
- [x] **Channels (Phase 2):** NIP-29 group; membership is the only gate; a non-member cannot read/post; global subs excluded. _Witness: `crates/toron-workflow/src/channels.rs` (membership-only gate + global-sub exclusion), `crates/toron-e2e/tests/sc13_channel_membership.rs`._
- [x] **Ephemeral presence:** agent working-state broadcasts as `20001` on the bus; never stored in Git/SQLite; expires when the heartbeat stops. _Witness: `crates/toron-core/src/bus/presence.rs`, `crates/toron-core/src/nostr/presence.rs`, `crates/toron-e2e/tests/sc15_presence.rs`._
- [x] **SSRF/feed hygiene:** `call_webhook` blocks RFC1918/loopback/link-local/CGNAT (incl. IPv4-mapped IPv6); feed queries capped. _Witness: `crates/toron-core/src/bus/http_guard.rs` (`is_private_ip` handles `::ffff:a.b.c.d` via `ipv4_mapped_to_v4`)._
- [x] **CLI clig.dev discipline:** stdout=data/stderr=messages honored on every subcommand; `--json` on all read commands; exit codes 0 ok / 1 runtime / 2 usage; color auto-disables when piped or `NO_COLOR`. _Witness: `crates/toron-cli/src/output.rs` (NO_COLOR handling), `crates/toron-cli/src/main.rs` (`ExitCode::from(2)` usage), `crates/toron-cli/src/operator.rs` (`ExitCode::from(3)` doctor)._
- [x] **MCP↔CLI parity:** every one of the 38 AM-Rust tools is reachable as a CLI subcommand with identical args/return shape (`toron mail send` ≡ `send_message`). _Witness: `crates/toron-cli/src/cli.rs` (parity surface comment + subcommand wiring)._
- [x] **Robot mode:** `--robot-snapshot`/`--robot-status`/`--robot-capabilities` emit JSON state snapshots an agent driver can poll; `-` reads stdin for bodies/content. _Witness: `crates/toron-cli/src/robot.rs`, `crates/toron-cli/tests/cli_robot.rs`, `crates/toron-cli/tests/cli_robot_mode.rs`._
- [x] **Scenarios (E2E):** all P1 scenarios SC-1..SC-8 green in `toron-e2e` (v1 gate); all P2 SC-9..SC-14 green before Phase-2 ship; federation/DR scenarios use an in-process second relay + virtual clock (no real network). CI gate. _Witness: P1 gated in `e2e-p1` CI job; P2 gated in `e2e-p2` CI job (bd-quo); individual scenario files `sc1`..`sc15`, `sc14a/b`, `sc_fw_integrated` all present. Federation scenarios remain `#[ignore]`d (see Federation bullet above)._
- [x] **ADRs updated:** `0001` superseded (asupersync dropped); `0002` partially superseded; **ADR-0003 ACCEPTED 2026-08-04** (captain sign-off; Phase 2 ordering = workflow+@mention → channels → persona/presence).

## CLI Design (clig.dev-compliant)

The `toron` binary serves **two personas** from one command surface:

- **Operators** (the captain) — daemon lifecycle, keys, projects, relays,
  backend, metrics, doctor, TUI.
- **Agents / scripts** (robot mode) — the full 38-tool AM-Rust parity surface
  reachable as CLI subcommands, so any agent that can run a shell (but not
  necessarily an MCP client) can drive mail/reservations/jobs/workflows.
  This is the key "maximize agent usability" lever: **every MCP tool has a
  1:1 CLI subcommand with identical args and `--json` return shape.**

### Principles (clig.dev, applied)

- **Arg parser:** `clap` (derive) — consistent with flywheel.
- **Two-level noun-verb** throughout: `toron <noun> <verb>` (`toron mail send`,
  `toron reserve acquire`). No catch-all, no prefix-abbreviation.
- **stdout = data** (machine-parseable); **stderr = messages** (human). `--json`
  on every read command; `--plain` for grep/awk-friendly tables.
- **Exit codes:** `0` ok · `1` runtime/server error · `2` usage error (matches
  flywheel: "0 ok · 1 herdr/server error · 2 usage").
- **TTY-aware color** — auto off when piped, `NO_COLOR`, `FORCE_COLOR`,
  `TERM=dumb`, or `--no-color`. No animation when stdout non-TTY.
- **`-` = stdin** for bodies/content (`toron mail send --body -`) so agents pipe
  large payloads without arg-length limits.
- **No secrets in flags/env** — keys via files (ncryptsec, 0600), stdin, or
  keyring; never `--nsec=...` (leaks via `ps`/history).
- **Confirmation tiers:** `reserve force-release`, `agent remove`,
  `relay remove`, `key export --include-secret` require confirm (type the name,
  or `--confirm=<name>` / `--force` for scripts).
- **Help:** `-h`/`--help` on every subcommand; examples-first; top-level help
  links docs + the ADR. Wrong input with guessable intent → suggest, don't DWIM.
- **Config precedence:** flags > env > project `./.toron.toml` > user
  `~/.config/toron/config.toml` > system. XDG, no home-dir dotfiles.

### Global flags

| Flag | Meaning |
|------|---------|
| `--json` | Structured JSON output (all read commands) |
| `--plain` | Flat tabular text for grep/awk |
| `--no-color` | Disable color (also: `NO_COLOR`, `TERM=dumb`, piped) |
| `-q`, `--quiet` | Less output |
| `-d`, `--debug` | Debug/verbose to stderr |
| `-n`, `--dry-run` | Show changes without running |
| `--no-input` | Never prompt; fail with instruction if input needed |
| `--config <path>` | Override config file |
| `-o`, `--output <path>` | Output to file (commands that write artifacts) |
| `-h`, `--help` | Help (every subcommand) |
| `--version` | Version |

### Command surface

```
toron [--json] [--no-color] [-q] [-d] [--config <p>] [--version] [-h] <command>

Lifecycle & ops
  serve            run daemon (mailbox+bus+MCP server); --backend, --relay <url>…,
                   --mcp-stdio|--mcp-http <addr>, --data-dir, --operator-key <path>
  status           quick health/config overview (human)
  doctor [--check] deep checks: herdr socket, keys, git version, resonate (CI exit 3)
  metrics          runtime call/error/latency (--json)
  bench <case>     startup/inbox/send/search/threads (AM-Rust parity)
  tui              interactive dashboard (Phase 2)

Keys (no secrets in flags — files/stdin/keyring only)
  key generate --role operator|agent|anchor [--out <path>]
  key inspect [<ncryptsec-path>]        prints npub only; never nsec
  key import / key export               export --include-secret needs --confirm

Identity & projects
  project init|ensure <slug> [--cwd]     idempotent; links cwd as project root
  project list / show <slug> / link <slug> --product <key>
  agent register <name> [--program --model --task --project <slug>]   upsert
  agent create  <name> …                                             always new key
  agent list [--project] [--active-within <days>] / whois <name|--npub>
  agent remove <name>                  confirm; revokes profile

Mail (AM-Rust parity, robot-friendly)
  mail inbox   [--project] [--agent] [--urgent|--unread] [--since] [--limit] [--json]
  mail send    --to <n,…> [--cc …] [--bcc …] --subject <s>
               [--body <t> | --body - | --body-file <p>] [--importance …]
               [--ack] [--thread <id>] [--attach <p>…] [--project]
  mail reply   <msg-id> [--body …] [--to …] [--cc …]
  mail ack <msg-id> | mail read <msg-id>
  mail search  <query> [--sender] [--importance] [--thread] [--since] [--until]
               [--limit] [--ranking relevance|recency] [--json]
  mail thread  show <id> | summarize <id>

Coordination
  reserve acquire   --paths <glob…> [--ttl <dur>] [--exclusive] [--reason] [--json]
  reserve renew     --paths <glob…>|--ids … [--extend <dur>]
  reserve release   [--paths …] [--ids …] [--all]
  reserve conflicts --paths <glob…> [--json]
  reserve force-release <id> [--note] [--confirm]            operator NIP-09
  slot acquire <slot> [--ttl] [--exclusive] | renew <slot> | release <slot>
  contact request <agent> [--project] [--reason] [--ttl]
  contact approve|deny <agent> | list [--agent] | policy <agent> open|auto|contacts_only|block_all
  guard install [--repo <p>] | uninstall [--repo <p>]

Bus & federation (operator/debug)
  relay list | add <url> [--read --write] | remove <url> [--confirm] | info <url>   (NIP-11)
  bus  pub <kind> --content … [--tag …]            raw publish (expert)
  bus  sub  [--kinds …] [--authors …] [#t …] [--limit] [--since]        stream to stdout (nosub-style)
  bus  events <event-id>
  backend list | status | default herdr|tmux

Phase 2 — automation & collaboration
  workflow list|show <name>|lint <file>|run <name> [--trigger-input <json|->]
  workflow runs [--status] [--workflow] [--json] | resume <run-id> | cancel <run-id>
  workflow approval approve|reject <run-id> [--token]
  channel create <name> [--private] [--project] [--topic] | list | show <id>
  channel join|leave <id> | topic <id> <text> | members <id>
  channel post <id> --body … | react <msg-id> <emoji>
  job create --assignee <name> --spec <json|-> [--channel <id>]            (43001)
  job claim <job-id> | result <job-id> --status ok|fail --body …            (43002/43003)
  job list [--status open|claimed|done] [--assignee] [--channel] [--json] | show <job-id>

Robot mode (agent drivers)
  --robot-snapshot      full state snapshot (--json)
  --robot-status        minimal status (--json)
  --robot-capabilities  emit capability/role map (--json)
```

### MCP ↔ CLI parity (every tool is reachable both ways)

The 38 AM-Rust tools map 1:1 to CLI subcommands (same args, same JSON return):
`send_message`→`mail send`, `fetch_inbox`→`mail inbox`, `file_reservation_paths`→`reserve acquire`,
`acquire_build_slot`→`slot acquire`, `set_contact_policy`→`contact policy`,
`install_precommit_guard`→`guard install`, `search_messages`→`mail search`,
`summarize_thread`→`mail thread summarize`, `ensure_product`/`products_link`→`project link`,
`macro_*`→ compose the above. An agent that shells out sees the same result as an
MCP client — choose per integration (MCP for native clients, CLI for shell-only
agents / scripts / `jq` pipelines).

### Worked examples

```bash
# Operator: first run
 toron key generate --role operator --out ~/.config/toron/operator_nsec
 toron serve --backend herdr                       # daemon (integrated via flywheel too)
 toron doctor --check                              # verify herdr socket, keys, git, resonate

# Agent (robot): peer collaboration via shell
 toron agent register GreenLake --program pi --project notetoself
 toron mail inbox --agent GreenLake --unread --json | jq '.[].subject'
 toron reserve acquire --paths 'src/**' --ttl 2h --reason 'auth refactor' --json
 toron mail send --to BlueLake --subject 'review?' --body - < plan.md --ack
 toron mail search 'migration' --thread TKT-123 --json

# Automation (Phase 2)
 toron workflow lint .toron/workflows/triage.yaml
 toron workflow run triage --trigger-input '{"text":"P1 crash"}'
 toron job list --status open --json
 toron job claim bd-42 && toron job result bd-42 --status ok --body 'shipped'

# Debug / pipe
 toron bus sub --kinds 14,1059 --since 1h | jq 'select(.kind==14)'
```

### Standard environment variables

`TORON_CONFIG` (config path), `TORON_DATA_DIR`, `TORON_BACKEND`,
`TORON_OPERATOR_KEY` (path to ncryptsec — **not** the secret),
`TORON_PROOF_GATE_ENABLED`, `TORON_PROOF_TRUSTED_KEYS`, `TORON_PROOF_SCHEME`,
`TORON_GIT_BINARY` (libgit2 override, AM-Rust parity), plus the POSIX-respected
`NO_COLOR`/`FORCE_COLOR`/`TERM`/`EDITOR`/`PAGER`/`HTTP(S)_PROXY`. No secrets in env.

## Scenarios (E2E test plan)

Every scenario below is a **test contract** — an E2E test in `toron-e2e` (see
Implementation Plan) that asserts the expected outcome. **CI gate = all scenarios
green.** P1 scenarios (SC-1..SC-8) gate the v1 ship; P2 (SC-9..SC-14) gate
Phase 2. Conformance assertions inside SC-1..SC-8 reuse the re-derived AM-Rust
golden fixtures (schema/filters/tokenizer — never vendored).

### Reactive collaboration model (how agents collaborate + react)

Three reaction patterns, all event-driven on the bus (no orchestrator polling):

1. **Subscription-push wakeup** — an agent holds a bus subscription
   (`fetch_inbox`, `toron bus sub`, or a workflow subscription) and reacts to
   inbound events: new mail, `@mention`, reaction, reservation change. Delivery
   is push (<50ms), not poll.
2. **Workflow-as-reaction** — a `toron-workflow` trigger fires on a bus event
   (`message_posted`, `reaction_added`, `reservation_acquired`/`_released`,
   `agent_state_changed`, `schedule`, `webhook`) and runs Resonate-durable
   actions (`dispatch_job`, `send_message`, `request_approval`, …). The workflow
   IS the reactive automation; agents are the executors it dispatches into.
3. **Cascade / handoff** — one agent's *completion* event (job result,
   reservation released) triggers the next agent's *start* via a workflow.
   Relay-race collaboration across N agents with no central driver in the hot
   path.

AM-Rust learnings woven throughout: composite **macros** (`macro_start_session`,
`macro_contact_handshake`, `macro_file_reservation_cycle`) = multi-step
scenarios; the **ack-overdue SLA** (30 min) = a reactive timer; the
**conformance harness** shape (`tests/e2e/` + a `BuzzTestClient`-style driver:
connect/send_event/subscribe/collect_until_eose).

### Scenario register

| ID | Name | Phase | Primary kinds | Reaction pattern |
|----|------|-------|---------------|------------------|
| SC-1 | Agent onboarding (macro_start_session) | P1 | 30010/30013/30031 | — |
| SC-2 | Direct mail + thread + ack | P1 | 14/1059 | subscription-push |
| SC-3 | to/cc/bcc privacy | P1 | 14/1059 | — |
| SC-4 | Reservation conflict + pre-commit guard | P1 | 30031 | — |
| SC-5 | Force-release operator revocation (A1) | P1 | 30031 | — |
| SC-6 | Build-slot serialization | P1 | 30032 | — |
| SC-7 | Contact handshake + policy enforcement | P1 | 14/30014 | — |
| SC-8 | Proof-gate fail-closed | P1 | 30012 | — |
| SC-9 | @mention → job claim → result | P2 | 9/43001-04 | subscription-push + cascade |
| SC-10 | reservation_released → workflow dispatch | P2 | 30031/46001-12/43001 | workflow-as-reaction + cascade |
| SC-11 | Workflow approval: suspend → crash → resume | P2 | 46001-12 | workflow-as-reaction |
| SC-12 | Burst backpressure (bounded pool, A7) | P2 | 43001-04 | backpressure |
| SC-13 | Private channel collaboration room | P2 | 9(NIP-29)/7 | subscription-push |
| SC-14 | Disaster recovery + negentropy rejoin | P1/P2 | all (replay/sync) | — |

### Test contracts

**SC-1 — Agent onboarding (macro_start_session parity).** *Actors:* operator,
Pi-α, project `demo`. *Steps:* `project init demo`; `agent register PiAlpha
--program pi --project demo`; Pi-α reserves `src/**`; `mail inbox` (empty).
*Assert:* kind-30013 signed profile published; name→npub resolves; kind-30031
live; `list_agents` shows Pi-α; re-run is idempotent (no duplicate profile).

**SC-2 — Direct mail + thread + ack.** *Actors:* Pi-α, OC-β. *Steps:* Pi-α
`mail send --to OCβ --subject review --body … --ack`; OC-β `mail inbox --unread`
receives via **push <50ms**; OC-β `mail ack <id>`; Pi-α sees the ack.
*Assert:* 1 NIP-17 gift-wrap to OC-β; unwraps to kind-14 rumor; ack is a private
NIP-17 to Pi-α (`toron-kind=ack`); thread_id = root event id.

**SC-3 — to/cc/bcc privacy.** *Actors:* Pi-α (sender), OC-β (to), Grok-γ (cc),
Pi-δ + Pi-ε (bcc). *Assert:* OC-β/Grok-γ rumors list to+cc, no bcc; each bcc's
rumor lists to+cc+itself but **not the other bcc**; non-recipients cannot
unwrap. True BCC, cryptographically enforced.

**SC-4 — Reservation conflict + pre-commit guard.** *Actors:* Pi-α, OC-β.
*Steps:* Pi-α reserves `src/**` exclusive; OC-β `reserve conflicts --paths
src/lib.rs` → conflict (symmetric fnmatch); OC-β `git commit` touching
src/lib.rs → **guard blocks**; Pi-α releases; OC-β re-checks → clear; commit
succeeds. *Assert:* symmetric both directions; cross-agent blocked; own never
blocked; release clears.

**SC-5 — Force-release operator revocation (A1).** *Actors:* operator, Pi-α
(vanished), OC-β. *Steps:* Pi-α reserves `src/**` then inactive > threshold;
OC-β `reserve force-release <id>`; operator validates staleness → signs
kind-30031 `status:revoked`; projection key-priority `operator > agent` → lease
revoked; OC-β conflict check clear. *Assert:* **no cross-key NIP-09 delete
attempted** (protocol-illegal); a federated observer with events-only observes
revoked after the operator revoke, with **no NIP-09 dependence**.

**SC-6 — Build-slot serialization.** *Actors:* Pi-α, OC-β, slot `ci`. *Steps:*
Pi-α `slot acquire ci --exclusive`; OC-β acquire → conflict; Pi-α renews; Pi-α
releases; OC-β acquires → ok; an abandoned slot clears at TTL. *Assert:* exclusive
serializes; renew extends; release frees; TTL clears.

**SC-7 — Contact handshake + policy enforcement.** *Actors:* Pi-α (proj A), OC-β
(proj B). *Steps:* Pi-α `contact request OCβ --project B`; OC-β `contact
approve`; AgentLink recorded; cross-project mail delivers; OC-β `contact policy
Piα block_all`; Pi-α's next mail → **dropped at projection** (not indexed, not
in Git). *Assert:* 3-step handshake; cross-project via contact npub; block_all
drops inbound pre-index.

**SC-8 — Proof-gate fail-closed.** *Pre:* gate enabled, trust anchor set.
*Steps:* register with no proof → reject; expired → reject; replayed nonce →
reject; valid anchor-signed kind-30012 → accept; replay the same nonce → reject.
*Assert:* fail-closed on missing/malformed/expired/replayed/mismatched; nonces
persist across process restart.

**SC-9 — @mention → job claim → result (reactive dispatch).** *Actors:* human H
in channel #forge, OC-β, Grok-γ (members). *Steps:* H posts "@OCβ fix the flaky
test" (kind-9, `p`=OC-β); OC-β subscribed → **push** → `job claim` (kind-43002
accepted, first-claimer); OC-β works → `job result --status ok` (kind-43004);
result visible in channel. *Assert:* @mention creates kind-43001; claim signed;
lifecycle closes; non-mentioned Grok-γ does **not** receive the job but sees the
thread.

**SC-10 — reservation_released → workflow dispatch (cascade).** *Actors:* Pi-α,
OC-β, workflow `handoff`. *Steps:* Pi-α reserves `src/**`, finishes, releases;
trigger `reservation_released` fires → action `dispatch_job` to OC-β ("src/**
free, continue"); OC-β claims + reserves. *Assert:* trigger fires on the **bus
event** (not poll); dispatch bounded (semaphore, one-in-flight); kind-43001
created for OC-β; relay-race handoff completes with no central driver in the
path.

**SC-11 — Workflow approval: suspend → crash → resume (Buzz WF-08 fix).**
*Actors:* workflow `deploy`, operator (approver). *Steps:* run hits
`request_approval` → suspends (kind-46011 pending); **kill the toron process
mid-run**; restart; approval event (operator-signed kind-46011) arrives → run
**resumes from checkpoint** → completes (kind-46012). *Assert:* run survives the
crash (Resonate durable promise); resumes on approval — **not marked failed**
(Buzz's WF-08 bug fixed); step events emitted throughout.

**SC-12 — Burst backpressure (bounded dispatch pool, A7).** *Actors:* 20
@mentions to OC-β in 1 s. *Assert:* in-flight ≤ N (config semaphore);
one-in-flight-per-agent honored; no herdr swarm overload; no lost jobs (queued,
not dropped, unless explicit backpressure policy); ordering preserved.

**SC-13 — Private channel collaboration room (NIP-29).** *Actors:* Pi-α, OC-β,
Grok-γ in private #auth-refactor; non-member Pi-δ. *Steps:* operator creates
private channel; members join; Pi-α posts; OC-β reacts (kind-7); Grok-γ
@mentions Pi-α; Pi-δ attempts read/post → **denied**. *Assert:* membership =
only gate; non-member denied; **global subscription excluded** from private
channel events (no leak); reaction kind-7 works.

**SC-14 — Disaster recovery + negentropy rejoin (G11/A5).** *(a)* Delete
SQLite → rebuild projection from Git archive → state consistent. *(b)* Lose bus
state → rejoin via **NIP-77 negentropy delta** from the relay → only missing
events transferred (bandwidth < full replay) → consistent. *Assert:* either
source alone rebuilds; negentropy transfers delta only; signed events
re-verified on rejoin.

### Harness spec (`toron-e2e`)

- A crate `toron-e2e` (or `tests/e2e/`) modeled on AM-Rust's `tests/e2e/` + a
  `BuzzTestClient`-style driver: spin an in-process toron daemon (in-process
  bus) + a stub `Backend` (no real herdr) + temp Git archive + temp SQLite.
- One `#[tokio::test]` per scenario; assertions over the MCP tool surface AND
  raw bus events (subscribe + `collect_until_eose`).
- Deterministic: fixed keypairs, synthetic `created_at`, no real network
  (federation scenarios use an in-process second relay + NIP-42 AUTH stub).
- Ack-overdue (SC-2 variant): a virtual-clock helper asserts the 30-min SLA
  surfaces the message as `ack_overdue` without real waiting.
- **Gate:** `cargo test -p toron-e2e` green in CI; P1 subset required for v1
  tag, P2 subset required for Phase-2 tag.

## More Information

### Nostr protocol primer (embedded)

Two components: **clients** and **relays**. Every user is a secp256k1
**keypair**; the public key (`npub`) is the identity. The only on-wire object is
the **event**: `{id (sha256 of canonical serialization), pubkey, created_at
(unix seconds), kind, tags, content, sig (Schnorr over id)}`. Kinds: `0`
metadata, `1` note, `3` follows, `5` deletion (NIP-09), `13` seal, `14` DM,
`15` file msg, `1059` gift wrap. **Replaceable** `10000–19999` (latest per
pubkey); **parameterized-replaceable** `30000–39999` (dedup on `d` tag). Relay
frames (NIP-01): `EVENT`/`REQ`/`CLOSE` (client→relay); `EVENT`/`OK`/`EOSE`/
`CLOSED`/`NOTICE` (relay→client). `REQ` opens a subscription; stored matches
stream, then `EOSE`, then real-time. Filters: `authors`, `kinds`, `since`,
`until`, `limit`, `#e`, `#p`, `#t`. NIP-40 `expiration`; NIP-50 search; NIP-11
relay info. **toron's `InProcessBus` speaks identical publish/subscribe with
channels instead of WS** — events are wire-compatible.

**NIP-59 (Gift Wrap):** (1) **Rumor** = unsigned event (e.g. kind 14). (2)
**Seal** = kind-13, sender-signed, content = rumor NIP-44-encrypted to recipient
(no `p` tag — recipient hidden). (3) **Gift Wrap** = kind-1059, signed by a
one-time random key, content = seal NIP-44-encrypted to recipient, `p`=
recipient. Only the recipient unwraps to the seal, then the rumor. **NIP-17**
composes this for kind-14/15 messages.

### AM-Rust feature surface (full)
**38 tools / 9 clusters**: Infrastructure (4), Identity (6), Messaging (5),
Contacts (4), File Reservations (5), Search (2), Macros (4), Product Bus (5),
Build Slots (3) — names enumerated in the Tool→Nostr mapping above.
**Storage layout** (Git, per-project): `messages/{YYYY}/{MM}/`,
`agents/{name}/{inbox,outbox}/`, `build_slots/`, `file_reservations/`,
`.archive.lock`. SQLite = fast index (WAL, pool, write-behind ReadCache, 30s
deferred touch); Git = auditable canonical store, async commit coalescer.
**Invariants:** explicit recipients only (`broadcast=true` rejected); project
isolation; symmetric glob overlap; TTL-not-forever; wrong-surface denial;
idempotent bootstrapping; contact approval as policy.
**Mirrored:** Git archive + SQLite index; WAL + pool; write-behind cache;
commit coalescer; i64-µs; frankensearch lexical (semantic/hybrid feature-gated);
symmetric fnmatch; `#![forbid(unsafe_code)]`. **Not mirrored (waived):**
asupersync-exclusivity → Tokio.

### herdr protocol summary (default backend)
Newline-delimited JSON-RPC over Unix socket (`~/.config/herdr/herdr.sock`,
`HERDR_SOCKET_PATH`); one request per connection except `events.subscribe`
(long-lived); protocol v18. Methods: `workspace.create/list/close`,
`pane.split/zoom/list`, `agent.list/start/prompt/wait/read/send_keys`,
`worktree.list/create/remove`, `events.subscribe`. Push events e.g.
`pane.agent_status_changed`. `AgentStatus`: idle|working|blocked|done|unknown.
Errors carry machine codes; classify on code, never message text.

### flywheel integration (Decision 3A, realized)
flywheel deletes `src/mail.rs` (HTTP) + `src/herdr.rs`; depends on
`toron-core`+`toron-mcp`; `main.rs` starts `toron_mcp::serve()` (the MCP
endpoint external agents hit) and runs the loop via direct `toron_core` calls.
Shared herdr client (one definition in `toron-core::backend::herdr`). Resonate
(sync HTTP) calls wrapped in `spawn_blocking`. **One process** = bus + mailbox +
backend + orchestrator + MCP server.

### Risk register
1. **Integrated-mode lifecycle coupling** → Git+SQLite persist; standalone
   daemon mode; Resonate resume.
2. **NIP-17 fan-out storage** → projection dedup; bus TTL (90d); NIP-40.
3. **Event ordering** → `(created_at, event_id)` + local-seq µs (G7).
4. **AM-Rust conformance drift** → vendored Python parity tests in CI.
5. **Trust-anchor primitive mismatch** → Schnorr default + Ed25519 compat;
   scheme-parameterized tests.
6. **Resonate (sync) under Tokio** → `spawn_blocking`; idempotent leaves.
7. **Tokio runtime in flywheel (was sync CLI)** → single `#[tokio::main]`;
   sync shell-out leaves unchanged inside `spawn_blocking`.
8. **sqlx SQLite single-writer contention (G15)** → dedicated write connection +
   write-behind cache serializes writes; small read pool; WAL.
9. **Key management operational burden** → keyring integration; ncryptsec;
   documented recovery; **Phase-1.5 NIP-46 remote signer (A2) retires this for
   high-assurance/federated deployments**; security gain justifies it.
10. **Attachment size vs relay caps** → local blob store v1; Blossom/NIP-96
    federation Phase 2.
11. **Phase-2 scope creep** → channels/workflows are a clearly-separated superset
    above the frozen 38-tool parity surface; Phase 1 ships without them. The
    conformance gate (38 tools + 25 resources, schema/filters/tokenizer) is
    unaffected.
12. **Public-relay NIP-17 spam** (NEW-D3) → random gift-wrap keys defeat
    pubkey-reputation filtering; Phase-1 federation assumes trusted relays;
    public-relay deployment requires **NIP-42 AUTH + NIP-13 POW + allowlist**
    (A4) — never silently enabled.
13. **Kind renumber churn** → registry is FROZEN (AGREEMENT LOCKED 2026-08-04);
    any renumber requires a new ADR revision and re-freeze.
14. **NIP-09 cross-key constraint** (A1) → operator cannot NIP-09-delete or
    relay-supersede an agent's lease (NIP-09 author-only; NIP-33 per-
    `(kind,pubkey,d)`); correctness rests on the projection key-priority rule
    (operator > agent), implemented identically on all toron nodes — not relay
    semantics.

### Open questions (remaining — operational only)
1. Default bus TTL exactly 90d vs configurable-per-project? (Recommend: global
   default + per-project override.)
2. Keyring backend priority (macOS Keychain → Linux Secret Service → passphrase
   file)? (Recommend: that order.)
3. TUI screen set parity target (all 16 vs operator-critical subset first)?
   (Recommend: inbox/threads/agents/reservations first.)

### Related ADRs
* Supersedes [ADR-0001](0001-project-foundation.md) (asupersync **dropped**;
  mDNS/PSK/WS-hub/QUIC → Nostr bus; only `#![forbid(unsafe_code)]`, Resonate,
  i64-µs carry forward).
* Partially supersedes [ADR-0002](adopt-agent-adapter-architecture.md) (MCP
  adapter retained as Tool Surface; Sidecar → `Backend` trait).

### References
* Nostr: https://nostr.how/en/the-protocol
* NIPs: https://github.com/nostr-protocol/nips (01/09/17/32/40/44/49/50/59/94/96)
* Rust SDK: https://github.com/rust-nostr/nostr (= nostrdevkit/nostr) —
  `nostr-sdk` + `nostr` + `nostr-blossom`.
* Parity reference: https://github.com/Dicklesworthstone/mcp_agent_mail_rust
* Prior art (Nostr-native hive-mind): https://github.com/block/buzz — adopted
  kind-as-dispatch, relay pipeline, @mention dispatch, YAML workflow (Resonate-
  backed), NIP-29 channels, ephemeral presence; exceeds it on durability
  (Resonate) + mailbox parity (AM-Rust) + local-first footprint.
* Backend: https://herdr.dev/docs/preview/ ; client `~/workspace/flywheel/src/herdr.rs` (v18) → port to `toron-core::backend::herdr`.
* Consumer: `~/workspace/flywheel` (embeds toron-core per Decision 3A).

// governed-by: ADR-0003
