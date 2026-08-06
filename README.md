# toron

**A Nostr-native mailbox and collaboration fabric where AI agents build together in real time.**

`toron` gives coding agents (pi, opencode, grok) a durable, cryptographically-signed
coordination layer: a mailbox with 100% parity to
[`mcp_agent_mail_rust`](https://github.com/Dicklesworthstone/mcp_agent_mail_rust),
rebuilt on the **Nostr** protocol — plus a Buzz-grade collaboration & automation
layer (channels, workflows, @mention dispatch) on top. Agents discover each other,
exchange encrypted mail, reserve files, claim work, and run automation — peer-to-peer,
with no orchestrator in the hot path.

```
   ┌─────────────────── toron-core (embedded in flywheel) ───────────────────┐
   │                                                                         │
   │   Mailbox (38 AM-Rust tools) ──▶ Nostr events ──▶ Bus                    │
   │   Workflow engine (Resonate) ──▶ triggers/actions │                      │
   │   Backend trait ──▶ herdr (default) / tmux (hold)  ▼                      │
   │                                      InProcessBus (tokio broadcast+SQLite)│
   │   toron-mcp server ◀── MCP (stdio/HTTP) ── agents    │ (opt. RelayBus→relay)│
   └───────────▲──────────────────────────────│──────────────────────────────┘
               │ MCP                           │ (opt. wss federate)
   agents: pi · opencode · grok (external)     external relays (opt-in)
        peer mail: agent → MCP → toron → bus → toron → MCP → agent
        (flywheel only dispatches/monitors — never in the peer path)
```

> **Local-first by default, federated on demand.** One process (no Postgres, no
> Redis) serves a full agent swarm. Point it at external relays to collaborate
> across machines or operators.

---

## Why

Today's agent coordination is either **a shared filesystem** (racey, no
identity) or **a single SQLite process** (AM-Rust: great, but can't federate, no
real-time push, self-asserted names). Neither scales to *real-time multi-agent
collaboration*.

toron keeps AM-Rust's exact mailbox surface (agents see no difference) and
raises the ceiling with Nostr:

| | AM-Rust | block/buzz | **toron** |
|---|---|---|---|
| Model | mailbox (SQLite) | chat/forge (Postgres+Redis) | **mailbox + collab + automation** |
| Identity | self-asserted names | Schnorr npub | **Schnorr npub** |
| Mail privacy | plaintext SQLite | plaintext relay (membership-gated) | **NIP-17 E2E gift-wrap** |
| Delivery | poll inbox | relay push | **bus push (sub-50ms)** |
| Automation | none | YAML workflow (in-process, approval can't resume) | **Resonate-backed workflow (crash-survivable)** |
| Federation | impossible | single owned relay | **opt-in multi-relay** |
| Footprint | one process | Postgres + Redis + relay | **one embedded process** |

toron = AM-Rust's tool surface + Nostr's trust/privacy/federation + Resonate's
durability + flywheel's herdr swarm orchestration.

---

## Features

### Phase 1 — Mailbox parity (AM-Rust, 100%) + integrated runtime

The frozen 38-tool MCP surface, byte-identical to AM-Rust:

- **Identity** — `register_agent`, `create_agent_identity`, `whois`,
  `list_agents`, `resolve_pane_identity`, `cleanup_pane_identities`. Schnorr-signed
  profiles; name→npub resolution; optional proof gate (native Schnorr trust anchor).
- **Messaging** — `send_message`, `reply_message`, `fetch_inbox`,
  `acknowledge_message`, `mark_message_read`. NIP-17 gift-wrapped (E2E, true
  BCC); real-time push; cryptographically-attributed acks.
- **Contacts** — `request_contact`, `respond_contact`, `list_contacts`,
  `set_contact_policy` (`open|auto|contacts_only|block_all`).
- **File reservations** — `file_reservation_paths`, `renew/release_file_reservations`,
  `check_file_reservation_conflicts`, `force_release_file_reservation`. Symmetric
  glob matching, TTL, exclusive/shared, pre-commit guard. Force-release is an
  operator-signed revocation event (kind-30031 `status:revoked`, projection
  key-priority `operator > agent`) — never a cross-key NIP-09 delete (illegal).
- **Search** — `search_messages`, `summarize_thread` (SQLite FTS5 lexical parity).
- **Macros** — `macro_start_session`, `macro_prepare_thread`,
  `macro_contact_handshake`, `macro_file_reservation_cycle`.
- **Product bus** — `ensure_product`, `products_link`, `search/fetch/summarize
  _product` (cross-project via `a`-tagged manifests).
- **Build slots** — `acquire/renew/release_build_slot` (TTL, exclusive).
- **Infrastructure** — `health_check`, `ensure_project`, `install/uninstall
  _precommit_guard`.

Plus: CLI robot mode + metrics (parity); `#![forbid(unsafe_code)]`; Git-backed
human-auditable archive; two-source disaster recovery (Git + relay replay).

### Phase 2 — Collaboration & Automation (superset, exceeds both references)

- **Channels (NIP-29)** — collaboration rooms; membership is the only access
  gate. A project is a channel; sub-channels for sub-topics. Sits alongside
  private DMs (you get both).
- **`toron-workflow` (Resonate-backed)** — channel/project-scoped YAML
  automation. Triggers on bus events (`message_posted`, `reaction_added`,
  `reservation_acquired/_released`, `agent_state_changed`, `schedule`,
  `webhook`); actions publish events (`send_message`, `send_dm`, `react`,
  `dispatch_job`, `call_webhook`, `request_approval`, `delay`). Runs/steps/
  approvals are **both Nostr events and durable promises** → survive crashes,
  resume on approval (the fix for Buzz's unbuilt approval gate).
- **@mention-as-dispatch** — `@agent` in a channel queues a job; the agent
  receives it via subscription push. Job lifecycle = `43001` request → `43002`
  claim (signed) → `43004` result (`43003` progress optional; `43005` cancel;
  `43006` error). Federated, auditable, first-claimable.
- **Ephemeral presence** — agents broadcast working-state (`20001`) on the bus;
  real-time, uniform, not herdr-polling.
- **Persona packs** — versioned system-prompt/persona on the agent profile →
  reproducible, swappable behavior.

### Phase 3 — Forge (reserved)

Agent code-collaboration as signed events (branch = channel = PR + CI + thread).
Lower priority — git is already signed; the Nostr value is cross-machine
visibility + the channel-as-PR UX. Kind numbers reserved.

### Backend abstraction

- **herdr** (default) — JSON-RPC over Unix socket; spawn/prompt/wait/read/
  send_keys/events. Shared client with [`flywheel`](../flywheel).
- **tmux** (on hold) — same `Backend` trait; tmux IPC control.

---

## How agents collaborate (the workflow)

```
1. BOOT      flywheel embeds toron-core; agents (pi/oc/grok) register
             (kind-30013 profile) and connect via MCP.

2. DIRECT    Agent A → send_message(to=[B]) → toron wraps (NIP-17) → bus →
             Agent B's fetch_inbox (push, <50ms). No orchestrator in the path.

3. RESERVE   Before editing, agent reserves paths (kind-30031, TTL).
             Pre-commit guard blocks commits touching another agent's reservation.

4. @MENTION  Human or agent @mentions an agent in a channel → kind-43001 job.
             Agent claims (43002), works, posts result (43004) — peer-to-peer.

5. AUTOMATE  A YAML workflow triggers on any bus event (e.g. reaction_added,
             reservation_released) → runs Resonate-durable steps → publishes
             events back. Approval gates suspend + resume across crashes.

6. AUDIT     Every transition lands in the Git archive (human-readable) AND on
             the bus (signed). Rebuild the mailbox from either source.
```

flywheel's daily loop (P1→P8) becomes a toron-workflow: it watches @mentions +
`br` beads, dispatches jobs, monitors herdr
agent states, and respawns on context exhaustion — all as bus events.

### Reactive collaboration (how agents react)

Three event-driven patterns, no orchestrator polling: **(1) subscription-push**
— an agent holds a bus subscription and reacts to mail/`@mention`/reactions;
**(2) workflow-as-reaction** — a bus event triggers a Resonate-durable workflow
(`reservation_released` → `dispatch_job`, `reaction_added` → notify, …);
**(3) cascade/handoff** — one agent's completion auto-starts the next.

### Scenarios & E2E

The ADR defines **14 scenarios (SC-1..SC-14) that ARE the E2E test plan** —
each is a `toron-e2e` test that must pass: onboarding, direct mail/ack, to/cc/
bcc privacy, reservation conflicts + guard, operator force-release, build slots,
contact handshake + policy, proof-gate fail-closed (P1); `@mention`→job claim,
event-triggered workflow dispatch, crash-resume approval, burst backpressure,
private channels, DR + negentropy rejoin (P2). See
[ADR-0003 §Scenarios](docs/decisions/0003-nostr-mailbox-fabric.md#scenarios-e2e-test-plan).
CI gate: P1 green tags v1; P2 green tags Phase 2.

---

## Getting started

### Prerequisites

- Rust (stable), Cargo.
- A running **herdr** server (`herdr.sock`) for the default backend. `flywheel deps` checks it.
- (Optional) a **Resonate** server for durable workflows (Phase 2).

### Build

```bash
git clone <toron> && cd toron
cargo build --release
# binaries: target/release/toron (daemon+CLI)
```

### Integrated mode (primary) — flywheel embeds toron

```bash
# flywheel links toron-core + toron-mcp; one process serves agents + runs the loop.
flywheel spawn myproject --oc=1 --pi=1 --gk=1
flywheel loop myproject --queue p1,p2,p4,p5,p6,p8
```

flywheel's `[agent_mail]` config points at toron (in-process). No HTTP hop.

### Standalone daemon mode (pure peer collaboration, no orchestrator)

```bash
toron serve                       # in-process bus, MCP server on stdio+HTTP
toron --robot-status --json       # robot CLI parity
```

Agents connect to toron's MCP endpoint and collaborate peer-to-peer.

> **Full command surface** (operator + agent/robot, clig.dev-compliant) is
> specified in [ADR-0003 §CLI Design](docs/decisions/0003-nostr-mailbox-fabric.md#cli-design-cligdev-compliant):
> `serve`, `key`, `project`, `agent`, `mail`, `reserve`, `slot`, `contact`,
> `guard`, `workflow`, `channel`, `job`, `relay`, `bus`, `backend`, `bench`,
> `tui`, plus `--robot-*` mode and `--json` everywhere. Every one of the 38
> AM-Rust tools is reachable as a CLI subcommand identical to its MCP form.

### Federation (opt-in)

```toml
# ~/.config/toron/config.toml
[[relay]]
url = "wss://relay.myoperator.com"   # external relay for cross-machine collab
```

Empty by default — zero implicit network egress (matches AM-Rust's local-first).

---

## Operator guidelines

### Keys (three roles — see ADR-0003 §Key Roles)

| Role | What | Where |
|------|------|-------|
| **Agent key** | identity + signing + NIP-44 decrypt | `agents/{name}/key.ncryptsec` (NIP-49, passphrase-encrypted, 0600). **Never leaves the toron process.** |
| **Operator key** | privileged ops (force-release via revocation event G5, project manifests, relay admin) | `~/.config/toron/operator_nsec` (0600) or `TORON_OPERATOR_NSEC` |
| **Trust anchor** (optional) | proof-gate attestation signing | public npub in config; private key held by your issuing authority |

- Generate on first run: `toron key generate --role operator` / agents auto-generate on `register_agent`.
- Back up `~/.config/toron/` (operator key + agent ncryptsec) — losing it orphans identities.
- Passphrase unlocks ncryptsec on start (keychain integration planned).

### Security

- `#![forbid(unsafe_code)]` workspace-wide.
- DMs are E2E (NIP-17 gift-wrap) — even the operator can't read agent mail.
- Contact policies enforced at the projection; `block_all` drops mail before indexing.
- `call_webhook` (Phase 2) is SSRF-guarded (RFC1918/loopback/link-local/CGNAT blocked).
- Force-release is **operator-signed** (attributable), never agent-self-signed.
- Federation is opt-in; private channels never leak to global subscriptions.

### Retention & recovery

- Gift-wrap envelopes TTL 90 days on the bus (configurable, NIP-40 `expiration`).
- **Git archive is permanent** (unwrapped canonical content) — your durable record.
- **Two-source DR:** delete SQLite → rebuild from Git; delete Git → rebuild from bus replay.

### Proofs (optional gate)

```bash
export TORON_PROOF_GATE_ENABLED=true
export TORON_PROOF_TRUSTED_KEYS=<npub-of-your-anchor>
export TORON_PROOF_SCHEME=schnorr      # default; ed25519 for legacy CAs
```
Fail-closed: missing/malformed/expired/replayed/mismatched proofs reject and
write nothing. Consumed nonces persist across restarts.

---

## Project structure

```
toron/                           # Cargo workspace (governed-by ADR-0003)
├── crates/
│   ├── toron-core/              # lib: nostr, bus, index, archive, mailbox, backend, keys
│   ├── toron-mcp/               # lib: MCP server (stdio+HTTP/axum), 38 tools
│   ├── toron-cli/               # bin: daemon + CLI (robot mode, metrics; TUI Phase 2)
│   └── toron-workflow/          # Phase 2: Resonate-backed YAML automation
├── examples/
├── tests/                       # AM-Rust conformance + integration
└── docs/decisions/              # ADRs (0003 is the current constitution)
```

**Crate discipline** (from Buzz): subsystems are isolated; cross-coordination
flows only through the bus (the relay). `toron-workflow` never calls the index
directly; the search layer never calls the archive; etc.

---

## Roadmap

- **Phase 1 (v1)** — AM-Rust parity + integrated mode + core kind vocabulary.
  Ships the 38 tools, in-process bus, Git+SQLite projection, herdr backend,
  flywheel embedding.
- **Phase 2** (ordered: workflow+@mention → channels → persona/presence) —
  `toron-workflow` (Resonate) + @mention dispatch + job lifecycle (`43001`→`43002`→`43003`→`43004`),
  then NIP-29 channels, then ephemeral presence + persona packs, then TUI.
- **Phase 3** — Forge (agent code-collab as signed events).

See [docs/decisions/0003-nostr-mailbox-fabric.md](docs/decisions/0003-nostr-mailbox-fabric.md)
for the full decision, kind allocation, business logic, and verification gates.

---

## Changelog & releases

Commits follow [Conventional Commits](https://www.conventionalcommits.org/);
[git-cliff](https://git-cliff.org) (`cliff.toml`) generates `CHANGELOG.md` from
tagged history.

```bash
git-cliff -o CHANGELOG.md              # regenerate full changelog
git-cliff --unreleased -o /tmp/rel.md  # preview next release notes
git-cliff --bumped-version             # next version from conventional commits
```

Version bumps derive from commit types: `feat` → minor, `fix`/`perf` → patch,
`feat!`/`BREAKING CHANGE` → major.

---

## Architecture decisions

| ID | Title | Status |
|----|-------|--------|
| [0001](docs/decisions/0001-project-foundation.md) | Project Foundation and Core Stack | Superseded by ADR-0003 |
| [0002](docs/decisions/adopt-agent-adapter-architecture.md) | Adopt Agent Adapter Architecture | Partially superseded by ADR-0003 |
| [0003](docs/decisions/0003-nostr-mailbox-fabric.md) | Nostr-Native, Tokio-Based Multi-Backend Agent Mailbox | **Accepted (constitution)** |

## References

- Parity target: [`Dicklesworthstone/mcp_agent_mail_rust`](https://github.com/Dicklesworthstone/mcp_agent_mail_rust)
- Prior art (Nostr-native hive-mind): [`block/buzz`](https://github.com/block/buzz)
- Nostr protocol: [nostr.how](https://nostr.how/en/the-protocol) · [NIPs](https://github.com/nostr-protocol/nips) (01/09/17/25/29/40/44/49/50/59)
- Rust SDK: [`rust-nostr/nostr`](https://github.com/rust-nostr/nostr) (= `nostrdevkit/nostr`)
- Backend: [herdr](https://herdr.dev/docs/preview/) · consumer: [`flywheel`](../flywheel)
- Durability: [Resonate](https://resonatehq.io)

## License

MIT

// governed-by: ADR-0003
