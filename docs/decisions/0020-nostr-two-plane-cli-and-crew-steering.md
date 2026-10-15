<!-- governed-by: ADR-0003, ADR-0004, ADR-0007, ADR-0014, ADR-0015, ADR-0018, ADR-0019 -->
---
status: accepted
date: 2026-08-15
decision-makers: Captain (bangedorrunt)
consulted: block/buzz desktop + NOSTR.md + VISION_AGENT.md (2026-08-15 fetch); 2026-08-07 comparison note; grilling on worktrees (ADR-0019)
informed: flywheel maintainers; skill owners (toron, flywheel, swarm)
---

# ADR-0020: Expose the Nostr Two-Plane Surface (CLI-first) so Crews Steer Each Other

**Status:** Accepted (2026-08-15). Wave 1 verified: SC-27..33 green; CLI
`channel`/`job`/`react`/`presence`/`watch` shipped; MCP tool count stays 41 (38 AM-Rust parity + 3 toron additive — corrected 2026-09-04, GS-016; the 39 figure predates the additive pair, and `ask_judgment` has since joined the two — 2026-09-30).
**Amended by:** [ADR-0023](0023-nip29-group-metadata-kinds.md) (2026-08-24) —
create/close emit **39000**, join/leave emit **39002**; 40/41 ingest-only.
**Date:** 2026-08-15
**Decision-makers:** Captain (bangedorrunt)
**Extends:** [ADR-0003](0003-nostr-mailbox-fabric.md) (Nostr fabric, SC-9/SC-13), [ADR-0004](0004-operational-hardening.md) (Buzz learnings), [ADR-0014](0014-kind-registry-specification.md)
**Amends:** ADR-0003 Phase-2 channel wire (implementation drifted to kinds 40/41/42; this ADR restores **kind 9** as the emit), ADR-0014 registry table (add standard kinds 7 and 9 as first-class, mark 42 as legacy ingest)
**Does not amend:** frozen MCP tool **names** (38 AM-Rust + 1 additive `release_file_reservations_by_reason`) or the 25 resource templates. New capability lands as **CLI + skills + robot-capabilities**, not a 40th MCP tool, unless wave 2 proves an MCP-only agent cannot complete SC-27.
**Companion:** [ADR-0015](0015-flywheel-toron-integration-contract.md) (flywheel still owns spawn/loop; toron owns the event log), [ADR-0018](0018-agent-identity-naming-grammar.md) (canonical names sign jobs), [ADR-0019](0019-bead-verified-execution-and-principle-playbooks.md) (beads, no worktrees, no Graphite)
**Tracker:** beads. Never pull requests.

> **Scope:** make the Nostr plane that already exists in the engine **usable by crews**. Humans and agents share rooms (NIP-29 kind 9), reactions (kind 7), jobs (43001–43006), and presence (20001). Gift-wrap DMs stay the private plane. The captain is optional. CLI is the ground truth. Skills teach the recipe. Tests named below must go green before this ADR is Accepted.

---

## 1. TL;DR

We matched Agent Mail Rust's 38 mailbox verbs. That hid Nostr. Buzz desktop is not "more MCP tools." It is a NIP-29 client on the same signed log the agents use. Rooms, reactions, membership, live REQ, jobs, presence. One identity model.

Toron already has most of that **in crates**. Crews cannot drive it:

- `channels.rs` implements kinds **40/41/42** while ADR-0003, ADR-0014, the skill, and SC-13 prose all say **kind 9**.
- SC-9 (`sc9_mention_job.rs`) is a **gift-wrap DM** plus hand-built `Keys::generate()` jobs. It never shells `toron job claim`. It never posts to a channel.
- There is no `toron channel`, `toron job`, `toron react`, `toron presence`, or `toron watch` command.
- Level 2 in the skill ("agents steer each other") is a paragraph. No recipe. No CLI. No test that a crew can finish without flywheel `send`.

**Decision:** two planes, CLI-first, kind-9 emit, crew-steer protocol with must-pass scenarios. No new MCP tools in wave 1. No Buzz desktop. No NIP-34 Forge (beads, not PRs). No worktrees (ADR-0019). Federation stays off until a later ADR turns relays on.

```
Plane P (private)     NIP-17 gift-wrap 1059 / rumor 14     toron mail *
Plane S (shared)      NIP-29 kind 9 + kind 7 + 43001–06    toron channel * / job * / react
Signal                kind 20001 presence                  toron presence *
Live                  NIP-01 REQ                           toron watch
```

---

## 2. Context and Problem Statement

### 2.1 What we copied, and what that hid

AM-Rust is a local SQLite mailbox. Its verbs (`send_message`, `fetch_inbox`, `file_reservation_paths`, …) are the frozen MCP cluster. Toron implemented them on a Nostr bus. That was Phase 1. It is done.

Buzz (`block/buzz`) is a self-hosted Nostr relay plus a Tauri desktop client. Every chat line, reaction, workflow step, and git event is a signed event. Desktop and `buzz-cli` / ACP are peers on that log. Agents are members of rooms, not bots behind a private inbox.

Toron's engine already took Buzz's collaboration layer (kinds, @mention → 43001, first-claimer, coalescer, NIP-29 membership gate, presence module, `react` workflow action). The **wire agents speak** is still AM. The Nostr log is an implementation detail.

### 2.2 Measured drift (not vibes)

| Claim in ADR-0003 / skill | Code today |
|---|---|
| Channel posts are kind **9** | `CHANNEL_MESSAGE = 42` in `toron-workflow/src/channels.rs`. SC-13 asserts 42. |
| SC-9: H posts kind-9 `@OCβ …`; OC claims via `job claim` | Test uses `send_message` (gift-wrap) and `build_job_claim` with a **fresh random keypair**, not the registered agent's key. |
| Level 2: crews steer without the captain | No CLI. No skill recipe with copy-pasteable commands. Flywheel `send` is the only practical path. |
| Presence replaces herdr polling | `presence.rs` builds 20001. Nothing publishes it from a pane. Loop still polls herdr. |
| Live push | `Bus::subscribe` exists. Agents poll `fetch_inbox`. |
| Federation | `relays = []`. Correct default. Unused advantage. |

### 2.3 Why more MCP tools is the wrong first cut

- Tool names are frozen (ADR-0007 C3). A 40th tool is an ADR-sized event even when the method already exists.
- Coding crews on herdr have a shell. `br` is already CLI-only. The same pattern fits the Nostr plane.
- MCP-only sessions (this grok chat) already have the 39 mailbox tools. They do not need a second clone of `fetch_inbox` to start steering. They need a **documented CLI** the next pane can run.
- Buzz's agent MCP (`buzz-dev-mcp`) is shell + file edit. Their Nostr surface is the **relay**, not 40 more tools.

### 2.4 The problem statement

> How do we make the Nostr shared plane (rooms, reactions, jobs, presence, live watch) a **first-class, testable, skill-taught surface** so two registered agents can steer each other without the captain, without growing the frozen MCP cluster, without cloning Buzz desktop, and without breaking gift-wrap DMs or the kind freeze?

---

## 3. Decision Drivers

1. **Kind freeze + standard NIPs.** We may **adopt** kinds 7 and 9 (already claimed in ADR-0003). We may not invent 40000+ Buzz-only kinds. We may not silently keep emitting 42 while docs say 9.
2. **CLI-first.** New verbs land in `crates/toron-cli` `Command` enum, `--json` robot shape, `robot-capabilities`, `toron catalog`. Same skill-sync contract as today, minus a new `#[tool]`.
3. **Crews steer crews.** A complete loop (post → mention → 43001 → claim → result → visible) must run as shell commands two agents can type. Flywheel `send` is optional.
4. **Two planes.** Private mail stays NIP-17. Shared rooms are not gift-wrapped. Mixing them is a bug.
5. **Identity is ADR-0018.** Job claims and channel posts are signed by the agent's registered key, never `Keys::generate()` in production paths. Tests that use random keys are labeled fixtures, not the CLI path.
6. **Beads, not PRs.** No NIP-34 Forge in this ADR. Bead id = `thread_id` and/or channel `h` tag `bead:<id>`.
7. **No worktrees** (ADR-0019). Channel fan-out is not a reason to clone the repo.
8. **Federation stays off** until a dedicated ADR + two-host test. This ADR must not require a public relay.
9. **TDD.** Every scenario in §7 is a failing test first, then the CLI, then green.
10. **Laziness.** Do not build a desktop. Do not wrap `br`. Do not add typing (20002), huddles, canvases, or media comments.

---

## 4. Considered Options

### A — Add an MCP "Nostr cluster" now (8+ tools)

Rejected as wave 1. Frozen names. Token cost on every MCP session. Crews have a shell.

### B — CLI-first two-plane + kind-9 correction + crew-steer scenarios (CHOSEN)

New `toron channel|job|react|presence|watch`. Skills teach the recipe. MCP unchanged. Wave 2 may add thin MCP wrappers if SC-27 cannot be completed from an MCP-only session that is **not allowed** to shell out.

### C — Clone Buzz desktop / full NIP-29 admin (9000–9008, 39000–39002)

Rejected. Operator console is flywheel + existing TUI stub. Membership stays `ChannelStore` (already membership-only). Optional 39000 metadata emit is wave 2 polish, not the gate.

### D — Do nothing (engine stays hidden)

Rejected. Level 2 stays fiction. Kind 9 vs 42 stays a lie.

### E — NIP-34 Forge as the Nostr bet

Rejected here. PR-shaped. Conflicts with ADR-0019. File under a future ADR if we ever want signed git events.

---

## 5. Decision Outcome

Chosen option: **B**.

### Spec 0 — Two planes (binding)

| Plane | Wire | Who sees it | CLI | Use |
|---|---|---|---|---|
| **P — private** | NIP-17 wrap 1059, rumor 14, NIP-10 threads | named to/cc/bcc only | `toron mail *` | secrets, operator, 1:1, BCC |
| **S — shared** | kind **9** (`h`=channel id, `t`=project slug, `p`=mentions), kind **7** reactions | channel members only (private channels); members + open readers (open channels) | `toron channel *`, `toron react` | coalition board, bead room, debate, votes |
| **J — jobs** | 43001–43006, `e`=parent (kind 9 or 14), `assignee` tag (never `#t`) | mentioned assignee for the request; result is visible to channel members if parent is kind 9 | `toron job *` | peer dispatch, first-claimer |
| **N — now** | 20001 ephemeral | anyone on the local bus | `toron presence *` | working/blocked/idle |
| **W — watch** | NIP-01 REQ | subscriber | `toron watch` | push, no poll |

`#t` remains project slug only (ADR-0014). Job assignee is the `assignee` tag (already in `jobs.rs`). Channel id is `h`. Bead binding is optional tag `bead` = bead id (not `#t`).

Gift-wrap is **forbidden** on plane S. A kind-9 post is plaintext to members. If the body must stay secret, use plane P.

### Spec 1 — Kind correction (restore ADR-0003)

`toron-workflow/src/channels.rs` today:

```
CHANNEL_CREATE   = 40
CHANNEL_METADATA = 41
CHANNEL_MESSAGE  = 42
REACTION         = 7   // already correct
```

**Emit (canonical):**

| Action | Kind | Tags |
|---|---|---|
| channel post | **9** | `h`=channel id, `t`=project slug, `p`=each mention npub |
| reaction | **7** | `e`=target, `h`=channel id |
| channel create (store) | local `ChannelStore` + optional kind **39000** (wave 2) | `d`=`h`=channel id, `name` |

**Ingest (one minor of compatibility):**

- Accept kind **9** (canonical) and kind **42** (legacy). Project both as channel posts.
- New tests assert emit == 9.
- SC-13 rewrite: `assert_eq!(post.kind, 9)`.
- After one released version, 42 ingest may be deleted in a follow-up ADR.

Do **not** implement the full NIP-29 admin set (9000–9008, 9021, 44100) in this ADR. `ChannelStore::join/leave/create` remains the membership gate. That is enough for SC-13/SC-27. A later ADR may emit 39000/39002 so `nak req -k 39000` lists rooms.

`kinds.rs` gains:

```
pub const CHANNEL_MESSAGE: u16 = 9;
pub const REACTION: u16 = 7;
pub const CHANNEL_MESSAGE_LEGACY: u16 = 42; // ingest only
```

ADR-0014 registry table gains rows for 7 and 9 (standard NIPs, status: adopted). 42 marked **Legacy ingest**.

### Spec 2 — CLI surface (ground truth)

Add to `crates/toron-cli/src/cli.rs` `Command`. Noun-verb. `--json` on every read. `--as <canonical-name>` required for writes (ADR-0018). `--project <slug>` required. Exit 0 ok, 1 runtime, 2 usage (existing).

```
toron channel create   <id> --name <str> [--private] --project <slug> --as <name>
toron channel join     <id> --project <slug> --as <name>
toron channel leave    <id> --project <slug> --as <name>
toron channel list     --project <slug> [--as <name>]
toron channel members  <id> --project <slug>
toron channel post     <id> --body <md> --project <slug> --as <name> [--mention <name>]...
toron channel read     <id> --project <slug> --as <name> [--limit N] [--since <rfc3339>]

toron react            --to <event-id> --emoji <s> --project <slug> --as <name> [--channel <id>]

toron job list         --project <slug> [--as <name>] [--assignee <name>] [--state open|claimed|done|error]
toron job show         <job-id> --project <slug>
toron job claim        <job-id> --project <slug> --as <name>
toron job progress     <job-id> --body <s> --project <slug> --as <name>
toron job result       <job-id> --status ok|failed --body <s> --project <slug> --as <name>
toron job error        <job-id> --message <s> --project <slug> --as <name>
toron job cancel       <job-id> --project <slug> --as <name>   # requester or operator only

toron presence publish --state idle|working|blocked|done --project <slug> --as <name> [--status <s>]
toron presence list    --project <slug>

toron watch            --project <slug> --as <name>
                       [--kinds 9,7,43001,20001]
                       [--channel <id>]
                       [--timeout-ms N]
                       [--once]          # exit 0 on first matching event
```

**JSON shapes (binding for robot mode):**

`channel post` → `{ "event_id": "<hex>", "kind": 9, "channel": "<id>", "mentions": ["<npub>", ...] }`

`job claim` → `{ "job_id": "<hex>", "kind": 43002, "claimer": "<canonical-name>", "npub": "<hex>", "won": true|false }`
`won: false` is exit **1** (second claimer). Do not pretend success.

`watch --once` → one event object `{ "kind", "id", "pubkey", "content", "tags" }` then exit 0. Timeout → exit 1, stderr `watch: timeout`.

`presence list` → `[{ "name", "npub", "state", "status", "ts" }]`

Writes resolve `--as` through the project's identity index (ADR-0018). Unknown name → usage error, not a new keypair.

**flywheel:** no new `flywheel channel` in wave 1. Crews call `toron`. Flywheel skill points at `toron`. A later flywheel alias is optional sugar.

### Spec 3 — MCP and resources stay frozen (wave 1)

- No new `#[tool(name = ...)]`.
- No new `toron://` resource templates (25 remain).
- `toron catalog --json` **does** grow the **CLI** section. `catalog_freshness` updates.
- `--robot-capabilities` lists `channel`, `job`, `react`, `presence`, `watch` with the subcommands above.

**Wave 2 gate (only if measured):** an MCP-only agent with **no shell** cannot complete SC-27. Then add thin MCP tools that call the same functions as the CLI, under a follow-up ADR. Do not pre-build them.

### Spec 4 — How a channel @mention becomes a job

Already specified. This ADR makes it reachable.

1. Member posts kind 9. Body contains `@Name`. Tags include `p`=that agent's npub (required; no `p` → no dispatch).
2. Mention coalescer (already shipped) batches bursts per assignee → one stamped 43001.
3. 43001 `e`-tags the kind-9 event. `assignee` tag = canonical name. `#t` = project slug only.
4. Assignee `toron job claim` publishes 43002 signed by **their** registered key. First observed 43002 wins (`first_claimer`). *Amended (bd-sc28-two-winners-flake-9b60):* "first observed" is the store's arrival order (the `bus_arrivals` seq, assigned inside the insert transaction), not the claimers' `created_at`, which is a whole-second clock followed by a random id. `job claim` decides from its own publish with no wait, and `job list`/`show` name the same winner (`first_claimer_by_arrival`, `job_state_by_arrival`). The `(created_at, id)` rule (`first_claimer`) remains only for a backend with no arrival log, with the bounded quiescence wait.
5. Winner `toron job progress` (optional 43003), then `result` (43004) or `error` (43006).
6. Channel members see the result because `job show` reads the bus and, when the parent is kind 9, `channel read` shows a system line or the result event is `e`-tagged into the channel. **Wave 1 minimum:** `toron job show` is enough. Posting a kind-9 "done" summary is the claimer's job (skill recipe), not magic.

DM @mention path (current SC-9) **stays**. Two parents are legal: kind 14 rumor or kind 9 post.

### Spec 5 — Crew-steer protocol (no captain)

This is the Level 2 contract. Skills quote it verbatim.

```
0. Both agents:  toron mail agents <project>   # already registered (ADR-0018 names)
1. A:            toron channel create steer-<bead> --name "bead <id>" --private \
                    --project <slug> --as <A>
2. A:            (tells B the channel id on a plane-P DM or in the bead thread)
3. B:            toron channel join steer-<bead> --project <slug> --as <B>
4. A:            toron channel post steer-<bead> --body "@B take the repro" \
                    --mention B --project <slug> --as <A>
5. B:            toron watch --project <slug> --as B --channel steer-<bead> \
                    --kinds 9,43001 --once --timeout-ms 60000
6. B:            toron job list --project <slug> --as B --state open
7. B:            toron job claim <job-id> --project <slug> --as B
8. B:            …work (reservations, no worktree)…
9. B:            toron job result <job-id> --status ok --body "<receipt>" \
                    --project <slug> --as B
10. B:           toron channel post steer-<bead> --body "done. receipt=…" \
                    --project <slug> --as B
11. A:           toron job show <job-id> --project <slug>
                 # status done, body matches
```

A may instead **react** `:+1:` on B's done post. A workflow with `trigger: reaction_added` may fire. That is SC-30.

If B is already working, B publishes `toron presence publish --state working`. A `toron presence list` before assigning. Do not @mention a `blocked` peer for a new job unless the skill says to.

**Captain is optional.** Flywheel `send` may still inject the first prompt that *teaches* this recipe. After that, the loop can stay idle while crews talk on the channel.

### Spec 6 — `watch`

- Process-local subscribe on the in-process bus (default) or the configured `[mail.toron]` daemon.
- Filter: kinds allowlist (default `9,7,43001,20001`), optional `#h` channel, `#t` project, `#p` = watcher's npub for p-gated kinds.
- `--once`: first match, print JSON, exit 0.
- `--timeout-ms`: required when `--once` (no infinite hang in CI).
- Without `--once`: newline-delimited JSON until SIGINT or timeout. stderr shows `watching kinds=…`.
- Does **not** open a herdr pane. Does **not** create a worktree.
- Kind 1059 (gift-wrap) is **not** in the default allowlist. Watching another agent's DMs is forbidden. A watcher may add `1059` only when `#p` is **their own** npub (same rule Buzz uses).

### Spec 7 — Presence

- `publish` emits 20001. Not stored (already the module's contract). Broker expiry unchanged.
- Payload `PresenceState` already matches herdr vocabulary. Keep it.
- Optional `project` field in the JSON payload (the current builder has no `#t` on purpose so the broker sees it). CLI `--project` writes that payload field.
- Loop integration is **out of wave 1**. The loop may keep polling herdr. A later bead may publish presence on state change. Do not block this ADR on loop edits.

### Spec 8 — Signing keys

Production CLI loads the agent's NIP-49 key from the identity record (`key_path`). It never generates a throwaway key for a claim.

SC-9's `Keys::generate()` path stays as a **unit-level** fixture for `first_claimer` math. A new test (`sc27`) uses registered agents and the CLI.

### Spec 9 — Federation

`relays = []` remains the default. `toron watch` / `channel post` work on the local bus. No public relay. No change to ADR-0009 Resonate selection.

A future "turn federation on" ADR owns two-host SC-14b-style tests. This ADR's tests must pass with zero egress.

### Spec 10 — Skills (this ADR's own revision)

Skills are the agent-facing contract. A command not in the skill is invisible.

**Must update when this ADR is accepted and when the CLI lands:**

| File | Change |
|---|---|
| `~/.grok/skills/toron/SKILL.md` and `~/.agents/skills/toron/SKILL.md` | Two-plane section. CLI table rows. Level 2 becomes Spec 5 recipe. Strike "DMs ARE the only model." |
| `~/.grok/skills/toron/references/crew-steering.md` | Spec 5 + scenarios, copy-paste commands |
| `~/.grok/skills/flywheel/SKILL.md` | Crew-to-crew: prefer `toron channel/job` over another `flywheel send` when two live panes already exist |
| `~/.grok/skills/swarm/SKILL.md` | After spawn, agents join `steer-<epic>` and claim jobs; captain is not in the hot path |
| `~/workspace/toron/AGENTS.md` | Skill-sync: new `Command` variants listed |
| `~/workspace/flywheel/AGENTS.md` | Pointer only (no new flywheel command in wave 1) |

Wave 1 of **this ADR's implementation** ships the CLI. The skill files are revised **now** (proposed surface labeled) so captains and crews see the destination. Implementations must match the labeled commands or the skill is updated again.

### Spec 11 — Flywheel

- Does not gain channel/job subcommands in wave 1.
- `decide` does not gain new rows (no "steer via nostr" primitive). Existing `steer` / `loop-work` / `coalition-form` stay.
- Coalition intent board **may** bind to `toron channel create coalition-<id>` in a later bead. Not a gate for this ADR.
- Loop mail-wake stays message-id dedup on plane P. Channel traffic is not "urgent mail."

### Spec 12 — Waves

| Wave | Ships | Gate |
|---|---|---|
| **1** (this ADR) | Kind-9 emit + 42 ingest. CLI `channel`, `job`, `react`, `presence`, `watch`. Robot-capabilities + catalog CLI section. Skills. SC-13 rewrite. SC-27..SC-33. | All §7 must-pass tests green. No new MCP tool. |
| **2** (optional) | 39000 metadata emit. MCP thin wrappers **only if** MCP-no-shell cannot pass SC-27. Loop publishes presence on herdr state change. | Separate beads. |
| **3** (not this ADR) | Federation on. NIP-77 live sync. Two-host tests. | New ADR. |
| **never here** | Desktop, huddles, canvases, NIP-34 Forge, `br` MCP, worktrees, 20002 typing. | — |

---

## 6. Practical scenarios (tests that must pass)

Each row is a test. RED first. Name, crate, command, assert.

### 6.1 Unit / lib (fast, no herdr)

| ID | File (proposed) | Assert |
|---|---|---|
| U-9-EMIT | `toron-workflow` channels | `post()` event `kind == 9`. Tags include `h`. Mentions add `p`. |
| U-42-INGEST | same | A constructed kind-42 event still appears in `history()`. |
| U-7-REACT | same | `react()` kind == 7, `e` = target. Non-member `react` errs. |
| U-CLAIM | `toron-workflow` jobs | Two 43002, `first_claimer` = first observed pubkey. |
| U-PRESENCE | `toron-core` presence | Round-trip `PresencePayload` `{state: working}`. |
| U-AS-RESOLVE | CLI or mailbox helper | Unknown `--as` is an error. No new key created. |

### 6.2 E2E / CLI (must use the binary)

Gate: `cargo test -p toron-e2e --features p2` plus new un-gated CLI tests that spawn `toron` via `current_exe` / `CARGO_BIN_EXE_toron`.

**SC-13 (rewrite, existing file).** Same actors. Posts emit **kind 9**. Non-member denied. Global sub excluded. Kind 7 works.

**SC-27 — Crew steer, no captain, CLI only.**
- Actors: A = `flywheel-toron-coder1`, B = `flywheel-toron-coder2` (or test names that pass ADR-0018 `open` mode).
- Setup: temp mailbox root, two registered agents, operator key.
- Steps: Spec 5 commands 1–11, invoked as the `toron` binary with `--json --no-input`.
- Assert:
  - create/join succeed.
  - post JSON `kind == 9` and `mentions` non-empty.
  - `job list --state open` contains one row for B after the mention (coalescer or direct dispatch; one 43001, not N).
  - `job claim` as B → `won: true`. Second `job claim` as A → exit 1, `won: false`.
  - `job result` as B → `job show` `state == done`.
  - No `flywheel` binary invoked.
  - No `worktree` in logs.
  - Gift-wrap count for this channel post is **0** (plane S).

**SC-28 — First-claimer via CLI.**
- Two `toron job claim` nearly together (threads). Exactly one `won: true`. The other exit 1.

**SC-29 — Non-member CLI post fails.**
- C not joined. `toron channel post` exit 1. `channel read` as C on a **private** channel exit 1. History length unchanged.

**SC-30 — Reaction triggers a workflow once.**
- YAML: `trigger: reaction_added`, action `send_message` to operator (or a test sink).
- B `toron react --to <post> --emoji :+1:`.
- Assert: workflow run emits exactly one stamped action. A second identical react does not double-fire (idempotent trigger filter).

**SC-31 — Presence list.**
- A `presence publish --state working`. B `presence list` includes A `working` within 1s.
- After broker expiry (use a short test TTL or mock clock), A disappears or is `unknown`.

**SC-32 — Watch --once.**
- Process 1: `toron watch --kinds 9 --channel <id> --once --timeout-ms 5000`.
- Process 2 (after watch is listening): `toron channel post`.
- Process 1 exits 0. stdout JSON `kind == 9`.
- Timeout with no post: exit 1.

**SC-33 — Plane P regression.**
- `toron mail send` A→B still gift-wraps (existing unwrap test).
- `fetch_inbox` as C does not see it.
- Channel post from SC-27 is visible to members and is **not** a 1059.

**SC-9 (keep).** Existing DM mention path stays green. Do not "fix" it into a channel test. Document in the file header: "plane P mention. Plane S is SC-27."

### 6.3 Catalog / skill / clippy

| ID | Assert |
|---|---|
| C-CAT | `toron catalog --json` lists `channel`, `job`, `react`, `presence`, `watch` under CLI. `cargo test catalog_freshness` green after regen. |
| C-ROBOT | `--robot-capabilities` JSON includes those nouns. |
| C-SYNC | `Command` enum variants exist. AGENTS.md STRUCTURE mentions them. Skill CLI table matches `--help`. |
| C-NO-TOOL | `rg '#\[tool\(name'` count unchanged at acceptance (39). Live 2026-09-04: **40** (38 parity + 2 additive, GS-016). Live 2026-09-30: **41** (38 parity + 3 additive — `catalog::ADDITIVE_TOOLS` is ground truth). |
| C-NO-KIND-42-EMIT | `rg 'CHANNEL_MESSAGE = 42'` is gone or only `LEGACY`. |
| C-DEP | `! grep -r 'use flywheel' crates/` still holds. |

### 6.4 Scenario that must **not** pass if someone "helps"

- Creating a git/herdr worktree to "isolate" a channel poster → fail the test if the helper does that (SC-27 assert).
- Adding `send_message` as the implementation of `channel post` (would gift-wrap) → SC-27/33 fail.
- `job claim` with a generated key that is not the registered agent → SC-27 fail (claimer npub must match `whois B`).

---

## 7. Implementation Plan

TDD order. One bead per scenario cluster if desired. Do not implement watch before channel post (watch has nothing to see).

### 7.1 Affected paths

| Path | Change |
|---|---|
| `crates/toron-workflow/src/channels.rs` | `CHANNEL_MESSAGE = 9`. Ingest 9+42. `h`+`t`+`p` tags on emit. |
| `crates/toron-core/src/nostr/kinds.rs` | consts for 7, 9, 42-legacy. Tests. |
| `docs/decisions/0014-*.md` | registry rows (when this ADR is accepted, edit 0014 in the same wave). |
| `crates/toron-cli/src/cli.rs` | `Channel`, `Job`, `React`, `Presence`, `Watch` commands. |
| `crates/toron-cli/src/robot.rs` (or new `plane.rs`) | handlers, `--json`. |
| `crates/toron-cli/src/main.rs` | dispatch. |
| `crates/toron-mcp/src/catalog.rs` | CLI section only. |
| `crates/toron-e2e/tests/sc13_channel_membership.rs` | kind 9. |
| `crates/toron-e2e/tests/sc27_crew_steer_cli.rs` | **new.** binary-driven. |
| `crates/toron-e2e/tests/sc28_first_claimer_cli.rs` | **new.** |
| `crates/toron-e2e/tests/sc29_nonmember_cli.rs` | **new** or fold into SC-13 CLI. |
| `crates/toron-e2e/tests/sc30_reaction_workflow.rs` | **new.** |
| `crates/toron-e2e/tests/sc31_presence_cli.rs` | **new.** |
| `crates/toron-e2e/tests/sc32_watch_once.rs` | **new.** |
| `crates/toron-e2e/tests/sc33_plane_p_regression.rs` | **new** or extend existing gift-wrap test. |
| `crates/toron-cli/tests/cli_robot.rs` | JSON snapshots for new verbs. |
| skills + AGENTS.md | Spec 10. |

### 7.2 Patterns to follow

- `MailCommand` clap shape + `robot.rs` `emit(--json)`.
- `jobs.rs` builders. Do not reimplement first-claimer.
- `ChannelStore` membership gate. Do not add a second ACL.
- Mention coalescer already stamps 43001. CLI `channel post --mention` must go through the same dispatcher the workflow host uses, or SC-27 will see zero jobs.

**Wiring gap to close:** today `ChannelStore::post` does **not** call the mention coalescer. Wave 1 must connect `channel post` → bus.publish(kind 9) → same trigger path that DMs use (`spawn_mail_trigger_loop` / mention parse). If that path only listens for gift-wrap, **extend it to kind 9**. That is the load-bearing implementation task. A unit test: posting kind 9 with `@B` + `p` produces one 43001.

### 7.3 Patterns to avoid

- `Keys::generate()` in CLI write paths.
- Gift-wrapping kind 9.
- New MCP tools "for completeness."
- `flywheel channel` duplicate.
- Full NIP-29 admin kinds.
- Default `watch` on 1059 without `#p` self.

### 7.4 Migration

- Old in-memory 42 events: ingest continues.
- No SQLite migration (channel store is in-memory / bus). If any test fixture JSON has `"kind": 42` as expected emit, update to 9.

---

## 8. Verification (acceptance)

Wave 1 is not done until every box is checked.

- [ ] `CHANNEL_MESSAGE` emit is 9. SC-13 asserts 9.
- [ ] Kind 42 still *reads*. A test posts a raw 42 and `history()` includes it.
- [ ] `toron channel --help` and `toron job --help` exist. `--json` on reads.
- [ ] SC-27 green: two CLIs, no flywheel, no worktree, one 43001, first-claimer, result visible, post not gift-wrapped.
- [ ] SC-28, SC-29, SC-30, SC-31, SC-32, SC-33 green.
- [ ] SC-9 still green (plane P).
- [ ] `--as` unknown name fails. Claim npub == `whois` npub.
- [x] `#[tool(name` count == 39 at acceptance; **40 live** at the time (38 parity + 2 additive since, GS-016); **41** as of 2026-09-30 (+ `ask_judgment`).
- [ ] `catalog_freshness` green after `toron catalog --json` regen.
- [ ] `--robot-capabilities` lists the five nouns.
- [ ] Skills: two-plane + Spec 5 recipe present. "DMs are the only model" gone.
- [ ] AGENTS.md skill-sync lists `channel`, `job`, `react`, `presence`, `watch`.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` on touched crates.
- [ ] No `use flywheel` in toron crates.

---

## 9. Consequences

### Good

- Crews can assign work to each other in a room a human can follow with `toron channel read` (and later any NIP-29 client once 39000 exists).
- Docs stop lying about kind 9.
- Frozen MCP cluster stays small. Shell agents get the Nostr plane the same way they get `br`.
- Plane P privacy is explicit. Plane S is honest plaintext-to-members.

### Bad

- CLI surface grows. Help text and catalog must be kept in sync (skill-sync already requires this).
- Kind 9 vs 42 dual ingest is temporary complexity.
- MCP-only sessions cannot complete SC-27 until wave 2. Accept that, or allow them to shell `toron`.
- Connecting `channel post` to the mention dispatcher is real wiring, not a clap wrapper. Underestimate this and SC-27 stays red.

### Neutral

- Flywheel loop and decide table unchanged.
- Federation still off. "Nostr-native" is still local until ADR-00xx turns relays on.
- Presence is published but the loop does not consume it yet.

### Risks

| Risk | Mitigation |
|---|---|
| Mention dispatcher ignores kind 9 | SC-27 is the gate. Implement the wire before declaring CLI done. |
| Agents gift-wrap "to be safe" | Skill + SC-33. `channel post` has no `--wrap` flag. |
| Watch hangs CI | `--timeout-ms` required with `--once`. Tests set 5s. |
| Second claimer silent success | `won: false` + exit 1. SC-28. |
| Skill describes unshipped CLI | Banner: "ADR-0020 proposed. Commands fail until wave 1 lands." Remove banner when green. |

---

## 10. Pros and Cons of the Options

### A — MCP cluster now

- Good: MCP-only agents get rooms immediately.
- Bad: breaks the freeze for a session type that can shell out in herdr. Grok-in-chat is not the swarm.

### B — CLI-first (chosen)

- Good: matches `br`, matches Buzz (relay + cli, not 40 mailbox tools), keeps C3.
- Bad: MCP-only must wait or shell.

### C — Desktop / full NIP-29

- Good: humans get Buzz-like UX.
- Bad: product fat. Not the scarce path. Membership store is enough.

### D — Nothing

- Bad: Level 2 stays a story. Kind drift remains.

---

## 11. More Information

### Intention

A year from now a captain types `flywheel spawn` once. The two panes join `steer-<epic>`. They @mention, claim, result, and react. The captain reads `toron channel read` in the morning. Flywheel `send` was not in the hot path. Gift-wrap still hides the operator passphrase chat. No worktree. No PR. No 50th MCP tool.

### Related

- `docs/notes/2026-08-07-buzz-agentmail-comparison.md`
- `docs/notes/2026-08-07-forge-nip34-research.md` (not in scope)
- ADR-0019 Spec 3 (worktrees forbidden)
- Buzz `NOSTR.md` (kind 9, 7, 20001, 43001, p-gated 1059)

### Revisit

- Wave 2 MCP wrappers: only after a written note that an MCP-no-shell agent is a real user of plane S.
- Federation: only with two-host tests.
- 39000 emit: when we want `nak` to list rooms without toron CLI.

### Acceptance of this ADR

Status stays **proposed** until the captain flips it. Implementation beads may start under proposed. Merge of kind-9 emit + SC-27 should wait for **accepted** or an explicit override.
