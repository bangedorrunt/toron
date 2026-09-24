# Toron vs Buzz vs Agent Mail (Rust) — Comparative Study

> 2026-08-07. Sources: `block/buzz` (main HEAD), `Dicklesworthstone/mcp_agent_mail_rust`
> (main HEAD), `mcpagentmail.com`. Cross-referenced against toron v1.0.0 (ADR-0003 rev7
> implementation, post-bd-j5v). Governed by ADR-0003.

## TL;DR

toron **matches agentmail on the 38-tool mailbox parity contract** (verified by the
`exactly_25_resources` test + 38 `#[tool]` fns + the bd-96a real-e2e probe catalog).
toron **matches or exceeds buzz** on the collaboration/automation primitives it adopted
(kind-as-dispatch, relay pipeline, @mention dispatch, YAML workflows, NIP-29 channels,
ephemeral presence). toron **uniquely wins** on three axes buzz/agentmail cannot touch:
(1) crypto-native identity (Nostr Schnorr, gift-wrapped E2E DMs), (2) federation
(NIP-17 over relays), (3) durable workflow resumption (Resonate — buzz's WF-08 is
unbuilt, agentmail has no workflow engine at all).

toron **lags** on the *operational* surface that both peers ship but toron defers:
doctor reconstruct/archive-scan, share/export bundles, golden-conformance capture,
ATC learning, deep robot mode, the 16-screen TUI, and buzz's Forge (NIP-34 git
events as signed channel-scoped PRs).

The high-leverage learnings for **agent automation + workflow hardening** are below,
ranked.

---

## 1. Does toron match agentmail functionality?

**Yes on the mailbox contract. No on the operational/operator surface.**

### Parity matrix (mailbox — toron ✅)

| agentmail cluster | tools | toron | evidence |
|---|---|---|---|
| Infrastructure (4) | health_check, ensure_project, install/uninstall_precommit_guard | ✅ | `tools.rs`, `mailbox/guard.rs` (libgit2, `TORON_GIT_BINARY`) |
| Identity (6) | register/create/whois/resolve_pane/cleanup/list_agents | ✅ | kind-30013 signed profiles; **stronger**: npub identity vs AM's integer id |
| Messaging (5) | send/reply/fetch_inbox/ack/mark_read | ✅ | NIP-17 gift-wrap; **stronger**: true BCC (E2E), private acks |
| Contacts (4) | request/respond/list/policy | ✅ | kind-30014; enforced at projection (G14) |
| File Reservations (5) | check/acquire/renew/release/force_release | ✅ | symmetric fnmatch, TTL; **stronger**: force-release = operator-signed revocation (G5 A1) vs AM's unsigned server action |
| Search (2) | search_messages, summarize_thread | ✅ | FTS5 (`porter unicode61 remove_diacritics 2`, `prefix='2 3'` — AM tokenizer contract). **Weaker**: relevance-compatible, not Search-V3 tantivy+embeddings fusion |
| Macros (4) | start_session/prepare_thread/contact_handshake/reservation_cycle | ✅ | |
| Product Bus (5) | ensure_product/products_link/search/fetch_inbox/summarize_thread | ✅ | kind-30011 `a`-tagged manifests; **stronger**: content-addressed, federatable |
| Build Slots (3) | acquire/renew/release | ✅ | kind-30032 |

toron has 38 MCP tools + 25 resources + CLI 1:1 parity (`toron mail send` ≡ `send_message`). bd-96a catalogues the **intentional** response-shape divergences (npub vs id, ts_us vs ISO, event-id returns) — these are BY DESIGN per ADR-0003 (Nostr-native), not gaps.

### Where agentmail is ahead (toron gaps — operational, not mailbox)

| Feature | agentmail | toron | impact |
|---|---|---|---|
| **doctor reconstruct** | archive-first SQLite rebuild from Git when DB untrustworthy | ❌ only `doctor --check` | recovery story incomplete; ADR-0003 claims DR via Git↔bus replay (bd-sc14a ships rebuild-from-bus, but not the doctor-reconstruct operator path) |
| **doctor archive-scan / archive-normalize** | non-mutating Git hygiene report + safe remediation (quarantine dup canonical msgs, rewrite project.json) | ❌ | archive debt accumulates silently |
| **share export/import bundle** | `am share export` produces a verifiable bundle; live deploy validated against it | ❌ | no deployment-validation artifact |
| **golden conformance capture** | `golden capture/verify/list` — record/replay fixture harness | partial (toron has `resource_surface_matches_fixture_count` + re-derived first-party fixtures, but no `capture` CLI) | conformance drift detected at test time only, not capturable from a live run |
| **ATC (Adaptive Tool Calling) learning** | `atc_experiences` table, decision/evidence ledger, calibration tick loop, outcome feedback | ❌ | no learning loop on tool-call quality |
| **robot mode depth** | 18 `am robot` subcommands (status/inbox/timeline/overview/thread/search/message/navigate/reservations/metrics/health/analytics/agents/contacts/projects/attachments/atc) | partial: `--robot-snapshot/--robot-status/--robot-capabilities` only | shell-only agents get a thin surface vs AM's rich pollable state |
| **16-screen TUI** | ratatui cockpit (inbox/threads/agents/search/reservations/metrics/health/analytics/attachments/archive/ATC) | ❌ deferred to "Phase 2" but never started | no live operator console |
| **Web UI** | server-rendered `/mail/` + Overseer compose (human can always reach any agent, bypass policy) | ❌ | no human oversight surface |
| **Search V3 (hybrid)** | tantivy lexical + embeddings semantic + fusion (behind `hybrid` feature) | FTS5 only | semantic/natural-language queries weaker |
| **beads CLI cluster** | `am beads ready/list/show/status` (br integration) | ❌ (we have `br` directly, but not surfaced through toron) | agents must shell out to `br`, not call a tool |

**Verdict:** mailbox parity = green. Operational/operator parity = the real gap.

---

## 2. What toron can learn from Buzz

Buzz is **prior art toron already studied** (ADR-0003 §"Prior Art & Leverage" documents
the adoption). The patterns toron took are sound. The patterns toron **didn't** take —
or took incompletely — are where the lessons are.

### 2.1 What toron adopted well (confirm)

- **Kind-as-dispatch** — toron's frozen registry (30010–30014, 30031/30032, 43001–06,
  46001–12, 20001) mirrors buzz's `kind.rs` discipline. New feature = new kind, zero
  break. ✅
- **Relay pipeline** — toron's `InProcessBus` runs verify→auth→dedup→access-gated-
  fanout→search→audit→workflow-trigger (7 stages, parity-tested). ✅
- **@mention-as-dispatch** — toron's `dispatch_job` + bounded semaphore + one-in-flight-
  per-agent (A7) = buzz-acp's claim/return pattern. SC-12 (burst backpressure) proves it.
  ✅
- **Ephemeral presence** — kind 20001, fanned-out live, never stored. ✅
- **Membership-only-gate + global-sub exclusion** — toron channels (NIP-29) enforce this;
  SC-13 proves non-members denied + global subs excluded. ✅
- **SSRF/feed hygiene** — `is_private_ip` handles IPv4-mapped-IPv6 (`::ffff:a.b.c.d`).
  ✅

### 2.2 Where toron beats Buzz (confirm the ADR-0003 superiority claims)

| Dimension | Buzz | toron |
|---|---|---|
| Workflow approval resumption | **WF-08 UNBUILT** — `request_approval` returns Suspended+UUID but doesn't persist/resume; runs hitting a gate marked **failed** | **Resonate-backed** — SC-11 proves suspend→crash→resume completes (not failed) |
| Mailbox semantics | none (chat/forge only) | full 38-tool AM-Rust parity |
| Local-first footprint | Postgres + Redis + relay (3 services) | one embedded process (in-process bus + SQLite + Git) |
| Workflow `delay` cap | **270s** (in-process) | **Resonate durable sleep = unbounded** (hours/days/weeks) |
| Identity | relay-issued | Schnorr npub (unforgeable) |
| DM privacy | relay-visible | NIP-17 gift-wrap E2E |
| Federation | single-relay only | opt-in NIP-17 relays |

### 2.3 What toron should LEARN from Buzz (the gaps)

These are the transferable patterns toron adopted **in name only** or skipped. Ranked
by leverage on agent-automation + workflow robustness.

#### 🔴 HIGH LEVERAGE

**L1 — buzz-audit hash-chain (tamper-evident, not just append-only).**
toron's audit = Git commits (chains naturally, but no separate tamper-evidence for
non-Git events like bus/relay events, presence, ephemeral). Buzz's `buzz-audit`:
SHA-256 `prev_hash` chain, `pg_advisory_lock` single-writer, `catch_unwind` lock-release,
10 canonical actions, `verify_chain()` recompute. **toron gap:** a relay-observer or
federated peer has no tamper-evidence on bus events that never hit Git (ephemeral,
workflow-run transitions before commit). **Fix:** add a `toron-audit` hash-chain over
the bus event stream (not replacing Git — complementing it for non-archived events).
ADR-0003 §risk-register mentions "ephemeral-audit pointer (A8)" as already-covered-by-
this-sentence — it's a one-line stub, not implemented.

**L2 — buzz-acp queue.rs batching + dedup (prompt coalescing).**
toron's `dispatch.rs` has the claim/return semaphore (A7) but the **per-channel event
queue + batching into one session/prompt** is buzz-acp's `queue.rs` (2,565 LOC).
toron's SC-12 proves backpressure but does toron batch N queued @mentions into ONE
prompt? Check: `dispatch_job` creates one kind-43001 per mention. **Buzz batches** —
"N queued events batched into a single prompt." **toron gap:** an @mention burst to one
agent = N separate prompts, not 1 coalesced. For agent-automation efficiency this
matters: 20 mentions → 20 prompts burns context; 20 mentions → 1 batched prompt is the
buzz-acp win. **Fix:** add a coalescing layer in `dispatch.rs` — collect mentions within
a short window (e.g. 2s) per assignee, emit one `session/prompt`-equivalent.

**L3 — Buzz Forge (NIP-34 git events as channel-scoped PRs).**
Buzz's VISION_PROJECTS: push → channel created for branch → CI agent picks up → posts
results → reviewer approves (signed event) → merge → channel archives. Kinds 30617
(repo), 30618 (push), 30621 (project), 1630 (CI-passed), 1631 (merge), 46011 (approval).
**toron gap:** toron reserved kind numbers for a "Phase 3 Forge" but shipped nothing.
This is the single biggest agent-collaboration feature toron lacks vs buzz. flywheel
does git orchestration today via filesystem + herdr, not via signed events. **Fix
(deferred):** the Forge is a large build; file as a Phase 3 epic, not a quick win. But
the *concept* — "every git push/CI/approval/merge is a signed event in a channel" —
is the blueprint for federated agent code-collab.

#### 🟡 MEDIUM LEVERAGE

**L4 — Buzz workflow `if:` guards with evalexpr (100ms timeout, custom fns).**
toron's `lint.rs` validates workflow YAML statically but does the runtime evaluator
have the `if: str_contains(trigger_text, 'production')` guard layer? Buzz: 4 custom
fns (`str_contains`, `str_starts_with`, `str_ends_with`, `str_len`), 100ms timeout to
prevent adversarial expressions. **toron gap to verify:** does `toron-workflow`'s
runtime apply `if:` guards with a timeout? If not, a malformed guard expression could
hang a workflow worker. **Fix:** ensure guard evaluation is bounded (timeout + sandbox).

**L5 — Buzz workflow loop-prevention is explicit and tested.**
Buzz excludes kinds 46001–46012, relay-signed `buzz:workflow`-tagged events, and
KIND_GIFT_WRAP from triggering workflows. **toron gap to verify:** toron's workflow
trigger filter must exclude its own job/workflow-run kinds (43001–06, 46001–12) +
gift wraps, or a workflow that `send_message`s creates an infinite loop
(message_posted → workflow → send_message → message_posted). Check the `wf.on_event`
exclusion list in toron's bus.

**L6 — Buzz multi-community (tenant) isolation is end-to-end.**
Buzz: `req.community = resolve_host(connection.host)` established BEFORE auth/event/
req/rest/media/git/search/workflow/pubsub. Unknown hosts fail closed. NIP-98 stamps
must agree with host-derived community. **toron analogue:** project scoping via `#t`
tag + NIP-32 labels (A3 dual-emit). **toron gap:** is project isolation enforced at
every bus entry point, or only at the projection? Buzz enforces at the *relay ingress*,
before the event is even stored. toron's contact-policy enforcement is at the projection
(G14) — an event from a blocked key is *stored* then *dropped at index*. Buzz never
stores it. **Fix (hardening):** move contact-policy/project-isolation checks earlier in
the pipeline (ingress), not just projection.

#### 🟢 LOWER LEVERAGE (but worth noting)

**L7 — Buzz push-gateway (Expo/APNs/FCM).** Mobile push. Out of scope for toron
(agent-automation focus) but noted for completeness.

**L8 — Buzz voice/huddle (Opus relay).** Real-time audio. Out of scope.

**L9 — Buzz relay-mesh.** Multi-relay federation topology. toron has NIP-77 negentropy
for DR rejoin; relay-mesh is the always-on federated version. Deferred.

---

## 3. What toron can learn from Agent Mail (Rust)

agentmail is the **parity target** — toron already matches its mailbox. The lessons are
in the *operational* layers agentmail built that toron deferred.

### 🔴 HIGH LEVERAGE

**A1 — doctor reconstruct (archive-first SQLite rebuild).**
agentmail: `am doctor reconstruct` captures forensic bundle, quarantines bad DB, rebuilds
fresh SQLite from Git archive, merges into place. **toron:** has `sc14a_dr_rebuild`
(test proving rebuild-from-Git works) but no operator CLI command. The capability exists;
the operator surface doesn't. **Fix:** expose `toron doctor reconstruct` wrapping the
existing rebuild logic. Small effort, large ops value.

**A2 — doctor archive-scan / archive-normalize.**
Non-mutating Git hygiene report + safe remediation. Catches: missing project.json, dup
canonical message ids, malformed canonical files. **toron gap:** Git archive debt
accumulates silently. **Fix:** `toron doctor archive-scan` (read-only report) + archive-
normalize (quarantine-only, never delete). Medium effort.

**A3 — share export/import bundle (deployment validation).**
`am share export -o /tmp/bundle --no-zip` → verify live deploy against bundle. **toron
gap:** no deployment-validation artifact. For a federated/distributed system this is
how you prove two instances are consistent. **Fix:** `toron share export` (bundle of
project manifests + agent profiles + canonical message hashes) + `toron share verify`.

### 🟡 MEDIUM LEVERAGE

**A4 — robot mode depth (18 subcommands vs toron's 3).**
agentmail's `am robot` is a full agent-driver surface: status/inbox/timeline/overview/
thread/search/message/navigate/reservations/metrics/health/analytics/agents/contacts/
projects/attachments/atc. toron has `--robot-snapshot/--robot-status/--robot-capabilities`.
**The gap:** a shell-only agent (no MCP client) driving toron gets a thin snapshot, not
navigable state. **Fix (incremental):** add the high-value robot subcommands —
`robot inbox`, `robot thread`, `robot search`, `robot reservations` — each emits JSON
identical to the MCP tool return. The ADR-0003 CLI design section already specifies
this ("every MCP tool has a 1:1 CLI subcommand"); it's a completion gap, not a design
gap.

**A5 — ATC (Adaptive Tool Calling) learning loop.**
agentmail's differentiator: `atc_experiences` table records every tool-call decision +
outcome, a calibration tick loop adjusts routing/conflict/liveness math, outcome
feedback closes the loop. **toron gap:** no learning. Every tool call is stateless w.r.t.
quality. **Fix (large, research-y):** this is agentmail's moat; adopting it is a
multi-quarter effort. File as a research epic, not a quick win. Note: hachiko's
"calibration" work (ADR-0040/0050) is the spiritual analogue — toron could borrow the
*framework* (experience row + outcome) without the trading-specific math.

**A6 — Overseer (human-always-reachable).**
agentmail's Web UI has an Overseer compose form: messages from `HumanOverseer` agent,
always high-importance, bypass contact policies, with a "pause current work" preamble.
**toron gap:** a human operator wanting to redirect an agent mid-task must use the CLI
and respect contact policies. **Fix:** add an operator-bypass path (operator key signs
a high-importance message that skips policy enforcement at the projection). Small,
well-motivated — matches ADR-0003's operator-key role.

### 🟢 LOWER LEVERAGE

**A7 — Search V3 hybrid (tantivy + embeddings).** toron's FTS5 is "relevance-compatible"
per ADR-0003, deliberately not Search-V3-identical. The hybrid path is behind a feature
flag in agentmail. Adopting tantivy would be a large rewrite for marginal gain at toron's
message volumes. Defer.

**A8 — beads cluster as MCP tools.** agentmail exposes `am beads ready/list/show/status`.
toron agents shell out to `br`. Minor — `br` is richer than an MCP wrapper would be.

**A9 — 16-screen TUI.** Deferred per ADR-0003. The integrated-mode primary UI is
flywheel's loop; standalone daemon UI is the TUI. Still not built. Medium-large effort,
operator convenience value.

---

## 4. Ranked action list (agent automation + workflow hardening focus)

Ordered by (leverage × inverse-effort). Each is a bead candidate.

| # | Source | Item | Effort | Leverage |
|---|---|---|---|---|
| 1 | Buzz L2 | **Mention coalescing in dispatch.rs** — batch N @mentions to one assignee within a 2s window into one job/prompt | S | 🔴🔥 |
| 2 | AM A1 | **`toron doctor reconstruct`** — expose the existing rebuild-from-Git as an operator command | S | 🔴 |
| 3 | Buzz L5 | **Verify + harden workflow loop-prevention** — confirm 43001–06/46001–12/gift-wrap excluded from triggers; add a test | S | 🔴 (correctness) |
| 4 | Buzz L4 | **Bound workflow `if:` guard evaluation** — 100ms timeout + sandbox; test adversarial expressions | S | 🟡 (robustness) |
| 5 | AM A6 | **Operator-bypass (Overseer) message path** — operator-key signs high-importance msg that skips contact policy | S | 🟡 |
| 6 | AM A4 | **Robot mode expansion** — `robot inbox/thread/search/reservations` JSON subcommands (CLI≡MCP parity completion) | M | 🟡 |
| 7 | Buzz L6 | **Move project/contact isolation to bus ingress** — reject blocked-key events before storage, not just projection | M | 🟡 (hardening) |
| 8 | AM A2 | **`toron doctor archive-scan` + `archive-normalize`** | M | 🟡 |
| 9 | Buzz L1 | **`toron-audit` hash-chain** for non-Git events (ephemeral, workflow transitions pre-commit) | M | 🟡 (tamper-evidence) |
| 10 | AM A3 | **`toron share export/verify`** deployment-validation bundle | M | 🟡 (federation ops) |
| 11 | AM A5 | **ATC learning loop** (research epic — borrow framework from hachiko calibration) | L | 🔴 (long-term moat) |
| 12 | Buzz L3 | **Forge (NIP-34 git events as channel-scoped PRs)** — Phase 3 epic | L | 🔴 (federated code-collab) |

Items 1–5 are quick wins (S = hours-to-a-day each). Items 6–10 are medium (days each).
Items 11–12 are quarter-scale.

---

## 5. What toron should NOT copy

- **Buzz's Postgres+Redis dependency stack.** toron's local-first one-process footprint
  is a feature, not a debt. Keep SQLite+Git.
- **Buzz's single-relay-only model.** toron's federation is additive; don't regress.
- **agentmail's integer-id identity.** toron's npub is strictly stronger.
- **agentmail's unsigned server actions.** toron's operator-signed revocations are
  attributable; don't weaken.
- **Search V3's full hybrid stack** unless message volume justifies it.

---

## 6. Net assessment

toron is **architecturally ahead** of both peers (Nostr-native identity + federation +
durable workflows + local-first). It is **operationally behind** agentmail (doctor/
share/robot/TUI/ATC) and **feature-behind** buzz on the collaboration UX layer (Forge,
rich workflow guards, multi-tenant). The mailbox parity contract — toron's reason for
existing — is met.

The path to "better, more efficient, robust, especially agent automation + workflow" is:
**ship items 1–5 (quick wins)**, then **6–10 (medium)**, then decide between the two
long bets (ATC vs Forge) based on whether the priority is *smarter agents* (ATC) or
*federated code-collab* (Forge).

**Research notes filed (2026-08-07, lane OC-C):**
- ATC learning loop (item 11, bd-nyg): `docs/notes/2026-08-07-atc-learning-loop.md`
  (experience-row schema + two-phase outcome + calibration tick; hachiko analogue).
- Forge NIP-34 (item 12, bd-9o9): `docs/notes/2026-08-07-forge-nip34-research.md`
  (**kind-map correction** — canonical NIP-34 assigns `30621`=git commit not
  "project", `1630`=Open-status not "CI-passed"; only `46011` is toron-custom).

// governed-by: ADR-0003

---

## GS-061 delta (2026-09-24)

Scout: `/home/box/agent-data/grok-ship/reports/GS-061.md` · Buzz tip `d01e5f82…`.
Pointer: **ADR-0003 Revision 9** (leverage/backlog refresh — not a protocol rewrite).

### Aug-07 L-items — landed vs open @ toron tip `b514dea9`

| ID | Theme | Status |
|----|-------|--------|
| L2 | Mention coalescing | **LANDED** (`MentionCoalescer`; CHANGELOG Ship 1) |
| L4 | Workflow `if:` guards + 100ms | **LANDED** (`guard.rs`) |
| L5 | Loop-prevention | **LANDED** (`trigger_filter`) |
| L1 | `toron-audit` hash-chain | **OPEN** (Buzz TLV #7492 refreshes encoding) |
| L3 | Forge / NIP-34 | **OPEN / epic** (captain kinds gate; ADR-0033 freeze) |
| L6 | Ingress isolation (reject before store) | **PARTIAL** — verify-before-route tripwires landed GS-061; projection-side still weaker than Buzz relay ingress |
| L7–L9 | Push / voice / relay-mesh | **REJECT (scope)** |

### New P0/P1 from 2026-08→09 Buzz (this ship)

- **P0** busy-owner bound+fence+deadline wake (#7337/#7340) — **library wedge / not on live path** (`try_acquire` still live); verify-before-route tripwires (#7010) — wired
- **P1** convergence / `presence_admit` (#7298/#7532, SQLite) — library + hardcoded `presence_admit(true)` (do **not** claim fail-closed presence shipped); workflow delete honesty (#7735) / delivery-cursor (#7620) — pure modules without host call sites; wake quiet/deaf (#7546) — toron `WakeStats` only; flywheel classifier on `gs-061/wake-liveness`

Do **not** invent-green LIVE features beyond what the code + tests now prove. Historical body of this note is unchanged.
