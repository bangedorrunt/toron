---
status: accepted
date: 2026-08-07
accepted: 2026-08-07
revision: 1
decision-makers: bangedorrunt (captain)
consulted: ""
informed: ""
supersedes: []
related: [ADR-0003, block/buzz, Dicklesworthstone/mcp_agent_mail_rust, mcpagentmail.com]
---

# ADR-0004: Operational Hardening & Agent-Automation Efficiency (Post-Parity)

> Governs the post-parity hardening wave: 5 concrete ships that close the
> operational-and-automation gaps surfaced by the 2026-08-07 comparative study
> of `block/buzz` and `Dicklesworthstone/mcp_agent_mail_rust` (AM-Rust) against
> toron v1.0.0. Each ship is tracked as a bead with explicit acceptance
> criteria; this ADR is the architectural frame that makes them a coherent
> decision rather than five independent patches.
>
> **What this ADR is NOT:** it does not change ADR-0003's mailbox contract (38
> tools, 25 resources, 9 clusters — frozen), the kind registry (frozen), the
> runtime (Tokio), the MCP server (rmcp), the local-first footprint
> (SQLite+Git, one process), or the `#![forbid(unsafe_code)]` discipline. It
> hardens the layers ADR-0003 shipped.

## Context and Problem Statement

### Where toron stands after ADR-0003

ADR-0003 is **shipped**: Phase 1 (AM-Rust mailbox parity + integrated mode) and
Phase 2 (Buzz-grade collaboration: `toron-workflow`, @mention dispatch, NIP-29
channels, ephemeral presence) are complete. Verification (2026-08-07 review,
bead `bd-x3y`): 286 lib tests + e2e + integration green; `clippy --workspace
-D warnings` clean; `scripts/verify-gates.sh` green (no asupersync, no
fastmcp, `forbid(unsafe_code)` in all 5 crate roots); all ADR-0003 witnesses
cross-check (43 `#[tool]` markers, `exactly_25_resources`, single
`agent_prompt` in toron-core, 115 governed-by files, 3 honest federation
`#[ignore]`s).

### What the comparative study found

Full study: [`docs/notes/2026-08-07-buzz-agentmail-comparison.md`](../notes/2026-08-07-buzz-agentmail-comparison.md).
Net assessment:

- **toron is architecturally ahead** of both peers on four axes AM-Rust and
  Buzz cannot match: (1) Schnorr npub identity (AM-Rust uses integer ids),
  (2) NIP-17 gift-wrapped E2E DMs with true BCC (AM-Rust is plaintext SQLite),
  (3) opt-in federation over relays (both peers are single-process), (4)
  Resonate-backed durable workflow resumption — Buzz's `request_approval`
  (workflow kind WF-08) is **unbuilt**: runs that hit an approval gate are
  marked **failed**; toron's SC-11 proves suspend→crash→resume completes.
- **toron is operationally behind** AM-Rust on the operator surface (doctor
  reconstruct/archive-scan, share/export bundles, golden conformance capture,
  robot-mode depth, 16-screen TUI, Web UI/Overseer).
- **toron is feature-behind** Buzz on two automation-efficiency /
  correctness-hardening fronts that directly weaken "better, more efficient,
  robust agent automation + workflow" — the captain's stated priority.

### The problem (concretely)

Two of the gaps are **correctness/efficiency must-haves** that compound
regardless of any future strategic direction:

1. **Mention burst → N prompts (context burn).** toron's `dispatch_job`
   creates one kind-43001 per @mention. An @mention burst to one assignee
   fires N separate prompts, burning the agent's context window. Buzz's
   `buzz-acp` `queue.rs` (2,565 LOC) batches N queued events per channel into
   ONE `session/prompt`. This is the single highest-leverage agent-automation
   efficiency win in the study.
2. **Unverified workflow loop-prevention (infinite-loop risk).** Buzz
   explicitly excludes workflow-run kinds (46001–46012), relay-signed
   `buzz:workflow`-tagged events, and `KIND_GIFT_WRAP` from triggering
   workflows. A `send_message` workflow that re-triggers on its own emitted
   message is an infinite loop (`message_posted → workflow → send_message →
   message_posted`) unless the trigger filter excludes the workflow's own
   emitted kinds. toron's exclusion list must be verified, and if absent,
   hardened — this is a latent correctness bug.

Two more are **robustness/footgun** hardening:

3. **Unbounded workflow `if:` guard evaluation.** Buzz bounds guard
   expressions with `evalexpr` + 100ms timeout + 4 custom functions
   (`str_contains`, `str_starts_with`, `str_ends_with`, `str_len`). An
   unbounded evaluator lets a malformed/adversarial guard expression hang a
   workflow worker.
4. **No operator-bypass path.** AM-Rust's Web UI has an Overseer compose form:
   messages from `HumanOverseer`, always high-importance, **bypass contact
   policies**, with a "pause current work" preamble. toron forces a human
   operator redirecting an agent to respect contact policies — friction that
   contradicts ADR-0003's operator-key privileged role.

One is an **operational-recovery gap**:

5. **No `doctor reconstruct` CLI.** AM-Rust ships `am doctor reconstruct`:
   capture forensic bundle, quarantine bad DB, rebuild fresh SQLite from Git
   archive, merge atomically. toron **has the rebuild logic** (the
   `sc14a_dr_rebuild` test proves rebuild-from-Git works; ADR-0003 §G11 claims
   two-source DR) but **no operator command** — the capability exists, the
   surface doesn't. An operator with a corrupted SQLite has no documented
   recovery path.

### Why now

The mailbox parity contract — toron's reason for existing — is met. The next
binding constraint on agent-automation quality is the operational +
efficiency/correctness layer above it. These 5 ships are low-effort/high-leverage
and **orthogonal to the strategic fork** (ATC vs Forge — see Non-Goals):
shipping them now de-risks either future direction.

## Decision Drivers

* **Agent-automation efficiency.** Context-window budget is the scarce
  resource on the agent↔agent path; mention coalescing is the biggest lever.
* **Workflow correctness.** An infinite-loop or a hung worker is a silent
  overnight-loop killer (cf. flywheel's hard-won reliability discipline).
* **Operational recoverability.** A corrupted SQLite with no `reconstruct`
  command is an unrecoverable mailbox in the field.
* **Operator reachability.** A blocked-by-policy operator is an operator who
  can't redirect a runaway agent — a safety regression vs AM-Rust.
* **No regression on ADR-0003 wins.** Identity/federation/durability/local-first
  are preserved; this hardens, never weakens.
* **Orthogonality to the strategic fork.** None of the 5 ships pre-empts the
  ATC-vs-Forge decision; they're table stakes either way.

## Considered Options

### Option A — Ship the 5 quick/medium wins; defer the 2 long bets (CHOSEN)

The 5 ships below. Defer ATC learning loop and Forge (NIP-34 git events) as
research epics pending a captain fork (Non-Goals).

### Option B — Commit to one long bet now (ATC or Forge); skip the quick wins

Rejected. The 5 wins are correctness/efficiency must-haves that compound
regardless of which long bet is chosen. Deferring them leaves known gaps
(infinite-loop risk, context burn, no recovery path) while a multi-quarter
research bet runs — the wrong priority order.

### Option C — Adopt Buzz's full Postgres + Redis dependency stack

Rejected. toron's local-first one-process footprint (SQLite + Git, embedded
in flywheel) is a **feature**, not a debt — it is what makes integrated mode
(zero-IPC orchestration) possible. Buzz needs 3 services; toron needs 1.

### Option D — Regress to AM-Rust's integer-id / plaintext-DM model for parity simplicity

Rejected. ADR-0003's Nostr-native wins (Schnorr identity, E2E DMs, federation,
attributable operator revocations) are the **point** of toron. This ADR
explicitly preserves them.

## Decision Outcome

**Chosen: Option A.** The 5 ships, each governed by its bead. Order = the
study's ranked list (leverage × inverse-effort). All 5 land in toron-core /
toron-workflow / toron-cli — **no new crate**, **no kind-registry change**,
**no ADR-0003 contract change**.

### The 5 ships

| # | Bead | P | Layer | What |
|---|------|---|-------|------|
| 1 | `bd-bzu` | 1 | workflow | **Mention coalescing** — batch N @mentions to one assignee within a configurable window (default 2s) into ONE coalesced job, preserving order + dedup. (Buzz `buzz-acp/queue.rs` pattern.) |
| 2 | `bd-nsv` | 1 | workflow | **Workflow loop-prevention hardening** — audit + harden the trigger-ingress exclusion list: confirm kinds 43001–43006 (jobs), 46001–46012 (workflow-run), 1059 (gift-wrap), and any `toron:*`-tagged relay-signed events are EXCLUDED from triggering workflows. Add a regression test. (Buzz `wf.on_event` filter.) |
| 3 | `bd-jwa` | 2 | ops | **`toron doctor reconstruct`** — expose the existing rebuild-from-Git as an operator command: forensic bundle → quarantine bad DB → rebuild fresh SQLite from Git archive → atomic merge. Reuse `crates/toron-core/src/index/rebuild.rs`. (AM-Rust `am doctor reconstruct`.) |
| 4 | `bd-n87` | 2 | workflow | **Bound workflow `if:` guard evaluation** — 100ms timeout + sandbox; register the 4 Buzz custom functions if missing; adversarial-expression test. (Buzz `evalexpr` + timeout.) |
| 5 | `bd-o1h` | 2 | mailbox | **Operator-bypass (Overseer) message path** — operator-key-signed high-importance messages SKIP contact-policy enforcement at the projection (`block_all` does not block the operator); CLI `toron mail overseer`; tag `toron:overseer`. (AM-Rust Overseer.) |

### Architectural rules these ships obey (invariants)

1. **No new crate.** All 5 land in existing crates (`toron-core`, `toron-workflow`,
   `toron-cli`). The workspace shape from ADR-0003 is unchanged.
2. **No kind-registry change.** The ADR-0003 frozen registry (30010–30014,
   30031/30032, 43001–06, 46001–12, 20001, 14/15/1059) is untouched. Ship 5
   (`toron:overseer`) is a **tag**, not a kind — it rides on kind-14.
3. **No mailbox-contract change.** The 38-tool / 25-resource / 9-cluster
   surface is frozen. None of these ships adds/removes/renames a tool or
   changes a response shape. (bd-96a's catalogued divergences are intentional
   and unaffected.)
4. **ADR-0003 wins preserved.** Schnorr identity, NIP-17 E2E, federation,
   Resonate durability, local-first footprint, `#![forbid(unsafe_code)]`,
   Tokio, rmcp — all unchanged.
5. **Coalescing is opt-in-safe.** Ship 1's default window (2s) is short enough
   that a single @mention dispatches immediately (no latency regression for
   the common case); only true bursts coalesce.
6. **Loop-prevention is at ingress, not projection.** Ship 2 excludes
   workflow-emitted kinds at the **trigger filter** (before the workflow
   engine evaluates them), not at the projection — matching Buzz's `wf.on_event`
   placement. This is the only correct layer for loop-prevention.
7. **Guard bounding is fail-stop, not fail-silent.** Ship 4 times out the
   evaluator and marks the step failed (not hung); the workflow run is
   observable as failed, not stuck.
8. **Overseer bypass is key-scoped, not name-scoped.** Ship 5's policy bypass
   triggers on **operator-key signature**, not on a sender name. An agent
   cannot spoof Overseer by naming itself "HumanOverseer" — only the
   operator's npub unlocks the bypass. This is strictly stronger than
   AM-Rust's name-based `HumanOverseer` agent.

## Consequences

### Good

- **Agent-automation efficiency up.** Mention bursts stop burning context
  (ship 1) — the highest-leverage win in the study.
- **Workflow correctness floor raised.** No infinite loops (ship 2), no hung
  workers (ship 4) — the two latent overnight-loop killers closed.
- **Operational recoverability complete.** A corrupted SQLite has a
  documented, tested recovery path (ship 3) — AM-Rust parity on the doctor
  surface.
- **Operator reachability restored.** A blocked-by-policy agent can still be
  redirected by the operator (ship 5) — safety parity with AM-Rust, achieved
  via crypto identity (strictly stronger).
- **Strategic fork de-risked.** Whether the next bet is ATC or Forge, these
  5 ships are table stakes already paid.

### Bad

- **Coalescing adds a dispatch-layer buffer.** Ship 1 introduces a 2s (default)
  windowed buffer in `dispatch.rs`. For @mention bursts this is the win; for a
  lone @mention it's up-to-2s of latency before the dispatch flushes. Mitigated
  by: flush-on-window-close + immediate-flush if no further mentions arrive
  (the common case dispatches at first mention, not at window expiry).
- **Loop-prevention exclusion list must track the kind registry.** Ship 2's
  exclusion list is a second place where the workflow-run kind range appears
  (the first is the registry). Mitigated by: a single `WORKFLOW_EMITS_KINDS`
  constant in `toron-workflow` sourced from the registry, not hand-maintained.
- **`doctor reconstruct` is destructive-ish.** Ship 3 quarantines the bad DB
  and writes a fresh one. Mitigated by: forensic bundle captured first;
  quarantine (not delete); atomic merge; dry-run (`--dry-run`) supported.
- **Overseer bypass weakens contact-policy absolutism.** Ship 5 means
  `block_all` is not absolutely blocking — the operator can always reach.
  This is **by design** (matches ADR-0003's operator-key privileged role and
  AM-Rust's Overseer semantics) but must be documented so agents setting
  `block_all` understand the operator exception.

### Neutral

- **No new dependency.** evalexpr (ship 4) is already transitively present via
  workflow YAML evaluation if toron-workflow uses it; if not, it's a small,
  pure-Rust, well-audited crate (Buzz's choice). No Postgres/Redis (Option C
  rejected).
- **5 beads, 1 ADR.** The umbrella framing makes the wave coherent; each bead
  remains independently shippable and revertable.

## Non-Goals (deferred — separate ADR if pursued)

1. **ATC (Adaptive Tool Calling) learning loop** (`bd-nyg`, P3 research epic).
   AM-Rust's `atc_experiences` table + calibration tick loop + outcome feedback
   is its long-term moat. toron has no learning. This is a multi-quarter
   research bet, not a hardening ship. The framework shape (experience row +
   outcome) is reusable from hachiko's `CalibrationTracker`/`ForecastRecorder`
   work (ADR-0040) — but adopting it is a separate decision. **Deferred until
   agent-automation quality is the binding constraint and the captain forks
   ATC-vs-Forge.**
2. **Forge (NIP-34 git events as channel-scoped PRs)** (`bd-9o9`, P3 Phase 3
   epic). Buzz's VISION_PROJECTS: push → channel-per-branch → CI result →
   signed approval → merge → archive. The single biggest agent-collaboration
   feature toron lacks. Requires NIP-34 kind reservation (30617/30618/30621,
   1630/1631, 46011) + a new listener surface (git post-receive hook). Large
   build. **Deferred until federated code-collab is the priority; needs its
   own ADR (kind reservation + listener architecture).**
3. **Search V3 hybrid (tantivy + embeddings).** toron's FTS5 is
   "relevance-compatible" per ADR-0003, deliberately not Search-V3-identical.
   Marginal gain at toron's message volumes. Defer.
4. **16-screen TUI + Web UI.** Deferred per ADR-0003. The integrated-mode
   primary UI is flywheel's loop; the standalone daemon's UI is the TUI.
   Operator convenience, not agent-automation leverage.
5. **`am share export/verify` deployment-validation bundle.** Medium
   leverage for federated ops; not in this wave. File as a follow-up if
   federation deployment-validation becomes a real need.

These deferrals are **decisions not to decide now**, not commitments to decide
later. Each will need its own ADR if/when pursued.

## Implementation Plan

> Each ship is tracked by its bead (acceptance criteria, files, tests). This
> section is the cross-ship architectural frame; the beads are the per-ship
> contracts.

### Ship 1 — Mention coalescing (`bd-bzu`, P1)

- **Affected paths**: `crates/toron-workflow/src/dispatch.rs`,
  `crates/toron-workflow/src/jobs.rs`, `crates/toron-e2e/tests/` (new
  coalesce test).
- **Pattern**: a windowed per-assignee buffer in `dispatch.rs`. On
  `dispatch_job`: (a) if a buffer is open for this assignee, append the
  mention + reset the timer; (b) if no buffer, open one with the mention +
  start the timer (default 2s, env `TORON_COALESCE_WINDOW_MS`). On timer
  expiry: flush the buffer as ONE coalesced kind-43001 (combined `spec`,
  ordered mention list, dedup identical mentions). Immediate-flush if the
  buffer sees no new mention within a short sub-window (e.g. 500ms) so the
  common single-mention case dispatches without waiting the full window.
- **Avoid**: changing the kind-43001 wire shape (the coalesced job is a valid
  43001 with a richer `spec`, not a new kind); infinite coalesce (hard cap:
  flush at window expiry regardless of buffer size); reordering (preserve
  mention arrival order in the coalesced spec).
- **Tests**: `burst-coalesce` (20 mentions → 1 job within window); ` lone-
  mention-immediate` (1 mention → dispatches < window); `ordering-preserved`;
  SC-12 (burst backpressure) still green.
- **governed-by**: this ADR + ADR-0003 §Phase 2 (@mention-as-dispatch).

### Ship 2 — Workflow loop-prevention hardening (`bd-nsv`, P1)

- **Affected paths**: `crates/toron-workflow/src/` (trigger ingress filter),
  `crates/toron-e2e/tests/` (new regression test).
- **Pattern**: a single `WORKFLOW_EMITS_KINDS: &[u32]` constant in
  `toron-workflow` (sourced from the kind registry, not hand-maintained)
  listing 43001–43006, 46001–46012. The trigger-ingress filter rejects any
  event whose kind is in that list OR is kind-1059 (gift-wrap) OR carries a
  `toron:workflow-emitted` tag (relay-signed workflow output). Mirror's Buzz's
  `wf.on_event` exclusion.
- **Avoid**: excluding at the projection (too late — the loop has already
  fired); hand-maintaining the kind list (drift risk); excluding kind-14/15
  wholesale (legitimate user DMs must still trigger `message_posted`).
- **Tests**: `self-send-message-workflow-terminates` (a workflow whose action
  is `send_message` does NOT re-trigger itself; assert run completes with ≤1
  emitted message); all existing workflow SC tests green.
- **governed-by**: this ADR + ADR-0003 §Buzz adopt (relay pipeline).

### Ship 3 — `toron doctor reconstruct` (`bd-jwa`, P2)

- **Affected paths**: `crates/toron-cli/src/operator.rs` (or new `doctor.rs`),
  `crates/toron-core/src/index/rebuild.rs` (reuse, extend with forensic
  bundle), `crates/toron-cli/src/cli.rs` (subcommand wiring).
- **Pattern**: `toron doctor reconstruct [--dry-run] [--data-dir <p>]`.
  Steps: (1) capture forensic bundle (SQLite dump via `.dump` + config +
  `PRAGMA integrity_check` output) to `<data-dir>/forensic-<ts>/`; (2)
  quarantine bad DB to `<data-dir>/<db>.quarantined-<ts>`; (3) rebuild fresh
  SQLite from Git archive via the existing `rebuild.rs` logic; (4) atomic
  merge into place (rename). `--dry-run` does steps 1 + report only.
- **Avoid**: duplicating `rebuild.rs` (reuse it); deleting the bad DB
  (quarantine); silent failure (exit 1 + forensic bundle path on rebuild
  failure; exit 3 for doctor-diagnostic per ADR-0003 CLI exit codes).
- **Tests**: new e2e `sc_doctor_reconstruct` (corrupt SQLite → reconstruct →
  state consistent with pre-corruption snapshot); `sc14a_dr_rebuild` still
  green.
- **governed-by**: this ADR + ADR-0003 §DR (G11).

### Ship 4 — Bound workflow `if:` guard evaluation (`bd-n87`, P2)

- **Affected paths**: `crates/toron-workflow/src/` (guard/runtime evaluator).
- **Pattern**: wrap the `if:`-guard evaluator in a `tokio::time::timeout`
  (100ms default, env `TORON_GUARD_TIMEOUT_MS`). Register the 4 Buzz custom
  functions (`str_contains`, `str_starts_with`, `str_ends_with`, `str_len`)
  if missing. On timeout: step result = `Failed("guard timeout")`, workflow
  run observable as failed (not hung).
- **Avoid**: recursive template resolution (Buzz is single-pass; stay
  single-pass); unbounded string operations inside guards (the timeout is the
  backstop, not the only defense — keep the fn set minimal).
- **Tests**: `adversarial-guard-times-out` (an expression that would hang is
  killed at ~100ms); `legitimate-guards-pass` (the 4 custom fns evaluate
  correctly within budget).
- **governed-by**: this ADR.

### Ship 5 — Operator-bypass (Overseer) message path (`bd-o1h`, P2)

- **Affected paths**: `crates/toron-core/src/mailbox/contacts.rs` (policy
  check), `crates/toron-cli/src/cli.rs` (`mail overseer` subcommand),
  `crates/toron-core/src/keys/` (operator-key verification at the policy
  enforcement point).
- **Pattern**: at the projection contact-policy enforcement (G14), before
  dropping an inbound gift-wrap from a policy-violating key, check whether
  the event is signed by the **operator key** (config `operator_key` / env
  `TORON_OPERATOR_KEY` — a **path** to the encrypted ncryptsec, matching the
  existing `toron key generate` + daemon load path; the ADR's original
  `TORON_OPERATOR_NSEC` raw-secret-in-env form was deliberately NOT adopted:
  toron never places a raw nsec in env). If so, AND the event carries a
  `toron:overseer` tag (a `t` tag, consistent with the Ship-2
  `toron:workflow-emitted` marker), AND importance is `high`/`urgent`,
  **skip the drop** (deliver + index + archive). CLI: `toron mail overseer
  --to <agent> --body <...>` signs with the operator key, sets
  importance=high, adds the `toron:overseer` tag, and injects the preamble
  ("Operator override. Pause current work, handle this, then resume.").
- **Avoid**: name-based bypass (AM-Rust's `HumanOverseer` agent name is
  spoofable; toron's is npub-scoped — strictly stronger); bypass for
  non-operator keys (agents cannot unlock this); bypass without the
  `toron:overseer` tag (defense in depth — the operator key alone isn't a
  blanket bypass of all that key's messages, only intentional overseer ones).
- **Tests**: `overseer-bypasses-block_all` (operator-signed overseer msg to a
  `block_all` agent → delivered); `non-overseer-blocked` (agent-signed msg to
  `block_all` → still dropped); `overseer-requires-tag` (operator-signed msg
  WITHOUT the tag → normal policy applies, no bypass).
- **governed-by**: this ADR + ADR-0003 §operator key role + §G14 (policy at
  projection).

### Cross-ship: documentation + index

- **Affected paths**: `docs/decisions/README.md` (add ADR-0004 row),
  `docs/notes/2026-08-07-buzz-agentmail-comparison.md` (link this ADR from
  items 1–5), `README.md` (mention `doctor reconstruct` + `mail overseer` in
  the CLI surface once shipped).
- **Code ↔ ADR linking**: add `// governed-by: ADR-0004` to the top of each
  file touched by these ships.

## Verification

- [ ] **Ship 1 (coalescing, `bd-bzu`):** `burst-coalesce` test asserts 20
  mentions → 1 job within window; `lone-mention-immediate` asserts <window
  dispatch; SC-12 green; no kind-43001 wire-shape change. _Witness: `crates/toron-workflow/src/dispatch.rs`, `crates/toron-e2e/tests/sc_coalesce.rs` (new)._
- [ ] **Ship 2 (loop-prevention, `bd-nsv`):** `WORKFLOW_EMITS_KINDS` constant
  exists in toron-workflow and is sourced from the registry; trigger-ingress
  filter excludes 43001–06/46001–12/1059/`toron:workflow-emitted`;
  `self-send-message-workflow-terminates` regression test green; all workflow
  SC tests green. _Witness: `crates/toron-workflow/src/` (trigger filter), `crates/toron-e2e/tests/`._
- [ ] **Ship 3 (`doctor reconstruct`, `bd-jwa`):** `toron doctor reconstruct`
  on a corrupted SQLite → fresh working DB from Git; forensic bundle
  preserved at `<data-dir>/forensic-<ts>/`; bad DB quarantined (not deleted);
  `--dry-run` reports without mutating; `sc14a_dr_rebuild` still green. _Witness: `crates/toron-cli/src/operator.rs` (or `doctor.rs`), `crates/toron-core/src/index/rebuild.rs`._
- [ ] **Ship 4 (guard bounding, `bd-n87`):** guard evaluator wrapped in
  `tokio::time::timeout` (100ms default, `TORON_GUARD_TIMEOUT_MS`);
  `adversarial-guard-times-out` green (killed at ~100ms, not hung); 4 custom
  fns registered; `legitimate-guards-pass` green. _Witness: `crates/toron-workflow/src/` (guard evaluator)._
- [ ] **Ship 5 (Overseer, `bd-o1h`):** `overseer-bypasses-block_all` green
  (operator-signed + `toron:overseer` tag + high importance → delivered to
  `block_all` agent); `non-overseer-blocked` green (agent-signed → still
  dropped); `overseer-requires-tag` green (operator-signed without tag → no
  bypass); `toron mail overseer` CLI signs with operator key. _Witness:
  `crates/toron-core/src/mailbox/contacts.rs`, `crates/toron-cli/src/cli.rs`._
- [ ] **No ADR-0003 regression:** 38 tools / 25 resources unchanged (no
  add/remove/rename); kind registry unchanged; `verify-gates.sh` green;
  `clippy --workspace -D warnings` clean; full `cargo test --workspace`
  green; `#![forbid(unsafe_code)]` in all crate roots. _Witness: `crates/toron-mcp/src/tools.rs` (38 `#[tool]`), `crates/toron-mcp/src/resources.rs` (`exactly_25_resources`), `scripts/verify-gates.sh`._
- [ ] **All 5 beads closed with commit evidence** (`bd-bzu`, `bd-nsv`,
  `bd-jwa`, `bd-n87`, `bd-o1h`); this ADR marked fully-implemented in
  `## More Information` once the last bead closes.

## More Information

### Relationship to ADR-0003

This ADR **hardens** ADR-0003's outputs; it does not supersede any part of
it. ADR-0003's mailbox contract (38 tools / 25 resources / 9 clusters), frozen
kind registry, Tokio runtime, rmcp MCP server, local-first footprint, and
`#![forbid(unsafe_code)]` discipline are all preserved unchanged. Where this
ADR's ships touch ADR-0003-governed surfaces (the operator key role §G1, the
projection policy enforcement §G14, the DR two-source model §G11, the
@mention dispatch §Phase 2), they **extend** within the ADR-0003 frame, never
contradict.

### Relationship to the study

The comparative study (`docs/notes/2026-08-07-buzz-agentmail-comparison.md`,
bead `bd-0sc`) is the evidence base. This ADR's 5 ships are the study's
ranked items 1–5; the study's items 11 (ATC) and 12 (Forge) are this ADR's
deferred Non-Goals.

### Post-acceptance lifecycle

Each ship's bead closes with commit evidence (the toron convention since
`bd-j5v`). When the last of the 5 closes, this ADR's `## Verification`
checkboxes are walked, results recorded here in `## More Information` with a
date stamp, and the ADR is marked fully-implemented (status stays `accepted`;
ADRs in this repo don't transition to `implemented` — the verification
checkboxes are the implementation record).

### Revisit triggers

- **If ATC becomes the priority**: open a new ADR for the learning loop
  (Non-Goal 1). This ADR's ships don't pre-empt it.
- **If Forge becomes the priority**: open a new ADR for NIP-34 kind
  reservation + the git listener architecture (Non-Goal 2). This ADR's ships
  don't pre-empt it.
- **If coalescing's 2s window proves wrong in production**: tune via
  `TORON_COALESCE_WINDOW_MS` (no ADR needed — env-configurable by design).
- **If the operator-bypass is abused or needs tightening**: add a
  rate-limit or an audit-log entry for overseer bypasses (no ADR needed for
  the tightening; an ADR if the bypass semantics themselves change).

### References

- Study: [`docs/notes/2026-08-07-buzz-agentmail-comparison.md`](../notes/2026-08-07-buzz-agentmail-comparison.md)
- ADR-0003: [`0003-nostr-mailbox-fabric.md`](0003-nostr-mailbox-fabric.md)
- Buzz: https://github.com/block/buzz (studied at main HEAD, 2026-08-07)
- AM-Rust: https://github.com/Dicklesworthstone/mcp_agent_mail_rust (main HEAD)
- mcpagentmail.com: https://mcpagentmail.com/
- Beads: `bd-bzu`, `bd-nsv`, `bd-jwa`, `bd-n87`, `bd-o1h` (children of `bd-0sc`)

// governed-by: ADR-0004
