# ADR-0031: Herdr Runtime Study — Pane/Agent Split, Evidence-Based Detection, Frozen Contracts, Elm Decision Cores, Superlogical-Proof Seams

**Status:** Accepted (captain word 2026-09-10: operator message `accepted and close all beads`; all slices S1–S5 plus xstate steals landed green, closed bd-r1si, bd-ucjx, bd-nc5d, bd-i7bh, bd-hie0, bd-mvf1, bd-ii04, bd-eg09)
**Date:** 2026-09-10
**Decision-makers:** Captain (bangedorrunt)
**Extends:** [ADR-0002](adopt-agent-adapter-architecture.md) (backend trait), [ADR-0003](0003-nostr-mailbox-fabric.md) (mailbox fabric), [ADR-0005](0005-orchestration-layer.md) (orchestration tier), [ADR-0014](0014-kind-registry-specification.md) (kind registry), [ADR-0020](0020-nostr-two-plane-cli-and-crew-steering.md) (two-plane surface), [ADR-0027](0027-one-workspace-tick-control-plane.md) (one workspace tick), [ADR-0028](0028-jido-behavior-first-on-wheelhouse-nouns.md) (behavior-first contracts), flywheel [ADR-0012](../../flywheel/docs/decisions/0012-doorbell-plane-independent-runtime.md) (doorbell plane D, accepted rev 3 — this ADR is its toron-side companion; toron carries, doorbell drives, neither duplicates the other)
**Does not amend:** frozen MCP tool surface (40), kind registry spellings, one-way dep flywheel → toron, beads as work graph, doorbell as in-process wake, 30013 as agent profile.
**Tracker:** beads. Never pull requests as the log.

> **Scope:** what we steal from herdr 0.9.0, where the Elm/reducer pattern is law and where it is forbidden, and why every adoption below survives a future Superlogical backend. This record is counsel. Slices ship as green.

---

## 1. Source studied

herdr (`github.com/herdrdev/herdr`), version 0.9.0, cloned 2026-09-10. Read: `AGENTS.md` (universal rules + maintainer workflow), `README.md`, docs `concepts / agent-automation / socket-api / plugins`, `skills/herdr/SKILL.md`, `src/` layout (`app/`, `detect/manifests/`, `persist/`, `protocol/`, `server/`, `platform/`, `cli/`), `justfile`, `Cargo.toml`. Live cross-checks against our tree: flywheel `src/backend/` (unified `Backend` trait, `superlogical.rs` stub), `crates/doorbell-rt` (`msg.rs`, `host.rs`, `brain.rs`), `crates/doorbell-core` (`lib.rs`), toron `crates/toron-core/src/{bus,backend,mailbox}`.

---

## 2. Concepts learned (the mental model)

**2.1 Workspace > Tab > Pane; Agent is an ephemeral occupant, never a location.** A pane is a real terminal and always exists. An agent is the recognized process currently inside it, addressed by name or pane ID. Names match `[a-z][a-z0-9_-]{0,31}`, are unique among live agents, follow the current occupant, and are cleared on exit, release, or replace. Closed tab/pane IDs are never reused; a moved pane gets a new workspace-qualified ID.

**2.2 Server owns runtime; the TUI is one client.** Detach never stops work. Multiple clients may view different workspaces/tabs; same-tab contention resolves to last-interacting client. A session is a persistent server namespace; workspaces are the project container inside it. Our mapping: herdr is the execution plane, toron the coordination plane, beads the ledger plane. Never merge them.

**2.3 Lifecycle is five states with a seen-split.** `working` (active), `blocked` (needs input/approval), `done` (finished, unseen), `idle` (finished, seen), `unknown` (present, unclassifiable). `done` vs `idle` is server-seen vs client-seen: explicit focus marks seen, reads never do, each client tracks its own badge. `blocked` is an await-signal, not do-not-disturb. `unknown` never proves completion.

**2.4 Three primitives, never mixed.** Layout (workspace/tab/pane topology) creates locations. Pane commands control raw terminals (`run`, `send-text`, `send-keys`, `wait-output`). Agent commands control interpreted lifecycle (`start`, `prompt`, `wait`, `send-keys`, `read`). `agent start` requires an available shell pane at its prompt and never creates layout. A prompt that sent bytes is not a prompt that started work.

**2.5 Detection is decoupled and evidence-based.** The detector reads only a bottom-buffer snapshot, never parser or viewport state. Rules live in versioned per-agent TOML manifests with explicit AND/OR gates, priority, and regions (`osc_title`, `bottom_non_empty_lines(12)`). Tuning loop: capture snapshot → encode invariant controls → hot-reload → committed manifest stays source of truth. Never match whole-pane incidental text; never use the user-scrollable viewport for status.

**2.6 Plugin model: the entire CLI is the plugin API.** A plugin is a directory with a `herdr-plugin.toml` manifest plus argv commands; no separate SDK, no restricted command set. Portability via env-injected binary path (`HERDR_BIN_PATH`). Manifest validation + per-plugin config/state dirs; code itself unsandboxed, trust is explicit.

---

## 3. Techniques learned (steal these)

**3.1 Prompt-wait activity gate.** After submission herdr waits up to 5s to observe `working` or `blocked`; unrelated `idle`/`done`/session churn never satisfies it. No activity → `agent_prompt_stalled`; caller timeout first → `timeout`. Neither proves non-delivery: the rule is **read before retry, never blind resubmit**.

**3.2 Occupant-pinned waits.** `agent.wait` resolves the pane occupant once; a replacement process cannot satisfy it. `agent.prompt --wait` submits plus waits in one socket call (no race between two calls). Already-`blocked` → `agent_blocked` fail-fast before sending any bytes.

**3.3 Alt-screen honesty.** Full-screen agents render in the alternate screen, not host scrollback. Over-limit reads drive the agent's mouse-scroll interface (collect overlapping pages, restore viewport to bottom) and return `agent_not_idle` while working/blocked/unknown. Hard fallback: ask the agent to dump Markdown to a temp dir and read the file. Never pretend scrollback holds what it never captured.

**3.4 Subscribe-then-snapshot bootstrap.** Open `events.subscribe` on a second connection, buffer, then call `session.snapshot`, install, replay buffered events in order. Snapshot is one-shot, never a subscription; re-snapshot after reconnect or suspected staleness.

**3.5 Endpoint generation contract (Gen-1 frozen).** Named codecs immutable; new JSON fields optional or defaulted; new enums need an `Unknown` fallback; a missing method disables only that action, never the connection; typing never dismisses notices. Enforced by frozen fixtures, bincode digests, and a method-shapes fixture. Our `catalog_freshness` is the same instinct at smaller scope.

**3.6 Multiplicative perf discipline.** Every addition names frequency × cardinality (per byte/event/render × panes/tabs/workspaces × clients). Rules: narrow accessors, minimal core-lock hold, hidden-source early exits, profile at 1 and 15+ populated panes (`bench-render-scale`). Benchmarks are supporting evidence; deterministic behavior tests are the gate.

**3.7 Caller-context injection.** Each managed pane inherits `HERDR_WORKSPACE_ID` / `HERDR_TAB_ID` / `HERDR_PANE_ID`; `--current` targets the caller, never another client's focused pane. Creation responses return the IDs to use next; consumers parse them from JSON, never derive from sidebar order.

---

## 4. Code/design patterns learned

**4.1 State separated from runtime; render pure.** `AppState` is pure data (testable without PTYs); `PaneState` split from `PaneRuntime`; `compute_view()` owns geometry and mutation, `render()` only draws. Tests construct via `AppState::test_new()` / `Workspace::test_new()` plus adversarial identity state and `assert_invariants_for_test()`.

**4.2 Runtime/client boundary guardrail.** New state is classified before it is added: shared runtime fact → server state + JSON/event path; presentation → TUI/client only. Neutral server names; no UI-surface names (`sidebar`, `card`, `widget`) in the protocol. No new shared behavior that works only through a private client socket.

**4.3 Platform isolation.** OS behavior lives in `src/platform/<os>.rs`; core holds only shared traits and testable contracts; no `#[cfg(target_os)]` in core modules.

**4.4 Vendored-dep patch ledger.** Base commit recorded (`*.vendor.json`); each local patch is a file plus a ledger entry stating why, upstream link, touched files, verification, and exact removal condition. Update = re-check every active patch against the new base.

**4.5 justfile + contract tests.** `just test/check/lint` as the only entry; Python `unittest` maintenance tests enforce agent-detection manifests, changelog, config reference, translation parity, installer, and vendor invariants. Docs-contract tests run in CI, not in heads.

**4.6 Skill-sync contract.** Adding a command updates all of: CLI enum, MCP/tool surface, AGENTS.md, skill file, tool-count constant, catalog regen. A command not in the skill is invisible to agents.

---

## 5. Idioms learned

Single Rust binary, no Electron. `ratatui` + `portable-pty` + `tokio`, `clap` CLI, `serde` JSON everywhere, schema-printable API (`api schema`). Agent entry contract: `HERDR_ENV=1` gate, `--current` over focused pane, `--no-focus` default for background work, JSON-syntax-errors exit 2 / server-errors exit 1 with JSON on stderr, never `server stop` from inside a live session.

---

## 6. Decision — adopt (A1–A8) and refuse (N1–N4)

**A1 — Pane/Agent split as a cross-stack fence.** Toron mail is interpreted coordination; herdr pane is raw execution; beads is the ledger. Flywheel prompt paths must route through agent-surface (validated identity + lifecycle) or declare raw-terminal intent explicitly. No byte-send that claims turn-start without lifecycle proof.

**A2 — Done-vs-seen split for bead close vs captain ack.** Close is terminal (beads gate + SHA, fail-closed, unchanged). Seen is per observer: the `[bd-*] done` mail is the captain's copy, the close reason is the durable copy (already our convention; now with herdr semantics — reads never mark seen, explicit focus/ack does). Kills the "looks compliant" class of incident.

**A3 — Activity gate + read-before-retry on every prompt-wait.** Port §3.1–§3.2 into flywheel prompt and toron dispatch: 5s post-submit activity observation, `stalled` distinct from `timeout`, occupant pinning, `blocked` fail-fast, one-call submit-plus-wait. See §9 code.

**A4 — Versioned detection manifests for presence.** Move toron presence classification out of inline heuristics into per-kind TOML rules (priority + region + AND/OR gates), hot-reloadable, evidence-captured from pane snapshots. Normalizer maps every backend dialect to the five canonical states before any reducer sees them.

**A5 — Pure decision cores with host traits (Elm-everywhere, §7).** New decision logic lands as `tick(Model, Msg) -> (Model, Vec<Effect>)` with `MockHost` replay tests. Existing doorbell-core and `workspace::tick` are the reference; toron dispatch and beads-gate transitions are next.

**A6 — Frozen-contract discipline extended.** Keep `catalog_freshness`; add a method-shapes fixture plus wire-digest test so additive tools cannot silently reshape parity tools. New fields optional/defaulted; new enums get `Unknown`; missing capability disables one action, never the session.

**A7 — CLI-is-the-API for plugins and workflows.** Flywheel workflows call toron CLI verbs; toron exposes `TORON_BIN_PATH`-style portability; no second SDK. Skill-sync checklist (§4.6) becomes our own gate for every new verb.

**A8 — Test constructors + adversarial identity.** `MailSurface` and dispatch cores get `test_new()` constructors free of daemon/Nostr/PTY, plus adversarial pin/reservation fixtures and invariant asserts, mirroring herdr's `test_with_adversarial_identity_state()`.

**N1 — Do not reimplement PTY ownership, multiplexing, or remote persistence.** Herdr (now) or Superlogical (later) owns terminals. We integrate downward through the `Backend` trait.

**N2 — Do not copy the mouse-first TUI, kitty-graphics layers, or per-client badge machinery.** Server-seen vs client-seen semantics yes; pixel pipelines no.

**N3 — Do not put I/O, crypto, or transport inside any reducer.** Effects stay behind host traits. A reducer that awaits is a bug.

**N4 — Do not mint new Nostr kinds, MCP tools, or profile types for any adoption above.** Presence store, receipts table, 30013 profile, and existing CLI nouns already cover A1–A8.

---

## 7. Elm-everywhere ruling (the doorbell question)

Adopt the reducer as law **for decision cores only**. The proven shape in our tree (`doorbell-rt/src/msg.rs`, `host.rs`, `doorbell-core/src/lib.rs`):

```rust
// All state changes enter as messages. Wall-clock enters only as payload.
pub enum BrainMsg {
    Watch(String),
    HerdrStatus { pin: String, status: String },
    InboxFetched { pin: String, rows: Vec<serde_json::Value> },
    SlaFired { posts: Vec<String> },
    Degraded { source: String, reason: String },
    Ask { kind: AskKind, reply: tokio::sync::oneshot::Sender<AskReply> },
    Shutdown,
}

// Intents the brain journals; the host executes. No I/O in tick.
pub enum Effect {
    Prompt { pin: String, line: String },
    FetchInbox { pin: String, since: String },
    CheckAckSla,
    RestartWatch,
    Audit { kind: String, detail: serde_json::Value },
}

pub trait Host: Send {
    fn tick(&mut self, msg: &BrainMsg) -> Vec<Effect>; // pure decision
    fn now_unix(&self) -> u64;                          // clock as payload
    fn fetch_inbox(&mut self, pin: &str, since: &str) -> Vec<serde_json::Value>;
    fn scan_ack_sla(&mut self) -> Vec<String>;
    fn restart_watch(&mut self);
}
```

Law: one `Model`, one `Msg` union, one `tick`, effects as data, journaled before execution, replayable (`same Msg log → byte-identical Effects`, cf. `brain.rs` determinism proof). Forbidden: awaiting I/O inside `tick`, second reducers for the same Model, side effects that bypass the journal. Slices compose as `pub(crate)` reducers under one entry (Tick Law, ADR-0027).

---

## 8. Superlogical-proof analysis

Every adoption above survives a Superlogical backend because each was placed above the seam that changes:

| Adoption | Lives where | Superlogical impact |
| :--- | :--- | :--- |
| A1 Pane/Agent split, A2 done-vs-seen, A3 activity gate + pinning | Reducer + `Backend` trait consumers | None. Only adapter method bodies change. |
| A4 detection manifests | Backend adapter (herdr dialect today) | New dialect file, same five normalized states out. |
| A5 Elm cores, A8 test constructors | Pure crates, no backend imports | None by construction (compile-enforced). |
| A6 frozen contracts, A7 CLI-is-API | Tool/catalog surface | None; Superlogical arrives as a backend impl, not a tool. |
| §3.4 snapshot bootstrap, §3.7 caller context | Dispatcher + adapter | Re-implement against the new SDK once; semantics unchanged. |

Two leak risks must be fixed before Superlogical lands, or they become migration debt:

1. **Pane-ID leakage.** Workspace-qualified IDs (`w1:p1`) must never enter toron receipts or bead reasons as strings. Wrap in an opaque `PaneId` newtype at the adapter edge (§9).
2. **Status-dialect leakage.** Raw backend strings (`idle`, herdr-specific text) must never enter a reducer. The A4 normalizer is the only importer of dialect; everything downstream sees the canonical five.

Graduation plan (already reserved): `FLYWHEEL_BACKEND=superlogical` + `backend-superlogical` feature + `superlogical.rs` stub → real impl when the SDK ships (ADR-0002 R2). Stub today returns honest `Unsupported`, never silent fallback to herdr. No ADR-0031 slice may depend on herdr-specific IDs, status text, or socket shapes.

---

## 8.5 — Companion contract with flywheel ADR-0012 (under implementation)

Flywheel ADR-0012 (accepted rev 3, Phase 0 landed via `bd-asr2`: `doorbell-core` / `doorbell-rt` / `doorbell-bin` crates exist) spins doorbell out as plane D, an always-on daemon. This ADR is its toron-side companion. The load-bearing rule, rejected as Option D on both sides: **wake policy never moves into toron, mailbox logic never moves into doorbell**. Concretely:

- **toron-core stays a leaf library doorbell-rt consumes.** `doorbell-rt` depends on toron-core `bus`/`mailbox`/`receipts` as a library and parses wire shapes from `serde_json::Value`; toron-core gains zero flywheel/doorbell types and zero new deps under S1–S5. Any slice that would add either is rejected in review.
- **Exactly-once + store-replay primitives stay frozen.** ADR-0012 §2.2 builds its crash-resume story on toron semantics: `subscribe_trusted` snapshot + delta + EOSE, `bus_events` sqlite (100k cap / 90d prune) as the cross-process share, receipts `owed→acked→resulted` monotonic, 20001 never stored with a 60s-TTl broker view, channel-full publisher-drop + counter with store replay. S1–S5 change none of these; reviewers must flag any behavioral touch as out of scope.
- **Read-only observation for the Phase-4 beads-poller.** ADR-0012 D7 fills its producer gap with a read-only WAL poll of br sqlite. Toron-side guarantee needed and already true: receipts/bus sqlite run WAL + `busy_timeout`, so a concurrent read-only poller never blocks writers. No toron code change; the acceptance is a contention test (poller at 30s cadence under write load, zero blocked-writer timeouts), owned by the flywheel slice.
- **No parallel wake journal, no wake kind.** ADR-0012 D10 forbids a second bus and any 30015-style wake kind; `emit` publishes through toron as an edge. Toron mints nothing wake-specific; the doorbell journal (v1 now, v2 with subscriber scoping later) is flywheel-owned. Our S5 fixtures cover the tool surface only and never shadow the journal envelope.
- **Single-writer fence is flywheel-enforced, toron-compatible.** The D9 lease + bind fence + embedded deference guarantees one loop per workspace; toron contributes nothing new beyond the frozen primitives above. Identity resolution (30013 + whois + spawn-registry-as-one-resolver) requires toron to keep whois/30013 stable with no second profile, which is exactly our N4.
- **Sequencing.** S1 (opaque IDs + normalizer) should land before flywheel Phase 2 (human digest subscriptions), because cross-identity delivery is where a leaked herdr string would do the most damage. S3 (pure dispatch core) may proceed in parallel with flywheel Phase 1; both copy the same Phase-0 verbatim-move pattern and neither touches the other's journal.

## 9. Implementation blueprint (slices, each independently shippable)

**S1 — Opaque backend IDs + status normalization (closes both leak risks).**

```rust
// toron-core/src/backend/types.rs — no herdr imports allowed.
pub struct PaneId(String); // opaque; adapters construct, cores only compare/clone
pub enum AgentState { Working, Blocked, Done, Idle, Unknown }

pub fn normalize_herdr_status(raw: &str) -> AgentState {
    match raw {
        "working" => AgentState::Working,
        "blocked" => AgentState::Blocked, // await-signal, not do-not-disturb
        "done" => AgentState::Done,       // finished + unseen
        "idle" => AgentState::Idle,       // finished + seen
        _ => AgentState::Unknown,         // never proves completion
    }
}
```

**S2 — Activity-gated, occupant-pinned prompt-wait.**

```rust
pub enum WaitOutcome { Settled(AgentState), Stalled, Blocked, Timeout }

pub fn prompt_wait(
    host: &mut impl PromptHost,
    target: &PromptTarget, // pinned occupant at submit time
    line: &str,
    timeout_ms: u64,
) -> WaitOutcome {
    if host.state(target) == AgentState::Blocked { return WaitOutcome::Blocked; }
    host.submit(target, line); // text + encoded Enter, one ordered write
    if !host.observe_activity(target, 5_000) { return WaitOutcome::Stalled; }
    host.await_settled(target, timeout_ms) // replacements cannot satisfy
}
// Caller rule (enforced in docs + CLI help): Stalled/Timeout → read → decide.
// Never blind resubmit.
```

**S3 — Toron dispatch as a pure core.** Extract `classify()` / inject-gate / queue-coalesce (last-write-wins per class) into a dependency-free module, copying the ADR-0012 Phase-0 pattern: verbatim move, no signature changes, imports become crate paths, additive version fields only. The module must build standalone with no `tokio`, no Nostr client types, no flywheel/doorbell imports — enforced by a `verify` fence in the slice's fence script, mirroring `verify-0012.sh` Phase-0 checks. Thin host executes `Prompt / FetchInbox / CheckAckSla / Audit`. Determinism test:

```rust
#[test]
fn same_msg_log_byte_identical_effects() {
    let log = scripted_msgs(); // Watch, HerdrStatus, InboxFetched, SlaFired…
    let a = replay(&log);
    let b = replay(&log);
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}
```

**S4 — Detection manifest (example rule shape, TOML).**

```toml
id = "toron-kind"
version = "2026.09.10.1"

[[rules]]
id = "blocked_approval"
state = "blocked"
priority = 1000
region = "bottom_non_empty_lines(12)"
any = [{ contains = ["approve"] }, { contains = ["waiting for permission"] }]
not = [{ contains = ["esc to cancel"] }]
```

**S5 — Contract fixtures.** Add `tests/fixtures/tool-method-shapes-v1.json` + digest test beside `catalog_freshness`: any wire/shape drift without regen fails red.

**Blast radius.** S1 touches the adapter edge only. S2 touches prompt/wait paths. S3 is new-module extraction; effect layer untouched. S4 is additive files + reload verb. S5 is test-only. Forbidden zones: kind registry, MCP tool count, 30013 profile, beads close table.

---

## 10. Verification

- `mbx test --workspace` green; new determinism test (S3) + normalizer unit tests (S1) + prompt-wait scripted-host tests (S2) all adjacent to code.
- `mbx clippy --workspace --all-targets -- -D warnings`, `mbx fmt --check`.
- `mbx test catalog_freshness` + new shapes-fixture test green on purpose-drifted trial (prove the gate bites).
- Cross-backend check: full suite passes with `FLYWHEEL_BACKEND=herdr`; identical decision-core tests pass with the Superlogical stub (all backend ops honest-`Unsupported`, zero reducer failures).
- Companion check (flywheel ADR-0012): no toron slice duplicates `doorbell-rt` behavior or shadows its journal envelope; `toron-core` still builds as a dependency-free leaf (no flywheel/doorbell imports); read-only poller contention trial green (zero blocked-writer timeouts under 30s-cadence poll + write load).
- `br` slices filed per S1–S5 with `verify` fences naming the exact commands above; close per ledger rules (gate row + SHA citing the bead).

---

## 11. Risks and non-goals

- Over-fitting to herdr socket shapes: contained by S1/S4 adapter quarantine; reviewers must reject any herdr string crossing into cores.
- Reducer zeal: PTY bytes, crypto, and transport stay imperative; forcing them into `tick` would destroy the determinism property this ADR exists to gain.
- Manifest sprawl: one rule file per backend dialect, capped; generic regexes without `not` guards are rejected in review (herdr's evidence-based tuning loop is mandatory, not advisory).
- Non-goals: no new kinds/tools/profiles, no TUI work, no remote-persistence work, no Superlogical SDK implementation (stub stays until upstream ships).

---

## 12. Expected benefits if implemented

- **Wakes that can be trusted (A3, S2).** The activity gate plus occupant-pinned waits make a stalled prompt report `stalled`, never silently resubmit, and a replacement process can never satisfy another wait. Fewer duplicate prompts, fewer blind retries compounding them.
- **Presence that stops lying (A4, S4).** Versioned detection manifests plus the five-state normalizer move lifecycle classification out of inline heuristics into hot-reloadable rules with evidence snapshots. One dialect file per backend, one truth downstream; a new agent kind becomes a TOML file, not a code change.
- **Closes that mean something (A2).** The done-vs-seen split keeps the ledger close terminal and fail-closed while the captain's copy becomes explicitly per-observer. The "looks compliant" failure class (2026-09-09 solo lane) gets structurally harder, not just procedurally discouraged.
- **Interchangeable backends (A1, S1, §8).** Opaque IDs plus status normalization quarantine every backend dialect at the adapter edge, so a future backend changes method bodies, never reducers. The swap is proven by replay tests, not by a staging weekend.
- **Coverage no human writes (A5, S3).** Model-based path generation turns the dispatch core's own transition table into its test suite: illegal transitions are caught by generated paths instead of production incidents, and "same log, same effects" becomes a CI fact alongside the envelope conformance test (S5).
- **Immediate, zero-risk half (A6, A7).** The AGENTS.md conventions block and VERIFY honesty already landed: every future bead inherits them, and report beads can no longer borrow test gates. That half pays rent from today with no implementation risk.

---

## Amendment A1 (2026-11-22) — MCP method-shape regen (bd-reqp4)

`mbx test -p toron-mcp --test catalog_shapes` was red on HEAD with 22 drifted
shapes. Frozen fixture `crates/toron-mcp/tests/fixtures/tool-method-shapes-v2.json`
was regenerated with `TORON_REGEN_SHAPES=1` after this classification.

**Evidence.** Tool count stays 41 (38 parity + 3 additive), resource shapes are
byte-identical, and the ONLY input-schema change across all 41 tools is the
`queue` boolean on `file_reservation_paths`. Every other drift is an
`output_schema` delta: either a regenerated `$defs` body or a `required` list
now naming fields already present in the response type. No parity tool's call
contract (name, params, types, required-ness of inputs) changed meaning.

**Generator-only (rmcp 3.11 unification, bd-6zo01; no wire meaning change):**
`acquire_build_slot`, `check_file_reservation_conflicts`, `fetch_inbox`,
`fetch_inbox_product`, `file_reservation_paths` (output `$defs` half only),
`list_agents`, `list_contacts`, `macro_file_reservation_cycle`,
`macro_start_session`, `renew_build_slot` (shape), `renew_file_reservations`,
`search_messages_product`. These differ only in generated `$defs` bodies and
inferred `required` lists; no input field moved.

**Intentional, named:**
| Tool | Change | Bead |
|---|---|---|
| `file_reservation_paths` | `queue: boolean` input; FIFO waitlist acquire | bd-ll7bs |
| `list_file_reservations` | `waitlists: [{path, waiters}]` output | bd-ll7bs |
| `acknowledge_message`, `mark_message_read` | `acknowledged_at_us` now required in output | bd-fhf0 / bd-wave5-tojson-core-x7kj |
| `request_contact`, `respond_contact`, `macro_contact_handshake` | `to_project` / `link` now required in output | bd-fhf0 |
| `whois`, `list_agents` (WhoIsOut def) | pane identity/program/model fields now required in output | bd-fhf0 |
| `resolve_pane_identity`, `cleanup_pane_identities` | resolution/cleanup result fields now required in output | bd-fhf0 |
| `search_messages` | `next_cursor` now required in output | 627f5a4a (SEP-2106 outputSchema) |

The fixture is the reviewed contract for this revision; S5's tamper witness
still fails on a one-field change.
