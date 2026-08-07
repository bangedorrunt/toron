// governed-by: ADR-0003, ADR-0005
#![forbid(unsafe_code)]

// bd-led.6.1: `Backend` trait — supervisor abstraction for agent panes.
// herdr is the default backend (ported in bd-led.6.2); tmux returns
// Unsupported until un-held (ADR-0003 G9).
//
// ---------------------------------------------------------------------------
// bd-p1-design-gn0 — P1.1 Unified Backend trait design record (ADR-0005 P1).
// Landed 2026-08-07 by EmeraldSwan (lane OC-B). This is the DESIGN record the
// migration bead bd-p1-unify-y9j implements; do not treat this comment as the
// trait itself (the unified surface lands in that bead).
//
// ## Problem
// Two structurally incompatible trait surfaces:
//   - flywheel `MultiplexerBackend` (flywheel/src/backend/mod.rs:204): SYNC,
//     `&mut self`, object-safe (`Box<dyn>`), 18 methods, every method
//     Unsupported-default except `name()`, ~20 real call sites.
//   - flywheel `MailBackend` (mod.rs:310): SYNC, `&mut self`, ~30 methods,
//     Unsupported-defaults. (Fate decided below.)
//   - this crate's `Backend` (below): ASYNC, `&self`, native AFIT, 6 methods,
//     NO defaults, NOT dyn-capable. VERIFIED: zero production callers through
//     the trait — concrete `HerdrBackend` is consumed via `.client()` only
//     (mailbox/identity.rs, toron-mcp, toron-cli); `MockBackend` only in the
//     e2e harness. The async trait is latent surface, not load-bearing.
// The herdr CLIENT is already unified (flywheel herdr_shim.rs:31 re-exports
// toron-core's `Client`); only the trait SURFACE is duplicated.
//
// ## Decisions
// 1. **Object-safety: YES (dyn-capable).** flywheel's factory returns
//    `Box<dyn MultiplexerBackend>` and ~20 call sites take `&dyn`. Cost:
//    trait methods use concrete types only — no generic methods, no async fns
//    (sync return types). Accepted; the only consumers are concrete.
// 2. **Receiver: `&self` + interior mutability.** Impls wrap their stateful
//    socket/HTTP clients in `parking_lot::Mutex`/`tokio::sync` (this crate's
//    `HerdrBackend` already does — `client: Mutex<Client>`). Call sites drop
//    `&mut dyn` → `&dyn` and `as_mut()`. Keeps the backend `Send + Sync`
//    shareable across threads.
// 3. **Execution model: SYNC, not async.** The bead framed the cost as
//    "no-generic-async-fns", but the binding constraint is that every real
//    call site is a SYNC CLI command / hidden subcommand (cmd_debate,
//    cmd_plan_review, __send, __mail-blocked, find_workspace_dyn,
//    resolve_agents_dyn, settle_agent); the durable loop isolates blocking
//    herdr calls behind `std::process::Command` subprocesses. An async trait
//    would force a tokio runtime into every command entry — far beyond the
//    ~20-site migration. toron-core's async `Backend` (zero production
//    callers) is replaced by the unified sync trait.
// 4. **Method set: SUPERSET = flywheel's 18 (name + workspace/pane/agent/
//    prompt_coded/subscribe).** This crate's 6 (spawn/prompt/wait/read/
//    send_keys/events) map onto the richer flywheel shapes: prompt →
//    prompt/prompt_coded, wait → agent_wait, read → agent_read, send_keys →
//    agent_send_keys, events → subscribe. P2's profile-spawn adds a
//    SpawnSpec-carrying signature when bd-p2-resolve-ntt lands (open item).
// 5. **Defaults: Unsupported-open (ADR-0002 §6).** Every method except
//    `name()` has a default body returning `BackendError::Unsupported`
//    naming the backend + method. Future backends inherit Unsupported for
//    ops they don't implement — not sealed.
// 6. **Error type: this crate's `BackendError`** (machine-code typed,
//    ADR-0003 classify-on-code). The unified trait lives in toron-core so it
//    CANNOT return flywheel's `BackendError` (dep inversion). flywheel's
//    local `BackendError{Unsupported{backend,method,reason}, Other}` is
//    deleted; pattern-matches migrate to `err.code()`.
// 7. **`MailBackend`: EXPLICITLY DEFERRED (not merged).** MailBackend is a
//    separate domain — 30 AM-parity coordination ops (send/reply/inbox/ack/
//    reserve/search), no toron-core twin to unify, and merging would create
//    a ~50-method god-trait hostile to implementers and to Unsupported-noise.
//    It stays in flywheel as a distinct trait. A future phase MAY move it to
//    toron-core as a second `MailBackend` trait, but P1 does NOT merge it.
//    ADR-0005 invariant 4 ("fate decided, not silently half-scoped") is
//    satisfied by this explicit decision.
//
// ## Migration shape (implemented by bd-p1-unify-y9j)
//   - toron-core: rewrite `Backend` below to the sync, `&self`, dyn-capable,
//     Unsupported-open surface above; rework HerdrBackend/MockBackend/Tmux
//     impls to sync.
//   - flywheel: delete local `MultiplexerBackend`/`MailBackend` definitions
//     (keep re-export shim if back-compat needs it); HerdrBackend/AgentMail
//     impls move onto the unified trait; ~20 call sites `&mut dyn` → `&dyn`.
//
// ## Captain review gate
// bd-p1-unify-y9j must NOT start until this record is accepted (ADR-0005 P1:
// "Reviewed by the captain before p1-unify starts").
// ---------------------------------------------------------------------------

pub mod error;
pub mod herdr;
pub mod mock;
pub mod tmux;
pub mod types;

pub use error::{BackendError, BackendErrorCode};
pub use types::{AgentStatus, BackendEvent, PaneId, SpawnOptions};

use tokio::sync::mpsc;

/// In-flight backend event stream. Backends push `BackendEvent`s here.
pub type EventStream = mpsc::Receiver<BackendEvent>;

/// Supervisor backend: spawn/prompt/wait/read/send_keys/events.
/// Errors are typed with machine codes — classify on `BackendError::code`,
/// never message text.
///
/// Native AFIT (NOT `async_trait`): `Backend` is never used as a trait
/// object (consumers call the concrete `HerdrBackend`/`MockBackend`), so the
/// boxing async-trait adds would be pure overhead. Futures are Send by
/// construction: `Self: Send + Sync` makes the `&self` capture Send, all args
/// are Send, and no impl holds a non-Send guard across an await point. No
/// caller requires a nameable Send bound on these futures (they are awaited
/// inline, never spawned cross-thread).
#[expect(
    async_fn_in_trait,
    reason = "Backend futures are Send by construction (Self: Send+Sync, Send args, no non-Send guard across await); no dyn usage; no caller needs a nameable auto-trait bound."
)]
pub trait Backend: Send + Sync {
    /// Spawn an agent pane in the backend and return its pane id.
    async fn spawn(&self, opts: &SpawnOptions) -> Result<PaneId, BackendError>;

    /// Send a prompt/input line to the pane.
    async fn prompt(&self, pane: &PaneId, input: &str) -> Result<(), BackendError>;

    /// Wait for the agent to reach a terminal-ish state; returns current status.
    async fn wait(&self, pane: &PaneId) -> Result<AgentStatus, BackendError>;

    /// Read buffered output from the pane.
    async fn read(&self, pane: &PaneId) -> Result<String, BackendError>;

    /// Inject raw key/control sequences.
    async fn send_keys(&self, pane: &PaneId, keys: &str) -> Result<(), BackendError>;

    /// Subscribe to backend push events.
    async fn events(&self) -> Result<EventStream, BackendError>;
}
