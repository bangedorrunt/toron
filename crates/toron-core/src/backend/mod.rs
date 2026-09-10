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
pub mod wait;

pub use error::{BackendError, BackendErrorCode};
pub use types::{
    AgentStatus, BackendEvent, PaneId, PromptLanding, SpawnOptions, normalize_herdr_status,
};
pub use wait::{ACTIVITY_GATE_MS, PromptHost, PromptTarget, WaitOutcome, prompt_wait};

use std::time::Duration;

use serde_json::Value;

/// Sync push-event stream yielded by [`Backend::subscribe`] (ADR-0005 P1).
///
/// `read_event` blocks until a push arrives (or the stream closes →
/// `Ok(None)`); `set_read_timeout` bounds each blocking read (default: no-op
/// — a backend without per-read timeouts leaves them unbounded).
pub trait EventStream: Send {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError>;
    fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), BackendError> {
        Ok(())
    }
}

/// Unified supervisor backend (ADR-0005 P1) — the ONE backend trait, by
/// design. Superset of flywheel's former `MultiplexerBackend` (18 methods:
/// workspace/pane/agent/subscribe) and this crate's former async `Backend`
/// (spawn/prompt/wait/read/send_keys/events — mapped onto the richer shapes:
/// spawn→agent_start, prompt→prompt/prompt_coded, wait→agent_wait,
/// read→agent_read, send_keys→agent_send_keys, events→subscribe).
///
/// - **SYNC + `&self` + object-safe** (`Box<dyn Backend>`): every real call
///   site (flywheel CLI commands + hidden subprocess leaves) is sync; impls
///   use interior mutability for their stateful clients. No async fns, no
///   generic methods — dyn-capable.
/// - **Unsupported-open (ADR-0002 §6):** every method except `name()` has a
///   default body returning `BackendError` with `BackendErrorCode::Unsupported`
///   naming the backend + method. Future backends inherit `Unsupported`.
/// - **Errors are machine-code typed** (`BackendError::code`, classify on
///   code — ADR-0003). flywheel's local `BackendError{Unsupported, Other}`
///   was deleted; pattern-matches migrate to `err.code()`.
pub trait Backend: Send + Sync {
    /// Stable backend identifier ("herdr" | "superlogical" | "tmux" | ...).
    fn name(&self) -> &'static str;

    // ---- workspace ----
    fn workspace_list(&self) -> Result<Vec<herdr::WorkspaceInfo>, BackendError> {
        Err(unsupported(self.name(), "workspace_list"))
    }
    fn workspace_create(&self, _label: Option<&str>) -> Result<herdr::WorkspaceInfo, BackendError> {
        Err(unsupported(self.name(), "workspace_create"))
    }
    fn workspace_close(&self, _workspace_id: &str, _close_group: bool) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "workspace_close"))
    }

    // ---- pane ----
    fn pane_split(
        &self,
        _parent_pane_id: &str,
        _direction: &str,
        _cwd: Option<&str>,
    ) -> Result<herdr::PaneInfo, BackendError> {
        Err(unsupported(self.name(), "pane_split"))
    }
    fn pane_list(&self, _workspace_id: Option<&str>) -> Result<Vec<herdr::PaneInfo>, BackendError> {
        Err(unsupported(self.name(), "pane_list"))
    }
    fn pane_close(&self, _pane_id: &str) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "pane_close"))
    }
    fn pane_run(&self, _pane_id: &str, _command: &str) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "pane_run"))
    }
    fn pane_wait_output(
        &self,
        _pane_id: &str,
        _needle: &str,
        _timeout_ms: u64,
    ) -> Result<Value, BackendError> {
        Err(unsupported(self.name(), "pane_wait_output"))
    }

    // ---- agent ----
    fn agent_list(&self) -> Result<Vec<herdr::AgentInfo>, BackendError> {
        Err(unsupported(self.name(), "agent_list"))
    }
    fn agent_start(
        &self,
        _name: &str,
        _kind: &str,
        _pane_id: &str,
        _args: &[String],
    ) -> Result<herdr::AgentInfo, BackendError> {
        Err(unsupported(self.name(), "agent_start"))
    }
    fn agent_rename(&self, _target: &str, _new_name: Option<&str>) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "agent_rename"))
    }
    fn prompt(
        &self,
        _target: &str,
        _text: &str,
        _wait: Option<&herdr::WaitOptions>,
    ) -> Result<Value, BackendError> {
        Err(unsupported(self.name(), "prompt"))
    }
    /// Coded `agent.prompt` — landing data (status XOR protocol code), so a
    /// backend-agnostic caller can classify like the herdr client
    /// (ADR-0002 review-fix F6).
    fn prompt_coded(
        &self,
        _target: &str,
        _text: &str,
        _wait: Option<&herdr::WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        Err(unsupported(self.name(), "prompt_coded"))
    }
    fn agent_wait(
        &self,
        _target: &str,
        _until: &[AgentStatus],
        _timeout_ms: Option<u64>,
    ) -> Result<herdr::AgentInfo, BackendError> {
        Err(unsupported(self.name(), "agent_wait"))
    }
    fn agent_read(&self, _target: &str, _source: &str, _lines: u32) -> Result<Value, BackendError> {
        Err(unsupported(self.name(), "agent_read"))
    }
    fn agent_send_keys(&self, _target: &str, _keys: &[&str]) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "agent_send_keys"))
    }

    // ---- events ----
    fn subscribe(
        &self,
        _filters: &[herdr::Subscription],
    ) -> Result<Box<dyn EventStream>, BackendError> {
        Err(unsupported(self.name(), "subscribe"))
    }
}

/// Default-body helper: an Unsupported machine-code error naming the backend
/// + method. Free fn (not a trait method) so `Backend` stays dyn-compatible.
fn unsupported(backend: &'static str, method: &'static str) -> BackendError {
    BackendError::unsupported(format!("{backend} does not implement {method}"))
}
