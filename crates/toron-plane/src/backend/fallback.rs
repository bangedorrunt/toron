// governed-by: ADR-0045

//! Multiplexer fallback: rex first, hc on a transport failure.
//!
//! `FallbackBackend` tries the primary and replays the same call on the
//! fallback, but **only when the failure provably did not change the world**.
//! That condition depends on what the call does, so every arm names its policy:
//!
//! - [`Policy::Read`] — a snapshot read. `Connect`, `Io`, and `Timeout` all
//!   leave no state behind, so any of them may replay.
//! - [`Policy::Wait`] — a bounded wait. Its timeout IS the caller's answer;
//!   replaying on the other plane would spend the caller's budget twice, so a
//!   timeout surfaces instead.
//! - [`Policy::Write`] — a mutating call. Only `Connect` is proof that nothing
//!   was dispatched. A `Timeout` or mid-stream `Io` may have committed at the
//!   provider (a pane created, a prompt typed), and replaying it on a second
//!   plane would double-apply an effect that has no idempotency key.
//!
//! Every other code — semantic rejects, malformed replies, unsupported
//! methods — surfaces loud. Falling back there would mask a real gap as a
//! working call. `name()` is `"auto"` so janitors matching `"hc"` exactly skip
//! auto, which is the safe direction (never delete a binding the fallback may
//! be serving through hc).

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};

use serde_json::Value;

use super::error::{BackendError, BackendErrorCode};
use super::types::{PromptLanding, Subscription, WaitOptions, WorkspaceInfo};
use super::{Backend, EventStream};
use toron_core::pane_types::{AgentInfo, AgentStatus, PaneInfo};

/// The plane pinned for a session (`PinnedPlane` / `FallbackBackend`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanePin {
    Primary,
    Fallback,
}

/// When a failed call may be replayed on the fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    /// A write: only a refused connection proves nothing was dispatched.
    Write,
    /// A cheap snapshot read: no failure mode leaves state behind.
    Read,
    /// A bounded wait: its timeout is the answer, not a transport fault.
    Wait,
}

fn may_replay(code: &BackendErrorCode, policy: Policy) -> bool {
    match policy {
        Policy::Write => matches!(code, BackendErrorCode::Connect),
        // A snapshot read may itself be harmless, but its RESULT is not:
        // identity/reconciliation callers act on it (pane_list drives pane
        // cleanup and lease reaping). A timeout or mid-stream Io must surface,
        // never silently answer from the other plane's different id space.
        // Only a refused connection proves the primary was never consulted.
        Policy::Read => matches!(code, BackendErrorCode::Connect),
        Policy::Wait => matches!(code, BackendErrorCode::Connect | BackendErrorCode::Io),
    }
}

/// rex first, hc when rex is unreachable. The fallback is fixed at
/// construction; an absent fallback leaves a primary-only backend whose
/// transport errors surface unchanged.
///
/// Implements session pinning (`PinnedPlane`): once a call succeeds on the
/// primary (or is explicitly pinned), mid-session transport failures on the
/// primary surface directly rather than crossing namespaces to hc (which would
/// return disjoint pane IDs and wipe rex identities/leases).
pub struct FallbackBackend {
    primary: Box<dyn Backend>,
    fallback: Option<Box<dyn Backend>>,
    pin: AtomicU8, // 0 = unpinned, 1 = primary, 2 = fallback
}

/// Alias for `FallbackBackend` under the session-pinned contract.
pub type PinnedPlane = FallbackBackend;

impl FallbackBackend {
    pub fn new(primary: Box<dyn Backend>, fallback: Option<Box<dyn Backend>>) -> Self {
        Self {
            primary,
            fallback,
            pin: AtomicU8::new(0),
        }
    }

    pub fn pinned(
        primary: Box<dyn Backend>,
        fallback: Option<Box<dyn Backend>>,
        pin: PlanePin,
    ) -> Self {
        let pin_val = match pin {
            PlanePin::Primary => 1,
            PlanePin::Fallback => 2,
        };
        Self {
            primary,
            fallback,
            pin: AtomicU8::new(pin_val),
        }
    }

    /// Current pinned plane, if any.
    pub fn pin(&self) -> Option<PlanePin> {
        match self.pin.load(Ordering::Acquire) {
            1 => Some(PlanePin::Primary),
            2 => Some(PlanePin::Fallback),
            _ => None,
        }
    }

    /// Explicitly set the session plane pin.
    pub fn set_pin(&self, pin: PlanePin) {
        let val = match pin {
            PlanePin::Primary => 1,
            PlanePin::Fallback => 2,
        };
        self.pin.store(val, Ordering::Release);
    }

    /// The serving primary's name (`rex`), for diagnostics.
    pub fn primary_name(&self) -> &'static str {
        self.primary.name()
    }

    /// Whether a fallback plane was constructed.
    pub fn has_fallback(&self) -> bool {
        self.fallback.is_some()
    }

    /// One call against the primary, replayed on the fallback per `policy`.
    /// When pinned, calls strictly route to the pinned plane without crossing namespaces.
    fn run<T>(
        &self,
        policy: Policy,
        op: impl Fn(&dyn Backend) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        match self.pin() {
            Some(PlanePin::Fallback) => match &self.fallback {
                Some(fallback) => op(&**fallback),
                None => op(&*self.primary),
            },
            Some(PlanePin::Primary) => op(&*self.primary),
            None => match op(&*self.primary) {
                Ok(val) => {
                    self.set_pin(PlanePin::Primary);
                    Ok(val)
                }
                Err(error) if may_replay(error.code(), policy) => match &self.fallback {
                    Some(fallback) => {
                        let res = op(&**fallback);
                        if res.is_ok() {
                            self.set_pin(PlanePin::Fallback);
                        }
                        res
                    }
                    None => Err(error),
                },
                Err(error) => Err(error),
            },
        }
    }

    /// Route an id-bound call to the plane that OWNS the id. hc ids carry the
    /// `hc:` prefix; a workspace created on hc during a rex outage must keep
    /// addressing hc even after rex recovers (bd-fallback-coldcreate).
    fn run_for_id<T>(
        &self,
        id: &str,
        policy: Policy,
        op: impl Fn(&dyn Backend) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        if id.starts_with("hc:")
            && let Some(fallback) = &self.fallback
        {
            return op(&**fallback);
        }
        self.run(policy, op)
    }
}

impl fmt::Debug for FallbackBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FallbackBackend")
            .field("primary", &self.primary.name())
            .field("fallback", &self.fallback.as_ref().map(|b| b.name()))
            .finish()
    }
}

impl Backend for FallbackBackend {
    fn name(&self) -> &'static str {
        "auto"
    }

    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        self.run(Policy::Read, |backend| backend.workspace_list())
    }

    fn workspace_create(&self, label: Option<&str>) -> Result<WorkspaceInfo, BackendError> {
        self.run(Policy::Write, |backend| backend.workspace_create(label))
    }

    fn workspace_close(&self, workspace_id: &str, close_group: bool) -> Result<(), BackendError> {
        self.run_for_id(workspace_id, Policy::Write, |backend| {
            backend.workspace_close(workspace_id, close_group)
        })
    }

    fn pane_split(
        &self,
        parent_pane_id: &str,
        direction: &str,
        cwd: Option<&str>,
    ) -> Result<PaneInfo, BackendError> {
        self.run_for_id(parent_pane_id, Policy::Write, |backend| {
            backend.pane_split(parent_pane_id, direction, cwd)
        })
    }

    fn pane_list(&self, workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        match workspace_id {
            Some(id) => {
                self.run_for_id(id, Policy::Read, |backend| backend.pane_list(workspace_id))
            }
            None => self.run(Policy::Read, |backend| backend.pane_list(None)),
        }
    }

    fn pane_close(&self, pane_id: &str) -> Result<(), BackendError> {
        self.run_for_id(pane_id, Policy::Write, |backend| {
            backend.pane_close(pane_id)
        })
    }

    fn pane_run(&self, pane_id: &str, command: &str) -> Result<(), BackendError> {
        self.run_for_id(pane_id, Policy::Write, |backend| {
            backend.pane_run(pane_id, command)
        })
    }

    fn pane_wait_output(
        &self,
        pane_id: &str,
        needle: &str,
        timeout_ms: u64,
    ) -> Result<Value, BackendError> {
        self.run_for_id(pane_id, Policy::Wait, |backend| {
            backend.pane_wait_output(pane_id, needle, timeout_ms)
        })
    }

    fn agent_alive(&self, pane_id: &str, kind: &str) -> Result<Option<bool>, BackendError> {
        self.run_for_id(pane_id, Policy::Read, |backend| {
            backend.agent_alive(pane_id, kind)
        })
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        self.run(Policy::Read, |backend| backend.agent_list())
    }

    /// Both lists are one read. Without this override, `pane_list` and
    /// `agent_list` could each fall back independently and reconciliation
    /// would compare different planes' id spaces.
    fn identity_snapshot(&self) -> Result<(Vec<PaneInfo>, Vec<AgentInfo>), BackendError> {
        self.run(Policy::Read, |backend| backend.identity_snapshot())
    }

    fn agent_start(
        &self,
        name: &str,
        kind: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        self.run_for_id(pane_id, Policy::Write, |backend| {
            backend.agent_start(name, kind, pane_id, args)
        })
    }

    fn agent_rename(&self, target: &str, new_name: Option<&str>) -> Result<(), BackendError> {
        self.run_for_id(target, Policy::Write, |backend| {
            backend.agent_rename(target, new_name)
        })
    }

    fn prompt(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<Value, BackendError> {
        self.run_for_id(target, Policy::Write, |backend| {
            backend.prompt(target, text, wait)
        })
    }

    fn prompt_coded(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        self.run_for_id(target, Policy::Write, |backend| {
            backend.prompt_coded(target, text, wait)
        })
    }

    fn agent_wait(
        &self,
        target: &str,
        until: &[AgentStatus],
        timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        self.run_for_id(target, Policy::Wait, |backend| {
            backend.agent_wait(target, until, timeout_ms)
        })
    }

    fn agent_read(&self, target: &str, source: &str, lines: u32) -> Result<Value, BackendError> {
        self.run_for_id(target, Policy::Read, |backend| {
            backend.agent_read(target, source, lines)
        })
    }

    fn agent_send_keys(&self, target: &str, keys: &[&str]) -> Result<(), BackendError> {
        self.run_for_id(target, Policy::Write, |backend| {
            backend.agent_send_keys(target, keys)
        })
    }

    fn subscribe(&self, filters: &[Subscription]) -> Result<Box<dyn EventStream>, BackendError> {
        self.run(Policy::Read, |backend| backend.subscribe(filters))
    }
}

#[cfg(test)]
mod fallback_tests {
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;

    #[derive(Default)]
    struct ScriptState {
        outcomes: Mutex<Vec<Option<BackendErrorCode>>>,
        calls: Mutex<usize>,
    }

    /// A backend that pops one scripted outcome per call and counts calls.
    /// `Clone` shares the state, so the test keeps a handle while the backend
    /// owns another.
    #[derive(Clone)]
    struct Scripted {
        name: &'static str,
        state: Arc<ScriptState>,
    }

    impl Scripted {
        fn new(name: &'static str, outcomes: Vec<Option<BackendErrorCode>>) -> Self {
            Self {
                name,
                state: Arc::new(ScriptState {
                    outcomes: Mutex::new(outcomes),
                    calls: Mutex::new(0),
                }),
            }
        }

        fn ok(name: &'static str) -> Self {
            Self::new(name, Vec::new())
        }

        fn failing(name: &'static str, code: BackendErrorCode) -> Self {
            Self::new(name, vec![Some(code)])
        }

        fn calls(&self) -> usize {
            *self.state.calls.lock()
        }

        fn next(&self, method: &str) -> Result<(), BackendError> {
            *self.state.calls.lock() += 1;
            let mut outcomes = self.state.outcomes.lock();
            match if outcomes.is_empty() {
                None
            } else {
                outcomes.remove(0)
            } {
                Some(code) => Err(BackendError::new(code, format!("scripted {method}"))),
                None => Ok(()),
            }
        }
    }

    impl Backend for Scripted {
        fn name(&self) -> &'static str {
            self.name
        }

        fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
            self.next("workspace_list").map(|()| Vec::new())
        }

        fn pane_close(&self, _pane_id: &str) -> Result<(), BackendError> {
            self.next("pane_close")
        }

        fn agent_wait(
            &self,
            _target: &str,
            _until: &[AgentStatus],
            _timeout_ms: Option<u64>,
        ) -> Result<AgentInfo, BackendError> {
            self.next("agent_wait")?;
            Err(BackendError::timeout("scripted wait returned no agent"))
        }

        fn pane_list(&self, _workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
            self.next("pane_list").map(|()| Vec::new())
        }

        fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
            self.next("agent_list").map(|()| Vec::new())
        }
    }

    fn backend(primary: Scripted, fallback: Option<Scripted>) -> FallbackBackend {
        FallbackBackend::new(
            Box::new(primary),
            fallback.map(|f| Box::new(f) as Box<dyn Backend>),
        )
    }

    /// A read whose primary TIMED OUT may have been answered by the primary;
    /// replaying it on the other plane would hand reconciliation callers a
    /// different id space (and wipe rex identities/leases). Only a refused
    /// connection proves the primary was never consulted.
    #[test]
    fn a_read_timeout_surfaces_and_never_crosses_planes() {
        let primary = Scripted::failing("rex", BackendErrorCode::Timeout);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        let error = backend
            .workspace_list()
            .expect_err("a timeout must surface");
        assert_eq!(error.code(), &BackendErrorCode::Timeout);
        assert_eq!(primary.calls(), 1);
        assert_eq!(fallback.calls(), 0, "a timeout must not cross planes");
    }

    #[test]
    fn a_read_replays_a_connect_refusal() {
        let primary = Scripted::failing("rex", BackendErrorCode::Connect);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        assert!(
            backend
                .workspace_list()
                .expect("fallback answers")
                .is_empty()
        );
        assert_eq!(primary.calls(), 1);
        assert_eq!(fallback.calls(), 1);
    }

    #[test]
    fn an_hc_prefixed_id_routes_to_the_fallback_plane() {
        let primary = Scripted::ok("rex");
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        backend.pane_close("hc:t1").expect("hc id closes on hc");
        assert_eq!(primary.calls(), 0, "an hc id never reaches rex");
        assert_eq!(fallback.calls(), 1);
    }

    /// Mutation: treat a write like a read and a timed-out `pane.close` is
    /// replayed on a second plane, closing a pane the primary may have already
    /// closed (or closing the wrong plane's same-named pane).
    #[test]
    fn a_write_does_not_replay_a_timeout() {
        let primary = Scripted::failing("rex", BackendErrorCode::Timeout);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        let error = backend.pane_close("b7").expect_err("timeout must surface");
        assert_eq!(error.code(), &BackendErrorCode::Timeout);
        assert_eq!(
            fallback.calls(),
            0,
            "a write must not replay after a timeout"
        );
    }

    /// A refused connection is proof the primary never dispatched.
    #[test]
    fn a_write_replays_a_refused_connection() {
        let primary = Scripted::failing("rex", BackendErrorCode::Connect);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        backend.pane_close("b7").expect("hc serves the call");
        assert_eq!(primary.calls(), 1);
        assert_eq!(fallback.calls(), 1);
    }

    /// A bounded wait's timeout is the caller's answer; a replay would spend
    /// the budget twice.
    #[test]
    fn a_wait_does_not_replay_its_own_timeout() {
        let primary = Scripted::new("rex", vec![Some(BackendErrorCode::Timeout)]);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        let error = backend
            .agent_wait("b7", &[AgentStatus::Idle], Some(10))
            .expect_err("timeout must surface");
        assert_eq!(error.code(), &BackendErrorCode::Timeout);
        assert_eq!(fallback.calls(), 0);
    }

    /// The identity pair is one read: a connection refusal on the pane half
    /// replays BOTH halves on the fallback, never a rex pane list against an
    /// hc agent list.
    #[test]
    fn an_identity_snapshot_replays_as_one_unit() {
        let primary = Scripted::new("rex", vec![Some(BackendErrorCode::Connect), None, None]);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        let (panes, agents) = backend
            .identity_snapshot()
            .expect("the pair is replayed on hc");
        assert!(panes.is_empty() && agents.is_empty());
        assert_eq!(primary.calls(), 1, "the primary pair stops at pane_list");
        assert_eq!(
            fallback.calls(),
            2,
            "both lists come from the same fallback plane"
        );
    }

    /// Unsupported is a verdict about the call, not a transport fault. A
    /// fallback here would mask a real gap as a working call.
    #[test]
    fn unsupported_never_replays() {
        let primary = Scripted::failing("rex", BackendErrorCode::Unsupported);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));
        let error = backend.workspace_list().expect_err("gap must surface");
        assert_eq!(error.code(), &BackendErrorCode::Unsupported);
        assert_eq!(fallback.calls(), 0);
    }

    /// An absent fallback preserves the primary's error unchanged.
    #[test]
    fn no_fallback_preserves_the_primary_error() {
        let primary = Scripted::failing("rex", BackendErrorCode::Connect);
        let backend = backend(primary, None);
        let error = backend.workspace_list().expect_err("no fallback");
        assert_eq!(error.code(), &BackendErrorCode::Connect);
        assert!(!backend.has_fallback());
    }

    /// Janitors matching `"hc"` exactly must skip auto: deleting a binding the
    /// fallback serves through hc would be the unsafe direction.
    #[test]
    fn the_composite_plane_is_named_auto() {
        let backend = backend(Scripted::ok("rex"), Some(Scripted::ok("hc")));
        assert_eq!(backend.name(), "auto");
        assert_eq!(backend.primary_name(), "rex");
        assert!(backend.has_fallback());
        let debug = format!("{backend:?}");
        assert!(debug.contains("primary: \"rex\""), "{debug}");
    }

    /// Once pinned to primary, mid-session failures (even Connect) must surface
    /// directly rather than crossing namespaces to hc.
    #[test]
    fn a_pinned_primary_never_falls_back_on_mid_session_connect_failure() {
        let primary = Scripted::new("rex", vec![None, Some(BackendErrorCode::Connect)]);
        let fallback = Scripted::ok("hc");
        let backend = backend(primary.clone(), Some(fallback.clone()));

        assert!(backend.workspace_list().is_ok());
        assert_eq!(primary.calls(), 1);
        assert_eq!(fallback.calls(), 0);
        assert_eq!(backend.pin(), Some(PlanePin::Primary));

        let error = backend
            .workspace_list()
            .expect_err("mid-session error must surface");
        assert_eq!(error.code(), &BackendErrorCode::Connect);
        assert_eq!(primary.calls(), 2);
        assert_eq!(
            fallback.calls(),
            0,
            "mid-session connect failure must never cross to hc"
        );
    }
}
