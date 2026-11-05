// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! Normalized action boundary (ADR-0032 Spec 1 + the action-boundary portion
//! of Spec 2).
//!
//! One boundary sits between graph sequencing and effect dispatch: a rendered
//! action is partitioned (engine-owned control, named host sink, or explicit
//! unsupported), validated on input and output, and settled into either an
//! [`ActionOutcome`] or a structured [`ActionFailure`]. The frozen
//! [`FailureKind`] set is closed — contract codes live in
//! [`ActionErrorCode`] and never expand it.
//!
//! The old context-free runner is deleted (bd-ctj7); pure leaves compute
//! through [`invoke_compat`] with no sink, and the production effect path
//! routes every call through [`partition`] and output
//! normalization first, so no live variant can fall through to success.
//!
//! This module never retries: [`invoke`] calls the dispatch closure exactly
//! once. Retry, backoff, and effect-safety live in the one engine/host retry
//! coordinator (ADR-0032 Spec 7). The journal itself is host-owned;
//! [`RunContext`] is the consumable contract, and `RunContext.provider_result`
//! carries the one sink the engine executes itself (`verify`, via the
//! embedding's `VerifySink`, ADR-0024 Spec 2) so a journaling runner settles
//! AROUND an effect that already happened instead of re-performing it.
//!
//! Host-owned does not mean silent, and the embedding declares how loud it
//! wants to be (`EffectJournalMode`, ADR-0032 Spec 6):
//! - `Recording` (default) marks an effectful host sink dispatched with no
//!   injected [`crate::engine::EffectRunner`] as
//!   [`crate::engine::EFFECT_JOURNAL_BYPASSED`], and a sink an injected runner
//!   does not cover as [`crate::engine::EFFECT_JOURNAL_SINK_UNCOVERED`], each
//!   with a loud reason — the dry arms (`workflow run` without `--execute`,
//!   `cron run` without `--live`) record instead of perform, so their leaves
//!   stay visibly unjournaled rather than settled.
//! - `Enforced` REFUSES an effectful host sink with no injected runner
//!   (fail-closed), so a dispatcher that really performs effects cannot run an
//!   effect nobody journaled.
//!
//! Both embeddings adopted the contract 2026-09-14
//! (`bd-engine-run-path-journaling-fwlx`): flywheel injects a host-owned runner
//! (`flywheel/src/effect_journal.rs`) and the toron host its own
//! (`toron-mcp/src/workflow_host.rs`), each Enforced on the arms that really
//! dispatch and Recording on the dry arms. `verify` now routes through
//! [`partition`]'s host-sink path like every other effectful leaf.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use toron_core::index::workflow_effects::EffectState;

use crate::schema::Action;

/// Stage at which an action boundary crossing failed (ADR-0032 Spec 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStage {
    Normalize,
    Input,
    Execute,
    Output,
    Effect,
    Park,
}

impl ActionStage {
    /// Canonical wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normalize => "normalize",
            Self::Input => "input",
            Self::Execute => "execute",
            Self::Output => "output",
            Self::Effect => "effect",
            Self::Park => "park",
        }
    }
}

/// Graph-admission disposition for a failed action (ADR-0032 Spec 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Continue,
    Retry,
    Reroute,
    Blocked,
    TerminalFailed,
    Cancelled,
    Reconcile,
}

impl Disposition {
    /// Canonical wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Retry => "retry",
            Self::Reroute => "reroute",
            Self::Blocked => "blocked",
            Self::TerminalFailed => "terminal_failed",
            Self::Cancelled => "cancelled",
            Self::Reconcile => "reconcile",
        }
    }
}

/// Frozen runtime failure kinds (ADR-0026, re-stated by ADR-0032 §2.2).
/// Closed: [`std::str::FromStr`] rejects everything outside these eight
/// spellings, and contract errors use [`ActionErrorCode`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Auth,
    RateLimit,
    Timeout,
    Blocked,
    Provider,
    Cancelled,
    Unknown,
    Billing,
}

impl FailureKind {
    /// Canonical wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::RateLimit => "rate_limit",
            Self::Timeout => "timeout",
            Self::Blocked => "blocked",
            Self::Provider => "provider",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
            Self::Billing => "billing",
        }
    }

    /// All eight frozen spellings, in enum order.
    pub const ALL: [&'static str; 8] = [
        "auth",
        "rate_limit",
        "timeout",
        "blocked",
        "provider",
        "cancelled",
        "unknown",
        "billing",
    ];

    /// Advisory retry candidacy for the one retry coordinator: `rate_limit`
    /// and `timeout` are candidates only when the effect state is known safe
    /// or the provider honors the stable effect key (ADR-0032 Spec 7). This
    /// flag never causes a retry by itself — the boundary never retries.
    pub const fn is_retry_candidate(self) -> bool {
        matches!(self, Self::RateLimit | Self::Timeout)
    }
}

impl std::str::FromStr for FailureKind {
    type Err = ActionErrorCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auth" => Ok(Self::Auth),
            "rate_limit" => Ok(Self::RateLimit),
            "timeout" => Ok(Self::Timeout),
            "blocked" => Ok(Self::Blocked),
            "provider" => Ok(Self::Provider),
            "cancelled" => Ok(Self::Cancelled),
            "unknown" => Ok(Self::Unknown),
            "billing" => Ok(Self::Billing),
            _ => Err(ActionErrorCode::InvalidInput),
        }
    }
}

/// Owned contract error codes (ADR-0032 Spec 1). These are program-decision
/// codes; they never expand [`FailureKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionErrorCode {
    InvalidInput,
    InvalidOutput,
    UnresolvedRef,
    UnknownAction,
    MissingSink,
    Cycle,
    StalledGraph,
    ProviderError,
    EffectUnknown,
    /// A live variant with no wired sink (deprecated, fail-closed).
    UnsupportedAction,
    /// An engine-owned control action reached the effect runner.
    ControlActionMisdispatch,
}

impl ActionErrorCode {
    /// Canonical wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::InvalidOutput => "invalid_output",
            Self::UnresolvedRef => "unresolved_ref",
            Self::UnknownAction => "unknown_action",
            Self::MissingSink => "missing_sink",
            Self::Cycle => "cycle",
            Self::StalledGraph => "stalled_graph",
            Self::ProviderError => "provider_error",
            Self::EffectUnknown => "effect_unknown",
            Self::UnsupportedAction => "unsupported_action",
            Self::ControlActionMisdispatch => "control_action_misdispatch",
        }
    }
}

/// Bounded failure evidence (ADR-0032 Spec 1): identifiers and a truncated
/// detail string only. Never a stack trace, secret, process handle, closure,
/// or unbounded payload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Maximum bytes kept in [`EvidenceRef::detail`]; longer provider messages
/// are truncated with an ellipsis marker.
pub const MAX_EVIDENCE_DETAIL: usize = 512;

impl EvidenceRef {
    /// Build evidence with a length-bounded detail string.
    pub fn with_detail(detail: impl Into<String>) -> Self {
        let mut d: String = detail.into();
        if d.len() > MAX_EVIDENCE_DETAIL {
            d.truncate(MAX_EVIDENCE_DETAIL);
            d.push_str("…[truncated]");
        }
        Self {
            detail: Some(d),
            ..Self::default()
        }
    }
}

/// Settlement projection for one boundary crossing (ADR-0032 Spec 1).
/// `None` for pure/control actions; journal-backed states arrive with the
/// host-owned effect journal (follow-up bead) — this module never claims a
/// settlement it did not record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectSettlement {
    None,
    Recorded {
        effect_key: String,
        state: EffectState,
    },
}

/// Successful boundary result: the contract-valid value plus its settlement.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionOutcome {
    pub value: serde_json::Value,
    pub effect: EffectSettlement,
}

/// Structured boundary failure (ADR-0032 Spec 7): stage, stable code, human
/// message, optional frozen runtime kind, bounded evidence, effect state,
/// and disposition. The durable/public projection is [`Self::to_value`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionFailure {
    pub stage: ActionStage,
    pub code: ActionErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<FailureKind>,
    pub retryable: bool,
    pub resumable: bool,
    pub disposition: Disposition,
    /// The journaled state when the failure names a row; `None` when the
    /// failure precedes any row (unsupported action, misdispatch, missing
    /// sink). The core enum plus `Option` for "no row" — no second
    /// spelling of the state table.
    pub effect_state: Option<EffectState>,
    pub evidence: EvidenceRef,
}

impl ActionFailure {
    /// Durable/public projection: `ok:false` plus stage/code/effect state
    /// and bounded identifiers. Never a stack trace or secret: the message
    /// is truncated to [`MAX_EVIDENCE_DETAIL`] bytes.
    pub fn to_value(&self) -> serde_json::Value {
        let mut message = self.message.clone();
        if message.len() > MAX_EVIDENCE_DETAIL {
            message.truncate(MAX_EVIDENCE_DETAIL);
            message.push_str("…[truncated]");
        }
        serde_json::json!({
            "ok": false,
            "error": message,
            "code": self.code.as_str(),
            "stage": self.stage.as_str(),
            "kind": self.kind.map(FailureKind::as_str),
            "retryable": self.retryable,
            "effect_state": self.effect_state.map(EffectState::as_str).unwrap_or("none"),
            "disposition": self.disposition.as_str(),
            "evidence": self.evidence,
        })
    }
}

/// Run/node/effect context carried by every effectful invocation
/// (ADR-0032 Spec 1, invariant I3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunContext {
    pub run_id: String,
    pub workflow_digest: String,
    pub node_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iteration_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_key: Option<String>,
    pub attempt: u32,
    /// The sink's already-executed result, for sinks the engine executes
    /// synchronously before the runner is invoked (today: `verify` through
    /// the embedding's `VerifySink`, ADR-0024 Spec 2). The journaling runner
    /// settles AROUND this result — the external effect already happened; a
    /// journal row must observe it, never re-perform it.
    /// `#[serde(default)]` keeps old serialized invocations loadable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_result: Option<serde_json::Value>,
}

/// Execution policy selected by the one engine/host coordinator
/// (ADR-0032 Spec 7). Carried, never acted on, by the boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPolicy {
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// The context-bearing invocation: rendered action plus run/node/effect
/// context plus policy (ADR-0032 Spec 1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionInvocation {
    pub action: Action,
    pub run: RunContext,
    pub execution: ExecutionPolicy,
}

/// Engine-sequenced actions outside the historical `ACTION_TYPES` and
/// `CONTROL_FLOW_ACTION_TYPES` lists (bd-i6kc): `spawn` parks/sequences in
/// the engine, `verify` runs through the embedding-owned `VerifySink` —
/// neither is dispatched by name lookup.
pub const ENGINE_ACTION_TYPES: [&str; 4] = ["request_approval", "delay", "spawn", "verify"];

/// Deprecated actions with no wired sink (bd-i6kc retain-or-remove
/// decision: remove). They parse and lint for migration, but the boundary
/// rejects them with `unsupported_action` — they never execute.
pub const DEPRECATED_ACTION_TYPES: [&str; 2] = ["react", "call_webhook"];

/// The advertised action vocabulary is not a stale copy of the closed enum:
/// every name in the historical schema lists plus the boundary lists must
/// cover each live `Action::type_name()` exactly once as a set. Adding a
/// variant without listing it here fails this test; dispatching a variant
/// without a partition arm fails to compile.
pub fn advertised_action_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = Vec::new();
    names.extend(crate::schema::ACTION_TYPES.iter().copied());
    names.extend(crate::schema::CONTROL_FLOW_ACTION_TYPES.iter().copied());
    names.extend(ENGINE_ACTION_TYPES.iter().copied());
    names.extend(DEPRECATED_ACTION_TYPES.iter().copied());
    names.sort_unstable();
    names.dedup();
    names
}

/// Exhaustive action partition (ADR-0032 invariant I1): every `Action`
/// variant is engine-owned control flow, a named host sink, or explicitly
/// unsupported. There is no fourth arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionPartition {
    /// Sequenced/parked by the engine; must never reach an effect runner.
    EngineControl,
    /// Dispatched by the embedding through this named sink.
    HostSink {
        /// Sink name (`mailbox`, `job`, `verify`).
        sink: &'static str,
    },
    /// Deprecated with no wired sink: fail closed with this migration note.
    Unsupported {
        /// Why the action cannot run and what replaces it.
        reason: &'static str,
    },
}

/// Classify one rendered action. Exhaustive over `Action`: adding a variant
/// without extending this match is a compile error, which is the witness
/// that no live variant can fall through.
pub const fn partition(action: &Action) -> ActionPartition {
    match action {
        Action::Delay { .. }
        | Action::RequestApproval { .. }
        | Action::Retry { .. }
        | Action::Branch { .. }
        | Action::Foreach { .. }
        | Action::Parallel { .. }
        | Action::CallWorkflow { .. }
        | Action::Reduce { .. }
        | Action::Loop { .. }
        | Action::Spawn { .. } => ActionPartition::EngineControl,
        Action::Park { .. } => ActionPartition::EngineControl,
        Action::SendMessage { .. } | Action::SendDm { .. } => {
            ActionPartition::HostSink { sink: "mailbox" }
        }
        Action::DispatchJob { .. } => ActionPartition::HostSink { sink: "job" },
        Action::Verify { .. } => ActionPartition::HostSink { sink: "verify" },
        Action::React { .. } => ActionPartition::Unsupported {
            reason: "react is deprecated: no reaction sink is wired (bd-i6kc); \
                     remove the step or replace it with send_message",
        },
        Action::CallWebhook { .. } => ActionPartition::Unsupported {
            reason: "call_webhook is deprecated: no webhook sink is wired and the \
                     SSRF contract (scheme/host policy, redirect and DNS-rebinding \
                     control, bounds, redaction) is unmet (bd-i6kc); remove the step",
        },
    }
}

/// Fail-closed rejection for an explicitly unsupported action.
pub fn failure_for_unsupported(action: &Action) -> ActionFailure {
    let reason = match partition(action) {
        ActionPartition::Unsupported { reason } => reason,
        _ => "action is not available on this runner",
    };
    ActionFailure {
        stage: ActionStage::Execute,
        code: ActionErrorCode::UnsupportedAction,
        message: format!("unsupported action '{}': {reason}", action.type_name()),
        kind: None,
        retryable: false,
        resumable: false,
        disposition: Disposition::TerminalFailed,
        effect_state: None,
        evidence: EvidenceRef::default(),
    }
}

/// Fail-closed rejection for an engine-owned control action that reached an
/// effect runner (accidental dispatch — never executed, never retried).
pub fn failure_for_control_misdispatch(action: &Action) -> ActionFailure {
    ActionFailure {
        stage: ActionStage::Execute,
        code: ActionErrorCode::ControlActionMisdispatch,
        message: format!(
            "control action '{}' reached the effect runner: engine-owned actions \
             (delay, request_approval, retry, branch, foreach, parallel, \
             call_workflow, spawn) must be sequenced by the engine, never dispatched",
            action.type_name()
        ),
        kind: None,
        retryable: false,
        resumable: false,
        disposition: Disposition::TerminalFailed,
        effect_state: None,
        evidence: EvidenceRef::default(),
    }
}

/// Fail-closed rejection when the required named sink is absent on this
/// runner (e.g. `verify` on the mailbox-only runner).
pub fn failure_for_missing_sink(action: &Action, sink: &'static str) -> ActionFailure {
    ActionFailure {
        stage: ActionStage::Effect,
        code: ActionErrorCode::MissingSink,
        message: format!(
            "action '{}' requires the '{sink}' sink, which is not available on this runner",
            action.type_name()
        ),
        kind: None,
        retryable: false,
        resumable: false,
        disposition: Disposition::Blocked,
        effect_state: None,
        evidence: EvidenceRef::default(),
    }
}

/// Fail-closed rejection for a `Null` dispatch result: no action on this
/// boundary declares unit output, so null is `invalid_output`, never success.
pub fn failure_for_invalid_output(type_name: &'static str, evidence: EvidenceRef) -> ActionFailure {
    ActionFailure {
        stage: ActionStage::Output,
        code: ActionErrorCode::InvalidOutput,
        message: format!(
            "action '{type_name}' returned null: no action on this boundary declares \
             unit output (bd-i6kc)"
        ),
        kind: None,
        retryable: false,
        resumable: false,
        disposition: Disposition::TerminalFailed,
        effect_state: Some(EffectState::Failed),
        evidence,
    }
}
/// How one provider error reads against the journal (bd-zczp, I5):
/// a proven failure vs an ambiguous outcome no retry may assume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorClass {
    /// Durably proven no-dispatch (unknown recipient, missing signer,
    /// validation rejection): Failed + TerminalFailed stands.
    Proven,
    /// The send may or may not have happened (timeout, transport loss):
    /// Unknown + Reconcile, never blanket Failed.
    Ambiguous,
}

/// Classify one provider error message (bd-zczp, I5 — a timeout is not
/// proof of absence). Conservative by design: only recognized ambiguity
/// signals read Ambiguous; everything else stays Proven (today's
/// behavior). The signal list names transport uncertainty, never
/// deterministic rejections — and it deliberately avoids the bare word
/// "unknown" so "unknown agent" (proven: no such recipient) never reads
/// as ambiguous.
pub fn classify_provider_error_msg(msg: &str) -> ProviderErrorClass {
    const AMBIGUOUS: &[&str] = &[
        "timeout",
        "timed out",
        "timed-out",
        "deadline",
        "transport",
        "connection",
        "network",
        "unavailable",
        "ambiguous",
        "not confirmed",
        "unknown outcome",
        "may have",
        "broken pipe",
        "reset by peer",
        "eof",
    ];
    let lower = msg.to_lowercase();
    if AMBIGUOUS.iter().any(|sig| lower.contains(sig)) {
        ProviderErrorClass::Ambiguous
    } else {
        ProviderErrorClass::Proven
    }
}

/// The frozen runtime kind for an ambiguous error: timeouts keep their
/// kind (a retry candidate per Spec 7); other transport uncertainty
/// takes the frozen `unknown` kind — the set never expands.
fn ambiguous_kind(msg: &str) -> FailureKind {
    const TIMEOUT: &[&str] = &["timeout", "timed out", "timed-out", "deadline"];
    let lower = msg.to_lowercase();
    if TIMEOUT.iter().any(|sig| lower.contains(sig)) {
        FailureKind::Timeout
    } else {
        FailureKind::Unknown
    }
}

/// Build the boundary failure for one provider error (bd-zczp): proven
/// errors keep Failed + TerminalFailed + retryable:false; ambiguous
/// errors surface Unknown + Reconcile with the frozen kind. Shared by
/// [`normalize_dispatch_result`] and [`invoke_compat`] so both paths
/// classify identically.
pub fn failure_for_provider_error(message: String, evidence: EvidenceRef) -> ActionFailure {
    if classify_provider_error_msg(&message) == ProviderErrorClass::Ambiguous {
        let kind = ambiguous_kind(&message);
        return ActionFailure {
            stage: ActionStage::Effect,
            code: ActionErrorCode::ProviderError,
            message,
            kind: Some(kind),
            retryable: kind.is_retry_candidate(),
            resumable: false,
            disposition: Disposition::Reconcile,
            effect_state: Some(EffectState::Unknown),
            evidence,
        };
    }
    ActionFailure {
        stage: ActionStage::Effect,
        code: ActionErrorCode::ProviderError,
        message,
        kind: None,
        retryable: false,
        resumable: false,
        disposition: Disposition::TerminalFailed,
        effect_state: Some(EffectState::Failed),
        evidence,
    }
}

/// Normalize one dispatch result at the boundary: a provider error becomes
/// a structured failure; `Null` is rejected because no action crossing this
/// boundary declares unit output (ADR-0032 Spec 2: null is not a universal
/// success fallback).
///
/// The failure is boxed: `ActionFailure` carries owned messages and evidence
/// well above the `result_large_err` threshold.
pub fn normalize_dispatch_result(
    invocation: &ActionInvocation,
    result: anyhow::Result<serde_json::Value>,
) -> Result<ActionOutcome, Box<ActionFailure>> {
    match result {
        Err(e) => Err(Box::new(failure_for_provider_error(
            format!("dispatch failed: {e:#}"),
            EvidenceRef {
                run_id: Some(invocation.run.run_id.clone()),
                node: Some(invocation.run.node_path.clone()),
                effect_key: invocation.run.effect_key.clone(),
                attempt: Some(invocation.run.attempt),
                ..EvidenceRef::with_detail(format!("{e:#}"))
            },
        ))),
        Ok(value) => {
            if value.is_null() {
                Err(Box::new(failure_for_invalid_output(
                    invocation.action.type_name(),
                    EvidenceRef {
                        run_id: Some(invocation.run.run_id.clone()),
                        node: Some(invocation.run.node_path.clone()),
                        ..EvidenceRef::default()
                    },
                )))
            } else {
                Ok(ActionOutcome {
                    value,
                    effect: EffectSettlement::None,
                })
            }
        }
    }
}

/// The context-bearing boundary: partition, dispatch exactly once, and
/// normalize. Control and unsupported actions are rejected before the
/// dispatch closure runs; the closure is never retried here.
pub fn invoke(
    invocation: &ActionInvocation,
    dispatch: impl FnOnce(&Action) -> anyhow::Result<serde_json::Value>,
) -> Result<ActionOutcome, Box<ActionFailure>> {
    match partition(&invocation.action) {
        ActionPartition::EngineControl => Err(Box::new(failure_for_control_misdispatch(
            &invocation.action,
        ))),
        ActionPartition::Unsupported { .. } => {
            Err(Box::new(failure_for_unsupported(&invocation.action)))
        }
        ActionPartition::HostSink { .. } => {
            normalize_dispatch_result(invocation, dispatch(&invocation.action))
        }
    }
}

/// Pure-leaf helper: route one effect-free call through the boundary
/// partition and return the runner-shaped value (`{ok:false,…}` failure
/// object, never bare `Null`).
/// `dispatch_host` runs only for named-sink actions the caller can serve;
/// when it returns `None` the sink is missing on this runner.
pub fn invoke_compat(
    action: &Action,
    dispatch_host: impl FnOnce(&Action) -> Option<anyhow::Result<serde_json::Value>>,
) -> serde_json::Value {
    match partition(action) {
        ActionPartition::EngineControl => failure_for_control_misdispatch(action).to_value(),
        ActionPartition::Unsupported { .. } => failure_for_unsupported(action).to_value(),
        ActionPartition::HostSink { sink } => match dispatch_host(action) {
            None => failure_for_missing_sink(action, sink).to_value(),
            Some(Err(e)) => failure_for_provider_error(
                format!("dispatch failed: {e:#}"),
                EvidenceRef::with_detail(format!("{e:#}")),
            )
            .to_value(),
            Some(Ok(value)) => {
                if value.is_null() {
                    failure_for_invalid_output(action.type_name(), EvidenceRef::default())
                        .to_value()
                } else {
                    value
                }
            }
        },
    }
}

/// `true` when a runner-shaped value failed the step.
///
/// Two spellings are the same fact: a leaf's `{ok:false}` and a control-flow
/// terminal `{status:failed}`. A graph scheduler that only reads one of them
/// stores the other and reports the run successful. `ok:true` with an
/// informational `error` field does not fail. A skip (`skipped: true`) does
/// not fail.
pub fn is_failure_value(value: &serde_json::Value) -> bool {
    if value.get("ok") == Some(&serde_json::Value::Bool(false)) {
        return true;
    }
    value.get("status").and_then(serde_json::Value::as_str) == Some("failed")
}

/// Stable logical effect identity (ADR-0032 Spec 3): SHA-256 over the
/// canonical components, hex-encoded. Excludes the retry attempt — retries
/// of one logical effect share the key; the attempt lives in
/// [`RunContext::attempt`] and the journal's `attempt_id`.
pub fn derive_effect_key(
    workflow_digest: &str,
    run_id: &str,
    node_path: &str,
    iteration_key: Option<&str>,
    action_digest: &str,
) -> String {
    let mut h = Sha256::new();
    h.update(workflow_digest.as_bytes());
    h.update([0x1f]);
    h.update(run_id.as_bytes());
    h.update([0x1f]);
    h.update(node_path.as_bytes());
    h.update([0x1f]);
    h.update(iteration_key.unwrap_or("").as_bytes());
    h.update([0x1f]);
    h.update(action_digest.as_bytes());
    hex::encode(h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ContextMode, Step, StepOnError};
    use std::cell::Cell;

    fn leaf_delay() -> Step {
        Step {
            name: None,
            guard: None,
            on_error: StepOnError::Continue,
            build_slot: false,
            action: Action::Delay { secs: 0 },
        }
    }

    fn test_spec() -> crate::schema::DispatchSpec {
        serde_json::from_value(serde_json::json!({"task": "t"})).expect("spec")
    }

    /// Every live `Action` variant, one per type name.
    fn every_action() -> Vec<Action> {
        vec![
            Action::SendMessage {
                to: vec!["a".into()],
                subject: "s".into(),
                body: "b".into(),
                project: None,
            },
            Action::SendDm {
                to: "a".into(),
                body: "b".into(),
                project: None,
            },
            Action::React {
                message_id: "m".into(),
                emoji: "+1".into(),
            },
            Action::DispatchJob {
                assignee: "a".into(),
                spec: test_spec(),
                channel: None,
            },
            Action::CallWebhook {
                url: "https://x".into(),
                method: None,
                body: None,
            },
            Action::RequestApproval {
                message: "m".into(),
            },
            Action::Delay { secs: 1 },
            Action::Retry {
                max_attempts: 1,
                backoff_secs: 0,
                step: Box::new(leaf_delay()),
            },
            Action::Branch {
                if_guard: "true".into(),
                then: vec![],
                else_steps: vec![],
            },
            Action::Foreach {
                over: "x".into(),
                each: Box::new(leaf_delay()),
            },
            Action::Parallel {
                over: "1".into(),
                each: Box::new(leaf_delay()),
            },
            Action::CallWorkflow {
                workflow: "w".into(),
                inputs: serde_json::json!({}),
            },
            Action::Spawn {
                profile: "@p".into(),
                harness: None,
                workflow: None,
                await_: false,
                no_claim_timeout: 0,
                context: Some(ContextMode::Inherit),
            },
            Action::Loop {
                max: "1".into(),
                until: None,
                body: vec![leaf_delay()],
            },
            Action::Verify {
                command: "true".into(),
            },
            Action::Reduce {
                over: "x".into(),
                op: crate::schema::ReduceOp::Count,
                key: None,
                template: None,
                separator: None,
                init: None,
            },
        ]
    }

    fn invocation_for(action: Action) -> ActionInvocation {
        ActionInvocation {
            action,
            run: RunContext {
                run_id: "run-1".into(),
                workflow_digest: "digest".into(),
                node_path: "node".into(),
                iteration_key: None,
                effect_key: None,
                attempt: 1,
                provider_result: None,
            },
            execution: ExecutionPolicy {
                max_attempts: 1,
                timeout_secs: None,
            },
        }
    }

    /// bd-i6kc honesty gate: the partition is exhaustive — all 14 live
    /// variants classify into exactly one arm, and the type names are the
    /// 14 canonical spellings.
    #[test]
    fn honesty_gate_partition_is_exhaustive() {
        let actions = every_action();
        assert_eq!(actions.len(), 16, "one case per live Action variant");
        let mut names: Vec<&str> = actions
            .iter()
            .map(crate::schema::Action::type_name)
            .collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(
            names.len(),
            16,
            "type names are unique per variant: {names:?}"
        );
        for action in &actions {
            match partition(action) {
                ActionPartition::EngineControl
                | ActionPartition::HostSink { .. }
                | ActionPartition::Unsupported { .. } => {}
            }
        }
        let control = actions
            .iter()
            .filter(|a| matches!(partition(a), ActionPartition::EngineControl))
            .count();
        assert_eq!(control, 10, "engine-owned control actions");
        let unsupported = actions
            .iter()
            .filter(|a| matches!(partition(a), ActionPartition::Unsupported { .. }))
            .count();
        assert_eq!(unsupported, 2, "deprecated react + call_webhook");
    }

    /// bd-i6kc honesty gate: a control action crossing the boundary is
    /// rejected with the misdispatch code and the dispatch closure never runs.
    #[test]
    fn honesty_gate_effect_runner_rejects_control() {
        let called = Cell::new(false);
        let inv = invocation_for(Action::Delay { secs: 1 });
        let err = invoke(&inv, |_| {
            called.set(true);
            Ok(serde_json::json!({"ok": true}))
        })
        .expect_err("control action must be rejected");
        assert!(!called.get(), "dispatch must not run for control actions");
        assert_eq!(err.code, ActionErrorCode::ControlActionMisdispatch);
        assert_eq!(err.stage, ActionStage::Execute);
        assert_eq!(err.effect_state, None);
        assert!(!err.retryable);
        let v = err.to_value();
        assert_eq!(v["ok"], serde_json::json!(false));
        assert_eq!(v["code"], serde_json::json!("control_action_misdispatch"));
    }

    /// bd-i6kc honesty gate: unsupported actions can never report success —
    /// rejection carries `unsupported_action` and dispatch never runs.
    #[test]
    fn honesty_gate_unsupported_never_succeeds() {
        for action in [
            Action::React {
                message_id: "m".into(),
                emoji: "+1".into(),
            },
            Action::CallWebhook {
                url: "https://x".into(),
                method: None,
                body: None,
            },
        ] {
            let called = Cell::new(false);
            let inv = invocation_for(action);
            let err = invoke(&inv, |_| {
                called.set(true);
                Ok(serde_json::json!({"ok": true}))
            })
            .expect_err("unsupported action must be rejected");
            assert!(
                !called.get(),
                "dispatch must not run for unsupported actions"
            );
            assert_eq!(err.code, ActionErrorCode::UnsupportedAction);
            assert!(!err.retryable);
            assert_eq!(err.to_value()["ok"], serde_json::json!(false));
        }
    }

    /// bd-i6kc honesty gate: the boundary never retries — the dispatch
    /// closure runs exactly once per invoke (retry lives in the coordinator).
    #[test]
    fn honesty_gate_no_retry_in_callback() {
        let calls = Cell::new(0u32);
        let inv = invocation_for(Action::SendDm {
            to: "a".into(),
            body: "b".into(),
            project: None,
        });
        let out = invoke(&inv, |_| {
            calls.set(calls.get() + 1);
            Ok(serde_json::json!({"ok": true}))
        })
        .expect("host-sink action dispatches");
        assert_eq!(calls.get(), 1, "dispatch runs exactly once");
        assert_eq!(out.value["ok"], serde_json::json!(true));
        assert_eq!(out.effect, EffectSettlement::None);
    }

    /// bd-i6kc honesty gate (bd-zczp amended): a dispatch error crosses as
    /// a structured `provider_error` failure — and a TIMEOUT crosses as
    /// Unknown + Reconcile (I5: a timeout is not proof of absence), never
    /// a fresh retry and never blanket Failed.
    #[test]
    fn honesty_gate_dispatch_error_is_structured_failure() {
        let calls = Cell::new(0u32);
        let inv = invocation_for(Action::SendDm {
            to: "a".into(),
            body: "b".into(),
            project: None,
        });
        let err = invoke(&inv, |_| {
            calls.set(calls.get() + 1);
            Err(anyhow::anyhow!("relay timeout"))
        })
        .expect_err("dispatch error must surface");
        assert_eq!(calls.get(), 1, "no retry on dispatch error");
        assert_eq!(err.code, ActionErrorCode::ProviderError);
        assert_eq!(err.stage, ActionStage::Effect);
        assert_eq!(err.effect_state, Some(EffectState::Unknown));
        assert_eq!(err.disposition, Disposition::Reconcile);
        assert_eq!(err.kind, Some(FailureKind::Timeout));
        assert!(
            err.kind.is_some(),
            "classified errors take a frozen runtime kind"
        );
    }

    /// bd-i6kc honesty gate: `Null` is not success — no action on this
    /// boundary declares unit output, so a null dispatch result fails with
    /// `invalid_output`.
    #[test]
    fn honesty_gate_null_is_not_success() {
        let inv = invocation_for(Action::SendDm {
            to: "a".into(),
            body: "b".into(),
            project: None,
        });
        let err = invoke(&inv, |_| Ok(serde_json::Value::Null)).expect_err("null must fail");
        assert_eq!(err.code, ActionErrorCode::InvalidOutput);
        assert_eq!(err.stage, ActionStage::Output);
    }

    /// bd-i6kc honesty gate: the runtime `FailureKind` set is frozen —
    /// exactly the eight ADR-0026 spellings parse; contract codes and
    /// lookalikes are rejected.
    #[test]
    fn honesty_gate_failure_kind_set_is_frozen() {
        for spelling in FailureKind::ALL {
            assert!(spelling.parse::<FailureKind>().is_ok(), "{spelling} parses");
        }
        assert_eq!(FailureKind::ALL.len(), 8);
        for bad in [
            "",
            "invalid_input",
            "invalid_output",
            "unknown_action",
            "missing_sink",
            "RATE_LIMIT",
            "RateLimit",
            "timeout ",
            "ok",
        ] {
            assert!(
                bad.parse::<FailureKind>().is_err(),
                "{bad:?} must not parse as a kind"
            );
        }
        assert!(FailureKind::RateLimit.is_retry_candidate());
        assert!(FailureKind::Timeout.is_retry_candidate());
        assert!(!FailureKind::Auth.is_retry_candidate());
        assert!(!FailureKind::Unknown.is_retry_candidate());
    }

    /// bd-i6kc honesty gate: failure projections carry stage/code/effect
    /// state and bounded identifiers — and bound the message.
    #[test]
    fn honesty_gate_failure_projection_is_bounded() {
        let err = ActionFailure {
            stage: ActionStage::Effect,
            code: ActionErrorCode::ProviderError,
            message: "x".repeat(10_000),
            kind: Some(FailureKind::Provider),
            retryable: false,
            resumable: false,
            disposition: Disposition::TerminalFailed,
            effect_state: Some(EffectState::Unknown),
            evidence: EvidenceRef::with_detail("y".repeat(10_000)),
        };
        let v = err.to_value();
        assert_eq!(v["ok"], serde_json::json!(false));
        assert_eq!(v["stage"], serde_json::json!("effect"));
        assert_eq!(v["code"], serde_json::json!("provider_error"));
        assert_eq!(v["kind"], serde_json::json!("provider"));
        assert_eq!(v["effect_state"], serde_json::json!("unknown"));
        let msg = v["error"].as_str().expect("error message");
        assert!(msg.len() <= MAX_EVIDENCE_DETAIL + 32, "message is bounded");
        assert!(msg.ends_with("…[truncated]"));
    }

    /// bd-i6kc honesty gate: `ok:false` fails, `ok:true` (even with an
    /// informational error field) does not.
    #[test]
    fn honesty_gate_ok_false_is_the_failure_predicate() {
        assert!(is_failure_value(
            &serde_json::json!({"ok": false, "error": "x"})
        ));
        assert!(!is_failure_value(&serde_json::json!({"ok": true})));
        assert!(!is_failure_value(
            &serde_json::json!({"ok": true, "error": "note"})
        ));
        assert!(!is_failure_value(&serde_json::Value::Null));
        assert!(is_failure_value(
            &serde_json::json!({"status": "failed", "error": "child"})
        ));
        assert!(!is_failure_value(
            &serde_json::json!({"skipped": true, "node": "gate"})
        ));
    }

    /// bd-i6kc honesty gate: the advertised action lists are not a stale
    /// copy of the closed enum — every live `Action::type_name()` appears
    /// in the advertised set, and the set names nothing dead.
    #[test]
    fn honesty_gate_advertised_action_lists_are_exhaustive() {
        use std::collections::BTreeSet;
        let advertised: BTreeSet<&str> = advertised_action_names().into_iter().collect();
        let live: BTreeSet<&str> = every_action()
            .iter()
            .map(crate::schema::Action::type_name)
            .collect();
        assert_eq!(live.len(), 16, "one name per live Action variant");
        assert_eq!(advertised, live, "advertised names == live type names");
    }

    /// bd-i6kc honesty gate: stable effect keys exclude the attempt — one
    /// logical effect keeps its key across retries; distinct nodes differ.
    #[test]
    fn honesty_gate_effect_key_stable_across_attempts() {
        let a = derive_effect_key("digest", "run", "node", None, "action-digest");
        let b = derive_effect_key("digest", "run", "node", None, "action-digest");
        assert_eq!(a, b, "same logical effect, same key");
        assert_eq!(a.len(), 64, "hex sha256");
        let other_node = derive_effect_key("digest", "run", "other", None, "action-digest");
        assert_ne!(a, other_node, "distinct nodes differ");
        let iterated = derive_effect_key("digest", "run", "node", Some("item-3"), "action-digest");
        assert_ne!(a, iterated, "iteration key participates");
    }

    /// bd-zczp RED: a timeout is not proof of absence — an ambiguous
    /// provider error surfaces as Unknown + Reconcile, never Failed +
    /// TerminalFailed.
    #[test]
    fn boundary_timeout_is_unknown_not_failed() {
        let inv = invocation_for(Action::SendDm {
            to: "a".into(),
            body: "b".into(),
            project: None,
        });
        let err =
            invoke(&inv, |_| Err(anyhow::anyhow!("relay timeout"))).expect_err("must surface");
        assert_eq!(err.code, ActionErrorCode::ProviderError);
        assert_eq!(err.effect_state, Some(EffectState::Unknown));
        assert_eq!(err.disposition, Disposition::Reconcile);
        assert_eq!(err.kind, Some(FailureKind::Timeout));
        let v = err.to_value();
        assert_eq!(v["effect_state"], serde_json::json!("unknown"));
        assert_eq!(v["disposition"], serde_json::json!("reconcile"));
    }

    /// bd-zczp RED: transport loss is ambiguous — Unknown + Reconcile.
    #[test]
    fn boundary_transport_error_is_unknown() {
        let inv = invocation_for(Action::SendDm {
            to: "a".into(),
            body: "b".into(),
            project: None,
        });
        let err = invoke(&inv, |_| {
            Err(anyhow::anyhow!("transport: connection reset by peer"))
        })
        .expect_err("must surface");
        assert_eq!(err.effect_state, Some(EffectState::Unknown));
        assert_eq!(err.disposition, Disposition::Reconcile);
    }

    /// bd-zczp RED: a deterministic rejection stays Failed + TerminalFailed
    /// — only recognized-ambiguous signals become Unknown.
    #[test]
    fn boundary_deterministic_error_stays_failed() {
        let inv = invocation_for(Action::SendDm {
            to: "a".into(),
            body: "b".into(),
            project: None,
        });
        let err = invoke(&inv, |_| Err(anyhow::anyhow!("unknown agent: nobody")))
            .expect_err("must surface");
        assert_eq!(err.effect_state, Some(EffectState::Failed));
        assert_eq!(err.disposition, Disposition::TerminalFailed);
        assert!(!err.retryable);
    }

    /// bd-zczp RED: the compat adapter classifies too — a host-closure
    /// timeout is Unknown, never blanket Failed.
    #[test]
    fn boundary_compat_timeout_is_unknown() {
        let v = invoke_compat(
            &Action::SendDm {
                to: "a".into(),
                body: "b".into(),
                project: None,
            },
            |_| Some(Err(anyhow::anyhow!("send timeout"))),
        );
        assert_eq!(v["ok"], serde_json::json!(false));
        assert_eq!(v["effect_state"], serde_json::json!("unknown"));
        assert_eq!(v["disposition"], serde_json::json!("reconcile"));
    }

    /// bd-zczp RED: the compat adapter keeps proven failures Failed.
    #[test]
    fn boundary_compat_deterministic_stays_failed() {
        let v = invoke_compat(
            &Action::SendDm {
                to: "a".into(),
                body: "b".into(),
                project: None,
            },
            |_| Some(Err(anyhow::anyhow!("no job signer configured"))),
        );
        assert_eq!(v["ok"], serde_json::json!(false));
        assert_eq!(v["effect_state"], serde_json::json!("failed"));
        assert_eq!(v["disposition"], serde_json::json!("terminal_failed"));
    }
}
