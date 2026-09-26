// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! Closed per-variant action contracts (ADR-0032 Spec 2).
//!
//! One exhaustive, co-located [`ActionContract`] for every closed [`Action`]
//! variant: YAML name, action class, host sink, template-capable fields,
//! output shape, effect policy, and execution bounds. The same table is
//! consumed at three gates — parse/normalize (structural validation),
//! lint (authoring errors), and invoke/output-validation — so a new variant
//! without a contract entry fails the exhaustiveness test, and a contract
//! that disagrees with the engine partition fails the agreement test.
//! No open runtime registry: [`contract_for`] is an exhaustive match, so a
//! new variant without an entry is a compile error.

use crate::engine::{ActionErrorCode, ActionPartition, partition};
use crate::schema::Action;

/// What an action IS (ADR-0032 Spec 2 contract table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionClass {
    /// Pure data transform with no durable or external consequence.
    Pure,
    /// Sequenced by the engine (DAG/composite control); never dispatched.
    DurableControl,
    /// Parks the run on a promise the host lifecycle drains.
    Parked,
    /// Reads external state without changing it.
    ReadOnlyExternal,
    /// Changes external state through a named host sink.
    EffectfulExternal,
}

/// The declared output shape of one action (ADR-0032 Spec 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputKind {
    /// Exactly `Null` (unit actions).
    Unit,
    /// A JSON array (ordered collection results).
    Array,
    /// A JSON object (child-run and envelope results).
    Object,
    /// An object carrying a boolean `ok` field.
    ObjectWithOk,
    /// `{"branch": "then"|"else", "outputs": {...}}`.
    Branch,
    /// Any non-`Null` value (host-runner results validated downstream).
    AnyNonNull,
}

/// One contract violation: field, stable code, human message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub field: &'static str,
    pub code: ActionErrorCode,
    pub message: String,
}

/// The effect policy of one action: where its effect lives and which sink
/// owns it. This names the sink and the action class — it never claims
/// provider idempotency or exactly-once execution (those need the
/// wave-3 journal plus a provider contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectKind {
    /// Engine-sequenced control: no direct external effect.
    NoneControl,
    /// Parks on a host-drained promise (`sink` names the drain).
    Parked {
        /// Sink/drain name (`approval`, `spawn`).
        sink: &'static str,
    },
    /// Dispatched through a named host sink.
    External {
        /// Sink name (`mailbox`, `job`, `verify`).
        sink: &'static str,
    },
}

/// One exhaustive contract entry per closed action variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionContract {
    /// Canonical YAML type name (matches `Action::type_name()`).
    pub type_name: &'static str,
    /// What the action is.
    pub class: ActionClass,
    /// Its effect policy.
    pub effect: EffectKind,
    /// String fields that render `{{...}}` templates, by YAML field name.
    pub template_fields: &'static [&'static str],
    /// Declared output shape.
    pub output: OutputKind,
    /// `false` for deprecated variants with no wired sink: they parse and
    /// lint for migration but never execute (fail-closed).
    pub supported: bool,
}

const SEND_MESSAGE: ActionContract = ActionContract {
    type_name: "send_message",
    class: ActionClass::EffectfulExternal,
    effect: EffectKind::External { sink: "mailbox" },
    template_fields: &["to", "subject", "body", "project"],
    output: OutputKind::AnyNonNull,
    supported: true,
};
const SEND_DM: ActionContract = ActionContract {
    type_name: "send_dm",
    class: ActionClass::EffectfulExternal,
    effect: EffectKind::External { sink: "mailbox" },
    template_fields: &["to", "body", "project"],
    output: OutputKind::AnyNonNull,
    supported: true,
};
const REACT: ActionContract = ActionContract {
    type_name: "react",
    class: ActionClass::EffectfulExternal,
    effect: EffectKind::External { sink: "reaction" },
    template_fields: &["message_id", "emoji"],
    output: OutputKind::AnyNonNull,
    supported: false,
};
const DISPATCH_JOB: ActionContract = ActionContract {
    type_name: "dispatch_job",
    class: ActionClass::EffectfulExternal,
    effect: EffectKind::External { sink: "job" },
    template_fields: &["assignee", "channel", "spec"],
    output: OutputKind::AnyNonNull,
    supported: true,
};
const CALL_WEBHOOK: ActionContract = ActionContract {
    type_name: "call_webhook",
    class: ActionClass::EffectfulExternal,
    effect: EffectKind::External { sink: "webhook" },
    template_fields: &["url", "method", "body"],
    output: OutputKind::AnyNonNull,
    supported: false,
};
const REQUEST_APPROVAL: ActionContract = ActionContract {
    type_name: "request_approval",
    class: ActionClass::Parked,
    effect: EffectKind::Parked { sink: "approval" },
    template_fields: &["message"],
    output: OutputKind::Object,
    supported: true,
};
const DELAY: ActionContract = ActionContract {
    type_name: "delay",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &[],
    output: OutputKind::Unit,
    supported: true,
};
const RETRY: ActionContract = ActionContract {
    type_name: "retry",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &[],
    output: OutputKind::AnyNonNull,
    supported: true,
};
const BRANCH: ActionContract = ActionContract {
    type_name: "branch",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &[],
    output: OutputKind::Branch,
    supported: true,
};
const FOREACH: ActionContract = ActionContract {
    type_name: "foreach",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &["over"],
    output: OutputKind::Array,
    supported: true,
};
const PARALLEL: ActionContract = ActionContract {
    type_name: "parallel",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &["over"],
    output: OutputKind::Array,
    supported: true,
};
const CALL_WORKFLOW: ActionContract = ActionContract {
    type_name: "call_workflow",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &["workflow", "inputs"],
    output: OutputKind::Object,
    supported: true,
};
const SPAWN: ActionContract = ActionContract {
    type_name: "spawn",
    class: ActionClass::Parked,
    effect: EffectKind::Parked { sink: "spawn" },
    template_fields: &["profile", "workflow"],
    output: OutputKind::ObjectWithOk,
    supported: true,
};
const VERIFY: ActionContract = ActionContract {
    type_name: "verify",
    class: ActionClass::EffectfulExternal,
    effect: EffectKind::External { sink: "verify" },
    template_fields: &["command"],
    output: OutputKind::ObjectWithOk,
    supported: true,
};
const REDUCE: ActionContract = ActionContract {
    type_name: "reduce",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &["over", "template"],
    output: OutputKind::AnyNonNull,
    supported: true,
};
const LOOP: ActionContract = ActionContract {
    type_name: "loop",
    class: ActionClass::DurableControl,
    effect: EffectKind::NoneControl,
    template_fields: &["max", "until"],
    output: OutputKind::ObjectWithOk,
    supported: true,
};

/// The contract for one action. Exhaustive over [`Action`]: a new variant
/// without an arm is a compile error.
pub const fn contract_for(action: &Action) -> &'static ActionContract {
    match action {
        Action::SendMessage { .. } => &SEND_MESSAGE,
        Action::SendDm { .. } => &SEND_DM,
        Action::React { .. } => &REACT,
        Action::DispatchJob { .. } => &DISPATCH_JOB,
        Action::CallWebhook { .. } => &CALL_WEBHOOK,
        Action::RequestApproval { .. } => &REQUEST_APPROVAL,
        Action::Delay { .. } => &DELAY,
        Action::Retry { .. } => &RETRY,
        Action::Branch { .. } => &BRANCH,
        Action::Foreach { .. } => &FOREACH,
        Action::Parallel { .. } => &PARALLEL,
        Action::CallWorkflow { .. } => &CALL_WORKFLOW,
        Action::Spawn { .. } => &SPAWN,
        Action::Verify { .. } => &VERIFY,
        Action::Reduce { .. } => &REDUCE,
        Action::Loop { .. } => &LOOP,
    }
}

const fn violation(field: &'static str, code: ActionErrorCode, message: String) -> Violation {
    Violation {
        field,
        code,
        message,
    }
}

fn non_empty(field: &'static str, value: &str, type_name: &str) -> Option<Violation> {
    if value.trim().is_empty() {
        Some(violation(
            field,
            ActionErrorCode::InvalidInput,
            format!("{type_name} requires a non-empty `{field}`"),
        ))
    } else {
        None
    }
}

/// Validate a fully-typed action's structural contract: required fields
/// present and non-empty, cross-field rules hold, deprecated variants
/// rejected. Rendered-shape facts the types cannot carry (non-empty strings
/// after rendering, valid counts) are checked on values the engine already
/// rendered. Returns every violation found (empty = valid).
pub fn validate_action(action: &Action) -> Vec<Violation> {
    let mut out = Vec::new();
    let push = |out: &mut Vec<Violation>, v: Option<Violation>| {
        if let Some(v) = v {
            out.push(v);
        }
    };
    match action {
        Action::SendMessage { to, .. } => {
            if to.iter().all(|t| t.trim().is_empty()) {
                out.push(violation(
                    "to",
                    ActionErrorCode::InvalidInput,
                    "send_message requires at least one `to` recipient".into(),
                ));
            }
        }
        Action::SendDm { to, .. } => {
            push(&mut out, non_empty("to", to, "send_dm"));
        }
        Action::React { .. } => {
            out.push(violation(
                "action",
                ActionErrorCode::UnsupportedAction,
                "react is deprecated with no wired sink; remove the step".into(),
            ));
        }
        Action::DispatchJob { assignee, .. } => {
            push(&mut out, non_empty("assignee", assignee, "dispatch_job"));
        }
        Action::CallWebhook { .. } => {
            out.push(violation(
                "action",
                ActionErrorCode::UnsupportedAction,
                "call_webhook is deprecated with no wired sink and unmet SSRF contract; remove the step".into(),
            ));
        }
        Action::RequestApproval { .. } => {}
        Action::Delay { .. } => {}
        Action::Retry {
            max_attempts, step, ..
        } => {
            if *max_attempts == 0 {
                out.push(violation(
                    "max_attempts",
                    ActionErrorCode::InvalidInput,
                    "retry requires max_attempts >= 1".into(),
                ));
            }
            if matches!(step.action, Action::Retry { .. }) {
                out.push(violation(
                    "step",
                    ActionErrorCode::InvalidInput,
                    "retry must not nest directly inside retry; flatten to one wrapper".into(),
                ));
            }
        }
        Action::Branch { if_guard, .. } => {
            push(&mut out, non_empty("if", if_guard, "branch"));
        }
        Action::Foreach { over, .. } => {
            push(&mut out, non_empty("over", over, "foreach"));
        }
        Action::Parallel { over, .. } => {
            push(&mut out, non_empty("over", over, "parallel"));
        }
        Action::CallWorkflow { workflow, .. } => {
            push(&mut out, non_empty("workflow", workflow, "call_workflow"));
        }
        Action::Spawn { profile, .. } => {
            push(&mut out, non_empty("profile", profile, "spawn"));
        }
        Action::Loop { max, body, .. } => {
            push(&mut out, non_empty("max", max, "loop"));
            if body.is_empty() {
                out.push(violation(
                    "body",
                    ActionErrorCode::InvalidInput,
                    "loop requires a non-empty body".into(),
                ));
            }
        }
        Action::Verify { command } => {
            push(&mut out, non_empty("command", command, "verify"));
        }
        Action::Reduce {
            over,
            op,
            template,
            init,
            ..
        } => {
            push(&mut out, non_empty("over", over, "reduce"));
            // Join folds through its template, never a key.
            if *op == crate::schema::ReduceOp::Join {
                match template {
                    Some(t) if !t.trim().is_empty() => {}
                    _ => out.push(violation(
                        "template",
                        ActionErrorCode::InvalidInput,
                        "reduce join requires a non-empty `template`".into(),
                    )),
                }
            }
            // The accumulator is a literal: templates belong in `over` and
            // join `template`, so a `{{...}}` here would silently persist.
            if let Some(init) = init {
                if init.to_string().contains("{{") {
                    out.push(violation(
                        "init",
                        ActionErrorCode::InvalidInput,
                        "reduce `init` is a literal accumulator and must not contain templates"
                            .into(),
                    ));
                } else {
                    let type_ok = match op {
                        crate::schema::ReduceOp::All | crate::schema::ReduceOp::Any => {
                            init.is_boolean()
                        }
                        crate::schema::ReduceOp::Count | crate::schema::ReduceOp::Sum => {
                            init.is_number()
                        }
                        crate::schema::ReduceOp::Join => init.is_string(),
                    };
                    if !type_ok {
                        out.push(violation(
                            "init",
                            ActionErrorCode::InvalidInput,
                            format!("reduce `init` must match the {op:?} accumulator shape"),
                        ));
                    }
                }
            }
        }
    }
    out
}

/// Validate one action's declared output value against its contract.
/// `Null` is valid only for [`OutputKind::Unit`]; anything else crosses as
/// [`ActionErrorCode::InvalidOutput`].
pub fn check_output(action: &Action, value: &serde_json::Value) -> Result<(), Violation> {
    let contract = contract_for(action);
    let ok = match contract.output {
        OutputKind::Unit => value.is_null(),
        OutputKind::Array => value.is_array(),
        OutputKind::Object => value.is_object(),
        OutputKind::ObjectWithOk => {
            value
                .get("ok")
                .and_then(serde_json::Value::as_bool)
                .is_some()
                && value.is_object()
        }
        OutputKind::Branch => {
            matches!(
                value.get("branch").and_then(|v| v.as_str()),
                Some("then") | Some("else")
            ) && value
                .get("outputs")
                .is_some_and(serde_json::Value::is_object)
        }
        OutputKind::AnyNonNull => !value.is_null(),
    };
    if ok {
        Ok(())
    } else {
        Err(violation(
            "output",
            ActionErrorCode::InvalidOutput,
            format!(
                "{:?} output violates its {:?} contract: {}",
                contract.type_name,
                contract.output,
                crate::identity::canonical_json(value)
            ),
        ))
    }
}

/// Agreement witness: the contract's sink must match the engine partition
/// for every variant, so the table can never become a stale copy.
pub fn contract_partition_agrees(action: &Action) -> bool {
    let contract = contract_for(action);
    match (contract.supported, &contract.effect, partition(action)) {
        (false, _, ActionPartition::Unsupported { .. }) => true,
        (
            true,
            EffectKind::External { sink },
            ActionPartition::HostSink {
                sink: partition_sink,
            },
        ) => *sink == partition_sink,
        (true, EffectKind::Parked { .. }, ActionPartition::EngineControl) => true,
        (true, EffectKind::NoneControl, ActionPartition::EngineControl) => true,
        _ => false,
    }
}
