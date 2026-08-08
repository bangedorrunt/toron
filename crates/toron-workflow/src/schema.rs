// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! YAML workflow schema (bd-osp, ADR-0003 Phase 2).
//!
//! Buzz-adapted automation: triggers fire on bus events, actions publish
//! events back. `{{trigger}}` / `{{steps}}` template syntax + evalexpr guards
//! (evaluated with a 100ms timeout — the Buzz pattern). Resonate durability is
//! wired in bd-oge; this crate owns the schema + lint.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A complete workflow definition (one YAML document).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    /// Workflow name (identifier used in logs, runs, approvals).
    pub name: String,
    /// Human description (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The trigger that fires this workflow.
    pub trigger: Trigger,
    /// Optional evalexpr guard over the trigger payload (100ms timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard: Option<String>,
    /// Steps executed when the trigger fires (ordered).
    #[serde(default)]
    pub steps: Vec<Step>,
}

/// A workflow trigger (one of the ADR Phase-2 set).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    /// A message was posted to a channel/project.
    MessagePosted {
        #[serde(default)]
        channel: Option<String>,
        #[serde(default)]
        project: Option<String>,
    },
    /// A reaction (kind-7) was added.
    ReactionAdded {
        #[serde(default)]
        channel: Option<String>,
        #[serde(default)]
        emoji: Option<String>,
    },
    /// A file reservation was acquired.
    ReservationAcquired {
        project: String,
        #[serde(default)]
        path: Option<String>,
    },
    /// A file reservation was released.
    ReservationReleased {
        project: String,
        #[serde(default)]
        path: Option<String>,
    },
    /// An agent's working-state changed.
    AgentStateChanged {
        agent: String,
        #[serde(default)]
        to_state: Option<String>,
    },
    /// A cron schedule.
    Schedule { cron: String },
    /// An external webhook fired.
    Webhook {
        #[serde(default)]
        event: Option<String>,
    },
}

impl Trigger {
    /// The canonical trigger type name (as written in YAML).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::MessagePosted { .. } => "message_posted",
            Self::ReactionAdded { .. } => "reaction_added",
            Self::ReservationAcquired { .. } => "reservation_acquired",
            Self::ReservationReleased { .. } => "reservation_released",
            Self::AgentStateChanged { .. } => "agent_state_changed",
            Self::Schedule { .. } => "schedule",
            Self::Webhook { .. } => "webhook",
        }
    }
}

/// A workflow step: an action plus optional guard/name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    /// Optional step name (referenced by `{{steps.<name>}}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Optional evalexpr guard (100ms timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard: Option<String>,
    /// Failure policy for this step (ADR-0005 Spec 0). Defaults to
    /// [`StepOnError::Continue`] — the pre-P0 behavior (errors captured
    /// per-step, run completes). Opt-in `fail_run` aborts the run with a
    /// deterministic failed outcome.
    #[serde(default, skip_serializing_if = "StepOnError::is_continue")]
    pub on_error: StepOnError,
    /// The action to run.
    #[serde(flatten)]
    pub action: Action,
}

/// A step's failure policy (ADR-0005 Spec 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StepOnError {
    /// Capture the error per-step (`{"ok":false,"error":…}`) and continue the
    /// run — the pre-P0 behavior (backward compat).
    #[default]
    Continue,
    /// Fail the run: `run_workflow` RETURNS a failed-outcome value
    /// (`{"status":"failed","error":…,"failed_step":…}`) — a deterministic
    /// durable return, never a panic. (The embedding maps that value to the
    /// kind-46012 terminal `status=failed`.)
    FailRun,
}

impl StepOnError {
    /// True for the default `Continue` (drives serde skip).
    pub fn is_continue(&self) -> bool {
        matches!(self, Self::Continue)
    }
}

/// A workflow action (one of the ADR Phase-2 set).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum Action {
    /// Publish a kind-14 message (to/cc/bcc supported by the mailbox).
    SendMessage {
        to: Vec<String>,
        subject: String,
        body: String,
        #[serde(default)]
        project: Option<String>,
    },
    /// Send a private DM to one agent.
    SendDm {
        to: String,
        body: String,
        #[serde(default)]
        project: Option<String>,
    },
    /// Add a reaction (kind-7) to a message.
    React { message_id: String, emoji: String },
    /// Dispatch a job to an agent (kind-43001; bounded semaphore A7).
    DispatchJob {
        assignee: String,
        spec: serde_json::Value,
        #[serde(default)]
        channel: Option<String>,
    },
    /// Call an external webhook (SSRF-guarded in the engine, bd-k8s).
    CallWebhook {
        url: String,
        #[serde(default)]
        method: Option<String>,
        #[serde(default)]
        body: Option<String>,
    },
    /// Request a human approval (suspends the run — WF-08 fix is bd-oge).
    RequestApproval { message: String },
    /// Wait (in-process cap 270s per Buzz; Resonate unbounded in bd-oge).
    Delay { secs: u64 },
}

impl Action {
    /// The canonical action name (as written in YAML).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::SendMessage { .. } => "send_message",
            Self::SendDm { .. } => "send_dm",
            Self::React { .. } => "react",
            Self::DispatchJob { .. } => "dispatch_job",
            Self::CallWebhook { .. } => "call_webhook",
            Self::RequestApproval { .. } => "request_approval",
            Self::Delay { .. } => "delay",
        }
    }
}

/// Parse a workflow from YAML text.
pub fn parse_workflow(yaml: &str) -> Result<Workflow, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
}

/// Parse many workflows from a YAML document stream (multi-doc).
pub fn parse_workflows(yaml: &str) -> Result<Vec<Workflow>, serde_yaml::Error> {
    serde_yaml::Deserializer::from_str(yaml)
        .map(Workflow::deserialize)
        .collect()
}

/// The 7 ADR Phase-2 trigger types (validated against `Trigger::type_name`).
pub const TRIGGER_TYPES: [&str; 7] = [
    "message_posted",
    "reaction_added",
    "reservation_acquired",
    "reservation_released",
    "agent_state_changed",
    "schedule",
    "webhook",
];

/// The 7 ADR Phase-2 action types (validated against `Action::type_name`).
pub const ACTION_TYPES: [&str; 7] = [
    "send_message",
    "send_dm",
    "react",
    "dispatch_job",
    "call_webhook",
    "request_approval",
    "delay",
];

/// Template-payload access for `{{trigger.<field>}}` / `{{steps.<name>}}`.
/// The engine substitutes these at run time; this is the key contract so lint
/// can validate referenced names.
#[derive(Debug, Clone, Default)]
pub struct TemplateContext {
    /// Fields available under `trigger.*`.
    pub trigger: BTreeMap<String, serde_json::Value>,
    /// Step outputs available under `steps.<name>.*`.
    pub steps: BTreeMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_example_message_posted_workflow() {
        let yaml = r##"
name: triage
description: Triage inbound messages
trigger:
  type: message_posted
  channel: "#support"
guard: "trigger.channel == '#support'"
steps:
  - name: ack
    action: send_dm
    to: "pi-alpha"
    body: "Received {{trigger.subject}}"
  - name: dispatch
    action: dispatch_job
    assignee: "oc-beta"
    spec: { task: "triage {{trigger.id}}" }
"##;
        let wf = parse_workflow(yaml).expect("parse");
        assert_eq!(wf.name, "triage");
        assert_eq!(wf.trigger.type_name(), "message_posted");
        assert_eq!(wf.guard.as_deref(), Some("trigger.channel == '#support'"));
        assert_eq!(wf.steps.len(), 2);
        assert_eq!(wf.steps[0].name.as_deref(), Some("ack"));
        assert!(matches!(wf.steps[0].action, Action::SendDm { .. }));
        assert!(matches!(wf.steps[1].action, Action::DispatchJob { .. }));
    }

    #[test]
    fn parse_all_seven_triggers() {
        let cases: &[(&str, &str)] = &[
            ("message_posted", "type: message_posted\n  channel: x"),
            ("reaction_added", "type: reaction_added\n  channel: x"),
            (
                "reservation_acquired",
                "type: reservation_acquired\n  project: demo",
            ),
            (
                "reservation_released",
                "type: reservation_released\n  project: demo",
            ),
            (
                "agent_state_changed",
                "type: agent_state_changed\n  agent: a",
            ),
            ("schedule", "type: schedule\n  cron: '* * * * *'"),
            ("webhook", "type: webhook\n  event: deploy"),
        ];
        for (expect, yaml) in cases {
            let wf = parse_workflow(&format!("name: t\ntrigger:\n  {yaml}"))
                .unwrap_or_else(|e| panic!("{expect}: {e}"));
            assert_eq!(wf.trigger.type_name(), *expect);
        }
    }

    #[test]
    fn parse_all_seven_actions() {
        let step = |action_yaml: &str| {
            // Indent continuation lines so they sit under the `- ` list item.
            let indented = action_yaml.replace('\n', "\n    ");
            parse_workflow(&format!(
                "name: t\ntrigger:\n  type: webhook\nsteps:\n  - {indented}"
            ))
            .expect("parse")
            .steps
            .remove(0)
            .action
        };
        assert!(matches!(
            step("action: send_message\nto: [a]\nsubject: s\nbody: b"),
            Action::SendMessage { .. }
        ));
        assert!(matches!(
            step("action: send_dm\nto: a\nbody: b"),
            Action::SendDm { .. }
        ));
        assert!(matches!(
            step("action: react\nmessage_id: m\nemoji: '+1'"),
            Action::React { .. }
        ));
        assert!(matches!(
            step("action: dispatch_job\nassignee: a\nspec: {}"),
            Action::DispatchJob { .. }
        ));
        assert!(matches!(
            step("action: call_webhook\nurl: https://x"),
            Action::CallWebhook { .. }
        ));
        assert!(matches!(
            step("action: request_approval\nmessage: ok?"),
            Action::RequestApproval { .. }
        ));
        assert!(matches!(
            step("action: delay\nsecs: 5"),
            Action::Delay { .. }
        ));
    }

    #[test]
    fn invalid_yaml_fails() {
        assert!(parse_workflow("name: [broken").is_err());
        assert!(parse_workflow("").is_err());
    }

    #[test]
    fn on_error_typo_is_rejected() {
        // The strict enum is the fail-stop: any spelling other than
        // continue/fail_run fails to parse (ADR-0005 Spec 0 lint contract).
        let yaml = "name: t\ntrigger:\n  type: webhook\nsteps:\n  - action: delay\n    secs: 1\n    on_error: failrun\n";
        let err = parse_workflow(yaml).expect_err("typo must fail parse");
        assert!(
            err.to_string().contains("unknown variant") || err.to_string().contains("on_error"),
            "typo fail-stop: {err}"
        );
    }

    #[test]
    fn on_error_parse_roundtrip() {
        let yaml = "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n    on_error: fail_run\n";
        let wf = parse_workflow(yaml).expect("fail_run parses");
        assert_eq!(wf.steps[0].on_error, StepOnError::FailRun);

        let yaml = "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n    on_error: continue\n";
        let wf = parse_workflow(yaml).expect("continue parses");
        assert_eq!(wf.steps[0].on_error, StepOnError::Continue);

        // Default (omitted) = Continue.
        let yaml = "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n";
        let wf = parse_workflow(yaml).expect("omitted parses");
        assert_eq!(wf.steps[0].on_error, StepOnError::Continue);
    }

    #[test]
    fn multi_document_stream() {
        let yaml = "---\nname: a\ntrigger:\n  type: webhook\n---\nname: b\ntrigger:\n  type: schedule\n  cron: '* * * * *'\n";
        let wfs = parse_workflows(yaml).expect("stream");
        assert_eq!(wfs.len(), 2);
        assert_eq!(wfs[0].name, "a");
        assert_eq!(wfs[1].name, "b");
    }
}
