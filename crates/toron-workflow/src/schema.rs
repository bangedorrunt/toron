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
    /// The trigger that fires this workflow. `None` → invoke-only (ADR-0005
    /// Spec 2: cannot fire reactively, only via `WorkflowEngine::invoke`).
    /// `Some` → reactive AND invocable.
    #[serde(default)]
    pub trigger: Option<Trigger>,
    /// Cron failure/success policy (ADR-0005 Spec 3). Only meaningful for a
    /// `schedule` trigger; the cron manager + smart cron consume it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<CronPolicy>,
    /// Optional evalexpr guard over the trigger payload (100ms timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard: Option<String>,
    /// Steps executed when the trigger fires (ordered).
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Opt-in DAG (`graph:`) — the general case; `steps:` is the linear
    /// special case (ADR-0005 Spec 4b). When present, the engine runs the
    /// graph instead of `steps`. Edges are DERIVED from `needs:` (single
    /// encoding; `edges:` was dropped as redundant).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Graph>,
    /// Named inputs for the on-demand invoke path (ADR-0005 Spec 2). On
    /// invoke, the engine synthesizes a `trigger.*` template context from the
    /// provided inputs so `{{inputs.x}}` / `{{trigger.x}}` resolve.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<InputDecl>,
    /// Auto-crew (ADR-0005 Spec 4c). `Some(Auto)` → the orchestrator infers
    /// the crew from the workflow's own `profile:` refs. `Some(Explicit(map))`
    /// → role:count squad spawned once. `None` → no crew (each spawn is
    /// standalone). The ENGINE does not spawn — the orchestrator (flywheel)
    /// resolves the crew via the injected `ProfileResolver` DI seam; the
    /// engine only consumes `@crew.<role>` fan-out counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crew: Option<CrewSpec>,
}

/// Auto-crew declaration (ADR-0005 Spec 4c). Deserializes both `crew: auto`
/// (the bare string) and `crew: { role: count }` (a map).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum CrewSpec {
    /// `crew: auto` — infer members from the workflow's `profile:` refs
    /// (count = max parallelism the graph demands). Zero-config.
    Auto,
    /// `crew: { role: count }` — spawn this squad once, bind `@crew.<role>`.
    Explicit(BTreeMap<String, u32>),
}

impl<'de> serde::Deserialize<'de> for CrewSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Auto(String),
            Explicit(BTreeMap<String, u32>),
        }
        match Repr::deserialize(deserializer)? {
            Repr::Auto(s) if s == "auto" => Ok(Self::Auto),
            Repr::Explicit(map) => Ok(Self::Explicit(map)),
            other => Err(serde::de::Error::custom(format!(
                "crew must be `auto` or a {{role: count}} map, got {other:?}"
            ))),
        }
    }
}

impl CrewSpec {
    /// The crew role names this spec declares (explicit map keys; auto is
    /// resolved by the orchestrator).
    pub fn roles(&self) -> Vec<String> {
        match self {
            Self::Auto => Vec::new(),
            Self::Explicit(map) => map.keys().cloned().collect(),
        }
    }
}

/// A named workflow input declaration (ADR-0005 Spec 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDecl {
    /// Input name (referenced as `{{inputs.<name>}}`).
    pub name: String,
}

/// A workflow DAG (`graph:` — ADR-0005 Spec 4b). Edges are derived from each
/// node's `needs:` (a node runs when ALL its `needs` have produced output);
/// independent nodes run concurrently within the one Resonate run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Graph {
    /// node name → node definition.
    pub nodes: BTreeMap<String, GraphNode>,
}

/// One DAG node: an action plus its dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    /// The node's action (the 7 existing actions; control-flow actions land
    /// in P5.3).
    #[serde(flatten)]
    pub action: Action,
    /// Node names that must complete first (edges derived from this).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub needs: Vec<String>,
    /// Optional evalexpr guard (100ms timeout).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guard: Option<String>,
    /// Failure policy for this node (ADR-0005 Spec 0).
    #[serde(default, skip_serializing_if = "StepOnError::is_continue")]
    pub on_error: StepOnError,
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
    /// A cron schedule (ADR-0005 Spec 3). The owning profile + signing scope
    /// live here; the failure/success `policy` is a workflow-level field (the
    /// cron manager + smart cron consume both).
    Schedule {
        cron: String,
        /// The profile that OWNS this cron (Spec 3): the fixer's identity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner: Option<String>,
        /// Signing scope (Spec 1 scope rule): host = operator identity;
        /// project = a project-scoped agent.
        #[serde(default)]
        scope: CronScope,
    },
    /// An external webhook fired.
    Webhook {
        #[serde(default)]
        event: Option<String>,
    },
}

/// The signing scope for a `schedule` trigger (ADR-0005 Spec 3 / Spec 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CronScope {
    /// Host scope: the operator identity signs (cross-project maintenance).
    #[default]
    Host,
    /// Project scope: a project-scoped agent signs.
    Project,
}

/// What to do on a cron run's failure/success (ADR-0005 Spec 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FailureAction {
    /// Log the failure (no external effect).
    #[default]
    Log,
    /// Open a `br` bead with the failure bundle.
    Bead,
    /// Escalate (kind-14 DM to the captain, importance=high).
    Escalate,
}

/// The cron policy block (ADR-0005 Spec 3): how a schedule run's outcome is
/// handled by smart cron.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronPolicy {
    /// `on_failure` — the first-failure action.
    #[serde(default)]
    pub on_failure: FailureAction,
    /// `on_repeat_failure` — after `repeat_threshold` consecutive failures.
    #[serde(default)]
    pub on_repeat_failure: FailureAction,
    /// Consecutive failures before `on_repeat_failure` triggers.
    #[serde(default = "default_repeat_threshold")]
    pub repeat_threshold: u32,
    /// `on_success` — the success action.
    #[serde(default)]
    pub on_success: FailureAction,
    /// In-run retries before declaring failure.
    #[serde(default)]
    pub max_retries: u32,
    /// Abort a spawn-await after N seconds unclaimed (default 600s).
    #[serde(default = "default_no_claim_timeout")]
    pub no_claim_timeout: u64,
}

/// Default repeat threshold (3 consecutive failures).
fn default_repeat_threshold() -> u32 {
    3
}

/// Default spawn-await claim timeout (600s = 10m).
fn default_no_claim_timeout() -> u64 {
    600
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
    /// Retry a sub-step with max attempts + backoff (ADR-0005 Spec 4).
    Retry {
        #[serde(default = "default_max_attempts")]
        max_attempts: u32,
        #[serde(default)]
        backoff_secs: u64,
        step: Box<Step>,
    },
    /// Branch: evaluate a guard; run the `then` or `else` step list
    /// (ADR-0005 Spec 4). The guard is `if:` (renamed from `if_guard` to
    /// avoid the step-level `guard` field collision under `#[serde(flatten)]`).
    Branch {
        #[serde(rename = "if")]
        if_guard: String,
        then: Vec<Step>,
        #[serde(default, rename = "else")]
        else_steps: Vec<Step>,
    },
    /// Iterate over a list value (`over` = template ref to a list); one
    /// sub-run per item; outputs collected under `steps.<name>` (ADR-0005
    /// Spec 4).
    Foreach {
        over: String,
        each: Box<Step>,
    },
    /// Fan out `each` across a crew role (`over: "@crew.r"`) or N elastic
    /// copies (`over: <N>`); collected into a list under `steps.<name>`
    /// (ADR-0005 Spec 4c).
    Parallel {
        over: String,
        each: Box<Step>,
    },
    /// Invoke another workflow as a sub-run; outputs under `steps.<name>`
    /// (ADR-0005 Spec 2; idempotent on sub-run-id).
    CallWorkflow {
        workflow: String,
        #[serde(default)]
        inputs: serde_json::Value,
    },
    /// Spawn an agent and optionally await its job result (ADR-0005 Spec 4a).
    /// The profile is resolved via the injected `ProfileResolver` DI seam; the
    /// embedding emits kind-43001 carrying the promise_id in its spec. With
    /// `await: true` the run SUSPENDS on the job's terminal (43004/43006);
    /// `no_claim_timeout` aborts an unclaimed job (a FAILURE, not a decision).
    Spawn {
        /// Profile name (`@housekeeper`) to spawn.
        profile: String,
        /// Optional sub-workflow the spawned agent runs.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workflow: Option<String>,
        /// `await: true` → suspend until the job's terminal event.
        #[serde(default, rename = "await")]
        await_: bool,
        /// Abort an unclaimed spawn-await after N seconds (default 600).
        #[serde(default)]
        no_claim_timeout: u64,
    },
}

/// Default retry attempts (used by `Action::Retry` serde default).
fn default_max_attempts() -> u32 {
    3
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
            Self::Retry { .. } => "retry",
            Self::Branch { .. } => "branch",
            Self::Foreach { .. } => "foreach",
            Self::Parallel { .. } => "parallel",
            Self::CallWorkflow { .. } => "call_workflow",
            Self::Spawn { .. } => "spawn",
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

// ---------------------------------------------------------------------------
// ADR-0012: cron is a DISTINCT type (not a workflow). CronSpec carries a
// schedule + owner + scope + mode + policy + steps body; the steps use the
// SAME Step/Action DSL as Workflow (one engine, two input types).
// ---------------------------------------------------------------------------

/// The cron run mode (ADR-0012 Spec 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CronMode {
    /// Headless (default; no churn).
    #[default]
    Headless,
    /// Pane mode (visible workspace churn).
    Pane,
}

/// Missed-tick catch-up policy on wake (ADR-0012 Spec 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CronCatchup {
    /// Run the most-recent due tick on wake (never a burst).
    #[default]
    Latest,
    /// Skip missed ticks entirely.
    Skip,
}

/// The cron policy block (ADR-0012 Spec 3). Cron-specific — deliberately a
/// distinct type from the workflow-level [`CronPolicy`]: a cron policy carries
/// `catchup`, which does not belong on a plain workflow (a workflow is
/// invoke/reactive; only a cron owns a schedule + catch-up).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronSpecPolicy {
    /// `on_failure` — the first-failure action.
    #[serde(default)]
    pub on_failure: FailureAction,
    /// `on_repeat_failure` — after `repeat_threshold` consecutive failures.
    #[serde(default)]
    pub on_repeat_failure: FailureAction,
    /// Consecutive failures before `on_repeat_failure` triggers.
    #[serde(default = "default_repeat_threshold")]
    pub repeat_threshold: u32,
    /// `on_success` — the success action.
    #[serde(default)]
    pub on_success: FailureAction,
    /// In-run retries before declaring failure.
    #[serde(default)]
    pub max_retries: u32,
    /// Abort a spawn-await after N seconds unclaimed (default 600s).
    #[serde(default = "default_no_claim_timeout")]
    pub no_claim_timeout: u64,
    /// Missed-tick catch-up policy (`latest` | `skip`).
    #[serde(default)]
    pub catchup: CronCatchup,
}

/// A complete cron definition (ADR-0012 — one YAML document). Distinct from
/// [`Workflow`]: a cron has `schedule` + `owner` + policy; a workflow has a
/// trigger. The `steps` body shares the Step/Action DSL with workflows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronSpec {
    /// Cron name (identifier used in logs, runs, ticks).
    pub name: String,
    /// Human description (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 5-field Vixie cron expression (`"30 3 * * 0"`).
    pub schedule: String,
    /// The owning profile (the fixer's identity).
    pub owner: String,
    /// Signing scope: host = operator identity; project = project-scoped
    /// agent.
    #[serde(default)]
    pub scope: CronScope,
    /// Run mode: headless (default) or pane.
    #[serde(default)]
    pub mode: CronMode,
    /// Failure/success + catch-up policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<CronSpecPolicy>,
    /// The body — the SAME step DSL as Workflow (shared engine; composes
    /// workflows via `call_workflow`).
    #[serde(default)]
    pub steps: Vec<Step>,
    /// Opt-in DAG (same contract as [`Workflow::graph`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Graph>,
}

/// Parse a cron from YAML text (ADR-0012).
pub fn parse_cron(yaml: &str) -> Result<CronSpec, serde_yaml::Error> {
    serde_yaml::from_str(yaml)
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

/// The P5.3 control-flow action types (ADR-0005 Spec 4) added on top of the
/// original 7.
pub const CONTROL_FLOW_ACTION_TYPES: [&str; 5] = [
    "retry",
    "branch",
    "foreach",
    "parallel",
    "call_workflow",
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
        assert_eq!(
            wf.trigger.as_ref().map(Trigger::type_name),
            Some("message_posted")
        );
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
            assert_eq!(
                wf.trigger.as_ref().map(Trigger::type_name),
                Some(*expect)
            );
        }
    }

    #[test]
    fn triggerless_workflow_is_invoke_only() {
        // No `trigger:` key → None (invoke-only, ADR-0005 Spec 2). The old
        // required-trigger schema would reject this; now it parses and cannot
        // auto-fire (the engine only runs it via invoke).
        let yaml = "name: invoke-only\ninputs:\n  - name: goal\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n";
        let wf = parse_workflow(yaml).expect("trigger-less workflow parses");
        assert!(wf.trigger.is_none(), "no trigger -> invoke-only");
        assert_eq!(wf.inputs.len(), 1);

        // Backward compat: a workflow WITH a trigger still parses as Some.
        let yaml = "name: reactive\ntrigger:\n  type: webhook\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n";
        let wf = parse_workflow(yaml).expect("triggered workflow parses");
        assert!(wf.trigger.is_some(), "triggered workflow -> Some");
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

    /// P5.3: the five control-flow actions parse (ADR-0005 Spec 4).
    #[test]
    fn parse_control_flow_actions() {
        let step = |action_yaml: &str| {
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
            step("action: retry\nmax_attempts: 3\nstep:\n  action: send_dm\n  to: a\n  body: b"),
            Action::Retry { .. }
        ));
        assert!(matches!(
            step("action: branch\nif: \"true\"\nthen:\n  - action: send_dm\n    to: a\n    body: b"),
            Action::Branch { .. }
        ));
        assert!(matches!(
            step("action: foreach\nover: \"[1]\"\neach:\n  action: send_dm\n  to: a\n  body: b"),
            Action::Foreach { .. }
        ));
        assert!(matches!(
            step("action: parallel\nover: \"2\"\neach:\n  action: send_dm\n  to: a\n  body: b"),
            Action::Parallel { .. }
        ));
        assert!(matches!(
            step("action: call_workflow\nworkflow: triage\ninputs: {}"),
            Action::CallWorkflow { .. }
        ));
    }

    /// P4.2 (ADR-0005 Spec 4a): the `spawn` action parses with await +
    /// no_claim_timeout.
    #[test]
    fn parse_spawn_action() {
        let step = |action_yaml: &str| {
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
            step("action: spawn\nprofile: \"@housekeeper\"\nworkflow: docker-upgrade\nawait: true\nno_claim_timeout: 600"),
            Action::Spawn { .. }
        ));
        // Bare spawn (no await) parses with defaults.
        assert!(matches!(
            step("action: spawn\nprofile: \"@scout\""),
            Action::Spawn { .. }
        ));
    }

    /// P3.6: the `crew:` field parses — auto and explicit (ADR-0005 Spec 4c).
    #[test]
    fn parse_crew_auto_and_explicit() {
        // crew: auto
        let yaml = "name: debate\ntrigger:\n  type: webhook\ncrew: auto\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n";
        let wf = parse_workflow(yaml).expect("crew auto parses");
        assert_eq!(wf.crew, Some(CrewSpec::Auto));

        // crew: { role: count }
        let yaml = "name: ship\ntrigger:\n  type: webhook\ncrew:\n  planner: 1\n  coder: 2\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n";
        let wf = parse_workflow(yaml).expect("crew explicit parses");
        assert_eq!(
            wf.crew,
            Some(CrewSpec::Explicit(BTreeMap::from([
                ("planner".to_string(), 1),
                ("coder".to_string(), 2),
            ])))
        );
        assert_eq!(wf.crew.unwrap().roles().len(), 2);

        // omitted → None
        let yaml = "name: plain\ntrigger:\n  type: webhook\nsteps:\n  - name: a\n    action: send_dm\n    to: x\n    body: y\n";
        let wf = parse_workflow(yaml).expect("no crew parses");
        assert_eq!(wf.crew, None);
    }

    /// P4.1 (ADR-0005 Spec 3): the schedule trigger extension parses —
    /// owner/scope/policy. Backward-compatible: bare `cron` still parses.
    #[test]
    fn parse_schedule_trigger_extension() {
        let yaml = r##"
name: weekly-upgrade
trigger:
  type: schedule
  cron: "30 3 * * 0"
  owner: housekeeper
  scope: host
policy:
  on_failure: bead
  on_repeat_failure: escalate
  repeat_threshold: 3
  on_success: log
  max_retries: 1
  no_claim_timeout: 600
steps:
  - name: upgrade
    action: send_dm
    to: pi
    body: go
"##;
        let wf = parse_workflow(yaml).expect("extended schedule parses");
        match &wf.trigger {
            Some(Trigger::Schedule { cron, owner, scope, .. }) => {
                assert_eq!(cron, "30 3 * * 0");
                assert_eq!(owner.as_deref(), Some("housekeeper"));
                assert_eq!(*scope, CronScope::Host);
            }
            other => panic!("expected Schedule trigger, got {other:?}"),
        }
        let p = wf.policy.as_ref().expect("policy present");
        assert_eq!(p.on_failure, FailureAction::Bead);
        assert_eq!(p.on_repeat_failure, FailureAction::Escalate);
        assert_eq!(p.repeat_threshold, 3);
        assert_eq!(p.on_success, FailureAction::Log);
        assert_eq!(p.max_retries, 1);
        assert_eq!(p.no_claim_timeout, 600);
    }

    /// P4.1: `scope: project` parses; defaults apply when omitted.
    #[test]
    fn parse_schedule_scope_project_and_defaults() {
        let yaml = r##"
name: project-cron
trigger:
  type: schedule
  cron: "* * * * *"
  scope: project
steps:
  - name: a
    action: send_dm
    to: pi
    body: x
"##;
        let wf = parse_workflow(yaml).expect("project scope parses");
        match &wf.trigger {
            Some(Trigger::Schedule { scope, .. }) => {
                assert_eq!(*scope, CronScope::Project);
            }
            other => panic!("expected Schedule trigger, got {other:?}"),
        }
        // Omitted policy → None (cron manager uses defaults).
        assert!(wf.policy.is_none());

        // Backward compat: bare cron (no owner/scope/policy) → defaults.
        let yaml = "name: bare\ntrigger:\n  type: schedule\n  cron: '* * * * *'\nsteps:\n  - name: a\n    action: send_dm\n    to: pi\n    body: x\n";
        let wf = parse_workflow(yaml).expect("bare cron parses");
        match &wf.trigger {
            Some(Trigger::Schedule { scope, owner, .. }) => {
                assert_eq!(*scope, CronScope::Host, "default scope = host");
                assert!(owner.is_none());
            }
            other => panic!("expected Schedule trigger, got {other:?}"),
        }
        assert!(wf.policy.is_none());
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

    // ---- ADR-0012: CronSpec is a DISTINCT type (bd-25g7.1) ----

    /// The ADR-0012 Spec 3 example (weekly-upgrade) parses verbatim —
    /// schedule + owner + scope + mode + policy (incl. catchup) + steps body.
    #[test]
    fn parse_adr_example_cron() {
        let yaml = r##"
name: weekly-upgrade
schedule: "30 3 * * 0"
owner: housekeeper
scope: host
mode: headless
policy:
  on_failure: bead
  on_repeat_failure: escalate
  repeat_threshold: 3
  on_success: log
  max_retries: 1
  no_claim_timeout: 600
  catchup: latest
steps:
  - { name: probe,  action: call_workflow, workflow: health-check }
  - { name: upgrade, action: call_workflow, workflow: docker-upgrade, if: "{{steps.probe.healthy}}" }
  - { name: verify, action: call_workflow, workflow: health-check }
"##;
        let spec = parse_cron(yaml).expect("ADR example parses");
        assert_eq!(spec.name, "weekly-upgrade");
        assert_eq!(spec.schedule, "30 3 * * 0");
        assert_eq!(spec.owner, "housekeeper");
        assert_eq!(spec.scope, CronScope::Host);
        assert_eq!(spec.mode, CronMode::Headless);
        let p = spec.policy.as_ref().expect("policy present");
        assert_eq!(p.on_failure, FailureAction::Bead);
        assert_eq!(p.on_repeat_failure, FailureAction::Escalate);
        assert_eq!(p.repeat_threshold, 3);
        assert_eq!(p.on_success, FailureAction::Log);
        assert_eq!(p.max_retries, 1);
        assert_eq!(p.no_claim_timeout, 600);
        assert_eq!(p.catchup, CronCatchup::Latest);
        assert_eq!(spec.steps.len(), 3);
        assert_eq!(spec.steps[0].name.as_deref(), Some("probe"));
        assert!(matches!(spec.steps[0].action, Action::CallWorkflow { .. }));
        assert_eq!(spec.steps[1].name.as_deref(), Some("upgrade"));
    }

    /// Minimal cron YAML: defaults apply (scope host, mode headless, policy
    /// None).
    #[test]
    fn parse_cron_defaults() {
        let yaml = "name: ping\nschedule: '* * * * *'\nowner: scout\nsteps:\n  - action: send_dm\n    to: pi\n    body: x\n";
        let spec = parse_cron(yaml).expect("minimal cron parses");
        assert_eq!(spec.scope, CronScope::Host, "default scope = host");
        assert_eq!(spec.mode, CronMode::Headless, "default mode = headless");
        assert!(spec.policy.is_none(), "omitted policy -> None");
    }

    /// Policy defaults: a partial policy block applies per-field defaults
    /// (catchup included).
    #[test]
    fn parse_cron_policy_defaults() {
        let yaml = "name: p\nschedule: '* * * * *'\nowner: o\npolicy:\n  catchup: skip\nsteps: []\n";
        let spec = parse_cron(yaml).expect("partial policy parses");
        let p = spec.policy.as_ref().expect("policy present");
        assert_eq!(p.on_failure, FailureAction::Log, "default on_failure");
        assert_eq!(p.repeat_threshold, 3, "default repeat_threshold");
        assert_eq!(p.no_claim_timeout, 600, "default no_claim_timeout");
        assert_eq!(p.catchup, CronCatchup::Skip);
    }

    /// scope/mode variants parse (project + pane).
    #[test]
    fn parse_cron_scope_and_mode_variants() {
        let yaml = "name: p\nschedule: '* * * * *'\nowner: o\nscope: project\nmode: pane\nsteps: []\n";
        let spec = parse_cron(yaml).expect("variants parse");
        assert_eq!(spec.scope, CronScope::Project);
        assert_eq!(spec.mode, CronMode::Pane);
    }

    /// A cron is NOT a workflow: top-level `trigger:` is meaningless in a
    /// cron YAML and must not sneak in via the workflow schema.
    #[test]
    fn cron_yaml_has_no_trigger() {
        let yaml = "name: p\nschedule: '* * * * *'\nowner: o\nsteps: []\n";
        let spec = parse_cron(yaml).expect("cron parses");
        assert_eq!(spec.steps.len(), 0);
        // The cron YAML does not carry a workflow trigger — no trigger field
        // exists on CronSpec (distinct type, ADR-0012).
        let _ = spec;
    }

    #[test]
    fn invalid_cron_yaml_fails() {
        assert!(parse_cron("name: [broken").is_err());
        assert!(parse_cron("").is_err());
        // Missing required schedule/owner -> parse error (required fields).
        assert!(parse_cron("name: p\nsteps: []\n").is_err());
    }
}
