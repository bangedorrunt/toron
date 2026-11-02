// governed-by: ADR-0003
// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! toron-workflow — Phase 2 automation engine schema + lint (bd-osp).
//!
//! Buzz-adapted YAML workflows (ADR-0003 Phase 2): triggers fire on bus
//! events, actions publish events back. This crate owns:
//! - [`schema`]: the YAML models (7 triggers, 7 actions, guards, templates).
//! - [`lint`]: static validation (guard syntax via evalexpr, cron shape,
//!   field requirements, the Buzz 100ms guard timeout + 270s delay-cap
//!   patterns).
//!
//! Resonate durability (the Buzz WF-08 crash-resume fix) is wired by bd-oge.
//! The cron host loop and the kernel port reach the framework through
//! `toron_core::kernel`; this crate never names an elm-effect crate directly.

pub mod channels;
pub mod coalesce;
pub mod cron;
pub mod cron_core;
pub mod cron_elm;
pub mod cron_failure;
pub mod delivery_cursor;
pub mod dispatch;
pub mod engine;
pub mod guard;
pub mod jobs;
pub mod lifecycle;
pub mod lint;
pub mod mention;
pub mod playbook_lint;
pub mod schema;
pub mod trigger_filter;
// ADR-0032 Spec 2 + Spec 3 (bd-htoo): closed contracts + canonical identity.
pub mod action_contract;
pub mod identity;

pub use channels::{
    CHANNEL_CREATE, CHANNEL_CREATE_LEGACY, CHANNEL_MEMBERS, CHANNEL_MESSAGE,
    CHANNEL_MESSAGE_LEGACY, CHANNEL_METADATA_LEGACY, Channel, ChannelEvent, ChannelStore, REACTION,
};
pub use cron::{
    CronRegistration, CronRegistry, CronSpecRegistration, CronSpecRegistry,
    latest_due_at_or_before, next_due,
};
pub use cron_elm::{CronFire, ElmCron};
pub use cron_failure::{
    FailureBundle, PolicyEvent, PolicySink, evaluate, failure_details, is_failed,
};

pub use coalesce::{
    CoalesceDecision, Coalesced, DEFAULT_MAX_BUFFER, DEFAULT_SUBWINDOW, DEFAULT_WINDOW, DlqEntry,
    DlqReason, FlushOutcome, InboundMention, MentionCoalescer, REQUEUE_BACKOFF_BASE_MS,
    REQUEUE_BACKOFF_CAP_MS, REQUEUE_MAX_ATTEMPTS, RequeueOutcome, RequeueSchedule,
    subwindow_from_env, transient_backoff_delay_ms, window_from_env,
};
pub use dispatch::{DispatchAcquire, DispatchConfig, DispatchPermit, DispatchPool, QueuedJob};
pub use engine::{
    Decision, ProfileResolver, RunInput, SpawnSink, WorkflowEngine, WorkflowResolver,
    build_workflow_run_terminal, stamp_workflow_emitted,
};
pub use guard::{
    DEFAULT_TIMEOUT_MS, GuardOutcome, evaluate_guard, evaluate_guard_default,
    inject_buzz_functions, timeout_from_env,
};
pub use jobs::{
    JobError, JobRequest, JobResult, JobState, build_and_serialize, build_job_cancel,
    build_job_claim, build_job_error, build_job_progress, build_job_request, build_job_result,
    first_claimer, job_parent, job_request_json, job_state, kind_for_step,
};
pub use lint::{
    Level, LintIssue, LintReport, lint_workflow, lint_workflow_with_principles, lint_yaml,
};
pub use mention::{
    Mention, build_job_from_coalesced, content_mentions, is_mention, parse_mentions,
    wire_mention_dispatch, wire_mention_dispatch_with_cursor,
};
pub use schema::{
    Action, CrewSpec, CronPolicy, FailureAction, Graph, GraphNode, InputDecl, Phase, Step,
    StepOnError, TemplateContext, Trigger, Workflow, parse_workflow, parse_workflows,
};
pub use trigger_filter::{
    GIFT_WRAP, JOB_EMITS_KINDS, WORKFLOW_EMITTED_TAG, has_workflow_emitted_tag,
    is_workflow_emitted_kind, job_kind, should_evaluate_trigger_event,
    should_evaluate_trigger_kind, workflow_run_kind,
};

/// The canonical example workflow (lints clean — asserted in tests).
pub const EXAMPLE_WORKFLOW: &str = include_str!("../examples/triage.yaml");

/// The lint gate: `workflow lint` exit 0 on clean, 1 on any error.
pub fn lint_file(yaml: &str) -> Result<LintReport, String> {
    lint_yaml(yaml)
}

pub use delivery_cursor::{
    DeliveryCursor, DeliveryCursorStore, DeliveryEffect, ReservationGuard,
    agent_key_is_not_delivery_proof,
};
pub use lifecycle::{LifecycleEffect, WorkflowLifecycle, WorkflowRecord};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_workflow_lints_clean() {
        let report = lint_yaml(EXAMPLE_WORKFLOW).expect("example parses");
        assert!(
            report.is_clean(),
            "example must lint clean: {:?}",
            report.issues
        );
    }
}
