// governed-by: ADR-0003
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
//! Resonate durability (the Buzz WF-08 crash-resume fix) is wired by bd-oge;
//! this crate is deliberately engine-free.

pub mod channels;
pub mod coalesce;
pub mod cron;
pub mod cron_failure;
pub mod dispatch;
pub mod engine;
pub mod guard;
pub mod jobs;
pub mod lint;
pub mod mention;
pub mod schema;
pub mod trigger_filter;

pub use channels::{
    CHANNEL_CREATE, CHANNEL_MESSAGE, CHANNEL_METADATA, Channel, ChannelEvent, ChannelStore,
    REACTION,
};
pub use cron::{
    CronManager, CronRegistration, CronRegistry, CronSpecRegistration, CronSpecRegistry,
    latest_due_at_or_before, next_due,
};
pub use cron_failure::{FailureBundle, PolicyEvent, PolicySink, evaluate, failure_details, is_failed};

pub use coalesce::{
    CoalesceDecision, Coalesced, DEFAULT_MAX_BUFFER, DEFAULT_SUBWINDOW, DEFAULT_WINDOW,
    FlushOutcome, InboundMention, MentionCoalescer, subwindow_from_env, window_from_env,
};
pub use dispatch::{DispatchConfig, DispatchPermit, DispatchPool, QueuedJob};
pub use engine::{
    ActionRunner, Decision, GraphNodeInput, ParallelCopyInput, ProfileResolver, RunInput,
    SpawnSink, WorkflowEngine, WorkflowResolver, build_workflow_run_terminal, run_graph_node,
    run_parallel_copy, run_workflow, stamp_workflow_emitted,
};
pub use guard::{
    DEFAULT_TIMEOUT_MS, GuardOutcome, evaluate_guard, evaluate_guard_default,
    inject_buzz_functions, timeout_from_env,
};
pub use jobs::{
    JobError, JobRequest, JobResult, JobState, build_and_serialize, build_job_cancel,
    build_job_claim, build_job_error, build_job_progress, build_job_request, build_job_result,
    first_claimer, job_parent, job_request_json, job_state,
};
pub use lint::{Level, LintIssue, LintReport, lint_workflow, lint_yaml};
pub use mention::{Mention, content_mentions, is_mention, parse_mentions};
pub use schema::{
    Action, CrewSpec, CronPolicy, FailureAction, Graph, GraphNode, InputDecl, Step, StepOnError,
    TemplateContext, Trigger, Workflow, parse_workflow, parse_workflows,
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
