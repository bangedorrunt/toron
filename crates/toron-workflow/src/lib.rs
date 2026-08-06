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

pub mod dispatch;
pub mod engine;
pub mod jobs;
pub mod lint;
pub mod mention;
pub mod schema;

pub use dispatch::{DispatchConfig, DispatchPermit, DispatchPool, QueuedJob};
pub use engine::{run_workflow, ActionRunner, Decision, RunInput, WorkflowEngine};
pub use jobs::{
    build_and_serialize, build_job_cancel, build_job_claim, build_job_error, build_job_progress,
    build_job_request, build_job_result, first_claimer, job_parent, job_request_json, job_state,
    JobError, JobRequest, JobResult, JobState,
};
pub use lint::{lint_workflow, lint_yaml, Level, LintIssue, LintReport};
pub use mention::{content_mentions, is_mention, parse_mentions, Mention};
pub use schema::{
    parse_workflow, parse_workflows, Action, Step, TemplateContext, Trigger, Workflow,
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
