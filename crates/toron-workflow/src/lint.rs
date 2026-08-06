// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Workflow lint (bd-osp): validate a workflow file against the YAML schema,
//! guard-expression syntax, template references, and the Buzz 100ms timeout
//! pattern. No evaluation is performed (that's the engine, bd-oge) — lint is
//! a static validator with actionable errors.

use crate::schema::{Action, Trigger, Workflow};

/// The Buzz-derived guard evaluation timeout (ms). Lint enforces that guards
/// are simple expressions; the engine applies the timeout at eval time.
pub const GUARD_TIMEOUT_MS: u64 = 100;

/// The in-process `delay` action cap (Buzz: 270s; Resonate unbounded later).
pub const DELAY_CAP_SECS: u64 = 270;

/// A lint diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintIssue {
    /// Severity.
    pub level: Level,
    /// Human-readable, actionable message.
    pub message: String,
    /// 1-based line hint when known.
    #[allow(dead_code)]
    pub line: Option<usize>,
}

/// Lint severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Blocking: the workflow cannot run.
    Error,
    /// Advisory: correct but unusual.
    Warning,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
        }
    }
}

/// The lint result: issues + a pass/fail flag.
#[derive(Debug, Clone, Default)]
pub struct LintReport {
    pub issues: Vec<LintIssue>,
}

impl LintReport {
    /// True when there are no blocking issues.
    pub fn is_clean(&self) -> bool {
        !self.issues.iter().any(|i| i.level == Level::Error)
    }
}

/// Lint a parsed workflow. Returns issues (errors block, warnings advise).
pub fn lint_workflow(wf: &Workflow) -> LintReport {
    let mut report = LintReport::default();

    // Name is required and must be a valid identifier.
    if wf.name.trim().is_empty() {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: "workflow `name` must not be empty".into(),
            line: None,
        });
    } else if !wf
        .name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!(
                "workflow name {:?} contains invalid characters (alnum/_/-/.)",
                wf.name
            ),
            line: None,
        });
    }

    // Trigger type is validated by serde (unknown `type` fails parse); the
    // remaining trigger-specific checks:
    match &wf.trigger {
        Trigger::MessagePosted { channel, project } => {
            if channel.is_none() && project.is_none() {
                report.issues.push(LintIssue {
                    level: Level::Warning,
                    message: "message_posted trigger has neither channel nor project — will match all messages".into(),
                    line: None,
                });
            }
        }
        Trigger::Schedule { cron } => {
            // Validate the 5/6-field cron shape crudely (non-empty, 5+ fields).
            let fields: Vec<&str> = cron.split_whitespace().collect();
            if fields.len() < 5 || fields.len() > 6 {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: format!(
                        "schedule cron {:?} must have 5-6 fields, got {}",
                        cron,
                        fields.len()
                    ),
                    line: None,
                });
            }
        }
        Trigger::ReservationAcquired { project, .. }
        | Trigger::ReservationReleased { project, .. } => {
            if project.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "reservation trigger requires a non-empty `project`".into(),
                    line: None,
                });
            }
        }
        Trigger::AgentStateChanged { agent, .. } => {
            if agent.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "agent_state_changed trigger requires a non-empty `agent`".into(),
                    line: None,
                });
            }
        }
        Trigger::ReactionAdded { .. } | Trigger::Webhook { .. } => {}
    }

    // Workflow-level guard.
    if let Some(guard) = &wf.guard {
        check_guard(&mut report, guard, "workflow guard");
    }

    // Steps.
    if wf.steps.is_empty() {
        report.issues.push(LintIssue {
            level: Level::Warning,
            message: "workflow has no steps — trigger will fire but nothing runs".into(),
            line: None,
        });
    }
    let mut step_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for step in &wf.steps {
        if let Some(name) = &step.name {
            if !step_names.insert(name) {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: format!(
                        "duplicate step name {name:?} — {{{{steps.{name}}}}} is ambiguous"
                    ),
                    line: None,
                });
            }
        }
        if let Some(guard) = &step.guard {
            check_guard(&mut report, guard, "step guard");
        }
        check_action(&mut report, &step.action);
    }

    report
}

/// Validate a guard expression's syntax with evalexpr (no evaluation).
fn check_guard(report: &mut LintReport, guard: &str, ctx: &str) {
    // Template refs ({{trigger.x}}, {{steps.y}}) are substituted before eval;
    // the raw guard is not valid evalexpr when it references them, so lint
    // replaces template tokens with a literal before syntax-checking.
    let substituted = substitute_templates(guard);
    let parse_err = evalexpr::build_operator_tree::<evalexpr::DefaultNumericTypes>(&substituted)
        .err()
        .map(|e| e.to_string());
    // evalexpr's builder is lenient (deferred errors); also reject guards that
    // trail a binary operator or are empty after substitution — those would
    // fail at eval time, so lint flags them now.
    let trimmed = substituted.trim();
    let trailing_op = trimmed.ends_with("==")
        || trimmed.ends_with("!=")
        || trimmed.ends_with("<=")
        || trimmed.ends_with(">=")
        || trimmed.ends_with("&&")
        || trimmed.ends_with("||")
        || trimmed.ends_with('+')
        || trimmed.ends_with('-')
        || trimmed.ends_with('*')
        || trimmed.ends_with('/')
        || trimmed.ends_with('<')
        || trimmed.ends_with('>');
    let empty = trimmed.is_empty();
    if let Some(err) = parse_err {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!("{ctx} expression {guard:?} is invalid: {err}"),
            line: None,
        });
    } else if trailing_op || empty {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!("{ctx} expression {guard:?} is invalid: trailing operator or empty"),
            line: None,
        });
    } else {
        // Emit the 100ms timeout contract as an advisory.
        report.issues.push(LintIssue {
            level: Level::Warning,
            message: format!(
                "guard {:?} will be evaluated with a {GUARD_TIMEOUT_MS}ms timeout (Buzz pattern)",
                substitute_templates(guard)
            ),
            line: None,
        });
    }
}

/// Replace `{{...}}` template tokens with a placeholder literal so the guard
/// parses as evalexpr (the engine substitutes real values before eval).
fn substitute_templates(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                out.push('0');
                rest = &after[end + 2..];
            }
            None => {
                out.push_str("{{");
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Validate an action's fields.
fn check_action(report: &mut LintReport, action: &Action) {
    match action {
        Action::SendMessage {
            to, subject, body, ..
        } => {
            if to.is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "send_message action requires at least one `to` recipient".into(),
                    line: None,
                });
            }
            if subject.trim().is_empty() && body.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Warning,
                    message: "send_message action has empty subject and body".into(),
                    line: None,
                });
            }
        }
        Action::SendDm { to, body, .. } => {
            if to.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "send_dm action requires a non-empty `to`".into(),
                    line: None,
                });
            }
            if body.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Warning,
                    message: "send_dm action has an empty body".into(),
                    line: None,
                });
            }
        }
        Action::React { message_id, emoji } => {
            if message_id.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "react action requires a non-empty `message_id`".into(),
                    line: None,
                });
            }
            if emoji.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "react action requires a non-empty `emoji`".into(),
                    line: None,
                });
            }
        }
        Action::DispatchJob { assignee, .. } => {
            if assignee.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "dispatch_job action requires a non-empty `assignee`".into(),
                    line: None,
                });
            }
        }
        Action::CallWebhook { url, .. } => {
            if url.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "call_webhook action requires a non-empty `url`".into(),
                    line: None,
                });
            }
        }
        Action::RequestApproval { message } => {
            if message.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Warning,
                    message: "request_approval action has an empty prompt message".into(),
                    line: None,
                });
            }
        }
        Action::Delay { secs } => {
            if *secs > DELAY_CAP_SECS {
                report.issues.push(LintIssue {
                    level: Level::Warning,
                    message: format!(
                        "delay {secs}s exceeds the in-process cap {DELAY_CAP_SECS}s — Resonate (bd-oge) lifts this"
                    ),
                    line: None,
                });
            }
        }
    }
}

/// The complete lint entrypoint: parse YAML then lint.
pub fn lint_yaml(yaml: &str) -> Result<LintReport, String> {
    let wf = crate::schema::parse_workflow(yaml).map_err(|e| format!("yaml parse error: {e}"))?;
    Ok(lint_workflow(&wf))
}

/// Render the lint report as human lines (for `toron workflow lint`).
pub fn render(report: &LintReport) -> Vec<String> {
    report
        .issues
        .iter()
        .map(|i| format!("[{}] {}", i.level.as_str(), i.message))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_workflow;

    #[test]
    fn example_lints_clean() {
        let yaml = r##"
name: triage
trigger:
  type: message_posted
  channel: "#support"
guard: "trigger.channel == '#support'"
steps:
  - name: ack
    action: send_dm
    to: "pi-alpha"
    body: "Received"
  - name: dispatch
    action: dispatch_job
    assignee: "oc-beta"
    spec: { task: "triage" }
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report.is_clean(),
            "example must lint clean: {:?}",
            report.issues
        );
    }

    #[test]
    fn invalid_yaml_fails_with_actionable_error() {
        let bad = "name: triage\ntrigger:\n  type: message_posted\nsteps: [broken";
        let err = lint_yaml(bad).expect_err("invalid yaml fails");
        assert!(err.contains("yaml parse error"), "actionable: {err}");
        assert!(
            err.contains("message_posted") || err.contains("steps"),
            "points at the issue: {err}"
        );
    }

    #[test]
    fn unknown_trigger_type_fails_parse() {
        let yaml = "name: t\ntrigger:\n  type: no_such_trigger";
        let err = lint_yaml(yaml).expect_err("unknown trigger");
        assert!(
            err.contains("no_such_trigger"),
            "names the unknown type: {err}"
        );
    }

    #[test]
    fn invalid_guard_expression_is_an_error() {
        let yaml = r##"
name: t
trigger:
  type: webhook
guard: "trigger.channel =="
steps:
  - action: delay
    secs: 5
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("invalid")),
            "invalid guard flagged: {:?}",
            report.issues
        );
    }

    #[test]
    fn delay_over_cap_is_a_warning() {
        let yaml = r##"
name: t
trigger:
  type: webhook
steps:
  - action: delay
    secs: 3000
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.message.contains("in-process cap")),
            "over-cap delay warned: {:?}",
            report.issues
        );
    }

    #[test]
    fn guard_timeout_contract_emitted() {
        let yaml = r##"
name: t
trigger:
  type: webhook
guard: "trigger.ok"
steps:
  - action: react
    message_id: m
    emoji: "+1"
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.message.contains("100ms timeout")),
            "timeout contract advisory present: {:?}",
            report.issues
        );
    }

    #[test]
    fn template_substitution_keeps_guards_valid() {
        // Guards with {{trigger.x}} must lint clean (tokens substituted).
        assert_eq!(
            substitute_templates("{{trigger.channel}} == 'x'"),
            "0 == 'x'"
        );
        assert_eq!(substitute_templates("{{steps.a.out}} > 0"), "0 > 0");
        // Valid evalexpr after substitution.
        assert!(evalexpr::build_operator_tree::<evalexpr::DefaultNumericTypes>("0 == 'x'").is_ok());
    }
}
