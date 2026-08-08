// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Workflow lint (bd-osp): validate a workflow file against the YAML schema,
//! guard-expression syntax, template references, and the Buzz 100ms timeout
//! pattern. No evaluation is performed (that's the engine, bd-oge) — lint is
//! a static validator with actionable errors.

use std::collections::BTreeMap;

use crate::schema::{Action, Graph, Trigger, Workflow};

/// The Buzz-derived guard evaluation timeout (ms). Lint enforces that guards
/// are simple expressions; the engine applies the timeout at eval time.
/// Single source of truth: `guard::DEFAULT_TIMEOUT_MS` (the runtime
/// evaluator's constant) — the lint advisory must never drift from it.
pub const GUARD_TIMEOUT_MS: u64 = crate::guard::DEFAULT_TIMEOUT_MS;

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
    // remaining trigger-specific checks. `None` trigger = invoke-only
    // (ADR-0005 Spec 2) — no reactive checks apply.
    if let Some(trigger) = &wf.trigger {
        match trigger {
            Trigger::MessagePosted { channel, project } => {
                if channel.is_none() && project.is_none() {
                    report.issues.push(LintIssue {
                        level: Level::Warning,
                        message: "message_posted trigger has neither channel nor project — will match all messages".into(),
                        line: None,
                    });
                }
            }
            Trigger::Schedule { cron, .. } => {
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
    }

    // Workflow-level guard.
    if let Some(guard) = &wf.guard {
        check_guard(&mut report, guard, "workflow guard");
    }

    // Steps.
    if wf.steps.is_empty() && wf.graph.is_none() {
        report.issues.push(LintIssue {
            level: Level::Warning,
            message: "workflow has no steps and no graph — trigger will fire but nothing runs".into(),
            line: None,
        });
    }
    let mut step_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for step in &wf.steps {
        if let Some(name) = &step.name
            && !step_names.insert(name)
        {
            report.issues.push(LintIssue {
                level: Level::Error,
                message: format!(
                    "duplicate step name {name:?} — {{{{steps.{name}}}}} is ambiguous"
                ),
                line: None,
            });
        }
        if let Some(guard) = &step.guard {
            check_guard(&mut report, guard, "step guard");
        }
        check_action(&mut report, &step.action);
    }

    // Graph (ADR-0005 Spec 4b): DAG acyclicity + resolvable needs + no orphans.
    if let Some(graph) = &wf.graph {
        check_graph(&mut report, graph);
    }

    report
}

/// Validate a `graph:` — the DAG contract (ADR-0005 Spec 4b / P5.4):
///
/// 1. **Acyclic** — a cycle would deadlock the concurrent scheduler.
/// 2. **`needs:` resolvable** — every dependency names an existing node.
/// 3. **No orphans** — every node is reachable from a root (a node whose
///    `needs` are all satisfied) or IS a root.
///
/// Fail-stop at lint, never at runtime.
fn check_graph(report: &mut LintReport, graph: &Graph) {
    use crate::schema::GraphNode;
    let nodes = &graph.nodes;
    if nodes.is_empty() {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: "graph is empty — no nodes".into(),
            line: None,
        });
        return;
    }

    // (2) needs resolvable + collect the dependency edge set.
    let mut edges: Vec<(&str, &str)> = Vec::new(); // (needs → node)
    for (name, node) in nodes {
        check_action(report, &node.action);
        if let Some(guard) = &node.guard {
            check_guard(report, guard, "graph node guard");
        }
        for need in &node.needs {
            if !nodes.contains_key(need) {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: format!(
                        "graph node {name:?} needs {need:?}, which is not a node — unresolvable"
                    ),
                    line: None,
                });
            }
            edges.push((need.as_str(), name.as_str()));
        }
    }

    // (1) acyclic — DFS with a recursion stack. A back-edge to an in-progress
    // node is a cycle.
    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut in_stack: std::collections::HashSet<&str> = std::collections::HashSet::new();
    fn dfs<'a>(
        node: &'a str,
        nodes: &'a BTreeMap<String, GraphNode>,
        edges: &[(&'a str, &'a str)],
        visited: &mut std::collections::HashSet<&'a str>,
        in_stack: &mut std::collections::HashSet<&'a str>,
        report: &mut LintReport,
    ) {
        visited.insert(node);
        in_stack.insert(node);
        for (from, _to) in edges.iter().filter(|(_, to)| *to == node) {
            if in_stack.contains(*from) {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: format!(
                        "graph has a cycle: node {from:?} depends on {node:?} which depends back on it"
                    ),
                    line: None,
                });
            } else if !visited.contains(*from) {
                dfs(from, nodes, edges, visited, in_stack, report);
            }
        }
        in_stack.remove(node);
    }
    for name in nodes.keys() {
        if !visited.contains(name.as_str()) {
            dfs(name, nodes, &edges, &mut visited, &mut in_stack, report);
        }
    }

    // (3) no orphans — every node must be reachable from a root. A root is a
    // node with NO incoming dependency edges (nothing is a `to` of an edge:
    // edges are stored (need → node), so a node with a `needs` entry appears
    // as a `to`).
    let has_incoming: std::collections::HashSet<&str> =
        edges.iter().map(|(_, to)| *to).collect();
    let roots: Vec<&str> = nodes
        .keys()
        .map(String::as_str)
        .filter(|n| !has_incoming.contains(n))
        .collect();
    // Reachability from all roots.
    let mut reachable: std::collections::HashSet<&str> = std::collections::HashSet::new();
    fn reach<'a>(
        node: &'a str,
        edges: &[(&'a str, &'a str)],
        reachable: &mut std::collections::HashSet<&'a str>,
    ) {
        reachable.insert(node);
        for (_from, to) in edges.iter().filter(|(from, _)| *from == node) {
            if !reachable.contains(to) {
                reach(*to, edges, reachable);
            }
        }
    }
    for root in roots {
        reach(root, &edges, &mut reachable);
    }
    for name in nodes.keys() {
        if !reachable.contains(name.as_str()) {
            report.issues.push(LintIssue {
                level: Level::Error,
                message: format!(
                    "graph node {name:?} is an orphan — not reachable from any root"
                ),
                line: None,
            });
        }
    }
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
        Action::Retry {
            max_attempts,
            step,
            ..
        } => {
            if *max_attempts == 0 {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "retry requires max_attempts >= 1".into(),
                    line: None,
                });
            }
            check_action(report, &step.action);
        }
        Action::Branch { if_guard, then, else_steps } => {
            check_guard(report, if_guard, "branch guard");
            for s in then.iter().chain(else_steps.iter()) {
                check_action(report, &s.action);
            }
        }
        Action::Foreach { over, each } => {
            if over.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "foreach requires a non-empty `over`".into(),
                    line: None,
                });
            }
            check_action(report, &each.action);
        }
        Action::Parallel { over, each } => {
            if over.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "parallel requires a non-empty `over`".into(),
                    line: None,
                });
            }
            check_action(report, &each.action);
        }
        Action::CallWorkflow { workflow, .. } => {
            if workflow.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "call_workflow requires a non-empty `workflow` name".into(),
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

    /// P5.4: a cyclic `graph:` is rejected by lint (fail-stop, never runtime).
    #[test]
    fn lint_cyclic_graph_rejected() {
        let yaml = r##"
name: cyclic
trigger:
  type: webhook
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, needs: [b] }
    b: { action: send_dm, to: x, body: y, needs: [a] }
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            !report.is_clean(),
            "cyclic graph must fail lint: {:?}",
            report.issues
        );
        assert!(
            report.issues.iter().any(|i| i.message.contains("cycle")),
            "cycle named: {:?}",
            report.issues
        );
    }

    /// P5.4: an unresolvable `needs:` reference is a lint error.
    #[test]
    fn lint_unresolvable_needs_rejected() {
        let yaml = r##"
name: bad-needs
trigger:
  type: webhook
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, needs: [ghost] }
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            !report.is_clean(),
            "unresolvable needs must fail lint: {:?}",
            report.issues
        );
        assert!(
            report.issues.iter().any(|i| i.message.contains("not a node")),
            "unresolvable named: {:?}",
            report.issues
        );
    }

    /// P5.4: an orphan node (not reachable from any root) is a lint error.
    #[test]
    fn lint_orphan_node_rejected() {
        let yaml = r##"
name: orphan
trigger:
  type: webhook
graph:
  nodes:
    a: { action: send_dm, to: x, body: y }
    b: { action: send_dm, to: x, body: y, needs: [a] }
    c: { action: send_dm, to: x, body: y }   # no incoming, is a root — fine
    d: { action: send_dm, to: x, body: y, needs: [b] }
    stray: { action: send_dm, to: x, body: y, needs: [ghost] }
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report.issues.iter().any(|i| i.message.contains("stray")),
            "orphan flagged: {:?}",
            report.issues
        );
    }

    /// P5.4: a valid DAG (the Spec 4b example) lints clean.
    #[test]
    fn lint_valid_dag_clean() {
        let yaml = r##"
name: ship
trigger:
  type: webhook
graph:
  nodes:
    scout: { action: send_dm, to: x, body: y }
    plan:  { action: send_dm, to: x, body: y, needs: [scout] }
    fix_a: { action: send_dm, to: x, body: y, needs: [plan] }
    fix_b: { action: send_dm, to: x, body: y, needs: [plan] }
    ship:  { action: send_dm, to: x, body: y, needs: [fix_a, fix_b] }
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report.is_clean(),
            "valid DAG must lint clean: {:?}",
            report.issues
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

    /// P4.1: the extended schedule trigger (owner/scope/policy) lints clean —
    /// valid cron + valid policy enums.
    #[test]
    fn lint_extended_schedule_clean() {
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
        let report = lint_yaml(yaml).expect("parses");
        assert!(
            report.is_clean(),
            "extended schedule must lint clean: {:?}",
            report.issues
        );
    }

    /// P4.1: an invalid policy enum value is a parse error (fail-stop).
    #[test]
    fn lint_invalid_policy_enum_fails() {
        let yaml = r##"
name: bad-cron
trigger:
  type: schedule
  cron: "* * * * *"
  owner: housekeeper
policy:
  on_failure: explode
steps:
  - name: a
    action: send_dm
    to: pi
    body: x
"##;
        let err = lint_yaml(yaml).expect_err("bad policy enum fails parse");
        assert!(
            err.contains("on_failure") || err.contains("unknown variant"),
            "names the bad policy value: {err}"
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
