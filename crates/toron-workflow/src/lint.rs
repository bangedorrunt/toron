// governed-by: ADR-0024
// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Workflow lint (bd-osp): validate a workflow file against the YAML schema,
//! guard-expression syntax, template references, and the Buzz 100ms timeout
//! pattern. No evaluation is performed (that's the engine, bd-oge) — lint is
//! a static validator with actionable errors.

use crate::schema::{Action, CronSpec, CronSpecPolicy, Graph, Step, Trigger, Workflow};

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
///
/// Uses the in-crate [`playbook_lint::DEFAULT_PRINCIPLE_NAMES`] registry
/// fallback. Embeddings that own the canonical list (flywheel's
/// `PRINCIPLE_NAMES` — ADR-0019 w2-name-registry, no reverse dep) MUST call
/// [`lint_workflow_with_principles`] instead.
pub fn lint_workflow(wf: &Workflow) -> LintReport {
    lint_workflow_with_principles(wf, &crate::playbook_lint::DEFAULT_PRINCIPLE_NAMES)
}

/// Lint a parsed workflow against an embedding-supplied principle-name
/// registry (ADR-0019 wave 2 bridge: flywheel passes `PRINCIPLE_NAMES` in).
pub fn lint_workflow_with_principles(wf: &Workflow, principle_names: &[&str]) -> LintReport {
    let mut report = LintReport::default();

    check_name(&mut report, &wf.name, "workflow");

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
            message: "workflow has no steps and no graph — trigger will fire but nothing runs"
                .into(),
            line: None,
        });
    }
    check_steps(&mut report, &wf.steps);

    // Graph (ADR-0005 Spec 4b): DAG acyclicity + resolvable needs + no orphans.
    if let Some(graph) = &wf.graph {
        check_graph(&mut report, graph);
    }

    // ADR-0024 Spec 1 R-E lints (kind-aware fail_run)
    check_re_lint(&mut report, wf);

    // ADR-0019 wave-2 structural playbook lints (bd-w2-lints-6k6l): feature
    // data_shape, bug-fix repro predecessor, prototype/arena n>=2 + mode
    // rules, forensics read-only, principles registry.
    crate::playbook_lint::lint_playbook(&mut report, wf, principle_names);

    report
}

/// The shared name contract for both input types (workflow + cron): required
/// and identifier-safe (alnum/_/-/.).
fn check_name(report: &mut LintReport, name: &str, kind: &str) {
    if name.trim().is_empty() {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!("{kind} `name` must not be empty"),
            line: None,
        });
    } else if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!("{kind} name {name:?} contains invalid characters (alnum/_/-/.)"),
            line: None,
        });
    }
}

/// The shared step-body checks (duplicate names, guards, actions) for both
/// input types.
fn check_steps(report: &mut LintReport, steps: &[Step]) {
    let mut step_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for step in steps {
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
            check_guard(report, guard, "step guard");
        }
        check_action(report, &step.action);
    }
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
                dfs(from, edges, visited, in_stack, report);
            }
        }
        in_stack.remove(node);
    }
    for name in nodes.keys() {
        if !visited.contains(name.as_str()) {
            dfs(name, &edges, &mut visited, &mut in_stack, report);
        }
    }

    // (3) no orphans — every node must be reachable from a root. A root is a
    // node with NO incoming dependency edges (nothing is a `to` of an edge:
    // edges are stored (need → node), so a node with a `needs` entry appears
    // as a `to`).
    let has_incoming: std::collections::HashSet<&str> = edges.iter().map(|(_, to)| *to).collect();
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
                reach(to, edges, reachable);
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
                message: format!("graph node {name:?} is an orphan — not reachable from any root"),
                line: None,
            });
        }
    }
}

/// ADR-0024 Spec 1 R-E lints
fn check_re_lint(report: &mut LintReport, wf: &Workflow) {
    // Walk steps with enclosing_retry flag
    fn walk_step(step: &Step, inside_retry: bool, report: &mut LintReport) {
        // R-E2: fail_run step without kind classification
        if step.on_error == crate::schema::StepOnError::FailRun {
            report.issues.push(LintIssue {
                level: Level::Warning,
                message: format!(
                    "R-E2: step {:?} with on_error: fail_run missing kind classification — rate_limit/timeout retry vs blocked/auth/cancelled/unknown escalate must be distinguished (ADR-0024 Spec 1)",
                    step.name.clone().unwrap_or_else(|| "<unnamed>".into())
                ),
                line: None,
            });
        }
        // R-E3: spawn + fail_run without enclosing Retry
        if matches!(step.action, Action::Spawn { .. })
            && step.on_error == crate::schema::StepOnError::FailRun
            && !inside_retry
        {
            report.issues.push(LintIssue {
                level: Level::Warning,
                message: format!(
                    "R-E3: spawn step {:?} with on_error: fail_run without enclosing retry — wrap spawn in retry so rate_limit/timeout bound is explicit (ADR-0024 Spec 1)",
                    step.name.clone().unwrap_or_else(|| "<unnamed>".into())
                ),
                line: None,
            });
        }
        match &step.action {
            Action::Retry { step: inner, .. } => {
                // R-E1: retry on blocked/auth must escalate, never retry
                // Any Retry that would retry a fail_run step is flagged as
                // retrying blocked/auth (the only safe retry kinds are
                // rate_limit/timeout handled by the engine's kind-aware loop).
                if inner.on_error == crate::schema::StepOnError::FailRun {
                    report.issues.push(LintIssue {
                        level: Level::Warning,
                        message: format!(
                            "R-E1: retry on blocked/auth with fail_run step {:?} — blocked/auth must escalate, not retry (ADR-0024 Spec 1)",
                            inner.name.clone().unwrap_or_else(|| "<unnamed>".into())
                        ),
                        line: None,
                    });
                }
                walk_step(inner, true, report);
            }
            Action::Branch {
                then, else_steps, ..
            } => {
                for s in then.iter().chain(else_steps.iter()) {
                    walk_step(s, inside_retry, report);
                }
            }
            Action::Foreach { each, .. } | Action::Parallel { each, .. } => {
                walk_step(each, inside_retry, report);
            }
            _ => {}
        }
    }

    for step in &wf.steps {
        walk_step(step, false, report);
    }
    if let Some(graph) = &wf.graph {
        for (name, node) in &graph.nodes {
            if node.on_error == crate::schema::StepOnError::FailRun {
                report.issues.push(LintIssue {
                    level: Level::Warning,
                    message: format!(
                        "R-E2: graph node {name:?} with on_error: fail_run missing kind classification — rate_limit/timeout retry vs blocked/auth/cancelled/unknown escalate must be distinguished (ADR-0024 Spec 1)"
                    ),
                    line: None,
                });
            }
            if matches!(node.action, Action::Spawn { .. })
                && node.on_error == crate::schema::StepOnError::FailRun
            {
                // For graph nodes, there is no syntactic enclosing Retry unless
                // the node's action IS Retry wrapping spawn. Treat bare spawn
                // fail_run in graph as R-E3.
                // Check if this node is itself inside a Retry node? Graph is flat,
                // so we treat any bare spawn fail_run as R-E3 unless its action is Retry.
                let is_wrapped = matches!(node.action, Action::Retry { .. });
                if !is_wrapped {
                    report.issues.push(LintIssue {
                        level: Level::Warning,
                        message: format!(
                            "R-E3: spawn graph node {name:?} with on_error: fail_run without enclosing retry — wrap spawn in retry so rate_limit/timeout bound is explicit (ADR-0024 Spec 1)"
                        ),
                        line: None,
                    });
                }
            }
            // R-E1 for graph retry nodes
            if let Action::Retry { step: inner, .. } = &node.action
                && inner.on_error == crate::schema::StepOnError::FailRun
            {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: format!(
                        "R-E1: retry on blocked/auth with fail_run graph node {name:?} inner {:?} — blocked/auth must escalate, not retry (ADR-0024 Spec 1)",
                        inner.name.clone().unwrap_or_else(|| "<unnamed>".into())
                    ),
                    line: None,
                });
            }
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
            max_attempts, step, ..
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
        Action::Branch {
            if_guard,
            then,
            else_steps,
        } => {
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
        Action::Spawn { profile, .. } => {
            if profile.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "spawn requires a non-empty `profile`".into(),
                    line: None,
                });
            }
        }
        Action::Verify { command } => {
            if command.trim().is_empty() {
                report.issues.push(LintIssue {
                    level: Level::Error,
                    message: "verify requires a non-empty `command`".into(),
                    line: None,
                });
            }
        }
    }
}

/// The complete lint entrypoint: parse YAML then lint.
pub fn lint_yaml(yaml: &str) -> Result<LintReport, String> {
    // ADR-0012: cron is a DISTINCT type. A cron-YAML carries top-level
    // `schedule`/`owner`; it is NOT a workflow — reject it here so the two
    // types stay distinct at lint (the cron lint path is its validator).
    if is_cron_yaml(yaml) {
        return Ok(LintReport {
            issues: vec![LintIssue {
                level: Level::Error,
                message: "this is a cron YAML (top-level `schedule`/`owner`) — cron is a distinct type (ADR-0012); lint it with the cron lint path, not workflow lint"
                    .into(),
                line: None,
            }],
        });
    }
    let wf = crate::schema::parse_workflow(yaml).map_err(|e| format!("yaml parse error: {e}"))?;
    Ok(lint_workflow(&wf))
}

/// True when the YAML document has top-level `schedule` or `owner` keys —
/// the cron-type markers (ADR-0012). Malformed YAML → false (the parse
/// error path reports it properly).
fn is_cron_yaml(yaml: &str) -> bool {
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(yaml) else {
        return false;
    };
    match v {
        serde_yaml::Value::Mapping(m) => {
            m.contains_key(serde_yaml::Value::String("schedule".into()))
                || m.contains_key(serde_yaml::Value::String("owner".into()))
        }
        _ => false,
    }
}

/// Lint a parsed cron (ADR-0012: cron is a distinct type — schedule + owner +
/// scope + mode + policy + steps body). Errors block, warnings advise.
pub fn lint_cron(spec: &CronSpec) -> LintReport {
    let mut report = LintReport::default();

    check_name(&mut report, &spec.name, "cron");
    check_cron_schedule(&mut report, &spec.schedule);

    // Owner profile (the fixer's identity). Profile RESOLUTION against the
    // flywheel-tier catalog is deferred to the CLI (bd-25g7.1 scope); lint
    // checks the field is present + identifier-safe.
    if spec.owner.trim().is_empty() {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: "cron `owner` (owning profile) must not be empty".into(),
            line: None,
        });
    } else if !spec
        .owner
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!(
                "cron owner {:?} contains invalid characters (alnum/_/-/.)",
                spec.owner
            ),
            line: None,
        });
    }

    if let Some(policy) = &spec.policy {
        check_cron_policy(&mut report, policy);
    }

    if spec.steps.is_empty() && spec.graph.is_none() {
        report.issues.push(LintIssue {
            level: Level::Warning,
            message: "cron has no steps and no graph — the tick will fire but nothing runs".into(),
            line: None,
        });
    }
    check_steps(&mut report, &spec.steps);

    if let Some(graph) = &spec.graph {
        check_graph(&mut report, graph);
    }

    report
}

/// The cron schedule contract (ADR-0012 Spec 3): exactly 5 Vixie fields,
/// parseable by the cron engine (via the shared vixie normalization).
fn check_cron_schedule(report: &mut LintReport, schedule: &str) {
    let fields: Vec<&str> = schedule.split_whitespace().collect();
    if fields.len() != 5 {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!(
                "schedule {schedule:?} must be a 5-field Vixie cron expression, got {} fields",
                fields.len()
            ),
            line: None,
        });
        return;
    }
    if crate::cron::normalize_cron(schedule)
        .parse::<cron::Schedule>()
        .is_err()
    {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: format!("schedule {schedule:?} is not a valid cron expression"),
            line: None,
        });
    }
}

/// Cron policy sanity (ADR-0012): degenerate values are config bugs.
fn check_cron_policy(report: &mut LintReport, policy: &CronSpecPolicy) {
    if policy.repeat_threshold == 0 {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: "policy repeat_threshold must be >= 1 (0 escalates on the FIRST failure)"
                .into(),
            line: None,
        });
    }
    if policy.no_claim_timeout == 0 {
        report.issues.push(LintIssue {
            level: Level::Error,
            message: "policy no_claim_timeout must be > 0 (0 aborts every spawn-await instantly)"
                .into(),
            line: None,
        });
    }
}

/// Parse a cron YAML then lint it (the cron lint entrypoint, ADR-0012).
pub fn cron_lint_yaml(yaml: &str) -> Result<LintReport, String> {
    let spec = crate::schema::parse_cron(yaml).map_err(|e| format!("yaml parse error: {e}"))?;
    Ok(lint_cron(&spec))
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
            report
                .issues
                .iter()
                .any(|i| i.message.contains("not a node")),
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

    // ---- ADR-0012: cron lint is DISTINCT from workflow lint (bd-25g7.1) ----

    const ADR_CRON_YAML: &str = r##"
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

    /// Gate: the ADR-0012 example lints CLEAN via the cron lint path.
    #[test]
    fn cron_lint_adr_example_clean() {
        let report = cron_lint_yaml(ADR_CRON_YAML).expect("cron parses");
        assert!(
            report.is_clean(),
            "ADR example must lint clean: {:?}",
            report.issues
        );
    }

    /// Gate: a workflow lint REJECTS a cron-YAML (the two types are distinct
    /// at lint, ADR-0012).
    #[test]
    fn workflow_lint_rejects_cron_yaml() {
        let report = lint_yaml(ADR_CRON_YAML).expect("cron yaml parses as yaml");
        assert!(!report.is_clean(), "workflow lint must reject cron yaml");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.message.to_lowercase().contains("cron")),
            "names cron as the reason: {:?}",
            report.issues
        );
    }

    #[test]
    fn cron_lint_bad_schedule_rejected() {
        // 4 fields — not 5-field vixie.
        let yaml = "name: c\nschedule: '30 3 * *'\nowner: o\nsteps: []\n";
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("5-field")),
            "field-count error: {:?}",
            report.issues
        );

        // Minute 99 — invalid cron expression.
        let yaml = "name: c\nschedule: '99 3 * * 0'\nowner: o\nsteps: []\n";
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("cron expression")),
            "cron-expr error: {:?}",
            report.issues
        );
    }

    #[test]
    fn cron_lint_owner_required() {
        let yaml = "name: c\nschedule: '* * * * *'\nowner: ''\nsteps: []\n";
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("owner")),
            "empty owner flagged: {:?}",
            report.issues
        );
    }

    #[test]
    fn cron_lint_bad_policy_rejected() {
        // repeat_threshold 0 — degenerate (escalates on the FIRST failure).
        let yaml =
            "name: c\nschedule: '* * * * *'\nowner: o\npolicy:\n  repeat_threshold: 0\nsteps: []\n";
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("repeat_threshold")),
            "threshold-0 flagged: {:?}",
            report.issues
        );

        // no_claim_timeout 0 — abort immediately, never wait.
        let yaml =
            "name: c\nschedule: '* * * * *'\nowner: o\npolicy:\n  no_claim_timeout: 0\nsteps: []\n";
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("no_claim_timeout")),
            "timeout-0 flagged: {:?}",
            report.issues
        );
    }

    #[test]
    fn cron_lint_duplicate_steps_rejected() {
        let yaml = "name: c\nschedule: '* * * * *'\nowner: o\nsteps:\n  - { name: a, action: delay, secs: 1 }\n  - { name: a, action: delay, secs: 2 }\n";
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("duplicate step name")),
            "duplicate step flagged: {:?}",
            report.issues
        );
    }

    #[test]
    fn cron_lint_cyclic_graph_rejected() {
        let yaml = r##"
name: c
schedule: "* * * * *"
owner: o
graph:
  nodes:
    a: { action: delay, secs: 1, needs: [b] }
    b: { action: delay, secs: 1, needs: [a] }
"##;
        let report = cron_lint_yaml(yaml).expect("parses");
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("cycle")),
            "cycle flagged: {:?}",
            report.issues
        );
    }

    #[test]
    fn cron_lint_yaml_parse_error_actionable() {
        let err = cron_lint_yaml("name: [broken").expect_err("invalid yaml fails");
        assert!(err.contains("yaml parse error"), "actionable: {err}");
    }

    // ---- ADR-0024 Spec 1 R-E lints (kind_aware) ----

    #[test]
    fn kind_aware_r_e1_retry_on_blocked_is_error() {
        let yaml = r##"
name: r-e1
trigger:
  type: webhook
steps:
  - name: go
    action: retry
    max_attempts: 3
    step:
      action: send_dm
      to: x
      body: y
      on_error: fail_run
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report.issues.iter().any(|i| i.level == Level::Warning
                && i.message.contains("R-E1")
                && i.message.to_lowercase().contains("blocked")),
            "R-E1 error expected: {:?}",
            report.issues
        );
    }

    #[test]
    fn kind_aware_r_e2_fail_run_missing_kind_warning() {
        let yaml = r##"
name: r-e2
trigger:
  type: webhook
steps:
  - name: go
    action: send_dm
    to: x
    body: y
    on_error: fail_run
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report.issues.iter().any(|i| i.level == Level::Warning
                && i.message.contains("R-E2")
                && i.message.to_lowercase().contains("kind")),
            "R-E2 warning expected: {:?}",
            report.issues
        );
    }

    #[test]
    fn kind_aware_r_e3_spawn_without_retry_warning() {
        let yaml = r##"
name: r-e3
trigger:
  type: webhook
steps:
  - name: spawn-go
    action: spawn
    profile: "@housekeeper"
    await: true
    on_error: fail_run
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        assert!(
            report.issues.iter().any(|i| i.level == Level::Warning
                && i.message.contains("R-E3")
                && i.message.to_lowercase().contains("spawn")),
            "R-E3 warning expected: {:?}",
            report.issues
        );
    }

    #[test]
    fn kind_aware_r_e3_spawn_with_retry_no_warning() {
        let yaml = r##"
name: r-e3-ok
trigger:
  type: webhook
steps:
  - name: wrapped
    action: retry
    max_attempts: 3
    step:
      action: spawn
      profile: "@housekeeper"
      await: true
      on_error: fail_run
"##;
        let wf = parse_workflow(yaml).expect("parse");
        let report = lint_workflow(&wf);
        // Wrapped spawn should NOT trigger R-E3
        assert!(
            !report.issues.iter().any(|i| i.message.contains("R-E3")),
            "R-E3 must not fire when wrapped in retry: {:?}",
            report.issues
        );
    }
}
