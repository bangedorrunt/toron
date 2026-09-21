// governed-by: ADR-0005 (Spec 5 — toron workflow CLI, P3.7)
#![forbid(unsafe_code)]

//! `toron workflow` (fabric, standalone) — ADR-0005 Spec 5.
//!
//! toron-cli owns the FABRIC surface: single-file, standalone workflow
//! handling. Orchestration (catalogs, crews, cron, runs) lives in flywheel.
//!
//! - `lint` / `validate` — static fail-stop checks (schema, guards, template
//!   refs, graph acyclicity). Exit 0 clean, 1 blocking issue, 2 parse failure.
//! - `run` — run a workflow against a trigger payload.
//! - `invoke` — run on-demand with inputs (no trigger event).
//! - `dry-run` — lint + describe without executing.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde_json::json;

use toron_workflow::engine::{ActionRunner, RunInput, WorkflowEngine};
use toron_workflow::schema::{Action, parse_workflow};

/// The exit-code contract (clig.dev, ADR-0003): 0 ok · 1 runtime · 2 usage.
pub const EXIT_OK: i32 = 0;
pub const EXIT_PARSE: i32 = 2;

/// A no-op action runner for the standalone CLI (the fabric has no mailbox
/// surface; actions are recorded, not executed — the durable run still
/// exercises the engine: delays, approvals, graph scheduling, control-flow).
fn noop_runner() -> ActionRunner {
    Arc::new(|a: &Action| Ok(json!({ "action": a.type_name() })))
}

use std::sync::Arc;

/// `toron workflow lint <file>` — the fail-stop gate.
pub fn lint(file: &PathBuf) -> Result<i32> {
    let yaml = fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
    match toron_workflow::lint_yaml(&yaml) {
        Ok(report) => {
            let blocking = report
                .issues
                .iter()
                .filter(|i| i.level == toron_workflow::lint::Level::Error)
                .count();
            for issue in &report.issues {
                eprintln!(
                    "{:>7} {}",
                    match issue.level {
                        toron_workflow::lint::Level::Error => "error",
                        toron_workflow::lint::Level::Warning => "warning",
                    },
                    issue.message
                );
            }
            if blocking > 0 {
                bail!("{blocking} blocking lint issue(s)");
            }
            Ok(EXIT_OK)
        }
        Err(e) => {
            eprintln!("parse error: {e}");
            Ok(EXIT_PARSE)
        }
    }
}

/// `toron workflow run <file> --trigger <json>`.
///
/// Owns its runtime at the edge (`#[tokio::main]`): the engine is awaited
/// inline, so this module performs no thread-hop bridge (bd-ckkf, ADR-0035
/// Spec 2.2). The standalone runner stays a pure sync valueeeper.
pub fn run(file: &PathBuf, trigger: &str) -> Result<i32> {
    let output = run_async(file, trigger)?;
    print_json(&output);
    Ok(EXIT_OK)
}

#[tokio::main]
async fn run_async(file: &PathBuf, trigger: &str) -> Result<serde_json::Value> {
    let workflow = load(file)?;
    preflight(&workflow)?;
    let trigger_value: serde_json::Value = serde_json::from_str(trigger)
        .with_context(|| format!("trigger must be valid JSON, got: {trigger:?}"))?;
    let runner = noop_runner();
    let engine = WorkflowEngine::local(runner.clone()).context("engine")?;
    let run_id = format!(
        "cli-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    engine
        .run(
            &run_id,
            RunInput {
                run_id: Some(run_id.clone()),
                workflow,
                trigger: trigger_value,
            },
        )
        .await
}

/// `toron workflow invoke <file> --inputs <json>`.
///
/// Same runtime-at-the-edge shape as [`run`](self::run) (bd-ckkf).
pub fn invoke(file: &PathBuf, inputs: &str) -> Result<i32> {
    let output = invoke_async(file, inputs)?;
    print_json(&output);
    Ok(EXIT_OK)
}

#[tokio::main]
async fn invoke_async(file: &PathBuf, inputs: &str) -> Result<serde_json::Value> {
    let workflow = load(file)?;
    preflight(&workflow)?;
    let inputs_value: serde_json::Value = serde_json::from_str(inputs)
        .with_context(|| format!("inputs must be valid JSON, got: {inputs:?}"))?;
    let runner = noop_runner();
    let engine = WorkflowEngine::local(runner.clone()).context("engine")?;
    let run_id = format!(
        "cli-invoke-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    engine.invoke(&run_id, workflow, inputs_value).await
}

/// `toron workflow dry-run <file> --trigger <json>` — lint + describe.
pub fn dry_run(file: &PathBuf, _trigger: &str) -> Result<i32> {
    let workflow = load(file)?;
    let mut plan = serde_json::Map::new();
    plan.insert("name".into(), json!(workflow.name));
    plan.insert("steps".into(), json!(workflow.steps.len()));
    plan.insert(
        "graph".into(),
        json!(workflow.graph.as_ref().map(|g| g.nodes.len())),
    );
    plan.insert(
        "trigger".into(),
        json!(
            workflow
                .trigger
                .as_ref()
                .map(toron_workflow::Trigger::type_name)
        ),
    );
    print_json(&serde_json::Value::Object(plan));
    Ok(EXIT_OK)
}

/// ADR-0032 Spec 6: parked actions need a draining host. The standalone CLI
/// runs a no-op runner with no sink drain and no resolvers, so a workflow
/// containing a parked action would park forever with no operator-visible
/// pending event. Refuse before starting, naming the required host/mode.
/// Returns the refusal reason, or `None` when the workflow needs no host.
pub fn parked_preflight(workflow: &toron_workflow::schema::Workflow) -> Option<String> {
    use toron_workflow::schema::{Action, Step};
    fn step_needs_host(step: &Step) -> Option<&'static str> {
        action_needs_host(&step.action)
    }
    fn steps_need_host(steps: &[Step]) -> Option<&'static str> {
        steps.iter().find_map(step_needs_host)
    }
    fn action_needs_host(action: &Action) -> Option<&'static str> {
        match action {
            Action::RequestApproval { .. } => Some("request_approval"),
            Action::Spawn { await_, .. } if *await_ => Some("spawn await:true"),
            Action::Retry { step, .. } => step_needs_host(step),
            Action::Branch {
                then, else_steps, ..
            } => steps_need_host(then).or_else(|| steps_need_host(else_steps)),
            Action::Foreach { each, .. } | Action::Parallel { each, .. } => step_needs_host(each),
            _ => None,
        }
    }
    let found = steps_need_host(&workflow.steps).or_else(|| {
        workflow
            .graph
            .as_ref()
            .and_then(|g| g.nodes.values().find_map(|n| action_needs_host(&n.action)))
    });
    found.map(|kind| {
        format!(
            "workflow {:?} contains parked action `{kind}` but the standalone CLI has no draining host (no approval/spawn sink, no resolvers) — run it under a host lifecycle that drains sinks and publishes pending events"
        , workflow.name)
    })
}

/// Enforce [`parked_preflight`]: refuse parked workflows before they can park
/// forever in the no-op host.
fn preflight(workflow: &toron_workflow::schema::Workflow) -> Result<()> {
    if let Some(reason) = parked_preflight(workflow) {
        bail!("{reason}");
    }
    Ok(())
}

/// Load + lint a workflow file (the gate before any run).
fn load(file: &PathBuf) -> Result<toron_workflow::schema::Workflow> {
    let yaml = fs::read_to_string(file).with_context(|| format!("read {}", file.display()))?;
    let report =
        toron_workflow::lint_yaml(&yaml).map_err(|e| anyhow::anyhow!("parse error: {e}"))?;
    if report
        .issues
        .iter()
        .any(|i| i.level == toron_workflow::lint::Level::Error)
    {
        bail!("workflow has blocking lint issues (toron workflow lint)");
    }
    parse_workflow(&yaml).with_context(|| format!("parse {}", file.display()))
}

/// Print a JSON value to stdout (respecting `--json` conventions).
fn print_json(value: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wf(yaml: &str) -> toron_workflow::schema::Workflow {
        parse_workflow(yaml).expect("test workflow parses")
    }

    #[test]
    fn preflight_refuses_request_approval() {
        let w = wf(
            "name: p\ntrigger:\n  type: webhook\nsteps:\n  - { name: g, action: request_approval, message: q }\n",
        );
        let reason = parked_preflight(&w).expect("refused");
        assert!(reason.contains("request_approval"), "{reason}");
        assert!(preflight(&w).is_err(), "run/invoke must refuse");
    }

    #[test]
    fn preflight_refuses_spawn_await_nested_in_retry() {
        let w = wf(
            "name: p\ntrigger:\n  type: webhook\nsteps:\n  - name: w\n    action: retry\n    max_attempts: 2\n    step:\n      action: spawn\n      profile: \"@housekeeper\"\n      await: true\n",
        );
        let reason = parked_preflight(&w).expect("nested spawn-await refused");
        assert!(reason.contains("spawn await:true"), "{reason}");
    }

    #[test]
    fn preflight_allows_detached_spawn_and_plain_steps() {
        let w = wf(
            "name: p\ntrigger:\n  type: webhook\nsteps:\n  - { name: f, action: spawn, profile: \"@h\", await: false }\n  - { name: d, action: send_dm, to: x, body: y }\n",
        );
        assert!(parked_preflight(&w).is_none(), "no parked action");
        assert!(preflight(&w).is_ok());
    }

    #[test]
    fn preflight_refuses_graph_parked_node() {
        let w = wf(
            "name: p\ntrigger:\n  type: webhook\ngraph:\n  nodes:\n    g: { action: request_approval, message: q }\n",
        );
        assert!(parked_preflight(&w).is_some(), "graph node refused");
    }
}
