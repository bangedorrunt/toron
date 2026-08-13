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
                bail!("{} blocking lint issue(s)", blocking);
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
pub fn run(file: &PathBuf, trigger: &str) -> Result<i32> {
    let workflow = load(file)?;
    let trigger_value: serde_json::Value = serde_json::from_str(trigger)
        .with_context(|| format!("trigger must be valid JSON, got: {trigger:?}"))?;
    let runner = noop_runner();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build runtime")?;
    let output = rt.block_on(async {
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
                    workflow,
                    trigger: trigger_value,
                },
            )
            .await
    })?;
    print_json(&output);
    Ok(EXIT_OK)
}

/// `toron workflow invoke <file> --inputs <json>`.
pub fn invoke(file: &PathBuf, inputs: &str) -> Result<i32> {
    let workflow = load(file)?;
    let inputs_value: serde_json::Value = serde_json::from_str(inputs)
        .with_context(|| format!("inputs must be valid JSON, got: {inputs:?}"))?;
    let runner = noop_runner();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build runtime")?;
    let output = rt.block_on(async {
        let engine = WorkflowEngine::local(runner.clone()).context("engine")?;
        let run_id = format!(
            "cli-invoke-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        engine.invoke(&run_id, workflow, inputs_value).await
    })?;
    print_json(&output);
    Ok(EXIT_OK)
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
        json!(workflow.trigger.as_ref().map(|t| t.type_name())),
    );
    print_json(&serde_json::Value::Object(plan));
    Ok(EXIT_OK)
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
