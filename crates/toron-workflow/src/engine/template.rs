// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Template interpolation (F21.ENGINE-SPLIT move from the former single-file
//! engine): `{{trigger.<path>}}` / `{{steps.<name>.<path>}}` / `{{item}}` /
//! `{{inputs.x}}` substitution for action fields and `foreach` `over`
//! resolution.
//!
//! ADR-0032 Spec 4: unresolvable refs are LOUD. `render_action` fails naming
//! the workflow/node context, field path, and reference path — never a
//! verbatim placeholder. (The guard-syntax lint keeps its own lenient
//! substitution; that is static analysis, not execution.)

use std::fmt::Write as _;

use crate::schema::Action;

/// Render context carried into errors: workflow + node identity so a missing
/// ref names where it broke (ADR-0032 Spec 4).
pub(crate) struct RenderContext<'a> {
    pub workflow: &'a str,
    pub node: &'a str,
}

impl<'a> RenderContext<'a> {
    pub const fn new(workflow: &'a str, node: &'a str) -> Self {
        Self { workflow, node }
    }
}

/// Substitute `{{trigger.<path>}}` / `{{steps.<name>.<path>}}` templates in an
/// action's string fields before the runner sees them (ADR-0005 Spec 0:
/// value-returning actions flow into `steps.<name>` for downstream templates).
///
/// FAILS on the first unresolvable token: the error names the workflow, node,
/// field path, and reference path (ADR-0032 Spec 4 / I10 — no placeholders).
/// Strictness applies at EXECUTION points (leaf dispatch, iteration bodies).
/// The graph scheduler uses [`render_action_lenient`] for planning renders —
/// nested bodies carry deferred context (`{{item}}`, later-wave outputs)
/// that only exists at execution time.
pub(crate) fn render_action(
    action: &Action,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    context: &RenderContext,
) -> anyhow::Result<Action> {
    let mut value = serde_json::to_value(action)?;
    render_json_value_strict(&mut value, trigger, step_outputs, context, "$")
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    serde_json::from_value(value)
        .map_err(|e| anyhow::anyhow!("re-deserialize rendered action: {e}"))
}

/// Planning render for the graph scheduler: unresolvable tokens stay verbatim
/// because nested bodies may carry deferred context resolved at execution
/// (where [`render_action`] enforces strictness). Scheduling never executes.
pub(crate) fn render_action_lenient(
    action: &Action,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Action> {
    let mut value = serde_json::to_value(action)?;
    render_json_value_lenient(&mut value, trigger, step_outputs);
    serde_json::from_value(value)
        .map_err(|e| anyhow::anyhow!("re-deserialize rendered action: {e}"))
}

/// Recursively render template tokens leniently (verbatim on miss).
fn render_json_value_lenient(
    value: &mut serde_json::Value,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(s) => {
            *s = render_template_lenient(s, trigger, step_outputs);
        }
        serde_json::Value::Array(items) => {
            for item in items {
                render_json_value_lenient(item, trigger, step_outputs);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values_mut() {
                render_json_value_lenient(item, trigger, step_outputs);
            }
        }
        _ => {}
    }
}

/// Lenient token substitution: unresolvable refs stay verbatim (planning
/// only — execution uses the strict path).
fn render_template_lenient(
    s: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let token = &after[..end];
                if let Some(replaced) = resolve_template(token.trim(), trigger, step_outputs) {
                    out.push_str(&replaced);
                } else {
                    let _ = write!(out, "{{{{{token}}}}}");
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Recursively render template tokens in every string leaf of a JSON value,
/// tracking the field path for loud errors.
fn render_json_value_strict(
    value: &mut serde_json::Value,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    context: &RenderContext,
    path: &str,
) -> Result<(), String> {
    match value {
        serde_json::Value::String(s) => {
            *s = render_template_strict(s, trigger, step_outputs).map_err(|token| {
                format!(
                    "workflow {:?} node {:?} field {:?}: unresolved template ref {{{{{token}}}}}",
                    context.workflow, context.node, path
                )
            })?;
            Ok(())
        }
        serde_json::Value::Array(items) => {
            for (i, item) in items.iter_mut().enumerate() {
                let p = format!("{path}[{i}]");
                render_json_value_strict(item, trigger, step_outputs, context, &p)?;
            }
            Ok(())
        }
        serde_json::Value::Object(map) => {
            for (k, item) in map.iter_mut() {
                let p = format!("{path}.{k}");
                render_json_value_strict(item, trigger, step_outputs, context, &p)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Replace every `{{...}}` token in `s` with the value it names
/// (`steps.<name>.<path>` or `trigger.<path>`), rendered as JSON text.
/// FAILS with the raw token when a ref does not resolve — the caller turns
/// it into a workflow/node/field/path error (ADR-0032 Spec 4).
///
/// [`render_text`] is the single-string entrypoint for execution points
/// (per-item join templates); this is its shared implementation.
fn render_template_strict(
    s: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let token = &after[..end];
                match resolve_template(token.trim(), trigger, step_outputs) {
                    Some(replaced) => out.push_str(&replaced),
                    // Unresolvable — LOUD (ADR-0032 I10), never verbatim.
                    None => return Err(token.trim().to_string()),
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                break;
            }
        }
    }
    out.push_str(rest);
    Ok(out)
}

/// Resolve one template token to a rendered string, or `None` when the path
/// does not resolve (callers fail loud — ADR-0032 I10).
pub(crate) fn resolve_template(
    token: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let (root, path) = match token.split_once('.') {
        Some((root, path)) => (root, path),
        // Bare `{{item}}` — the current foreach iteration's value (injected
        // into the iteration trigger by run_foreach).
        None if token == "item" => ("item", ""),
        None => return None,
    };
    let value = match root {
        "trigger" => trigger,
        // `{{item}}` / `{{item.<path>}}` — the current foreach iteration value.
        "item" => {
            let item = trigger.get("item")?;
            if path.is_empty() {
                return Some(render_scalar(item));
            }
            return descend(item, path.split('.')).map(render_scalar);
        }
        // `{{inputs.x}}`: the invoke path embeds the caller's inputs under
        // `trigger.inputs` (invoke synthesizes the trigger payload from them).
        // Resolve the bare `inputs` root too — `{{inputs.goal}}` ==
        // `{{trigger.inputs.goal}}` on the invoke path.
        "inputs" => {
            let inputs = trigger.get("inputs")?;
            if path.is_empty() {
                return Some(render_scalar(inputs));
            }
            return descend(inputs, path.split('.')).map(render_scalar);
        }
        "steps" => return resolve_steps(path, step_outputs),
        _ => return None,
    };
    descend(value, path.split('.')).map(render_scalar)
}

/// Resolve `steps.<name>.<path>…` (the whole object when no `<path>`).
pub(crate) fn resolve_steps(
    path: &str,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let (name, rest) = match path.split_once('.') {
        Some((name, rest)) => (name, rest),
        None => (path, ""),
    };
    let value = step_outputs.get(name)?;
    if rest.is_empty() {
        return Some(render_scalar(value));
    }
    descend(value, rest.split('.')).map(render_scalar)
}

/// Walk a dotted path through a JSON value.
pub(crate) fn descend<'a>(
    mut value: &'a serde_json::Value,
    path: std::str::Split<'_, char>,
) -> Option<&'a serde_json::Value> {
    for part in path {
        value = value.get(part)?;
    }
    Some(value)
}

/// Render a resolved value as template text: scalars verbatim, structures as
/// compact JSON.
pub(crate) fn render_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
