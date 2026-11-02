// governed-by: ADR-0032, ADR-0043
#![forbid(unsafe_code)]

//! `Action::Reduce`: a bounded pure fold over a proper list.
//!
//! Pure by contract: the fold reads `trigger` and the landed `steps` outputs,
//! never the host. It runs in the durable fold (linear) and in the single-shot
//! node executor (graph), with one implementation so the two substrates cannot
//! drift.
//!
//! The contracts the old engine pinned (bd-3pti) are kept exactly: a non-list
//! `over` fails loud, a missing key names the key and index, join without a
//! template fails, inputs over 1024 fail naming the bound, and sum never
//! coerces a non-number.

use serde_json::{Map, Value, json};

use crate::schema::{Action, ReduceOp};
use crate::engine::template::{RenderContext, render_action, render_scalar};

/// The bounded fold size (bd-3pti).
pub const MAX_REDUCE_ITEMS: usize = 1024;

/// Evaluate one reduce action to a step value (`{ok:false,error}` on refusal).
pub fn evaluate(
    action: &Action,
    trigger: &Value,
    outputs: &std::collections::BTreeMap<String, Value>,
    workflow: &str,
    node: &str,
) -> Value {
    if !matches!(action, Action::Reduce { .. }) {
        return fail("reduce: not a reduce action");
    }
    // Strict render first: an unresolvable ref fails here with attribution.
    let render_outputs: Map<String, Value> =
        outputs.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    // `{{item}}` is deferred context: render the action with the join
    // template removed, then restore it for the per-item pass.
    let mut to_render = action.clone();
    if let Action::Reduce { template, .. } = &mut to_render {
        *template = None;
    }
    let rendered = match render_action(
        &to_render,
        trigger,
        &render_outputs,
        &RenderContext::new(workflow, node),
    ) {
        Ok(action) => action,
        Err(e) => return fail(format!("render: {e}")),
    };
    let original_template = match action {
        Action::Reduce { template, .. } => template.clone(),
        _ => None,
    };
    let Action::Reduce {
        over,
        op,
        key,
        template: _,
        separator,
        init,
    } = &rendered
    else {
        unreachable!("rendered action is still a reduce")
    };
    let list = match resolve_list(over, trigger, outputs) {
        Ok(list) => list,
        Err(e) => return fail(e),
    };
    if list.len() > MAX_REDUCE_ITEMS {
        return fail(format!(
            "reduce: {} items exceeds the bound {MAX_REDUCE_ITEMS}",
            list.len()
        ));
    }
    match op {
        ReduceOp::All => fold_bool(&list, key.as_deref(), init, true, |a, b| a && b),
        ReduceOp::Any => fold_bool(&list, key.as_deref(), init, false, |a, b| a || b),
        ReduceOp::Count => {
            let start = match init {
                Some(Value::Number(n)) => n.as_u64().unwrap_or(0),
                Some(other) => {
                    return fail(format!("reduce count: init must be a number, got {other}"))
                }
                None => 0,
            };
            let mut n = start;
            for (index, item) in list.iter().enumerate() {
                let value = match operand_checked(item, key.as_deref(), index) {
                    Ok(v) => v,
                    Err(e) => return fail(e),
                };
                if is_truthy(value) {
                    n += 1;
                }
            }
            json!(n)
        }
        ReduceOp::Sum => {
            let mut total = match init {
                Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                Some(other) => {
                    return fail(format!("reduce sum: init must be a number, got {other}"))
                }
                None => 0.0,
            };
            for (index, item) in list.iter().enumerate() {
                let value = match operand_checked(item, key.as_deref(), index) {
                    Ok(v) => v,
                    Err(e) => return fail(e),
                };
                let Some(n) = value.as_f64() else {
                    return fail(format!(
                        "reduce sum: item {index} is not a number: {value}"
                    ));
                };
                total += n;
            }
            if total.fract() == 0.0 {
                json!(total as i64)
            } else {
                json!(total)
            }
        }
        ReduceOp::Join => {
            let Some(template) = &original_template else {
                return fail("reduce join: a template is required");
            };
            let sep = separator.as_deref().unwrap_or("");
            let mut out = match init {
                Some(Value::String(s)) => s.clone(),
                Some(other) => {
                    return fail(format!("reduce join: init must be a string, got {other}"))
                }
                None => String::new(),
            };
            let rendered: Vec<String> = list
                .iter()
                .map(|item| render_item(template, item))
                .collect();
            for (i, piece) in rendered.iter().enumerate() {
                if i > 0 || !out.is_empty() {
                    out.push_str(sep);
                }
                out.push_str(piece);
            }
            json!(out)
        }
    }
}

fn fold_bool(
    list: &[Value],
    key: Option<&str>,
    init: &Option<Value>,
    empty: bool,
    op: impl Fn(bool, bool) -> bool,
) -> Value {
    let start = match init {
        Some(Value::Bool(b)) => *b,
        Some(other) => return fail(format!("reduce all/any: init must be a boolean, got {other}")),
        None => empty,
    };
    let mut acc = start;
    for (index, item) in list.iter().enumerate() {
        let value = match operand_checked(item, key, index) {
            Ok(v) => v,
            Err(e) => return fail(e),
        };
        acc = op(acc, is_truthy(value));
    }
    json!(acc)
}

/// A missing key is a refusal, not `null`: the caller must know the row.
fn operand_checked<'a>(item: &'a Value, key: Option<&str>, index: usize) -> Result<&'a Value, String> {
    let Some(key) = key else { return Ok(item) };
    let mut cur = item;
    for part in key.split('.') {
        match cur.get(part) {
            Some(next) => cur = next,
            None => {
                return Err(format!(
                    "reduce: key {key:?} missing on item {index}: {item}"
                ));
            }
        }
    }
    Ok(cur)
}

fn resolve_list(
    over: &str,
    trigger: &Value,
    outputs: &std::collections::BTreeMap<String, Value>,
) -> Result<Vec<Value>, String> {
    let text = over.trim();
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        return match value {
            Value::Array(items) => Ok(items),
            other => Err(format!(
                "reduce: over must resolve to a proper list, got {}",
                kind(&other)
            )),
        };
    }
    // A ref path: trigger.<path> / steps.<name>.<path>.
    let path = text.strip_prefix("{{").and_then(|s| s.strip_suffix("}}"));
    let path = path.map(str::trim).unwrap_or(text);
    let value = resolve_path(path, trigger, outputs).ok_or_else(|| {
        format!("reduce: over must resolve to a proper list, got {text:?} (unresolved ref)")
    })?;
    match value {
        Value::Array(items) => Ok(items.clone()),
        other => Err(format!(
            "reduce: over must resolve to a proper list, got {}",
            kind(other)
        )),
    }
}

fn resolve_path<'a>(
    path: &str,
    trigger: &'a Value,
    outputs: &'a std::collections::BTreeMap<String, Value>,
) -> Option<&'a Value> {
    let (root, rest) = path.split_once('.')?;
    let base = match root {
        "trigger" => trigger,
        "steps" => {
            let (name, field) = rest.split_once('.')?;
            let value = outputs.get(name)?;
            return walk(value, field);
        }
        _ => return None,
    };
    walk(base, rest)
}

fn walk<'a>(mut value: &'a Value, path: &str) -> Option<&'a Value> {
    for part in path.split('.') {
        value = value.get(part)?;
    }
    Some(value)
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "object",
    }
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Render `{{item}}` / `{{item.path}}` for one join piece.
fn render_item(template: &str, item: &Value) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let token = after[..end].trim();
        let value = if token == "item" {
            item
        } else if let Some(path) = token.strip_prefix("item.") {
            walk(item, path).unwrap_or(&Value::Null)
        } else {
            // An unknown token stays verbatim, matching the renderer's
            // deferred-context rule for iteration bodies.
            out.push_str(&rest[start..start + 2 + end + 2]);
            rest = &after[end + 2..];
            continue;
        };
        out.push_str(&render_scalar(value));
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

fn fail(error: impl Into<String>) -> Value {
    json!({ "ok": false, "error": error.into() })
}
