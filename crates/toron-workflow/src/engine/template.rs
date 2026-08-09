// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Template interpolation (F21.ENGINE-SPLIT move from the former single-file
//! engine): `{{trigger.<path>}}` / `{{steps.<name>.<path>}}` / `{{item}}` /
//! `{{inputs.x}}` substitution for action fields and `foreach` `over`
//! resolution. Unresolvable tokens stay verbatim (backward compatible).

use crate::schema::Action;

/// Substitute `{{trigger.<path>}}` / `{{steps.<name>.<path>}}` templates in an
/// action's string fields before the runner sees them (ADR-0005 Spec 0:
/// value-returning actions flow into `steps.<name>` for downstream templates).
///
/// Unresolvable tokens stay verbatim (backward-compatible — a body that
/// references a step that didn't produce output keeps its literal text).
pub(crate) fn render_action(
    action: &Action,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Action> {
    let mut value = serde_json::to_value(action)?;
    render_json_value(&mut value, trigger, step_outputs);
    serde_json::from_value(value)
        .map_err(|e| anyhow::anyhow!("re-deserialize rendered action: {e}"))
}

/// Recursively render template tokens in every string leaf of a JSON value.
pub(crate) fn render_json_value(
    value: &mut serde_json::Value,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(s) => {
            *s = render_template(s, trigger, step_outputs);
        }
        serde_json::Value::Array(items) => {
            for item in items {
                render_json_value(item, trigger, step_outputs);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values_mut() {
                render_json_value(item, trigger, step_outputs);
            }
        }
        _ => {}
    }
}

/// Replace every `{{...}}` token in `s` with the value it names
/// (`steps.<name>.<path>` or `trigger.<path>`), rendered as JSON text.
/// Unresolvable tokens are left verbatim.
pub(crate) fn render_template(
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
                    // Unresolvable — keep the literal token.
                    out.push_str(&format!("{{{{{token}}}}}"));
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

/// Resolve one template token to a rendered string, or `None` when the path
/// does not resolve (caller keeps the literal).
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
        "steps" => return resolve_steps(&path, step_outputs),
        _ => return None,
    };
    descend(value, path.split('.')).map(|v| render_scalar(v))
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
    mut path: std::str::Split<'_, char>,
) -> Option<&'a serde_json::Value> {
    while let Some(part) = path.next() {
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
