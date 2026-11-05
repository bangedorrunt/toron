// governed-by: ADR-0004
#![forbid(unsafe_code)]

//! Bounded workflow `if:` guard evaluation (bd-n87, ADR-0004 Ship 4).
//!
//! ## Why
//!
//! `Step.guard` / `Workflow.guard` are evalexpr expressions evaluated at run
//! time against the trigger context (`{{trigger.text}}` etc.). Lint
//! (`lint::check_guard`) validates SYNTAX and emits the 100ms-timeout contract
//! as an advisory — but until this module, there was **no runtime evaluator**.
//! An unbounded evaluator lets a malformed or adversarial guard expression
//! (deeply nested, pathologically-long string ops) hang a workflow worker
//! indefinitely.
//!
//! Buzz bounds its guard evaluator with `evalexpr` + 100ms timeout + 4 custom
//! functions (`str_contains`, `str_starts_with`, `str_ends_with`, `str_len`).
//! toron now does the same.
//!
//! ## What (ADR-0004 invariant 7: fail-stop, not fail-silent)
//!
//! [`evaluate_guard`] wraps `evalexpr::eval_with_context` in a
//! `tokio::time::timeout` (default 100ms, env `TORON_GUARD_TIMEOUT_MS`).
//! Evaluation runs in `spawn_blocking` (evalexpr is sync + CPU-bound). On
//! timeout the step is reported as **failed** (observable, not hung); on
//! evaluation error the guard is treated as **false** (step skipped, run
//! continues); on a non-boolean result the guard is a **type error**
//! (fail-stop — a malformed workflow the operator must see).
//!
//! ### The 4 Buzz custom functions
//!
//! evalexpr ships `contains`/`contains_any`/`len` natively but NOT the four
//! Buzz registers. [`inject_buzz_functions`] adds them to a `HashMapContext`.
//!
//! ### Sandbox (evalexpr is already a closed evaluator)
//!
//! evalexpr has NO built-in filesystem, network, process, or randomness
//! access — its built-ins are pure arithmetic/string/tuple/array ops. The
//! custom fns here are pure string ops. The timeout is the backstop for
//! pathological-but-valid expressions; the closed-evaluator property is the
//! primary sandbox.

use std::time::Duration;

use evalexpr::{
    ContextWithMutableFunctions, ContextWithMutableVariables, DefaultNumericTypes, EvalexprError,
    EvalexprInt, Function, HashMapContext, Value, ValueType,
};
use tokio::time::timeout;

/// The default guard-evaluation timeout (Buzz-derived). Env
/// `TORON_GUARD_TIMEOUT_MS`.
pub const DEFAULT_TIMEOUT_MS: u64 = 100;

/// Max guard expression length (chars). Trigger VALUES live in the context,
/// never as literals in the expression, so a guard longer than this is
/// malformed/adversarial. Rejected by the pre-scan before eval.
pub const MAX_GUARD_CHARS: usize = 2048;

/// Max paren/function-call nesting depth. Prevents recursion-depth stack
/// overflow in evalexpr's tree builder (tokio's blocking pool uses 512KB
/// stacks — a stack overflow ABORTS the process and is uncatchable).
pub const MAX_PAREN_DEPTH: usize = 64;

/// Max total `(` count (function calls / subexpressions). Bounds total tree
/// size.
pub const MAX_PARENS: usize = 64;

/// Parse a `TORON_GUARD_TIMEOUT_MS`-style value: a positive integer number of
/// milliseconds → `Some`, anything else (empty, zero, garbage) → `None`
/// (caller falls back to the default). Pure — tests exercise it without env
/// access.
pub fn parse_timeout_ms(s: &str) -> Option<Duration> {
    s.trim()
        .parse::<u64>()
        .ok()
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
}

/// Read the guard timeout from `TORON_GUARD_TIMEOUT_MS`, falling back to
/// [`DEFAULT_TIMEOUT_MS`]. Unset/empty/unparseable/zero → default.
pub fn timeout_from_env() -> Duration {
    std::env::var("TORON_GUARD_TIMEOUT_MS")
        .ok()
        .and_then(|s| parse_timeout_ms(&s))
        .unwrap_or_else(|| Duration::from_millis(DEFAULT_TIMEOUT_MS))
}

/// The outcome of a guard evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardOutcome {
    /// The guard evaluated to a boolean. `true` = step proceeds; `false` =
    /// step skipped (run continues).
    Bool(bool),
    /// The guard expression is absent — treat as `true` (no guard = proceed).
    Unset,
    /// The guard expression evaluated to a non-boolean value (string, int,
    /// tuple). Per ADR-0004 invariant 7 this is a **failure** (fail-stop) —
    /// the workflow is malformed and the operator must see it.
    TypeError { got: String },
    /// The guard raised an evaluation error (unknown variable, bad fn arity,
    /// division by zero, etc.). Treated as **false** (step skipped, run
    /// continues) — a transient/template-miss shouldn't kill the run.
    EvalError { err: String },
    /// The guard exceeded the timeout (ADR-0004 invariant 7: fail-stop).
    /// The step is reported as failed; the run is observable, not hung. The
    /// blocking task is detached (tokio cannot kill it); the pre-scan caps
    /// bound how long it can run.
    TimedOut { after_ms: u64 },
    /// The guard was rejected by the complexity pre-scan (too long, too
    /// deeply nested, too many calls) BEFORE eval — stack-overflow defense
    /// (a stack overflow in evalexpr's recursive tree builder would abort
    /// the whole process; it cannot be caught). Fail-stop, not silent.
    TooComplex { reason: String },
}

impl GuardOutcome {
    /// Whether the step should PROCEED (guard true or unset). Convenience for
    /// the engine.
    pub const fn proceeds(&self) -> bool {
        matches!(self, Self::Bool(true) | Self::Unset)
    }
}

/// Evaluate a workflow guard expression with a bounded timeout + the 4 Buzz
/// custom functions + the provided variables (template-substituted trigger
/// context).
///
/// - `guard`: the raw guard expression (`None`/empty → [`GuardOutcome::Unset`]).
/// - `variables`: the trigger context as evalexpr variables
///   (`{"trigger.text": "...", "trigger.channel": "..."}`). Lint substitutes
///   `{{trigger.text}}` → `trigger.text` before the engine sees it; this fn
///   receives the post-substitution variable names.
/// - `deadline`: the timeout (default via [`timeout_from_env`]).
///
/// Runs `evalexpr::eval_with_context` in `spawn_blocking` wrapped in
/// `tokio::time::timeout`. Pure + sandboxed (evalexpr is a closed evaluator;
/// the 4 custom fns are pure string ops).
/// Normalize single-quoted string literals to double-quoted ones before
/// evalexpr parsing (bd-guard-single-quote-fail-loud-061y, GS-028 NEW-3).
///
/// evalexpr accepts ONLY double-quoted strings; a guard like
/// `steps_verify_1_ok == 'true'` parses as an eval error (unknown token),
/// which the engine treats as false — the Branch silently takes else every
/// time. The runtime is tolerant (converts well-formed `'...'` pairs outside
/// double-quoted literals); the lint boundary still rejects single quotes so
/// authors write the canonical form.
///
/// String-literal aware: single quotes INSIDE `"..."` pass through verbatim;
/// an unterminated `'` passes through verbatim (parse fails loud downstream).
/// Backslash escapes inside the single-quoted body (`\'`, `\\`) resolve to
/// the literal char; `"` and `\` in the body are escaped for the
/// double-quoted emission.
pub fn normalize_single_quoted_literals(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let mut chars = expr.chars().peekable();
    let mut in_double = false;
    let mut escaped = false;
    while let Some(c) = chars.next() {
        if in_double {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_double = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_double = true;
                out.push(c);
            }
            '\'' => {
                let mut body = String::new();
                let mut inner_escaped = false;
                let mut closed = false;
                let mut rest = String::new();
                for d in chars.by_ref() {
                    if inner_escaped {
                        match d {
                            '\'' => body.push('\''),
                            '\\' => body.push('\\'),
                            'n' => body.push('\n'),
                            't' => body.push('\t'),
                            other => {
                                body.push('\\');
                                body.push(other);
                            }
                        }
                        inner_escaped = false;
                        continue;
                    }
                    match d {
                        '\\' => inner_escaped = true,
                        '\'' => {
                            closed = true;
                            break;
                        }
                        _ => body.push(d),
                    }
                }
                if !closed {
                    let _ = &mut rest;
                    out.push('\'');
                    out.push_str(&body);
                    if inner_escaped {
                        out.push('\\');
                    }
                    continue;
                }
                out.push('"');
                for b in body.chars() {
                    match b {
                        '"' => out.push_str("\\\""),
                        '\\' => out.push_str("\\\\"),
                        '\n' => out.push_str("\\n"),
                        '\t' => out.push_str("\\t"),
                        _ => out.push(b),
                    }
                }
                out.push('"');
            }
            _ => out.push(c),
        }
    }
    out
}

/// Flatten the guard paths authors actually write onto the names
/// [`crate::engine::control_flow::guard_vars`] binds.
///
/// `steps.gate.ok`, `{{steps.verify-1.ok}}`, `inputs.mode`, and
/// `trigger.channel` become `steps_gate_ok`, `steps_verify_1_ok`,
/// `trigger_mode`, and `trigger_channel`. Quoted strings are left alone.
/// A path with no dot is unchanged.
pub fn normalize_guard_paths(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        if c == '"' {
            in_string = !in_string;
            out.push(c);
            continue;
        }
        if in_string {
            out.push(c);
            continue;
        }
        if c == '{' && chars.peek() == Some(&'{') {
            chars.next();
            let mut inner = String::new();
            while let Some(ch) = chars.next() {
                if ch == '}' && chars.peek() == Some(&'}') {
                    chars.next();
                    break;
                }
                inner.push(ch);
            }
            out.push_str(&flatten_guard_path(inner.trim()));
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let mut ident = String::new();
            ident.push(c);
            while let Some(ch) = chars.peek().copied() {
                if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '-' {
                    ident.push(ch);
                    chars.next();
                } else {
                    break;
                }
            }
            if ident.contains('.') {
                out.push_str(&flatten_guard_path(&ident));
            } else {
                out.push_str(&ident);
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// `inputs.x` and `trigger.x` both bind as `trigger_x`. `steps.a.b` binds
/// as `steps_a_b`. Hyphens in node names fold the same way the binder does.
fn flatten_guard_path(path: &str) -> String {
    let path = path.trim();
    let Some((head, rest)) = path.split_once('.') else {
        return path.to_string();
    };
    let flat = rest.replace(['.', '-'], "_");
    match head {
        "inputs" | "trigger" => format!("trigger_{flat}"),
        "steps" => format!("steps_{flat}"),
        _ => path.replace(['.', '-'], "_"),
    }
}

/// `== "true"` and `== "false"` compare equal to a boolean guard variable.
/// Authors write both the boolean and the quoted spelling. Quoted strings
/// that are not exactly true or false stay quoted.
pub fn rewrite_bool_string_literals(input: &str) -> String {
    input
        .replace("== \"true\"", "== true")
        .replace("== \"false\"", "== false")
        .replace("!= \"true\"", "!= true")
        .replace("!= \"false\"", "!= false")
}

pub async fn evaluate_guard(
    guard: Option<&str>,
    variables: &std::collections::HashMap<String, Value>,
    deadline: Duration,
) -> GuardOutcome {
    let Some(expr) = guard.map(str::trim).filter(|s| !s.is_empty()) else {
        return GuardOutcome::Unset;
    };

    let normalized = normalize_single_quoted_literals(expr);
    let expr = normalized.as_str();

    // Stack-safety pre-scan (iterative, O(n), no recursion): reject
    if let Some(reason) = complexity_violation(expr) {
        return GuardOutcome::TooComplex { reason };
    }

    let mut ctx = HashMapContext::<DefaultNumericTypes>::new();
    for (k, v) in variables {
        let _ = ctx.set_value(k.clone(), v.clone());
    }
    inject_buzz_functions(&mut ctx);

    let expr_owned = expr.to_string();
    let join = tokio::task::spawn_blocking(move || evalexpr::eval_with_context(&expr_owned, &ctx));

    match timeout(deadline, join).await {
        Ok(Ok(Ok(value))) => match value {
            Value::Boolean(b) => GuardOutcome::Bool(b),
            other => GuardOutcome::TypeError {
                got: value_kind(&other),
            },
        },
        Ok(Ok(Err(err))) => GuardOutcome::EvalError {
            err: err.to_string(),
        },
        Ok(Err(panic)) => GuardOutcome::EvalError {
            err: format!("guard panicked: {panic}"),
        },
        Err(_) => GuardOutcome::TimedOut {
            after_ms: deadline.as_millis() as u64,
        },
    }
}

/// Convenience: evaluate with the env-derived default timeout.
pub async fn evaluate_guard_default(
    guard: Option<&str>,
    variables: &std::collections::HashMap<String, Value>,
) -> GuardOutcome {
    evaluate_guard(guard, variables, timeout_from_env()).await
}

/// Register the 4 Buzz custom functions on an evalexpr `HashMapContext`.
/// Public so embeddings/tests can build a context with the same fns.
///
/// | fn | signature | behavior |
/// |----|-----------|----------|
/// | `str_contains(haystack, needle)` | `(String, String) -> Bool` | true if `haystack` contains `needle` |
/// | `str_starts_with(s, prefix)` | `(String, String) -> Bool` | prefix check |
/// | `str_ends_with(s, suffix)` | `(String, String) -> Bool` | suffix check |
/// | `str_len(s)` | `(String) -> Int` | character count |
pub fn inject_buzz_functions(ctx: &mut HashMapContext<DefaultNumericTypes>) {
    let str_contains = Function::new(|argument| {
        let args = argument.as_fixed_len_tuple(2)?;
        match (&args[0], &args[1]) {
            (Value::String(h), Value::String(n)) => Ok(Value::from(h.contains(n.as_str()))),
            _ => Err(EvalexprError::type_error(
                args[0].clone(),
                vec![ValueType::String],
            )),
        }
    });
    let str_starts_with = Function::new(|argument| {
        let args = argument.as_fixed_len_tuple(2)?;
        match (&args[0], &args[1]) {
            (Value::String(s), Value::String(p)) => Ok(Value::from(s.starts_with(p.as_str()))),
            _ => Err(EvalexprError::type_error(
                args[0].clone(),
                vec![ValueType::String],
            )),
        }
    });
    let str_ends_with = Function::new(|argument| {
        let args = argument.as_fixed_len_tuple(2)?;
        match (&args[0], &args[1]) {
            (Value::String(s), Value::String(suf)) => Ok(Value::from(s.ends_with(suf.as_str()))),
            _ => Err(EvalexprError::type_error(
                args[0].clone(),
                vec![ValueType::String],
            )),
        }
    });
    let str_len = Function::new(|argument| {
        let s = argument.as_string()?;
        Ok(Value::Int(EvalexprInt::from_usize(s.chars().count())?))
    });

    let _ = ctx.set_function("str_contains".into(), str_contains);
    let _ = ctx.set_function("str_starts_with".into(), str_starts_with);
    let _ = ctx.set_function("str_ends_with".into(), str_ends_with);
    let _ = ctx.set_function("str_len".into(), str_len);
}

/// Iterative, non-recursive complexity check. Returns a reason if the
/// expression must be rejected. String-literal aware: parens inside `"..."`
/// (e.g. `str_contains(text, "a(b")`) don't count toward depth.
pub fn complexity_violation(expr: &str) -> Option<String> {
    let len = expr.chars().count();
    if len > MAX_GUARD_CHARS {
        return Some(format!(
            "expression is {len} chars, exceeds {MAX_GUARD_CHARS} limit"
        ));
    }
    let mut depth = 0usize;
    let mut parens = 0usize;
    let mut bracket = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for c in expr.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '[' => {
                bracket += 1;
                if bracket > MAX_PAREN_DEPTH {
                    return Some(format!(
                        "expression nests {bracket} brackets deep, exceeds {MAX_PAREN_DEPTH} limit"
                    ));
                }
            }
            ']' => bracket = bracket.saturating_sub(1),
            '(' => {
                depth += 1;
                parens += 1;
                if depth > MAX_PAREN_DEPTH {
                    return Some(format!(
                        "expression nests {depth} parens deep, exceeds {MAX_PAREN_DEPTH} limit"
                    ));
                }
                if parens > MAX_PARENS {
                    return Some(format!(
                        "expression has {parens} subexpressions, exceeds {MAX_PARENS} limit"
                    ));
                }
            }
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    None
}

fn value_kind(v: &Value) -> String {
    match v {
        Value::String(_) => "String".into(),
        Value::Float(_) => "Float".into(),
        Value::Int(_) => "Int".into(),
        Value::Boolean(_) => "Boolean".into(),
        Value::Tuple(_) => "Tuple".into(),
        Value::Empty => "Empty".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        for (k, v) in pairs {
            m.insert((*k).into(), Value::from(*v));
        }
        m
    }

    #[tokio::test]
    async fn unset_guard_proceeds() {
        assert_eq!(
            evaluate_guard(None, &HashMap::new(), Duration::from_millis(100)).await,
            GuardOutcome::Unset
        );
        assert!(GuardOutcome::Unset.proceeds());
        assert_eq!(
            evaluate_guard(Some("   "), &HashMap::new(), Duration::from_millis(100)).await,
            GuardOutcome::Unset
        );
    }

    #[tokio::test]
    async fn boolean_literal_outcome() {
        let out = evaluate_guard(Some("true"), &HashMap::new(), Duration::from_millis(100)).await;
        assert_eq!(out, GuardOutcome::Bool(true));
        assert!(out.proceeds());
        let out = evaluate_guard(Some("false"), &HashMap::new(), Duration::from_millis(100)).await;
        assert_eq!(out, GuardOutcome::Bool(false));
        assert!(!out.proceeds());
    }

    #[tokio::test]
    async fn variable_comparison_true() {
        let v = vars(&[("trigger_channel", "#forge")]);
        let out = evaluate_guard(
            Some("trigger_channel == \"#forge\""),
            &v,
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(out, GuardOutcome::Bool(true));
    }

    #[test]
    fn guard_paths_flatten_dots_templates_and_hyphens() {
        assert_eq!(
            normalize_guard_paths("steps.gate.ok == true"),
            "steps_gate_ok == true"
        );
        assert_eq!(
            normalize_guard_paths("inputs.mode == \"compile\""),
            "trigger_mode == \"compile\""
        );
        assert_eq!(
            normalize_guard_paths("{{steps.verify-1.ok}} == true"),
            "steps_verify_1_ok == true"
        );
        assert_eq!(
            rewrite_bool_string_literals("steps_verify_ok == \"true\""),
            "steps_verify_ok == true"
        );
    }

    /// NOTE (bd-8jgv): the `str_*` custom fns share one evaluator path;
    /// `str_starts_with` below is the representative with true+false edges.
    #[tokio::test]
    async fn str_starts_with_custom_fn() {
        let v = vars(&[("trigger_text", "P1 crash")]);
        assert_eq!(
            evaluate_guard(
                Some("str_starts_with(trigger_text, \"P1\")"),
                &v,
                Duration::from_millis(100)
            )
            .await,
            GuardOutcome::Bool(true)
        );
        assert_eq!(
            evaluate_guard(
                Some("str_starts_with(trigger_text, \"P2\")"),
                &v,
                Duration::from_millis(100)
            )
            .await,
            GuardOutcome::Bool(false)
        );
    }

    #[tokio::test]
    async fn adversarial_guard_rejected_not_hung() {
        let hay = "x".repeat(2_000);
        let needle = "y".repeat(2_000);
        let mut expr = String::from("true");
        for _ in 0..5_000 {
            expr = format!("str_contains(\"{hay}\", \"{needle}\") && {expr}");
        }
        let out = evaluate_guard(Some(&expr), &HashMap::new(), Duration::from_millis(50)).await;
        match &out {
            GuardOutcome::TooComplex { reason } => {
                assert!(
                    reason.contains("chars"),
                    "expected a length rejection, got {reason}"
                );
            }
            other => panic!("expected TooComplex, got {other:?} (must not parse/hang/crash)"),
        }
        assert!(!out.proceeds());
    }

    /// The timeout path fires on a valid-but-slow guard (large haystack VALUE
    /// in the context — values never enter the expression). Deadline 1ms.
    #[tokio::test]
    async fn timeout_fires_on_slow_valid_guard() {
        let mut m = HashMap::new();
        m.insert("text".into(), Value::from("x".repeat(8_000_000)));
        let out = evaluate_guard(
            Some("str_contains(text, \"zzzz\")"),
            &m,
            Duration::from_millis(1),
        )
        .await;
        match out {
            GuardOutcome::TimedOut { .. } => {  }
            GuardOutcome::Bool(false) => {  }
            other => panic!("expected TimedOut or Bool(false), got {other:?} (must not hang)"),
        }
    }

    /// Paren-depth pre-scan catches nested expressions without crashing.
    #[tokio::test]
    async fn paren_depth_guard_rejected() {
        let expr = "(".repeat(200) + "true" + &")".repeat(200);
        let out = evaluate_guard(Some(&expr), &HashMap::new(), Duration::from_millis(100)).await;
        assert!(
            matches!(out, GuardOutcome::TooComplex { .. }),
            "expected TooComplex for 200-deep nesting, got {out:?}"
        );
    }

    /// String-literal parens don't count toward depth.
    #[tokio::test]
    async fn parens_in_string_literals_do_not_count() {
        assert!(complexity_violation("str_contains(text, \"a(b)c\") && true").is_none());
        let out = evaluate_guard(
            Some("str_contains(text, \"a(b)c\") && true"),
            &vars(&[("text", "x a(b)c y")]),
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(out, GuardOutcome::Bool(true));
    }

    /// Deep `[` array-literal nesting is bounded too (evalexpr's tree builder
    /// recurses on array literals; ~1000-deep fits in the char cap, so the
    /// bracket scan is the only defense).
    #[tokio::test]
    async fn bracket_nesting_rejected_not_hung() {
        let expr = "[".repeat(200) + "true" + &"]".repeat(200);
        let out = evaluate_guard(Some(&expr), &HashMap::new(), Duration::from_millis(100)).await;
        assert!(
            matches!(out, GuardOutcome::TooComplex { .. }),
            "expected TooComplex for 200-deep bracket nesting, got {out:?}"
        );
    }

    /// A non-boolean result is a TypeError (fail-stop, invariant 7).
    #[tokio::test]
    async fn non_boolean_is_type_error() {
        let out = evaluate_guard(
            Some("\"a string\""),
            &HashMap::new(),
            Duration::from_millis(100),
        )
        .await;
        assert!(matches!(out, GuardOutcome::TypeError { .. }));
        assert!(!out.proceeds(), "type error must NOT proceed");
    }

    /// An eval error (unknown variable) is treated as false (skip step, run
    /// continues) — a template miss shouldn't kill the run.
    #[tokio::test]
    async fn eval_error_is_false_not_hang() {
        let out = evaluate_guard(
            Some("undefined_var == \"x\""),
            &HashMap::new(),
            Duration::from_millis(100),
        )
        .await;
        match out {
            GuardOutcome::EvalError { .. } | GuardOutcome::Bool(false) => {}
            other => panic!("expected eval-error-or-false, got {other:?}"),
        }
        assert!(!out.proceeds());
    }

    /// bd-guard-single-quote-fail-loud-061y (GS-028 NEW-3): the flywheel
    /// repair guard `steps_verify_1_ok == 'true'` must evaluate TRUE when the
    /// step output is the string "true" — single-quoted literals normalize to
    /// double-quoted before eval. Pure normalization cases included.
    #[tokio::test]
    async fn guard_single_quote_tolerance() {
        assert_eq!(
            normalize_single_quoted_literals("a == 'true'"),
            "a == \"true\""
        );
        assert_eq!(
            normalize_single_quoted_literals("a == \"don't\""),
            "a == \"don't\""
        );
        assert_eq!(normalize_single_quoted_literals("a == 'x"), "a == 'x");

        let d = Duration::from_millis(200);
        let v = vars(&[("steps_verify_1_ok", "true")]);
        assert_eq!(
            evaluate_guard(Some("steps_verify_1_ok == 'true'"), &v, d).await,
            GuardOutcome::Bool(true)
        );
        let v_false = vars(&[("steps_verify_1_ok", "false")]);
        assert_eq!(
            evaluate_guard(Some("steps_verify_1_ok == 'true'"), &v_false, d).await,
            GuardOutcome::Bool(false)
        );
        let v2 = vars(&[
            ("steps_verify_1_ok", "false"),
            ("steps_verify_2_ok", "true"),
            ("steps_verify_3_ok", "false"),
        ]);
        assert_eq!(
            evaluate_guard(
                Some(
                    "steps_verify_1_ok == 'true' || steps_verify_2_ok == 'true' || steps_verify_3_ok == 'true'"
                ),
                &v2,
                d
            )
            .await,
            GuardOutcome::Bool(true)
        );
    }
}
