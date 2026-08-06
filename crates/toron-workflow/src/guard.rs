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

/// Read the guard timeout from `TORON_GUARD_TIMEOUT_MS`, falling back to
/// [`DEFAULT_TIMEOUT_MS`]. Unset/empty/unparseable/zero → default.
pub fn timeout_from_env() -> Duration {
    std::env::var("TORON_GUARD_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
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
    pub fn proceeds(&self) -> bool {
        matches!(
            self,
            Self::Bool(true) | Self::Unset
        )
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
pub async fn evaluate_guard(
    guard: Option<&str>,
    variables: &std::collections::HashMap<String, Value>,
    deadline: Duration,
) -> GuardOutcome {
    let Some(expr) = guard.map(str::trim).filter(|s| !s.is_empty()) else {
        return GuardOutcome::Unset;
    };

    // Stack-safety pre-scan (iterative, O(n), no recursion): reject
    // pathological expressions before evalexpr's recursive tree builder can
    // overflow the 512KB blocking-pool stack (uncatchable SIGABRT).
    if let Some(reason) = complexity_violation(expr) {
        return GuardOutcome::TooComplex { reason };
    }

    // Build the context once (cheap). Variables + the 4 custom fns.
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
        // The blocking task panicked — treat as an eval error (don't hang the
        // run on a panicking expression).
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
            _ => Err(EvalexprError::type_error(args[0].clone(), vec![ValueType::String])),
        }
    });
    let str_starts_with = Function::new(|argument| {
        let args = argument.as_fixed_len_tuple(2)?;
        match (&args[0], &args[1]) {
            (Value::String(s), Value::String(p)) => Ok(Value::from(s.starts_with(p.as_str()))),
            _ => Err(EvalexprError::type_error(args[0].clone(), vec![ValueType::String])),
        }
    });
    let str_ends_with = Function::new(|argument| {
        let args = argument.as_fixed_len_tuple(2)?;
        match (&args[0], &args[1]) {
            (Value::String(s), Value::String(suf)) => Ok(Value::from(s.ends_with(suf.as_str()))),
            _ => Err(EvalexprError::type_error(args[0].clone(), vec![ValueType::String])),
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
        // Empty/whitespace guard is also Unset.
        assert_eq!(
            evaluate_guard(Some("   "), &HashMap::new(), Duration::from_millis(100)).await,
            GuardOutcome::Unset
        );
    }

    #[tokio::test]
    async fn boolean_true_proceeds() {
        let out = evaluate_guard(Some("true"), &HashMap::new(), Duration::from_millis(100)).await;
        assert_eq!(out, GuardOutcome::Bool(true));
        assert!(out.proceeds());
    }

    #[tokio::test]
    async fn boolean_false_skips() {
        let out = evaluate_guard(Some("false"), &HashMap::new(), Duration::from_millis(100)).await;
        assert_eq!(out, GuardOutcome::Bool(false));
        assert!(!out.proceeds());
    }

    #[tokio::test]
    async fn variable_comparison_true() {
        let v = vars(&[("trigger_channel", "#forge")]);
        // evalexpr identifiers can't contain '.', so tests use underscores;
        // the engine substitutes {{trigger.channel}} → a valid identifier.
        let out =
            evaluate_guard(Some("trigger_channel == \"#forge\""), &v, Duration::from_millis(100))
                .await;
        assert_eq!(out, GuardOutcome::Bool(true));
    }

    #[tokio::test]
    async fn str_contains_custom_fn() {
        let v = vars(&[("trigger_text", "production is down")]);
        let out = evaluate_guard(
            Some("str_contains(trigger_text, \"production\")"),
            &v,
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(out, GuardOutcome::Bool(true));
    }

    #[tokio::test]
    async fn str_starts_with_custom_fn() {
        let v = vars(&[("trigger_text", "P1 crash")]);
        assert_eq!(
            evaluate_guard(Some("str_starts_with(trigger_text, \"P1\")"), &v, Duration::from_millis(100)).await,
            GuardOutcome::Bool(true)
        );
        assert_eq!(
            evaluate_guard(Some("str_starts_with(trigger_text, \"P2\")"), &v, Duration::from_millis(100)).await,
            GuardOutcome::Bool(false)
        );
    }

    #[tokio::test]
    async fn str_ends_with_custom_fn() {
        let v = vars(&[("trigger_text", "deploy.log")]);
        assert_eq!(
            evaluate_guard(Some("str_ends_with(trigger_text, \".log\")"), &v, Duration::from_millis(100)).await,
            GuardOutcome::Bool(true)
        );
    }

    #[tokio::test]
    async fn str_len_custom_fn() {
        let v = vars(&[("trigger_text", "hello")]);
        assert_eq!(
            evaluate_guard(Some("str_len(trigger_text) > 3"), &v, Duration::from_millis(100)).await,
            GuardOutcome::Bool(true)
        );
    }

    /// ADR-0004 verification: an adversarial guard expression that would
    /// stack-overflow evalexpr's recursive tree builder (uncatchable SIGABRT)
    /// is rejected by the complexity pre-scan BEFORE eval — never hung, never
    /// crashing the process.
    #[tokio::test]
    async fn adversarial_guard_rejected_not_hung() {
        // ~20MB expression: 5000 nested str_contains calls with 2000-char
        // string args. Pathological but formerly VALID evalexpr — and it
        // stack-overflowed the whole test process (this test is the
        // regression: pre-scan must reject it before parse).
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
        // 8MB haystack; values live in the context, so the EXPRESSION stays
        // short and passes the pre-scan.
        m.insert("text".into(), Value::from("x".repeat(8_000_000)));
        let out =
            evaluate_guard(Some("str_contains(text, \"zzzz\")"), &m, Duration::from_millis(1)).await;
        match out {
            GuardOutcome::TimedOut { .. } => { /* expected: 1ms deadline vs ms-scale scan */ }
            GuardOutcome::Bool(false) => { /* occasionally finishes <1ms — also fine */ }
            other => panic!("expected TimedOut or Bool(false), got {other:?} (must not hang)"),
        }
    }

    /// Paren-depth pre-scan catches nested expressions without crashing.
    #[tokio::test]
    async fn paren_depth_guard_rejected() {
        // 200-deep nested ( ... ) — over MAX_PAREN_DEPTH=64, under the char
        // cap. Parens here are real grouping, not string literals.
        let expr = "(".repeat(200) + "true" + &")".repeat(200);
        let out =
            evaluate_guard(Some(&expr), &HashMap::new(), Duration::from_millis(100)).await;
        assert!(
            matches!(out, GuardOutcome::TooComplex { .. }),
            "expected TooComplex for 200-deep nesting, got {out:?}"
        );
    }

    /// String-literal parens don't count toward depth.
    #[tokio::test]
    async fn parens_in_string_literals_do_not_count() {
        assert!(complexity_violation("str_contains(text, \"a(b)c\") && true").is_none());
        // A legit-ish guard under the caps passes.
        let out = evaluate_guard(
            Some("str_contains(text, \"a(b)c\") && true"),
            &vars(&[("text", "x a(b)c y")]),
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(out, GuardOutcome::Bool(true));
    }

    /// A non-boolean result is a TypeError (fail-stop, invariant 7).
    #[tokio::test]
    async fn non_boolean_is_type_error() {
        let out =
            evaluate_guard(Some("\"a string\""), &HashMap::new(), Duration::from_millis(100)).await;
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

    /// Env override reads through; zero/garbage fall back to default.
    #[test]
    fn env_timeout_override() {
        std::env::set_var("TORON_GUARD_TIMEOUT_MS", "42");
        assert_eq!(timeout_from_env(), Duration::from_millis(42));
        std::env::set_var("TORON_GUARD_TIMEOUT_MS", "0");
        assert_eq!(timeout_from_env(), Duration::from_millis(DEFAULT_TIMEOUT_MS));
        std::env::set_var("TORON_GUARD_TIMEOUT_MS", "garbage");
        assert_eq!(timeout_from_env(), Duration::from_millis(DEFAULT_TIMEOUT_MS));
        std::env::remove_var("TORON_GUARD_TIMEOUT_MS");
        assert_eq!(timeout_from_env(), Duration::from_millis(DEFAULT_TIMEOUT_MS));
    }

    /// `evaluate_guard_default` uses the env timeout.
    #[tokio::test]
    async fn default_uses_env_timeout() {
        std::env::set_var("TORON_GUARD_TIMEOUT_MS", "200");
        let out = evaluate_guard_default(Some("true"), &HashMap::new()).await;
        assert_eq!(out, GuardOutcome::Bool(true));
        std::env::remove_var("TORON_GUARD_TIMEOUT_MS");
    }

    /// A built context with the 4 fns evaluates each correctly (smoke for
    /// inject_buzz_functions via the public evaluator path).
    #[tokio::test]
    async fn all_four_fns_callable() {
        let v = vars(&[("s", "hello world")]);
        let d = Duration::from_millis(200);
        assert_eq!(
            evaluate_guard(Some("str_contains(s, \"world\")"), &v, d).await,
            GuardOutcome::Bool(true)
        );
        assert_eq!(
            evaluate_guard(Some("str_starts_with(s, \"hello\")"), &v, d).await,
            GuardOutcome::Bool(true)
        );
        assert_eq!(
            evaluate_guard(Some("str_ends_with(s, \"world\")"), &v, d).await,
            GuardOutcome::Bool(true)
        );
        assert_eq!(
            evaluate_guard(Some("str_len(s) == 11"), &v, d).await,
            GuardOutcome::Bool(true)
        );
    }
}
