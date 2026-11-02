// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Pure branch guards (bd-durable-branch-hhgc).
//!
//! A branch is a CHOICE inside the linear plan, so its guard is decided in the
//! pure fold — no async, no host I/O. The accepted grammar is the pure
//! template-comparison subset the guard module already normalizes:
//!
//! ```text
//! expr    := or
//! or      := and ("||" and)*
//! and     := not ("&&" not)*
//! not     := "!" not | cmp
//! cmp     := primary (("=="|"!="|"<="|">="|"<"|">") primary)?
//! primary := "(" expr ")" | literal | ident
//! literal := true | false | null | number | string
//! ident   := [A-Za-z_][A-Za-z0-9_]*
//! ```
//!
//! Anything outside it — a function call, arithmetic, an operator we do not
//! list — refuses the WHOLE workflow before any row, because the fold cannot
//! decide it from trigger + outputs alone. The reference names are the ones
//! [`super::control_flow::guard_vars`] binds (`trigger_<key>`,
//! `steps_<name>_<key>`), and the input is normalized by the guard module's
//! own [`crate::guard::normalize_guard_paths`] /
//! [`crate::guard::rewrite_bool_string_literals`] — there is no second
//! template path syntax.
//!
//! One deliberate difference from the async evaluator: `null` is a literal
//! here. evalexpr has no `null` token, so the old evaluator read it as an
//! unknown variable and failed the step. The authored toron guard
//! `{{steps.triage.patch}} != null` only means what it says under this
//! checker.

use serde_json::Value;

/// One branch-guard outcome in the fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PureGuard {
    /// Take the `then` arm.
    True,
    /// Take the `else` arm.
    False,
    /// The guard is in the grammar but cannot be decided (unknown reference,
    /// type mismatch). The branch step's `on_error` applies — never a silent
    /// `else`.
    Error(String),
}

/// Why a guard is not pure, when it is not.
///
/// # Errors
///
/// The first token or structural problem outside the accepted grammar.
pub fn check_pure_guard(guard: &str) -> Result<(), String> {
    let normalized = normalize(guard);
    let tokens = tokenize(&normalized)?;
    parse(&tokens).map(|_| ())
}

/// Decide one pure guard from the trigger and the outputs so far.
///
/// Never panics and never touches the host: an undecidable guard is an
/// [`PureGuard::Error`].
pub fn evaluate_pure_guard(
    guard: &str,
    trigger: &Value,
    outputs: &std::collections::BTreeMap<String, Value>,
) -> PureGuard {
    let normalized = normalize(guard);
    let parsed = tokenize(&normalized).and_then(|tokens| parse(&tokens));
    let expr = match parsed {
        Ok(expr) => expr,
        Err(error) => return PureGuard::Error(format!("guard {guard:?} is not pure: {error}")),
    };
    let vars = bind(trigger, outputs);
    match eval(&expr, &vars) {
        Ok(Lit::Bool(true)) => PureGuard::True,
        Ok(Lit::Bool(false)) => PureGuard::False,
        Ok(other) => PureGuard::Error(format!(
            "guard {guard:?} evaluated to {}, not a boolean",
            other.describe()
        )),
        Err(error) => PureGuard::Error(format!("guard {guard:?} failed to evaluate: {error}")),
    }
}

fn normalize(guard: &str) -> String {
    let flat = crate::guard::normalize_guard_paths(guard.trim());
    crate::guard::rewrite_bool_string_literals(&flat)
}

/// The reference bindings, named exactly like `guard_vars` (the one guard
/// binding scheme), but JSON-native so `null` compares as null.
fn bind(
    trigger: &Value,
    outputs: &std::collections::BTreeMap<String, Value>,
) -> std::collections::BTreeMap<String, Lit> {
    let mut vars = std::collections::BTreeMap::new();
    if let Some(obj) = trigger.as_object() {
        for (k, v) in obj {
            vars.insert(format!("trigger_{k}"), lit(v));
        }
    }
    for (name, output) in outputs {
        let name = name.replace('-', "_");
        if let Some(obj) = output.as_object() {
            // The durable spawn envelope is `{ok, spawn_key, result}`; the
            // child's value is `result`. Bind its fields BOTH explicitly
            // (`steps_<node>_result_<field>`) and transparently
            // (`steps_<node>_<field>`), so a guard authored for the old engine
            // — where the receipt landed at the top level — reads the same
            // value on both paths. Envelope fields win on a name collision.
            if let Some(serde_json::Value::Object(result)) = obj.get("result") {
                for (k, v) in result {
                    let key = k.replace('-', "_");
                    vars.insert(format!("steps_{name}_result_{key}"), lit(v));
                    vars.insert(format!("steps_{name}_{key}"), lit(v));
                }
            }
            for (k, v) in obj {
                vars.insert(format!("steps_{name}_{}", k.replace('-', "_")), lit(v));
            }
        } else {
            vars.insert(format!("steps_{name}"), lit(output));
        }
    }
    vars
}

fn lit(value: &Value) -> Lit {
    match value {
        Value::Bool(b) => Lit::Bool(*b),
        Value::Number(n) => Lit::Num(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) if s == "true" => Lit::Bool(true),
        Value::String(s) if s == "false" => Lit::Bool(false),
        Value::String(s) => Lit::Str(s.clone()),
        Value::Null => Lit::Null,
        other => Lit::Str(other.to_string()),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Lit {
    Bool(bool),
    Num(f64),
    Str(String),
    Null,
}

impl Lit {
    fn describe(&self) -> String {
        match self {
            Self::Bool(b) => format!("boolean {b}"),
            Self::Num(n) => format!("number {n}"),
            Self::Str(s) => format!("string {s:?}"),
            Self::Null => "null".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Lit(Lit),
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Not,
    LParen,
    RParen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
enum Expr {
    Lit(Lit),
    Var(String),
    Not(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Cmp(CmpOp, Box<Expr>, Box<Expr>),
}

fn tokenize(input: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let two = |a: char, b: char| chars.get(i + 1).copied() == Some(b) && c == a;
        if two('=', '=') {
            out.push(Tok::Eq);
            i += 2;
        } else if two('!', '=') {
            out.push(Tok::Ne);
            i += 2;
        } else if two('<', '=') {
            out.push(Tok::Le);
            i += 2;
        } else if two('>', '=') {
            out.push(Tok::Ge);
            i += 2;
        } else if two('&', '&') {
            out.push(Tok::And);
            i += 2;
        } else if two('|', '|') {
            out.push(Tok::Or);
            i += 2;
        } else {
            match c {
                '<' => {
                    out.push(Tok::Lt);
                    i += 1;
                }
                '>' => {
                    out.push(Tok::Gt);
                    i += 1;
                }
                '!' => {
                    out.push(Tok::Not);
                    i += 1;
                }
                '(' => {
                    out.push(Tok::LParen);
                    i += 1;
                }
                ')' => {
                    out.push(Tok::RParen);
                    i += 1;
                }
                '"' | '\'' => {
                    let (s, next) = string_literal(&chars, i)?;
                    out.push(Tok::Lit(Lit::Str(s)));
                    i = next;
                }
                '-' if chars.get(i + 1).is_some_and(char::is_ascii_digit) => {
                    let (n, next) = number_literal(&chars, i)?;
                    out.push(Tok::Lit(Lit::Num(n)));
                    i = next;
                }
                c if c.is_ascii_digit() => {
                    let (n, next) = number_literal(&chars, i)?;
                    out.push(Tok::Lit(Lit::Num(n)));
                    i = next;
                }
                c if c.is_ascii_alphabetic() || c == '_' => {
                    let mut ident = String::new();
                    while let Some(&ch) = chars.get(i) {
                        if ch.is_ascii_alphanumeric() || ch == '_' {
                            ident.push(ch);
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    out.push(match ident.as_str() {
                        "true" => Tok::Lit(Lit::Bool(true)),
                        "false" => Tok::Lit(Lit::Bool(false)),
                        "null" => Tok::Lit(Lit::Null),
                        _ => Tok::Ident(ident),
                    });
                }
                other => {
                    return Err(format!(
                        "token {other:?} is outside the pure comparison grammar"
                    ));
                }
            }
        }
    }
    Ok(out)
}

fn string_literal(chars: &[char], start: usize) -> Result<(String, usize), String> {
    let quote = chars[start];
    let mut out = String::new();
    let mut i = start + 1;
    while let Some(&c) = chars.get(i) {
        if c == '\\'
            && let Some(&next) = chars.get(i + 1)
        {
            out.push(next);
            i += 2;
            continue;
        }
        if c == quote {
            return Ok((out, i + 1));
        }
        out.push(c);
        i += 1;
    }
    Err(format!(
        "unterminated string literal starting at char {start}"
    ))
}

fn number_literal(chars: &[char], start: usize) -> Result<(f64, usize), String> {
    let mut text = String::new();
    let mut i = start;
    if chars[i] == '-' {
        text.push('-');
        i += 1;
    }
    while let Some(&c) = chars.get(i) {
        if c.is_ascii_digit() || c == '.' {
            text.push(c);
            i += 1;
        } else {
            break;
        }
    }
    text.parse::<f64>()
        .map(|n| (n, i))
        .map_err(|e| format!("number {text:?}: {e}"))
}

fn parse(tokens: &[Tok]) -> Result<Expr, String> {
    let mut pos = 0;
    let expr = parse_or(tokens, &mut pos)?;
    match tokens.get(pos) {
        None => Ok(expr),
        Some(tok) => Err(format!("unexpected trailing token {tok:?}")),
    }
}

fn parse_or(tokens: &[Tok], pos: &mut usize) -> Result<Expr, String> {
    let mut left = parse_and(tokens, pos)?;
    while tokens.get(*pos) == Some(&Tok::Or) {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(tokens: &[Tok], pos: &mut usize) -> Result<Expr, String> {
    let mut left = parse_not(tokens, pos)?;
    while tokens.get(*pos) == Some(&Tok::And) {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_not(tokens: &[Tok], pos: &mut usize) -> Result<Expr, String> {
    if tokens.get(*pos) == Some(&Tok::Not) {
        *pos += 1;
        return Ok(Expr::Not(Box::new(parse_not(tokens, pos)?)));
    }
    parse_cmp(tokens, pos)
}

fn parse_cmp(tokens: &[Tok], pos: &mut usize) -> Result<Expr, String> {
    let left = parse_primary(tokens, pos)?;
    let op = match tokens.get(*pos) {
        Some(Tok::Eq) => CmpOp::Eq,
        Some(Tok::Ne) => CmpOp::Ne,
        Some(Tok::Lt) => CmpOp::Lt,
        Some(Tok::Le) => CmpOp::Le,
        Some(Tok::Gt) => CmpOp::Gt,
        Some(Tok::Ge) => CmpOp::Ge,
        _ => return Ok(left),
    };
    *pos += 1;
    let right = parse_primary(tokens, pos)?;
    Ok(Expr::Cmp(op, Box::new(left), Box::new(right)))
}

fn parse_primary(tokens: &[Tok], pos: &mut usize) -> Result<Expr, String> {
    let Some(tok) = tokens.get(*pos) else {
        return Err("unexpected end of guard".to_string());
    };
    *pos += 1;
    match tok {
        Tok::Lit(lit) => Ok(Expr::Lit(lit.clone())),
        Tok::Ident(name) => Ok(Expr::Var(name.clone())),
        Tok::LParen => {
            let inner = parse_or(tokens, pos)?;
            if tokens.get(*pos) != Some(&Tok::RParen) {
                return Err("missing ')'".to_string());
            }
            *pos += 1;
            Ok(inner)
        }
        other => Err(format!("unexpected token {other:?}")),
    }
}

fn eval(expr: &Expr, vars: &std::collections::BTreeMap<String, Lit>) -> Result<Lit, String> {
    match expr {
        Expr::Lit(lit) => Ok(lit.clone()),
        Expr::Var(name) => vars
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown reference {name:?}")),
        Expr::Not(inner) => match eval(inner, vars)? {
            Lit::Bool(b) => Ok(Lit::Bool(!b)),
            other => Err(format!("'!' on {}", other.describe())),
        },
        Expr::And(left, right) => match eval(left, vars)? {
            Lit::Bool(false) => Ok(Lit::Bool(false)),
            Lit::Bool(true) => match eval(right, vars)? {
                Lit::Bool(b) => Ok(Lit::Bool(b)),
                other => Err(format!("'&&' on {}", other.describe())),
            },
            other => Err(format!("'&&' on {}", other.describe())),
        },
        Expr::Or(left, right) => match eval(left, vars)? {
            Lit::Bool(true) => Ok(Lit::Bool(true)),
            Lit::Bool(false) => match eval(right, vars)? {
                Lit::Bool(b) => Ok(Lit::Bool(b)),
                other => Err(format!("'||' on {}", other.describe())),
            },
            other => Err(format!("'||' on {}", other.describe())),
        },
        Expr::Cmp(op, left, right) => compare(*op, &eval(left, vars)?, &eval(right, vars)?),
    }
}

fn compare(op: CmpOp, left: &Lit, right: &Lit) -> Result<Lit, String> {
    match op {
        CmpOp::Eq => Ok(Lit::Bool(equal(left, right))),
        CmpOp::Ne => Ok(Lit::Bool(!equal(left, right))),
        CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
            let ordering = match (left, right) {
                (Lit::Num(a), Lit::Num(b)) => a.partial_cmp(b),
                (Lit::Str(a), Lit::Str(b)) => Some(a.cmp(b)),
                _ => {
                    return Err(format!(
                        "cannot order {} against {}",
                        left.describe(),
                        right.describe()
                    ));
                }
            };
            let Some(ordering) = ordering else {
                return Err("comparison is not ordered (NaN)".to_string());
            };
            Ok(Lit::Bool(match op {
                CmpOp::Lt => ordering.is_lt(),
                CmpOp::Le => ordering.is_le(),
                CmpOp::Gt => ordering.is_gt(),
                CmpOp::Ge => ordering.is_ge(),
                CmpOp::Eq | CmpOp::Ne => unreachable!("handled above"),
            }))
        }
    }
}

fn equal(left: &Lit, right: &Lit) -> bool {
    match (left, right) {
        (Lit::Null, Lit::Null) => true,
        (Lit::Bool(a), Lit::Bool(b)) => a == b,
        (Lit::Num(a), Lit::Num(b)) => a == b,
        (Lit::Str(a), Lit::Str(b)) => a == b,
        // A null reference never equals a non-null literal.
        _ => false,
    }
}
