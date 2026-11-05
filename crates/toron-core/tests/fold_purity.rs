// governed-by: ADR-0041
//! Fold purity (ADR-0041; stands in for the elm-effect-lints crate, which
//! is not wired into toron yet). The comms folds must be pure: the outbox
//! `Program` (init/update) and the whole delivery FSM read no clock, no
//! randomness, no env, no filesystem, no store, no runtime. Effects belong
//! to the interpreter (`OutboxRun`) and the store, never to a fold.

const FORBIDDEN: &[&str] = &[
    "SystemTime",
    "Instant",
    "now_us",
    "rand::",
    "std::env",
    "env::var",
    "std::fs",
    "sqlx",
    "tokio",
    "spawn",
    "thread::",
    "std::process",
    "println!",
    "eprintln!",
    "ToronLog",
    "InboxStore",
    "provider_append",
    "provider_reconcile",
];

/// Lines between `start` (first match) and the next line that starts a
/// top-level item at column 0 that is not `}`.
fn block<'a>(src: &'a str, start: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut inside = false;
    for line in src.lines() {
        if !inside {
            if line.starts_with(start) {
                inside = true;
                out.push(line);
            }
            continue;
        }
        if line == "}" {
            out.push(line);
            break;
        }
        out.push(line);
    }
    assert!(inside, "block `{start}` not found");
    out
}

fn offenders(lines: &[&str]) -> Vec<String> {
    lines
        .iter()
        .filter(|l| !l.trim_start().starts_with("//"))
        .filter_map(|l| {
            FORBIDDEN
                .iter()
                .find(|f| l.contains(**f))
                .map(|f| format!("{f}: {l}"))
        })
        .collect()
}

#[test]
fn outbox_program_fold_is_pure() {
    let src = include_str!("../src/outbox.rs");
    let fold = block(src, "impl Program for Outbox");
    assert!(fold.len() > 10, "fold block is the real one");
    let bad = offenders(&fold);
    assert!(
        bad.is_empty(),
        "impure tokens in the outbox fold:\n{}",
        bad.join("\n")
    );
}

#[test]
fn delivery_fsm_is_pure() {
    let src = include_str!("../src/delivery.rs");
    let body: Vec<&str> = src
        .split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .collect();
    let bad = offenders(&body);
    assert!(
        bad.is_empty(),
        "impure tokens in the delivery FSM:\n{}",
        bad.join("\n")
    );
}

#[test]
fn wake_decision_is_pure() {
    let src = include_str!("../src/wake.rs");
    let body: Vec<&str> = src
        .split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .collect();
    assert!(body.len() > 100, "the wake module is the real one");
    let bad = offenders(&body);
    assert!(
        bad.is_empty(),
        "impure tokens in the wake decision:\n{}",
        bad.join("\n")
    );
}

#[test]
fn inbox_program_fold_is_pure() {
    let src = include_str!("../src/inbox_program.rs");
    let body: Vec<&str> = src
        .split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .collect();
    assert!(body.len() > 200, "the inbox program is the real one");
    let bad = offenders(&body);
    assert!(
        bad.is_empty(),
        "impure tokens in the inbox program:\n{}",
        bad.join("\n")
    );
}

#[test]
fn authority_table_is_pure() {
    let src = include_str!("../src/authority.rs");
    let body: Vec<&str> = src
        .split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .collect();
    assert!(body.len() > 150, "the authority module is the real one");
    let bad = offenders(&body);
    assert!(
        bad.is_empty(),
        "impure tokens in the authority table:\n{}",
        bad.join("\n")
    );
}

#[test]
fn topology_projection_is_pure() {
    let src = include_str!("../src/topology.rs");
    let body: Vec<&str> = src
        .split("#[cfg(test)]")
        .next()
        .unwrap_or("")
        .lines()
        .collect();
    assert!(body.len() > 80, "the topology module is the real one");
    let bad = offenders(&body);
    assert!(
        bad.is_empty(),
        "impure tokens in the topology projection:\n{}",
        bad.join("\n")
    );
    assert!(
        !body.iter().any(|l| l.contains("HashMap")),
        "projection is BTree, never HashMap"
    );
}

#[test]
fn the_purity_check_bites() {
    let src = include_str!("../src/outbox.rs");
    let run = block(src, "impl Interpret<Outbox> for OutboxRun");
    assert!(
        !offenders(&run).is_empty(),
        "the interpreter must trip the check"
    );
}
