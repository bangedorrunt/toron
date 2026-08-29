// governed-by: ADR-0024
// governed-by: ADR-0019
// governed-by: ADR-0022
#![forbid(unsafe_code)]

//! ADR-0019 wave-2 structural playbook lints (bd-w2-lints-6k6l).
//!
//! The structural half of the resequenced wave 2: these rules land WITH the
//! YAML they constrain and apply to any workflow (builtin or user). They are
//! static — no evaluation — and fail-stop at `Error`.
//!
//! Rules (ADR-0019 Spec 6 mechanisms + Spec 7.2 playbook shapes):
//! - `feature`-shaped workflows require a `data_shape` input (declared in
//!   `inputs:` or referenced as `{{inputs.data_shape}}`); missing -> Error.
//! - `bug-fix`-shaped workflows require a `repro` predecessor (transitive
//!   `needs:` closure in a graph, or positional order in linear steps) before
//!   any `fix` step; missing -> Error.
//! - `prototype` / `arena` require `n >= 2` (a statically-declared default of
//!   1 -> Error) plus mode rules: `compile` arms must declare build-slot
//!   serialization (`build_slot: true` on the arms step); `sketch` arms must
//!   not reference repo build paths.
//! - `forensics`-shaped workflows contain NO fix/mutating step (readonly
//!   assertion — a fix is a separate `bug-fix` after the captain accepts the
//!   diagnosis).
//! - `principles:` names validate against the canonical 21-name registry
//!   (ADR-0019 Spec 5). The embedding passes the list in (flywheel passes
//!   `PRINCIPLE_NAMES` — no reverse dep); [`DEFAULT_PRINCIPLE_NAMES`] is the
//!   in-crate fixture fallback until the passed-in list is wired at every
//!   call site.

use crate::lint::{Level, LintIssue, LintReport};
use crate::schema::{Action, Graph, Step, Workflow};

/// The 21 canonical principle names (ADR-0019 Spec 5, pstack-derived). This
/// is the in-crate fixture fallback — byte-identical to flywheel's
/// `PRINCIPLE_NAMES` (bd-w2-name-registry-8d6u); the embedding passes its own
/// list in via [`lint_workflow_with_principles`] to keep the dep direction
/// clean. Kept in sync by `test_default_registry_matches_spec5`.
pub const DEFAULT_PRINCIPLE_NAMES: [&str; 21] = [
    // Core
    "laziness-protocol",
    "foundational-thinking",
    "redesign-from-first-principles",
    "subtract-before-you-add",
    "minimize-reader-load",
    "outcome-oriented-execution",
    "experience-first",
    "exhaust-the-design-space",
    "build-the-lever",
    // Architecture
    "model-the-domain",
    "boundary-discipline",
    "type-system-discipline",
    "make-operations-idempotent",
    "migrate-callers-then-delete-legacy-apis",
    "separate-before-serializing-shared-state",
    // Verification
    "prove-it-works",
    "fix-root-causes",
    "sequence-verifiable-units",
    // Delegation
    "guard-the-context-window",
    "never-block-on-the-human",
    // Meta
    "encode-lessons-in-structure",
];

/// The canonical playbook shapes the structural rules constrain (ADR-0019
/// Spec 7.2 names — the builtins are compiled in under exactly these names).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybookShape {
    Feature,
    BugFix,
    Prototype,
    Arena,
    Forensics,
}

/// Detect the playbook shape from the workflow name (canonical playbook
/// names only; a non-playbook workflow is not structurally constrained).
fn shape_of(name: &str) -> Option<PlaybookShape> {
    match name {
        "feature" => Some(PlaybookShape::Feature),
        "bug-fix" => Some(PlaybookShape::BugFix),
        "prototype" => Some(PlaybookShape::Prototype),
        "arena" => Some(PlaybookShape::Arena),
        "forensics" => Some(PlaybookShape::Forensics),
        _ => None,
    }
}

/// Entrypoint: run the ADR-0019 wave-2 structural playbook lints against a
/// parsed workflow, appending issues to `report`. `principle_names` is the
/// canonical-name registry supplied by the embedding.
pub fn lint_playbook(report: &mut LintReport, wf: &Workflow, principle_names: &[&str]) {
    lint_principles(report, wf, principle_names);
    lint_phase_rules(report, wf);

    let Some(shape) = shape_of(&wf.name) else {
        return; // Non-playbook workflows carry no structural constraint.
    };
    match shape {
        PlaybookShape::Feature => lint_feature(report, wf),
        PlaybookShape::BugFix => lint_bug_fix(report, wf),
        PlaybookShape::Prototype | PlaybookShape::Arena => lint_prototype_arena(report, wf),
        PlaybookShape::Forensics => lint_forensics(report, wf),
    }
}

// ---------------------------------------------------------------------------
// ADR-0022 §3.5 phase gates (R-P1..R-P4)
// ---------------------------------------------------------------------------

/// ADR-0022 §3.5 Layer 5 phase gates. Phase-scoped, NOT playbook-shape
/// scoped: they fire on any workflow whose `dispatch_job` specs carry a
/// `phase` (built-ins included). Specs without a phase stay unconstrained.
fn lint_phase_rules(report: &mut LintReport, wf: &Workflow) {
    let has_data_shape_input = wf.inputs.iter().any(|i| i.name == "data_shape");
    for step in &wf.steps {
        for s in step_or_nested(step) {
            lint_phase_step(report, s, has_data_shape_input);
        }
    }
}

/// Spec keys claimed by the ADR-0022 phase vocabulary, staged in `extra`
/// until their graduation wave (§3.1: keys graduate via a schema-rs change;
/// §3.3 already dispatches `prompt:` through the headless `extra` fallback).
/// R-P1 does not warn on these — the phase gates consume them.
const PHASE_CARRIER_KEYS: [&str; 3] = ["prompt", "acceptance", "n"];

fn lint_phase_step(report: &mut LintReport, step: &Step, has_data_shape_input: bool) {
    let Action::DispatchJob { assignee, spec, .. } = &step.action else {
        return;
    };

    // R-P1 — the graduation valve stays loud: unknown extra fields warn,
    // naming the field (ADR-0022 §3.1: extra is a staging area, not a shim).
    for key in spec.extra.keys() {
        if PHASE_CARRIER_KEYS.contains(&key.as_str()) {
            continue;
        }
        warning(
            report,
            format!(
                "dispatch_job spec has unknown extra field {key:?} — graduate it to a named \
                 DispatchSpec field via a schema change or remove it (ADR-0022 §3.1/§3.5 R-P1)"
            ),
        );
    }

    let Some(phase) = spec.phase.as_deref() else {
        return; // Unphased dispatches are not gated.
    };

    match phase {
        "implement" => {
            // R-P2 — implement carries its acceptance command (extends the
            // bug-fix repro rule family: evidence over claims).
            let has_acceptance = spec
                .extra
                .get("acceptance")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|v| !v.trim().is_empty());
            if !has_acceptance {
                error(
                    report,
                    "phase: implement dispatch must carry an acceptance command — declare \
                     `acceptance: <gate command>` on the spec; the bead's gate exit 0 is the \
                     completion evidence (ADR-0022 §4/§3.5 R-P2)"
                        .to_string(),
                );
            }
        }
        "review" => {
            // R-P3 — review fan-outs declare n >= 2 statically (extends the
            // prototype/arena n rule).
            match spec.extra.get("n").and_then(parse_u64) {
                Some(n) if n >= 2 => {}
                other => error(
                    report,
                    format!(
                        "phase: review fan-out must declare n >= 2 statically — got {}; put \
                         `n: <count>` on the spec (ADR-0022 §3.5 R-P3)",
                        other.map_or("nothing".to_string(), |n| n.to_string())
                    ),
                ),
            }
        }
        "plan" => {
            // R-P4 — plan declares inputs.data_shape and rides a planner role.
            let references_data_shape = spec
                .string_values()
                .iter()
                .any(|v| v.contains("{{inputs.data_shape}}"));
            if !has_data_shape_input && !references_data_shape {
                error(
                    report,
                    "phase: plan requires declared run input `data_shape` — add \
                     `inputs: [{name: data_shape}]` or reference {{inputs.data_shape}} in the \
                     spec (ADR-0022 §3.5 R-P4)"
                        .to_string(),
                );
            }
            let planner_role = assignee.contains("planner")
                || spec
                    .profile
                    .as_deref()
                    .is_some_and(|p| p.contains("planner"));
            if !planner_role {
                error(
                    report,
                    format!(
                        "phase: plan must dispatch to a planner-role profile — assignee \
                         {assignee:?} is not a planner role (ADR-0022 §3.5 R-P4)"
                    ),
                );
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// feature: data_shape input required
// ---------------------------------------------------------------------------

/// ADR-0019 Spec 6 `foundational-thinking`: the `feature` workflow requires a
/// `data_shape` input — declared in `inputs:` or provided at invoke
/// (`{{inputs.data_shape}}` referenced by a step). Missing -> Error.
fn lint_feature(report: &mut LintReport, wf: &Workflow) {
    let declared = wf.inputs.iter().any(|i| i.name == "data_shape");
    if declared {
        return;
    }
    let mut strings = Vec::new();
    for step in &wf.steps {
        collect_step_strings(step, &mut strings);
    }
    if let Some(graph) = &wf.graph {
        for node in graph.nodes.values() {
            collect_action_strings(&node.action, &mut strings);
        }
    }
    let provided = strings.iter().any(|s| s.contains("{{inputs.data_shape}}"));
    if !provided {
        error(
            report,
            "feature workflow requires a `data_shape` input — declare `inputs: [{name: data_shape}]` or reference `{{inputs.data_shape}}` (ADR-0019 Spec 6: foundational-thinking)",
        );
    }
}

// ---------------------------------------------------------------------------
// bug-fix: repro predecessor before any fix step
// ---------------------------------------------------------------------------

/// ADR-0019 Spec 6 `fix-root-causes`: a `bug-fix` workflow must reproduce
/// before it fixes. Graph form: every node named `fix` must reach a `repro`
/// node through its transitive `needs:` closure. Linear form: every step
/// named `fix` must follow a step named `repro`. A `fix` with no repro at all
/// -> Error (graph) / Error (linear).
fn lint_bug_fix(report: &mut LintReport, wf: &Workflow) {
    match &wf.graph {
        Some(graph) => lint_bug_fix_graph(report, graph),
        None => lint_bug_fix_steps(report, &wf.steps),
    }
}

fn lint_bug_fix_graph(report: &mut LintReport, graph: &Graph) {
    let nodes = &graph.nodes;
    let has_repro = nodes.contains_key("repro");
    if !has_repro {
        error(
            report,
            "bug-fix workflow has no `repro` node — a fix without a reproduction fails lint (ADR-0019 Spec 6: fix-root-causes)",
        );
        return;
    }
    // Transitive needs-closure per node (repro reachable through any chain).
    for (name, node) in nodes {
        if name != "fix" {
            continue;
        }
        let mut seen = std::collections::HashSet::new();
        let mut stack: Vec<&str> = node.needs.iter().map(String::as_str).collect();
        let mut reaches_repro = false;
        while let Some(need) = stack.pop() {
            if need == "repro" {
                reaches_repro = true;
                break;
            }
            if !seen.insert(need) {
                continue;
            }
            if let Some(dep) = nodes.get(need) {
                stack.extend(dep.needs.iter().map(String::as_str));
            }
        }
        if !reaches_repro {
            error(
                report,
                format!(
                    "bug-fix node `fix` has no `repro` predecessor — repro must be in fix's transitive `needs:` closure (e.g. repro -> tdd -> fix); got needs {:?}",
                    node.needs
                ),
            );
        }
    }
}

fn lint_bug_fix_steps(report: &mut LintReport, steps: &[Step]) {
    let repro_idx: Option<usize> = steps
        .iter()
        .position(|s| s.name.as_deref() == Some("repro"));
    if repro_idx.is_none() {
        error(
            report,
            "bug-fix workflow has no `repro` step — a fix without a reproduction fails lint (ADR-0019 Spec 6: fix-root-causes)",
        );
        return;
    }
    for (i, step) in steps.iter().enumerate() {
        if step.name.as_deref() == Some("fix") && i <= repro_idx.unwrap() {
            error(
                report,
                "bug-fix step `fix` must follow the `repro` step (reproduce first, then fix — ADR-0019 Spec 6: fix-root-causes)",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// prototype / arena: n >= 2 + mode rules
// ---------------------------------------------------------------------------

/// ADR-0019 Spec 6 `exhaust-the-design-space`: prototype/arena require n >= 2
/// competing arms (n=1 -> Error). Mode rules (Spec 7.2 as amended
/// 2026-08-15): compile-mode arms declare build-slot serialization statically
/// (`build_slot: true` on the arms step); sketch arms must not reference repo
/// build paths (text-only under /tmp/flywheel-proto-*, no crate copy).
fn lint_prototype_arena(report: &mut LintReport, wf: &Workflow) {
    let n_decl = wf.inputs.iter().find(|i| i.name == "n");
    let mode_decl = wf.inputs.iter().find(|i| i.name == "mode");

    if n_decl.is_none() {
        warning(
            report,
            "prototype/arena should declare input `n` (e.g. `inputs: [{name: n, default: 2}]`) so n >= 2 is statically checkable (ADR-0019 Spec 7.2)",
        );
    } else if let Some(default) = n_decl.and_then(|d| d.default.as_ref()) {
        match parse_u64(default) {
            Some(n) if n < 2 => {
                error(
                    report,
                    format!(
                        "prototype/arena require n >= 2 competing arms (exhaust-the-design-space); declared default n={n} is a single arm"
                    ),
                );
            }
            Some(_) => {}
            None => {
                error(
                    report,
                    format!(
                        "input `n` default {default:?} is not an integer — declare an integer default >= 2 (ADR-0019 Spec 7.2)"
                    ),
                );
            }
        }
    } else {
        warning(
            report,
            "input `n` has no static default — n >= 2 cannot be lint-checked; declare `default: 2` (ADR-0019 Spec 7.2)",
        );
    }

    let Some(mode_default) = mode_decl.and_then(|m| m.default.as_ref()) else {
        if mode_decl.is_none() {
            warning(
                report,
                "prototype/arena should declare input `mode` (`sketch` default | `compile`) so mode rules are statically checkable (ADR-0019 Spec 7.2)",
            );
        } else {
            warning(
                report,
                "input `mode` has no static default — mode rules not enforced; declare `default: sketch` or `default: compile` (ADR-0019 Spec 7.2)",
            );
        }
        return;
    };

    let mode = mode_default.as_str().unwrap_or_default();
    match mode {
        "compile" => lint_compile_mode(report, wf),
        "sketch" => lint_sketch_mode(report, wf),
        other => {
            error(
                report,
                format!(
                    "input `mode` default {other:?} is invalid — must be `sketch` (default) or `compile` (ADR-0019 Spec 7.2)"
                ),
            );
        }
    }
}

/// Compile-mode arms serialize on the build slot (the 2026-08-15 amendment:
/// compiling variants pay the serialize rule like everyone else). At least
/// one step must declare `build_slot: true` — the static contract marker the
/// embedding honours via the kind-30032 lease.
fn lint_compile_mode(report: &mut LintReport, wf: &Workflow) {
    let declares_slot_steps = wf
        .steps
        .iter()
        .any(|s| step_or_nested(s).any(|inner| inner.build_slot));
    let declares_slot_graph = wf
        .graph
        .as_ref()
        .is_some_and(|g| g.nodes.values().any(|n| n.build_slot));
    if !declares_slot_steps && !declares_slot_graph {
        error(
            report,
            "compile-mode prototype/arena arms must declare build-slot serialization — put `build_slot: true` on the arms step (one arm at a time under the build slot; ADR-0019 Spec 7.2 amendment 2026-08-15)",
        );
    }
}

/// Sketch-mode arms write text only under /tmp/flywheel-proto-* — they must
/// not reference repo build paths (no crate copy, no cargo, no .beads).
fn lint_sketch_mode(report: &mut LintReport, wf: &Workflow) {
    let mut strings = Vec::new();
    for step in &wf.steps {
        collect_sketch_step(step, &mut strings);
    }
    if let Some(graph) = &wf.graph {
        for node in graph.nodes.values() {
            // A node that itself opts into build-slot serialization is a
            // compile-mode subtree — build paths are legitimate there.
            if node.build_slot {
                continue;
            }
            collect_sketch_action(&node.action, &mut strings);
        }
    }
    for s in &strings {
        // Allow the explicit ban phrase "no .beads" — the task documents that
        // sketches must NOT write to .beads, not that they do. The lint should
        // not flag the ban itself as a violation (the test expects the ban is
        // present, the lint expects no repo path is referenced).
        if s.to_lowercase().contains("no .beads") {
            continue;
        }
        if let Some(tok) = REPO_BUILD_PATH_TOKENS.iter().find(|t| s.contains(**t)) {
            error(
                report,
                format!(
                    "sketch-mode prototype/arena arms must not reference repo build paths — found {tok:?} in a step; sketches write text only under /tmp/flywheel-proto-* (ADR-0019 Spec 7.2 amendment 2026-08-15)"
                ),
            );
            return;
        }
    }
}

/// Compile-mode subtree marker: the step opts into build-slot serialization
/// or its guard selects the compile mode. Build paths are legitimate there;
/// the text-only sketch contract applies to the other arms (ADR-0019 7.2).
fn sketch_exempt(guard: Option<&String>, build_slot: bool) -> bool {
    build_slot || guard.is_some_and(|g| g.contains("compile"))
}

fn collect_sketch_step(step: &Step, out: &mut Vec<String>) {
    if sketch_exempt(step.guard.as_ref(), step.build_slot) {
        return;
    }
    if let Some(name) = &step.name {
        out.push(name.clone());
    }
    if let Some(guard) = &step.guard {
        out.push(guard.clone());
    }
    collect_sketch_action(&step.action, out);
}

/// Mirror of `collect_action_strings` that walks compile-selecting branches
/// mode-aware: the `then` side of a compile branch is exempt, the `else`
/// side (and unguarded branches) are scanned as sketch material.
fn collect_sketch_action(action: &Action, out: &mut Vec<String>) {
    match action {
        Action::Branch {
            if_guard,
            then,
            else_steps,
        } => {
            let compile_selected = if_guard.contains("compile");
            if !compile_selected {
                for s in then {
                    collect_sketch_step(s, out);
                }
            }
            for s in else_steps {
                collect_sketch_step(s, out);
            }
        }
        Action::Retry { step, .. } => collect_sketch_step(step, out),
        Action::Foreach { over, each, .. } => {
            out.push(over.clone());
            collect_sketch_step(each, out);
        }
        Action::Parallel { over, each, .. } => {
            out.push(over.clone());
            collect_sketch_step(each, out);
        }
        other => collect_action_strings(other, out),
    }
}

/// Tokens that mark a repo build path / build action (sketch arms must never
/// touch these).
const REPO_BUILD_PATH_TOKENS: [&str; 9] = [
    "Cargo.toml",
    "Cargo.lock",
    "target/",
    ".beads",
    "cargo build",
    "cargo test",
    "cargo run",
    "cargo check",
    "git checkout",
];

// ---------------------------------------------------------------------------
// forensics: NO fix/mutating step
// ---------------------------------------------------------------------------

/// ADR-0019 Spec 7.2 `forensics`: diagnosis is the artifact — NO fix step, no
/// mutating step. A fix is a separate `bug-fix` after the captain accepts the
/// diagnosis ("I said repro, I did not ask for a fix").
fn lint_forensics(report: &mut LintReport, wf: &Workflow) {
    let mut strings = Vec::new();
    let mut walk = |s: &Step| {
        if let Some(name) = &s.name
            && FIX_STEP_NAMES.contains(&name.as_str())
        {
            error(
                report,
                format!(
                    "forensics workflow must contain NO fix/mutating step — step {name:?} is a fix; a fix is a separate bug-fix after the captain accepts the diagnosis (ADR-0019 Spec 7.2)"
                ),
            );
        }
        collect_step_strings(s, &mut strings);
    };
    for step in &wf.steps {
        walk(step);
    }
    if let Some(graph) = &wf.graph {
        for (name, node) in &graph.nodes {
            if FIX_STEP_NAMES.contains(&name.as_str()) {
                error(
                    report,
                    format!(
                        "forensics workflow must contain NO fix/mutating step — node {name:?} is a fix; a fix is a separate bug-fix after the captain accepts the diagnosis (ADR-0019 Spec 7.2)"
                    ),
                );
            }
            collect_action_strings(&node.action, &mut strings);
        }
    }
    for s in &strings {
        if contains_fix_verb(s) {
            error(
                report,
                "forensics workflow must contain NO fix/mutating step — step text requests a fix/patch/implement/repair; a fix is a separate bug-fix after the captain accepts the diagnosis (ADR-0019 Spec 7.2)",
            );
            return;
        }
    }
}

/// Step/node names that mark a fix-bearing step (forensics ban).
const FIX_STEP_NAMES: [&str; 10] = [
    "fix",
    "patch",
    "implement",
    "repair",
    "resolve",
    "change",
    "edit",
    "modify",
    "apply",
    "update",
];

/// True when `text` contains a fix verb as a word, not negated within the
/// three preceding tokens ("do not fix" / "no fix step" pass).
fn contains_fix_verb(text: &str) -> bool {
    const VERBS: [&str; 8] = [
        "fix",
        "patch",
        "implement",
        "repair",
        "resolve",
        "amend",
        "modify",
        "edit",
    ];
    const NEGATIONS: [&str; 7] = ["no", "not", "never", "without", "don't", "dont", "avoid"];
    let tokens: Vec<&str> = text
        .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
        .filter(|t| !t.is_empty())
        .collect();
    for (i, tok) in tokens.iter().enumerate() {
        if !VERBS.contains(tok) {
            continue;
        }
        let negated = tokens[..i]
            .iter()
            .rev()
            .take(3)
            .any(|p| NEGATIONS.contains(p));
        if !negated {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// principles: names validate against the registry
// ---------------------------------------------------------------------------

/// ADR-0019 wave-2: `principles:` names must be in the canonical 21-name
/// registry (Spec 5). Unknown -> Error; duplicate -> Warning.
fn lint_principles(report: &mut LintReport, wf: &Workflow, principle_names: &[&str]) {
    if wf.principles.is_empty() {
        return;
    }
    let mut seen = std::collections::HashSet::new();
    for name in &wf.principles {
        if !principle_names.contains(&name.as_str()) {
            error(
                report,
                format!(
                    "unknown principle name {name:?} — not in the canonical 21-name registry (ADR-0019 Spec 5; w2-name-registry); known: {}",
                    principle_names.join(", ")
                ),
            );
        } else if !seen.insert(name.as_str()) {
            warning(
                report,
                format!("duplicate principle name {name:?} in `principles:`"),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Parse an input default as an integer (accepts a YAML number or a numeric
/// string).
fn parse_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Collect every string field of a step (recursing into retry/branch/foreach/
/// parallel bodies) — the union of text a step can carry.
fn collect_step_strings(step: &Step, out: &mut Vec<String>) {
    if let Some(name) = &step.name {
        out.push(name.clone());
    }
    if let Some(guard) = &step.guard {
        out.push(guard.clone());
    }
    collect_action_strings(&step.action, out);
}

fn collect_action_strings(action: &Action, out: &mut Vec<String>) {
    match action {
        Action::SendMessage {
            to, subject, body, ..
        } => {
            out.extend(to.iter().cloned());
            out.push(subject.clone());
            out.push(body.clone());
        }
        Action::SendDm { to, body, .. } => {
            out.push(to.clone());
            out.push(body.clone());
        }
        Action::React { message_id, .. } => out.push(message_id.clone()),
        Action::DispatchJob { assignee, spec, .. } => {
            out.push(assignee.clone());
            out.extend(spec.string_values());
        }
        Action::CallWebhook {
            url, method, body, ..
        } => {
            out.push(url.clone());
            if let Some(m) = method {
                out.push(m.clone());
            }
            if let Some(b) = body {
                out.push(b.clone());
            }
        }
        Action::RequestApproval { message } => out.push(message.clone()),
        Action::Delay { .. } => {}
        Action::Retry { step, .. } => collect_step_strings(step, out),
        Action::Branch {
            then, else_steps, ..
        } => {
            for s in then {
                collect_step_strings(s, out);
            }
            for s in else_steps {
                collect_step_strings(s, out);
            }
        }
        Action::Foreach { over, each } => {
            out.push(over.clone());
            collect_step_strings(each, out);
        }
        Action::Parallel { over, each } => {
            out.push(over.clone());
            collect_step_strings(each, out);
        }
        Action::CallWorkflow { workflow, inputs } => {
            out.push(workflow.clone());
            out.push(inputs.to_string());
        }
        Action::Spawn {
            profile, workflow, ..
        } => {
            out.push(profile.clone());
            if let Some(w) = workflow {
                out.push(w.clone());
            }
        }
        Action::Verify { command } => out.push(command.clone()),
    }
}

/// Flatten a step and its nested bodies into an iterator (build_slot scan).
fn step_or_nested(step: &Step) -> Box<dyn Iterator<Item = &Step> + '_> {
    let mut v = vec![step];
    match &step.action {
        Action::Retry { step: inner, .. } => v.push(inner),
        Action::Branch {
            then, else_steps, ..
        } => {
            v.extend(then.iter());
            v.extend(else_steps.iter());
        }
        Action::Foreach { each, .. } | Action::Parallel { each, .. } => v.push(each),
        _ => {}
    }
    Box::new(v.into_iter())
}

fn error(report: &mut LintReport, message: impl Into<String>) {
    report.issues.push(LintIssue {
        level: Level::Error,
        message: message.into(),
        line: None,
    });
}

fn warning(report: &mut LintReport, message: impl Into<String>) {
    report.issues.push(LintIssue {
        level: Level::Warning,
        message: message.into(),
        line: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lint::lint_yaml;

    /// The registry must be exactly the 21 canonical names from ADR-0019
    /// Spec 5 (bd-w2-name-registry-8d6u contract — byte-identical to
    /// flywheel's `PRINCIPLE_NAMES`).
    #[test]
    fn default_registry_has_exactly_21_canonical_names() {
        let expected: [&str; 21] = [
            // Core
            "laziness-protocol",
            "foundational-thinking",
            "redesign-from-first-principles",
            "subtract-before-you-add",
            "minimize-reader-load",
            "outcome-oriented-execution",
            "experience-first",
            "exhaust-the-design-space",
            "build-the-lever",
            // Architecture
            "model-the-domain",
            "boundary-discipline",
            "type-system-discipline",
            "make-operations-idempotent",
            "migrate-callers-then-delete-legacy-apis",
            "separate-before-serializing-shared-state",
            // Verification
            "prove-it-works",
            "fix-root-causes",
            "sequence-verifiable-units",
            // Delegation
            "guard-the-context-window",
            "never-block-on-the-human",
            // Meta
            "encode-lessons-in-structure",
        ];
        assert_eq!(DEFAULT_PRINCIPLE_NAMES.len(), 21);
        assert_eq!(DEFAULT_PRINCIPLE_NAMES.as_slice(), expected);
        // The plausible fake from the registry bead's acceptance must fail.
        assert!(!DEFAULT_PRINCIPLE_NAMES.contains(&"prove-it-work"));
        // Each canonical name is known.
        for n in expected {
            assert!(DEFAULT_PRINCIPLE_NAMES.contains(&n));
        }
    }

    #[test]
    fn shape_detection_matches_canonical_playbook_names() {
        assert_eq!(shape_of("feature"), Some(PlaybookShape::Feature));
        assert_eq!(shape_of("bug-fix"), Some(PlaybookShape::BugFix));
        assert_eq!(shape_of("prototype"), Some(PlaybookShape::Prototype));
        assert_eq!(shape_of("arena"), Some(PlaybookShape::Arena));
        assert_eq!(shape_of("forensics"), Some(PlaybookShape::Forensics));
        // Non-playbook workflows are not constrained.
        assert_eq!(shape_of("goal"), None);
        assert_eq!(shape_of("perf"), None);
        assert_eq!(shape_of(""), None);
    }

    #[test]
    fn fix_verb_detection_respects_negation() {
        assert!(contains_fix_verb("apply the fix to the caller"));
        assert!(contains_fix_verb("implement the change"));
        assert!(!contains_fix_verb("do not fix anything, just diagnose"));
        assert!(!contains_fix_verb("no fix step; diagnosis only"));
        assert!(!contains_fix_verb(
            "instrumentation or artifact read; deliverable is a diagnosis"
        ));
    }

    #[test]
    fn parse_u64_accepts_number_and_numeric_string() {
        assert_eq!(parse_u64(&serde_json::json!(2)), Some(2));
        assert_eq!(parse_u64(&serde_json::json!("3")), Some(3));
        assert_eq!(parse_u64(&serde_json::json!("sketch")), None);
        assert_eq!(parse_u64(&serde_json::json!(null)), None);
    }

    /// Non-playbook workflows are untouched by the structural rules.
    #[test]
    fn non_playbook_workflow_not_constrained() {
        let yaml = r#"
name: goal
description: meta workflow
steps:
  - name: decompose
    action: dispatch_job
    assignee: x
    spec: { profile: "@planner", task: "plan: {{inputs.goal}}" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report.is_clean(),
            "goal must not be constrained: {:?}",
            report.issues
        );
    }

    // -------------------------------------------------------------------
    // ADR-0022 §3.5 phase gates R-P1..R-P4 (bd-adr22-phase-enum-lint-ahfh)
    // -------------------------------------------------------------------

    /// R-P1: unknown extra fields on a dispatch_job spec warn, naming the
    /// field — the graduation valve must stay loud (ADR-0022 §3.1/§3.5).
    #[test]
    fn rp1_unknown_spec_extra_field_warns_naming_it() {
        let yaml = r#"
name: ship-chain
steps:
  - name: build
    action: dispatch_job
    assignee: "@crew.coder"
    spec: { phase: implement, prompt: "build", acceptance: "cargo test", frobnicate: true }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Warning && i.message.contains("frobnicate")),
            "expected a warning naming `frobnicate`: {:?}",
            report.issues
        );
    }

    #[test]
    fn rp1_known_fields_and_phase_carriers_do_not_warn() {
        let yaml = r#"
name: ship-chain
steps:
  - name: build
    action: dispatch_job
    assignee: "@crew.coder"
    spec:
      phase: implement
      prompt: "build {{inputs.data_shape}}"
      acceptance: "cargo test -p toron-workflow"
      harness: opencode
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            !report
                .issues
                .iter()
                .any(|i| i.message.contains("extra field")),
            "known + carrier fields must not warn: {:?}",
            report.issues
        );
    }

    /// R-P2: phase=implement dispatches carry an acceptance command.
    #[test]
    fn rp2_implement_without_acceptance_is_error() {
        let yaml = r#"
name: ship-chain
steps:
  - name: build
    action: dispatch_job
    assignee: "@crew.coder"
    spec: { phase: implement, prompt: "build" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("acceptance")),
            "implement without acceptance must error: {:?}",
            report.issues
        );
    }

    #[test]
    fn rp2_implement_with_acceptance_is_clean() {
        let yaml = r#"
name: ship-chain
steps:
  - name: build
    action: dispatch_job
    assignee: "@crew.coder"
    spec: { phase: implement, prompt: "build", acceptance: "cargo test" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            !report
                .issues
                .iter()
                .any(|i| i.message.contains("acceptance")),
            "implement with acceptance must not flag acceptance: {:?}",
            report.issues
        );
    }

    /// R-P3: review fan-outs declare n >= 2 statically (arena rule family).
    #[test]
    fn rp3_review_requires_declared_n_ge_2() {
        for spec in [
            "{ phase: review, prompt: \"r\" }",
            "{ phase: review, prompt: \"r\", n: 1 }",
        ] {
            let yaml = format!(
                r#"
name: ship-chain
steps:
  - name: review
    action: dispatch_job
    assignee: "@crew.reviewer"
    spec: {spec}
"#
            );
            let report = lint_yaml(&yaml).unwrap();
            assert!(
                report
                    .issues
                    .iter()
                    .any(|i| i.level == Level::Error && i.message.contains("n >= 2")),
                "review spec {spec} must error on n >= 2: {:?}",
                report.issues
            );
        }
    }

    #[test]
    fn rp3_review_with_n_2_is_clean() {
        let yaml = r#"
name: ship-chain
steps:
  - name: review
    action: dispatch_job
    assignee: "@crew.reviewer"
    spec: { phase: review, prompt: "review", n: 2 }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            !report.issues.iter().any(|i| i.message.contains("n >= 2")),
            "review with n=2 must not flag fan-out: {:?}",
            report.issues
        );
    }

    /// R-P4: plan phases declare inputs.data_shape and ride a planner role.
    #[test]
    fn rp4_plan_requires_data_shape_input() {
        let yaml = r#"
name: ship-chain
inputs: [{name: goal}]
steps:
  - name: plan
    action: dispatch_job
    assignee: "@crew.planner"
    spec: { phase: plan, prompt: "{{inputs.goal}}" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("data_shape")),
            "plan without data_shape input must error: {:?}",
            report.issues
        );
    }

    #[test]
    fn rp4_plan_requires_planner_role_dispatch() {
        let yaml = r#"
name: ship-chain
inputs: [{name: data_shape}]
steps:
  - name: plan
    action: dispatch_job
    assignee: "@crew.coder"
    spec: { phase: plan, prompt: "{{inputs.data_shape}}" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report
                .issues
                .iter()
                .any(|i| i.level == Level::Error && i.message.contains("planner")),
            "plan dispatched to a non-planner role must error: {:?}",
            report.issues
        );
    }

    #[test]
    fn rp4_plan_fully_conformed_is_clean() {
        let yaml = r#"
name: ship-chain
inputs: [{name: data_shape}]
steps:
  - name: plan
    action: dispatch_job
    assignee: "@crew.planner"
    spec: { phase: plan, prompt: "decompose {{inputs.data_shape}}" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report.is_clean(),
            "conformed plan step must be clean: {:?}",
            report.issues
        );
    }

    /// Phase gates are phase-scoped, not playbook-shape-scoped: they fire on
    /// any workflow whose dispatches carry `phase` (built-ins included).
    #[test]
    fn phase_rules_fire_outside_canonical_playbook_shapes() {
        let yaml = r#"
name: totally-custom-pipeline
steps:
  - name: rev
    action: dispatch_job
    assignee: "@crew.reviewer"
    spec: { phase: review, prompt: "review" }
"#;
        let report = lint_yaml(yaml).unwrap();
        assert!(
            report.issues.iter().any(|i| i.level == Level::Error),
            "phase gate must fire on non-playbook shapes: {:?}",
            report.issues
        );
    }

    /// The vocabulary is closed (ADR-0022 §4): an unknown phase value is a
    /// parse error surfaced at lint time, never a silent degrade.
    #[test]
    fn unknown_phase_value_fails_parse_at_lint_time() {
        let yaml = r#"
name: ship-chain
steps:
  - name: s
    action: dispatch_job
    assignee: x
    spec: { phase: shipp, prompt: "x" }
"#;
        let err = lint_yaml(yaml).expect_err("unknown phase must fail parse");
        assert!(err.contains("unknown variant"), "{err}");
    }

    /// Specs WITHOUT a phase stay unconstrained by the phase gates.
    #[test]
    fn unphased_dispatch_is_not_gated() {
        let yaml = r#"
name: totally-custom-pipeline
steps:
  - name: d
    action: dispatch_job
    assignee: someone
    spec: { task: "ad hoc", mystery_knob: 3 }
"#;
        let report = lint_yaml(yaml).unwrap();
        // R-P1 still applies (extra-field warning), but no phase errors.
        assert!(
            !report.issues.iter().any(|i| i.level == Level::Error),
            "unphased specs get no phase errors: {:?}",
            report.issues
        );
    }
}
