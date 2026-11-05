// governed-by: ADR-0032
//!
//! Bounded reduce RED tests (bd-3pti).
//!
//! Action::Reduce (all/any/count/sum/join): proper-list resolution, bounded
//! fold, loud rejections, order preservation. All fail before the fix (at
//! runtime asserts — this file always compiles) and pass after.
//! Filter: `mbx test -p toron-workflow reduce_action`.

use toron_workflow::action_contract::{contract_for, contract_partition_agrees};
use toron_workflow::engine::{RunInput, WorkflowEngine};
use toron_workflow::schema::{Action, parse_workflow};

fn run_reduce_case(
    run_id: &str,
    steps_yaml: &str,
    trigger: serde_json::Value,
) -> serde_json::Value {
    let wf = parse_workflow(&format!(
        "name: reduce-case\ntrigger:\n  type: webhook\nsteps:\n{steps_yaml}"
    ))
    .expect("parse");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    rt.block_on(async {
        let engine = WorkflowEngine::local().expect("engine");
        engine
            .run(
                run_id,
                RunInput {
                    run_id: None,
                    workflow: wf,
                    trigger,
                },
            )
            .await
            .expect("run returns")
    })
}

fn reduce_step(over: &str, op: &str, extra: &str) -> String {
    format!("  - name: r\n    action: reduce\n    over: {over}\n    op: {op}\n{extra}")
}

/// all/any fold truthiness over booleans.
#[test]
fn reduce_action_all_any_truth_table() {
    let trigger = serde_json::json!({ "items": [true, true, false] });
    let out = run_reduce_case(
        "wf-reduce-all",
        &reduce_step("\"{{trigger.items}}\"", "all", ""),
        trigger.clone(),
    );
    assert_eq!(out["r"], false, "all with a false is false: {out}");
    let out = run_reduce_case(
        "wf-reduce-any",
        &reduce_step("\"{{trigger.items}}\"", "any", ""),
        trigger,
    );
    assert_eq!(out["r"], true, "any with a true is true: {out}");
}

/// count/sum fold with key selection and initial accumulators.
#[test]
fn reduce_action_count_sum_with_key_and_init() {
    let trigger = serde_json::json!({ "rows": [{"n": 2}, {"n": 3}] });
    let out = run_reduce_case(
        "wf-reduce-count",
        &reduce_step("\"{{trigger.rows}}\"", "count", "    key: n\n"),
        trigger.clone(),
    );
    assert_eq!(out["r"], 2, "count of truthy n: {out}");
    let out = run_reduce_case(
        "wf-reduce-sum",
        &reduce_step("\"{{trigger.rows}}\"", "sum", "    key: n\n    init: 10\n"),
        trigger,
    );
    assert_eq!(out["r"], 15, "sum n + init 10: {out}");
}

/// join renders the template per item in order with the separator.
#[test]
fn reduce_action_join_renders_in_order() {
    let trigger = serde_json::json!({ "items": [{"t": "a"}, {"t": "b"}, {"t": "c"}] });
    let out = run_reduce_case(
        "wf-reduce-join",
        &reduce_step(
            "\"{{trigger.items}}\"",
            "join",
            "    template: \"<{{item.t}}>\"\n    separator: \",\"\n",
        ),
        trigger,
    );
    assert_eq!(out["r"], "<a>,<b>,<c>", "ordered join: {out}");
}

/// Empty-list identities: all=true, any=false, count=0, sum=0(+init), join=init-or-"".
#[test]
fn reduce_action_empty_list_identities() {
    let trigger = serde_json::json!({ "items": [] });
    for (op, expected) in [
        ("all", serde_json::json!(true)),
        ("any", serde_json::json!(false)),
        ("count", serde_json::json!(0)),
        ("sum", serde_json::json!(0)),
        ("join", serde_json::json!("")),
    ] {
        let extra = if op == "join" {
            "    template: \"x{{item}}\"\n"
        } else {
            ""
        };
        let out = run_reduce_case(
            &format!("wf-reduce-empty-{op}"),
            &reduce_step("\"{{trigger.items}}\"", op, extra),
            trigger.clone(),
        );
        assert_eq!(out["r"], expected, "empty {op}: {out}");
    }
}

/// Non-list `over` fails loud — never a silent zero/empty fold.
#[test]
fn reduce_action_non_list_over_fails() {
    let trigger = serde_json::json!({ "items": "nope" });
    let out = run_reduce_case(
        "wf-reduce-nonlist",
        &reduce_step("\"{{trigger.items}}\"", "count", ""),
        trigger,
    );
    assert_eq!(out["r"]["ok"], false, "non-list fails: {out}");
    assert!(
        out["r"]["error"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("list"),
        "names the list contract: {out}"
    );
}

/// Unknown operations fail at parse — no fallback op.
#[test]
fn reduce_action_unknown_op_rejected_at_parse() {
    let yaml = "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: r\n    action: reduce\n    over: \"{{trigger.items}}\"\n    op: median\n";
    assert!(
        parse_workflow(yaml).is_err(),
        "unknown op median must fail parse"
    );
}

/// A missing key path on any item fails naming the key and index.
#[test]
fn reduce_action_missing_key_fails() {
    let trigger = serde_json::json!({ "rows": [{"n": 1}, {"m": 2}] });
    let out = run_reduce_case(
        "wf-reduce-missing-key",
        &reduce_step("\"{{trigger.rows}}\"", "sum", "    key: n\n"),
        trigger,
    );
    assert_eq!(out["r"]["ok"], false, "missing key fails: {out}");
    let err = out["r"]["error"].as_str().unwrap_or("").to_string();
    assert!(
        err.contains('n') && err.contains('1'),
        "names key + index: {out}"
    );
}

/// join without a template fails loud.
#[test]
fn reduce_action_join_without_template_fails() {
    let trigger = serde_json::json!({ "items": ["a"] });
    let out = run_reduce_case(
        "wf-reduce-join-notemplate",
        &reduce_step("\"{{trigger.items}}\"", "join", ""),
        trigger,
    );
    assert_eq!(out["r"]["ok"], false, "join w/o template fails: {out}");
    assert!(
        out["r"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("template"),
        "names template: {out}"
    );
}

/// Over-limit inputs fail loud instead of folding unboundedly.
#[test]
fn reduce_action_over_limit_fails() {
    let big: Vec<i64> = (0..1025).collect();
    let trigger = serde_json::json!({ "items": big });
    let out = run_reduce_case(
        "wf-reduce-overlimit",
        &reduce_step("\"{{trigger.items}}\"", "count", ""),
        trigger,
    );
    assert_eq!(out["r"]["ok"], false, "1025 items fail: {out}");
    assert!(
        out["r"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("1024"),
        "names the bound: {out}"
    );
}

/// Non-numeric sum operands fail with a type error, not coercion.
#[test]
fn reduce_action_sum_type_error() {
    let trigger = serde_json::json!({ "rows": [{"n": "x"}] });
    let out = run_reduce_case(
        "wf-reduce-sum-type",
        &reduce_step("\"{{trigger.rows}}\"", "sum", "    key: n\n"),
        trigger,
    );
    assert_eq!(out["r"]["ok"], false, "non-numeric sum fails: {out}");
}

/// A wrongly typed init fails loud.
#[test]
fn reduce_action_init_type_mismatch() {
    let trigger = serde_json::json!({ "rows": [{"n": 1}] });
    let out = run_reduce_case(
        "wf-reduce-init-type",
        &reduce_step(
            "\"{{trigger.rows}}\"",
            "sum",
            "    key: n\n    init: \"x\"\n",
        ),
        trigger,
    );
    assert_eq!(out["r"]["ok"], false, "string init for sum fails: {out}");
}

/// Reduce runs as a graph node (classification wired through run_step).
#[test]
fn reduce_action_runs_as_graph_node() {
    let wf = parse_workflow(
        "name: g\ntrigger:\n  type: webhook\ngraph:\n  nodes:\n    a: { action: send_dm, to: x, body: y }\n    r: { action: reduce, over: \"[1, 2, 3]\", op: count, needs: [a] }\n",
    )
    .expect("parse");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let out = rt.block_on(async {
        let engine = WorkflowEngine::local().expect("engine");
        engine
            .run(
                "wf-reduce-graph",
                RunInput {
                    run_id: None,
                    workflow: wf,
                    trigger: serde_json::json!({}),
                },
            )
            .await
            .expect("run returns")
    });
    assert_eq!(out["r"], 3, "graph node folds: {out}");
}

/// The contract entry agrees with the partition and the vocabulary.
#[test]
fn reduce_action_contract_entry_agrees() {
    let action = Action::Reduce {
        over: "{{trigger.items}}".into(),
        op: toron_workflow::schema::ReduceOp::Count,
        key: None,
        template: None,
        separator: None,
        init: None,
    };
    let contract = contract_for(&action);
    assert_eq!(contract.type_name, "reduce");
    assert!(contract.supported);
    assert!(contract_partition_agrees(&action));
    assert!(
        advertised_contains_reduce(),
        "reduce is advertised exactly once"
    );
}

fn advertised_contains_reduce() -> bool {
    let mut names = toron_workflow::engine::advertised_action_names();
    names.sort_unstable();
    names.iter().filter(|n| **n == "reduce").count() == 1
}

/// Reduce nests inside foreach (nested path threads workflow/node context).
#[test]
fn reduce_action_nests_in_foreach() {
    let trigger = serde_json::json!({ "groups": [[1, 2], [3]] });
    let out = run_reduce_case(
        "wf-reduce-nested",
        "  - name: r\n    action: foreach\n    over: \"{{trigger.groups}}\"\n    each:\n      action: reduce\n      over: \"{{item}}\"\n      op: sum\n",
        trigger,
    );
    assert_eq!(out["r"], serde_json::json!([3, 3]), "nested folds: {out}");
}

/// Truthiness edges: 0/""/null/false/[] are falsy; 1/"x"/true/[1] truthy.
#[test]
fn reduce_action_truthiness_edges() {
    let trigger = serde_json::json!({
        "items": [0, "", null, false, [], 1, "x", true, [1]]
    });
    let out = run_reduce_case(
        "wf-reduce-truth",
        &reduce_step("\"{{trigger.items}}\"", "count", ""),
        trigger.clone(),
    );
    assert_eq!(out["r"], 4, "four truthy items: {out}");
    let out = run_reduce_case(
        "wf-reduce-truth-all",
        &reduce_step("\"{{trigger.items}}\"", "all", ""),
        trigger,
    );
    assert_eq!(out["r"], false, "all with falsy is false: {out}");
}
