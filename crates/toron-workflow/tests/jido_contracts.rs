// governed-by: ADR-0032
//!
//! ADR-0032 Spec 2 + Spec-3 identity RED tests (bd-htoo).
//!
//! Every test names a contract the bead requires; all fail before the fix
//! (at runtime asserts — this file always compiles) and pass after.
//! Filter: `mbx test -p toron-workflow jido_contracts`.

use toron_workflow::action_contract::{
    ActionClass, EffectKind, OutputKind, check_output, contract_for, contract_partition_agrees,
    validate_action,
};
use toron_workflow::engine::advertised_action_names;
use toron_workflow::identity::{
    PreparedIdentity, SCHEMA_VERSION, action_digest_of, canonical_json, check_replay, effect_key,
    iteration_key, node_path, normalize_action, redact_value, sha256_hex, workflow_digest,
};
use toron_workflow::schema::{Action, parse_workflow};

fn parse_action(yaml_body: &str) -> Action {
    let yaml = format!("name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: n\n{yaml_body}");
    let wf = parse_workflow(&yaml).expect("test workflow parses");
    wf.steps[0].action.clone()
}

fn all_variant_actions() -> Vec<Action> {
    vec![
        parse_action("    action: send_message\n    to: [a]\n    subject: s\n    body: b\n"),
        parse_action("    action: send_dm\n    to: a\n    body: b\n"),
        parse_action("    action: react\n    message_id: m\n    emoji: \"+1\"\n"),
        parse_action("    action: dispatch_job\n    assignee: a\n    spec: { task: t }\n"),
        parse_action("    action: call_webhook\n    url: https://x.example\n"),
        parse_action("    action: request_approval\n    message: q\n"),
        parse_action("    action: delay\n    secs: 5\n"),
        parse_action(
            "    action: retry\n    max_attempts: 2\n    step:\n      action: delay\n      secs: 1\n",
        ),
        parse_action(
            "    action: branch\n    if: \"1 == 1\"\n    then:\n      - action: delay\n        secs: 1\n",
        ),
        parse_action(
            "    action: foreach\n    over: \"{{trigger.items}}\"\n    each:\n      action: delay\n      secs: 1\n",
        ),
        parse_action(
            "    action: parallel\n    over: \"2\"\n    each:\n      action: delay\n      secs: 1\n",
        ),
        parse_action("    action: call_workflow\n    workflow: kid\n    inputs: {}\n"),
        parse_action("    action: spawn\n    profile: \"@housekeeper\"\n    await: true\n"),
        parse_action("    action: verify\n    command: \"mbx test\"\n"),
    ]
}

/// The contract table covers every advertised name exactly once — adding a
/// variant without a contract entry fails here.
#[test]
fn contract_covers_every_advertised_name() {
    let mut advertised = advertised_action_names();
    advertised.sort_unstable();
    advertised.dedup();
    let mut covered: Vec<&str> = all_variant_actions()
        .iter()
        .map(|a| contract_for(a).type_name)
        .collect();
    covered.sort_unstable();
    covered.dedup();
    assert_eq!(
        covered, advertised,
        "contract table == advertised vocabulary"
    );
    assert_eq!(covered.len(), 14, "closed set has 14 variants");
}

/// The contract table never disagrees with the engine partition: same sink,
/// same support — the table cannot become a stale copy.
#[test]
fn contract_partition_agrees_for_all_variants() {
    for action in all_variant_actions() {
        assert!(
            contract_partition_agrees(&action),
            "contract/partition agree on {:?}",
            action.type_name()
        );
    }
}

/// Structural validation portraits: each bad action fails with a named
/// field; each good one validates clean.
#[test]
fn validate_action_portraits() {
    let bad: Vec<(&str, Action)> = vec![
        (
            "to",
            parse_action("    action: send_dm\n    to: \"\"\n    body: b\n"),
        ),
        (
            "to",
            parse_action("    action: send_message\n    to: []\n    subject: s\n    body: b\n"),
        ),
        (
            "assignee",
            parse_action("    action: dispatch_job\n    assignee: \"\"\n    spec: { task: t }\n"),
        ),
        (
            "max_attempts",
            parse_action(
                "    action: retry\n    max_attempts: 0\n    step:\n      action: delay\n      secs: 1\n",
            ),
        ),
        (
            "step",
            parse_action(
                "    action: retry\n    max_attempts: 2\n    step:\n      action: retry\n      max_attempts: 2\n      step:\n        action: delay\n        secs: 1\n",
            ),
        ),
        (
            "workflow",
            parse_action("    action: call_workflow\n    workflow: \"\"\n    inputs: {}\n"),
        ),
        (
            "profile",
            parse_action("    action: spawn\n    profile: \"\"\n    await: true\n"),
        ),
        (
            "command",
            parse_action("    action: verify\n    command: \"\"\n"),
        ),
        (
            "over",
            parse_action(
                "    action: foreach\n    over: \"\"\n    each:\n      action: delay\n      secs: 1\n",
            ),
        ),
        (
            "action",
            parse_action("    action: react\n    message_id: m\n    emoji: \"+1\"\n"),
        ),
        (
            "action",
            parse_action("    action: call_webhook\n    url: https://x.example\n"),
        ),
    ];
    for (field, action) in bad {
        let violations = validate_action(&action);
        assert!(
            violations.iter().any(|v| v.field == field),
            "{field} flagged on {:?}: {violations:?}",
            action.type_name()
        );
    }
    for action in all_variant_actions()
        .into_iter()
        .filter(|a| contract_for(a).supported)
    {
        assert!(
            validate_action(&action).is_empty(),
            "{:?} validates clean",
            action.type_name()
        );
    }
}

/// Deprecated variants fail closed with `unsupported_action` even when
/// their fields are valid.
#[test]
fn deprecated_variants_fail_closed() {
    use toron_workflow::engine::ActionErrorCode;
    for action in [
        parse_action("    action: react\n    message_id: m\n    emoji: \"+1\"\n"),
        parse_action("    action: call_webhook\n    url: https://x.example\n"),
    ] {
        assert!(
            !contract_for(&action).supported,
            "{:?} unsupported",
            action.type_name()
        );
        let violations = validate_action(&action);
        assert!(
            violations
                .iter()
                .any(|v| v.code == ActionErrorCode::UnsupportedAction),
            "unsupported_action code on {:?}: {violations:?}",
            action.type_name()
        );
    }
}

/// Output-shape portraits: `Null` is valid only for unit actions; branch
/// envelopes, arrays, and ok-objects validate by shape.
#[test]
fn check_output_portraits() {
    let delay = parse_action("    action: delay\n    secs: 1\n");
    assert_eq!(contract_for(&delay).output, OutputKind::Unit);
    assert!(check_output(&delay, &serde_json::Value::Null).is_ok());
    assert!(check_output(&delay, &serde_json::json!({"ok": true})).is_err());

    let foreach = parse_action(
        "    action: foreach\n    over: \"{{trigger.items}}\"\n    each:\n      action: delay\n      secs: 1\n",
    );
    assert!(check_output(&foreach, &serde_json::json!([1, 2])).is_ok());
    assert!(check_output(&foreach, &serde_json::json!({"ok": true})).is_err());

    let branch = parse_action(
        "    action: branch\n    if: \"1 == 1\"\n    then:\n      - action: delay\n        secs: 1\n",
    );
    assert!(
        check_output(
            &branch,
            &serde_json::json!({"branch": "then", "outputs": {}})
        )
        .is_ok()
    );
    assert!(check_output(&branch, &serde_json::json!({"branch": "then"})).is_err());

    let send_dm = parse_action("    action: send_dm\n    to: a\n    body: b\n");
    assert!(check_output(&send_dm, &serde_json::json!({"sent": true})).is_ok());
    assert!(check_output(&send_dm, &serde_json::Value::Null).is_err());

    let verify = parse_action("    action: verify\n    command: \"mbx test\"\n");
    assert!(check_output(&verify, &serde_json::json!({"ok": true, "exit": 0})).is_ok());
    assert!(check_output(&verify, &serde_json::json!({"ok": "yes"})).is_err());
}

/// Effect classes: mailbox/job/verify are external with named sinks;
/// approval/spawn park; composites are control; deprecated are unsupported.
#[test]
fn effect_classes_assigned() {
    let class_of = |yaml: &str| {
        let a = parse_action(yaml);
        (contract_for(&a).class, contract_for(&a).effect)
    };
    assert_eq!(
        class_of("    action: send_dm\n    to: a\n    body: b\n").0,
        ActionClass::EffectfulExternal
    );
    assert_eq!(
        class_of("    action: send_dm\n    to: a\n    body: b\n").1,
        EffectKind::External { sink: "mailbox" }
    );
    assert_eq!(
        class_of("    action: dispatch_job\n    assignee: a\n    spec: { task: t }\n").1,
        EffectKind::External { sink: "job" }
    );
    assert_eq!(
        class_of("    action: verify\n    command: x\n").1,
        EffectKind::External { sink: "verify" }
    );
    assert_eq!(
        class_of("    action: request_approval\n    message: q\n").0,
        ActionClass::Parked
    );
    assert_eq!(
        class_of("    action: delay\n    secs: 1\n").0,
        ActionClass::DurableControl
    );
    assert_eq!(
        class_of(
            "    action: retry\n    max_attempts: 2\n    step:\n      action: delay\n      secs: 1\n"
        )
        .0,
        ActionClass::DurableControl
    );
}

/// Lint rejects deprecated actions (authoring gate consumes the contract).
#[test]
fn lint_rejects_deprecated_actions() {
    let yaml = "name: old\ntrigger:\n  type: webhook\nsteps:\n  - name: r\n    action: react\n    message_id: m\n    emoji: \"+1\"\n";
    let wf = parse_workflow(yaml).expect("parses for migration");
    let report = toron_workflow::lint_workflow(&wf);
    assert!(
        !report.is_clean(),
        "deprecated react must fail lint: {:?}",
        report.issues
    );
}

/// Parse rejects unknown action fields (parse gate consumes the contract).
#[test]
fn parse_rejects_unknown_action_fields() {
    let yaml = "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: m\n    action: send_dm\n    to: a\n    body: b\n    to_typo: c\n";
    assert!(
        parse_workflow(yaml).is_err(),
        "unknown field to_typo must fail parse"
    );
}

/// Digests are deterministic, distinct per workflow, and bound to declared
/// inputs/defaults.
#[test]
fn workflow_digest_deterministic_and_input_bound() {
    let a = parse_workflow(
        "name: w\ntrigger:\n  type: webhook\ninputs:\n  - name: mode\n    default: fast\nsteps:\n  - name: s\n    action: delay\n    secs: 1\n",
    )
    .expect("parse");
    let b = parse_workflow(
        "name: w\ntrigger:\n  type: webhook\ninputs:\n  - name: mode\n    default: fast\nsteps:\n  - name: s\n    action: delay\n    secs: 1\n",
    )
    .expect("parse");
    let c = parse_workflow(
        "name: w\ntrigger:\n  type: webhook\ninputs:\n  - name: mode\n    default: slow\nsteps:\n  - name: s\n    action: delay\n    secs: 1\n",
    )
    .expect("parse");
    assert_eq!(workflow_digest(&a), workflow_digest(&b), "deterministic");
    assert_ne!(
        workflow_digest(&a),
        workflow_digest(&c),
        "declared defaults bind the digest"
    );
    assert!(!workflow_digest(&a).starts_with(SCHEMA_VERSION));
}

/// Node paths carry composite ancestry; top-level steps are bare names.
#[test]
fn node_path_carries_ancestry() {
    assert_eq!(node_path(&[], "go"), "go");
    assert_eq!(node_path(&["retry"], "go"), "retry/go");
    assert_eq!(node_path(&["outer", "branch"], "go"), "outer/branch/go");
}

/// Iteration keys bind parent collection + index: same index on a changed
/// collection is a different key.
#[test]
fn iteration_key_binds_collection() {
    let k1 = iteration_key("parent-a", 0);
    assert_eq!(k1, iteration_key("parent-a", 0), "deterministic");
    assert_ne!(k1, iteration_key("parent-a", 1), "index matters");
    assert_ne!(k1, iteration_key("parent-b", 0), "collection matters");
}

/// Effect keys are stable across attempts (no attempt in the inputs) and
/// distinct per node.
#[test]
fn effect_key_stable_inputs() {
    let k1 = effect_key("wd", "run-1", "go", None, "ad");
    assert_eq!(k1, effect_key("wd", "run-1", "go", None, "ad"));
    assert_ne!(k1, effect_key("wd", "run-1", "other", None, "ad"));
    assert_ne!(
        k1,
        effect_key("wd", "run-1", "go", Some("iter-0"), "ad"),
        "iteration distinguishes"
    );
}

/// Canonical JSON is key-ordered regardless of insertion order.
#[test]
fn canonical_json_key_ordered() {
    let mut m = serde_json::Map::new();
    m.insert("b".into(), serde_json::json!(1));
    m.insert("a".into(), serde_json::json!(2));
    assert_eq!(
        canonical_json(&serde_json::Value::Object(m)),
        r#"{"a":2,"b":1}"#
    );
}

/// sha256_hex is deterministic hex over 0x1f-joined parts.
#[test]
fn sha256_hex_frames_parts() {
    assert_eq!(sha256_hex(&["a", "b"]), sha256_hex(&["a", "b"]));
    assert_eq!(sha256_hex(&["a", "b"]).len(), 64);
    assert_ne!(sha256_hex(&["a", "b"]), sha256_hex(&["a", "c"]));
    assert_ne!(sha256_hex(&["ab"]), sha256_hex(&["a", "b"]));
}

/// Redaction strips secret-named leaves at any depth and reports bearing.
#[test]
fn redact_value_strips_secrets() {
    let v = serde_json::json!({
        "to": "pi",
        "spec": { "task": "t", "api_key": "SECRET", "nested": { "password": "pw" } }
    });
    let (redacted, bearing) = redact_value(&v);
    assert!(bearing, "secret detected");
    assert_eq!(redacted["spec"]["api_key"], "…[redacted]");
    assert_eq!(redacted["spec"]["nested"]["password"], "…[redacted]");
    assert_eq!(redacted["to"], "pi", "non-secrets survive");
    assert_eq!(redacted["spec"]["task"], "t");

    let clean = serde_json::json!({"to": "pi", "body": "hi"});
    let (same, bearing) = redact_value(&clean);
    assert!(!bearing);
    assert_eq!(same, clean);
}

/// Normalization validates, redacts, canonicalizes, and digests: the digest
/// covers the REDACTED form and the secret never persists in the value.
#[test]
fn normalize_action_redacts_and_digests() {
    let action = parse_action(
        "    action: dispatch_job\n    assignee: a\n    spec: { task: t, token: SECRET }\n",
    );
    let rendered = serde_json::json!({
        "action": "dispatch_job",
        "assignee": "a",
        "spec": { "task": "t", "token": "SECRET" }
    });
    let resolved = normalize_action(&action, &rendered).expect("normalizes");
    assert_eq!(resolved.type_name, "dispatch_job");
    assert!(resolved.secret_bearing);
    assert_eq!(resolved.value["spec"]["token"], "…[redacted]");
    assert_eq!(resolved.digest, action_digest_of(&resolved.value));
}

/// The replay predicate accepts the prepared digest and rejects any other
/// render for the same node — no silent new logical effect.
#[test]
fn check_replay_accepts_same_rejects_changed() {
    let prepared = PreparedIdentity {
        node_path: "go".into(),
        action_digest: "abc".into(),
        effect_key: "key-1".into(),
    };
    assert!(check_replay(&prepared, "abc").is_ok());
    let err = check_replay(&prepared, "def").expect_err("changed render rejected");
    assert!(
        err.contains("go") && err.contains("reconcile"),
        "names node + reconciliation: {err}"
    );
}
