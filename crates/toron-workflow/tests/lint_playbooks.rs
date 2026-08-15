// governed-by: ADR-0019
//! ADR-0019 wave-2 structural playbook lint tests (bd-w2-lints-6k6l),
//! fixture-driven: each failure mode has a checked-in YAML that MUST error
//! with the expected message; each passing control MUST stay clean.

use std::path::PathBuf;

use toron_workflow::lint::{Level, lint_yaml};

/// fixture filename -> expected blocking-error message substring.
/// `None` in the pass set means "must be clean".
const FAIL_CASES: &[(&str, &str)] = &[
    (
        "feature_missing_data_shape.yaml",
        "feature workflow requires a `data_shape` input",
    ),
    (
        "bugfix_no_repro.yaml",
        "bug-fix workflow has no `repro` node",
    ),
    (
        "bugfix_repro_after_fix.yaml",
        "bug-fix step `fix` must follow the `repro` step",
    ),
    ("prototype_n1.yaml", "prototype/arena require n >= 2"),
    (
        "prototype_compile_no_build_slot.yaml",
        "compile-mode prototype/arena arms must declare build-slot serialization",
    ),
    (
        "prototype_sketch_repo_path.yaml",
        "sketch-mode prototype/arena arms must not reference repo build paths",
    ),
    (
        "forensics_with_fix.yaml",
        "forensics workflow must contain NO fix/mutating step",
    ),
    ("principles_unknown_name.yaml", "unknown principle name"),
];

const PASS_CASES: &[&str] = &[
    "feature_ok.yaml",
    "bugfix_ok.yaml",
    "prototype_sketch_ok.yaml",
    "prototype_compile_ok.yaml",
    "forensics_ok.yaml",
    "principles_ok.yaml",
];

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("playbook-lint")
}

fn read_fixture(name: &str) -> String {
    std::fs::read_to_string(fixtures_dir().join(name))
        .unwrap_or_else(|e| panic!("read fixture {name}: {e}"))
}

#[test]
fn failure_mode_fixtures_error_with_expected_message() {
    for (fixture, expected) in FAIL_CASES {
        let yaml = read_fixture(fixture);
        let report = lint_yaml(&yaml).unwrap_or_else(|e| panic!("{fixture} must parse: {e}"));
        let blocking: Vec<&str> = report
            .issues
            .iter()
            .filter(|i| i.level == Level::Error)
            .map(|i| i.message.as_str())
            .collect();
        assert!(
            !blocking.is_empty(),
            "{fixture}: expected a blocking error containing {expected:?}, got clean: {:?}",
            report.issues
        );
        assert!(
            blocking.iter().any(|m| m.contains(expected)),
            "{fixture}: expected error containing {expected:?}, got {:?}",
            blocking
        );
    }
}

#[test]
fn passing_control_fixtures_stay_clean() {
    for fixture in PASS_CASES {
        let yaml = read_fixture(fixture);
        let report = lint_yaml(&yaml).unwrap_or_else(|e| panic!("{fixture} must parse: {e}"));
        assert!(
            report.is_clean(),
            "{fixture} must be lint-clean, got: {:?}",
            report.issues
        );
    }
}

/// Every checked-in fixture is covered by exactly one of the two manifests —
/// a new failure mode cannot silently land unasserted.
#[test]
fn every_fixture_is_covered() {
    let mut files: Vec<String> = std::fs::read_dir(fixtures_dir())
        .expect("fixtures dir")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".yaml"))
        .collect();
    files.sort();
    let mut covered: Vec<&str> = FAIL_CASES
        .iter()
        .map(|(f, _)| *f)
        .chain(PASS_CASES.iter().copied())
        .collect();
    covered.sort_unstable();
    assert_eq!(
        files, covered,
        "fixture dir and test manifests drifted — add new fixtures to FAIL_CASES or PASS_CASES"
    );
}
