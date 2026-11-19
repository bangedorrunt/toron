//! One `Backend` contract, proved once, run over every implementation
//! (bd-egvr6). A capability an implementation lacks MUST answer typed
//! `Unsupported`; every other method MUST NOT. A silently-unimplemented method
//! therefore fails the suite instead of passing by accident.

use super::{AgentStatus, Backend, BackendError, BackendErrorCode, WaitOptions};

/// The per-implementation expectations a conformance run checks.
pub struct ConformanceHarness<B: Backend> {
    pub backend: B,
    pub expected_name: &'static str,
    /// Methods this backend deliberately does not implement. Each must answer
    /// `Err(Unsupported)`; every other method must not.
    pub expected_unsupported: &'static [&'static str],
}

/// One broken contract observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceFailure {
    pub method: &'static str,
    pub detail: String,
}

/// Classify one typed result against the manifest. Kept public so its two
/// decisions have RED witnesses of their own.
pub fn check_result<T>(
    failures: &mut Vec<ConformanceFailure>,
    method: &'static str,
    expected_unsupported: &[&'static str],
    result: Result<T, BackendError>,
) {
    let is_unsupported = matches!(&result, Err(e) if e.code() == &BackendErrorCode::Unsupported);
    let should_be = expected_unsupported.contains(&method);
    match (should_be, is_unsupported) {
        (true, true) => {}
        (true, false) => failures.push(ConformanceFailure {
            method,
            detail: format!(
                "expected typed Unsupported, got {}",
                describe(&result)
            ),
        }),
        (false, true) => failures.push(ConformanceFailure {
            method,
            detail: "unexpected typed Unsupported".into(),
        }),
        (false, false) => {}
    }
}

fn describe<T>(result: &Result<T, BackendError>) -> String {
    match result {
        Ok(_) => "Ok".into(),
        Err(e) => format!("Err({})", e.code().as_str()),
    }
}

/// Run every trait method once with sentinel arguments and check its typed
/// result against the manifest. Sentinel ids are never real panes, so a live
/// backend cannot affect an operator's session.
pub fn conformance<B: Backend>(harness: &ConformanceHarness<B>) -> Vec<ConformanceFailure> {
    let backend: &dyn Backend = &harness.backend;
    let mut failures = Vec::new();
    let unsupported = harness.expected_unsupported;

    if backend.name() != harness.expected_name {
        failures.push(ConformanceFailure {
            method: "name",
            detail: format!("expected {}, got {}", harness.expected_name, backend.name()),
        });
    }

    let target = "conformance:sentinel";
    let label = "conformance-sentinel";
    let wait = WaitOptions {
        timeout_ms: Some(1),
        until: vec![],
    };
    let statuses: [AgentStatus; 0] = [];

    check_result(&mut failures, "workspace_list", unsupported, backend.workspace_list().map(|_| ()));
    check_result(&mut failures, "workspace_create", unsupported, backend.workspace_create(Some(label)).map(|_| ()));
    check_result(&mut failures, "workspace_close", unsupported, backend.workspace_close(target, false));
    check_result(&mut failures, "pane_split", unsupported, backend.pane_split(target, "h", None).map(|_| ()));
    check_result(&mut failures, "pane_list", unsupported, backend.pane_list(None).map(|_| ()));
    check_result(&mut failures, "pane_close", unsupported, backend.pane_close(target));
    check_result(&mut failures, "pane_run", unsupported, backend.pane_run(target, "true"));
    check_result(&mut failures, "pane_wait_output", unsupported, backend.pane_wait_output(target, "conformance-never", 1).map(|_| ()));
    check_result(&mut failures, "agent_alive", unsupported, backend.agent_alive(target, "opencode").map(|_| ()));
    check_result(&mut failures, "agent_list", unsupported, backend.agent_list().map(|_| ()));
    check_result(&mut failures, "identity_snapshot", unsupported, backend.identity_snapshot().map(|_| ()));
    check_result(&mut failures, "agent_start", unsupported, backend.agent_start(label, "opencode", target, &[]).map(|_| ()));
    check_result(&mut failures, "agent_rename", unsupported, backend.agent_rename(target, None));
    check_result(&mut failures, "prompt", unsupported, backend.prompt(target, "conformance", Some(&wait)).map(|_| ()));
    check_result(&mut failures, "prompt_coded", unsupported, backend.prompt_coded(target, "conformance", Some(&wait)).map(|_| ()));
    check_result(&mut failures, "agent_wait", unsupported, backend.agent_wait(target, &statuses, Some(1)).map(|_| ()));
    check_result(&mut failures, "agent_read", unsupported, backend.agent_read(target, "text", 1).map(|_| ()));
    check_result(&mut failures, "agent_send_keys", unsupported, backend.agent_send_keys(target, &["enter"]));
    check_result(&mut failures, "subscribe", unsupported, backend.subscribe(&[]).map(|_| ()));

    failures
}

/// The side-effect-free subset, for live backends: read calls and a sentinel
/// liveness probe only. Mutating methods would create real panes/sessions.
pub fn conformance_readonly<B: Backend>(harness: &ConformanceHarness<B>) -> Vec<ConformanceFailure> {
    let backend: &dyn Backend = &harness.backend;
    let mut failures = Vec::new();
    let unsupported = harness.expected_unsupported;
    let target = "conformance:sentinel";
    check_result(&mut failures, "workspace_list", unsupported, backend.workspace_list().map(|_| ()));
    check_result(&mut failures, "pane_list", unsupported, backend.pane_list(None).map(|_| ()));
    check_result(&mut failures, "agent_list", unsupported, backend.agent_list().map(|_| ()));
    check_result(&mut failures, "identity_snapshot", unsupported, backend.identity_snapshot().map(|_| ()));
    check_result(&mut failures, "agent_alive", unsupported, backend.agent_alive(target, "opencode").map(|_| ()));
    check_result(&mut failures, "agent_read", unsupported, backend.agent_read(target, "text", 1).map(|_| ()));
    check_result(&mut failures, "subscribe", unsupported, backend.subscribe(&[]).map(|_| ()));
    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::mock::MockBackend;

    fn mock_harness() -> ConformanceHarness<MockBackend> {
        ConformanceHarness {
            backend: MockBackend::new(),
            expected_name: "mock",
            expected_unsupported: &["agent_alive"],
        }
    }

    #[test]
    fn the_mock_satisfies_its_manifest() {
        let report = conformance(&mock_harness());
        assert!(report.is_empty(), "mock conformance failed: {report:?}");
    }

    #[test]
    fn an_unsupported_method_must_not_answer_ok() {
        let mut failures = Vec::new();
        check_result(
            &mut failures,
            "agent_alive",
            &["agent_alive"],
            Ok::<Option<bool>, BackendError>(None),
        );
        assert_eq!(failures.len(), 1, "a silently implemented capability must fail");
        assert_eq!(failures[0].method, "agent_alive");
    }

    #[test]
    fn an_unlisted_method_must_not_answer_unsupported() {
        let mut failures = Vec::new();
        check_result(
            &mut failures,
            "agent_alive",
            &[],
            Err::<(), BackendError>(BackendError::unsupported("agent_alive")),
        );
        assert_eq!(failures.len(), 1, "an unexpected Unsupported must fail");
        assert_eq!(failures[0].method, "agent_alive");
    }

    #[test]
    fn a_declared_unsupported_method_answers_unsupported() {
        let mut failures = Vec::new();
        check_result(
            &mut failures,
            "agent_alive",
            &["agent_alive"],
            Err::<(), BackendError>(BackendError::unsupported("agent_alive")),
        );
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    #[ignore = "live hc: needs HC_BIN and a local hc daemon"]
    fn live_hc_conformance() {
        let bin = std::env::var_os("HC_BIN")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/Users/bangedorrunt/.local/bin/hc"));
        assert!(
            bin.is_file(),
            "HC_BIN={} missing; gated live run refused",
            bin.display()
        );
        let backend = crate::backend::hc::HcBackend::connect(None).expect("connect hc");
        let harness = ConformanceHarness {
            backend,
            expected_name: "hc",
            expected_unsupported: &[],
        };
        let report = conformance_readonly(&harness);
        assert!(report.is_empty(), "hc conformance failed: {report:?}");
    }

    #[test]
    #[ignore = "live rex: needs REX_BIN_DIR with a rex CLI"]
    fn live_rex_conformance() {
        let dir = std::env::var_os("REX_BIN_DIR").expect("live rex conformance needs REX_BIN_DIR set");
        assert!(
            std::path::Path::new(&dir).is_dir(),
            "REX_BIN_DIR={} missing; gated live run refused",
            std::path::PathBuf::from(&dir).display()
        );
        let harness = ConformanceHarness {
            backend: crate::backend::superlogical::SuperlogicalBackend,
            expected_name: "superlogical",
            expected_unsupported: &[],
        };
        let report = conformance_readonly(&harness);
        assert!(report.is_empty(), "rex conformance failed: {report:?}");
    }
}
