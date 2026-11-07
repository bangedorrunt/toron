// governed-by: ADR-0002, ADR-0045
//! Versioned wire contract for the rex API surface this adapter depends on.
//!
//! Rex is an additive API. A method can disappear or change shape when a
//! server is older than the client, and new methods arrive without a crate
//! release here. Keep the methods we call, their request keys, and the response
//! keys we consume in ONE table so a server upgrade is a small diff, not a
//! codebase search. [`TESTED_API_REVISION`] names the server revision this
//! contract was last exercised against.
//!
//! The adapter still parses with its own typed functions. [`validate_response`]
//! is the cheap precondition those parsers call before reading a field, so a
//! missing field is reported as malformed output instead of a silent default.
//!
//! Candidate rows document reviewed methods that are not wired yet. They are
//! deliberately not required by the version-skew probe.

use std::fmt;

use serde_json::{Value, json};

/// The `rex api` revision this contract was last verified against.
pub const TESTED_API_REVISION: u32 = 11;

/// Whether the adapter currently calls a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RexApiState {
    /// Called by `rex_cli` / `superlogical`; breaking this is a live defect.
    Adopted,
    /// Reviewed and documented, but not called yet.
    Candidate,
}

impl RexApiState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Adopted => "adopted",
            Self::Candidate => "candidate",
        }
    }
}

/// One method in the rex wire contract.
///
/// `request` and `response` are the TOP-LEVEL keys the adapter sends or reads.
/// A method whose request/response has no named keys uses an empty slice.
#[derive(Debug, Clone, Copy)]
pub struct RexApiMethod {
    pub name: &'static str,
    pub state: RexApiState,
    pub request: &'static [&'static str],
    pub response: &'static [&'static str],
    /// Why this method is adopted, or what adopting it would buy.
    pub note: &'static str,
}

/// The checked-in contract. Add a row before calling a new method; flip a
/// candidate to adopted only when a parser and a test exist.
pub const METHODS: &[RexApiMethod] = &[
    RexApiMethod {
        name: "session.list",
        state: RexApiState::Adopted,
        request: &[],
        response: &["sessions"],
        note: "rex ls --json; also accepts a bare array for older servers",
    },
    RexApiMethod {
        name: "session.list_blocks",
        state: RexApiState::Adopted,
        request: &[],
        response: &["blocks"],
        note: "the roster and per-session block lookup",
    },
    RexApiMethod {
        name: "target.resolve",
        state: RexApiState::Adopted,
        request: &["kinds", "queries"],
        response: &["results"],
        note: "one all-session lookup; locate still requires an exact id or label",
    },
    RexApiMethod {
        name: "com.superlogical.terminal.format",
        state: RexApiState::Adopted,
        request: &["block_id", "args"],
        response: &["content"],
        note: "bounded screen/scrollback read; id and total are present only when more remains",
    },
    RexApiMethod {
        name: "com.superlogical.terminal.blob_read",
        state: RexApiState::Adopted,
        request: &["block_id", "args"],
        response: &["id", "data", "total", "eof"],
        note: "continues a format reply too large for one response",
    },
    RexApiMethod {
        name: "program_status",
        state: RexApiState::Adopted,
        request: &[],
        response: &["records"],
        note: "OSC 7501 records; empty records fall back to process",
    },
    RexApiMethod {
        name: "process",
        state: RexApiState::Adopted,
        request: &[],
        response: &["foreground"],
        note: "liveness fallback when no program reported a status",
    },
    RexApiMethod {
        name: "com.superlogical.terminal.write",
        state: RexApiState::Adopted,
        request: &["data"],
        response: &[],
        note: "prompt and pane_run write; Enter is a separate key event",
    },
    RexApiMethod {
        name: "block.close",
        state: RexApiState::Adopted,
        request: &["block_id"],
        response: &["block_id"],
        note: "pane_close",
    },
    RexApiMethod {
        name: "client.key",
        state: RexApiState::Adopted,
        request: &[],
        response: &[],
        note: "rex block send-key; key bindings see the key first",
    },
    RexApiMethod {
        name: "block.event",
        state: RexApiState::Adopted,
        request: &["block_id", "event"],
        response: &[],
        note: "rex events and the current send-key path",
    },
    RexApiMethod {
        name: "session.kill",
        state: RexApiState::Adopted,
        request: &[],
        response: &[],
        note: "workspace_close",
    },
    RexApiMethod {
        name: "data.connect",
        state: RexApiState::Candidate,
        request: &["session_id", "block_id", "data_ref_id"],
        response: &["ticket", "expires_in_ms"],
        note: "stream terminal changes instead of polling agent_read in pane_wait_output",
    },
    RexApiMethod {
        name: "session.describe_block",
        state: RexApiState::Candidate,
        request: &["session_id", "block_id"],
        response: &["block", "api"],
        note: "discover per-block methods, events, and actions for harness capability checks",
    },
    RexApiMethod {
        name: "session.attach_all",
        state: RexApiState::Candidate,
        request: &[],
        response: &[],
        note: "attach once to every session before opening a data connection",
    },
];

/// Look up one contract row by its exact method name.
pub fn method(name: &str) -> Option<&'static RexApiMethod> {
    METHODS.iter().find(|method| method.name == name)
}

/// True when the adapter has adopted `name`.
pub fn is_adopted(name: &str) -> bool {
    method(name).is_some_and(|method| method.state == RexApiState::Adopted)
}

/// Every adopted method, in contract order. The version-skew probe uses this
/// as its authoritative list.
pub fn adopted() -> impl Iterator<Item = &'static RexApiMethod> {
    METHODS
        .iter()
        .filter(|method| method.state == RexApiState::Adopted)
}

/// One schema violation. `missing` is empty when the whole response has the
/// wrong JSON shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RexSchemaError {
    pub method: String,
    pub missing: Vec<&'static str>,
    pub unknown: bool,
}

impl fmt::Display for RexSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.unknown {
            write!(
                f,
                "rex method {} is not registered in the schema contract",
                self.method
            )
        } else if self.missing.is_empty() {
            write!(
                f,
                "rex method {} returned a non-object response",
                self.method
            )
        } else {
            write!(
                f,
                "rex method {} response is missing {}",
                self.method,
                self.missing.join(", ")
            )
        }
    }
}

impl std::error::Error for RexSchemaError {}

/// Check the top-level response shape declared by the contract.
///
/// A method absent from [`METHODS`] is itself drift: an unregistered call must
/// not reach the wire without a contract row and a parser.
pub fn validate_response(method_name: &str, value: &Value) -> Result<(), RexSchemaError> {
    let Some(spec) = method(method_name) else {
        return Err(RexSchemaError {
            method: method_name.to_string(),
            missing: Vec::new(),
            unknown: true,
        });
    };
    let Some(object) = value.as_object() else {
        return Err(RexSchemaError {
            method: method_name.to_string(),
            missing: Vec::new(),
            unknown: false,
        });
    };
    let missing: Vec<&'static str> = spec
        .response
        .iter()
        .copied()
        .filter(|field| !object.contains_key(*field))
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(RexSchemaError {
            method: method_name.to_string(),
            missing,
            unknown: false,
        })
    }
}

/// Machine-readable contract for `flywheel backend schema`, docs, or a
/// migration diff. The JSON shape is intentionally boring and stable.
pub fn contract_json() -> Value {
    json!({
        "tested_revision": TESTED_API_REVISION,
        "methods": METHODS
            .iter()
            .map(|method| json!({
                "name": method.name,
                "state": method.state.as_str(),
                "request": method.request,
                "response": method.response,
                "note": method.note,
            }))
            .collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn contract_names_are_unique() {
        let mut seen = BTreeSet::new();
        for method in METHODS {
            assert!(
                seen.insert(method.name),
                "duplicate rex contract method {}",
                method.name
            );
        }
    }

    #[test]
    fn adopted_methods_are_real_and_candidates_stay_out_of_the_skew_probe() {
        assert!(is_adopted("target.resolve"));
        assert!(is_adopted("com.superlogical.terminal.format"));
        assert!(is_adopted("com.superlogical.terminal.blob_read"));
        assert!(!is_adopted("data.connect"));
        assert!(!is_adopted("session.describe_block"));
        assert_eq!(
            adopted().filter(|m| m.name == "data.connect").count(),
            0,
            "a candidate must not become a version requirement by accident"
        );
    }

    #[test]
    fn validate_response_accepts_the_declared_shape() {
        validate_response("target.resolve", &json!({ "results": [] }))
            .expect("target.resolve shape");
        validate_response(
            "com.superlogical.terminal.format",
            &json!({ "content": "" }),
        )
        .expect("format shape");
        validate_response(
            "com.superlogical.terminal.blob_read",
            &json!({ "id": 1, "data": "", "total": 0, "eof": true }),
        )
        .expect("blob_read shape");
    }

    #[test]
    fn validate_response_names_every_missing_field() {
        let err = validate_response("com.superlogical.terminal.blob_read", &json!({ "id": 1 }))
            .expect_err("missing data/total/eof must fail");
        assert_eq!(err.missing, ["data", "total", "eof"]);
        let text = err.to_string();
        assert!(text.contains("blob_read"), "{text}");
        assert!(text.contains("data"), "{text}");
    }

    #[test]
    fn an_unregistered_method_is_schema_drift() {
        let err = validate_response("not.in.the.contract", &json!({}))
            .expect_err("unregistered method must fail");
        assert!(err.missing.is_empty());
        assert!(
            err.unknown,
            "unknown method must be distinguished from a bad shape"
        );
        assert!(err.to_string().contains("not registered"));
        assert!(err.to_string().contains("not.in.the.contract"));
    }

    #[test]
    fn contract_json_carries_the_revision_and_both_states() {
        let value = contract_json();
        assert_eq!(value["tested_revision"], json!(TESTED_API_REVISION));
        let rows = value["methods"].as_array().expect("methods array");
        assert!(rows.iter().any(|row| row["state"] == "adopted"));
        assert!(rows.iter().any(|row| row["state"] == "candidate"));
    }
}
