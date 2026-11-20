// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! MCP Events U1 protocol surface (ADR-0039 D1/D2).
//!
//! These are JSON-RPC methods, NOT tools: the 41-tool surface stays frozen.
//! A handler error is an in-band JSON-RPC error; a missing `_meta` is rejected
//! by the method-scoped check in `server.rs` before this router runs.

use rmcp::ErrorData;
use rmcp::model::{CustomResult, ErrorCode};
use serde_json::{Value, json};
use toron_core::events::catalogue;

/// The capability key advertised under `capabilities.extensions`.
pub const EVENTS_EXTENSION: &str = "io.modelcontextprotocol/events";

/// Route an `events/*` custom request. `None` means "not ours"; the caller
/// then falls back to the default method-not-found handler.
pub fn route(method: &str, params: Option<&Value>) -> Option<Result<CustomResult, ErrorData>> {
    if !method.starts_with("events/") {
        return None;
    }
    Some(match method {
        "events/list" => Ok(CustomResult(json!({
            "events": catalogue::catalogue(),
        }))),
        "events/subscribe" | "events/poll" | "events/stream" => match valid_name(params) {
            Ok(name) => Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                format!("{method} for {name} is not implemented in U1"),
                None,
            )),
            Err(err) => Err(err),
        },
        "events/unsubscribe" => Err(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            format!("{method} is not implemented in U1"),
            None,
        )),
        other => Err(ErrorData::new(
            ErrorCode::METHOD_NOT_FOUND,
            format!("unknown method {other}"),
            None,
        )),
    })
}

/// SEP-3415 uses `name` (plus `arguments`), not `event`. An unknown name is
/// AE1's in-band `-32602`.
fn valid_name(params: Option<&Value>) -> Result<&str, ErrorData> {
    let name = params
        .and_then(|p| p.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if catalogue::catalogue().iter().all(|row| row.name != name) {
        return Err(ErrorData::new(
            ErrorCode::INVALID_PARAMS,
            format!("unknown event name {name:?}"),
            None,
        ));
    }
    Ok(name)
}

/// bd-6bgrd U2: the ONE authorization seam for `events/*`. U1 has no tokens;
/// U2 replaces this body with its principal resolver from the Authorization
/// header value. No other events handler reads auth.
pub fn authorize(_authorization: Option<&str>) -> Result<(), ErrorData> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_event_name_is_in_band_invalid_params() {
        let params = json!({"name": "not.an.event"});
        let err = route("events/subscribe", Some(&params))
            .expect("routed")
            .expect_err("unknown name");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        assert_eq!(err.code.0, -32602);
        assert!(err.message.contains("unknown event name"), "{err:?}");
    }

    #[test]
    fn known_names_on_unimplemented_methods_are_errors_never_ok() {
        let params = json!({"name": "mail.addressed", "arguments": {}});
        for method in ["events/subscribe", "events/poll", "events/stream"] {
            let out = route(method, Some(&params)).expect("routed");
            assert!(out.is_err(), "{method} must not fake success");
        }
    }

    #[test]
    fn only_events_methods_route_here() {
        assert!(route("tools/list", None).is_none());
        assert!(route("events/nope", None).is_some());
        assert!(route("events/list", None).is_some());
    }

    #[test]
    fn authorization_seam_exists_and_is_tokenless_in_u1() {
        assert!(authorize(None).is_ok());
        assert!(authorize(Some("Bearer anything")).is_ok());
    }
}
