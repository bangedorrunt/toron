// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! MCP Events U1 protocol surface (ADR-0039 D1/D2).
//!
//! These are JSON-RPC methods, NOT tools: the 41-tool surface stays frozen.
//! A handler error is an in-band JSON-RPC error (HTTP 200); a missing `_meta`
//! is rejected by the transport as HTTP 400 before it reaches here.

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
        "events/subscribe" => subscribe(params),
        "events/unsubscribe" | "events/poll" => Err(ErrorData::new(
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

fn subscribe(params: Option<&Value>) -> Result<CustomResult, ErrorData> {
    let params = params.ok_or_else(|| {
        ErrorData::new(
            ErrorCode::INVALID_PARAMS,
            "events/subscribe requires params with an event name",
            None,
        )
    })?;
    let name = params.get("event").and_then(Value::as_str).unwrap_or("");
    if catalogue::catalogue().iter().all(|row| row.name != name) {
        return Err(ErrorData::new(
            ErrorCode::INVALID_PARAMS,
            format!("unknown event name {name:?}"),
            None,
        ));
    }
    // U1 has no delivery backend yet (U2+); a valid name is accepted as a hint
    // contract, never as an active subscription.
    Ok(CustomResult(json!({
        "event": name,
        "status": "not_implemented",
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_event_name_is_in_band_invalid_params() {
        let params = json!({"event": "not.an.event"});
        let err = subscribe(Some(&params)).expect_err("unknown name");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        assert!(err.message.contains("unknown event name"), "{err:?}");
    }

    #[test]
    fn a_known_event_passes_the_name_check() {
        let params = json!({"event": "mail.addressed"});
        let out = subscribe(Some(&params)).expect("known name");
        assert_eq!(out.0["event"], "mail.addressed");
    }

    #[test]
    fn only_events_methods_route_here() {
        assert!(route("tools/list", None).is_none());
        assert!(route("events/nope", None).is_some());
        assert!(route("events/list", None).is_some());
    }
}
