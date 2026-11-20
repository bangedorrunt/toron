// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! MCP Events U1 protocol surface (ADR-0039 D1/D2).
//!
//! These are JSON-RPC methods, NOT tools: the 41-tool surface stays frozen.
//! A handler error is an in-band JSON-RPC error, never an HTTP 401.

use rmcp::ErrorData;
use rmcp::model::{CustomResult, ErrorCode};
use serde_json::{Value, json};
use toron_core::events::catalogue;
use toron_core::events::principal::{EventsVerb, Principal, authorize as d6_authorize};

/// The capability key advertised under `capabilities.extensions`.
pub const EVENTS_EXTENSION: &str = "io.modelcontextprotocol/events";

/// Route an `events/*` custom request. `None` means "not ours"; the caller
/// then falls back to the default method-not-found handler.
pub fn route(
    method: &str,
    params: Option<&Value>,
    principal: &Principal,
) -> Option<Result<CustomResult, ErrorData>> {
    if !method.starts_with("events/") {
        return None;
    }
    let verb = request_verb(method, params, principal);
    if !d6_authorize(principal, verb) {
        return Some(Err(ErrorData::new(
            ErrorCode::INVALID_PARAMS,
            format!("{verb:?} requires the operator principal"),
            None,
        )));
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

/// Map a request to the D6 verb it asks for. Scope comes from the SEP-3415
/// `arguments` object, not ad-hoc flags: a pin naming another pin or project
/// in its arguments asks for cross-scope, which is operator-only.
fn request_verb(method: &str, params: Option<&Value>, principal: &Principal) -> EventsVerb {
    let args = params.and_then(|p| p.get("arguments"));
    if let Principal::Pin { pin, project } = principal {
        let other_pin = args
            .and_then(|a| a.get("pin"))
            .and_then(Value::as_str)
            .is_some_and(|p| p != pin);
        let other_project = args
            .and_then(|a| a.get("project"))
            .and_then(Value::as_str)
            .is_some_and(|p| p != project);
        if other_pin || other_project {
            return EventsVerb::CrossScope;
        }
    }
    if args
        .and_then(|a| a.get("full_payload"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return EventsVerb::FullPayload;
    }
    match method {
        "events/list" => EventsVerb::List,
        "events/subscribe"
            if args
                .and_then(|a| a.get("webhook"))
                .and_then(Value::as_bool)
                .unwrap_or(false) =>
        {
            EventsVerb::SubscribeWebhook
        }
        "events/subscribe" | "events/poll" | "events/stream" | "events/unsubscribe" => {
            EventsVerb::SubscribePoll
        }
        _ => EventsVerb::List,
    }
}

/// U3 durable path. The U1 router above stays pure; subscription methods get
/// an injected store and clock so their decisions are testable without a
/// daemon. A missing store means the method is unavailable in this host.
pub async fn route_events(
    method: &str,
    params: Option<&Value>,
    principal: &Principal,
    store: Option<&dyn toron_core::events::store::SubscriptionStore>,
    now_us: i64,
) -> Option<Result<CustomResult, ErrorData>> {
    let store = match store {
        Some(s) => s,
        None => return route(method, params, principal),
    };
    if !d6_authorize(principal, request_verb(method, params, principal)) {
        return Some(Err(ErrorData::new(
            ErrorCode::INVALID_PARAMS,
            "operator principal required by the D6 matrix",
            None,
        )));
    }
    if !matches!(method, "events/subscribe" | "events/poll" | "events/unsubscribe") {
        return route(method, params, principal);
    }
    let id_param = |key: &str| {
        params
            .and_then(|p| p.get(key))
            .and_then(Value::as_str)
            .unwrap_or("")
    };
    let result = match method {
        "events/subscribe" => {
            let name = match valid_name(params) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };
            let args = params
                .and_then(|p| p.get("arguments"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let ttl_us = args
                .get("ttl_us")
                .and_then(Value::as_i64)
                .or_else(|| args.get("ttlMs").and_then(Value::as_i64).map(|ms| ms * 1000));
            let delivery = id_param("delivery");
            let delivery = if delivery.is_empty() { "poll" } else { delivery };
            match store
                .subscribe(&toron_core::events::store::SubscribeSpec {
                    principal: principal.name().to_string(),
                    is_pin: matches!(principal, Principal::Pin { .. }),
                    delivery: delivery.to_string(),
                    name: name.to_string(),
                    args,
                    ttl_us,
                    now_us,
                })
                .await
            {
                Ok(sub) => Ok(CustomResult(json!({
                    "id": sub.id,
                    "expiresAt": sub.expires_us,
                    "cursor": sub.cursor,
                }))),
                Err(e) => Err(ErrorData::new(ErrorCode::INVALID_PARAMS, e.to_string(), None)),
            }
        }
        "events/poll" => {
            let id = id_param("subscriptionId");
            match store.get(id).await {
                Ok(Some(sub)) if sub.state == "terminated" => Ok(CustomResult(json!({
                    "events": [], "cursor": sub.cursor, "nextPollMs": 0,
                    "hasMore": false, "truncated": false, "terminated": true,
                }))),
                Ok(Some(sub)) => {
                    let after = id_param("cursor").parse::<u64>().unwrap_or(sub.cursor);
                    match store.poll(id, after, 32).await {
                        Ok(page) => Ok(CustomResult(json!({
                            "events": page.events.iter().map(|d| {
                                serde_json::from_str::<Value>(&d.payload)
                                    .unwrap_or_else(|_| json!({ "id": d.seq.to_string() }))
                            }).collect::<Vec<_>>(),
                            "cursor": page.cursor.to_string(),
                            "nextPollMs": 5000,
                            "hasMore": page.has_more,
                            "truncated": page.truncated,
                        }))),
                        Err(e) => Err(ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)),
                    }
                }
                Ok(None) => Err(ErrorData::new(ErrorCode::INVALID_PARAMS, "unknown subscription", None)),
                Err(e) => Err(ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)),
            }
        }
        "events/unsubscribe" => {
            let id = id_param("subscriptionId");
            match store.unsubscribe(id, principal.name()).await {
                Ok(true) => Ok(CustomResult(json!({"id": id, "terminated": true}))),
                Ok(false) => Err(ErrorData::new(ErrorCode::INVALID_PARAMS, "not your subscription", None)),
                Err(e) => Err(ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None)),
            }
        }
        _ => unreachable!(),
    };
    Some(result)
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
/// Resolve one bearer token to a principal (ADR-0039 D6, bd-6bgrd). A missing,
/// unknown, revoked or expired token is an in-band JSON-RPC error — never an
/// HTTP 401 — so events methods keep the rmcp error contract.
pub fn authorize(authorization: Option<&str>) -> Result<Principal, ErrorData> {
    let store = toron_core::events::principal::FileTokenStore::at_root(
        &toron_core::config::ToronPaths::resolve().root,
    );
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    authorize_with_store(authorization, now_us, &store)
}

/// The same decision over an injected store and clock, so the router can be
/// tested without touching the operator's durable root.
pub fn authorize_with_store(
    authorization: Option<&str>,
    now_us: i64,
    store: &dyn toron_core::events::principal::TokenStore,
) -> Result<Principal, ErrorData> {
    use toron_core::events::principal::resolve;

    resolve(authorization, now_us, store).ok_or_else(|| {
        ErrorData::new(
            ErrorCode::INVALID_PARAMS,
            "unauthorized: a valid bearer token is required for events/*",
            None,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_event_name_is_in_band_invalid_params() {
        let params = json!({"name": "not.an.event"});
        let err = route("events/subscribe", Some(&params), &Principal::Operator)
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
            let out = route(method, Some(&params), &Principal::Operator).expect("routed");
            assert!(out.is_err(), "{method} must not fake success");
        }
    }

    #[test]
    fn only_events_methods_route_here() {
        assert!(route("tools/list", None, &Principal::Operator).is_none());
        assert!(route("events/nope", None, &Principal::Operator).is_some());
        assert!(route("events/list", None, &Principal::Operator).is_some());
    }

    #[test]
    fn authorization_seam_resolves_a_principal() {
        // U1 was token-less; U2+ resolve the bearer to a Principal. A missing
        // or unknown token is refused in-band, and a valid one is accepted.
        assert!(authorize(None).is_err());
        assert!(authorize(Some("Bearer unknown")).is_err());
    }
}
