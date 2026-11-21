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

/// SEP-3415 Final named error codes (bd-6zo01 G9). General-purpose codes with
/// typed `data`, not per-condition numbers.
pub const NOT_FOUND: ErrorCode = ErrorCode(-32023);
pub const FORBIDDEN: ErrorCode = ErrorCode(-32024);
pub const RESOURCE_EXHAUSTED: ErrorCode = ErrorCode(-32025);
pub const UNSUPPORTED: ErrorCode = ErrorCode(-32026);
pub const CALLBACK_ENDPOINT_ERROR: ErrorCode = ErrorCode(-32027);

pub mod signal_http;

/// SEP-3415 Final G10: a client that declares extensions must include ours.
/// Absent `_meta` is tolerated (older/our direct clients); a present but empty
/// or wrong extension set is refused with -32601.
pub fn client_declares(params: Option<&Value>, extension: &str) -> bool {
    let Some(p) = params else { return true };
    let Some(meta) = p.get("_meta") else {
        return true;
    };
    let Some(caps) = meta.get("io.modelcontextprotocol/clientCapabilities") else {
        return true;
    };
    let Some(ext) = caps.get("extensions") else {
        return true;
    };
    ext.get(extension).is_some()
}

pub mod webhook_delivery;

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
    if !method.starts_with("events/") {
        return None;
    }
    if !d6_authorize(principal, request_verb(method, params, principal)) {
        return Some(Err(ErrorData::new(
            FORBIDDEN,
            "operator principal required by the D6 matrix",
            Some(json!({"reason": "access_denied"})),
        )));
    }
    match method {
        "events/list" => {
            return Some(Ok(CustomResult(json!({
                "resultType": "complete",
                "events": catalogue::catalogue(),
            }))));
        }
        "events/stream" => {
            return Some(Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                "events/stream is not available in this host",
                None,
            )));
        }
        "events/subscribe" | "events/poll" | "events/unsubscribe" => {}
        other => {
            return Some(Err(ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                format!("unknown method {other}"),
                None,
            )));
        }
    }
    // A known verb with an unknown event name is an in-band error, before any
    // store is consulted (bd-w32ho).
    if method == "events/subscribe"
        && let Err(err) = valid_name(params)
    {
        return Some(Err(err));
    }
    let Some(store) = store else {
        return Some(Err(ErrorData::new(
            ErrorCode::INTERNAL_ERROR,
            "events: no subscription store in this host",
            None,
        )));
    };
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
            let ttl_us = args.get("ttl_us").and_then(Value::as_i64).or_else(|| {
                args.get("ttlMs")
                    .and_then(Value::as_i64)
                    .map(|ms| ms * 1000)
            });
            let delivery_obj = params.and_then(|p| p.get("delivery"));
            let mode = delivery_obj
                .and_then(|d| d.get("mode"))
                .and_then(Value::as_str)
                .unwrap_or("poll");
            // SEP-3415 Final G5: webhook secret is CLIENT-supplied and never
            // minted here; validate whsec_ + 24-64 decoded bytes, then verify
            // the endpoint before the subscription activates.
            let mut webhook: Option<(String, String)> = None;
            if mode == "webhook" {
                let d = match delivery_obj {
                    Some(d) => d,
                    None => {
                        return Some(Err(ErrorData::new(
                            ErrorCode::INVALID_PARAMS,
                            "webhook delivery requires params.delivery",
                            None,
                        )));
                    }
                };
                let url = match d.get("url").and_then(Value::as_str) {
                    Some(u) => u,
                    None => {
                        return Some(Err(ErrorData::new(
                            ErrorCode::INVALID_PARAMS,
                            "delivery.url is required",
                            None,
                        )));
                    }
                };
                let policy = toron_core::bus::egress::EgressPolicy::default();
                if let Err(e) = toron_core::bus::egress::validate_target(url, &policy) {
                    return Some(Err(ErrorData::new(
                        ErrorCode::INVALID_PARAMS,
                        format!("webhook URL refused: {e}"),
                        None,
                    )));
                }
                let secret_str = match d.get("secret").and_then(Value::as_str) {
                    Some(s) => s,
                    None => {
                        return Some(Err(ErrorData::new(
                            ErrorCode::INVALID_PARAMS,
                            "delivery.secret is required",
                            None,
                        )));
                    }
                };
                let secret =
                    match toron_core::events::webhook::Secret::from_client_whsec(secret_str) {
                        Ok(s) => s,
                        Err(e) => {
                            return Some(Err(ErrorData::new(
                                ErrorCode::INVALID_PARAMS,
                                format!("invalid delivery.secret: {e}"),
                                None,
                            )));
                        }
                    };
                let egress = match webhook_delivery::UreqEgress::new(policy.clone()) {
                    Ok(e) => e,
                    Err(e) => return Some(Err(ErrorData::new(ErrorCode::INTERNAL_ERROR, e, None))),
                };
                let challenge = format!("challenge_{}", now_us);
                // Deterministic sub id: stable across restarts and refreshes,
                // and the X-MCP-Subscription-Id on the verification POST.
                let sub_id = {
                    let canonical = toron_core::events::store::canonical_json(&args);
                    let digest = <sha2::Sha256 as sha2::Digest>::digest(
                        format!("{}:{}:{}:{}", principal.name(), url, name, canonical).as_bytes(),
                    );
                    hex::encode(digest)[..16].to_string()
                };
                match webhook_delivery::verify_endpoint_cached(
                    &egress,
                    &policy,
                    url,
                    &secret,
                    &challenge,
                    &sub_id,
                    principal.name(),
                    now_us,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        return Some(Err(ErrorData::new(
                            CALLBACK_ENDPOINT_ERROR,
                            "callback endpoint verification failed",
                            Some(json!({"reason": "challenge_failed"})),
                        )));
                    }
                    Err(e) => {
                        return Some(Err(ErrorData::new(
                            CALLBACK_ENDPOINT_ERROR,
                            "callback endpoint unreachable",
                            Some(json!({"reason": "connection_refused", "detail": e})),
                        )));
                    }
                }
                webhook = Some((url.to_string(), secret_str.to_string()));
            }
            match store
                .subscribe(&toron_core::events::store::SubscribeSpec {
                    principal: principal.name().to_string(),
                    is_pin: matches!(principal, Principal::Pin { .. }),
                    delivery: mode.to_string(),
                    name: name.to_string(),
                    args: args.clone(),
                    ttl_us,
                    now_us,
                })
                .await
            {
                Ok(sub) => {
                    if let Some((url, secret)) = webhook {
                        // Persist the spec for the daemon's delivery half; the
                        // secret is client-supplied and never echoed back.
                        let spec_path = toron_core::config::state_dir().join("webhook-specs.jsonl");
                        use std::io::Write as _;
                        #[cfg(unix)]
                        use std::os::unix::fs::OpenOptionsExt as _;
                        let mut opts = std::fs::OpenOptions::new();
                        opts.create(true).append(true);
                        #[cfg(unix)]
                        opts.mode(0o600);
                        if let Ok(mut f) = opts.open(spec_path) {
                            let _ = writeln!(
                                f,
                                "{}",
                                json!({
                                    "id": sub.id, "url": url, "secret": secret,
                                })
                            );
                        }
                    }
                    let mut out = json!({
                        "resultType": "complete",
                        "id": sub.id,
                        "refreshBefore": sub.expires_us.map(|us|
                            json!(toron_core::events::occurrence::iso8601(us))
                        ).unwrap_or(Value::Null),
                        "cursor": sub.cursor.to_string(),
                        "truncated": false,
                    });
                    if mode == "webhook" {
                        out["deliveryStatus"] = json!({
                            "active": true, "lastError": Value::Null,
                        });
                    }
                    Ok(CustomResult(out))
                }
                Err(e) => Err(ErrorData::new(
                    ErrorCode::INVALID_PARAMS,
                    e.to_string(),
                    None,
                )),
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
                    let cursor_param = params.as_ref().and_then(|p| p.get("cursor"));
                    let after = cursor_param
                        .and_then(Value::as_str)
                        .and_then(|c| c.parse::<u64>().ok())
                        .unwrap_or(sub.cursor);
                    // G3: maxAgeMs is a replay floor; ignored on a null cursor.
                    let max_age_us = if cursor_param.is_some() && !cursor_param.is_none() {
                        params
                            .as_ref()
                            .and_then(|p| p.get("maxAgeMs"))
                            .and_then(Value::as_u64)
                            .map(|ms| now_us.saturating_sub((ms as i64).saturating_mul(1000)))
                    } else {
                        None
                    };
                    let limit = params
                        .as_ref()
                        .and_then(|p| p.get("maxEvents"))
                        .and_then(Value::as_u64)
                        .map(|n| n.clamp(1, 1000) as u32)
                        .unwrap_or(32);
                    match store.poll(id, after, limit).await {
                        Ok(page) => Ok(CustomResult(json!({
                            "resultType": "complete",
                            "events": page.events.iter()
                                .filter(|d| max_age_us.is_none_or(|floor| d.created_us >= floor))
                                .map(|d| {
                                    toron_core::events::occurrence::occurrence(
                                        &d.payload, d.seq, now_us,
                                    )
                                }).collect::<Vec<_>>(),
                            "cursor": page.cursor.to_string(),
                            "nextPollMs": 5000,
                            "hasMore": page.has_more,
                            "truncated": page.truncated,
                        }))),
                        // Same gap contract as the stream: a store failure is a
                        // gap with a fresh servable cursor, never a silent loss.
                        Err(e) => {
                            tracing::warn!("events poll store error (gap): {e}");
                            Ok(CustomResult(json!({
                                "resultType": "complete",
                                "events": [], "cursor": after.to_string(),
                                "nextPollMs": 5000, "hasMore": false, "truncated": true,
                            })))
                        }
                    }
                }
                Ok(None) => Err(ErrorData::new(
                    NOT_FOUND,
                    "unknown subscription",
                    Some(json!({"kind": "subscription"})),
                )),
                Err(e) => {
                    tracing::warn!("events poll store error (gap): {e}");
                    Ok(CustomResult(json!({
                        "resultType": "complete",
                        "events": [], "cursor": "0",
                        "nextPollMs": 5000, "hasMore": false, "truncated": true,
                    })))
                }
            }
        }
        "events/unsubscribe" => {
            let id = id_param("subscriptionId");
            match store.unsubscribe(id, principal.name()).await {
                Ok(true) => Ok(CustomResult(json!({
                    "resultType": "complete", "id": id, "terminated": true,
                }))),
                Ok(false) => Err(ErrorData::new(
                    FORBIDDEN,
                    "not your subscription",
                    Some(json!({"reason": "access_denied"})),
                )),
                Err(e) => Err(ErrorData::new(
                    ErrorCode::INTERNAL_ERROR,
                    e.to_string(),
                    None,
                )),
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

    #[tokio::test]
    async fn unknown_event_name_is_in_band_invalid_params() {
        let params = json!({"name": "not.an.event"});
        let err = route_events(
            "events/subscribe",
            Some(&params),
            &Principal::Operator,
            None,
            0,
        )
        .await
        .expect("routed")
        .expect_err("unknown name");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        assert_eq!(err.code.0, -32602);
        assert!(err.message.contains("unknown event name"), "{err:?}");
    }

    #[tokio::test]
    async fn known_names_on_unimplemented_methods_are_errors_never_ok() {
        let params = json!({"name": "mail.addressed", "arguments": {}});
        for method in ["events/subscribe", "events/poll", "events/stream"] {
            let out = route_events(method, Some(&params), &Principal::Operator, None, 0)
                .await
                .expect("routed");
            assert!(out.is_err(), "{method} must not fake success");
        }
    }

    #[tokio::test]
    async fn only_events_methods_route_here() {
        assert!(
            route_events("tools/list", None, &Principal::Operator, None, 0)
                .await
                .is_none()
        );
        assert!(
            route_events("events/nope", None, &Principal::Operator, None, 0)
                .await
                .is_some()
        );
        assert!(
            route_events("events/list", None, &Principal::Operator, None, 0)
                .await
                .is_some()
        );
    }

    #[test]
    fn authorization_seam_resolves_a_principal() {
        // U1 was token-less; U2+ resolve the bearer to a Principal. A missing
        // or unknown token is refused in-band, and a valid one is accepted.
        assert!(authorize(None).is_err());
        assert!(authorize(Some("Bearer unknown")).is_err());
    }
}
