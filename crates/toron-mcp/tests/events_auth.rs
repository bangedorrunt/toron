// governed-by: ADR-0039

//! bd-6bgrd U2: the single events/* auth seam (D6 matrix, R13 tools token-less).
//! No live daemon, no operator key, hermetic in-memory token store.

use toron_core::events::principal::{
    FileTokenStore, Principal, Scope, TokenRecord, TokenStore, hash_token,
};

static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn store_with(record: TokenRecord) -> FileTokenStore {
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "toron-events-auth-{}-{}.json",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_file(&path);
    let store = FileTokenStore::at(path);
    store.insert(&record).expect("insert token");
    store
}

fn record(principal: Principal, scope: Scope, revoked: bool) -> TokenRecord {
    TokenRecord {
        hash: hash_token("a-valid-token"),
        principal,
        scope,
        created_us: 1000,
        revoked,
    }
}

#[test]
fn a_valid_pin_token_authorizes() {
    let store = store_with(record(
        Principal::Pin {
            pin: "AmberProbe".into(),
            project: "flywheel".into(),
        },
        Scope::for_pin("AmberProbe", "flywheel"),
        false,
    ));
    assert!(toron_mcp::events::authorize_with_store(
        Some("Bearer a-valid-token"),
        1000,
        &store
    )
    .is_ok());
}

#[test]
fn an_operator_token_authorizes() {
    let store = store_with(record(Principal::Operator, Scope::operator(), false));
    assert!(toron_mcp::events::authorize_with_store(
        Some("Bearer a-valid-token"),
        1000,
        &store
    )
    .is_ok());
}

#[test]
fn missing_unknown_and_revoked_tokens_are_in_band_errors() {
    let store = store_with(record(Principal::Operator, Scope::operator(), true));
    for header in [None, Some("Bearer nope"), Some("Basic xyz"), Some("Bearer a-valid-token")] {
        let err = toron_mcp::events::authorize_with_store(header, 1000, &store)
            .expect_err("must refuse a missing/unknown/revoked token");
        assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS);
        assert_eq!(err.code.0, -32602, "in-band error, never an HTTP 401");
        assert!(err.message.contains("unauthorized"), "{err:?}");
    }
}

#[test]
fn tools_stay_token_less_r13() {
    // The auth seam is events-only: `route` still declines tools/list, and the
    // tool handlers never pass through authorize.
    assert!(toron_mcp::events::route("tools/list", None).is_none());
    assert!(toron_mcp::events::route("tools/call", None).is_none());
}
