// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! Operator-minted bearer tokens for `events/*` (ADR-0039 D6, bd-6bgrd).
//!
//! A token is 32 random bytes shown once at mint. Only its SHA-256 hash is
//! stored, in the durable root, with its principal, scope, creation and
//! revocation. Tools stay token-less (R13): resolution is the events router's
//! job, never the tool-call path's.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const TOKEN_BYTES: usize = 32;
pub const MAX_PIN_TTL_US: i64 = 24 * 60 * 60 * 1_000_000;

/// The one identity behind an `events/*` call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Principal {
    Pin { pin: String, project: String },
    Operator,
}

impl Principal {
    pub fn name(&self) -> &str {
        match self {
            Self::Pin { pin, .. } => pin,
            Self::Operator => "operator",
        }
    }
}

/// What a token may reach. `None` on pin/project/ttl means unrestricted —
/// only an operator token is allowed to carry those (D6 matrix).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub pin: Option<String>,
    pub project: Option<String>,
    pub ttl_us: Option<i64>,
    pub full_payload: bool,
}

impl Scope {
    /// A per-pin token: own pin and project, ≤24h, hint payload only.
    pub fn for_pin(pin: impl Into<String>, project: impl Into<String>) -> Self {
        Self {
            pin: Some(pin.into()),
            project: Some(project.into()),
            ttl_us: Some(MAX_PIN_TTL_US),
            full_payload: false,
        }
    }

    /// The operator token: any pin/project, no expiry, full payload.
    pub fn operator() -> Self {
        Self {
            pin: None,
            project: None,
            ttl_us: None,
            full_payload: true,
        }
    }
}

/// A stored token record. `hash` is hex(SHA-256(plaintext)); no plaintext is
/// ever persisted or logged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRecord {
    pub hash: String,
    pub principal: Principal,
    pub scope: Scope,
    pub created_us: i64,
    pub revoked: bool,
}

/// The durable store for token records. Kept a trait so the router and CLI can
/// be tested against an in-memory fake without touching the real root.
pub trait TokenStore: Send + Sync {
    fn insert(&self, record: &TokenRecord) -> anyhow::Result<()>;
    fn get(&self, hash: &str) -> anyhow::Result<Option<TokenRecord>>;
    fn list(&self) -> anyhow::Result<Vec<TokenRecord>>;
    fn revoke(&self, hash: &str) -> anyhow::Result<()>;
}

/// JSON file at 0600 under the durable root. One file is enough: mint/revoke
/// are CLI-only and infrequent, and every write is atomic via a temp + rename.
pub struct FileTokenStore {
    path: PathBuf,
}

impl FileTokenStore {
    pub fn at_root(root: &Path) -> Self {
        Self {
            path: root.join("events_tokens.json"),
        }
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

fn read(path: &Path) -> anyhow::Result<Vec<TokenRecord>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn write(path: &Path, records: &[TokenRecord]) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(records)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

impl TokenStore for FileTokenStore {
    fn insert(&self, record: &TokenRecord) -> anyhow::Result<()> {
        let mut records = read(&self.path)?;
        if records.iter().any(|r| r.hash == record.hash) {
            anyhow::bail!("token hash already present");
        }
        records.push(record.clone());
        write(&self.path, &records)
    }

    fn get(&self, hash: &str) -> anyhow::Result<Option<TokenRecord>> {
        Ok(read(&self.path)?.into_iter().find(|r| r.hash == hash))
    }

    fn list(&self) -> anyhow::Result<Vec<TokenRecord>> {
        read(&self.path)
    }

    fn revoke(&self, hash: &str) -> anyhow::Result<()> {
        let mut records = read(&self.path)?;
        let Some(record) = records.iter_mut().find(|r| r.hash == hash) else {
            anyhow::bail!("unknown token");
        };
        record.revoked = true;
        write(&self.path, &records)
    }
}

/// Hash a plaintext token exactly as it is stored.
pub fn hash_token(plaintext: &str) -> String {
    hex::encode(Sha256::digest(plaintext.as_bytes()))
}

/// Generate a fresh 32-byte token and persist its hash. The plaintext is
/// returned ONCE to the caller (CLI prints it once at mint) and never stored.
pub fn mint(
    principal: Principal,
    scope: Scope,
    now_us: i64,
    store: &dyn TokenStore,
) -> anyhow::Result<String> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("token rng failed: {e}"))?;
    let plaintext = hex::encode(bytes);
    let record = TokenRecord {
        hash: hash_token(&plaintext),
        principal,
        scope,
        created_us: now_us,
        revoked: false,
    };
    store.insert(&record)?;
    Ok(plaintext)
}

/// Resolve an Authorization header to a live principal. Unknown, missing,
/// revoked and expired tokens are all `None` — the caller turns that into the
/// events 200-body unauthorized error, never an HTTP 401.
pub fn resolve(
    authorization: Option<&str>,
    now_us: i64,
    store: &dyn TokenStore,
) -> Option<Principal> {
    let raw = authorization?.strip_prefix("Bearer ")?;
    let record = store.get(&hash_token(raw.trim())).ok()??;
    if record.revoked {
        return None;
    }
    if let Some(ttl) = record.scope.ttl_us
        && now_us.saturating_sub(record.created_us) > ttl
    {
        return None;
    }
    Some(record.principal)
}

/// The D6 capability matrix. One pure decision per (verb, principal) pair, so
/// the ADR table can be checked by one table-driven test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventsVerb {
    List,
    SubscribePoll,
    SubscribeWebhook,
    CrossScope,
    FullPayload,
    MintRevoke,
    AllowlistEdit,
}

pub fn authorize(principal: &Principal, verb: EventsVerb) -> bool {
    match verb {
        EventsVerb::List | EventsVerb::SubscribePoll | EventsVerb::SubscribeWebhook => true,
        EventsVerb::CrossScope
        | EventsVerb::FullPayload
        | EventsVerb::MintRevoke
        | EventsVerb::AllowlistEdit => matches!(principal, Principal::Operator),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeStore(Mutex<Vec<TokenRecord>>);
    impl TokenStore for FakeStore {
        fn insert(&self, record: &TokenRecord) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(record.clone());
            Ok(())
        }
        fn get(&self, hash: &str) -> anyhow::Result<Option<TokenRecord>> {
            Ok(self.0.lock().unwrap().iter().find(|r| r.hash == hash).cloned())
        }
        fn list(&self) -> anyhow::Result<Vec<TokenRecord>> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn revoke(&self, hash: &str) -> anyhow::Result<()> {
            let mut rows = self.0.lock().unwrap();
            rows.iter_mut().find(|r| r.hash == hash).unwrap().revoked = true;
            Ok(())
        }
    }

    #[test]
    fn a_pin_token_resolves_to_a_pin_principal() {
        let store = FakeStore::default();
        let plaintext = mint(
            Principal::Pin {
                pin: "AmberProbe".into(),
                project: "flywheel".into(),
            },
            Scope::for_pin("AmberProbe", "flywheel"),
            1000,
            &store,
        )
        .unwrap();
        assert_eq!(plaintext.len(), TOKEN_BYTES * 2);
        assert_eq!(
            resolve(Some(&format!("Bearer {plaintext}")), 1000, &store),
            Some(Principal::Pin {
                pin: "AmberProbe".into(),
                project: "flywheel".into()
            })
        );
        let rows = store.list().unwrap();
        assert_eq!(rows.len(), 1);
        assert_ne!(rows[0].hash, plaintext, "plaintext must never be stored");
    }

    #[test]
    fn an_operator_token_resolves_to_the_operator_principal() {
        let store = FakeStore::default();
        let plaintext = mint(Principal::Operator, Scope::operator(), 1000, &store).unwrap();
        assert_eq!(
            resolve(Some(&format!("Bearer {plaintext}")), 1000, &store),
            Some(Principal::Operator)
        );
    }

    #[test]
    fn missing_unknown_revoked_and_expired_tokens_do_not_resolve() {
        let store = FakeStore::default();
        let plaintext = mint(
            Principal::Pin {
                pin: "P".into(),
                project: "f".into(),
            },
            Scope::for_pin("P", "f"),
            1000,
            &store,
        )
        .unwrap();
        assert_eq!(resolve(None, 1000, &store), None);
        assert_eq!(resolve(Some("Bearer nope"), 1000, &store), None);
        assert_eq!(resolve(Some("Basic abc"), 1000, &store), None);
        assert_eq!(
            resolve(Some(&format!("Bearer {plaintext}")), 1000 + MAX_PIN_TTL_US + 1, &store),
            None,
            "pin token expires after 24h"
        );
        let hash = hash_token(&plaintext);
        store.revoke(&hash).unwrap();
        assert_eq!(resolve(Some(&format!("Bearer {plaintext}")), 1000, &store), None);
    }

    #[test]
    fn an_operator_token_never_expires() {
        let store = FakeStore::default();
        let plaintext = mint(Principal::Operator, Scope::operator(), 1000, &store).unwrap();
        assert_eq!(
            resolve(Some(&format!("Bearer {plaintext}")), i64::MAX / 2, &store),
            Some(Principal::Operator)
        );
    }

    #[test]
    fn the_d6_matrix_is_enforced_in_one_table() {
        let pin = Principal::Pin {
            pin: "P".into(),
            project: "f".into(),
        };
        let operator = Principal::Operator;
        // (verb, pin allowed, operator allowed) — ADR-0039 D6.
        const MATRIX: &[(EventsVerb, bool, bool)] = &[
            (EventsVerb::List, true, true),
            (EventsVerb::SubscribePoll, true, true),
            (EventsVerb::SubscribeWebhook, true, true),
            (EventsVerb::CrossScope, false, true),
            (EventsVerb::FullPayload, false, true),
            (EventsVerb::MintRevoke, false, true),
            (EventsVerb::AllowlistEdit, false, true),
        ];
        for (verb, pin_ok, op_ok) in MATRIX {
            assert_eq!(authorize(&pin, *verb), *pin_ok, "pin on {verb:?}");
            assert_eq!(authorize(&operator, *verb), *op_ok, "operator on {verb:?}");
        }
    }

    #[test]
    fn a_pin_scope_cannot_request_operator_capabilities() {
        let scope = Scope::for_pin("P", "f");
        assert!(scope.pin.is_some() && scope.project.is_some());
        assert!(scope.ttl_us.is_some_and(|t| t <= MAX_PIN_TTL_US));
        assert!(!scope.full_payload);
    }
}
