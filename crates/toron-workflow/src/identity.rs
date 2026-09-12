// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! Canonical workflow/action identity (ADR-0032 Spec 3, Jido Flow.Identity
//! as evidence only — no Jido dependency).
//!
//! Deterministic constructors for `workflow_digest`, `node_path`,
//! `iteration_key`, and `action_digest`, plus the stable effect-key inputs
//! consumed through the engine boundary's [`derive_effect_key`]. Identity
//! is computed over REDACTED canonical forms: secret values never enter a
//! digest. The first prepared identity is fixed: [`check_replay`] rejects a
//! re-render that would mint a different logical effect under the same
//! run/node instead of silently adopting it. Persistence of the prepared
//! row belongs to the wave-3 journal; this module owns the pure constructors
//! and the replay predicate.
//!
//! This module never claims provider idempotency or exactly-once execution:
//! a stable key is necessary for dedupe and reconciliation, never sufficient.

use sha2::{Digest, Sha256};

use crate::action_contract::{contract_for, validate_action};
use crate::engine::ActionErrorCode;
use crate::schema::Action;

/// Schema version bound into every digest (a contract change re-keys).
pub const SCHEMA_VERSION: &str = "toron-workflow/v1";

/// Canonical JSON: `serde_json::Map` is a `BTreeMap`, so serialization is
/// key-ordered and deterministic without extra work.
pub fn canonical_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// SHA-256 hex over `0x1f`-separated parts (same framing as the engine
/// boundary's effect-key derivation).
pub fn sha256_hex(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            h.update([0x1f]);
        }
        h.update(part.as_bytes());
    }
    hex::encode(h.finalize())
}

/// `workflow_digest = H(schema_version, canonical_workflow_definition)`.
/// The canonical definition includes declared inputs and defaults, so a
/// changed default re-keys every effect derived from the workflow.
pub fn workflow_digest(workflow: &crate::schema::Workflow) -> String {
    let canonical = serde_json::to_value(workflow)
        .map(|v| canonical_json(&v))
        .unwrap_or_default();
    sha256_hex(&[SCHEMA_VERSION, &canonical])
}

/// `node_path = authored_name` prefixed by composite ancestry (`/`-joined).
/// Top-level steps have empty ancestry.
pub fn node_path(ancestry: &[&str], name: &str) -> String {
    if ancestry.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", ancestry.join("/"), name)
    }
}

/// Stable iteration identity: `H(parent_collection_digest, index)`. An index
/// alone is not enough — the parent digest binds the collection the index
/// ran against, so a changed collection cannot reuse an old key.
pub fn iteration_key(parent_digest: &str, index: usize) -> String {
    sha256_hex(&[parent_digest, &index.to_string()])
}

/// Field names whose string values never enter a digest or evidence
/// projection (matched case-insensitively at any depth).
pub const SECRET_FIELD_NAMES: &[&str] = &[
    "token",
    "secret",
    "password",
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "private_key",
];

/// Redact secret-bearing string leaves to `"…[redacted]"`. Returns the
/// redacted value and whether anything was redacted (`secret_bearing`).
/// Matches [`SECRET_FIELD_NAMES`] case-insensitively at any depth; every
/// other leaf survives verbatim.
pub fn redact_value(value: &serde_json::Value) -> (serde_json::Value, bool) {
    fn redact_into(value: &serde_json::Value, bearing: &mut bool) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let mut out = serde_json::Map::with_capacity(map.len());
                for (k, v) in map {
                    let secret = SECRET_FIELD_NAMES
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(k));
                    if secret && v.is_string() {
                        *bearing = true;
                        out.insert(k.clone(), serde_json::Value::String("…[redacted]".into()));
                    } else {
                        out.insert(k.clone(), redact_into(v, bearing));
                    }
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(|v| redact_into(v, bearing)).collect())
            }
            _ => value.clone(),
        }
    }
    let mut bearing = false;
    let redacted = redact_into(value, &mut bearing);
    (redacted, bearing)
}

/// `action_digest = H(canonical redacted rendered action)`. Takes an
/// already-redacted canonical value (see [`normalize_action`); standalone
/// callers must redact first via [`redact_value`].
pub fn action_digest_of(redacted_canonical: &serde_json::Value) -> String {
    sha256_hex(&[SCHEMA_VERSION, &canonical_json(redacted_canonical)])
}

/// A rendered action in normal form: canonical redacted value, its digest,
/// and whether redaction applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAction {
    /// Canonical YAML type name.
    pub type_name: String,
    /// Canonical REDACTED rendered value (safe to persist as evidence).
    pub value: serde_json::Value,
    /// `H(canonical redacted value)`.
    pub digest: String,
    /// True when redaction removed at least one secret leaf.
    pub secret_bearing: bool,
}

/// Normalization failure: stage, stable code, human message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizationError {
    pub code: ActionErrorCode,
    pub message: String,
}

/// Normalize one RENDERED action value against its closed contract:
/// validate structure, redact secrets, canonicalize, digest. Rendering
/// itself stays engine-side; this is the contract gate on rendered values.
pub fn normalize_action(
    action: &Action,
    rendered: &serde_json::Value,
) -> Result<ResolvedAction, NormalizationError> {
    let contract = contract_for(action);
    if !contract.supported {
        return Err(NormalizationError {
            code: ActionErrorCode::UnsupportedAction,
            message: format!(
                "{:?} is deprecated with no wired sink; remove the step",
                contract.type_name
            ),
        });
    }
    let violations = validate_action(action);
    if let Some(first) = violations.first() {
        return Err(NormalizationError {
            code: first.code,
            message: format!("field {:?}: {}", first.field, first.message),
        });
    }
    let (value, secret_bearing) = redact_value(rendered);
    let digest = action_digest_of(&value);
    Ok(ResolvedAction {
        type_name: contract.type_name.to_string(),
        value,
        digest,
        secret_bearing,
    })
}

/// The first prepared identity for one logical node: fixed at `prepared`
/// time and reused for every retry and recovery of that node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedIdentity {
    pub node_path: String,
    pub action_digest: String,
    pub effect_key: String,
}

/// Replay predicate: a re-rendered digest for the same logical node must
/// equal the prepared digest. A mismatch is a version/replay error — the
/// caller enters explicit reconciliation, never mints a new logical effect
/// silently.
pub fn check_replay(prepared: &PreparedIdentity, redigest: &str) -> Result<(), String> {
    if prepared.action_digest == redigest {
        Ok(())
    } else {
        Err(format!(
            "node {:?}: re-render digest {redigest} != prepared {}; reconcile explicitly, do not mint a new effect",
            prepared.node_path, prepared.action_digest
        ))
    }
}

/// Stable effect keys derive through the engine boundary's
/// [`derive_effect_key`] (same inputs, excludes the retry attempt).
/// Re-exported here so contract/identity consumers share one path.
pub use crate::engine::derive_effect_key as effect_key;
