// governed-by: ADR-0024
// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! DI sink newtypes (F21.ENGINE-SPLIT move from the former single-file
//! engine): approval/spawn promise-id side-channels. Distinct newtypes (not
//! `type` aliases) so the host dependency map keys each by a unique
//! `TypeId`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Optional side-channel for approval promise-ids (embedding/test hook). When
/// a run parks on `request_approval`, its promise_id is pushed here so the
/// embedding can publish the kind-46011 pending event.
///
/// Distinct newtype (not a `type` alias): the host dependency map keys by
/// `TypeId` of the value type, so two `Arc<Mutex<Vec<String>>>` aliases would
/// collide and `get_dependency` would resolve both sinks to the LAST
/// registered — a real 0.6-migration regression (a1fe947). The newtype gives
/// each sink a unique `TypeId`; `Deref` keeps the `.lock().await` call sites
/// unchanged.
#[derive(Clone, Default)]
pub struct ApprovalSink(Arc<tokio::sync::Mutex<Vec<String>>>);

impl std::ops::Deref for ApprovalSink {
    type Target = tokio::sync::Mutex<Vec<String>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Side-channel for `spawn` promise-ids (ADR-0005 Spec 4a). When a run parks
/// on a spawn-await, its promise_id is pushed here so the embedding can emit
/// the kind-43001 job carrying it in the `spec`.
///
/// Distinct newtype — see [`ApprovalSink`] for why (TypeId collision in the
/// host dependency map).
#[derive(Clone, Default)]
pub struct SpawnSink(Arc<tokio::sync::Mutex<Vec<String>>>);

impl std::ops::Deref for SpawnSink {
    type Target = tokio::sync::Mutex<Vec<String>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Verify request passed to the embedding's `VerifySink` (ADR-0024 Spec 2).
#[derive(Debug, Clone)]
pub struct VerifyRequest {
    /// The shell command to verify (e.g. `cargo test -p toron-core ...`).
    pub command: String,
}

/// Verify result returned by the embedding (ADR-0024 Spec 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    pub ok: bool,
    pub exit: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The command's stdout PARSED as JSON, when it is JSON.
    ///
    /// bd-jhz8x.2 (ADR-0021 D4.5): a downstream step must be able to consume a
    /// verify step's data — `foreach over: "{{steps.gate.json.ready}}"` is how
    /// wave-dispatch stops being a planner seat. `stdout_path` is a file, and a
    /// template reference cannot read a file, so the embedding carries the
    /// parsed value here. `None` means "not JSON" — never an empty object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json: Option<serde_json::Value>,
}

/// Embedding-provided VerifySink (ADR-0024 Spec 2). Unique TypeId, same pattern
/// as `ApprovalSink`/`SpawnSink`. `None` → `{ok:false, error:"verify sink missing"}`.
#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct VerifySink(
    pub  Arc<
        std::sync::Mutex<
            Option<Arc<dyn Fn(&VerifyRequest) -> anyhow::Result<VerifyResult> + Send + Sync>>,
        >,
    >,
);

impl std::fmt::Debug for VerifySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifySink")
            .field(
                "has_sink",
                &self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some(),
            )
            .finish()
    }
}

impl VerifySink {
    /// Execute one verify command through the embedding's sink (ADR-0024
    /// Spec 2) and project the result into the leaf's `{ok,…}` value.
    ///
    /// The sink owns BOTH halves of its contract — running the command and
    /// shaping the outcome — so the projection cannot drift between callers
    /// (the journaling effect path and any sink-direct path both call this).
    /// A missing sink, a serialization failure, and a sink error are all the
    /// same shape: `{ok:false, error:"…"}`, never a panic and never `ok:true`.
    ///
    /// "Missing sink" is deliberately reported here (not pre-checked by the
    /// caller) because it is the sink's own contract: `None` → `{ok:false,
    /// error:"verify sink missing"}`.
    pub fn project(&self, command: &str) -> serde_json::Value {
        let guard = match self.0.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let Some(f) = guard else {
            return serde_json::json!({ "ok": false, "error": "verify sink missing" });
        };
        let req = VerifyRequest {
            command: command.to_string(),
        };
        match f(&req) {
            Ok(res) => serde_json::to_value(&res)
                .unwrap_or_else(|e| serde_json::json!({ "ok": false, "error": e.to_string() })),
            Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
        }
    }
}

/// Which parked control produced the promise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParkKind {
    Approval,
    Spawn,
}

impl ParkKind {
    /// Canonical wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::Spawn => "spawn",
        }
    }
}

/// Maximum bytes kept in [`PendingEntry::question`].
pub const MAX_PENDING_QUESTION: usize = 512;

/// One structured parked promise (ADR-0032 Spec 6.1): everything the host
/// needs to publish the pending event, validate the resolution, and recover
/// after a restart. Identity is `(run_id, promise_id, generation)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntry {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub workflow: String,
    pub node_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_key: Option<String>,
    pub promise_id: String,
    pub generation: u64,
    pub kind: ParkKind,
    /// Bounded approval question / spawn summary (truncated, never a blob).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<String>,
    /// Bounded evidence artifact paths (never blobs).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_secs: Option<u64>,
    /// Full approval evidence (the graph `build_approval_payload`: nodes
    /// completed/failed/pending, artifacts, ledger path, blast radius,
    /// timeout, evidence hash). `None` on the linear path, whose kind-46011
    /// body stays exactly as it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

impl PendingEntry {
    /// Build an entry; the question is truncated to
    /// [`MAX_PENDING_QUESTION`] bytes.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        run_id: impl Into<String>,
        project: Option<String>,
        workflow: impl Into<String>,
        node_path: impl Into<String>,
        effect_key: Option<String>,
        promise_id: impl Into<String>,
        generation: u64,
        kind: ParkKind,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            project,
            workflow: workflow.into(),
            node_path: node_path.into(),
            effect_key,
            promise_id: promise_id.into(),
            generation,
            kind,
            question: None,
            artifacts: Vec::new(),
            evidence_hash: None,
            deadline_secs: None,
            payload: None,
        }
    }

    /// Attach a bounded question/summary payload.
    pub fn with_question(mut self, question: impl Into<String>) -> Self {
        let mut q: String = question.into();
        if q.len() > MAX_PENDING_QUESTION {
            q.truncate(MAX_PENDING_QUESTION);
            q.push_str("…[truncated]");
        }
        self.question = Some(q);
        self
    }

    /// Registry identity: `(run_id, promise_id, generation)`.
    pub fn key(&self) -> (String, String, u64) {
        (
            self.run_id.clone(),
            self.promise_id.clone(),
            self.generation,
        )
    }
}

/// Host-side registry of parked entries. Observation is idempotent on the
/// entry key: re-draining the same live sinks (e.g. after a restart)
/// reconstructs the identical registry instead of duplicating it.
#[derive(Debug, Default)]
pub struct PendingRegistry {
    entries: std::collections::HashMap<(String, String, u64), PendingEntry>,
}

impl PendingRegistry {
    /// Observe one entry. Returns `true` when it is new, `false` when the
    /// identical key was already recorded (duplicate observation).
    pub fn observe(&mut self, entry: PendingEntry) -> bool {
        use std::collections::hash_map::Entry as MapEntry;
        match self.entries.entry(entry.key()) {
            MapEntry::Occupied(_) => false,
            MapEntry::Vacant(v) => {
                v.insert(entry);
                true
            }
        }
    }

    /// Look up one parked entry by full identity.
    pub fn get(&self, run_id: &str, promise_id: &str, generation: u64) -> Option<&PendingEntry> {
        self.entries
            .get(&(run_id.to_string(), promise_id.to_string(), generation))
    }

    /// Look up the newest parked generation of a promise in a run, for
    /// resolution validation: a claimed older generation must report
    /// `StaleGeneration` (not `UnknownPromise`) so re-parked promises
    /// resolve only at their current generation.
    pub fn get_latest(&self, run_id: &str, promise_id: &str) -> Option<&PendingEntry> {
        self.entries
            .iter()
            .filter(|((r, p, _), _)| r == run_id && p == promise_id)
            .max_by_key(|((_, _, g), _)| *g)
            .map(|(_, e)| e)
    }

    /// Remove (on validated resolution) and return the entry.
    pub fn remove(
        &mut self,
        run_id: &str,
        promise_id: &str,
        generation: u64,
    ) -> Option<PendingEntry> {
        self.entries
            .remove(&(run_id.to_string(), promise_id.to_string(), generation))
    }

    /// Number of parked entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// True when any generation of this promise is parked.
    pub fn is_known_promise(&self, promise_id: &str) -> bool {
        self.entries.keys().any(|(_, p, _)| p == promise_id)
    }

    /// All entries for one run.
    pub fn entries_for_run(&self, run_id: &str) -> Vec<&PendingEntry> {
        self.entries
            .values()
            .filter(|e| e.run_id == run_id)
            .collect()
    }
}

/// How a parked promise was settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionVerdict {
    Approved,
    Rejected,
    SpawnCompleted,
}

impl ResolutionVerdict {
    /// Canonical wire spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::SpawnCompleted => "spawn_completed",
        }
    }

    /// Closed parse: only the three canonical spellings.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "approved" => Some(Self::Approved),
            "rejected" => Some(Self::Rejected),
            "spawn_completed" => Some(Self::SpawnCompleted),
            _ => None,
        }
    }
}

/// A claimed settlement of one parked promise, as parsed from a resolution
/// event (kind-46011 approval/rejection echoing the promise id).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resolution {
    pub run_id: String,
    /// Node the decision belongs to. Absent on the linear path (one park per
    /// run); a graph wave can park several approvals at once, so the host keys
    /// its registry by `(run_id, node)` and falls back to the single run entry
    /// when this is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub promise_id: String,
    pub generation: u64,
    pub verdict: ResolutionVerdict,
    #[serde(default)]
    pub note: String,
}

/// Why a claimed resolution must not resume a run (ADR-0032 Spec 6.2:
/// duplicate resolutions are harmless, stale/wrong ones are recorded and
/// ignored — a stale resolution must never resume another node or run).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionMismatch {
    /// No parked entry for this promise at all.
    UnknownPromise,
    /// The promise is parked under a different run — never cross-resume.
    WrongRun { expected: String },
    /// Project scope does not match the parked entry.
    WrongProject { expected: Option<String> },
    /// An older generation of a re-parked promise — the newer park owns it.
    StaleGeneration { expected: u64 },
}

/// Validate a claimed resolution against the parked entry (if any).
/// `None` entry → [`ResolutionMismatch::UnknownPromise`].
pub fn validate_resolution(
    entry: Option<&PendingEntry>,
    resolution: &Resolution,
) -> Result<(), ResolutionMismatch> {
    let Some(entry) = entry else {
        return Err(ResolutionMismatch::UnknownPromise);
    };
    if entry.run_id != resolution.run_id {
        return Err(ResolutionMismatch::WrongRun {
            expected: entry.run_id.clone(),
        });
    }
    if entry.project != resolution.project {
        return Err(ResolutionMismatch::WrongProject {
            expected: entry.project.clone(),
        });
    }
    if entry.generation != resolution.generation {
        return Err(ResolutionMismatch::StaleGeneration {
            expected: entry.generation,
        });
    }
    Ok(())
}

/// One semantic terminal identity (ADR-0032 Spec 5/6.2): canonical hash of
/// run id, workflow digest, terminal status, node, and evidence reference.
/// Event delivery may be at-least-once; consumers dedupe this identity.
pub fn terminal_digest(
    run_id: &str,
    workflow_digest: &str,
    status: &str,
    node: &str,
    evidence: &str,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for part in [run_id, workflow_digest, status, node, evidence] {
        h.update(part.as_bytes());
        h.update([0x1f]);
    }
    hex::encode(h.finalize())
}

/// One committed semantic terminal for a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRecord {
    pub run_id: String,
    pub digest: String,
    pub status: String,
    pub node: String,
}

/// How [`TerminalLedger::note`] classified a terminal publication attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalNote {
    /// First publication of this digest — publish it.
    New,
    /// Same digest republished — no-op, no second semantic terminal.
    Duplicate,
    /// A different digest for an already-terminal run — conflict requiring
    /// evidence, recorded but never silently merged.
    Conflict { existing_digest: String },
}

/// Host-side terminal dedupe: one semantic terminal per digest; republication
/// of the same digest is a no-op; a different digest for the same run is an
/// explicit conflict.
#[derive(Debug, Default)]
pub struct TerminalLedger {
    by_digest: std::collections::HashMap<String, TerminalRecord>,
    by_run: std::collections::HashMap<String, String>,
}

impl TerminalLedger {
    /// Record a terminal publication attempt.
    pub fn note(&mut self, record: TerminalRecord) -> TerminalNote {
        if self.by_digest.contains_key(&record.digest) {
            return TerminalNote::Duplicate;
        }
        if let Some(existing) = self.by_run.get(&record.run_id) {
            return TerminalNote::Conflict {
                existing_digest: existing.clone(),
            };
        }
        self.by_run
            .insert(record.run_id.clone(), record.digest.clone());
        self.by_digest.insert(record.digest.clone(), record);
        TerminalNote::New
    }

    /// Look up a published terminal by digest.
    pub fn get(&self, digest: &str) -> Option<&TerminalRecord> {
        self.by_digest.get(digest)
    }
}

/// Attribute newly observed promise ids to schema candidate nodes in order
/// (ADR-0032 Spec 6.1 generation discipline). Returns `(node, promise_id)`
/// pairs; the caller assigns per-node generations from its own counters.
/// Counts must match exactly — a mismatch returns empty (never guess which
/// promise belongs to which node; the caller warns and retries next drain).
pub fn attribute_parks(candidates: &[String], new_ids: &[String]) -> Vec<(String, String)> {
    if candidates.len() != new_ids.len() || candidates.is_empty() {
        return Vec::new();
    }
    candidates
        .iter()
        .zip(new_ids.iter())
        .map(|(node, id)| (node.clone(), id.clone()))
        .collect()
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    fn approval_entry() -> PendingEntry {
        PendingEntry::new(
            "run-1",
            None,
            "ship",
            "gate",
            None,
            "promise-1",
            1,
            ParkKind::Approval,
        )
    }

    /// bd-asx3: duplicate sink observations are idempotent — observing the
    /// same (run, promise, generation) twice records one entry.
    #[test]
    fn host_lifecycle_registry_dedupes_duplicate_observations() {
        let mut reg = PendingRegistry::default();
        assert!(reg.observe(approval_entry()), "first observation is new");
        assert!(!reg.observe(approval_entry()), "duplicate is idempotent");
        assert_eq!(reg.len(), 1);
        assert!(reg.is_known_promise("promise-1"));
        assert!(!reg.is_known_promise("promise-9"));
        let mut second_gen = approval_entry();
        second_gen.generation = 2;
        assert!(reg.observe(second_gen), "next generation is a new entry");
        assert_eq!(reg.len(), 2);
    }

    /// bd-asx3: resolution validates run/project/promise/generation/action
    /// identity — matches resolve, everything else is a typed mismatch.
    #[test]
    fn host_lifecycle_resolution_validation() {
        let entry = approval_entry();
        let ok = Resolution {
            run_id: "run-1".into(),
            node: None,
            project: None,
            promise_id: "promise-1".into(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: "ship it".into(),
        };
        assert!(validate_resolution(Some(&entry), &ok).is_ok());
        assert!(matches!(
            validate_resolution(None, &ok),
            Err(ResolutionMismatch::UnknownPromise)
        ));
        let wrong_run = Resolution {
            run_id: "run-2".into(),
            node: None,
            ..ok.clone()
        };
        assert!(matches!(
            validate_resolution(Some(&entry), &wrong_run),
            Err(ResolutionMismatch::WrongRun { .. })
        ));
        let stale = Resolution {
            generation: 0,
            ..ok.clone()
        };
        assert!(matches!(
            validate_resolution(Some(&entry), &stale),
            Err(ResolutionMismatch::StaleGeneration { expected: 1 })
        ));
        let wrong_project = Resolution {
            project: Some("other".into()),
            ..ok.clone()
        };
        assert!(matches!(
            validate_resolution(Some(&entry), &wrong_project),
            Err(ResolutionMismatch::WrongProject { .. })
        ));
        let mut scoped = approval_entry();
        scoped.project = Some("demo".into());
        let cross = Resolution {
            project: None,
            ..ok
        };
        assert!(matches!(
            validate_resolution(Some(&scoped), &cross),
            Err(ResolutionMismatch::WrongProject { .. })
        ));
    }

    /// bd-asx3: terminal ledger — first publication is new, same-digest
    /// republish is a duplicate no-op, a different digest for the same run
    /// is a conflict requiring evidence.
    #[test]
    fn host_lifecycle_terminal_ledger_dedupe_and_conflict() {
        let mut ledger = TerminalLedger::default();
        let digest = terminal_digest("run-1", "d", "succeeded", "gate", "ev");
        let rec = TerminalRecord {
            run_id: "run-1".into(),
            digest,
            status: "succeeded".into(),
            node: "gate".into(),
        };
        assert!(matches!(ledger.note(rec.clone()), TerminalNote::New));
        assert!(matches!(ledger.note(rec), TerminalNote::Duplicate));
        let other = TerminalRecord {
            run_id: "run-1".into(),
            digest: terminal_digest("run-1", "d", "failed", "gate", "ev2"),
            status: "failed".into(),
            node: "gate".into(),
        };
        assert!(matches!(ledger.note(other), TerminalNote::Conflict { .. }));
    }

    /// bd-asx3: park attribution zips candidates to new promise ids in
    /// order — and refuses to guess when counts mismatch.
    #[test]
    fn host_lifecycle_attribute_parks_zips_in_order_and_rejects_mismatch() {
        let got = attribute_parks(
            &["gate".into(), "second".into()],
            &["p1".into(), "p2".into()],
        );
        assert_eq!(
            got,
            vec![
                ("gate".to_string(), "p1".to_string()),
                ("second".to_string(), "p2".to_string()),
            ]
        );
        assert!(attribute_parks(&["gate".into()], &["p1".into(), "p2".into()]).is_empty());
        assert!(attribute_parks(&["gate".into()], &[]).is_empty());
    }
}

#[cfg(test)]
mod verify_result_json_tests {
    use super::VerifyResult;

    /// bd-jhz8x.2: the parsed-JSON carrier is optional and is OMITTED when the
    /// command printed no JSON — a consumer must be able to tell "not JSON"
    /// from "an empty object", so a `None` never serializes as `null`.
    #[test]
    fn a_verify_result_without_json_omits_the_field() {
        let plain = VerifyResult {
            ok: true,
            exit: 0,
            stdout_path: Some("/tmp/out".into()),
            stderr_path: None,
            error: None,
            json: None,
        };
        let text = serde_json::to_string(&plain).expect("serializes");
        assert!(!text.contains("json"), "an absent parse is omitted: {text}");

        let parsed = VerifyResult {
            json: Some(serde_json::json!({ "ready": [] })),
            ..plain
        };
        let text = serde_json::to_string(&parsed).expect("serializes");
        assert!(
            text.contains("\"ready\""),
            "the parsed value rides along: {text}"
        );
        let back: VerifyResult = serde_json::from_str(&text).expect("round-trips");
        assert_eq!(back.json, parsed.json);
    }
}
