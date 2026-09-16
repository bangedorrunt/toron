// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Job lifecycle 43001–43006 (bd-yt2), Buzz-verbatim.
//!
//! kind-as-dispatch: a job is `43001` request → `43002` accepted/claim (signed
//! by the claiming agent, **first-claimer wins**) → `43003` progress (optional)
//! → `43004` result; `43005` cancel; `43006` error. The `kind` is the only
//! switch — new lifecycle states never renumber existing kinds (ADR-0003
//! frozen registry).
//!
//! Claim resolution on the InProcessBus is a **total order** — the first
//! observed `43002` for a `(job-id, e)` wins. Federated claim races (two
//! agents claim on different relays simultaneously) are a P2.2 problem when
//! RelayBus lands; this module documents that boundary.

use serde::{Deserialize, Serialize};

use nostr::prelude::{Event, EventId, Keys, Kind, Tag, Timestamp};

use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::{
    JOB_ACCEPTED, JOB_CANCEL, JOB_ERROR, JOB_PROGRESS, JOB_REQUEST, JOB_RESULT, toron_kind,
};

/// A job request payload (kind-43001 `content`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRequest {
    /// The job title/summary.
    pub title: String,
    /// Free-form job spec (JSON — e.g. `{"task": "fix flaky test"}`).
    #[serde(default)]
    pub spec: serde_json::Value,
    /// Assigned agent name (optional; when set the request targets that agent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
}

impl JobRequest {
    /// New request.
    pub fn new(title: impl Into<String>, spec: serde_json::Value) -> Self {
        Self {
            title: title.into(),
            spec,
            assignee: None,
        }
    }
}

/// A job result payload (kind-43004).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    /// Terminal status.
    pub status: String,
    /// Outcome body.
    #[serde(default)]
    pub body: String,
}

/// A job error payload (kind-43006).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobError {
    /// Error message.
    pub message: String,
}

/// Build a job request (kind-43001) with `["e", <parent>]` linking to the
/// triggering message (per beads-review: channel-agnostic; the parent may be a
/// kind-14 DM or kind-9 channel message).
pub fn build_job_request(
    requester: &Keys,
    parent: EventId,
    payload: &JobRequest,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    build_job_request_with_tags(requester, Some(parent), payload, created_at, Vec::new())
}

/// Build a job request with CALLER-supplied extra tags (ADR-0007 F6/F7: the
/// workflow host stamps coalesced 43001s with `toron:workflow-emitted`).
/// `parent` is optional — coalesced bursts may lack a parseable parent id.
pub fn build_job_request_with_tags(
    requester: &Keys,
    parent: Option<EventId>,
    payload: &JobRequest,
    created_at: Timestamp,
    extra_tags: Vec<Tag>,
) -> Result<Event, nostr::error::Error> {
    let mut tags: Vec<Tag> = Vec::new();
    if let Some(parent) = parent {
        tags.push(Tag::event(parent));
    }
    if let Some(assignee) = &payload.assignee {
        // Do NOT use `t` — ADR-0003 A3/G6 reserves `#t` for project scoping.
        // A `t=assignee-name` tag makes global bus subscribers drop the job
        // (InProcessBus access_gate). Use a free-form `assignee` tag instead.
        tags.push(Tag::parse(["assignee", assignee]).expect("assignee tag"));
    }
    tags.extend(extra_tags);
    build_event(
        requester,
        toron_kind(JOB_REQUEST),
        serde_json::to_string(payload).expect("job request serializes"),
        tags,
        created_at,
    )
}

/// Build a job claim (kind-43002) referencing the job request via `e`.
pub fn build_job_claim(
    claimer: &Keys,
    job_request: EventId,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    build_event(
        claimer,
        toron_kind(JOB_ACCEPTED),
        r#"{"claimed":true}"#,
        vec![Tag::event(job_request)],
        created_at,
    )
}

/// Build a job progress update (kind-43003, optional).
pub fn build_job_progress(
    agent: &Keys,
    job_request: EventId,
    note: &str,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    build_event(
        agent,
        toron_kind(JOB_PROGRESS),
        note.to_string(),
        vec![Tag::event(job_request)],
        created_at,
    )
}

/// Build a job result (kind-43004) closing the lifecycle.
pub fn build_job_result(
    agent: &Keys,
    job_request: EventId,
    result: &JobResult,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    build_event(
        agent,
        toron_kind(JOB_RESULT),
        serde_json::to_string(result).expect("job result serializes"),
        vec![Tag::event(job_request)],
        created_at,
    )
}

/// Build a job cancel (kind-43005).
pub fn build_job_cancel(
    canceller: &Keys,
    job_request: EventId,
    reason: &str,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    build_event(
        canceller,
        toron_kind(JOB_CANCEL),
        reason.to_string(),
        vec![Tag::event(job_request)],
        created_at,
    )
}

/// Build a job error (kind-43006) — failed-with-error terminal state.
pub fn build_job_error(
    agent: &Keys,
    job_request: EventId,
    error: &JobError,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    build_event(
        agent,
        toron_kind(JOB_ERROR),
        serde_json::to_string(error).expect("job error serializes"),
        vec![Tag::event(job_request)],
        created_at,
    )
}

/// The parent (job) event id a job-lifecycle event references (`e` tag).
pub fn job_parent(event: &Event) -> Option<EventId> {
    event
        .tags
        .iter()
        .find(|t| t.as_slice().first().map(String::as_str) == Some("e"))
        .and_then(|t| t.as_slice().get(1))
        .and_then(|s| EventId::from_hex(s).ok())
}

/// Resolve the claiming agent for a job: the author of the FIRST observed
/// kind-43002 referencing the job (InProcessBus total order → deterministic).
///
/// Returns `None` when no claim is present. A subsequent 43002 by another
/// agent is ignored (first-claimer wins); the function returns the first.
pub fn first_claimer(job: EventId, claims: &[Event]) -> Option<String> {
    claims
        .iter()
        .filter(|e| e.kind == toron_kind(JOB_ACCEPTED) && job_parent(e) == Some(job))
        .min_by_key(|e| (e.created_at.as_secs(), e.id.to_hex()))
        .map(|e| e.pubkey.to_hex())
}

/// The job's current state given the full event stream (request..result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobState {
    Open,
    Claimed(String),
    InProgress(String),
    Done(String),
    Cancelled,
    Failed(String),
}

/// Compute a job's state from the events referencing it. The lifecycle is:
/// request → (claim → progress*) → result|cancel|error. Terminal states win.
pub fn job_state(job: EventId, events: &[Event]) -> JobState {
    let job_events: Vec<&Event> = events
        .iter()
        .filter(|e| job_parent(e) == Some(job))
        .collect();

    // Terminal states take priority (result, error, cancel).
    for ev in &job_events {
        match ev.kind.as_u16() {
            JOB_RESULT => {
                let r: JobResult = serde_json::from_str(&ev.content).unwrap_or(JobResult {
                    status: "done".into(),
                    body: ev.content.clone(),
                });
                return JobState::Done(r.status);
            }
            JOB_ERROR => {
                let e: JobError = serde_json::from_str(&ev.content).unwrap_or(JobError {
                    message: "error".into(),
                });
                return JobState::Failed(e.message);
            }
            JOB_CANCEL => return JobState::Cancelled,
            _ => {}
        }
    }

    // Non-terminal: claim (and optional progress) drive the state.
    let job_events_owned: Vec<Event> = job_events.iter().map(|e| (*e).clone()).collect();
    first_claimer(job, &job_events_owned).map_or(JobState::Open, |claimer| {
        let has_progress = job_events.iter().any(|e| e.kind.as_u16() == JOB_PROGRESS);
        if has_progress {
            JobState::InProgress(claimer)
        } else {
            JobState::Claimed(claimer)
        }
    })
}

/// The kind-43001 request as JSON (wire-identical, relay-acceptable).
pub fn job_request_json(event: &Event) -> String {
    event.as_json()
}

/// Convenience: build a job request event and serialize (round-trip helper).
pub fn build_and_serialize(
    requester: &Keys,
    parent: EventId,
    payload: &JobRequest,
    created_at: Timestamp,
) -> Result<String, nostr::error::Error> {
    build_job_request(requester, parent, payload, created_at).map(|e| e.as_json())
}

// Keep Kind import used for the trait-bound docs.
#[allow(unused_imports)]
use Kind as _Kind;

/// The compose-request job spec shape (ADR-0022 §3.7 T4, W7): the `spec`
/// payload an agent mails when it wants a workflow composed on its behalf.
/// The loop honors it only from an allowlisted sender AND when `decide`
/// reports no catalog coverage — and in W7 renders the candidate FOR
/// APPROVAL, never auto-dispatching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComposeRequestSpec {
    /// What the composed workflow should accomplish.
    pub objective: String,
    /// Optional explicit parts (`role:stage` | bare stage | `workflow:name`).
    /// Absent = the loop derives parts from the archetype classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<String>>,
    /// The mailing agent (filled by the loop from the message sender; a
    /// self-claimed requester field is overridden).
    #[serde(default)]
    pub requester: String,
}

impl ComposeRequestSpec {
    /// Parse + validate a spec payload. `objective` must be non-empty;
    /// explicit parts must each be non-empty tokens.
    pub fn parse(spec: &serde_json::Value) -> Result<Self, String> {
        let parsed: Self =
            serde_json::from_value(spec.clone()).map_err(|e| format!("compose-request: {e}"))?;
        if parsed.objective.trim().is_empty() {
            return Err("compose-request: objective must be non-empty".to_string());
        }
        if let Some(parts) = &parsed.parts
            && parts.iter().any(|p| p.trim().is_empty())
        {
            return Err("compose-request: parts must be non-empty tokens".to_string());
        }
        Ok(parsed)
    }

    /// Serialize as the JobRequest.spec value.
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::prelude::{Keys, Timestamp};

    const TS: u64 = 1_700_000_000;

    fn keys() -> Keys {
        Keys::generate()
    }

    #[test]
    fn job_request_roundtrip_wire_identical() {
        let requester = keys();
        let parent =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id");
        let payload = JobRequest::new("fix flaky", serde_json::json!({"task": "triage"}));
        let ev =
            build_job_request(&requester, parent, &payload, Timestamp::from(TS)).expect("build");
        assert_eq!(ev.kind.as_u16(), JOB_REQUEST);
        assert_eq!(job_parent(&ev), Some(parent), "e tag links parent");
        let parsed = Event::from_json(ev.as_json()).expect("parse");
        assert_eq!(parsed, ev, "wire-identical roundtrip");
        assert!(
            toron_core::nostr::event::verify_event(&ev).is_ok(),
            "signed"
        );
    }

    #[test]
    fn claim_first_wins_on_total_order() {
        let job =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("job id");
        let a = keys();
        let b = keys();
        let claim_a = build_job_claim(&a, job, Timestamp::from(TS)).expect("claim a");
        let claim_b = build_job_claim(&b, job, Timestamp::from(TS + 1)).expect("claim b");

        // First observed claim (a) wins, regardless of insertion order.
        let winner = first_claimer(job, &[claim_b.clone(), claim_a.clone()]);
        assert_eq!(winner, Some(a.public_key().to_hex()));
        assert_eq!(
            first_claimer(job, &[claim_a, claim_b]),
            Some(a.public_key().to_hex())
        );
    }

    #[test]
    fn no_claim_means_open() {
        let job =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("job id");
        let requester = keys();
        let req = build_job_request(
            &requester,
            job,
            &JobRequest::new("x", serde_json::json!({})),
            Timestamp::from(TS),
        )
        .expect("req");
        assert_eq!(job_state(job, &[req]), JobState::Open);
    }

    #[test]
    fn lifecycle_reaches_result() {
        let job =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("job id");
        let requester = keys();
        let claimer = keys();
        let req = build_job_request(
            &requester,
            job,
            &JobRequest::new("x", serde_json::json!({})),
            Timestamp::from(TS),
        )
        .expect("req");
        let claim = build_job_claim(&claimer, job, Timestamp::from(TS + 1)).expect("claim");
        let progress = build_job_progress(&claimer, job, "working", Timestamp::from(TS + 2))
            .expect("progress");
        let result = build_job_result(
            &claimer,
            job,
            &JobResult {
                status: "ok".into(),
                body: "done".into(),
            },
            Timestamp::from(TS + 3),
        )
        .expect("result");

        let events = vec![req, claim, progress, result];
        assert_eq!(job_state(job, &events), JobState::Done("ok".into()));
    }

    #[test]
    fn error_is_terminal() {
        let job =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("job id");
        let requester = keys();
        let claimer = keys();
        let req = build_job_request(
            &requester,
            job,
            &JobRequest::new("x", serde_json::json!({})),
            Timestamp::from(TS),
        )
        .expect("req");
        let claim = build_job_claim(&claimer, job, Timestamp::from(TS + 1)).expect("claim");
        let err = build_job_error(
            &claimer,
            job,
            &JobError {
                message: "boom".into(),
            },
            Timestamp::from(TS + 2),
        )
        .expect("err");
        let events = vec![req, claim, err];
        assert_eq!(job_state(job, &events), JobState::Failed("boom".into()));
    }

    #[test]
    fn cancel_is_terminal() {
        let job =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("job id");
        let requester = keys();
        let req = build_job_request(
            &requester,
            job,
            &JobRequest::new("x", serde_json::json!({})),
            Timestamp::from(TS),
        )
        .expect("req");
        let cancel = build_job_cancel(&requester, job, "not needed", Timestamp::from(TS + 1))
            .expect("cancel");
        let events = vec![req, cancel];
        assert_eq!(job_state(job, &events), JobState::Cancelled);
    }

    #[test]
    fn claimed_and_in_progress_states() {
        let job =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("job id");
        let requester = keys();
        let claimer = keys();
        let req = build_job_request(
            &requester,
            job,
            &JobRequest::new("x", serde_json::json!({})),
            Timestamp::from(TS),
        )
        .expect("req");
        let claim = build_job_claim(&claimer, job, Timestamp::from(TS + 1)).expect("claim");
        assert_eq!(
            job_state(job, &[req.clone(), claim.clone()]),
            JobState::Claimed(claimer.public_key().to_hex())
        );
        let progress = build_job_progress(&claimer, job, "working", Timestamp::from(TS + 2))
            .expect("progress");
        assert_eq!(
            job_state(job, &[req, claim, progress]),
            JobState::InProgress(claimer.public_key().to_hex())
        );
    }

    /// W7 (§3.7 T4): the compose-request spec parses, validates, and
    /// round-trips through the JobRequest.spec value.
    #[test]
    fn compose_request_spec_parses_and_roundtrips() {
        let v = serde_json::json!({
            "objective": "map the crate and find bugs",
            "parts": ["fresh-eyes-explore", "reviewer:code-review"],
            "requester": "SandySparrow",
        });
        let spec = ComposeRequestSpec::parse(&v).expect("parses");
        assert_eq!(spec.objective, "map the crate and find bugs");
        assert_eq!(spec.parts.as_ref().unwrap().len(), 2);
        assert_eq!(spec.requester, "SandySparrow");
        // Round-trip: to_value -> parse is identity.
        let again = ComposeRequestSpec::parse(&spec.to_value()).expect("roundtrip");
        assert_eq!(again, spec);
        // Parts optional.
        let bare = ComposeRequestSpec::parse(&serde_json::json!({"objective": "x"}))
            .expect("parts optional");
        assert!(bare.parts.is_none());
    }

    /// W7: invalid compose-request specs refuse — empty objective, empty
    /// part token, wrong shape entirely.
    #[test]
    fn compose_request_spec_rejects_invalid() {
        assert!(
            ComposeRequestSpec::parse(&serde_json::json!({"objective": "  "})).is_err(),
            "empty objective refuses"
        );
        assert!(
            ComposeRequestSpec::parse(&serde_json::json!({
                "objective": "x", "parts": ["ok", "   "]
            }))
            .is_err(),
            "empty part token refuses"
        );
        assert!(
            ComposeRequestSpec::parse(&serde_json::json!({"task": "not a compose request"}))
                .is_err(),
            "wrong shape refuses"
        );
    }
}
