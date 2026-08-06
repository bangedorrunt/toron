// governed-by: ADR-0004
#![forbid(unsafe_code)]

//! Workflow loop-prevention (bd-nsv, ADR-0004 Ship 2): the trigger-ingress
//! exclusion filter.
//!
//! ## Why
//!
//! A workflow whose action emits a bus event can re-trigger ITSELF if the
//! trigger filter doesn't exclude the workflow's own emitted events. The
//! canonical infinite loop:
//!
//! ```text
//! message_posted → workflow → send_message → message_posted → workflow → …
//! ```
//!
//! Buzz (`buzz-workflow`) prevents this explicitly: its `wf.on_event` excludes
//! workflow-execution kinds (Buzz 46001–46012), relay-signed `buzz:workflow`-
//! tagged events, and `KIND_GIFT_WRAP`. toron had **no such filter** — the
//! engine's `run_workflow` takes a pre-deserialized `RunInput { trigger }`, so
//! the embedding decides which events become trigger payloads, and nothing
//! filtered out workflow/job/gift-wrap emissions.
//!
//! ## What (ADR-0004 invariant 6: loop-prevention at ingress, not projection)
//!
//! A pure predicate [`should_evaluate_trigger_event`] the embedding calls
//! BEFORE evaluating a workflow's trigger spec against an event. It rejects:
//!
//! 1. **Job kinds 43001–43006** — a job request/result/progress/etc. is the
//!    output of `dispatch_job`, not a user action; triggering on it loops.
//! 2. **Workflow-run kinds 46001–46012** — the run/step/approval event stream
//!    itself; triggering on it loops.
//! 3. **Gift-wrap kind 1059 (NIP-59)** — the envelope wrapping DMs; a
//!    `send_message` emits gift wraps, and triggering on them loops. (The
//!    unwrapped rumor kind-14 is the legitimate `message_posted` source — but
//!    only recipients see it post-unwrap, which is not a workflow-trigger
//!    path.)
//! 4. **Events tagged `toron:workflow-emitted`** — the defense-in-depth belt:
//!    the engine tags its own action outputs with this single-`t` marker so
//!    the filter rejects them by tag, not just by kind. This survives future
//!    kind additions and catches workflow-emitted kind-14/15/7 events (a
//!    `send_message` action emits kind-14, which is otherwise a legitimate
//!    `message_posted` trigger source for user-authored messages).
//!
//! ## Where it sits
//!
//! ```text
//!   bus event arrives
//!     ↓
//!   embedding: should_evaluate_trigger_event(&event)?
//!     ├─ false → SKIP trigger evaluation (no workflow runs)
//!     └─ true  → match against Trigger spec → run_workflow(RunInput{…})
//! ```
//!
//! The filter is **pure + sync + allocation-light** (kind check + one tag
//! scan). It is the only correct layer for loop-prevention: excluding at the
//! projection (G14 contact-policy) is too late — the loop has already fired.
//!
//! ## What this filter does NOT do
//!
//! - It does NOT exclude kind-14 (DM) or kind-9 (channel message) wholesale —
//!   legitimate user-authored messages MUST still trigger `message_posted`.
//!   Only workflow-emitted ones are excluded (via the `toron:workflow-emitted`
//!   tag, set by the engine's action runner on its outputs).
//! - It does NOT exclude kind-7 (reaction) wholesale — same reason.
//! - It does NOT do project/channel scoping (that's the `Trigger` spec's job).
//! - It does NOT deduplicate redelivered events (that's the engine's
//!   idempotency-key job, already handled by `run_id`).

use nostr_sdk::nostr::Event;

use toron_core::nostr::kinds::{
    JOB_ACCEPTED, JOB_CANCEL, JOB_ERROR, JOB_PROGRESS, JOB_REQUEST, JOB_RESULT,
    WORKFLOW_RUN_FIRST, WORKFLOW_RUN_LAST,
};

/// NIP-59 gift wrap kind. A send_message emits these; triggering on them
/// loops. (rust-nostr exposes this as a constant in newer versions; we pin it
/// locally to avoid a version-coupled import.)
pub const GIFT_WRAP: u16 = 1059;

/// The single-`t` tag value the engine stamps on its own action outputs so the
/// ingress filter can reject them by tag (defense in depth beyond kind
/// checks — survives future kind additions and catches workflow-emitted
/// kind-14/15/7 events that are otherwise legitimate trigger sources).
pub const WORKFLOW_EMITTED_TAG: &str = "toron:workflow-emitted";

/// Job kinds 43001–43006 (dispatch outputs). Sourced from the registry, not
/// hand-maintained. (The registry exposes JOB_REQUEST..JOB_ERROR individually;
/// the range JOB_RUN_FIRST..=JOB_RUN_LAST is the contiguous envelope.)
pub const JOB_EMITS_KINDS: &[u16] = &[
    JOB_REQUEST,
    JOB_ACCEPTED,
    JOB_PROGRESS,
    JOB_RESULT,
    JOB_CANCEL,
    JOB_ERROR,
];

/// Workflow-run kinds 46001–46012 (the run/step/approval event stream).
/// Sourced from the registry range.
pub fn workflow_run_kind(kind: u16) -> bool {
    (WORKFLOW_RUN_FIRST..=WORKFLOW_RUN_LAST).contains(&kind)
}

/// Job kinds 43001–43006. Sourced from the registry range (the individual
/// constants are equivalent; the range check is the forward-compat form).
pub fn job_kind(kind: u16) -> bool {
    (JOB_RUN_FIRST..=JOB_RUN_LAST).contains(&kind)
}

/// All workflow-emitted kinds (jobs + workflow-run). Equivalent to
/// `job_kind(k) || workflow_run_kind(k)`; kept as one fn for the filter.
pub fn is_workflow_emitted_kind(kind: u16) -> bool {
    job_kind(kind) || workflow_run_kind(kind)
}

/// Does this event carry the `toron:workflow-emitted` single-`t` tag?
/// Defense-in-depth: rejects workflow-emitted kind-14/15/7 events that would
/// otherwise pass the kind check.
pub fn has_workflow_emitted_tag(event: &Event) -> bool {
    event
        .tags
        .iter()
        .any(|t| t.as_slice().first().map(String::as_str) == Some("t")
            && t.as_slice().get(1).map(String::as_str) == Some(WORKFLOW_EMITTED_TAG))
}

/// The loop-prevention predicate. The embedding calls this BEFORE evaluating a
/// workflow's trigger spec against an event. Returns `false` when the event is
/// workflow/job/gift-wrap output (or tagged as workflow-emitted) and must NOT
/// trigger any workflow.
///
/// Pure + sync + O(tags). No allocation on the fast path (kind check first).
pub fn should_evaluate_trigger_event(event: &Event) -> bool {
    // 1. Kind check (fast path — no tag scan needed for these).
    if is_workflow_emitted_kind(event.kind.as_u16()) {
        return false;
    }
    if event.kind.as_u16() == GIFT_WRAP {
        return false;
    }
    // 2. Tag check (defense in depth — catches workflow-emitted kind-14/15/7).
    if has_workflow_emitted_tag(event) {
        return false;
    }
    true
}

/// Same predicate, kind-only form (for embeddings that pre-filter by kind
/// before materializing the event, e.g. a bus subscription filter). Does NOT
/// catch the `toron:workflow-emitted` tag — use [`should_evaluate_trigger_event`]
/// for the full check once the event is materialized.
pub fn should_evaluate_trigger_kind(kind: u16) -> bool {
    !is_workflow_emitted_kind(kind) && kind != GIFT_WRAP
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::nostr::{Event, JsonUtil, Keys, Kind, Tag};

    /// Build a minimal signed event of `kind` with optional extra tags, for
    /// filter tests. Content is irrelevant to the filter.
    fn ev(kind: u16, tags: Vec<Tag>, keys: &Keys) -> Event {
        EventBuilder::new(Kind::from_u16(kind), "")
            .tags(tags)
            .sign_with_keys(keys)
            .expect("sign")
    }

    fn tag_t(value: &str) -> Tag {
        Tag::parse(["t", value]).expect("t tag")
    }

    /// ADR-0004 verification: every workflow-emitted kind is rejected.
    #[test]
    fn all_workflow_run_kinds_excluded() {
        let keys = Keys::generate();
        for k in WORKFLOW_RUN_FIRST..=WORKFLOW_RUN_LAST {
            let e = ev(k, vec![], &keys);
            assert!(
                !should_evaluate_trigger_event(&e),
                "kind {k} must NOT trigger workflows"
            );
            assert!(!should_evaluate_trigger_kind(k));
        }
    }

    /// ADR-0004 verification: every job kind is rejected.
    #[test]
    fn all_job_kinds_excluded() {
        let keys = Keys::generate();
        for k in JOB_RUN_FIRST..=JOB_RUN_LAST {
            let e = ev(k, vec![], &keys);
            assert!(
                !should_evaluate_trigger_event(&e),
                "job kind {k} must NOT trigger workflows"
            );
            assert!(!should_evaluate_trigger_kind(k));
        }
        // JOB_EMITS_KINDS list is consistent with the range.
        for &k in JOB_EMITS_KINDS {
            assert!(job_kind(k));
        }
    }

    /// Gift wrap (1059) is excluded.
    #[test]
    fn gift_wrap_excluded() {
        let keys = Keys::generate();
        let e = ev(GIFT_WRAP, vec![], &keys);
        assert!(!should_evaluate_trigger_event(&e));
        assert!(!should_evaluate_trigger_kind(GIFT_WRAP));
    }

    /// Legitimate user-authored kind-14 DM, kind-9 channel message, kind-7
    /// reaction are NOT excluded (they are valid message_posted /
    /// reaction_added sources when from users).
    #[test]
    fn legitimate_user_kinds_pass() {
        let keys = Keys::generate();
        for k in [14u16, 9, 7, 1] {
            let e = ev(k, vec![], &keys);
            assert!(
                should_evaluate_trigger_event(&e),
                "user kind {k} should be evaluated"
            );
            assert!(should_evaluate_trigger_kind(k));
        }
    }

    /// ADR-0004 invariant 6 (defense in depth): a workflow-emitted kind-14
    /// (a send_message action's output) carrying the tag is rejected EVEN
    /// THOUGH kind-14 is otherwise a legitimate trigger source.
    #[test]
    fn workflow_emitted_tag_rejects_kind_14() {
        let keys = Keys::generate();
        // Bare kind-14 → passes.
        let bare = ev(14, vec![], &keys);
        assert!(should_evaluate_trigger_event(&bare));
        // Tagged kind-14 → rejected.
        let tagged = ev(14, vec![tag_t(WORKFLOW_EMITTED_TAG)], &keys);
        assert!(
            !should_evaluate_trigger_event(&tagged),
            "workflow-emitted kind-14 must NOT re-trigger"
        );
    }

    /// THE regression test for the loop bug: a workflow whose action is
    /// `send_message` emits a kind-14 with the workflow-emitted tag; the
    /// filter rejects it, so the workflow cannot re-trigger itself.
    ///
    /// Models the exact loop: message_posted → workflow → send_message →
    /// (filtered) → no re-trigger.
    #[test]
    fn self_send_message_workflow_cannot_loop() {
        let workflow_keys = Keys::generate();
        // The workflow's send_message action emits this kind-14, stamped with
        // the workflow-emitted tag by the action runner.
        let emitted = ev(
            14,
            vec![tag_t(WORKFLOW_EMITTED_TAG)],
            &workflow_keys,
        );
        // The ingress filter MUST reject it.
        assert!(
            !should_evaluate_trigger_event(&emitted),
            "loop-prevention: workflow-emitted kind-14 must not re-trigger"
        );
        // Therefore no new RunInput is built → no new run → no infinite loop.
    }

    /// Same loop-prevention via kind: a `dispatch_job` action emits kind-43001;
    /// the kind check rejects it without needing the tag.
    #[test]
    fn dispatch_job_output_cannot_loop() {
        let keys = Keys::generate();
        let job_req = ev(JOB_REQUEST, vec![], &keys);
        assert!(!should_evaluate_trigger_event(&job_req));
    }

    /// has_workflow_emitted_tag is tag-specific (other `t` tags don't trip it).
    #[test]
    fn tag_check_is_specific() {
        let keys = Keys::generate();
        let other_tag = ev(14, vec![tag_t("some-other-tag")], &keys);
        assert!(should_evaluate_trigger_event(&other_tag));
        // Unrelated single-t.
        let project_tag = ev(14, vec![tag_t("my-project")], &keys);
        assert!(should_evaluate_trigger_event(&project_tag));
    }

    /// The kind-only predicate is consistent with the event predicate for
    /// kind-rejected events (no false sense of safety from the tag check).
    #[test]
    fn kind_predicate_consistent_with_event_predicate_for_kinds() {
        for k in [JOB_REQUEST, WORKFLOW_RUN_FIRST, WORKFLOW_RUN_LAST, GIFT_WRAP] {
            assert!(
                !should_evaluate_trigger_kind(k),
                "kind {k}: kind-predicate must agree"
            );
        }
        // The event predicate additionally catches the tag; the kind
        // predicate cannot (documented). A kind-14 bare event passes both;
        // a kind-14 tagged event passes kind-pred but fails event-pred.
        assert!(should_evaluate_trigger_kind(14));
    }

    /// Range helpers are correct envelopes.
    #[test]
    fn range_helpers_correct() {
        assert!(job_kind(JOB_RUN_FIRST));
        assert!(job_kind(JOB_RUN_LAST));
        assert!(!job_kind(JOB_RUN_FIRST - 1));
        assert!(!job_kind(JOB_RUN_LAST + 1));
        assert!(workflow_run_kind(WORKFLOW_RUN_FIRST));
        assert!(workflow_run_kind(WORKFLOW_RUN_LAST));
        assert!(!workflow_run_kind(WORKFLOW_RUN_FIRST - 1));
        assert!(!workflow_run_kind(WORKFLOW_RUN_LAST + 1));
        assert!(is_workflow_emitted_kind(JOB_REQUEST));
        assert!(is_workflow_emitted_kind(WORKFLOW_RUN_FIRST));
        assert!(!is_workflow_emitted_kind(14));
    }

    /// Round-trip a built event to confirm the filter operates on real signed
    /// events (not just stubs).
    #[test]
    fn filter_operates_on_wire_events() {
        let keys = Keys::generate();
        let e = EventBuilder::new(Kind::from_u16(JOB_RESULT), r#"{"status":"ok"}"#)
            .sign_with_keys(&keys)
            .expect("sign");
        // Wire round-trip.
        let parsed = Event::from_json(e.as_json()).expect("parse");
        assert_eq!(parsed, e);
        assert!(!should_evaluate_trigger_event(&parsed));
    }
}

// Local imports kept for the test block (rust-nostr's EventBuilder lives here).
#[cfg(test)]
use nostr_sdk::nostr::EventBuilder;

// The job range is derived locally from the individual JOB_* constants (the
// registry is frozen per ADR-0003; only WORKFLOW_RUN has a range pair). Keep
// this at the top so the helpers above see it.
const JOB_RUN_FIRST: u16 = JOB_REQUEST; // 43001
const JOB_RUN_LAST: u16 = JOB_ERROR; // 43006
