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

use nostr::prelude::Event;

use toron_core::bus::addressable::is_revocation;
use toron_core::nostr::kinds::{
    JOB_ACCEPTED, JOB_CANCEL, JOB_ERROR, JOB_PROGRESS, JOB_REQUEST, JOB_RESULT, WORKFLOW_RUN_FIRST,
    WORKFLOW_RUN_LAST,
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
pub const JOB_RUN_FIRST: u16 = JOB_REQUEST; // 43001
pub const JOB_RUN_LAST: u16 = JOB_ERROR; // 43006

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
    event.tags.iter().any(|t| {
        t.as_slice().first().map(String::as_str) == Some("t")
            && t.as_slice().get(1).map(String::as_str) == Some(WORKFLOW_EMITTED_TAG)
    })
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

// ---------------------------------------------------------------------------
// Trigger-spec evaluation (P11.1 reactive ingress — the wiring layer)
//
// The embedding (toron-mcp serve host, flywheel, any bus subscriber) runs:
//
//   should_evaluate_trigger_event(&event)?   // loop prevention (above)
//     → matches_trigger(trigger, &event)?    // this layer
//     → engine.run(trigger_run_id(...), RunInput { workflow, trigger: trigger_payload(&event) })
//
// Kind-level matching + the reservation acquired/released DISAMBIGUATION
// (both are kind-30031; a release marker must fire `reservation_released`,
// never `reservation_acquired`) + the Trigger's own project/path/channel/
// emoji scoping fields. The loop-prevention filter stays the layer above —
// it never consults trigger specs.
// ---------------------------------------------------------------------------

use serde_json::{Value, json};

use crate::schema::Trigger;

/// Kind of a file reservation (30031) — the reservation_acquired/released
/// trigger source.
pub const FILE_RESERVATION_KIND: u16 = 30031;

/// True when a 30031 event terminates a lease: a holder-released marker
/// (content `status:released` / JSON `"status":"released"` / a
/// `["status", "released"]` tag) OR an operator revocation (A1 force-release —
/// the lease is gone either way). Such events match `reservation_released`,
/// never `reservation_acquired`.
pub fn is_reservation_released_marker(event: &Event) -> bool {
    if is_revocation(event) {
        return true;
    }
    // Canonical forms only (mirrors is_revocation): the bare substring
    // "released" would misclassify an ACQUIRE whose free-text `reason`
    // mentions "released" — routing the trigger to the wrong arm.
    event.content.contains("status:released")
        || event.content.contains("\"status\":\"released\"")
        || event.tags.iter().any(|t| {
            t.as_slice().first().map(String::as_str) == Some("status")
                && t.as_slice().get(1).map(String::as_str) == Some("released")
        })
}

/// Match a workflow's trigger spec against an inbound event (kind-level +
/// the trigger's own scoping fields). The caller has already applied
/// [`should_evaluate_trigger_event`] (loop prevention).
///
/// Scoping semantics:
/// - reservation triggers carry a REQUIRED `project` (+ optional `path`
///   glob): the event must carry the project `t`-tag, and (when `path` is
///   set) the `d`-tag path must overlap it symmetrically;
/// - message/reaction `project`/`channel`/`emoji` are optional constraints
///   (absent = unconstrained);
/// - `agent_state_changed` matches kind-20001 (content schema is
///   presence-specific; kind-level only);
/// - `schedule` / `webhook` are not event-driven (cron/external) — never
///   match a bus event.
pub fn matches_trigger(trigger: &Trigger, event: &Event) -> bool {
    let kind = event.kind.as_u16();
    match trigger {
        Trigger::MessagePosted { channel, project } => {
            (kind == 14 || kind == 9 || kind == 1)
                && scoped_tag_match(project.as_deref(), event, "t")
                && scoped_tag_match(channel.as_deref(), event, "h")
        }
        Trigger::ReactionAdded { channel, emoji } => {
            kind == 7
                && emoji.as_ref().map(|e| event.content == *e).unwrap_or(true)
                && scoped_tag_match(channel.as_deref(), event, "h")
        }
        Trigger::ReservationAcquired { project, path } => {
            kind == FILE_RESERVATION_KIND
                && !is_reservation_released_marker(event)
                && tag_match(event, "t") == Some(project.as_str())
                && path_scoped_match(path.as_deref(), event)
        }
        Trigger::ReservationReleased { project, path } => {
            kind == FILE_RESERVATION_KIND
                && is_reservation_released_marker(event)
                && tag_match(event, "t") == Some(project.as_str())
                && path_scoped_match(path.as_deref(), event)
        }
        Trigger::AgentStateChanged { .. } => kind == 20001,
        Trigger::Schedule { .. } => false, // cron-managed, not event-driven
        Trigger::Webhook { .. } => false,  // external, not a bus event
    }
}

/// First value of a `["<name>", <value>, ...]` tag (pair form).
pub fn tag_match<'a>(event: &'a Event, name: &str) -> Option<&'a str> {
    event.tags.iter().find_map(|t| {
        (t.as_slice().first().map(String::as_str) == Some(name))
            .then(|| t.as_slice().get(1).map(String::as_str))
            .flatten()
    })
}

/// Optional scoping: `None` = unconstrained; `Some(want)` = the event's
/// `<tag>` must equal `want`.
fn scoped_tag_match(want: Option<&str>, event: &Event, tag: &str) -> bool {
    want.is_none_or(|w| tag_match(event, tag) == Some(w))
}

/// Optional reservation-path glob: `None` = unconstrained; `Some(glob)` =
/// symmetric overlap with the `d`-tag path (`project:path-glob`).
fn path_scoped_match(glob: Option<&str>, event: &Event) -> bool {
    let Some(glob) = glob else {
        return true;
    };
    let Some(d) = tag_match(event, "d") else {
        return false;
    };
    let Some((_, path)) = d.split_once(':') else {
        return false;
    };
    toron_core::mailbox::overlaps(glob, path)
}

/// The `{{trigger.*}}` payload for an inbound event: id/sender/content/kind/
/// created_at + the `tags` array + `t`/`h`/`importance` tag surfaces (project/
/// channel/importance). Reservation events gain a `status` field
/// (`acquired` | `released`) so guards can discriminate the two 30031 paths.
pub fn trigger_payload(event: &Event) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("id".into(), json!(event.id.to_hex()));
    m.insert("sender".into(), json!(event.pubkey.to_hex()));
    m.insert("content".into(), json!(event.content));
    m.insert("kind".into(), json!(event.kind.as_u16()));
    m.insert("created_at".into(), json!(event.created_at.as_secs()));
    let mut project: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut importance: Option<String> = None;
    let mut tags: Vec<Value> = Vec::new();
    for t in event.tags.iter() {
        let p = t.as_slice();
        tags.push(json!(p));
        if p.first().map(String::as_str) == Some("t") && project.is_none() {
            project = p.get(1).cloned();
        }
        if p.first().map(String::as_str) == Some("h") && channel.is_none() {
            channel = p.get(1).cloned();
        }
        if p.first().map(String::as_str) == Some("importance") && importance.is_none() {
            importance = p.get(1).cloned();
        }
    }
    m.insert("tags".into(), Value::Array(tags));
    if let Some(p) = project {
        m.insert("project".into(), json!(p));
    }
    if let Some(c) = channel {
        m.insert("channel".into(), json!(c));
    }
    if let Some(i) = importance {
        m.insert("importance".into(), json!(i));
    }
    if event.kind.as_u16() == FILE_RESERVATION_KIND {
        m.insert(
            "status".into(),
            json!(if is_reservation_released_marker(event) {
                "released"
            } else {
                "acquired"
            }),
        );
    }
    Value::Object(m)
}

/// Deterministic per-(workflow, event) run id — the engine's idempotency key:
/// a redelivered event reuses the same run (never double-fires).
pub fn trigger_run_id(workflow_name: &str, event: &Event) -> String {
    format!("wf-{workflow_name}-{}", event.id.to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::prelude::{Event, EventBuilder, FinalizeEvent, Keys, Kind, Tag};

    /// Build a minimal signed event of `kind` with optional extra tags, for
    /// filter tests. Content is irrelevant to the filter.
    fn ev(kind: u16, tags: Vec<Tag>, keys: &Keys) -> Event {
        EventBuilder::new(Kind::from_u16(kind), "")
            .tags(tags)
            .finalize(keys)
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
        let emitted = ev(14, vec![tag_t(WORKFLOW_EMITTED_TAG)], &workflow_keys);
        // The ingress filter MUST reject it.
        assert!(
            !should_evaluate_trigger_event(&emitted),
            "loop-prevention: workflow-emitted kind-14 must not re-trigger"
        );
        // Therefore no new RunInput is built → no new run → no infinite loop.
    }

    /// The kind-only predicate is consistent with the event predicate for
    /// kind-rejected events (no false sense of safety from the tag check).
    #[test]
    fn kind_predicate_consistent_with_event_predicate_for_kinds() {
        for k in [
            JOB_REQUEST,
            WORKFLOW_RUN_FIRST,
            WORKFLOW_RUN_LAST,
            GIFT_WRAP,
        ] {
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

    /// Round-trip a built event to confirm the filter operates on real signed
    /// events (not just stubs).
    #[test]
    fn filter_operates_on_wire_events() {
        let keys = Keys::generate();
        let e = EventBuilder::new(Kind::from_u16(JOB_RESULT), r#"{"status":"ok"}"#)
            .finalize(&keys)
            .expect("sign");
        // Wire round-trip.
        let parsed = Event::from_json(e.as_json()).expect("parse");
        assert_eq!(parsed, e);
        assert!(!should_evaluate_trigger_event(&parsed));
    }

    // ------------------------------------------------------------------
    // P11.1 trigger-spec evaluation
    // ------------------------------------------------------------------

    use crate::schema::{CronScope, Trigger};

    /// A kind-30031 event with project scoping + optional status marker.
    fn reservation(content: &str, tags: Vec<Tag>, keys: &Keys) -> Event {
        EventBuilder::new(Kind::from_u16(FILE_RESERVATION_KIND), content)
            .tags(tags)
            .finalize(keys)
            .expect("sign")
    }

    fn tag_d(project: &str, path: &str) -> Tag {
        Tag::parse(["d", &format!("{project}:{path}")]).expect("d")
    }

    #[test]
    fn reservation_acquired_vs_released_disambiguation() {
        let keys = Keys::generate();
        // Plain acquire (status-less content) → acquired only.
        let acquired = reservation(
            r#"{"exclusive":true}"#,
            vec![tag_d("demo", "src/**"), tag_t("demo")],
            &keys,
        );
        assert!(matches_trigger(
            &Trigger::ReservationAcquired {
                project: "demo".into(),
                path: None,
            },
            &acquired
        ));
        assert!(
            !matches_trigger(
                &Trigger::ReservationReleased {
                    project: "demo".into(),
                    path: None,
                },
                &acquired
            ),
            "an acquire must NOT fire reservation_released"
        );

        // Released marker in content → released only.
        let released = reservation(
            r#"{"status":"released"}"#,
            vec![tag_d("demo", "src/**"), tag_t("demo")],
            &keys,
        );
        assert!(matches_trigger(
            &Trigger::ReservationReleased {
                project: "demo".into(),
                path: None,
            },
            &released
        ));
        assert!(
            !matches_trigger(
                &Trigger::ReservationAcquired {
                    project: "demo".into(),
                    path: None,
                },
                &released
            ),
            "a release must NOT fire reservation_acquired"
        );

        // Released via the `["status", "released"]` tag form (release path).
        let tagged = reservation(
            r#"{"exclusive":true}"#,
            vec![
                tag_d("demo", "src/**"),
                tag_t("demo"),
                Tag::parse(["status", "released"]).expect("status"),
            ],
            &keys,
        );
        assert!(matches_trigger(
            &Trigger::ReservationReleased {
                project: "demo".into(),
                path: None,
            },
            &tagged
        ));

        // Operator revocation (force-release) → released (lease terminated).
        let revoked = reservation(
            r#"{"status":"revoked","heuristic":"stale-holder"}"#,
            vec![
                tag_d("demo", "src/**"),
                tag_t("demo"),
                Tag::parse(["status", "revoked"]).expect("status"),
            ],
            &keys,
        );
        assert!(
            is_reservation_released_marker(&revoked),
            "revocation terminates the lease"
        );
        assert!(matches_trigger(
            &Trigger::ReservationReleased {
                project: "demo".into(),
                path: None,
            },
            &revoked
        ));
    }

    /// Regression (bug hunt 2026-08-17): the acquire event embeds the
    /// holder's free-text `reason` in its content. A bare
    /// `content.contains("released")` misread such acquires as release
    /// markers — routing reservation_acquired triggers to the wrong arm and
    /// flipping `trigger_payload.status`. Only the canonical marker forms
    /// (`status:released` plain/JSON, or the status tag) may classify.
    #[test]
    fn reason_text_containing_released_is_not_a_release_marker() {
        let keys = Keys::generate();
        let acq_reason = reservation(
            r#"{"exclusive":true,"reason":"cleanup after v2 released"}"#,
            vec![tag_d("demo", "src/**"), tag_t("demo")],
            &keys,
        );
        assert!(
            !is_reservation_released_marker(&acq_reason),
            "an acquire whose reason mentions 'released' is NOT a release marker"
        );
        assert!(matches_trigger(
            &Trigger::ReservationAcquired {
                project: "demo".into(),
                path: None,
            },
            &acq_reason
        ));
        assert!(!matches_trigger(
            &Trigger::ReservationReleased {
                project: "demo".into(),
                path: None,
            },
            &acq_reason
        ));
        assert_eq!(
            trigger_payload(&acq_reason)["status"],
            serde_json::json!("acquired")
        );

        // Positive controls: both canonical content forms still classify.
        for content in [
            r#"{"status":"released"}"#.to_string(),
            "status:released".to_string(),
        ] {
            let marker = reservation(
                &content,
                vec![tag_d("demo", "src/**"), tag_t("demo")],
                &keys,
            );
            assert!(
                is_reservation_released_marker(&marker),
                "canonical form {content} must classify as released"
            );
        }
    }

    #[test]
    fn reservation_project_and_path_scoping() {
        let keys = Keys::generate();
        let acquired = reservation(
            r#"{"exclusive":true}"#,
            vec![tag_d("demo", "src/lib.rs"), tag_t("demo")],
            &keys,
        );
        // Wrong project → no match.
        assert!(!matches_trigger(
            &Trigger::ReservationAcquired {
                project: "other".into(),
                path: None,
            },
            &acquired
        ));
        // No project t-tag at all → no match (required scoping).
        let unscoped = reservation(
            r#"{"exclusive":true}"#,
            vec![tag_d("demo", "src/lib.rs")],
            &keys,
        );
        assert!(!matches_trigger(
            &Trigger::ReservationAcquired {
                project: "demo".into(),
                path: None,
            },
            &unscoped
        ));
        // Path glob: symmetric overlap (trigger glob vs d-tag path).
        assert!(matches_trigger(
            &Trigger::ReservationAcquired {
                project: "demo".into(),
                path: Some("src/**".into()),
            },
            &acquired
        ));
        assert!(matches_trigger(
            &Trigger::ReservationAcquired {
                project: "demo".into(),
                path: Some("src/lib.rs".into()),
            },
            &acquired
        ));
        assert!(!matches_trigger(
            &Trigger::ReservationAcquired {
                project: "demo".into(),
                path: Some("docs/**".into()),
            },
            &acquired
        ));
    }

    #[test]
    fn reaction_added_kind_emoji_and_channel() {
        let keys = Keys::generate();
        let thumbs = EventBuilder::new(Kind::from_u16(7), "+")
            .tags(vec![Tag::parse(["h", "room"]).expect("h")])
            .finalize(&keys)
            .expect("sign");
        assert!(matches_trigger(
            &Trigger::ReactionAdded {
                channel: None,
                emoji: None,
            },
            &thumbs
        ));
        // Emoji constraint matches the NIP-25 content.
        assert!(matches_trigger(
            &Trigger::ReactionAdded {
                channel: None,
                emoji: Some("+".into()),
            },
            &thumbs
        ));
        assert!(!matches_trigger(
            &Trigger::ReactionAdded {
                channel: None,
                emoji: Some(":fire:".into()),
            },
            &thumbs
        ));
        assert!(matches_trigger(
            &Trigger::ReactionAdded {
                channel: Some("room".into()),
                emoji: None,
            },
            &thumbs
        ));
        assert!(!matches_trigger(
            &Trigger::ReactionAdded {
                channel: Some("other".into()),
                emoji: None,
            },
            &thumbs
        ));
        // A kind-7 is NOT a message_posted trigger.
        assert!(!matches_trigger(
            &Trigger::MessagePosted {
                channel: None,
                project: None,
            },
            &thumbs
        ));
    }

    #[test]
    fn message_posted_kind_project_and_channel_scoping() {
        let keys = Keys::generate();
        let dm = EventBuilder::new(Kind::from_u16(14), "hello")
            .tags(vec![tag_t("demo"), Tag::parse(["h", "room"]).expect("h")])
            .finalize(&keys)
            .expect("sign");
        assert!(matches_trigger(
            &Trigger::MessagePosted {
                channel: None,
                project: None,
            },
            &dm
        ));
        assert!(matches_trigger(
            &Trigger::MessagePosted {
                channel: None,
                project: Some("demo".into()),
            },
            &dm
        ));
        assert!(!matches_trigger(
            &Trigger::MessagePosted {
                channel: None,
                project: Some("other".into()),
            },
            &dm
        ));
        assert!(matches_trigger(
            &Trigger::MessagePosted {
                channel: Some("room".into()),
                project: None,
            },
            &dm
        ));
        assert!(!matches_trigger(
            &Trigger::MessagePosted {
                channel: Some("nope".into()),
                project: None,
            },
            &dm
        ));
        // Schedule/webhook never match bus events.
        assert!(!matches_trigger(
            &Trigger::Schedule {
                cron: "* * * * *".into(),
                owner: None,
                scope: CronScope::default(),
            },
            &dm
        ));
    }

    #[test]
    fn trigger_payload_shapes_and_run_id() {
        let keys = Keys::generate();
        let acquired = reservation(
            r#"{"exclusive":true}"#,
            vec![tag_d("demo", "src/lib.rs"), tag_t("demo")],
            &keys,
        );
        let payload = trigger_payload(&acquired);
        assert_eq!(payload["kind"], json!(30031));
        assert_eq!(payload["project"], json!("demo"));
        assert_eq!(payload["status"], json!("acquired"));
        assert!(payload["tags"].is_array());
        let released = reservation(
            r#"{"status":"released"}"#,
            vec![tag_d("demo", "src/lib.rs"), tag_t("demo")],
            &keys,
        );
        assert_eq!(trigger_payload(&released)["status"], json!("released"));
        // A non-reservation payload has no status field.
        let dm = EventBuilder::new(Kind::from_u16(14), "hi")
            .finalize(&keys)
            .expect("sign");
        assert!(trigger_payload(&dm).get("status").is_none());
        // Deterministic run ids: stable per (workflow, event), distinct per event.
        assert_eq!(
            trigger_run_id("wf-a", &acquired),
            trigger_run_id("wf-a", &acquired)
        );
        assert_ne!(
            trigger_run_id("wf-a", &acquired),
            trigger_run_id("wf-b", &acquired)
        );
        assert_ne!(
            trigger_run_id("wf-a", &acquired),
            trigger_run_id("wf-a", &released)
        );
    }
}
