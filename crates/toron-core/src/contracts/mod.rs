// governed-by: ADR-0040, ADR-0041
//! Typed, versioned instruction contracts (redesign §7). Each contract is a
//! serde type with `deny_unknown_fields`; an envelope names one by
//! `name@major`. An unknown name or major is refused with a typed
//! [`Reject::UnsupportedContract`], never parsed loosely.

pub mod approval_v1;
pub mod cancel_v1;
pub mod context_v1;
pub mod not_ready_v1;
pub mod notify_v1;
pub mod reply_v1;
pub mod request_v1;
pub mod role_v1;
pub mod task_v1;
pub mod verdict_v1;

use crate::delivery::Reject;
use crate::envelope::{Class, Envelope};

/// Every contract this build understands, as `(name, major, class)`.
///
/// Control-class entries: `role@1` and `verdict@1` (Phase 4.1), `context@1`
/// (Phase 4.2), `cancel@1` and `approval@1` (Phase 4.3). `steer`, `goal`, and
/// `escalate` stay unnamed until Phase 4a; an envelope naming one is
/// [`Reject::UnsupportedContract`].
/// `not_ready@1` is [`Class::Receipt`] (the class exists; a receipt, not a control).
pub const KNOWN: &[(&str, u32, Class)] = &[
    ("task", 1, Class::Task),
    ("notify", 1, Class::Notify),
    ("role", 1, Class::Control),
    ("verdict", 1, Class::Control),
    ("context", 1, Class::Control),
    ("request", 1, Class::Request),
    ("reply", 1, Class::Reply),
    ("cancel", 1, Class::Control),
    ("approval", 1, Class::Control),
    // not_ready@1 is Class::Receipt, not Control.
    ("not_ready", 1, Class::Receipt),
];

/// A parsed, typed body.
#[derive(Clone, Debug, PartialEq)]
pub enum Body {
    /// `task@1`.
    Task(task_v1::TaskV1),
    /// `notify@1`.
    Notify(notify_v1::NotifyV1),
    /// `role@1` (Control).
    Role(role_v1::RoleV1),
    /// `verdict@1` (Control, provisional).
    Verdict(verdict_v1::VerdictV1),
    /// `context@1` (Control): the topology fact.
    Context(context_v1::ContextV1),
    /// `request@1`.
    Request(request_v1::RequestV1),
    /// `reply@1`.
    Reply(reply_v1::ReplyV1),
    /// `cancel@1` (Control).
    Cancel(cancel_v1::CancelV1),
    /// `approval@1` (Control). `env.task` is the run id.
    Approval(approval_v1::ApprovalV1),
    /// `not_ready@1` (Receipt).
    NotReady(not_ready_v1::NotReadyV1),
}

/// Check the envelope's contract: known, consistent with its class, and the
/// body parses as that contract's type.
pub fn check(env: &Envelope) -> Result<(), Reject> {
    parse(env).map(|_| ())
}

/// Parse the envelope body as its contract's type.
pub fn parse(env: &Envelope) -> Result<Body, Reject> {
    let c = &env.contract;
    let Some(&(_, _, class)) = KNOWN.iter().find(|(n, m, _)| *n == c.name && *m == c.major) else {
        return Err(Reject::UnsupportedContract {
            name: c.name.clone(),
            major: c.major,
        });
    };
    if class != env.class {
        return Err(Reject::ContractClassMismatch {
            contract: c.to_string(),
            class: env.class,
        });
    }
    let bad = |e: serde_json::Error| Reject::BadBody {
        contract: c.to_string(),
        error: e.to_string(),
    };
    let body = match c.name.as_str() {
        "task" => Body::Task(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "notify" => Body::Notify(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "role" => Body::Role(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "verdict" => Body::Verdict(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "context" => Body::Context(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "request" => Body::Request(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "reply" => Body::Reply(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "cancel" => Body::Cancel(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "approval" => Body::Approval(serde_json::from_value(env.body.clone()).map_err(bad)?),
        "not_ready" => Body::NotReady(serde_json::from_value(env.body.clone()).map_err(bad)?),
        _ => unreachable!("KNOWN and this match list the same names"),
    };
    match &body {
        Body::Task(t) => t.validate(env)?,
        Body::Notify(n) => n.validate()?,
        Body::Role(r) => r.validate(env)?,
        Body::Verdict(v) => v.validate(env)?,
        Body::Context(_) => {}
        Body::Request(r) => r.validate(env)?,
        Body::Reply(r) => r.validate(env)?,
        Body::Cancel(c) => c.validate(env)?,
        Body::Approval(a) => a.validate(env)?,
        Body::NotReady(n) => n.validate(env)?,
    }
    Ok(body)
}
