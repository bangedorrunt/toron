// governed-by: ADR-0003
//! Map job kinds 43001–43006 onto the task lifecycle (Phase 4.4).
//!
//! Pure. The numbers stay in [`crate::nostr::kinds`]; this module only names
//! which lifecycle step each kind is. Both directions.

use crate::nostr::kinds::{
    JOB_ACCEPTED, JOB_CANCEL, JOB_ERROR, JOB_PROGRESS, JOB_REQUEST, JOB_RESULT,
};

/// Where a job kind sits on the task lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    /// 43001 request: `task@1` sent.
    Sent,
    /// 43002 claim: ack accept, plus the claim holder.
    Claim,
    /// 43003 progress.
    Progress,
    /// 43004 result: done.
    Done,
    /// 43005 cancel: `cancel@1`.
    Cancel,
    /// 43006 error: reject / failed.
    Failed,
}

/// `Some` for 43001–43006. Any other kind is `None`.
pub fn from_kind(kind: u32) -> Option<Lifecycle> {
    Some(match kind {
        k if k == u32::from(JOB_REQUEST) => Lifecycle::Sent,
        k if k == u32::from(JOB_ACCEPTED) => Lifecycle::Claim,
        k if k == u32::from(JOB_PROGRESS) => Lifecycle::Progress,
        k if k == u32::from(JOB_RESULT) => Lifecycle::Done,
        k if k == u32::from(JOB_CANCEL) => Lifecycle::Cancel,
        k if k == u32::from(JOB_ERROR) => Lifecycle::Failed,
        _ => return None,
    })
}

/// The Nostr kind for a lifecycle step. Always one of 43001–43006.
pub fn to_kind(life: Lifecycle) -> u32 {
    u32::from(match life {
        Lifecycle::Sent => JOB_REQUEST,
        Lifecycle::Claim => JOB_ACCEPTED,
        Lifecycle::Progress => JOB_PROGRESS,
        Lifecycle::Done => JOB_RESULT,
        Lifecycle::Cancel => JOB_CANCEL,
        Lifecycle::Failed => JOB_ERROR,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_kinds_round_trip_and_unknown_is_none() {
        let pairs = [
            (43001, Lifecycle::Sent),
            (43002, Lifecycle::Claim),
            (43003, Lifecycle::Progress),
            (43004, Lifecycle::Done),
            (43005, Lifecycle::Cancel),
            (43006, Lifecycle::Failed),
        ];
        for (kind, life) in pairs {
            assert_eq!(from_kind(kind), Some(life));
            assert_eq!(to_kind(life), kind);
            assert_eq!(from_kind(to_kind(life)), Some(life));
        }
        assert_eq!(from_kind(0), None);
        assert_eq!(from_kind(43000), None);
        assert_eq!(from_kind(43007), None);
        assert_eq!(from_kind(9), None);
    }
}
