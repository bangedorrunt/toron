// governed-by: ADR-0003 Rev9 (GS-061 / Buzz #7298 #7532 pattern, SQLite — no Redis)
#![forbid(unsafe_code)]

//! Authority-side-effect convergence gate (kick / revoke / disable class).
//!
//! Buzz #7298 fires kick live side-effects only at **authority convergence**;
//! #7532 fails closed when presence storage cannot confirm. Toron maps both
//! onto a pure gate over SQLite authority: side-effects emit only after
//! `Commit`, and a re-add under a held generation is fenced. No Redis.

use serde::{Deserialize, Serialize};

/// Authority mutation the bus may apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthorityOp {
    Kick { target: String },
    Revoke { target: String },
    Disable { target: String },
}

/// Pure convergence state for one authority op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Convergence {
    /// Proposed; not yet committed — no live side-effects.
    Pending {
        op: AuthorityOp,
        generation: u64,
    },
    /// Authority committed; live fanout may fire exactly once.
    Committed {
        op: AuthorityOp,
        generation: u64,
    },
    /// Store/authority could not confirm — fail closed (no fanout).
    Rejected {
        reason: String,
    },
    /// Re-add raced under held generation — fenced.
    Fenced {
        attempted_generation: u64,
        held_generation: u64,
    },
}

/// Inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConvergenceMsg {
    /// Propose an authority op at `generation` (lock held by host).
    Propose { op: AuthorityOp, generation: u64 },
    /// Authority/store confirmed — commit.
    Commit,
    /// Authority/store failed — reject before fanout.
    Fail { reason: String },
    /// Re-add attempt while a generation is held.
    ReAdd { generation: u64 },
}

/// Effects (host performs fanout / persistence).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConvergenceEffect {
    /// Fire live side-effect (kick/revoke/disable) exactly once at commit.
    FireLive { op: AuthorityOp, generation: u64 },
    /// Do not fanout; surface the refusal (correlated OK false pattern).
    Refuse { reason: String },
    /// Re-add ignored under fence.
    FenceReAdd { attempted: u64, held: u64 },
}

/// Tick. `state=None` means idle (no in-flight authority op).
pub fn tick(
    state: Option<&Convergence>,
    msg: &ConvergenceMsg,
) -> (Option<Convergence>, Vec<ConvergenceEffect>) {
    match (state, msg) {
        (None, ConvergenceMsg::Propose { op, generation }) => (
            Some(Convergence::Pending {
                op: op.clone(),
                generation: *generation,
            }),
            vec![],
        ),
        (
            Some(Convergence::Pending { op, generation }),
            ConvergenceMsg::Commit,
        ) => (
            Some(Convergence::Committed {
                op: op.clone(),
                generation: *generation,
            }),
            vec![ConvergenceEffect::FireLive {
                op: op.clone(),
                generation: *generation,
            }],
        ),
        (Some(Convergence::Pending { .. }), ConvergenceMsg::Fail { reason }) => (
            Some(Convergence::Rejected {
                reason: reason.clone(),
            }),
            vec![ConvergenceEffect::Refuse {
                reason: reason.clone(),
            }],
        ),
        (
            Some(Convergence::Pending { generation: held, .. })
            | Some(Convergence::Committed { generation: held, .. }),
            ConvergenceMsg::ReAdd { generation },
        ) => {
            if generation != held {
                (
                    Some(Convergence::Fenced {
                        attempted_generation: *generation,
                        held_generation: *held,
                    }),
                    vec![ConvergenceEffect::FenceReAdd {
                        attempted: *generation,
                        held: *held,
                    }],
                )
            } else {
                (state.cloned(), vec![])
            }
        }
        // Fail-closed: commit/fail with no pending proposal refuses.
        (None, ConvergenceMsg::Commit) => (
            Some(Convergence::Rejected {
                reason: "commit without pending authority".into(),
            }),
            vec![ConvergenceEffect::Refuse {
                reason: "commit without pending authority".into(),
            }],
        ),
        (None, ConvergenceMsg::Fail { reason }) => (
            Some(Convergence::Rejected {
                reason: reason.clone(),
            }),
            vec![ConvergenceEffect::Refuse {
                reason: reason.clone(),
            }],
        ),
        // Idempotent / ignore elsewhere.
        (s, _) => (s.cloned(), vec![]),
    }
}

/// Presence/ephemeral fail-closed helper: if the authority confirm flag is
/// false, refuse before fanout (Buzz #7532 pattern without Redis).
pub fn presence_admit(authority_confirmed: bool) -> Result<(), &'static str> {
    if authority_confirmed {
        Ok(())
    } else {
        Err("presence authority unconfirmed — fail closed before fanout")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_effects_fire_only_at_commit() {
        let op = AuthorityOp::Kick {
            target: "npub1".into(),
        };
        let (s, fx) = tick(
            None,
            &ConvergenceMsg::Propose {
                op: op.clone(),
                generation: 1,
            },
        );
        assert!(fx.is_empty(), "propose must not fanout");
        let (_s, fx) = tick(s.as_ref(), &ConvergenceMsg::Commit);
        assert_eq!(
            fx,
            vec![ConvergenceEffect::FireLive {
                op,
                generation: 1
            }]
        );
    }

    #[test]
    fn fail_closed_before_fanout() {
        let (s, _) = tick(
            None,
            &ConvergenceMsg::Propose {
                op: AuthorityOp::Disable {
                    target: "x".into(),
                },
                generation: 2,
            },
        );
        let (_s, fx) = tick(
            s.as_ref(),
            &ConvergenceMsg::Fail {
                reason: "sqlite busy".into(),
            },
        );
        assert_eq!(
            fx,
            vec![ConvergenceEffect::Refuse {
                reason: "sqlite busy".into()
            }]
        );
    }

    #[test]
    fn readd_under_held_generation_is_fenced() {
        let (s, _) = tick(
            None,
            &ConvergenceMsg::Propose {
                op: AuthorityOp::Revoke {
                    target: "y".into(),
                },
                generation: 7,
            },
        );
        let (_s, fx) = tick(s.as_ref(), &ConvergenceMsg::ReAdd { generation: 8 });
        assert_eq!(
            fx,
            vec![ConvergenceEffect::FenceReAdd {
                attempted: 8,
                held: 7
            }]
        );
    }

    #[test]
    fn presence_admit_fail_closed_without_redis() {
        assert!(presence_admit(true).is_ok());
        assert!(presence_admit(false).is_err());
    }
}
