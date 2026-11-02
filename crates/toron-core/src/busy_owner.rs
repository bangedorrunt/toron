// governed-by: ADR-0003 Rev9 (GS-061 / Buzz #7337 #7340)
#![forbid(unsafe_code)]

//! Busy-owner hold bound + generation fence + deadline wake (pure core).
//!
//! Maps Buzz ACP busy-owner patterns onto toron dispatch **without** porting
//! ACP/stdio. Effects are data; the host (DispatchPool / engine park)
//! performs SleepUntilDeadline. Journal settlement ≠ provider completion
//! (ADR-0032).
//!
//! As-is: A7 semaphore + one-in-flight-per-agent, no timed hold, no generation.
//! To-be: timed hold bound (cross-channel starvation bound) + generation fence
//! so a stale/forked worker cannot reclaim + deadline wake as a journaled sleep.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Default busy-owner hold bound (Buzz #7337 class: ~10s).
pub const DEFAULT_HOLD_BOUND_MS: u64 = 10_000;

/// Pure busy-owner state layered on one-in-flight-per-agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusyOwnerModel {
    /// Per-agent generation (bumped on fork/rebind).
    pub generation: BTreeMap<String, u64>,
    /// Agents currently holding a slot, with the generation at acquire.
    pub holders: BTreeMap<String, Holder>,
    /// Agents waiting behind a busy owner until `until_ms`.
    pub holds: BTreeMap<String, Hold>,
    /// Max wait behind a busy owner before fork/deadline wake.
    pub hold_bound_ms: u64,
}

/// An in-flight holder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Holder {
    /// Generation at acquire time.
    pub generation: u64,
    /// Wall/ms clock when acquired (host-supplied).
    pub since_ms: u64,
}

/// A bounded wait for a busy owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hold {
    /// When the hold started.
    pub since_ms: u64,
    /// Absolute deadline (`since_ms + hold_bound_ms`).
    pub until_ms: u64,
    /// Generation observed when the hold began (stale if holder advanced).
    pub observed_generation: u64,
}

impl BusyOwnerModel {
    /// New model with the given hold bound.
    pub const fn new(hold_bound_ms: u64) -> Self {
        Self {
            generation: BTreeMap::new(),
            holders: BTreeMap::new(),
            holds: BTreeMap::new(),
            hold_bound_ms,
        }
    }

    /// Test constructor.
    pub const fn test_new() -> Self {
        Self::new(DEFAULT_HOLD_BOUND_MS)
    }

    /// Current generation for an agent (0 if never forked).
    pub fn generation_of(&self, agent: &str) -> u64 {
        self.generation.get(agent).copied().unwrap_or(0)
    }
}

/// Inputs to the pure transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BusyOwnerMsg {
    /// Try to acquire; if busy, start/continue a bounded hold.
    TryAcquire {
        agent: String,
        now_ms: u64,
        /// Caller's generation claim (must match current or fence).
        claimed_generation: u64,
    },
    /// Release the holder's slot (permit drop). Generation must match.
    Release {
        agent: String,
        claimed_generation: u64,
    },
    /// Deadline wake: hold expired → host may fork (generation++) and retry.
    DeadlineWake { agent: String, now_ms: u64 },
    /// Explicit fork/rebind: advance generation so stale workers cannot reclaim.
    ForkRebind { agent: String },
}

/// Journaled effects — no I/O in tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BusyOwnerEffect {
    /// Slot acquired at this generation.
    Acquired { agent: String, generation: u64 },
    /// Agent busy; host should park until deadline (engine park).
    SleepUntilDeadline {
        agent: String,
        until_ms: u64,
        observed_generation: u64,
    },
    /// Hold still active (re-check; do not restart the bound window).
    HoldContinued { agent: String, until_ms: u64 },
    /// Hold bound elapsed — host may fork and retry acquire.
    HoldExpired { agent: String, held_for_ms: u64 },
    /// Stale worker fenced (claimed generation ≠ current).
    FencedStale {
        agent: String,
        claimed: u64,
        current: u64,
    },
    /// Slot released.
    Released { agent: String, generation: u64 },
    /// Generation advanced (fork/rebind).
    GenerationAdvanced { agent: String, generation: u64 },
    /// Release ignored (no holder / generation mismatch already fenced).
    ReleaseIgnored { agent: String },
}

/// Pure transition.
pub fn tick(model: &BusyOwnerModel, msg: &BusyOwnerMsg) -> (BusyOwnerModel, Vec<BusyOwnerEffect>) {
    let mut next = model.clone();
    let effects = match msg {
        BusyOwnerMsg::TryAcquire {
            agent,
            now_ms,
            claimed_generation,
        } => {
            let current = next.generation_of(agent);
            if *claimed_generation != current {
                return (
                    next,
                    vec![BusyOwnerEffect::FencedStale {
                        agent: agent.clone(),
                        claimed: *claimed_generation,
                        current,
                    }],
                );
            }
            if next.holders.contains_key(agent) {
                // Busy: start or continue bounded hold.
                if let Some(hold) = next.holds.get(agent) {
                    if *now_ms >= hold.until_ms {
                        let held_for = now_ms.saturating_sub(hold.since_ms);
                        next.holds.remove(agent);
                        vec![BusyOwnerEffect::HoldExpired {
                            agent: agent.clone(),
                            held_for_ms: held_for,
                        }]
                    } else {
                        vec![BusyOwnerEffect::HoldContinued {
                            agent: agent.clone(),
                            until_ms: hold.until_ms,
                        }]
                    }
                } else {
                    let until = now_ms.saturating_add(next.hold_bound_ms);
                    next.holds.insert(
                        agent.clone(),
                        Hold {
                            since_ms: *now_ms,
                            until_ms: until,
                            observed_generation: current,
                        },
                    );
                    vec![BusyOwnerEffect::SleepUntilDeadline {
                        agent: agent.clone(),
                        until_ms: until,
                        observed_generation: current,
                    }]
                }
            } else {
                next.holds.remove(agent);
                next.holders.insert(
                    agent.clone(),
                    Holder {
                        generation: current,
                        since_ms: *now_ms,
                    },
                );
                vec![BusyOwnerEffect::Acquired {
                    agent: agent.clone(),
                    generation: current,
                }]
            }
        }
        BusyOwnerMsg::Release {
            agent,
            claimed_generation,
        } => {
            let current = next.generation_of(agent);
            if *claimed_generation != current {
                return (
                    next,
                    vec![BusyOwnerEffect::FencedStale {
                        agent: agent.clone(),
                        claimed: *claimed_generation,
                        current,
                    }],
                );
            }
            match next.holders.remove(agent) {
                Some(h) if h.generation == current => vec![BusyOwnerEffect::Released {
                    agent: agent.clone(),
                    generation: current,
                }],
                Some(_) => vec![BusyOwnerEffect::FencedStale {
                    agent: agent.clone(),
                    claimed: *claimed_generation,
                    current,
                }],
                None => vec![BusyOwnerEffect::ReleaseIgnored {
                    agent: agent.clone(),
                }],
            }
        }
        BusyOwnerMsg::DeadlineWake { agent, now_ms } => {
            if let Some(hold) = next.holds.get(agent).cloned() {
                if *now_ms >= hold.until_ms {
                    let held_for = now_ms.saturating_sub(hold.since_ms);
                    next.holds.remove(agent);
                    vec![BusyOwnerEffect::HoldExpired {
                        agent: agent.clone(),
                        held_for_ms: held_for,
                    }]
                } else {
                    vec![BusyOwnerEffect::HoldContinued {
                        agent: agent.clone(),
                        until_ms: hold.until_ms,
                    }]
                }
            } else {
                vec![]
            }
        }
        BusyOwnerMsg::ForkRebind { agent } => {
            let g = next.generation.entry(agent.clone()).or_insert(0);
            *g = g.saturating_add(1);
            let generation = *g;
            // Stale holder cannot reclaim after fork.
            let stale_holder = next
                .holders
                .get(agent)
                .is_some_and(|h| h.generation != generation);
            if stale_holder {
                next.holders.remove(agent);
            }
            next.holds.remove(agent);
            vec![BusyOwnerEffect::GenerationAdvanced {
                agent: agent.clone(),
                generation,
            }]
        }
    };
    (next, effects)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_free_agent_emits_acquired_at_generation_zero() {
        let m = BusyOwnerModel::test_new();
        let (n, fx) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 100,
                claimed_generation: 0,
            },
        );
        assert_eq!(
            fx,
            vec![BusyOwnerEffect::Acquired {
                agent: "a".into(),
                generation: 0
            }]
        );
        assert!(n.holders.contains_key("a"));
    }

    #[test]
    fn busy_owner_starts_bounded_hold_then_expires() {
        let m = BusyOwnerModel::new(1_000);
        let (m, _) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 0,
                claimed_generation: 0,
            },
        );
        let (m, fx) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 10,
                claimed_generation: 0,
            },
        );
        assert!(matches!(
            &fx[0],
            BusyOwnerEffect::SleepUntilDeadline {
                until_ms: 1_010,
                ..
            }
        ));
        let (_m, fx) = tick(
            &m,
            &BusyOwnerMsg::DeadlineWake {
                agent: "a".into(),
                now_ms: 1_010,
            },
        );
        assert_eq!(
            fx,
            vec![BusyOwnerEffect::HoldExpired {
                agent: "a".into(),
                held_for_ms: 1_000
            }]
        );
    }

    #[test]
    fn generation_fence_rejects_stale_worker() {
        let m = BusyOwnerModel::test_new();
        let (m, _) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 0,
                claimed_generation: 0,
            },
        );
        let (m, fx) = tick(&m, &BusyOwnerMsg::ForkRebind { agent: "a".into() });
        assert!(matches!(
            &fx[0],
            BusyOwnerEffect::GenerationAdvanced { generation: 1, .. }
        ));
        // Stale release at gen 0 is fenced.
        let (_m, fx) = tick(
            &m,
            &BusyOwnerMsg::Release {
                agent: "a".into(),
                claimed_generation: 0,
            },
        );
        assert_eq!(
            fx,
            vec![BusyOwnerEffect::FencedStale {
                agent: "a".into(),
                claimed: 0,
                current: 1
            }]
        );
    }

    #[test]
    fn hold_bound_does_not_restart_on_reacquire() {
        let m = BusyOwnerModel::new(5_000);
        let (m, _) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 0,
                claimed_generation: 0,
            },
        );
        let (m, _) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 100,
                claimed_generation: 0,
            },
        );
        let (_m, fx) = tick(
            &m,
            &BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 200,
                claimed_generation: 0,
            },
        );
        assert_eq!(
            fx,
            vec![BusyOwnerEffect::HoldContinued {
                agent: "a".into(),
                until_ms: 5_100
            }]
        );
    }

    #[test]
    fn same_msg_log_byte_identical_effects() {
        let log = [
            BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 0,
                claimed_generation: 0,
            },
            BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 1,
                claimed_generation: 0,
            },
            BusyOwnerMsg::ForkRebind { agent: "a".into() },
            BusyOwnerMsg::TryAcquire {
                agent: "a".into(),
                now_ms: 2,
                claimed_generation: 0,
            },
        ];
        let replay = |log: &[BusyOwnerMsg]| {
            let mut m = BusyOwnerModel::new(10_000);
            let mut all = Vec::new();
            for msg in log {
                let (n, fx) = tick(&m, msg);
                m = n;
                all.extend(fx);
            }
            all
        };
        assert_eq!(format!("{:?}", replay(&log)), format!("{:?}", replay(&log)));
    }
}
