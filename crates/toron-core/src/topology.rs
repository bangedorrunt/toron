// governed-by: ADR-0041
//! Topologies and their global types as data (redesign §6.2, Phase 4.2).
//!
//! Pure: no clock, no store, no env. [`project`] reads a [`Global`] and
//! returns the contract names one role may send. It is default-deny.
//! [`Topology::Supervisor`] is stored as a fact but [`global_of`] returns
//! [`ProjectError::NotYet`] until Phase 4a; it is not a swarm.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::authority::Role;

/// Which choreography a context runs (redesign §6.2).
///
/// Open and swarm are the modes. The wire word `loop` reads as [`Open`]:
/// a loop is work plus one verify check, not a third mode. Writers emit
/// `open`. Old rows that still say `loop` load as open.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    /// Captain, crews, one verifier. [`G_SWARM`].
    Swarm,
    /// Declared here, projected in Phase 4a. Not a swarm.
    Supervisor,
    /// One agent, or peers with no captain. [`G_OPEN`].
    Open,
}

impl<'de> Deserialize<'de> for Topology {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let word = String::deserialize(deserializer)?;
        Self::parse(&word).ok_or_else(|| {
            serde::de::Error::unknown_variant(&word, &["swarm", "supervisor", "open", "loop"])
        })
    }
}

impl Topology {
    /// Every mode, in a fixed order. `loop` is not a member.
    pub const ALL: [Self; 3] = [Self::Swarm, Self::Supervisor, Self::Open];

    /// The wire spelling (`snake_case`), as serde writes it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Swarm => "swarm",
            Self::Supervisor => "supervisor",
            Self::Open => "open",
        }
    }

    /// Parse the wire spelling. `loop` is open.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "swarm" => Some(Self::Swarm),
            "supervisor" => Some(Self::Supervisor),
            "open" | "loop" => Some(Self::Open),
            _ => None,
        }
    }
}

/// A global type as data. Not a session-type library: five combinators, no
/// stepper, no inference. Struct variants only (`End` is a unit).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Global {
    /// `from_role` may append `contract` for `to_role`.
    Send {
        /// Who sends.
        from_role: Role,
        /// Who the send is for.
        to_role: Role,
        /// Contract name, no major (`task`, `verdict`, …).
        contract: &'static str,
    },
    /// One of the arms.
    Choice {
        /// The alternatives.
        arms: &'static [Global],
    },
    /// Every branch, concurrently.
    Par {
        /// The branches.
        branches: &'static [Global],
    },
    /// The body, repeated.
    Loop {
        /// The body.
        body: &'static Global,
    },
    /// Nothing more.
    End,
}

const SWARM_CREW: &[Global] = &[
    Global::Send {
        from_role: Role::Crew,
        to_role: Role::Captain,
        contract: "progress",
    },
    Global::Send {
        from_role: Role::Crew,
        to_role: Role::Captain,
        contract: "done",
    },
    Global::Send {
        from_role: Role::Crew,
        to_role: Role::Captain,
        contract: "reject",
    },
];

const SWARM_BRANCHES: &[Global] = &[
    Global::Send {
        from_role: Role::Captain,
        to_role: Role::Crew,
        contract: "task",
    },
    Global::Choice { arms: SWARM_CREW },
    Global::Send {
        from_role: Role::Verifier,
        to_role: Role::Captain,
        contract: "verdict",
    },
    Global::Send {
        from_role: Role::Captain,
        to_role: Role::Captain,
        contract: "approval",
    },
];

/// Swarm (redesign §6.2): captain tasks a crew; the crew progresses and
/// finishes; a verifier verdicts; the captain asks for approval. A crew has
/// no `task` and no `verdict`.
pub static G_SWARM: Global = Global::Par {
    branches: SWARM_BRANCHES,
};

const LOOP_INNER: &[Global] = &[
    Global::Send {
        from_role: Role::LoopSelf,
        to_role: Role::LoopSelf,
        contract: "tick",
    },
    Global::Send {
        from_role: Role::LoopSelf,
        to_role: Role::Crew,
        contract: "task",
    },
    Global::Send {
        from_role: Role::Crew,
        to_role: Role::LoopSelf,
        contract: "done",
    },
    Global::Send {
        from_role: Role::LoopSelf,
        to_role: Role::Captain,
        contract: "notify",
    },
    Global::Send {
        from_role: Role::LoopSelf,
        to_role: Role::Captain,
        contract: "not_ready",
    },
];

const LOOP_BODY: Global = Global::Par {
    branches: LOOP_INNER,
};

/// Loop (redesign §6.2): the loop-self ticks and tasks. A peer has no sends here.
pub static G_LOOP: Global = Global::Loop { body: &LOOP_BODY };

const OPEN_BRANCHES: &[Global] = &[
    Global::Send {
        from_role: Role::Peer,
        to_role: Role::Peer,
        contract: "request",
    },
    Global::Send {
        from_role: Role::Peer,
        to_role: Role::Peer,
        contract: "reply",
    },
    Global::Send {
        from_role: Role::Peer,
        to_role: Role::Peer,
        contract: "notify",
    },
];

/// Open (redesign §6.2): peers request and reply. Nobody tasks.
pub static G_OPEN: Global = Global::Par {
    branches: OPEN_BRANCHES,
};

/// Supervisor's global type, declared so 4a has a name to fill. [`project`]
/// on this value is [`ProjectError::NotYet`]. It is `End` on purpose: walking
/// it must not yield swarm verbs, and the pointer check refuses before the walk.
pub static G_SUPERVISOR: Global = Global::End;

/// Why a projection refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectError {
    /// The topology is named but not projected yet (supervisor, until 4a).
    NotYet {
        /// What is missing (`supervisor`).
        what: String,
    },
}

/// The contract names a role may send. Default-deny: absent means refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllowedNow {
    /// Sorted contract names (`task`, `verdict`, …). Not CLI lines.
    pub verbs: Vec<String>,
}

/// The global type a topology projects, or [`ProjectError::NotYet`] for supervisor.
pub fn global_of(topology: Topology) -> Result<&'static Global, ProjectError> {
    match topology {
        Topology::Swarm => Ok(&G_SWARM),
        Topology::Open => Ok(&G_OPEN),
        Topology::Supervisor => Err(ProjectError::NotYet {
            what: "supervisor".into(),
        }),
    }
}

/// Verbs `role` may send in `global` at cursor `state`.
///
/// `state == 0` is the initial view: every send that role owns. `state == n`
/// (`n > 0`) keeps only the n-th branch (1-based) of the outermost [`Global::Par`]
/// or [`Global::Choice`]. A cursor past the end is default-deny, not an error.
/// [`G_SUPERVISOR`] is [`ProjectError::NotYet`] and is not walked.
pub fn project(global: &Global, role: Role, state: u32) -> Result<AllowedNow, ProjectError> {
    if std::ptr::eq(global, &G_SUPERVISOR) {
        return Err(ProjectError::NotYet {
            what: "supervisor".into(),
        });
    }
    let mut verbs = BTreeSet::new();
    collect(global, role, Some(state), &mut verbs);
    Ok(AllowedNow {
        verbs: verbs.into_iter().collect(),
    })
}

fn collect(global: &Global, role: Role, state: Option<u32>, out: &mut BTreeSet<String>) {
    match global {
        Global::Send {
            from_role,
            contract,
            ..
        } => {
            if *from_role == role && matches!(state, None | Some(0)) {
                out.insert((*contract).to_string());
            }
        }
        Global::Choice { arms } | Global::Par { branches: arms } => match state {
            None | Some(0) => {
                for arm in *arms {
                    collect(arm, role, Some(0), out);
                }
            }
            Some(n) => {
                let i = (n as usize).saturating_sub(1);
                if let Some(arm) = arms.get(i) {
                    collect(arm, role, Some(0), out);
                }
            }
        },
        Global::Loop { body } => collect(body, role, state, out),
        Global::End => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn verbs(global: &Global, role: Role) -> Vec<String> {
        project(global, role, 0).expect("projected").verbs
    }

    #[test]
    fn swarm_crew_cannot_send_task_or_verdict() {
        let crew = verbs(&G_SWARM, Role::Crew);
        assert!(
            !crew.iter().any(|v| v == "task" || v == "verdict"),
            "crew projection must not be allow-all: {crew:?}"
        );
        assert!(crew.iter().any(|v| v == "done"), "{crew:?}");
        assert!(
            verbs(&G_SWARM, Role::Captain).iter().any(|v| v == "task"),
            "captain still tasks, so this is not deny-all"
        );
        assert!(
            verbs(&G_SWARM, Role::Verifier)
                .iter()
                .any(|v| v == "verdict")
        );
    }

    #[test]
    fn open_and_loop_differ_from_swarm() {
        let loop_self = verbs(&G_LOOP, Role::LoopSelf);
        let peer = verbs(&G_OPEN, Role::Peer);
        assert!(
            loop_self.iter().any(|v| v == "task"),
            "loop_self may task: {loop_self:?}"
        );
        assert!(
            !peer.iter().any(|v| v == "task"),
            "a peer may not task: {peer:?}"
        );
        assert_ne!(loop_self, verbs(&G_SWARM, Role::LoopSelf));
        assert_ne!(peer, verbs(&G_SWARM, Role::Peer));
        assert!(verbs(&G_SWARM, Role::Peer).is_empty());
    }

    #[test]
    fn supervisor_projection_is_not_yet() {
        assert!(matches!(
            global_of(Topology::Supervisor),
            Err(ProjectError::NotYet { what }) if what == "supervisor"
        ));
        assert!(matches!(
            project(&G_SUPERVISOR, Role::Captain, 0),
            Err(ProjectError::NotYet { .. })
        ));
        // Walking End without the pointer check would be an empty allow-list,
        // which is not the same refusal. The other topologies still project.
        assert!(global_of(Topology::Swarm).is_ok());
    }

    #[test]
    fn a_cursor_selects_one_branch_and_still_denies_crew_task() {
        let at_task = project(&G_SWARM, Role::Crew, 1).unwrap();
        assert!(at_task.verbs.is_empty(), "{at_task:?}");
        let at_crew = project(&G_SWARM, Role::Crew, 2).unwrap();
        assert!(at_crew.verbs.iter().any(|v| v == "progress"));
        assert!(!at_crew.verbs.iter().any(|v| v == "task"));
        assert!(
            project(&G_SWARM, Role::Captain, 99)
                .unwrap()
                .verbs
                .is_empty()
        );
    }

    #[test]
    fn topology_wire_is_snake_case_and_the_fact_denies_unknown_fields() {
        use crate::contracts::context_v1::ContextV1;
        for t in Topology::ALL {
            assert_eq!(Topology::parse(t.as_str()), Some(t));
            assert_eq!(
                serde_json::to_value(t).unwrap(),
                serde_json::json!(t.as_str())
            );
        }
        let fact = ContextV1 {
            topology: Topology::Open,
            generation: 3,
        };
        let v = serde_json::to_value(&fact).unwrap();
        assert_eq!(v["topology"], "open");
        assert_eq!(v["gen"], 3);
        assert_eq!(
            serde_json::from_value::<ContextV1>(serde_json::json!({"topology": "loop", "gen": 3}))
                .unwrap()
                .topology,
            Topology::Open,
            "a stored loop fact is open"
        );
        assert_eq!(Topology::parse("loop"), Some(Topology::Open));
        assert!(v.get("generation").is_none());
        assert!(
            serde_json::from_value::<ContextV1>(serde_json::json!({"topology": "join", "gen": 1}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<ContextV1>(
                serde_json::json!({"topology": "swarm", "gen": 1, "extra": 1})
            )
            .is_err()
        );
    }
}
