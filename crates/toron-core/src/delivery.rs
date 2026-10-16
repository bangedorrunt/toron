// governed-by: ADR-0041
//! The delivery state machine (redesign §3), per `(msg_id, recipient)`.
//!
//! Pure: `std + serde` only, no clock, no store, no kernel types. Every
//! transition is one call to [`DeliveryFsm::step`], which returns the next
//! state or a typed [`Reject`]; the caller persists the state it gets back.
//! The Lean model (`proto/Proto/Delivery.lean`) states the same table and
//! `tests/proto_vectors.rs` replays its vectors against this function.
//!
//! Invariants kept here (redesign §3.3; Lean P2/P3/P4/P5):
//! 1. Monotone rank, except the verdict-gated rework edge; terminals absorb.
//! 3. A `done` whose `gen` is not the claim's `gen` cannot reach `Acted`.
//! 4. `Delivered` only by the recipient's own read at or past the offset.
//! 5. `Proved` only by a verdict whose author is not the `Acted` author.

use serde::{Deserialize, Serialize};

use crate::authority::Role;
use crate::envelope::Class;

/// Where one delivery is (redesign §3.1 states and exits).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Sender journaled the intent.
    Intent,
    /// Sender only: the append's outcome is unknown; reconcile is the exit.
    Unknown,
    /// Appended to the recipient's log.
    Sent,
    /// The recipient's own read passed the offset.
    Delivered,
    /// The recipient accepted the obligation (a Task: claimed with `gen`).
    Acknowledged,
    /// The recipient says done, with evidence.
    Acted,
    /// A verifier confirmed the evidence.
    Proved,
    /// Refused with a reason.
    Rejected,
    /// Deadline passed first.
    Expired,
    /// A higher `gen` replaced the claim.
    Superseded,
    /// Cancelled by control.
    Cancelled,
    /// Failed verification with no rework.
    Failed,
}

impl Phase {
    /// Progress rank. Exits rank above every live phase.
    pub const fn rank(self) -> u8 {
        match self {
            Self::Intent | Self::Unknown => 0,
            Self::Sent => 1,
            Self::Delivered => 2,
            Self::Acknowledged => 3,
            Self::Acted => 4,
            Self::Proved => 5,
            Self::Rejected | Self::Expired | Self::Superseded | Self::Cancelled | Self::Failed => 6,
        }
    }

    /// Spelling used in storage and CLI output.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Intent => "intent",
            Self::Unknown => "unknown",
            Self::Sent => "sent",
            Self::Delivered => "delivered",
            Self::Acknowledged => "acknowledged",
            Self::Acted => "acted",
            Self::Proved => "proved",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::Superseded => "superseded",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    /// Every phase, for tables and vector generation.
    pub const ALL: [Self; 12] = [
        Self::Intent,
        Self::Unknown,
        Self::Sent,
        Self::Delivered,
        Self::Acknowledged,
        Self::Acted,
        Self::Proved,
        Self::Rejected,
        Self::Expired,
        Self::Superseded,
        Self::Cancelled,
        Self::Failed,
    ];
}

/// The terminal phase for a class's success path (redesign §3.1).
pub const fn success_terminal(class: Class) -> Phase {
    match class {
        Class::Notify | Class::Receipt => Phase::Delivered,
        Class::Request | Class::Reply | Class::Control => Phase::Acknowledged,
        Class::Task => Phase::Proved,
    }
}

/// One delivery's state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryState {
    /// Where it is.
    pub phase: Phase,
    /// The message class (fixes where it stops).
    pub class: Class,
    /// The recipient npub (or group stream) this row is for.
    pub recipient: String,
    /// Log offset once appended.
    pub offset: Option<u64>,
    /// Current assignment generation.
    #[serde(rename = "gen")]
    pub generation: u32,
    /// Who holds the claim (Acknowledged and later).
    pub holder: Option<String>,
    /// Who reported done (for proof separation).
    pub doer: Option<String>,
}

impl DeliveryState {
    /// A fresh intent for `recipient`.
    pub fn intent(class: Class, recipient: impl Into<String>, generation: u32) -> Self {
        Self {
            phase: Phase::Intent,
            class,
            recipient: recipient.into(),
            offset: None,
            generation,
            holder: None,
            doer: None,
        }
    }

    /// True when no event can move this delivery.
    pub fn is_terminal(&self) -> bool {
        self.phase.rank() == 6 || self.phase == success_terminal(self.class)
    }
}

/// Facts that move a delivery. Each is emitted by a tool, never by prose.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "event")]
pub enum DeliveryEvent {
    /// The append returned an offset.
    Appended { offset: u64 },
    /// The append's outcome is unknown (crash/timeout).
    AppendUncertain,
    /// Reconcile found the message in the log.
    ReconcileFound { offset: u64 },
    /// Reconcile found nothing: retry the same triple.
    ReconcileAbsent,
    /// A reader committed its cursor to `cursor`.
    Read { reader: String, cursor: u64 },
    /// The deadline passed.
    DeadlinePassed,
    /// `toron ack accept`.
    Accept {
        by: String,
        #[serde(rename = "gen")]
        generation: u32,
    },
    /// `toron ack reject`.
    Refuse { by: String, reason: String },
    /// Progress (renews the lease).
    Progress {
        by: String,
        #[serde(rename = "gen")]
        generation: u32,
    },
    /// `toron done --evidence`.
    Done {
        by: String,
        #[serde(rename = "gen")]
        generation: u32,
    },
    /// A verifier's verdict.
    Verdict {
        by: String,
        pass: bool,
        rework: bool,
    },
    /// Control raised the generation.
    Supersede { new_gen: u32 },
    /// Control cancelled the work.
    Cancel,
}

impl DeliveryEvent {
    /// Short name for errors and vectors.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Appended { .. } => "appended",
            Self::AppendUncertain => "append_uncertain",
            Self::ReconcileFound { .. } => "reconcile_found",
            Self::ReconcileAbsent => "reconcile_absent",
            Self::Read { .. } => "read",
            Self::DeadlinePassed => "deadline_passed",
            Self::Accept { .. } => "accept",
            Self::Refuse { .. } => "refuse",
            Self::Progress { .. } => "progress",
            Self::Done { .. } => "done",
            Self::Verdict { .. } => "verdict",
            Self::Supersede { .. } => "supersede",
            Self::Cancel => "cancel",
        }
    }
}

/// Typed refusals: of a transition, or of an envelope at append.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case", tag = "reject")]
pub enum Reject {
    /// The envelope's schema major is not one this build reads.
    #[error("unsupported envelope v{v}")]
    UnsupportedEnvelope { v: u32 },
    /// The contract name/major is unknown.
    #[error("unsupported contract {name}@{major}")]
    UnsupportedContract { name: String, major: u32 },
    /// The contract does not belong to this class.
    #[error("contract {contract} does not carry class {class:?}")]
    ContractClassMismatch { contract: String, class: Class },
    /// The body does not parse as its contract.
    #[error("body is not a valid {contract}: {error}")]
    BadBody { contract: String, error: String },
    /// Malformed envelope.
    ///
    /// A struct variant, not a newtype: `Reject` is internally tagged
    /// (`tag = "reject"`) and serde cannot serialize a tagged newtype that
    /// holds a string, so a newtype here made every CLI refusal path panic
    /// (bd-reject-badenvelope-panic-bhls). Build it with
    /// [`Reject::bad_envelope`].
    #[error("bad envelope: {why}")]
    BadEnvelope {
        /// What was wrong.
        why: String,
    },
    /// An address is not an npub (names are never addresses).
    #[error("{field} is not an npub (64 lowercase hex): {value:?}")]
    BadAddress { field: &'static str, value: String },
    /// `from` must be the producer.
    #[error("from {from} is not the producer {producer}")]
    SenderIsNotProducer { from: String, producer: String },
    /// Groups carry Notify only in this slice.
    #[error("a group recipient takes notify only, not {class:?}")]
    GroupNotifyOnly { class: Class },
    /// Only the recipient's own key may move its delivery.
    #[error("{actor} is not the recipient {recipient}")]
    NotRecipient { actor: String, recipient: String },
    /// The cursor does not cover the offset.
    #[error("cursor {cursor} is behind offset {offset}")]
    CursorBehind { cursor: u64, offset: u64 },
    /// A generation that is not the current one.
    #[error("stale gen {sent}, current {current}")]
    StaleGen { sent: u32, current: u32 },
    /// Only the claim holder may report progress or done.
    #[error("{actor} does not hold the claim")]
    NotHolder { actor: String },
    /// A verdict by the doer is not proof.
    #[error("{actor} reported done and cannot verify it")]
    SelfVerdict { actor: String },
    /// The class does not admit this event.
    #[error("class {class:?} does not admit {event}")]
    NotApplicable { class: Class, event: &'static str },
    /// Terminal deliveries never move.
    #[error("delivery is terminal ({phase:?})")]
    Terminal { phase: Phase },
    /// No such edge.
    #[error("no transition from {phase:?} on {event}")]
    IllegalTransition { phase: Phase, event: &'static str },
    /// The sender's role does not allow this act in this context
    /// (redesign §6.1, Phase 4.1). `role` is the sender's role there (`None`:
    /// a governed context and the sender holds no role fact). `contract` is
    /// `name@major`, or the verb name `done` for the one non-envelope verb.
    /// `expected` is what the role may send instead, as `class/contract`
    /// (`class/*` = any contract of the class) and `done`; for a refused role
    /// fact it is the roles the issuer could grant (`role@1:<role>`), or
    /// `operator`.
    ///
    /// A struct variant (the enum is internally tagged).
    #[error(
        "{} may not send {contract} ({class:?}); allowed: [{}]",
        role_label(.role),
        .expected.join(", ")
    )]
    Unauthorized {
        role: Option<Role>,
        class: Class,
        contract: String,
        expected: Vec<String>,
    },
    /// The act names a topology this build stores but does not project yet
    /// (supervisor, until Phase 4a). Not a swarm send.
    ///
    /// A struct variant (the enum is internally tagged).
    #[error("not yet: {what}")]
    NotYet {
        /// What is not projected (`supervisor`).
        what: String,
    },
    /// A crew verb on a task whose steer is still `pending` (Phase 4a.2).
    /// `id` is that steer message's key; `op` is its `redirect|pause|resume|cancel`.
    /// The steer itself, and an ack of it, are not this refusal.
    ///
    /// A struct variant (the enum is internally tagged).
    #[error("steer {id} ({op}) is unacked")]
    SteerPending {
        /// The pending steer message (`producer:epoch:seq`).
        id: String,
        /// The steer op that is waiting.
        op: String,
    },
    /// A lead fact, or a steer naming a lead epoch, that is not strictly
    /// newer than the epoch on record (Phase 4a.3). No lead row: this is not
    /// raised, and the steer is not given an invented epoch.
    ///
    /// A struct variant (the enum is internally tagged).
    #[error("stale sup_epoch {sent}, current {current}")]
    StaleSupEpoch {
        /// The epoch the sender named.
        sent: u64,
        /// The epoch on record.
        current: u64,
    },
}

fn role_label(role: &Option<Role>) -> String {
    role.map_or_else(
        || "a sender with no role here".to_string(),
        |r| format!("role {r}"),
    )
}

impl Reject {
    /// A malformed envelope, with the reason.
    pub fn bad_envelope(why: impl Into<String>) -> Self {
        Self::BadEnvelope { why: why.into() }
    }
}

/// The pure transition function.
pub struct DeliveryFsm;

impl DeliveryFsm {
    /// One step. Never mutates; returns the next state or why not.
    pub fn step(s: &DeliveryState, e: &DeliveryEvent) -> Result<DeliveryState, Reject> {
        use DeliveryEvent as E;
        use Phase as P;
        if s.is_terminal() {
            return Err(Reject::Terminal { phase: s.phase });
        }
        let illegal = || Reject::IllegalTransition {
            phase: s.phase,
            event: e.name(),
        };
        let to = |phase: Phase| DeliveryState { phase, ..s.clone() };
        let obligation = !matches!(s.class, Class::Notify | Class::Receipt);
        match (s.phase, e) {
            (P::Intent, E::Appended { offset }) => Ok(DeliveryState {
                offset: Some(*offset),
                ..to(P::Sent)
            }),
            (P::Intent, E::AppendUncertain) => Ok(to(P::Unknown)),
            (P::Unknown, E::ReconcileFound { offset }) => Ok(DeliveryState {
                offset: Some(*offset),
                ..to(P::Sent)
            }),
            (P::Unknown, E::ReconcileAbsent) => Ok(to(P::Intent)),
            (P::Sent, E::Read { reader, cursor }) => {
                if *reader != s.recipient {
                    return Err(Reject::NotRecipient {
                        actor: reader.clone(),
                        recipient: s.recipient.clone(),
                    });
                }
                let offset = s.offset.ok_or_else(illegal)?;
                if *cursor < offset {
                    return Err(Reject::CursorBehind {
                        cursor: *cursor,
                        offset,
                    });
                }
                Ok(to(P::Delivered))
            }
            (P::Sent | P::Delivered, E::DeadlinePassed) => Ok(to(P::Expired)),
            (P::Delivered, E::Accept { by, generation }) => {
                if !obligation {
                    return Err(Reject::NotApplicable {
                        class: s.class,
                        event: e.name(),
                    });
                }
                if *by != s.recipient {
                    return Err(Reject::NotRecipient {
                        actor: by.clone(),
                        recipient: s.recipient.clone(),
                    });
                }
                if *generation != s.generation {
                    return Err(Reject::StaleGen {
                        sent: *generation,
                        current: s.generation,
                    });
                }
                Ok(DeliveryState {
                    holder: Some(by.clone()),
                    ..to(P::Acknowledged)
                })
            }
            (P::Delivered, E::Refuse { by, .. }) => {
                if !obligation {
                    return Err(Reject::NotApplicable {
                        class: s.class,
                        event: e.name(),
                    });
                }
                if *by != s.recipient {
                    return Err(Reject::NotRecipient {
                        actor: by.clone(),
                        recipient: s.recipient.clone(),
                    });
                }
                Ok(to(P::Rejected))
            }
            (P::Acknowledged, E::Progress { by, generation } | E::Done { by, generation }) => {
                if s.holder.as_deref() != Some(by.as_str()) {
                    return Err(Reject::NotHolder { actor: by.clone() });
                }
                if *generation != s.generation {
                    return Err(Reject::StaleGen {
                        sent: *generation,
                        current: s.generation,
                    });
                }
                match e {
                    E::Progress { .. } => Ok(s.clone()),
                    _ if s.class != Class::Task => Err(Reject::NotApplicable {
                        class: s.class,
                        event: e.name(),
                    }),
                    _ => Ok(DeliveryState {
                        doer: Some(by.clone()),
                        ..to(P::Acted)
                    }),
                }
            }
            (P::Acted, E::Verdict { by, pass, rework }) => {
                if s.doer.as_deref() == Some(by.as_str()) {
                    return Err(Reject::SelfVerdict { actor: by.clone() });
                }
                Ok(match (pass, rework) {
                    (true, _) => to(P::Proved),
                    (false, true) => DeliveryState {
                        doer: None,
                        ..to(P::Acknowledged)
                    },
                    (false, false) => to(P::Failed),
                })
            }
            (P::Acknowledged | P::Acted, E::Supersede { new_gen }) => {
                if *new_gen <= s.generation {
                    return Err(Reject::StaleGen {
                        sent: *new_gen,
                        current: s.generation,
                    });
                }
                Ok(DeliveryState {
                    generation: *new_gen,
                    ..to(P::Superseded)
                })
            }
            (P::Acknowledged, E::Cancel) => Ok(to(P::Cancelled)),
            _ => Err(illegal()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const B: &str = "bbbb";

    fn sent(class: Class) -> DeliveryState {
        DeliveryFsm::step(
            &DeliveryState::intent(class, B, 1),
            &DeliveryEvent::Appended { offset: 7 },
        )
        .unwrap()
    }

    #[test]
    fn task_runs_to_proved_and_stops() {
        let mut s = sent(Class::Task);
        for e in [
            DeliveryEvent::Read {
                reader: B.into(),
                cursor: 7,
            },
            DeliveryEvent::Accept {
                by: B.into(),
                generation: 1,
            },
            DeliveryEvent::Progress {
                by: B.into(),
                generation: 1,
            },
            DeliveryEvent::Done {
                by: B.into(),
                generation: 1,
            },
            DeliveryEvent::Verdict {
                by: "vvvv".into(),
                pass: true,
                rework: false,
            },
        ] {
            s = DeliveryFsm::step(&s, &e).unwrap();
        }
        assert_eq!(s.phase, Phase::Proved);
        assert!(matches!(
            DeliveryFsm::step(&s, &DeliveryEvent::Cancel),
            Err(Reject::Terminal { .. })
        ));
    }

    #[test]
    fn only_the_recipient_reads_and_only_past_the_offset() {
        let s = sent(Class::Notify);
        assert!(matches!(
            DeliveryFsm::step(
                &s,
                &DeliveryEvent::Read {
                    reader: "aaaa".into(),
                    cursor: 9
                }
            ),
            Err(Reject::NotRecipient { .. })
        ));
        assert!(matches!(
            DeliveryFsm::step(
                &s,
                &DeliveryEvent::Read {
                    reader: B.into(),
                    cursor: 6
                }
            ),
            Err(Reject::CursorBehind {
                cursor: 6,
                offset: 7
            })
        ));
        let d = DeliveryFsm::step(
            &s,
            &DeliveryEvent::Read {
                reader: B.into(),
                cursor: 7,
            },
        )
        .unwrap();
        assert!(d.is_terminal(), "notify stops at delivered");
    }

    #[test]
    fn stale_gen_done_never_reaches_acted() {
        let mut s = sent(Class::Task);
        s = DeliveryFsm::step(
            &s,
            &DeliveryEvent::Read {
                reader: B.into(),
                cursor: 7,
            },
        )
        .unwrap();
        s = DeliveryFsm::step(
            &s,
            &DeliveryEvent::Accept {
                by: B.into(),
                generation: 1,
            },
        )
        .unwrap();
        assert!(matches!(
            DeliveryFsm::step(
                &s,
                &DeliveryEvent::Done {
                    by: B.into(),
                    generation: 0
                }
            ),
            Err(Reject::StaleGen {
                sent: 0,
                current: 1
            })
        ));
        assert!(matches!(
            DeliveryFsm::step(
                &s,
                &DeliveryEvent::Done {
                    by: "cccc".into(),
                    generation: 1
                }
            ),
            Err(Reject::NotHolder { .. })
        ));
    }

    #[test]
    fn the_doer_cannot_prove_its_own_work() {
        let mut s = sent(Class::Task);
        for e in [
            DeliveryEvent::Read {
                reader: B.into(),
                cursor: 7,
            },
            DeliveryEvent::Accept {
                by: B.into(),
                generation: 1,
            },
            DeliveryEvent::Done {
                by: B.into(),
                generation: 1,
            },
        ] {
            s = DeliveryFsm::step(&s, &e).unwrap();
        }
        assert!(matches!(
            DeliveryFsm::step(
                &s,
                &DeliveryEvent::Verdict {
                    by: B.into(),
                    pass: true,
                    rework: false
                }
            ),
            Err(Reject::SelfVerdict { .. })
        ));
        let rework = DeliveryFsm::step(
            &s,
            &DeliveryEvent::Verdict {
                by: "vvvv".into(),
                pass: false,
                rework: true,
            },
        )
        .unwrap();
        assert_eq!(
            rework.phase,
            Phase::Acknowledged,
            "the verdict-gated rework edge"
        );
    }

    #[test]
    fn unknown_has_only_reconcile_exits() {
        let u = DeliveryFsm::step(
            &DeliveryState::intent(Class::Task, B, 1),
            &DeliveryEvent::AppendUncertain,
        )
        .unwrap();
        assert_eq!(u.phase, Phase::Unknown);
        assert!(DeliveryFsm::step(&u, &DeliveryEvent::Appended { offset: 1 }).is_err());
        assert_eq!(
            DeliveryFsm::step(&u, &DeliveryEvent::ReconcileAbsent)
                .unwrap()
                .phase,
            Phase::Intent
        );
        assert_eq!(
            DeliveryFsm::step(&u, &DeliveryEvent::ReconcileFound { offset: 3 })
                .unwrap()
                .phase,
            Phase::Sent
        );
    }
}
