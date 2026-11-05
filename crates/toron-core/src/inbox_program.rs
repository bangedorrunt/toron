// governed-by: ADR-0040, ADR-0041
//! `InboxProgram`: the one fold that owns the recipient side of the wake
//! (redesign §4, Phase 3). It replaces the doorbell's `DoorbellModel` and
//! `WakeModel`: per-recipient head, cursor, binding and ring bookkeeping
//! live in one serializable model, and every decision comes from the pure
//! [`crate::wake::should_ring`].
//!
//! The fold is level-triggered. Hints (`Head`, `Hint`) only make it look
//! sooner; a periodic `Tick` evaluates every recipient from durable facts,
//! so correctness holds when every hint is lost. The fold keeps no clock
//! of its own: time arrives as `now_ms` on `Tick` and `Hint`.
//!
//! What it never does: ring on herdr's word alone, or count a ring as
//! delivered. `rings_since_advance` resets **only** on a `Cursor` fact
//! (the recipient ran `toron next`). At the ring budget it answers with the
//! `deaf{r, wm}` fact, a `Control/notify` for the supervisor and a captain
//! notification, once per watermark, and stops ringing.
//!
//! A deaf recipient is not rung again. A binding to a *different* pane (a
//! restarted incarnation) gets a fresh budget; re-verifying the same pane
//! does not.
//!
//! A binding that is unverified, stale (generation behind the model's) or
//! whose pane is gone is never typed into: the fold asks the host to
//! `Reverify` it each tick and, if that persists for the ring budget,
//! escalates with cause `unbound`. An `EventsLost` or `PlaneRestarted`
//! input bumps the generation, which stales every binding at once and so
//! forces a full re-evaluation before any ring.
//!
//! The fold is pure: no clock, no store, no `HashMap`. The host (the
//! flywheel doorbell) interprets the commands: `Ring` types the constant
//! wake line by pane id and binding generation, `Reverify` re-runs the
//! env + `report_metadata` check, the three escalation commands record the
//! fact and notify.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::kernel::{Boot, Cmd as KCmd, Durability, Failure, Program, Update};
use crate::wake::{PaneState, RingDecision, RingInput, Urgency, should_ring, wake_line};

/// A recipient's pane binding: a fact the host establishes by verifying
/// the pane's env and `report_metadata`, never inferred from a name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Binding {
    /// herdr pane id the wake line is typed into.
    pub pane_id: String,
    /// plane session the pane belongs to.
    #[serde(alias = "herdr_session")]
    pub plane_session: String,
    /// The generation this binding was verified at.
    #[serde(rename = "gen")]
    pub generation: u64,
    /// Whether verification succeeded.
    pub verified: bool,
    /// Who or what verified it (for the audit trail).
    pub verified_by: String,
}

/// Everything the fold knows about one recipient.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecipientWake {
    /// Newest mail offset for the recipient.
    pub head: u64,
    /// Committed cursor (advances only through a `Cursor` fact).
    pub cursor: u64,
    /// Urgency of the unread mail.
    pub urgency: Urgency,
    /// The pane binding, if one was ever reported.
    pub binding: Option<Binding>,
    /// herdr's last word on the pane.
    pub pane: PaneState,
    /// Watermark of the last ring.
    pub last_ring_wm: Option<u64>,
    /// When the last ring went out.
    pub last_ring_at_ms: Option<u64>,
    /// Rings since the cursor last advanced.
    pub rings_since_advance: u32,
    /// Consecutive evaluations that found mail but no usable binding.
    pub unbound_ticks: u32,
    /// The watermark a `deaf` fact was emitted for, if any.
    pub deaf: Option<u64>,
}

impl Default for RecipientWake {
    fn default() -> Self {
        Self {
            head: 0,
            cursor: 0,
            urgency: Urgency::Normal,
            binding: None,
            pane: PaneState::Unknown,
            last_ring_wm: None,
            last_ring_at_ms: None,
            rings_since_advance: 0,
            unbound_ticks: 0,
            deaf: None,
        }
    }
}

/// The fold's model. BTreeMap, never HashMap. Serializable: the host
/// restarts from `{model, offset}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxModel {
    /// The current binding generation; a bump stales every binding.
    pub generation: u64,
    /// Per recipient npub.
    pub recipients: BTreeMap<String, RecipientWake>,
    /// `events_lost` frames seen (each also bumped the generation).
    pub events_lost: u64,
    /// Commands the host reported as failed (the next tick decides again).
    pub failed_cmds: u64,
}

/// Why the generation moved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Invalidate {
    /// The herdr event stream shed frames: the model may have missed state.
    EventsLost,
    /// hc restarted: pane ids are not the ones we verified.
    #[serde(alias = "herdr_restarted", alias = "HerdrRestarted")]
    PlaneRestarted,
}

/// Inputs: durable facts from the log and the host, plus time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "msg", rename_all = "snake_case")]
pub enum InboxMsg {
    /// Mail for `recipient` now reaches `head` (a hint: may repeat or be
    /// lost; the `Tick` reads the same fact).
    Head {
        /// Recipient npub.
        recipient: String,
        /// Newest offset.
        head: u64,
        /// Urgency of the newest mail.
        urgency: Urgency,
    },
    /// The recipient's cursor is now `cursor` (it ran `toron next`).
    Cursor {
        /// Recipient npub.
        recipient: String,
        /// Committed cursor.
        cursor: u64,
    },
    /// A binding fact from the host.
    Bound {
        /// Recipient npub.
        recipient: String,
        /// herdr pane id.
        pane_id: String,
        /// plane session.
        #[serde(alias = "herdr_session")]
        plane_session: String,
        /// Generation the verification ran at.
        #[serde(rename = "gen")]
        generation: u64,
        /// Did verification pass.
        verified: bool,
        /// Who verified.
        verified_by: String,
    },
    /// herdr's state for the recipient's pane changed.
    Pane {
        /// Recipient npub.
        recipient: String,
        /// New state.
        state: PaneState,
    },
    /// The generation moves: every binding goes stale.
    Invalidate(Invalidate),
    /// Evaluate one recipient now.
    Hint {
        /// Recipient npub.
        recipient: String,
        /// The caller's clock.
        now_ms: u64,
    },
    /// Evaluate every recipient (the periodic full re-evaluation).
    Tick {
        /// The caller's clock.
        now_ms: u64,
    },
    /// A command failed in the host; the next tick decides again.
    CmdFailed(String),
}

/// Why a recipient is deaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeafCause {
    /// Rung the whole budget; the cursor never moved.
    NoAnswer,
    /// Mail waited the whole budget with no usable binding.
    Unbound,
}

impl DeafCause {
    fn tag(self) -> &'static str {
        match self {
            Self::NoAnswer => "no_answer",
            Self::Unbound => "unbound",
        }
    }
}

/// Outputs: intents for the host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum InboxCmd {
    /// Type the constant wake line into the pane, by pane id and binding
    /// generation (never by agent name).
    Ring {
        /// Recipient npub.
        recipient: String,
        /// herdr pane id.
        pane_id: String,
        /// plane session.
        #[serde(alias = "herdr_session")]
        plane_session: String,
        /// The binding generation to check before typing.
        binding_gen: u64,
        /// The watermark the line names.
        wm: u64,
        /// The exact line to type ([`wake_line`]).
        line: String,
    },
    /// Re-run binding verification (env + `report_metadata`).
    Reverify {
        /// Recipient npub.
        recipient: String,
    },
    /// The `deaf{r, wm}` fact.
    RecordDeaf {
        /// Recipient npub.
        recipient: String,
        /// Unanswered watermark.
        wm: u64,
        /// Why.
        cause: DeafCause,
    },
    /// A `Control/notify` envelope for the supervisor's inbox.
    NotifySupervisor {
        /// Recipient npub.
        recipient: String,
        /// Unanswered watermark.
        wm: u64,
        /// Why.
        cause: DeafCause,
    },
    /// A herdr notification to the captain.
    NotifyCaptain {
        /// Recipient npub.
        recipient: String,
        /// Unanswered watermark.
        wm: u64,
        /// Why.
        cause: DeafCause,
    },
}

impl KCmd for InboxCmd {
    fn durability(&self) -> Durability {
        match self {
            Self::Ring { .. } | Self::Reverify { .. } => Durability::Ephemeral,
            Self::RecordDeaf {
                recipient,
                wm,
                cause,
            } => Durability::Durable {
                key: format!("deaf:{recipient}:{wm}:{}", cause.tag()),
            },
            Self::NotifySupervisor {
                recipient,
                wm,
                cause,
            } => Durability::Durable {
                key: format!("deaf-supervisor:{recipient}:{wm}:{}", cause.tag()),
            },
            Self::NotifyCaptain {
                recipient,
                wm,
                cause,
            } => Durability::Durable {
                key: format!("deaf-captain:{recipient}:{wm}:{}", cause.tag()),
            },
        }
    }
}

/// The program.
pub struct InboxProgram;

fn escalate(
    r: &mut RecipientWake,
    recipient: &str,
    wm: u64,
    cause: DeafCause,
    cmds: &mut Vec<InboxCmd>,
) {
    if r.deaf == Some(wm) {
        return;
    }
    r.deaf = Some(wm);
    let recipient = recipient.to_string();
    cmds.push(InboxCmd::RecordDeaf {
        recipient: recipient.clone(),
        wm,
        cause,
    });
    cmds.push(InboxCmd::NotifySupervisor {
        recipient: recipient.clone(),
        wm,
        cause,
    });
    cmds.push(InboxCmd::NotifyCaptain {
        recipient,
        wm,
        cause,
    });
}

fn evaluate(
    generation: u64,
    recipient: &str,
    r: &mut RecipientWake,
    now_ms: u64,
    cmds: &mut Vec<InboxCmd>,
) {
    let (verified, bgen) = r
        .binding
        .as_ref()
        .map_or((false, 0), |b| (b.verified, b.generation));
    let input = RingInput {
        head: r.head,
        cursor: r.cursor,
        binding_verified: verified,
        binding_gen: bgen,
        current_gen: generation,
        pane_state: r.pane,
        last_ring_wm: r.last_ring_wm,
        last_ring_at_ms: r.last_ring_at_ms,
        rings_since_advance: r.rings_since_advance,
        now_ms,
        urgency: r.urgency,
    };
    match should_ring(&input) {
        RingDecision::NothingUnread => r.unbound_ticks = 0,
        RingDecision::Unbound => {
            r.unbound_ticks = r.unbound_ticks.saturating_add(1);
            cmds.push(InboxCmd::Reverify {
                recipient: recipient.to_string(),
            });
            if r.unbound_ticks >= r.urgency.max_rings() {
                escalate(r, recipient, r.head, DeafCause::Unbound, cmds);
            }
        }
        RingDecision::Busy | RingDecision::Hold { .. } => r.unbound_ticks = 0,
        RingDecision::Ring { wm } => {
            r.unbound_ticks = 0;
            r.last_ring_wm = Some(wm);
            r.last_ring_at_ms = Some(now_ms);
            r.rings_since_advance = r.rings_since_advance.saturating_add(1);
            if let Some(b) = &r.binding {
                cmds.push(InboxCmd::Ring {
                    recipient: recipient.to_string(),
                    pane_id: b.pane_id.clone(),
                    plane_session: b.plane_session.clone(),
                    binding_gen: b.generation,
                    wm,
                    line: wake_line(wm),
                });
            }
        }
        RingDecision::Escalate { wm } => {
            r.unbound_ticks = 0;
            escalate(r, recipient, wm, DeafCause::NoAnswer, cmds);
        }
    }
}

impl Program for InboxProgram {
    type Model = InboxModel;
    type Msg = InboxMsg;
    type Cmd = InboxCmd;
    type Err = String;

    fn init(_boot: &Boot) -> Update<InboxModel, InboxCmd> {
        Update::quiet(InboxModel::default())
    }

    fn update(model: &InboxModel, msg: &InboxMsg) -> Update<InboxModel, InboxCmd> {
        let mut next = model.clone();
        let mut cmds = Vec::new();
        match msg {
            InboxMsg::Head {
                recipient,
                head,
                urgency,
            } => {
                let r = next.recipients.entry(recipient.clone()).or_default();
                let was_unread = r.head > r.cursor;
                r.urgency = if was_unread && r.urgency == Urgency::Urgent {
                    Urgency::Urgent
                } else {
                    *urgency
                };
                r.head = r.head.max(*head);
            }
            InboxMsg::Cursor { recipient, cursor } => {
                let r = next.recipients.entry(recipient.clone()).or_default();
                if *cursor > r.cursor {
                    r.cursor = *cursor;
                    r.rings_since_advance = 0;
                    r.unbound_ticks = 0;
                    r.deaf = None;
                    if r.cursor >= r.head {
                        r.urgency = Urgency::Normal;
                    }
                }
            }
            InboxMsg::Bound {
                recipient,
                pane_id,
                plane_session,
                generation,
                verified,
                verified_by,
            } => {
                let r = next.recipients.entry(recipient.clone()).or_default();
                let new_pane = r.binding.as_ref().is_none_or(|b| b.pane_id != *pane_id);
                if new_pane {
                    r.rings_since_advance = 0;
                    r.unbound_ticks = 0;
                    r.deaf = None;
                }
                r.binding = Some(Binding {
                    pane_id: pane_id.clone(),
                    plane_session: plane_session.clone(),
                    generation: *generation,
                    verified: *verified,
                    verified_by: verified_by.clone(),
                });
            }
            InboxMsg::Pane { recipient, state } => {
                next.recipients.entry(recipient.clone()).or_default().pane = *state;
            }
            InboxMsg::Invalidate(why) => {
                next.generation = next.generation.saturating_add(1);
                if *why == Invalidate::EventsLost {
                    next.events_lost = next.events_lost.saturating_add(1);
                }
            }
            InboxMsg::Hint { recipient, now_ms } => {
                let generation = next.generation;
                if let Some(r) = next.recipients.get_mut(recipient) {
                    evaluate(generation, recipient, r, *now_ms, &mut cmds);
                }
            }
            InboxMsg::Tick { now_ms } => {
                let generation = next.generation;
                for (recipient, r) in &mut next.recipients {
                    evaluate(generation, recipient, r, *now_ms, &mut cmds);
                }
            }
            InboxMsg::CmdFailed(_) => {
                next.failed_cmds = next.failed_cmds.saturating_add(1);
            }
        }
        Update::with_commands(next, cmds)
    }

    fn failure(failure: &Failure<String>) -> InboxMsg {
        InboxMsg::CmdFailed(format!("{failure:?}"))
    }
}
