// governed-by: ADR-0031
//! Pure dispatch decision core (ADR-0031 S3/A5, bead bd-hie0).
//!
//! Verbatim move of the `toron-workflow` dispatch-pool kernel — bounded concurrency + one-in-flight-per-agent + FIFO queue —
//! into a dependency-free module: **std + serde only**. No tokio, no Nostr
//! client types, no flywheel/doorbell imports (fenced by test below).
//!
//! ## Shape (XState `setup()` split)
//!
//! - [`Guard`]s are named pure predicates with [`Guard::And`]/`Or`/`Not`
//!   composition — no inline boolean soup in [`tick`] arms.
//! - [`DispatchEffect`]s are parameterized data — no closures, no captured
//!   environment inside the machine.
//! - [`machine_config`] serializes the machine (states, transitions, guard
//!   names) to JSON so a future visualizer renders it without the runtime.
//!
//! ## Law
//!
//! One `Model`, one `Msg` union, one [`tick`]: `tick(&Model, &Msg) ->
//! (Model, Vec<Effect>)`. Effects are data; the thin host (today
//! `DispatchPool`, adopting follow-on) executes them. Same Msg log →
//! byte-identical Effects (tested).
//!
//! The core routes on ids only: job specs live in the bus kind-43001 store
//! (lossless-projection rule), so [`QueuedRef`] carries `job_id` +
//! `assignee` and nothing else.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

/// Model: the whole dispatch state. Clone-and-return keeps `tick` pure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchModel {
    /// Global max in-flight jobs (bounded semaphore, Buzz default 100).
    pub max_concurrency: usize,
    /// Global in-flight count (the semaphore counter).
    pub used: usize,
    /// Per-agent in-flight count.
    pub in_flight: BTreeMap<String, usize>,
    /// FIFO queue mirror (durable record lives in the bus store).
    pub queue: VecDeque<QueuedRef>,
}

impl DispatchModel {
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            max_concurrency,
            used: 0,
            in_flight: BTreeMap::new(),
            queue: VecDeque::new(),
        }
    }

    /// Test constructor free of daemon/network/PTY (ADR-0031 A8).
    pub fn test_new() -> Self {
        Self::new(100)
    }
}

/// Queue entry: identity + routing key only (spec stays in the bus store).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedRef {
    pub job_id: String,
    pub assignee: String,
}

/// All state changes enter as messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DispatchMsg {
    /// Buffer a job (mirror; caller persists kind-43001 first).
    Submit { job_id: String, assignee: String },
    /// Request a dispatch slot for an agent.
    Acquire { assignee: String },
    /// Free an agent's slot (permit drop).
    Release { assignee: String },
    /// Dispatch the first queued job whose assignee passes the admit guard.
    Poll,
}

/// Intents the core journals; the host executes. No I/O in [`tick`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DispatchEffect {
    Queued {
        job_id: String,
    },
    Admitted {
        assignee: String,
    },
    Denied {
        assignee: String,
        reason: DenyReason,
    },
    Released {
        assignee: String,
    },
    Dispatched {
        job_id: String,
        assignee: String,
    },
}

/// Why an acquire was denied (mirrors pool branch order: agent first).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DenyReason {
    AgentBusy,
    GlobalCap,
}

/// Named pure predicates over the model. Composite variants compose with
/// and/or/not — tick arms name guards, never inline conditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Guard {
    /// Agent has no in-flight job (one-in-flight-per-agent).
    AgentFree,
    /// Global in-flight below max (semaphore permits remain).
    GlobalCapacity,
    And(Box<Guard>, Box<Guard>),
    Or(Box<Guard>, Box<Guard>),
    Not(Box<Guard>),
}

impl Guard {
    pub fn and(a: Guard, b: Guard) -> Guard {
        Guard::And(Box::new(a), Box::new(b))
    }

    pub fn or(a: Guard, b: Guard) -> Guard {
        Guard::Or(Box::new(a), Box::new(b))
    }

    pub fn not(a: Guard) -> Guard {
        Guard::Not(Box::new(a))
    }

    /// Evaluate against the model for an assignee. Pure.
    pub fn eval(&self, model: &DispatchModel, assignee: &str) -> bool {
        match self {
            Guard::AgentFree => model.in_flight.get(assignee).copied().unwrap_or(0) == 0,
            Guard::GlobalCapacity => model.used < model.max_concurrency,
            Guard::And(a, b) => a.eval(model, assignee) && b.eval(model, assignee),
            Guard::Or(a, b) => a.eval(model, assignee) || b.eval(model, assignee),
            Guard::Not(a) => !a.eval(model, assignee),
        }
    }

    /// Canonical name for config/docs (leaf name or operator expression).
    pub fn name(&self) -> String {
        match self {
            Guard::AgentFree => "agent_free".to_string(),
            Guard::GlobalCapacity => "global_capacity".to_string(),
            Guard::And(a, b) => format!("({} and {})", a.name(), b.name()),
            Guard::Or(a, b) => format!("({} or {})", a.name(), b.name()),
            Guard::Not(a) => format!("(not {})", a.name()),
        }
    }
}

/// The admit gate: agent-free AND global-capacity (pool branch order).
pub fn can_admit(model: &DispatchModel, assignee: &str) -> bool {
    Guard::and(Guard::AgentFree, Guard::GlobalCapacity).eval(model, assignee)
}

fn admit(model: &mut DispatchModel, assignee: &str) {
    *model.in_flight.entry(assignee.to_string()).or_insert(0) += 1;
    model.used += 1;
}

fn deny_reason(model: &DispatchModel, assignee: &str) -> DenyReason {
    if model.in_flight.get(assignee).copied().unwrap_or(0) >= 1 {
        DenyReason::AgentBusy
    } else {
        DenyReason::GlobalCap
    }
}

/// Pure transition: one model in, new model + journaled effects out.
/// Mirrors `DispatchPool` branch-for-branch (verbatim-move semantics).
pub fn tick(model: &DispatchModel, msg: &DispatchMsg) -> (DispatchModel, Vec<DispatchEffect>) {
    let mut next = model.clone();
    let effects = match msg {
        DispatchMsg::Submit { job_id, assignee } => {
            next.queue.push_back(QueuedRef {
                job_id: job_id.clone(),
                assignee: assignee.clone(),
            });
            vec![DispatchEffect::Queued {
                job_id: job_id.clone(),
            }]
        }
        DispatchMsg::Acquire { assignee } => {
            if can_admit(&next, assignee) {
                admit(&mut next, assignee);
                vec![DispatchEffect::Admitted {
                    assignee: assignee.clone(),
                }]
            } else {
                vec![DispatchEffect::Denied {
                    assignee: assignee.clone(),
                    reason: deny_reason(&next, assignee),
                }]
            }
        }
        DispatchMsg::Release { assignee } => {
            if let Some(count) = next.in_flight.get_mut(assignee) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    next.in_flight.remove(assignee);
                }
            }
            next.used = next.used.saturating_sub(1);
            vec![DispatchEffect::Released {
                assignee: assignee.clone(),
            }]
        }
        DispatchMsg::Poll => {
            let idx = next
                .queue
                .iter()
                .position(|job| can_admit(&next, &job.assignee));
            match idx {
                Some(i) => {
                    let job = next.queue.remove(i).expect("polled index exists");
                    admit(&mut next, &job.assignee);
                    vec![DispatchEffect::Dispatched {
                        job_id: job.job_id,
                        assignee: job.assignee,
                    }]
                }
                None => vec![],
            }
        }
    };
    (next, effects)
}

/// Serializable machine config (states, transitions, guard names) — the
/// visualizer input. Debug-printable at minimum; JSON because it is cheap.
pub fn machine_config() -> serde_json::Value {
    serde_json::json!({
        "machine": "dispatch_core",
        "states": ["model"],
        "transitions": [
            {"on": "Submit", "guard": null, "effects": ["Queued"]},
            {"on": "Acquire", "guard": can_admit_name(), "effects": ["Admitted", "Denied"]},
            {"on": "Release", "guard": null, "effects": ["Released"]},
            {"on": "Poll", "guard": can_admit_name(), "effects": ["Dispatched"]},
        ],
        "guards": ["agent_free", "global_capacity", can_admit_name()],
    })
}

fn can_admit_name() -> String {
    Guard::and(Guard::AgentFree, Guard::GlobalCapacity).name()
}

// ponytail: one test module covering fence + determinism + guards + kernel
// parity with DispatchPool. New transitions bring their own cases here.
#[cfg(test)]
mod pure_cores_tests {
    use super::*;

    /// Standalone-build fence: this file's IMPORTS must never touch the
    /// runtime (tokio, Nostr clients, flywheel/doorbell, sibling backends).
    /// Scoped to `use` lines so doc prose may name what is forbidden.
    /// (Mirrors verify-0012.sh Phase-0 checks; allow-list: none needed.)
    #[test]
    fn core_source_has_no_runtime_imports() {
        let src = include_str!("dispatch_core.rs");
        // Fence the shipped module only: the test mod below legitimately
        // uses `super::*`.
        let shipped = src
            .split("#[cfg(test)]")
            .next()
            .expect("test module marker");
        let imports: Vec<&str> = shipped
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("use "))
            .collect();
        assert!(!imports.is_empty(), "fence must see the use lines");
        for banned in [
            "tokio",
            "nostr",
            "flywheel",
            "doorbell",
            "herdr",
            "sqlx",
            "axum",
            "rmcp",
            "parking_lot",
            "crate::",
            "super::",
        ] {
            for line in &imports {
                assert!(
                    !line.contains(banned),
                    "dispatch_core.rs imports banned `{banned}`: {line}"
                );
            }
        }
    }

    fn scripted_log() -> Vec<DispatchMsg> {
        use DispatchMsg::*;
        vec![
            Submit {
                job_id: "j1".into(),
                assignee: "a".into(),
            },
            Submit {
                job_id: "j2".into(),
                assignee: "b".into(),
            },
            Acquire {
                assignee: "a".into(),
            },
            Acquire {
                assignee: "a".into(),
            },
            Poll,
            Release {
                assignee: "a".into(),
            },
            Poll,
        ]
    }

    fn replay(log: &[DispatchMsg]) -> (DispatchModel, Vec<DispatchEffect>) {
        let mut model = DispatchModel::new(1);
        let mut all = Vec::new();
        for msg in log {
            let (next, effects) = tick(&model, msg);
            model = next;
            all.extend(effects);
        }
        (model, all)
    }

    #[test]
    fn same_msg_log_byte_identical_effects() {
        let log = scripted_log();
        let (a_model, a_fx) = replay(&log);
        let (b_model, b_fx) = replay(&log);
        assert_eq!(format!("{a_fx:?}"), format!("{b_fx:?}"));
        assert_eq!(a_model, b_model);
        // And the log means what the pool does: j1 admitted, dup denied
        // (agent-busy), poll dispatches j2? No — max 1, a holds it: poll
        // skips j1 (busy a)? j1 was submitted not dispatched... trace:
        // submit j1, submit j2, acquire a (admitted), acquire a (denied
        // busy), poll (j1: a busy → skip; j2: b free but global cap 1 used
        // → skip; no effect), release a, poll (j1 dispatched).
        assert_eq!(
            a_fx,
            vec![
                DispatchEffect::Queued {
                    job_id: "j1".into()
                },
                DispatchEffect::Queued {
                    job_id: "j2".into()
                },
                DispatchEffect::Admitted {
                    assignee: "a".into()
                },
                DispatchEffect::Denied {
                    assignee: "a".into(),
                    reason: DenyReason::AgentBusy,
                },
                DispatchEffect::Released {
                    assignee: "a".into()
                },
                DispatchEffect::Dispatched {
                    job_id: "j1".into(),
                    assignee: "a".into(),
                },
            ]
        );
    }

    #[test]
    fn global_cap_denies_with_cap_reason() {
        let (m, _) = replay(&[
            DispatchMsg::Acquire {
                assignee: "a".into(),
            },
            DispatchMsg::Acquire {
                assignee: "b".into(),
            },
        ]);
        // max 1: b denied on global cap (agent b itself is free).
        let (_, fx) = tick(
            &m,
            &DispatchMsg::Acquire {
                assignee: "c".into(),
            },
        );
        assert_eq!(
            fx,
            vec![DispatchEffect::Denied {
                assignee: "c".into(),
                reason: DenyReason::GlobalCap,
            }]
        );
    }

    #[test]
    fn guard_composition_truth_table() {
        let mut m = DispatchModel::new(1);
        m.used = 1; // cap reached, agents free
        let free_and_cap = Guard::and(Guard::AgentFree, Guard::GlobalCapacity);
        assert!(!free_and_cap.eval(&m, "a"));
        assert!(Guard::or(Guard::AgentFree, Guard::GlobalCapacity).eval(&m, "a"));
        assert!(!Guard::not(Guard::AgentFree).eval(&m, "a"));
        assert!(Guard::not(Guard::GlobalCapacity).eval(&m, "a"));
        assert_eq!(free_and_cap.name(), "(agent_free and global_capacity)");
        // Busy agent, free cap: deny names agent first (pool branch order).
        m.used = 0;
        m.in_flight.insert("a".into(), 1);
        assert!(!can_admit(&m, "a"));
        assert_eq!(deny_reason(&m, "a"), DenyReason::AgentBusy);
    }

    #[test]
    fn machine_config_serializes() {
        let cfg = machine_config();
        assert_eq!(cfg["machine"], "dispatch_core");
        assert_eq!(cfg["transitions"].as_array().map(|a| a.len()), Some(4));
        let text = serde_json::to_string(&cfg).expect("serializes");
        assert!(text.contains("agent_free"));
    }
}
