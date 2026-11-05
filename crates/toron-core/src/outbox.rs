// governed-by: ADR-0040, ADR-0041
//! The sender side of a send (redesign §2.4): an elm-effect [`Program`]
//! whose one durable command is `mail.append`.
//!
//! 1. `Send(envelope)` folds into `Cmd::Append`, which is
//!    `Durability::Durable { key = msg_id }`: the loop appends the intent to
//!    its journal (a [`ToronLog`] on the `journal` stream) **before** the
//!    provider runs (L14).
//! 2. The provider appends the envelope to the inbox stream. Same triple
//!    appends once, so a retry can never duplicate.
//! 3. An `Unavailable` answer (outcome unknown) consults
//!    [`kernel::plan_attempt`] with `mail.append`'s capability
//!    `{idempotency key, reconcile query}`: the plan is `ReconcileFirst`, so
//!    the loop asks the log whether it holds the triple before any retry,
//!    and never retries blind.
//! 4. After a crash, [`recover`] reads the journal's intents and reconciles
//!    each: present → settled with its offset; absent → appended with the
//!    SAME triple.
//!
//! The fold is pure: no clock, no store, no `HashMap`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::envelope::Envelope;
use crate::inbox::{ToronLog, l16_json};
use crate::kernel::{
    self, AppendError, Boot, BuiltServices, Cmd as KCmd, Defect, DispatchPlan, Durability,
    DurableLog, Effect, EffectState, EffectView, Failure, Interpret, Offset, Program,
    ProviderCapability, ServiceTag, Update, use_service,
};

/// `mail.append`'s capability (redesign §2.4): the log is the provider, so
/// it both dedupes on the key and answers the reconcile query.
pub const MAIL_APPEND: ProviderCapability = ProviderCapability {
    supports_idempotency_key: true,
    supports_reconcile_query: true,
};

/// The inbox the provider appends to, handed to the interpreter as a
/// service (the loop's runtime is built from a layer holding it).
pub struct InboxService;

impl ServiceTag for InboxService {
    type Service = InboxHandle;
    const ID: &'static str = "toron/inbox";
}

/// What the provider needs: the inbox stream, plus the reconcile query.
#[derive(Clone)]
pub struct InboxHandle {
    /// The inbox stream as a DurableLog.
    pub log: ToronLog,
}

/// Reconcile attempts per send before it parks as `Unknown`.
pub const MAX_RECONCILE: u32 = 3;

/// Where one send is, as the fold sees it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SendState {
    /// Intent journaled; append running.
    InFlight,
    /// Outcome unknown; reconciling (attempts so far). After
    /// [`MAX_RECONCILE`] unanswered attempts the send parks here: no more
    /// commands until a host re-feeds it (`Recover`), never a blind retry.
    Unknown(u32),
    /// Settled at this offset.
    Settled(u64),
    /// Refused by the log or the contract (never retried).
    Refused(String),
}

/// The fold's model: one entry per msg key. BTreeMap, never HashMap.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxModel {
    /// Send state per `msg_key`.
    pub sends: BTreeMap<String, SendState>,
}

/// Inputs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OutboxMsg {
    /// Send one envelope.
    Send(Envelope),
    /// Reconcile one journaled intent after a restart.
    Recover(Envelope),
    /// The append returned an offset.
    Appended { key: String, offset: u64 },
    /// The append's outcome is unknown.
    Uncertain { envelope: Envelope, why: String },
    /// Reconcile found the triple already in the log.
    Reconciled { envelope: Envelope, offset: u64 },
    /// The log or the contract refused.
    Refused { key: String, why: String },
    /// The journal refused the intent (e.g. stale epoch): nothing ran.
    NotJournaled(String),
    /// The journal's append outcome is unknown: the intent may have landed.
    /// Parked, never retried blind; a host re-feeds `Recover` to reconcile.
    JournalUnknown(String),
}

/// Commands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum OutboxCmd {
    /// `mail.append`, durable under the msg key.
    Append(Envelope),
    /// The reconcile query, ephemeral.
    Reconcile(Envelope),
}

impl KCmd for OutboxCmd {
    fn durability(&self) -> Durability {
        match self {
            Self::Append(e) => Durability::Durable { key: e.msg.key() },
            Self::Reconcile(_) => Durability::Ephemeral,
        }
    }
}

/// The program.
pub struct Outbox;

impl Program for Outbox {
    type Model = OutboxModel;
    type Msg = OutboxMsg;
    type Cmd = OutboxCmd;
    type Err = String;

    fn init(_boot: &Boot) -> Update<OutboxModel, OutboxCmd> {
        Update::quiet(OutboxModel::default())
    }

    fn update(model: &OutboxModel, msg: &OutboxMsg) -> Update<OutboxModel, OutboxCmd> {
        let mut next = model.clone();
        match msg {
            OutboxMsg::Send(env) => {
                let key = env.msg.key();
                if matches!(
                    next.sends.get(&key),
                    Some(SendState::Settled(_) | SendState::InFlight)
                ) {
                    return Update::quiet(next);
                }
                next.sends.insert(key, SendState::InFlight);
                Update::with_commands(next, vec![OutboxCmd::Append(env.clone())])
            }
            OutboxMsg::Recover(env) => {
                next.sends.insert(env.msg.key(), SendState::Unknown(1));
                Update::with_commands(next, vec![OutboxCmd::Reconcile(env.clone())])
            }
            OutboxMsg::Appended { key, offset } => {
                next.sends.insert(key.clone(), SendState::Settled(*offset));
                Update::quiet(next)
            }
            OutboxMsg::Uncertain { envelope, .. } => {
                let attempts = match next.sends.get(&envelope.msg.key()) {
                    Some(SendState::Unknown(n)) => n + 1,
                    _ => 1,
                };
                next.sends
                    .insert(envelope.msg.key(), SendState::Unknown(attempts));
                if attempts > MAX_RECONCILE {
                    return Update::quiet(next);
                }
                let view = EffectView {
                    state: EffectState::Unknown,
                    attempt: i64::from(attempts),
                    lease_live: false,
                };
                match kernel::plan_attempt(Some(&view), &MAIL_APPEND) {
                    DispatchPlan::ReconcileFirst { .. } => {
                        Update::with_commands(next, vec![OutboxCmd::Reconcile(envelope.clone())])
                    }
                    other => {
                        next.sends.insert(
                            envelope.msg.key(),
                            SendState::Refused(format!("plan {other:?}")),
                        );
                        Update::quiet(next)
                    }
                }
            }
            OutboxMsg::Reconciled { envelope, offset } => {
                next.sends
                    .insert(envelope.msg.key(), SendState::Settled(*offset));
                Update::quiet(next)
            }
            OutboxMsg::Refused { key, why } => {
                next.sends
                    .insert(key.clone(), SendState::Refused(why.clone()));
                Update::quiet(next)
            }
            OutboxMsg::NotJournaled(why) => {
                next.sends.insert(
                    format!("<not journaled> {why}"),
                    SendState::Refused(why.clone()),
                );
                Update::quiet(next)
            }
            OutboxMsg::JournalUnknown(why) => {
                next.sends.insert(
                    format!("<journal unknown> {why}"),
                    SendState::Unknown(MAX_RECONCILE),
                );
                Update::quiet(next)
            }
        }
    }

    fn failure(failure: &Failure<String>) -> OutboxMsg {
        match failure {
            Failure::NotAppended(e) => OutboxMsg::NotJournaled(e.to_string()),
            Failure::Unknown(e) => OutboxMsg::JournalUnknown(e.to_string()),
            other => OutboxMsg::NotJournaled(format!("{other:?}")),
        }
    }
}

/// The interpreter: the provider is the inbox log, reached as a service.
pub struct OutboxRun;

/// Test-only crash points (`fault-injection` feature): abort the process
/// with no unwinding, exactly as a SIGKILL would leave it.
fn fault(point: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("TORON_FAULT").as_deref() == Ok(point) {
        std::process::abort();
    }
    let _ = point;
}

fn provider_append(log: &ToronLog, env: &Envelope) -> OutboxMsg {
    fault("after_intent");
    let appended = log.append_json(&l16_json(env));
    if appended.is_ok() {
        fault("after_append");
    }
    match appended {
        Ok(Offset::At(raw)) => OutboxMsg::Appended {
            key: env.msg.key(),
            offset: raw.parse().unwrap_or_default(),
        },
        Ok(other) => OutboxMsg::Refused {
            key: env.msg.key(),
            why: format!("log minted a sentinel {other:?}"),
        },
        Err(AppendError::Unavailable(why)) => OutboxMsg::Uncertain {
            envelope: env.clone(),
            why,
        },
        Err(e) => OutboxMsg::Refused {
            key: env.msg.key(),
            why: e.to_string(),
        },
    }
}

fn provider_reconcile(log: &ToronLog, env: &Envelope) -> OutboxMsg {
    match log.lookup(&env.msg) {
        Ok(Some(offset)) => OutboxMsg::Reconciled {
            envelope: env.clone(),
            offset,
        },
        Ok(None) => provider_append(log, env),
        Err(e) => OutboxMsg::Uncertain {
            envelope: env.clone(),
            why: e.to_string(),
        },
    }
}

impl Interpret<Outbox> for OutboxRun {
    fn interpret(_cmd: &OutboxCmd) -> Effect<OutboxMsg, String> {
        Effect::die(Defect::new(
            "outbox needs its inbox service: boot with outbox::runtime(log)",
        ))
    }

    fn interpret_in_env(
        cmd: &OutboxCmd,
        services: Option<&BuiltServices>,
    ) -> Effect<OutboxMsg, String> {
        let Some(services) = services else {
            return Self::interpret(cmd);
        };
        let cmd = cmd.clone();
        use_service::<InboxService>()
            .map_err(|_: std::convert::Infallible| String::from("unreachable"))
            .map(move |inbox| match &cmd {
                OutboxCmd::Append(env) => provider_append(&inbox.log, env),
                OutboxCmd::Reconcile(env) => provider_reconcile(&inbox.log, env),
            })
            .provide_from(services)
    }
}

/// A runtime whose layer holds the inbox handle.
pub fn runtime(inbox: ToronLog) -> kernel::ManagedRuntime {
    let layer =
        kernel::Layer::<InboxService, std::convert::Infallible, ()>::from_value(InboxHandle {
            log: inbox,
        });
    kernel::ManagedRuntime::make(&layer).expect("a value layer builds")
}

/// The journaled intents a crashed sender left: every `mail.append` intent
/// on `journal` (oldest first). Feed each to the loop as `Recover`.
pub fn recover(journal: &dyn DurableLog) -> Result<Vec<Envelope>, AppendError> {
    let mut out = Vec::new();
    for (_, payload) in journal.read_from(&Offset::Start)? {
        let v: serde_json::Value = serde_json::from_str(&payload)
            .map_err(|e| AppendError::NotAnEnvelope(e.to_string()))?;
        if let Some(body) = v.get("body").and_then(|b| b.get("Append")) {
            let env: Envelope = serde_json::from_value(body.clone())
                .map_err(|e| AppendError::NotAnEnvelope(e.to_string()))?;
            out.push(env);
        }
    }
    Ok(out)
}

/// The journal producer id for a sender npub (distinct from the npub, so
/// the journal's seq space never collides with the inbox triples).
pub fn journal_producer(npub: &str) -> String {
    format!("{npub}#journal")
}

/// What one [`send_and_recover`] run did, per msg key.
#[derive(Clone, Debug, Serialize)]
pub struct SendReport {
    /// The sender's journal epoch for this run (claimed from the log).
    pub journal_epoch: u64,
    /// Intents a crashed earlier run left unappended, and how each settled.
    pub recovered: BTreeMap<String, SendState>,
    /// The new sends, and how each settled.
    pub sent: BTreeMap<String, SendState>,
}

/// One sender process's lifetime (the `toron inbox send` verb and any
/// in-process host): boot a loop on its own OS thread under a fresh journal
/// epoch, reconcile this sender's journaled intents that never reached the
/// log, then send `sends` (journal first, then append).
///
/// Call from inside a multi-thread Tokio runtime; the loop never runs on a
/// Tokio worker (ADR-0040: no `block_on` inside Tokio).
pub async fn send_and_recover(
    store: crate::inbox::InboxStore,
    sender: String,
    sends: Vec<Envelope>,
) -> Result<SendReport, crate::inbox::InboxError> {
    use crate::inbox::{INBOX_STREAM, JOURNAL_STREAM};
    let producer = journal_producer(&sender);
    let holder = format!(
        "send_and_recover:{}:{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    let journal_epoch = store.claim_epoch(&producer, &holder).await?.epoch;
    let handle = tokio::runtime::Handle::current();
    let journal = ToronLog::spawn(store.clone(), JOURNAL_STREAM, &handle);
    let inbox = ToronLog::spawn(store, INBOX_STREAM, &handle);
    let joined = tokio::task::spawn_blocking(move || {
        std::thread::spawn(move || -> Result<SendReport, AppendError> {
            let mut pending = Vec::new();
            for e in recover(&journal)? {
                if e.from == sender && inbox.lookup(&e.msg)?.is_none() {
                    pending.push(e);
                }
            }
            let mut app = kernel::Loop::<Outbox, OutboxRun>::boot(
                &Boot::default(),
                runtime(inbox),
                std::sync::Arc::new(journal),
                kernel::Producer {
                    producer_id: producer,
                    epoch: journal_epoch,
                    seq: 0,
                },
            )?;
            let rkeys: Vec<String> = pending.iter().map(|e| e.msg.key()).collect();
            for e in pending {
                app.send(OutboxMsg::Recover(e))?;
            }
            let skeys: Vec<String> = sends.iter().map(|e| e.msg.key()).collect();
            for e in sends {
                app.send(OutboxMsg::Send(e))?;
            }
            let state = |k: &String| {
                app.model()
                    .sends
                    .get(k)
                    .cloned()
                    .unwrap_or(SendState::InFlight)
            };
            Ok(SendReport {
                journal_epoch,
                recovered: rkeys.iter().map(|k| (k.clone(), state(k))).collect(),
                sent: skeys.iter().map(|k| (k.clone(), state(k))).collect(),
            })
        })
        .join()
    })
    .await;
    match joined {
        Ok(Ok(r)) => Ok(r?),
        _ => Err(AppendError::Unavailable("sender loop thread died".into()).into()),
    }
}
