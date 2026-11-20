// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! Pure per-subscription kernel (ADR-0039 D3, bd-kz33f). ADR-0035 tick:
//! `(Model, Msg) -> (Model, Vec<Effect>)`; no I/O, clock reads or locks here.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionModel {
    pub id: String,
    pub expires_us: Option<i64>,
    pub state: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelMsg {
    FactMatched { payload: String },
    Clock { now_us: i64 },
    Refresh { ttl_us: Option<i64>, now_us: i64 },
    Unsubscribe,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelEffect {
    Deliver { payload: String },
    Terminate,
    Ignored,
}

/// The only state transition. `Terminate` is emitted once on the clock tick
/// that crosses expiry; a terminated subscription ignores all later messages.
pub fn tick(model: &SubscriptionModel, msg: &KernelMsg) -> (SubscriptionModel, Vec<KernelEffect>) {
    let mut next = model.clone();
    if next.state == "terminated" {
        return (next, vec![KernelEffect::Ignored]);
    }
    let effects = match msg {
        KernelMsg::FactMatched { payload } => vec![KernelEffect::Deliver {
            payload: payload.clone(),
        }],
        KernelMsg::Clock { now_us } => {
            if next.expires_us.is_some_and(|expiry| *now_us >= expiry) {
                next.state = "terminated".into();
                vec![KernelEffect::Terminate]
            } else {
                vec![]
            }
        }
        KernelMsg::Refresh { ttl_us, now_us } => {
            next.expires_us = ttl_us.map(|ttl| now_us.saturating_add(ttl));
            vec![]
        }
        KernelMsg::Unsubscribe => {
            next.state = "terminated".into();
            vec![KernelEffect::Terminate]
        }
    };
    (next, effects)
}

/// Effect-law host: journal every effect BEFORE executing any of them. The
/// caller supplies a journal callback; a real host then performs the effects.
pub fn journal_then_run<F: FnMut(&KernelEffect)>(
    model: &SubscriptionModel,
    msg: &KernelMsg,
    mut journal: F,
) -> SubscriptionModel {
    let (next, effects) = tick(model, msg);
    for effect in &effects {
        journal(effect);
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> SubscriptionModel {
        SubscriptionModel {
            id: "sub".into(),
            expires_us: Some(2000),
            state: "active".into(),
        }
    }

    #[test]
    fn a_clock_past_expiry_emits_terminate() {
        let (next, effects) = tick(&model(), &KernelMsg::Clock { now_us: 2001 });
        assert_eq!(effects, vec![KernelEffect::Terminate]);
        assert_eq!(next.state, "terminated");
    }

    #[test]
    fn a_clock_before_expiry_does_nothing() {
        let (next, effects) = tick(&model(), &KernelMsg::Clock { now_us: 1999 });
        assert!(effects.is_empty());
        assert_eq!(next, model());
    }

    #[test]
    fn a_terminated_subscription_ignores_everything() {
        let mut terminated = model();
        terminated.state = "terminated".into();
        for msg in [
            KernelMsg::FactMatched {
                payload: "x".into(),
            },
            KernelMsg::Clock { now_us: i64::MAX },
            KernelMsg::Refresh {
                ttl_us: None,
                now_us: 1,
            },
            KernelMsg::Unsubscribe,
        ] {
            let (_, effects) = tick(&terminated, &msg);
            assert_eq!(effects, vec![KernelEffect::Ignored]);
        }
    }

    #[test]
    fn the_terminate_effect_is_journaled_before_execution() {
        let mut log = Vec::new();
        let next = journal_then_run(&model(), &KernelMsg::Clock { now_us: 2001 }, |e| {
            log.push(format!("journal:{e:?}"));
            log.push("execute".into());
        });
        assert_eq!(next.state, "terminated");
        assert_eq!(log, vec!["journal:Terminate", "execute"]);
    }
}
