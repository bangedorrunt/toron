// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! Pure stream lifecycle (ADR-0039 D7, bd-o4bgx). One SSE request per
//! subscription: active -> streaming -> terminated. Heartbeats and expiry are
//! clock messages; every effect is journaled before the host executes it.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamState {
    Active,
    Streaming,
    Terminated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamMsg {
    Open,
    Fact { event_id: String, payload: String },
    Heartbeat { now_us: i64 },
    Clock { now_us: i64 },
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamEffect {
    Active,
    Event { event_id: String, payload: String },
    Heartbeat,
    Terminated,
    Ignored,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamModel {
    pub state: StreamState,
    pub expires_us: Option<i64>,
    pub last_event_at_us: i64,
}

impl StreamModel {
    pub fn new(expires_us: Option<i64>) -> Self {
        Self {
            state: StreamState::Active,
            expires_us,
            last_event_at_us: 0,
        }
    }
}

pub fn tick(model: &StreamModel, msg: &StreamMsg) -> (StreamModel, Vec<StreamEffect>) {
    let mut next = model.clone();
    if next.state == StreamState::Terminated {
        return (next, vec![StreamEffect::Ignored]);
    }
    let effects = match msg {
        StreamMsg::Open => {
            next.state = StreamState::Streaming;
            vec![StreamEffect::Active]
        }
        StreamMsg::Fact { event_id, payload } => {
            next.state = StreamState::Streaming;
            vec![StreamEffect::Event {
                event_id: event_id.clone(),
                payload: payload.clone(),
            }]
        }
        StreamMsg::Heartbeat { now_us } => {
            next.last_event_at_us = *now_us;
            vec![StreamEffect::Heartbeat]
        }
        StreamMsg::Clock { now_us } => {
            if next.expires_us.is_some_and(|expiry| *now_us >= expiry) {
                next.state = StreamState::Terminated;
                vec![StreamEffect::Terminated]
            } else {
                vec![]
            }
        }
        StreamMsg::Closed => {
            next.state = StreamState::Terminated;
            vec![StreamEffect::Terminated]
        }
    };
    (next, effects)
}

pub fn journal_then_run<F: FnMut(&StreamEffect)>(
    model: &StreamModel,
    msg: &StreamMsg,
    mut journal: F,
) -> StreamModel {
    let (next, effects) = tick(model, msg);
    for effect in &effects {
        journal(effect);
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_emits_active_then_facts_emit_events() {
        let model = StreamModel::new(None);
        let (open, effects) = tick(&model, &StreamMsg::Open);
        assert_eq!(effects, vec![StreamEffect::Active]);
        assert_eq!(open.state, StreamState::Streaming);
        let (_, effects) = tick(
            &open,
            &StreamMsg::Fact {
                event_id: "e1".into(),
                payload: "{}".into(),
            },
        );
        assert_eq!(
            effects,
            vec![StreamEffect::Event {
                event_id: "e1".into(),
                payload: "{}".into()
            }]
        );
    }

    #[test]
    fn heartbeat_and_expiry_and_terminal_guard() {
        let model = StreamModel::new(Some(1000));
        let (_, effects) = tick(&model, &StreamMsg::Heartbeat { now_us: 500 });
        assert_eq!(effects, vec![StreamEffect::Heartbeat]);
        let (expired, effects) = tick(&model, &StreamMsg::Clock { now_us: 1000 });
        assert_eq!(effects, vec![StreamEffect::Terminated]);
        assert_eq!(expired.state, StreamState::Terminated);
        for msg in [
            StreamMsg::Open,
            StreamMsg::Fact {
                event_id: "x".into(),
                payload: "{}".into(),
            },
            StreamMsg::Heartbeat { now_us: 1 },
            StreamMsg::Clock { now_us: i64::MAX },
            StreamMsg::Closed,
        ] {
            let (_, effects) = tick(&expired, &msg);
            assert_eq!(effects, vec![StreamEffect::Ignored]);
        }
    }

    #[test]
    fn the_terminate_effect_is_journaled_before_execution() {
        let model = StreamModel::new(Some(1000));
        let mut log = Vec::new();
        let next = journal_then_run(&model, &StreamMsg::Clock { now_us: 1000 }, |e| {
            log.push(format!("journal:{e:?}"));
            log.push("execute".into());
        });
        assert_eq!(next.state, StreamState::Terminated);
        assert_eq!(log, vec!["journal:Terminated", "execute"]);
    }
}
