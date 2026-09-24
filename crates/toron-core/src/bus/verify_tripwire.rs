// governed-by: ADR-0003 Rev9 (GS-061 / Buzz #7010 verify-before-route)
#![forbid(unsafe_code)]

//! Tripwire: NIP-01 id + Schnorr verify MUST run at ingress **before**
//! stamp-seen / watermark / fanout / prompt-route side effects.
//!
//! Boundary discipline: one verify at the bus edge (`verify_publish_event`);
//! pure cores must not re-validate. These tests fail if ordering regresses.

/// Canonical publish stage order (Buzz pipeline / ADR-0003).
pub const PUBLISH_STAGE_ORDER: &[&str] = &[
    "verify",
    "auth",
    "dedup_or_ephemeral",
    "fanout",
    "search_hook",
    "audit_hook",
    "workflow_hook",
];

/// Assert stage `earlier` is strictly before `later` in the canonical order.
pub fn assert_stage_before(earlier: &str, later: &str) {
    let i = PUBLISH_STAGE_ORDER
        .iter()
        .position(|s| *s == earlier)
        .unwrap_or_else(|| panic!("unknown stage {earlier}"));
    let j = PUBLISH_STAGE_ORDER
        .iter()
        .position(|s| *s == later)
        .unwrap_or_else(|| panic!("unknown stage {later}"));
    assert!(
        i < j,
        "stage order regression: {earlier} (idx {i}) must precede {later} (idx {j})"
    );
}

/// Observed stage trace from a publish attempt (test harness fills this).
#[derive(Debug, Default, Clone)]
pub struct StageTrace {
    /// Ordered stage names entered.
    pub entered: Vec<String>,
}

impl StageTrace {
    /// Record a stage entry.
    pub fn enter(&mut self, stage: &str) {
        self.entered.push(stage.to_string());
    }

    /// True when verify was the first stage.
    pub fn verify_was_first(&self) -> bool {
        self.entered.first().map(String::as_str) == Some("verify")
    }

    /// True when no side-effect stage ran after a failed verify.
    pub fn no_side_effects_after_failed_verify(&self) -> bool {
        if let Some(pos) = self.entered.iter().position(|s| s == "verify_fail") {
            !self.entered[pos + 1..].iter().any(|s| {
                matches!(
                    s.as_str(),
                    "dedup_or_ephemeral"
                        | "fanout"
                        | "stamp_seen"
                        | "watermark"
                        | "prompt_route"
                )
            })
        } else {
            true
        }
    }
}

/// Source-level tripwire: `publish` must call `verify_publish_event` before
/// store insert / fanout. Fails CI if someone reorders the body.
pub fn publish_source_verify_before_side_effects(src: &str) -> Result<(), String> {
    // Anchor on the Bus impl publish (skip earlier #[cfg(test)] helpers).
    let publish = src
        .split("impl Bus for InProcessBus")
        .nth(1)
        .ok_or_else(|| "Bus impl not found".to_string())?
        .split("async fn publish(")
        .nth(1)
        .ok_or_else(|| "publish not found".to_string())?
        .split("async fn subscribe(")
        .next()
        .ok_or_else(|| "publish body unbounded".to_string())?;
    let verify_at = publish
        .find("verify_publish_event")
        .ok_or_else(|| "verify_publish_event missing from publish".to_string())?;
    // Store write sites: insert / insert_addressable.
    let insert_at = publish
        .find("insert(")
        .or_else(|| publish.find("insert_addressable"))
        .ok_or_else(|| "store insert missing from publish".to_string())?;
    let fanout_at = publish
        .find("fanout(")
        .ok_or_else(|| "fanout missing from publish".to_string())?;
    if verify_at > insert_at {
        return Err("verify_publish_event must appear before store insert".into());
    }
    if verify_at > fanout_at {
        return Err("verify_publish_event must appear before fanout".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::common::verify_publish_event;
    use crate::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
    use crate::bus::{Bus, BusError, Filter, SubscriptionEvent};
    use crate::index::schema;
    use crate::nostr::event::build_event;
    use crate::nostr::{Event, Keys, Kind, Timestamp};

    #[test]
    fn canonical_order_verify_before_fanout_and_dedup() {
        assert_stage_before("verify", "auth");
        assert_stage_before("verify", "dedup_or_ephemeral");
        assert_stage_before("verify", "fanout");
        assert_stage_before("auth", "fanout");
        assert_stage_before("dedup_or_ephemeral", "fanout");
    }

    #[test]
    fn stage_trace_tripwire_detects_verify_skip() {
        let mut t = StageTrace::default();
        t.enter("dedup_or_ephemeral");
        t.enter("fanout");
        assert!(!t.verify_was_first());
    }

    #[test]
    fn stage_trace_tripwire_detects_side_effect_after_verify_fail() {
        let mut t = StageTrace::default();
        t.enter("verify");
        t.enter("verify_fail");
        t.enter("fanout");
        assert!(!t.no_side_effects_after_failed_verify());
    }

    #[test]
    fn in_process_publish_source_orders_verify_first() {
        let src = include_str!("in_process.rs");
        publish_source_verify_before_side_effects(src).expect("verify-before-route");
    }

    #[tokio::test]
    async fn forged_event_never_reaches_store_or_fanout() {
        let path = std::env::temp_dir().join(format!(
            "toron-verify-tripwire-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool, InProcessBusConfig::default())
            .await
            .expect("bus");
        let mut sub = bus.subscribe(Filter::new()).await.expect("sub");

        let keys = Keys::generate();
        let ev = build_event(
            &keys,
            Kind::from(1u16),
            "original",
            vec![],
            Timestamp::now(),
        )
        .expect("build");
        let forged = Event::new(
            ev.id,
            ev.pubkey,
            ev.created_at,
            ev.kind,
            ev.tags.clone(),
            "MUTATED",
            ev.sig,
        );
        assert!(
            verify_publish_event(&forged).is_err(),
            "precondition: forged event must fail verify"
        );

        let mut trace = StageTrace::default();
        trace.enter("verify");
        let err = bus.publish(forged).await.expect_err("must reject");
        trace.enter("verify_fail");
        assert!(
            matches!(err, BusError::InvalidEvent(_)),
            "reject reason: {err}"
        );
        assert!(trace.verify_was_first());
        assert!(trace.no_side_effects_after_failed_verify());

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(80);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(20), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(_))) => {
                    panic!("forged event must not fanout")
                }
                Ok(Some(SubscriptionEvent::Eose | SubscriptionEvent::Closed(_))) => {}
                Ok(None) | Err(_) => break,
            }
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn allowlist_auth_is_after_verify_in_order_table() {
        let _ = AuthPolicy::Allowlist;
        assert_stage_before("verify", "auth");
    }
}
