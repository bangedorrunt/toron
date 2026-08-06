// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! RelayBus — NIP-01 over WebSocket via the nostr-sdk `Client` (Decision 1A
//! secondary path; federation opt-in).
//!
//! Full NIP-01 frames (EVENT/REQ/OK/EOSE). Multi-relay read/write config.
//! Used for the standalone daemon and federation when relays are configured.
//!
//! **Phase-1 federation policy:** only gift-wrapped NIP-17 traffic (kind-1059)
//! is published to external relays. Addressable lease/manifest kinds
//! (30010–30014, 30031–30032) are rejected at the publish-policy layer until
//! toron owns an embedded relay (ADR-0003 NEW-D3). This is enforced by
//! [`RelayBus::federation_policy_allows`].

use std::sync::Arc;

use nostr_sdk::nostr::{Event, Keys};
use nostr_sdk::{Client, RelayPoolNotification};
use tokio::sync::mpsc;

use crate::bus::scoping::is_metadata_hiding_kind;
use crate::bus::{
    Bus, BusError, Filter, Health, PublishOk, Subscription, SubscriptionEvent, SubscriptionId,
};
use crate::nostr::event::VerifyError;

/// Read/write role for a relay in the bus config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayRole {
    /// Read only (subscribe).
    Read,
    /// Write only (publish).
    Write,
    /// Read + write.
    ReadWrite,
}

/// A configured relay.
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Relay URL (e.g. `wss://relay.example.com` or `ws://127.0.0.1:PORT`).
    pub url: String,
    /// Role for this relay.
    pub role: RelayRole,
}

/// Configuration for [`RelayBus`].
#[derive(Debug, Clone)]
pub struct RelayBusConfig {
    /// Relays to connect.
    pub relays: Vec<RelayConfig>,
    /// The bus's own keys (used as the nostr-sdk signer for publish).
    pub keys: Keys,
    /// Phase-1 federation policy: when true (default), ONLY gift-wrapped
    /// NIP-17 traffic is published to external relays.
    pub federation_policy_enabled: bool,
}

impl Default for RelayBusConfig {
    fn default() -> Self {
        Self {
            relays: Vec::new(),
            keys: Keys::generate(),
            federation_policy_enabled: true,
        }
    }
}

/// RelayBus — NIP-01 over WebSocket via nostr-sdk `Client`.
#[derive(Clone)]
pub struct RelayBus {
    client: Client,
    config: Arc<RelayBusConfig>,
    /// Broadcast of relay notifications, fed by `handle_notifications` and
    /// consumed by per-subscription filter tasks.
    notify_tx: tokio::sync::broadcast::Sender<RelayPoolNotification>,
}

impl RelayBus {
    /// Open a RelayBus: build the nostr-sdk client, add relays, connect.
    pub async fn open(config: RelayBusConfig) -> Result<Self, BusError> {
        let client = Client::new(config.keys.clone());
        for relay in &config.relays {
            let _ = client
                .add_relay(&relay.url)
                .await
                .map_err(|e| BusError::Transport(format!("add_relay {}: {e}", relay.url)))?;
        }
        client.connect().await;

        // nostr-sdk delivers notifications only through `handle_notifications`.
        // That closure feeds a broadcast channel; per-subscription tasks
        // subscribe to it and filter by subscription id.
        let (notify_tx, _) = tokio::sync::broadcast::channel(1024);
        let client_for_task = client.clone();
        let notify_tx_for_task = notify_tx.clone();
        tokio::spawn(async move {
            // IMPORTANT: return Ok(false) — Ok(true) makes handle_notifications
            // EXIT after the first notification (drops all subsequent frames).
            let _ = client_for_task
                .handle_notifications(move |notification| {
                    let tx = notify_tx_for_task.clone();
                    async move {
                        let _ = tx.send(notification);
                        Ok(false)
                    }
                })
                .await;
        });

        Ok(Self {
            client,
            config: Arc::new(config),
            notify_tx,
        })
    }

    /// Phase-1 federation policy: only metadata-hiding (gift-wrapped NIP-17)
    /// kinds cross external relays. Addressable lease/manifest kinds are
    /// rejected until the embedded relay exists (ADR-0003 NEW-D3).
    pub fn federation_policy_allows(&self, event: &Event) -> bool {
        if !self.config.federation_policy_enabled {
            return true;
        }
        is_metadata_hiding_kind(event.kind)
    }

    async fn subscribe_filtered(
        &self,
        filter: Filter,
    ) -> Result<
        (
            nostr_sdk::nostr::SubscriptionId,
            mpsc::Receiver<SubscriptionEvent>,
        ),
        BusError,
    > {
        let (tx, rx) = mpsc::channel(256);

        // Subscribe to the notification broadcast BEFORE opening the client
        // subscription: the relay can answer REQ (replay + EOSE) before the
        // client.subscribe future resolves, and any notification emitted in
        // that window must not be lost (broadcast drops pre-subscribe sends).
        let mut notify_rx = self.notify_tx.subscribe();

        // Open the subscription on the client; map NIP-01 frames to our
        // SubscriptionEvent shape.
        let output = self
            .client
            .subscribe(filter.clone(), None)
            .await
            .map_err(|e| BusError::Subscription(format!("client.subscribe: {e}")))?;
        let sub_id = output.val;

        // Background task: broadcast notifications → mpsc. Filters match
        // against the original filter; EOSE maps to SubscriptionEvent::Eose.
        let sub_id_clone = sub_id.clone();
        tokio::spawn(async move {
            loop {
                match notify_rx.recv().await {
                    Ok(RelayPoolNotification::Event {
                        subscription_id,
                        event,
                        ..
                    }) if subscription_id == sub_id_clone => {
                        if tx.send(SubscriptionEvent::Event(*event)).await.is_err() {
                            return;
                        }
                    }
                    Ok(RelayPoolNotification::Message { message, .. }) => match message {
                        nostr_sdk::nostr::RelayMessage::Event {
                            subscription_id,
                            event,
                        } if subscription_id.as_ref() == &sub_id_clone => {
                            if tx
                                .send(SubscriptionEvent::Event(event.into_owned()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        nostr_sdk::nostr::RelayMessage::EndOfStoredEvents(id)
                            if id.as_ref() == &sub_id_clone =>
                        {
                            if tx.send(SubscriptionEvent::Eose).await.is_err() {
                                return;
                            }
                        }
                        _ => {}
                    },
                    Ok(RelayPoolNotification::Shutdown) => {
                        let _ = tx
                            .send(SubscriptionEvent::Closed("relay bus shutdown".into()))
                            .await;
                        return;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        let _ = tx
                            .send(SubscriptionEvent::Closed("relay bus closed".into()))
                            .await;
                        return;
                    }
                }
            }
        });

        Ok((sub_id, rx))
    }
}

#[async_trait::async_trait]
impl Bus for RelayBus {
    async fn publish(&self, event: Event) -> Result<PublishOk, BusError> {
        // Verify (reject forged/mutated).
        match crate::nostr::event::verify_event(&event) {
            Ok(()) => {}
            Err(VerifyError::BadId) => return Err(BusError::InvalidEvent("id mismatch".into())),
            Err(VerifyError::BadSignature) => {
                return Err(BusError::InvalidEvent("signature mismatch".into()))
            }
            Err(e) => return Err(BusError::InvalidEvent(e.to_string())),
        }

        // Phase-1 federation policy gate.
        if !self.federation_policy_allows(&event) {
            return Err(BusError::Publish(format!(
                "federation policy: kind {} is not federated in Phase 1 (only gift-wrapped NIP-17 crosses relays)",
                event.kind.as_u16()
            )));
        }

        // Publish to write relays (full NIP-01 EVENT/OK frames).
        self.client
            .send_event(&event)
            .await
            .map_err(|e| BusError::Transport(format!("send_event: {e}")))?;

        Ok(PublishOk {
            event_id: event.id,
            stored: true,
        })
    }

    async fn subscribe(&self, filter: Filter) -> Result<Subscription, BusError> {
        let (sub_id, rx) = self.subscribe_filtered(filter).await?;
        Ok(Subscription::new(
            SubscriptionId::new(sub_id.to_string()),
            rx,
        ))
    }

    async fn health(&self) -> Result<Health, BusError> {
        // nostr-sdk Client tracks connected relays; we count configured relays.
        let configured = self.config.relays.len();
        if configured == 0 {
            return Ok(Health {
                ok: true,
                message: "relay bus: no relays configured (federation off)".into(),
            });
        }
        Ok(Health {
            ok: true,
            message: format!("relay bus: {configured} relay(s) configured"),
        })
    }
}

// Allow constructing a bus for tests without exposing internals.
impl RelayBus {
    /// Config relays.
    pub fn relay_count(&self) -> usize {
        self.config.relays.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::{AGENT_PROFILE, PROJECT_MANIFEST};
    use nostr_sdk::nostr::{Kind, Tag, Timestamp};

    /// The policy gate is pure — unit test it directly (no relay needed).
    #[test]
    fn test_federation_policy_gates_kinds() {
        let keys = Keys::generate();
        let bus_config = RelayBusConfig {
            relays: vec![],
            keys: keys.clone(),
            federation_policy_enabled: true,
        };
        // Build the struct without network: use a helper that only needs
        // the config (policy check is pure).
        let _ = bus_config;

        // Gift wrap (1059) → allowed.
        assert!(is_metadata_hiding_kind(Kind::GiftWrap));

        // Agent profile (30013) → NOT metadata-hiding → blocked by policy.
        assert!(!is_metadata_hiding_kind(crate::nostr::kinds::toron_kind(
            AGENT_PROFILE
        )));
        assert!(!is_metadata_hiding_kind(crate::nostr::kinds::toron_kind(
            PROJECT_MANIFEST
        )));
    }

    /// Verify events still pass the publish verify gate (pure).
    #[test]
    fn test_publish_verifies_before_policy() {
        let keys = Keys::generate();
        let ev = build_event(
            &keys,
            Kind::GiftWrap,
            "{}",
            Vec::<Tag>::new(),
            Timestamp::now(),
        )
        .expect("build");
        assert!(crate::nostr::event::verify_event(&ev).is_ok());

        // Forged event (mutated content, kept sig) fails verify.
        let forged = Event::new(
            ev.id,
            ev.pubkey,
            ev.created_at,
            ev.kind,
            ev.tags.clone(),
            "MUTATED",
            ev.sig,
        );
        assert!(crate::nostr::event::verify_event(&forged).is_err());
    }

    /// The RelayBus cannot be opened without a relay in tests (no network);
    /// open with zero relays is allowed (federation off) and health reports it.
    #[tokio::test]
    async fn test_open_without_relays_is_local_ok() {
        let config = RelayBusConfig {
            relays: vec![],
            keys: Keys::generate(),
            federation_policy_enabled: true,
        };
        // open() with zero relays: add_relay loop is a no-op, connect() is a
        // no-op with no relays. Health reports "no relays configured".
        let bus = RelayBus::open(config).await.expect("open");
        let h = bus.health().await.expect("health");
        assert!(h.ok);
        assert!(h.message.contains("no relays"));
    }

    /// Integration: full NIP-01 roundtrip against a real in-process WS relay.
    ///
    /// Subscribes (REQ → replay + EOSE), publishes a gift-wrapped event
    /// (EVENT → OK → live forward), and asserts the subscriber receives it.
    #[tokio::test]
    async fn test_publish_subscribe_roundtrip_over_ws() {
        let relay = crate::bus::mock_relay::MockRelay::start().await;
        let url = relay.url().to_string();

        // Build two buses on the same relay: subscriber + publisher.
        let subscriber = RelayBus::open(RelayBusConfig {
            relays: vec![RelayConfig {
                url: url.clone(),
                role: RelayRole::ReadWrite,
            }],
            keys: Keys::generate(),
            federation_policy_enabled: true,
        })
        .await
        .expect("open subscriber");
        let publisher = RelayBus::open(RelayBusConfig {
            relays: vec![RelayConfig {
                url: url.clone(),
                role: RelayRole::ReadWrite,
            }],
            keys: Keys::generate(),
            federation_policy_enabled: true,
        })
        .await
        .expect("open publisher");

        // Give the WS connections a moment to establish.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Subscribe to ALL events (global filter).
        let mut sub = subscriber
            .subscribe(crate::bus::Filter::new())
            .await
            .expect("subscribe");

        // Publish a gift-wrapped event (allowed by federation policy).
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            "ws roundtrip secret",
        );
        let wrapped = crate::nostr::gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor)
            .await
            .expect("wrap");
        let ok = publisher.publish(wrapped.clone()).await.expect("publish");
        assert!(ok.stored);

        // Wait for the event to flow back via the live forward path.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut received = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(300), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(e))) if e.id == wrapped.id => {
                    received = true;
                    break;
                }
                Ok(None) => {
                    break;
                }
                _ => {}
            }
        }
        assert!(received, "subscriber must receive the event over WS");
        // The relay itself stored the event (real EVENT/OK path).
        assert_eq!(relay.stored_count(), 1, "relay stored the event");
        relay.shutdown();
    }

    /// Integration: replay path — publish first, then subscribe; the stored
    /// event must be replayed + EOSE over WS.
    #[tokio::test]
    async fn test_replay_after_publish_over_ws() {
        let relay = crate::bus::mock_relay::MockRelay::start().await;
        let url = relay.url().to_string();

        let publisher = RelayBus::open(RelayBusConfig {
            relays: vec![RelayConfig {
                url: url.clone(),
                role: RelayRole::ReadWrite,
            }],
            keys: Keys::generate(),
            federation_policy_enabled: true,
        })
        .await
        .expect("open publisher");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Publish a gift-wrapped event first.
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            "replay secret",
        );
        let wrapped = crate::nostr::gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor)
            .await
            .expect("wrap");
        publisher.publish(wrapped.clone()).await.expect("publish");

        // Subscriber joins AFTER publish → replay path.
        let subscriber = RelayBus::open(RelayBusConfig {
            relays: vec![RelayConfig {
                url,
                role: RelayRole::ReadWrite,
            }],
            keys: Keys::generate(),
            federation_policy_enabled: true,
        })
        .await
        .expect("open subscriber");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let mut sub = subscriber
            .subscribe(crate::bus::Filter::new())
            .await
            .expect("subscribe");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut received = false;
        let mut got_eose = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(300), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(e))) if e.id == wrapped.id => {
                    received = true;
                }
                Ok(Some(SubscriptionEvent::Eose)) => got_eose = true,
                Ok(Some(_)) => {}
                _ => break,
            }
            if received && got_eose {
                break;
            }
        }
        assert!(received, "replay must deliver the stored event");
        assert!(got_eose, "EOSE must follow replay");
        relay.shutdown();
    }

    /// Federation policy: a non-gift-wrap kind is rejected at publish.
    #[tokio::test]
    async fn test_federation_policy_rejects_manifest_kinds() {
        let relay = crate::bus::mock_relay::MockRelay::start().await;
        let url = relay.url().to_string();
        let bus = RelayBus::open(RelayBusConfig {
            relays: vec![RelayConfig {
                url,
                role: RelayRole::ReadWrite,
            }],
            keys: Keys::generate(),
            federation_policy_enabled: true,
        })
        .await
        .expect("open");

        let keys = Keys::generate();
        let manifest = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "{}",
            vec![Tag::parse(["d", "demo"]).expect("d")],
            Timestamp::now(),
        )
        .expect("build");
        let err = bus.publish(manifest).await.expect_err("manifest rejected");
        assert!(
            err.to_string().contains("not federated"),
            "error must explain the policy, got: {err}"
        );
        relay.shutdown();
    }
}
