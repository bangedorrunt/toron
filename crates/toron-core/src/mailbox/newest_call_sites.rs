// governed-by: ADR-0003
#![cfg(test)]
#![allow(clippy::needless_pass_by_value)]

//! Call-site coverage for the `NewestEvent` id tiebreak (bd-9w94).
//!
//! `newest_tests` in the parent module exercises the HELPER, so it cannot see a
//! call site that stopped using it. These tests go through the public surfaces
//! instead, on a bus whose replay order is arrival order.
//!
//! Why a double is required rather than a convenience: every call site is fed
//! by `collect_replay`, and `InProcessBus`'s SQLite store always returns
//! `(created_at, event_id)` ASC (`event_store::scan_ids` sorts DESC then
//! reverses). `max_by_key(|e| e.created_at)` therefore returns the SAME event
//! as the tiebreak on the default bus — a created_at-only revert is invisible
//! there by construction. `Bus::subscribe` promises no ordering at all, and
//! `InProcessBus`'s own live fanout delivers a subscriber's tail in publish
//! order, so arrival order is a legal bus behaviour, not an invented one.
//!
//! Why the fixture reorders rather than trusting insertion order: event ids are
//! content hashes, so which of two events has the higher id is a coin flip. A
//! fixture that publishes in a fixed order passes roughly half the time under a
//! created_at-only revert — a coin-flip test that reads as coverage. Every test
//! here sorts the built events so the winner arrives LAST-but-one, which is the
//! only arrangement under which "highest id wins" and "last one wins" disagree.

use std::sync::Arc;

use crate::bus::scoping::with_project_scoping;
use crate::bus::{
    Bus, BusError, Filter, Health, PublishOk, Subscription, SubscriptionEvent, SubscriptionId,
};
use crate::mailbox::contacts::BusPolicySource;
use crate::mailbox::unwrap::PolicySource;
use crate::mailbox::{ContactPolicy, Mailbox};
use crate::nostr::event::build_event;
use crate::nostr::kinds::{AGENT_PROFILE, CONTACT_POLICY, PROJECT_MANIFEST, toron_kind};
use crate::nostr::{Event, Keys, Kind, MatchEventOptions, Tag, Timestamp};

/// A `Bus` that replays stored history in ARRIVAL order.
///
/// Everything else matches the trait: dedup on id, filter honoured, EOSE after
/// the replay. The one liberty — no sort — is the axis the trait leaves open.
struct ArrivalOrderBus {
    stored: parking_lot::Mutex<Vec<Event>>,
}

impl ArrivalOrderBus {
    fn new() -> Self {
        Self {
            stored: parking_lot::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl Bus for ArrivalOrderBus {
    async fn publish(&self, event: Event) -> Result<PublishOk, BusError> {
        let mut stored = self.stored.lock();
        let fresh = !stored.iter().any(|e| e.id == event.id);
        if fresh {
            stored.push(event.clone());
        }
        Ok(PublishOk {
            event_id: event.id,
            stored: fresh,
        })
    }

    async fn subscribe(&self, filter: Filter) -> Result<Subscription, BusError> {
        let snapshot: Vec<Event> = self
            .stored
            .lock()
            .iter()
            .filter(|e| filter.match_event(e, MatchEventOptions::default()))
            .cloned()
            .collect();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            for ev in snapshot {
                if tx.send(SubscriptionEvent::Event(ev)).await.is_err() {
                    return;
                }
            }
            let _ = tx.send(SubscriptionEvent::Eose).await;
        });
        Ok(Subscription::new(SubscriptionId::new("arrival-order"), rx))
    }

    async fn subscribe_trusted(&self, filter: Filter) -> Result<Subscription, BusError> {
        self.subscribe(filter).await
    }

    async fn health(&self) -> Result<Health, BusError> {
        Ok(Health {
            ok: true,
            message: "arrival-order double".to_string(),
        })
    }
}

/// A mailbox on a throwaway data dir over the arrival-order bus.
fn mailbox(bus: Arc<dyn Bus>, dir: &std::path::Path) -> Mailbox {
    std::fs::create_dir_all(dir).expect("data dir");
    Mailbox::new(bus, dir.to_path_buf(), None)
}

fn scratch(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("toron-newest-{tag}-{}", uuid::Uuid::new_v4()))
}

/// Publish same-second events of one kind, ordered so the event with the
/// SMALLEST id arrives last, and return them in that arrival order.
///
/// The order is derived from the built ids, never assumed.
fn adversarial_arrivals(
    keys: &Keys,
    kind: Kind,
    at: Timestamp,
    tags: &dyn Fn() -> Vec<Tag>,
    contents: &[&str],
) -> Vec<Event> {
    let mut events: Vec<Event> = contents
        .iter()
        .map(|content| build_event(keys, kind, *content, tags(), at).expect("build"))
        .collect();
    events.sort_by_key(|a| std::cmp::Reverse(a.id));
    events
}

/// Prove the fixture can fail, then hand back the event a correct call site
/// must resolve.
fn assert_adversarial(events: &[Event]) -> &Event {
    assert!(
        events
            .windows(2)
            .all(|w| w[0].created_at == w[1].created_at),
        "precondition: a shared timestamp or the tie never happens"
    );
    let winner = events.iter().max_by_key(|e| e.id).expect("an event");
    assert_ne!(
        events.last().expect("non-empty").id,
        winner.id,
        "precondition: the winner must not arrive last, or a created_at-only max \
         agrees with it and this test proves nothing"
    );
    winner
}

async fn publish_all(bus: &ArrivalOrderBus, events: &[Event]) {
    for ev in events {
        assert!(
            bus.publish(ev.clone()).await.expect("publish").stored,
            "fixture published a duplicate id"
        );
    }
}

fn project_d_tags(slug: &str, d: &str) -> impl Fn() -> Vec<Tag> {
    let slug = slug.to_string();
    let d = d.to_string();
    move || {
        with_project_scoping(
            toron_kind(CONTACT_POLICY),
            vec![Tag::parse(["d", &d]).expect("d tag")],
            Some(&slug),
        )
    }
}

/// contacts.rs `contact_policy_for`: the agent's inbound policy is the newest
/// kind-30014 it authored, ties broken by id.
#[tokio::test]
async fn contact_policy_for_picks_the_higher_id_on_a_shared_second() {
    let dir = scratch("policy");
    let keys = Keys::generate();
    let bus = Arc::new(ArrivalOrderBus::new());
    let events = adversarial_arrivals(
        &keys,
        toron_kind(CONTACT_POLICY),
        Timestamp::from_secs(1_700_000_000),
        &project_d_tags("demo", "demo:Agent"),
        &["open", "block_all"],
    );
    let winner = assert_adversarial(&events);
    let expected = ContactPolicy::parse(&winner.content).expect("winner is a policy");
    publish_all(&bus, &events).await;

    let resolved = mailbox(bus, &dir)
        .contact_policy_for("demo", &keys.public_key())
        .await;
    assert_eq!(
        resolved, expected,
        "a shared-second policy pair resolves by event id, not by arrival position"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// contacts.rs `BusPolicySource::load_roster`: the same question asked of the
/// roster snapshot, where one replay serves every agent in a project.
#[tokio::test]
async fn roster_policy_snapshot_picks_the_higher_id_on_a_shared_second() {
    let dir = scratch("roster");
    let keys = Keys::generate();
    let bus = Arc::new(ArrivalOrderBus::new());
    let events = adversarial_arrivals(
        &keys,
        toron_kind(CONTACT_POLICY),
        Timestamp::from_secs(1_700_000_000),
        &project_d_tags("demo", "demo:Agent"),
        &["contacts_only", "block_all"],
    );
    let winner = assert_adversarial(&events);
    let expected = ContactPolicy::parse(&winner.content).expect("winner is a policy");
    publish_all(&bus, &events).await;

    let mailbox = mailbox(bus, &dir);
    let source = BusPolicySource::load_roster(
        &mailbox,
        &[(
            "demo".to_string(),
            "Agent".to_string(),
            keys.public_key().to_hex(),
        )],
    )
    .await;
    assert_eq!(
        source.policy_for(&keys.public_key()),
        expected,
        "the roster snapshot resolves the same shared-second pair by event id"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// infra.rs `find_manifest`, reached through `ensure_project`: two same-second
/// manifests for one slug, and the project state must name the higher id.
#[tokio::test]
async fn ensure_project_names_the_higher_id_manifest_on_a_shared_second() {
    let dir = scratch("manifest");
    let keys = Keys::generate();
    let bus = Arc::new(ArrivalOrderBus::new());
    let events = adversarial_arrivals(
        &keys,
        toron_kind(PROJECT_MANIFEST),
        Timestamp::from_secs(1_700_000_000),
        &{
            let slug = "demo".to_string();
            move || {
                with_project_scoping(
                    toron_kind(PROJECT_MANIFEST),
                    vec![Tag::parse(["d", &slug]).expect("d tag")],
                    Some(&slug),
                )
            }
        },
        &["first", "second"],
    );
    let winner = assert_adversarial(&events);
    publish_all(&bus, &events).await;

    let state = mailbox(bus, &dir)
        .ensure_project("demo")
        .await
        .expect("ensure");
    assert_eq!(
        state.manifest_event_id,
        winner.id.to_hex(),
        "the existing project resolves to the higher-id manifest"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// A kind-30013 fixture: the wire `d` tag + content, and what `whois` must
/// answer from each.
struct Profile {
    d: &'static str,
    content: &'static str,
    name: &'static str,
    program: &'static str,
}

/// identity.rs `whois` on the npub branch: the display name comes from the
/// newest profile's `d` tag and the profile fields from the newest profile
/// event, so one fixture pins both call sites.
#[tokio::test]
async fn whois_reads_the_higher_id_profile_on_a_shared_second() {
    let dir = scratch("whois");
    let keys = Keys::generate();
    let bus = Arc::new(ArrivalOrderBus::new());
    let profiles = [
        Profile {
            d: "demo:OldName",
            content: r#"{"name":"OldName","program":"pi"}"#,
            name: "OldName",
            program: "pi",
        },
        Profile {
            d: "demo:NewName",
            content: r#"{"name":"NewName","program":"omp"}"#,
            name: "NewName",
            program: "omp",
        },
    ];
    let at = Timestamp::from_secs(1_700_000_000);
    let mut built: Vec<(Event, &str, &str)> = profiles
        .iter()
        .map(|p| {
            let event = build_event(
                &keys,
                toron_kind(AGENT_PROFILE),
                p.content,
                with_project_scoping(
                    toron_kind(AGENT_PROFILE),
                    vec![Tag::parse(["d", p.d]).expect("d tag")],
                    Some("demo"),
                ),
                at,
            )
            .expect("build");
            (event, p.name, p.program)
        })
        .collect();
    built.sort_by_key(|a| std::cmp::Reverse(a.0.id));
    let events: Vec<Event> = built.iter().map(|(e, _, _)| e.clone()).collect();
    let (winner, winner_name, winner_program) = built.first().expect("a profile");
    assert_eq!(
        assert_adversarial(&events).id,
        winner.id,
        "precondition: the first row after the DESC sort is the winner"
    );
    publish_all(&bus, &events).await;

    let who = mailbox(bus, &dir)
        .whois("demo", &keys.public_key().to_hex())
        .await
        .expect("whois by npub");
    assert_eq!(
        who.name, *winner_name,
        "whois takes the display name from the higher-id profile's d tag"
    );
    assert_eq!(
        who.program.as_deref(),
        Some(*winner_program),
        "whois takes the profile fields from the higher-id profile event"
    );
    let _ = std::fs::remove_dir_all(dir);
}
