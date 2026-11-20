//! Production-path webhook dispatcher test (bd-uak1x). Boots the daemon's
//! OWN startup function with a fake egress over a temp TORON_DATA_DIR, resumes
//! a pending retry from the journal, and proves a restart sends no duplicate.

use std::net::IpAddr;
use std::sync::{Arc, OnceLock};

fn state_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

use toron_core::bus::egress::EgressPolicy;
use toron_core::events::retry::Outcome;
use toron_mcp::events::webhook_delivery::EgressPort;
use toron_mcp::server::start_webhook_dispatcher;

type Headers = Vec<(String, String)>;

#[derive(Debug, Clone)]
struct FakeStats {
    posts: Arc<tokio::sync::Mutex<Vec<Headers>>>,
    bodies: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    outcomes: Arc<tokio::sync::Mutex<Vec<Outcome>>>,
}

struct Fake {
    stats: FakeStats,
}

#[async_trait::async_trait]
impl EgressPort for Fake {
    fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, String> {
        Ok(vec!["100.64.2.3".parse().unwrap()])
    }
    async fn post(
        &self,
        _t: &toron_core::bus::egress::Target,
        _s: IpAddr,
        headers: Vec<(String, String)>,
        b: &[u8],
    ) -> Result<Outcome, String> {
        self.stats.posts.lock().await.push(headers);
        self.stats.bodies.lock().await.push(b.to_vec());
        let mut o = self.stats.outcomes.lock().await;
        Ok(if o.len() > 1 {
            o.remove(0)
        } else {
            o[0].clone()
        })
    }
}

#[tokio::test]
async fn an_addressed_mail_produces_one_signed_webhook_post_then_no_duplicate_on_restart() {
    let _g = state_lock().lock().await;
    let dir = std::env::temp_dir().join(format!("fw-webhook-mail-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    unsafe {
        std::env::set_var("TORON_DATA_DIR", &dir);
        std::env::set_var("TORON_WEBHOOK_TICK_MS", "80");
    }
    let state = toron_core::config::state_dir();
    let _ = std::fs::create_dir_all(&state);
    let _ = std::fs::write(state.join("webhook-journal.jsonl"), "");
    let _ = std::fs::write(state.join("webhook-specs.jsonl"), "");

    let stats = FakeStats {
        posts: Arc::new(tokio::sync::Mutex::new(vec![])),
        bodies: Arc::new(tokio::sync::Mutex::new(vec![])),
        outcomes: Arc::new(tokio::sync::Mutex::new(vec![Outcome::Ok])),
    };
    let mailbox = toron_mcp::demo_mailbox().await.expect("mailbox");
    let pool = mailbox.index_pool().expect("pool").clone();
    let store = toron_core::events::store::SqliteSubscriptionStore::new(pool);
    use toron_core::events::store::SubscriptionStore as _;
    let sub = store
        .subscribe(&toron_core::events::store::SubscribeSpec {
            principal: "operator".into(),
            is_pin: false,
            delivery: "webhook".into(),
            name: "mail.addressed".into(),
            args: serde_json::json!({"url": "https://foo.example-tailnet.ts.net/hook"}),
            ttl_us: None,
            now_us: 0,
        })
        .await
        .expect("subscribe webhook");
    // The subscribe route persists this spec; in this test we write the same
    // row so the daemon boot refold sees it.
    std::fs::write(
        state.join("webhook-specs.jsonl"),
        format!("{{\"id\":\"{}\",\"url\":\"https://foo.example-tailnet.ts.net/hook\",\"secret\":\"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw\"}}\n", sub.id),
    )
    .unwrap();

    let handle = start_webhook_dispatcher(
        &mailbox,
        Fake {
            stats: stats.clone(),
        },
        toron_core::bus::egress::EgressPolicy::default(),
    )
    .await
    .expect("dispatcher starts");
    // Publish a catalogue fact on the REAL bus (the same shape the mail path
    // produces): kind 9 addressed to the webhook subscriber's pin.
    let keys = toron_core::nostr::Keys::generate();
    let ev = toron_core::nostr::event::build_event(
        &keys,
        toron_core::nostr::kinds::toron_kind(toron_core::nostr::kinds::CHANNEL_MESSAGE),
        "addressed mail".to_string(),
        vec![
            toron_core::nostr::Tag::parse(["p", "subscriber-pin"]).unwrap(),
            toron_core::nostr::Tag::parse(["toron-kind", "mail-delivered"]).unwrap(),
        ],
        toron_core::nostr::Timestamp::now(),
    )
    .unwrap();
    mailbox
        .bus()
        .publish(ev)
        .await
        .expect("publish addressed fact");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let posts = stats.posts.lock().await;
    assert_eq!(posts.len(), 1, "exactly one signed POST: {posts:?}");
    let sig = posts[0]
        .iter()
        .find(|(k, _)| k == "webhook-signature")
        .map(|(_, v)| v.clone())
        .expect("signed");
    assert!(sig.starts_with("v1,"), "Standard-Webhooks signature: {sig}");
    let wid = posts[0]
        .iter()
        .find(|(k, _)| k == "webhook-id")
        .map(|(_, v)| v.clone())
        .expect("webhook-id");
    assert!(wid.starts_with("msg_"), "event id carried: {wid}");
    drop(posts);

    drop(handle);

    let handle2 = start_webhook_dispatcher(
        &mailbox,
        Fake {
            stats: stats.clone(),
        },
        toron_core::bus::egress::EgressPolicy::default(),
    )
    .await
    .expect("restart");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        stats.posts.lock().await.len(),
        1,
        "restart sends no duplicate"
    );
    drop(handle2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_pending_retry_resumes_on_the_production_path_without_a_duplicate() {
    let _g = state_lock().lock().await;
    let dir = std::env::temp_dir().join(format!("fw-webhook-daemon-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    // Use the SAME state dir the daemon's startup function reads.
    unsafe {
        std::env::set_var("TORON_DATA_DIR", &dir);
        std::env::set_var("TORON_WEBHOOK_TICK_MS", "80");
    }
    let state = toron_core::config::state_dir();
    let _ = std::fs::create_dir_all(&state);
    // A pending retry: journal says attempt 1 got a 503 with a due timer.
    let rows = [
        r#"{"kind":"deliver-intent","key":"webhook:sub-1","attempt":1}"#.to_string(),
        r#"{"kind":"deliver-result","key":"webhook:sub-1","attempt":1,"decision":"retry","due_ms":1}"#.to_string(),
    ];
    std::fs::write(state.join("webhook-journal.jsonl"), rows.join("\n") + "\n").unwrap();
    std::fs::write(
        state.join("webhook-specs.jsonl"),
        r#"{"id":"sub-1","url":"https://foo.example-tailnet.ts.net/hook","secret":"whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"}"#.to_string() + "\n",
    )
    .unwrap();

    let stats = FakeStats {
        posts: Arc::new(tokio::sync::Mutex::new(vec![])),
        bodies: Arc::new(tokio::sync::Mutex::new(vec![])),
        outcomes: Arc::new(tokio::sync::Mutex::new(vec![Outcome::Ok])),
    };
    let mailbox = toron_mcp::demo_mailbox().await.expect("mailbox");
    let handle = start_webhook_dispatcher(
        &mailbox,
        Fake {
            stats: stats.clone(),
        },
        EgressPolicy::default(),
    )
    .await
    .expect("dispatcher starts");
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    assert_eq!(
        stats.posts.lock().await.len(),
        1,
        "exactly one resumed delivery"
    );
    let sig = stats.posts.lock().await[0]
        .iter()
        .find(|(k, _)| k == "webhook-signature")
        .map(|(_, v)| v.clone())
        .expect("signed");
    assert!(
        sig.starts_with("v1,"),
        "delivery is Standard-Webhooks signed: {sig}"
    );
    let journal_after = std::fs::read_to_string(state.join("webhook-journal.jsonl")).unwrap();
    assert!(
        journal_after.contains("delivered"),
        "journal records the delivery: {journal_after}"
    );
    drop(handle); // first daemon gone

    // Restart: re-fold the SAME journal (now with the delivered result row).
    let handle2 = start_webhook_dispatcher(
        &mailbox,
        Fake {
            stats: stats.clone(),
        },
        EgressPolicy::default(),
    )
    .await
    .expect("dispatcher restarts");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        stats.posts.lock().await.len(),
        1,
        "restart sends no duplicate"
    );
    drop(handle2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_bus_lag_produces_one_gap_delivery_per_webhook_not_silence() {
    use std::collections::BTreeMap;
    use toron_mcp::events::webhook_delivery::{PostSpecOwned, WebhookDispatcher, deliver_gaps};

    let _g = state_lock().lock().await;
    let stats = FakeStats {
        posts: Arc::new(tokio::sync::Mutex::new(vec![])),
        bodies: Arc::new(tokio::sync::Mutex::new(vec![])),
        outcomes: Arc::new(tokio::sync::Mutex::new(vec![Outcome::Ok])),
    };
    let mut dispatcher = WebhookDispatcher::new(
        Fake {
            stats: stats.clone(),
        },
        EgressPolicy::default(),
    );
    let mut specs = BTreeMap::new();
    for id in ["sub-a", "sub-b"] {
        specs.insert(
            format!("webhook:{id}"),
            PostSpecOwned {
                url: "https://foo.example-tailnet.ts.net/hook".into(),
                body: b"{}".to_vec(),
                secret_whsec: "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw".into(),
                webhook_id: format!("msg_{id}"),
            },
        );
    }
    let sent = deliver_gaps(&mut dispatcher, &specs, 7).await.unwrap();
    assert_eq!(sent, 2, "one gap delivery per webhook subscription");
    let bodies = stats.bodies.lock().await;
    assert_eq!(bodies.len(), 2);
    for body in bodies.iter() {
        assert_eq!(
            body.as_slice(),
            br#"{"kind":"gap"}"#,
            "gap body, not silence"
        );
    }
    let sigs = stats.posts.lock().await;
    for headers in sigs.iter() {
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "webhook-signature" && v.starts_with("v1,")),
            "gap delivery is signed",
        );
    }
}
