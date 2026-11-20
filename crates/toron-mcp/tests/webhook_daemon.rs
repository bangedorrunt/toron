//! Production-path webhook dispatcher test (bd-uak1x). Boots the daemon's
//! OWN startup function with a fake egress over a temp TORON_DATA_DIR, resumes
//! a pending retry from the journal, and proves a restart sends no duplicate.

use std::net::IpAddr;
use std::sync::{Arc, Mutex};

use toron_core::bus::egress::EgressPolicy;
use toron_core::events::retry::Outcome;
use toron_mcp::events::webhook_delivery::EgressPort;
use toron_mcp::server::start_webhook_dispatcher;

type Headers = Vec<(String, String)>;

#[derive(Debug, Clone)]
struct FakeStats {
    posts: Arc<Mutex<Vec<Headers>>>,
    outcomes: Arc<Mutex<Vec<Outcome>>>,
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
        _b: &[u8],
    ) -> Result<Outcome, String> {
        self.stats.posts.lock().unwrap().push(headers);
        let mut o = self.stats.outcomes.lock().unwrap();
        Ok(if o.len() > 1 {
            o.remove(0)
        } else {
            o[0].clone()
        })
    }
}

#[tokio::test]
async fn a_pending_retry_resumes_on_the_production_path_without_a_duplicate() {
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
        posts: Arc::new(Mutex::new(vec![])),
        outcomes: Arc::new(Mutex::new(vec![Outcome::Ok])),
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
        stats.posts.lock().unwrap().len(),
        1,
        "exactly one resumed delivery"
    );
    let sig = stats.posts.lock().unwrap()[0]
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
        stats.posts.lock().unwrap().len(),
        1,
        "restart sends no duplicate"
    );
    drop(handle2);
    let _ = std::fs::remove_dir_all(&dir);
}
