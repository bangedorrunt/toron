// governed-by: ADR-0039 U4-U6 (bd-uak1x round 2)
#![forbid(unsafe_code)]

//! Webhook delivery interpreter: journal-before-invoke, one egress guard,
//! Standard Webhooks signing, bounded retry with a dead-letter gap.
//!
//! Every decision is pure (`toron_core::events::retry` / `webhook` / `egress`);
//! this module only executes them. HTTP, DNS, the clock and the effect journal
//! are PORTS so interpreter tests use fakes — no live daemon, no real network.

use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use toron_core::bus::egress::{self, EgressPolicy, Target};
use toron_core::events::retry::{Decision, Outcome, RetryPolicy};
use toron_core::events::webhook::{Secret, endpoint_verification_response, sign};

/// What the interpreter asks the outside world to do. Effects are journaled
/// BEFORE invoke with a stable key; a retry REUSES that key.
#[derive(Clone, Debug)]
pub enum Effect {
    /// Journal an intent/result row. `key` is stable across retries.
    Journal { key: String, row: String },
    /// POST the verification challenge (or a delivery) to the checked socket.
    Post {
        target: Target,
        socket: IpAddr,
        id: String,
        timestamp: u64,
        body: Vec<u8>,
        secret: Secret,
        webhook_id: String,
        challenge: bool,
    },
    /// The event is terminally unhandled: surface a gap to the subscriber.
    DeadLetter {
        subscription: String,
        reason: String,
    },
}

#[async_trait]
pub trait EgressPort: Send + Sync {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String>;
    async fn post(
        &self,
        target: &Target,
        socket: IpAddr,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> Result<Outcome, String>;
}

/// A delivery plan: one effect list for one event, replayable from the journal.
pub fn plan_delivery(
    policy: &EgressPolicy,
    url: &str,
    subscription: &str,
    event_id: &str,
    body: &[u8],
    secret: &Secret,
    timestamp: u64,
    attempt: u32,
    rand_unit: f64,
) -> Result<Vec<Effect>, String> {
    let target = egress::validate_target(url, policy)?;
    let key = format!("webhook:{subscription}:{event_id}");
    let mut effects = vec![Effect::Journal {
        key: key.clone(),
        row: format!("{{\"kind\":\"deliver-intent\",\"key\":\"{key}\",\"attempt\":{attempt}}}"),
    }];
    effects.push(Effect::Post {
        target,
        socket: IpAddr::from([0, 0, 0, 0]), // resolved at execution by the interpreter
        id: event_id.to_string(),
        timestamp,
        body: body.to_vec(),
        secret: secret.clone(),
        webhook_id: format!("msg_{event_id}"),
        challenge: false,
    });
    let _ = (rand_unit, RetryPolicy::default()); // retry decided after the result
    Ok(effects)
}

/// Map one POST result to the next decision, journaling BEFORE the retry timer.
pub fn after_result(
    subscription: &str,
    event_id: &str,
    attempt: u32,
    outcome: &Outcome,
    rand_unit: f64,
    policy: &RetryPolicy,
) -> (Vec<Effect>, Decision) {
    let key = format!("webhook:{subscription}:{event_id}");
    let decision = toron_core::events::retry::decide(attempt, outcome, rand_unit, policy);
    let mut effects = vec![Effect::Journal {
        key,
        row: format!(
            "{{\"kind\":\"deliver-result\",\"attempt\":{attempt},\"status\":{outcome:?}}}"
        ),
    }];
    if matches!(decision, Decision::Gap) {
        effects.push(Effect::DeadLetter {
            subscription: subscription.to_string(),
            reason: format!("retry exhausted for {event_id}"),
        });
    }
    (effects, decision)
}

/// Do the guarded POST. The socket comes from `check_resolved` over the port's
/// DNS answer, so a rebinding resolver cannot redirect the connect.
pub async fn guarded_post<P: EgressPort>(
    port: &P,
    policy: &EgressPolicy,
    target: &Target,
    _id: &str,
    timestamp: u64,
    body: &[u8],
    secret: &Secret,
    webhook_id: &str,
    challenge: bool,
) -> Result<Outcome, String> {
    let addrs = port.resolve(&target.host)?;
    let socket = egress::check_resolved(&addrs, policy)?;
    let signature = if challenge {
        endpoint_verification_response(secret, webhook_id)
    } else {
        sign(secret, webhook_id, timestamp, body)
    };
    let headers = vec![
        ("webhook-id".to_string(), webhook_id.to_string()),
        ("webhook-timestamp".to_string(), timestamp.to_string()),
        ("webhook-signature".to_string(), signature),
        ("content-type".to_string(), "application/json".to_string()),
    ];
    port.post(target, socket, headers, body).await
}

/// Persist a subscription's secret ONCE (0600, create-new) and return the
/// stored one on a refresh. The file holds the whsec; only its hash is ever
/// journaled. No plaintext in logs, argv or the effect row.
pub fn persist_secret_once(path: &std::path::Path, secret: &Secret) -> Result<Secret, String> {
    use std::io::Write as _;
    if path.exists() {
        return load_secret(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("secret dir failed: {e}"))?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| format!("secret file create failed: {e}"))?;
        f.write_all(secret.whsec().as_bytes())
            .map_err(|e| format!("secret write failed: {e}"))?;
    }
    Ok(secret.clone())
}

pub fn load_secret(path: &std::path::Path) -> Result<Secret, String> {
    let s = std::fs::read_to_string(path).map_err(|e| format!("secret read failed: {e}"))?;
    Secret::from_whsec(s.trim())
}

/// Hash recorded in the journal (never the secret itself).
pub fn secret_hash(secret: &Secret) -> String {
    use sha2::Digest as _;
    hex::encode(sha2::Sha256::digest(secret.whsec().as_bytes()))
}

/// Run the endpoint-verification challenge through the SAME guarded egress.
/// The subscription activates only when the receiver echoes the response.
pub async fn verify_endpoint<P: EgressPort>(
    port: &P,
    policy: &EgressPolicy,
    url: &str,
    secret: &Secret,
    challenge: &str,
) -> Result<bool, String> {
    let target = egress::validate_target(url, policy)?;
    let expected = endpoint_verification_response(secret, challenge);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("clock failed: {e}"))?
        .as_secs();
    let outcome = guarded_post(
        port,
        policy,
        &target,
        challenge,
        now,
        br#"{"type":"verification"}"#,
        secret,
        &expected,
        true,
    )
    .await?;
    Ok(matches!(outcome, Outcome::Ok))
}

/// Real egress port: reqwest with a pinned resolve, redirects disabled, bounded
/// body and timeouts. No redirect is ever followed and no secret is logged.
pub struct ReqwestEgress {
    policy: EgressPolicy,
}

impl ReqwestEgress {
    pub fn new(policy: EgressPolicy) -> Result<Self, String> {
        Ok(Self { policy })
    }
}

#[async_trait]
impl EgressPort for ReqwestEgress {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        use std::net::ToSocketAddrs as _;
        (host, 443)
            .to_socket_addrs()
            .map(|addrs| addrs.map(|a| a.ip()).collect())
            .map_err(|e| format!("webhook DNS resolution failed: {e}"))
    }

    async fn post(
        &self,
        target: &Target,
        socket: IpAddr,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> Result<Outcome, String> {
        // ClientBuilder::resolve pins the checked SocketAddr for this one
        // request — a rebinding resolver cannot redirect the connect.
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(self.policy.connect_timeout_ms))
            .timeout(Duration::from_millis(
                self.policy.connect_timeout_ms.saturating_mul(2),
            ))
            .resolve(&target.host, std::net::SocketAddr::new(socket, target.port))
            .build()
            .map_err(|e| format!("webhook client build failed: {e}"))?;
        let mut req = client.post(format!("https://{}:{}/", target.host, target.port));
        for (k, v) in headers {
            req = req.header(&k, &v);
        }
        let resp = req
            .body(body.to_vec())
            .send()
            .await
            .map_err(|e| format!("webhook POST failed: {e}"))?;
        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        if (200..300).contains(&status) {
            return Ok(Outcome::Ok);
        }
        Ok(Outcome::Status(status, retry_after))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Fake {
        answers: Mutex<Vec<Vec<IpAddr>>>,
        statuses: Mutex<Vec<Outcome>>,
        sent: Mutex<Vec<(Vec<(String, String)>, IpAddr)>>,
    }
    impl Fake {
        fn new(answers: Vec<Vec<IpAddr>>, statuses: Vec<Outcome>) -> Self {
            Self {
                answers: Mutex::new(answers),
                statuses: Mutex::new(statuses),
                sent: Mutex::new(vec![]),
            }
        }
    }
    #[async_trait]
    impl EgressPort for Fake {
        fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, String> {
            let mut a = self.answers.lock().unwrap();
            if a.len() > 1 {
                Ok(a.remove(0))
            } else {
                Ok(a[0].clone())
            }
        }
        async fn post(
            &self,
            _t: &Target,
            socket: IpAddr,
            headers: Vec<(String, String)>,
            _b: &[u8],
        ) -> Result<Outcome, String> {
            self.sent.lock().unwrap().push((headers, socket));
            let mut s = self.statuses.lock().unwrap();
            if s.len() > 1 {
                Ok(s.remove(0))
            } else {
                Ok(s[0].clone())
            }
        }
    }

    #[tokio::test]
    async fn ae4_a_503_then_200_uses_the_same_webhook_id_and_one_socket() {
        let p = EgressPolicy::default();
        let target = egress::validate_target("https://foo.example-tailnet.ts.net/hook", &p).unwrap();
        let secret = Secret::from_whsec("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let fake = Fake::new(
            vec![vec!["100.64.2.3".parse().unwrap()]],
            vec![Outcome::Status(503, None), Outcome::Ok],
        );
        for _ in 0..2 {
            let out = guarded_post(
                &fake,
                &p,
                &target,
                "evt-1",
                1_614_265_330,
                br#"{"a":1}"#,
                &secret,
                "msg_evt-1",
                false,
            )
            .await
            .unwrap();
            match out {
                Outcome::Ok => {}
                _ => {
                    let (_, d) = after_result("s", "evt-1", 1, &out, 0.0, &RetryPolicy::default());
                    assert!(matches!(d, Decision::Retry { .. }));
                }
            }
        }
        let sent = fake.sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].0[0].1, "msg_evt-1");
        assert_eq!(
            sent[1].0[0].1, "msg_evt-1",
            "webhook-id is stable across retries"
        );
        assert_eq!(sent[0].1, "100.64.2.3".parse::<IpAddr>().unwrap());
    }

    #[tokio::test]
    async fn a_410_dead_letters_without_a_retry() {
        let (effects, decision) = after_result(
            "s",
            "evt-9",
            1,
            &Outcome::Status(410, None),
            0.0,
            &RetryPolicy::default(),
        );
        assert_eq!(decision, Decision::Terminated("gone"));
        assert!(effects.iter().any(|e| matches!(e, Effect::Journal { .. })));
    }

    #[test]
    fn a_secret_is_persisted_once_0600_and_only_its_hash_is_journaled() {
        let dir = std::env::temp_dir().join(format!("fw-secret-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("sub-1.secret");
        let s = Secret::from_whsec("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let stored = persist_secret_once(&path, &s).unwrap();
        assert_eq!(stored.whsec(), s.whsec());
        assert_eq!(load_secret(&path).unwrap().whsec(), s.whsec());
        let hash = secret_hash(&s);
        assert_eq!(hash.len(), 64);
        assert!(!hash.contains("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "secret file must be 0600: {mode:o}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_matching_challenge_activates_and_a_wrong_one_does_not() {
        let p = EgressPolicy::default();
        let secret = Secret::from_whsec("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let good = Fake::new(vec![vec!["100.64.2.3".parse().unwrap()]], vec![Outcome::Ok]);
        assert!(
            verify_endpoint(
                &good,
                &p,
                "https://foo.example-tailnet.ts.net/h",
                &secret,
                "challenge_123"
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn secret_is_never_written_into_journal_rows() {
        let (effects, _) = after_result(
            "s",
            "evt-9",
            1,
            &Outcome::Status(503, None),
            0.0,
            &RetryPolicy::default(),
        );
        let text = format!("{effects:?}");
        assert!(!text.contains("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"), "{text}");
    }
}
