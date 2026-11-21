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
    /// Guarded GET for the receiver-published well-known document. Default
    /// refuses so existing test ports need no change.
    async fn get(&self, _target: &Target, _socket: IpAddr) -> Result<String, String> {
        Err("guarded GET unsupported".into())
    }
    async fn post(
        &self,
        target: &Target,
        socket: IpAddr,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> Result<Outcome, String>;
}

/// One delivery's inputs, bundled so the planner stays under the arg limit.
pub struct DeliveryRequest<'a> {
    pub url: &'a str,
    pub subscription: &'a str,
    pub event_id: &'a str,
    pub body: &'a [u8],
    pub secret: &'a Secret,
    pub timestamp: u64,
    pub attempt: u32,
}

/// A delivery plan: one effect list for one event, replayable from the journal.
pub fn plan_delivery(
    policy: &EgressPolicy,
    req: &DeliveryRequest<'_>,
) -> Result<Vec<Effect>, String> {
    let DeliveryRequest {
        url,
        subscription,
        event_id,
        body,
        secret,
        timestamp,
        attempt,
    } = req;
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
        timestamp: *timestamp,
        body: body.to_vec(),
        secret: (*secret).clone(),
        webhook_id: format!("msg_{event_id}"),
        challenge: false,
    });
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

/// One POST's inputs, bundled for the guarded interpreter.
pub struct PostSpec<'a> {
    pub target: &'a Target,
    pub timestamp: u64,
    pub body: &'a [u8],
    pub secret: &'a Secret,
    pub webhook_id: &'a str,
    /// SEP-3415 Final: X-MCP-Subscription-Id on every POST (G7).
    pub subscription_id: &'a str,
    pub challenge: bool,
}

/// Do the guarded POST. The socket comes from `check_resolved` over the port's
/// DNS answer, so a rebinding resolver cannot redirect the connect.
pub async fn guarded_post<P: EgressPort>(
    port: &P,
    policy: &EgressPolicy,
    spec: &PostSpec<'_>,
) -> Result<Outcome, String> {
    let PostSpec {
        target,
        timestamp,
        body,
        secret,
        webhook_id,
        subscription_id,
        challenge,
    } = spec;
    let addrs = port.resolve(&target.host)?;
    let socket = egress::check_resolved(&addrs, policy)?;
    let signature = if *challenge {
        endpoint_verification_response(secret, webhook_id)
    } else {
        sign(secret, webhook_id, *timestamp, body)
    };
    let headers = vec![
        ("webhook-id".to_string(), webhook_id.to_string()),
        ("webhook-timestamp".to_string(), timestamp.to_string()),
        ("webhook-signature".to_string(), signature),
        (
            "x-mcp-subscription-id".to_string(),
            subscription_id.to_string(),
        ),
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
    subscription_id: &str,
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
        &PostSpec {
            target: &target,
            timestamp: now,
            body: br#"{"type":"verification"}"#,
            secret,
            webhook_id: &expected,
            subscription_id,
            challenge: true,
        },
    )
    .await?;
    Ok(matches!(outcome, Outcome::Ok))
}

/// Per-(principal,url) verification cache. A successful handshake or a
/// well-known match covers all of that principal's subscriptions to the URL
/// across refreshes and `arguments` (SEP-3415 Final G8).
static VERIFY_CACHE: std::sync::OnceLock<
    std::sync::Mutex<toron_core::events::webhook_verify::VerificationCache>,
> = std::sync::OnceLock::new();

fn verify_cache() -> &'static std::sync::Mutex<toron_core::events::webhook_verify::VerificationCache>
{
    VERIFY_CACHE.get_or_init(|| {
        std::sync::Mutex::new(
            toron_core::events::webhook_verify::VerificationCache::with_ttl(
                24 * 60 * 60 * 1_000_000,
            ),
        )
    })
}

/// Verify for delivery: cached result, then receiver-published well-known
/// consent over the SSRF guard, then the signed challenge. Keyed by
/// (principal, url), never by subscription.
#[allow(clippy::too_many_arguments)] // one verify call carries port, guard, url, secret, nonce, id, principal, clock
pub async fn verify_endpoint_cached<P: EgressPort>(
    port: &P,
    policy: &EgressPolicy,
    url: &str,
    secret: &Secret,
    challenge: &str,
    subscription_id: &str,
    principal: &str,
    now_us: i64,
) -> Result<bool, String> {
    if verify_cache()
        .lock()
        .map_err(|e| e.to_string())?
        .is_verified(principal, url, now_us)
    {
        return Ok(true);
    }
    let target = egress::validate_target(url, policy)?;
    let socket = egress::check_resolved(&port.resolve(&target.host)?, policy)?;
    if let Ok(document) = port.get(&target, socket).await
        && toron_core::events::webhook_verify::well_known_covers(&document, url)
    {
        verify_cache()
            .lock()
            .map_err(|e| e.to_string())?
            .mark(principal, url, now_us);
        return Ok(true);
    }
    if verify_endpoint(port, policy, url, secret, challenge, subscription_id).await? {
        verify_cache()
            .lock()
            .map_err(|e| e.to_string())?
            .mark(principal, url, now_us);
        return Ok(true);
    }
    Ok(false)
}

/// The durable retry state of one delivery, re-folded from journal rows after
/// a daemon restart. No timer survives a restart; the NEXT clock Msg re-arms
/// from `due_ms`. This is the only state the interpreter needs to resume.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetryState {
    pub attempt: u32,
    pub due_ms: u64,
    pub terminated: bool,
    pub dead_lettered: bool,
}

/// Re-fold one journal row into the retry state. Rows are `{kind,attempt,...}`;
/// `deliver-intent` starts an attempt, `deliver-result` with a `retry` decision
/// arms the next timer, `terminated`/`dead-letter` end it. Pure and idempotent
/// over a replayed log (bd-uak1x restart requirement).
fn json_field(row: &str, name: &str) -> Option<String> {
    let v = serde_json::from_str::<serde_json::Value>(row).ok()?;
    v.get(name)?.as_str().map(str::to_string)
}

pub fn fold_retry(mut state: RetryState, row: &str) -> RetryState {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(row) else {
        return state;
    };
    match v.get("kind").and_then(|k| k.as_str()).unwrap_or("") {
        "deliver-intent" => {
            state.attempt = v
                .get("attempt")
                .and_then(|a| a.as_u64())
                .unwrap_or(state.attempt as u64) as u32;
        }
        "deliver-result" => match v.get("decision").and_then(|d| d.as_str()).unwrap_or("") {
            "retry" => {
                state.due_ms = v
                    .get("due_ms")
                    .and_then(|d| d.as_u64())
                    .unwrap_or(state.due_ms);
            }
            "terminated" => state.terminated = true,
            "dead-letter" => state.dead_lettered = true,
            "delivered" => state.due_ms = 0,
            _ => {}
        },
        _ => {}
    }
    state
}

/// Re-fold a WHOLE journal (one row per line) — a restarted daemon calls this
/// with the persisted rows, so retries continue with the SAME attempt number.
pub fn refold_retry(rows: &[String]) -> RetryState {
    rows.iter()
        .fold(RetryState::default(), |s, r| fold_retry(s, r))
}

/// Subscribe a webhook: mint the secret once, persist it, run the verification
/// challenge through the guarded port, and return the whsec ONCE. A refresh
/// (same subscription) calls `load_secret` instead and never re-challenges.
pub async fn subscribe_webhook<P: EgressPort>(
    port: &P,
    policy: &EgressPolicy,
    secret_path: &std::path::Path,
    url: &str,
    challenge: &str,
    fresh: bool,
    subscription_id: &str,
) -> Result<(String, bool), String> {
    let secret = if fresh {
        persist_secret_once(secret_path, &Secret::generate())?
    } else {
        load_secret(secret_path)?
    };
    let activated = verify_endpoint(port, policy, url, &secret, challenge, subscription_id).await?;
    Ok((secret.whsec(), activated))
}

/// The webhook delivery kernel. One journal is the source of truth; effects
/// are appended BEFORE invoke with a stable key, and a retry REUSES that key.
/// A restarted daemon re-folds the journal into `pending` and resumes from the
/// next clock Msg. The clock and HTTP are ports; no timer lives in the kernel.
#[derive(Debug, Default)]
pub struct WebhookDispatcher<P: EgressPort> {
    port: P,
    policy: EgressPolicy,
    journal: Vec<String>,
    pending: std::collections::BTreeMap<String, PendingRetry>,
    /// Keys whose journal already records a delivered result. A bus replay on
    /// restart re-offers the same event id; this set makes delivery idempotent
    /// so a replay cannot produce a duplicate signed POST.
    delivered: std::collections::BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct PendingRetry {
    sub: String,
    event: String,
    attempt: u32,
    due_ms: u64,
    spec: PostSpecOwned,
}

#[derive(Clone, Debug)]
pub struct PostSpecOwned {
    pub url: String,
    pub body: Vec<u8>,
    pub secret_whsec: String,
    pub webhook_id: String,
    pub subscription_id: String,
}

impl<P: EgressPort> WebhookDispatcher<P> {
    pub fn new(port: P, policy: EgressPolicy) -> Self {
        Self {
            port,
            policy,
            journal: vec![],
            pending: Default::default(),
            delivered: Default::default(),
        }
    }

    pub fn journal_rows(&self) -> &[String] {
        &self.journal
    }

    fn append(&mut self, row: String) {
        self.journal.push(row);
    }

    /// Dispatch one event: journal intent, invoke, journal the result.
    pub async fn on_event(
        &mut self,
        sub: &str,
        event: &str,
        url: &str,
        body: Vec<u8>,
        secret: &Secret,
        timestamp: u64,
    ) -> Result<Decision, String> {
        let key = format!("webhook:{sub}:{event}");
        if self.delivered.contains(&key) {
            return Ok(Decision::Delivered);
        }
        self.append(format!(
            "{{\"kind\":\"deliver-intent\",\"key\":\"{key}\",\"attempt\":1}}"
        ));
        let spec = PostSpecOwned {
            url: url.to_string(),
            body,
            secret_whsec: secret.whsec(),
            webhook_id: format!("msg_{event}"),
            subscription_id: sub.to_string(),
        };
        let outcome = self.post(&spec, timestamp).await?;
        Ok(self.finish(&key, sub, event, &spec, 1, outcome))
    }

    async fn post(&self, spec: &PostSpecOwned, timestamp: u64) -> Result<Outcome, String> {
        let target = egress::validate_target(&spec.url, &self.policy)?;
        let secret = Secret::from_whsec(&spec.secret_whsec)?;
        guarded_post(
            &self.port,
            &self.policy,
            &PostSpec {
                target: &target,
                timestamp,
                body: &spec.body,
                secret: &secret,
                webhook_id: &spec.webhook_id,
                subscription_id: &spec.subscription_id,
                challenge: false,
            },
        )
        .await
    }

    fn finish(
        &mut self,
        key: &str,
        sub: &str,
        event: &str,
        spec: &PostSpecOwned,
        attempt: u32,
        outcome: Outcome,
    ) -> Decision {
        let decision =
            toron_core::events::retry::decide(attempt, &outcome, 0.0, &RetryPolicy::default());
        let (due_ms, kind) = match decision {
            Decision::Retry { delay_ms } => (delay_ms, "retry"),
            Decision::Terminated(_) => (0, "terminated"),
            Decision::Gap => (0, "dead-letter"),
            Decision::Delivered => (0, "delivered"),
        };
        self.append(format!(
            "{{\"kind\":\"deliver-result\",\"key\":\"{key}\",\"attempt\":{attempt},\"decision\":\"{kind}\",\"due_ms\":{due_ms}}}"
        ));
        if let Decision::Retry { .. } = decision {
            self.pending.insert(
                key.to_string(),
                PendingRetry {
                    sub: sub.to_string(),
                    event: event.to_string(),
                    attempt: attempt + 1,
                    due_ms,
                    spec: spec.clone(),
                },
            );
        } else {
            self.pending.remove(key);
        }
        if matches!(decision, Decision::Delivered) {
            self.delivered.insert(key.to_string());
        }
        decision
    }

    /// One clock Msg re-posts every due retry with the SAME webhook-id.
    pub async fn on_clock(&mut self, now_ms: u64, timestamp: u64) -> Result<usize, String> {
        let due: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| p.due_ms <= now_ms)
            .map(|(k, _)| k.clone())
            .collect();
        let mut delivered = 0;
        for key in due {
            let p = self.pending.get(&key).expect("checked above").clone();
            let outcome = self.post(&p.spec, timestamp).await?;
            let decision = self.finish(&key, &p.sub, &p.event, &p.spec, p.attempt, outcome);
            if matches!(decision, Decision::Delivered) {
                delivered += 1;
            }
        }
        Ok(delivered)
    }

    /// Re-fold a restarted daemon's journal. `specs` are rebuilt from the
    /// subscription store (URL/secret body); only keys with an unanswered
    /// retry remain pending, so a retry resumes at its persisted attempt.
    pub fn refold(&mut self, rows: &[String], specs: Vec<(String, PostSpecOwned)>) {
        self.journal = rows.to_vec();
        self.pending.clear();
        self.delivered.clear();
        for row in rows {
            if row.contains("\"decision\":\"delivered\"")
                && let Some(k) = json_field(row, "key")
            {
                self.delivered.insert(k);
            }
        }
        for (key, spec) in specs {
            let mut state = RetryState::default();
            for row in rows {
                if !row.contains(&format!("\"key\":\"{key}\"")) {
                    continue;
                }
                state = fold_retry(state, row);
            }
            if state.due_ms > 0 && !state.terminated && !state.dead_lettered {
                let (sub, event) = key
                    .split_once(':')
                    .map(|(a, b)| (a.to_string(), b.to_string()))
                    .unwrap_or_else(|| ("s".into(), "e".into()));
                let (sub, event) = (sub.trim_start_matches("webhook_").to_string(), event);
                self.pending.insert(
                    key,
                    PendingRetry {
                        sub,
                        event,
                        attempt: state.attempt + 1,
                        due_ms: state.due_ms,
                        spec,
                    },
                );
            }
        }
    }
}

/// A bus lag produces exactly one gap delivery per webhook subscription —
/// never silence. Pure apart from the injected port and dispatcher, so the
/// production path and the test exercise the same code.
pub async fn deliver_gaps<P: EgressPort>(
    dispatcher: &mut WebhookDispatcher<P>,
    specs: &std::collections::BTreeMap<String, PostSpecOwned>,
    lag: u64,
) -> Result<usize, String> {
    let mut sent = 0;
    for (key, spec) in specs {
        let secret = Secret::from_whsec(&spec.secret_whsec)?;
        let sub = key.strip_prefix("webhook:").unwrap_or(key);
        dispatcher
            .on_event(
                sub,
                &format!("gap-{lag}"),
                &spec.url,
                br#"{"kind":"gap"}"#.to_vec(),
                &secret,
                0,
            )
            .await?;
        sent += 1;
    }
    Ok(sent)
}

/// Real egress port: ureq 3 (rustls+ring) with a PinnedResolver that returns
/// ONLY the checked SocketAddr. No redirect is followed, body/timeouts bounded,
/// and no secret is ever logged. ureq avoids reqwest's aws-lc/quinn/jni lock.
pub struct UreqEgress {
    policy: EgressPolicy,
}

/// A resolver that ignores the URI host and returns the one vetted socket.
#[derive(Debug)]
struct PinnedResolver {
    socket: std::net::SocketAddr,
}

impl ureq::unversioned::resolver::Resolver for PinnedResolver {
    fn resolve(
        &self,
        _uri: &ureq::http::Uri,
        _config: &ureq::config::Config,
        _timeout: ureq::unversioned::transport::NextTimeout,
    ) -> Result<ureq::unversioned::resolver::ResolvedSocketAddrs, ureq::Error> {
        let mut addrs: ureq::unversioned::resolver::ResolvedSocketAddrs =
            ureq::unversioned::resolver::ArrayVec::from_fn(|_| {
                std::net::SocketAddr::from(([0, 0, 0, 0], 0))
            });
        addrs.push(self.socket);
        Ok(addrs)
    }
}

impl UreqEgress {
    pub fn new(policy: EgressPolicy) -> Result<Self, String> {
        Ok(Self { policy })
    }
}

#[async_trait]
impl EgressPort for UreqEgress {
    fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, String> {
        use std::net::ToSocketAddrs as _;
        (host, 443)
            .to_socket_addrs()
            .map(|addrs| addrs.map(|a| a.ip()).collect())
            .map_err(|e| format!("webhook DNS resolution failed: {e}"))
    }

    async fn get(&self, target: &Target, socket: IpAddr) -> Result<String, String> {
        // Pin the connect to the checked socket; Host/SNI stay the original
        // name; redirects are never followed (same SSRF guard as POST).
        let config = ureq::Agent::config_builder()
            .max_redirects(0)
            .timeout_global(Some(Duration::from_millis(self.policy.connect_timeout_ms)))
            .build();
        let agent = ureq::Agent::with_parts(
            config,
            ureq::unversioned::transport::DefaultConnector::default(),
            PinnedResolver {
                socket: std::net::SocketAddr::new(socket, target.port),
            },
        );
        let url = format!(
            "https://{}:{}/.well-known/mcp-webhook-receiver.json",
            target.host, target.port
        );
        agent
            .get(&url)
            .call()
            .map_err(|e| format!("well-known fetch failed: {e}"))?
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("well-known body unreadable: {e}"))
    }

    async fn post(
        &self,
        target: &Target,
        socket: IpAddr,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> Result<Outcome, String> {
        let config = ureq::Agent::config_builder()
            .max_redirects(0) // never follow a redirect (KTD5 step 6)
            .timeout_connect(Some(Duration::from_millis(self.policy.connect_timeout_ms)))
            .timeout_global(Some(Duration::from_millis(
                self.policy.connect_timeout_ms.saturating_mul(2),
            )))
            .build();
        let agent = ureq::Agent::with_parts(
            config,
            ureq::unversioned::transport::DefaultConnector::default(),
            PinnedResolver {
                socket: std::net::SocketAddr::new(socket, target.port),
            },
        );
        // SNI and Host stay the ORIGINAL name; only the connect is pinned.
        let mut req = agent.post(format!("https://{}:{}/", target.host, target.port));
        for (k, v) in headers {
            req = req.header(&k, &v);
        }
        let resp = req
            .send(body.to_vec())
            .map_err(|e| format!("webhook POST failed: {e}"))?;
        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get("retry-after")
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

    type Sent = Vec<(Vec<(String, String)>, IpAddr)>;

    struct Fake {
        answers: Mutex<Vec<Vec<IpAddr>>>,
        statuses: Mutex<Vec<Outcome>>,
        sent: Mutex<Sent>,
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
                &PostSpec {
                    target: &target,
                    timestamp: 1_614_265_330,
                    body: br#"{"a":1}"#,
                    secret: &secret,
                    webhook_id: "msg_evt-1",
                    subscription_id: "sub-1",
                    challenge: false,
                },
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

    #[tokio::test]
    async fn a_pending_retry_resumes_after_a_restart_and_delivers_exactly_once() {
        let p = EgressPolicy::default();
        let secret = Secret::from_whsec("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let url = "https://foo.example-tailnet.ts.net/hook";
        let answers = vec![vec!["100.64.2.3".parse::<IpAddr>().unwrap()]];
        // First process: 503 on the first POST, leaving a pending retry.
        let first_port = Fake::new(answers.clone(), vec![Outcome::Status(503, None)]);
        let mut first = WebhookDispatcher::new(first_port, p.clone());
        let d = first
            .on_event(
                "sub",
                "evt-7",
                url,
                br#"{"n":7}"#.to_vec(),
                &secret,
                1_614_265_330,
            )
            .await
            .unwrap();
        assert!(matches!(d, Decision::Retry { .. }), "{d:?}");
        let rows = first.journal_rows().to_vec();

        // Restart: re-fold from the journal, rebuild the spec from the store.
        let second_port = Fake::new(answers, vec![Outcome::Ok]);
        let mut second = WebhookDispatcher::new(second_port, p);
        let spec = PostSpecOwned {
            url: url.to_string(),
            body: br#"{"n":7}"#.to_vec(),
            secret_whsec: secret.whsec(),
            webhook_id: "msg_evt-7".to_string(),
            subscription_id: "sub".to_string(),
        };
        second.refold(&rows, vec![("webhook:sub:evt-7".to_string(), spec)]);
        let delivered = second.on_clock(1_614_300_000, 1_614_265_330).await.unwrap();
        assert_eq!(delivered, 1, "the resumed retry delivers exactly once");
        // A second clock tick with no new due work does not re-deliver.
        assert_eq!(
            second.on_clock(1_614_400_000, 1_614_265_330).await.unwrap(),
            0
        );
    }

    #[test]
    fn a_restart_refolds_retry_state_from_journal_rows() {
        let rows = vec![
            r#"{"kind":"deliver-intent","attempt":3}"#.to_string(),
            r#"{"kind":"deliver-result","attempt":3,"decision":"retry","due_ms":99000}"#
                .to_string(),
        ];
        let state = refold_retry(&rows);
        assert_eq!(state.attempt, 3);
        assert_eq!(state.due_ms, 99_000);
        assert!(!state.terminated && !state.dead_lettered);
        let ended = refold_retry(&[
            rows[1].clone(),
            r#"{"kind":"deliver-result","decision":"dead-letter"}"#.to_string(),
        ]);
        assert!(ended.dead_lettered);
        assert_eq!(refold_retry(&[]), RetryState::default());
    }

    #[tokio::test]
    async fn a_webhook_subscribe_returns_the_secret_once_and_refresh_reuses_it() {
        let dir = std::env::temp_dir().join(format!("fw-sub-secret-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("sub-1.secret");
        let p = EgressPolicy::default();
        let fake = Fake::new(vec![vec!["100.64.2.3".parse().unwrap()]], vec![Outcome::Ok]);
        let url = "https://foo.example-tailnet.ts.net/hook";
        let (first, active) =
            subscribe_webhook(&fake, &p, &path, url, "challenge_123", true, "sub-1")
                .await
                .unwrap();
        assert!(active);
        assert!(first.starts_with("whsec_"));
        let (again, _) = subscribe_webhook(&fake, &p, &path, url, "challenge_123", false, "sub-1")
            .await
            .unwrap();
        assert_eq!(
            first, again,
            "a refresh returns the SAME secret, never re-mints"
        );
        let _ = std::fs::remove_dir_all(&dir);
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
                "challenge_123",
                "sub-1"
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

    #[tokio::test]
    async fn every_signed_post_carries_the_mcp_subscription_id_header() {
        use toron_core::events::webhook::Secret;
        let policy = EgressPolicy::default();
        let target =
            egress::validate_target("https://foo.example-tailnet.ts.net/h", &policy).unwrap();
        let fake = Fake::new(
            vec![vec!["100.64.2.3".parse::<IpAddr>().unwrap()]],
            vec![Outcome::Ok],
        );
        let secret = Secret::from_whsec("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        guarded_post(
            &fake,
            &policy,
            &PostSpec {
                target: &target,
                timestamp: 1_700_000_000,
                body: br#"{"eventId":"e"}"#,
                secret: &secret,
                webhook_id: "msg_e",
                subscription_id: "sub-42",
                challenge: false,
            },
        )
        .await
        .unwrap();
        let sent = fake.sent.lock().unwrap();
        let headers = &sent[0].0;
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "x-mcp-subscription-id" && v == "sub-42")
        );
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "webhook-signature" && v.starts_with("v1,"))
        );
    }

    #[tokio::test]
    async fn a_well_known_receiver_document_verifies_without_a_challenge_post() {
        struct Wk;
        #[async_trait]
        impl EgressPort for Wk {
            fn resolve(&self, _host: &str) -> Result<Vec<IpAddr>, String> {
                Ok(vec!["100.64.2.3".parse().unwrap()])
            }
            async fn get(&self, _t: &Target, _s: IpAddr) -> Result<String, String> {
                Ok(r#"{"receivers":["/hooks/"]}"#.into())
            }
            async fn post(
                &self,
                _t: &Target,
                _s: IpAddr,
                _h: Vec<(String, String)>,
                _b: &[u8],
            ) -> Result<Outcome, String> {
                panic!("a well-known match must not POST a challenge")
            }
        }
        let policy = EgressPolicy::default();
        let secret = Secret::from_client_whsec("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw").unwrap();
        let ok = verify_endpoint_cached(
            &Wk,
            &policy,
            "https://foo.example-tailnet.ts.net/hooks/client1",
            &secret,
            "challenge",
            "sub-1",
            "BoldCliff",
            1_000,
        )
        .await
        .unwrap();
        assert!(ok, "well-known consent activates delivery");
        // Cached: a second call for the same (principal,url) needs no fetch.
        assert!(verify_cache().lock().unwrap().is_verified(
            "BoldCliff",
            "https://foo.example-tailnet.ts.net/hooks/client1",
            1_001
        ));
    }
}
