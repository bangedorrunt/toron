// governed-by: ADR-0036
//! Wake-bus socket tail, slice B (bd-981p, Spec 8 row 2).
//!
//! `watch --live --watch-sock` reads wake frames from the daemon `wake.sock`
//! (ADR-0036 Spec 6 JSONL) instead of the in-process bus. Absent socket
//! degrades LOUD to today's in-process paths, never silent. NO deletions:
//! the pump and the in-process live tail stay (wave 3 deletes).
//!
//! EFFECT RULES (binding): no I/O in constructors (`new` only stores
//! config); timeouts are config with env overrides (injectable, no wall
//! clock reads in unit tests — fakes serve frames over a temp socket);
//! the socket is owned by the tail and closed on drop (RAII, no spawned
//! tasks to leak); every wait is bounded (handshake + idle timeouts);
//! failures are the narrow [`WakeSockError`], never strings across the
//! boundary.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Env override for socket discovery (tests, exotic layouts without a
/// shared state dir). Unset = `default_state_dir()/wake.sock`.
pub const WAKE_SOCK_ENV: &str = "TORON_WAKE_SOCK";

/// Handshake ceiling: connect + subscribe write (default 5s).
const HANDSHAKE_DEFAULT: Duration = Duration::from_secs(5);
/// Quiet ceiling: no frame of any kind (heartbeats reset it; a quiet bus
/// still heartbeats, so silence past this is a deaf watcher, never a
/// quiet bus). Default 60s.
const IDLE_DEFAULT: Duration = Duration::from_secs(60);

fn env_ms(name: &str, fallback: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(fallback)
}

/// Where the daemon sidecar listens (ADR-0036 Spec 7). Env read only —
/// no I/O, safe to call anywhere.
pub fn discover_sock_path() -> PathBuf {
    discover_from(std::env::var(WAKE_SOCK_ENV).ok())
}

/// Pure discovery core: explicit override wins, else the daemon state
/// dir. Tested without touching the process env (no `set_var`:)
/// `forbid(unsafe_code)` bans it outside integration tests).
fn discover_from(override_path: Option<String>) -> PathBuf {
    if let Some(p) = override_path
        && !p.is_empty()
    {
        return PathBuf::from(p);
    }
    crate::daemon::default_state_dir().join("wake.sock")
}

/// One wake event frame. Field-for-field the binding watch shape
/// `{kind, id, pubkey, content, tags, created_at}`, so the printed line is
/// byte-identical to the in-process tail (one mapping, two transports).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SockEvent {
    pub kind: u16,
    pub id: String,
    pub pubkey: String,
    pub content: String,
    #[serde(default)]
    pub tags: Vec<Vec<String>>,
    pub created_at: u64,
}

/// Subscribe request (client → daemon, one line; ADR-0036 Spec 6).
#[derive(Debug, Serialize)]
struct SubscribeReq<'a> {
    subscribe: SubscribeBody<'a>,
}

#[derive(Debug, Serialize)]
struct SubscribeBody<'a> {
    kinds: &'a [u16],
    since_cursor: Option<&'a str>,
    project: Option<&'a str>,
}

/// Frames (daemon → client, one per line; ADR-0036 Spec 6).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SockFrame {
    Event(SockEvent),
    Eose(EoseFrame),
    Heartbeat(HeartbeatFrame),
    Shed(ShedFrame),
}

#[derive(Debug, Deserialize)]
struct EoseFrame {
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HeartbeatFrame {
    #[serde(default)]
    ts_ms: u64,
}

#[derive(Debug, Deserialize)]
struct ShedFrame {
    resync_from: String,
}

/// Narrow tail failures. Every variant carries the loud reason the caller
/// prints before falling back — an unprinted error is a silent watcher.
#[derive(Debug)]
pub enum WakeSockError {
    /// No socket at the path: daemon predates the sidecar or is down.
    /// Degrade to in-process replay+live, LOUD.
    Absent(PathBuf),
    /// Socket exists but connect/handshake failed.
    Connect { path: PathBuf, reason: String },
    /// Subscribe write or handshake overran the ceiling.
    HandshakeTimeout { path: PathBuf },
    /// No frame (not even a heartbeat) inside the idle ceiling: deaf
    /// watcher, never a quiet bus. Degrade LOUD.
    IdleTimeout { path: PathBuf },
    /// A line that is not a Spec 6 frame. Loud (server is ours; a bad
    /// frame is a bug), then fall back.
    BadFrame { path: PathBuf, reason: String },
}

impl std::fmt::Display for WakeSockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WakeSockError::Absent(p) => write!(
                f,
                "wake.sock absent at {} — degrading to in-process replay+live (LOUD fallback, never silent)",
                p.display()
            ),
            WakeSockError::Connect { path, reason } => write!(
                f,
                "wake.sock connect failed at {} ({reason}) — degrading to in-process replay+live (LOUD fallback, never silent)",
                path.display()
            ),
            WakeSockError::HandshakeTimeout { path } => write!(
                f,
                "wake.sock handshake timed out at {} — degrading to in-process replay+live (LOUD fallback, never silent)",
                path.display()
            ),
            WakeSockError::IdleTimeout { path } => write!(
                f,
                "wake.sock idle (no frame, no heartbeat) at {} — deaf watcher, degrading to in-process replay+live (LOUD fallback, never silent)",
                path.display()
            ),
            WakeSockError::BadFrame { path, reason } => write!(
                f,
                "wake.sock bad frame at {} ({reason}) — degrading to in-process replay+live (LOUD fallback, never silent)",
                path.display()
            ),
        }
    }
}

impl std::error::Error for WakeSockError {}

/// Tail configuration. Pure data: constructing it touches nothing.
#[derive(Debug, Clone)]
pub struct WakeSockConfig {
    /// Socket path (discovery or explicit).
    pub sock_path: PathBuf,
    /// Kind allowlist for the subscribe line.
    pub kinds: Vec<u16>,
    /// Resume cursor; None = replay from store head, then live.
    pub since_cursor: Option<String>,
    /// Project slug for the subscribe line.
    pub project: Option<String>,
    /// Connect + subscribe-write ceiling.
    pub handshake_timeout: Duration,
    /// No-frame-at-all ceiling (heartbeats reset it).
    pub idle_timeout: Duration,
}

impl WakeSockConfig {
    /// No I/O: stores config only.
    pub fn new(sock_path: PathBuf, kinds: Vec<u16>) -> Self {
        Self {
            sock_path,
            kinds,
            since_cursor: None,
            project: None,
            handshake_timeout: env_ms("TORON_WAKE_SOCK_HANDSHAKE_MS", HANDSHAKE_DEFAULT),
            idle_timeout: env_ms("TORON_WAKE_SOCK_IDLE_MS", IDLE_DEFAULT),
        }
    }

    /// No I/O beyond env reads: discovery + kinds in one call.
    pub fn discover(kinds: Vec<u16>) -> Self {
        Self::new(discover_sock_path(), kinds)
    }
}

/// How a socket run ended.
#[derive(Debug, PartialEq)]
pub enum TailOutcome {
    /// Server closed cleanly. `last_cursor` is the newest EOSE cursor
    /// seen (None when the server never sent one) — the resume point
    /// for the next subscribe (dispatcher plane-S cursor, gap 1).
    Done { last_cursor: Option<String> },
    /// Slow consumer was shed: replay from the cursor via the store, then
    /// resubscribe. Zero loss, zero duplicates (dedup window on replays).
    Shed { resync_from: String },
}

/// The tail. Owns nothing until [`WakeSockTail::run`]; the socket closes
/// on drop (RAII — no spawned tasks, nothing to leak).
pub struct WakeSockTail {
    config: WakeSockConfig,
}

impl WakeSockTail {
    /// No I/O: stores config only.
    pub const fn new(config: WakeSockConfig) -> Self {
        Self { config }
    }

    /// Connect and subscribe. Hands back an open connection for callers
    /// that need per-frame control (`--once` match-or-deadline, `wait
    /// --corr` store re-check per wake — both must `await` between frames,
    /// which a sync `emit` closure cannot).
    pub async fn connect(&self) -> Result<WakeSockConn, WakeSockError> {
        let path = &self.config.sock_path;
        if !Path::new(path).exists() {
            return Err(WakeSockError::Absent(path.clone()));
        }
        let stream = tokio::time::timeout(
            self.config.handshake_timeout,
            tokio::net::UnixStream::connect(path),
        )
        .await
        .map_err(|_| WakeSockError::HandshakeTimeout { path: path.clone() })?
        .map_err(|e| WakeSockError::Connect {
            path: path.clone(),
            reason: e.to_string(),
        })?;
        let (read_half, mut write_half) = stream.into_split();
        let req = SubscribeReq {
            subscribe: SubscribeBody {
                kinds: &self.config.kinds,
                since_cursor: self.config.since_cursor.as_deref(),
                project: self.config.project.as_deref(),
            },
        };
        let mut line = serde_json::to_string(&req).expect("subscribe serializes");
        line.push('\n');
        tokio::time::timeout(
            self.config.handshake_timeout,
            write_half.write_all(line.as_bytes()),
        )
        .await
        .map_err(|_| WakeSockError::HandshakeTimeout { path: path.clone() })?
        .map_err(|e| WakeSockError::Connect {
            path: path.clone(),
            reason: e.to_string(),
        })?;
        Ok(WakeSockConn {
            lines: BufReader::new(read_half).lines(),
            path: path.clone(),
            idle_timeout: self.config.idle_timeout,
            last_cursor: self.config.since_cursor.clone(),
        })
    }

    /// Connect, subscribe, then replay-then-live. `emit` returns false to
    /// stop the tail after a match (`--once`); true keeps tailing.
    /// Heartbeats reset the idle clock; EOSE records the cursor and the
    /// stream continues live; shed returns [`TailOutcome::Shed`].
    pub async fn run(
        &self,
        emit: &mut impl FnMut(&SockEvent) -> bool,
    ) -> Result<TailOutcome, WakeSockError> {
        let mut conn = self.connect().await?;
        loop {
            match conn.next().await? {
                ConnEvent::Event(ev) => {
                    if !emit(&ev) {
                        return Ok(TailOutcome::Done {
                            last_cursor: conn.last_cursor.clone(),
                        });
                    }
                }
                ConnEvent::Eose | ConnEvent::Heartbeat => {}
                ConnEvent::Shed { resync_from } => {
                    return Ok(TailOutcome::Shed { resync_from });
                }
                ConnEvent::Closed => {
                    return Ok(TailOutcome::Done {
                        last_cursor: conn.last_cursor.clone(),
                    });
                }
            }
        }
    }
}

/// One item from the socket stream (bd-qg6w, wave 3): per-frame control
/// for callers that must `await` between frames.
#[derive(Debug, PartialEq)]
pub enum ConnEvent {
    Event(SockEvent),
    Eose,
    Heartbeat,
    Shed { resync_from: String },
    Closed,
}

/// An open wake.sock subscription. Owned lines + path; closes on drop
/// (RAII — no spawned tasks, nothing to leak).
pub struct WakeSockConn {
    lines: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    path: PathBuf,
    idle_timeout: Duration,
    /// Newest EOSE cursor seen (resume point for resubscribe).
    pub last_cursor: Option<String>,
}

impl WakeSockConn {
    /// Next frame, bounded by the idle ceiling (any frame — event, EOSE,
    /// even heartbeat — resets it; silence past it is a deaf watcher).
    pub async fn next(&mut self) -> Result<ConnEvent, WakeSockError> {
        loop {
            let next = tokio::time::timeout(self.idle_timeout, self.lines.next_line())
                .await
                .map_err(|_| WakeSockError::IdleTimeout {
                    path: self.path.clone(),
                })?
                .map_err(|e| WakeSockError::Connect {
                    path: self.path.clone(),
                    reason: e.to_string(),
                })?;
            let Some(raw) = next else {
                return Ok(ConnEvent::Closed);
            };
            if raw.trim().is_empty() {
                continue;
            }
            let frame: SockFrame =
                serde_json::from_str(&raw).map_err(|e| WakeSockError::BadFrame {
                    path: self.path.clone(),
                    reason: e.to_string(),
                })?;
            match frame {
                SockFrame::Event(ev) => return Ok(ConnEvent::Event(ev)),
                SockFrame::Eose(e) => {
                    // Replay complete; the stream continues live on the
                    // same socket (replay-then-live, one connection).
                    self.last_cursor = e.cursor;
                    return Ok(ConnEvent::Eose);
                }
                SockFrame::Heartbeat(h) => {
                    // Quiet-bus proof: the arrival itself resets the
                    // idle clock. The stamp is the server's, kept for
                    // skew inspection, never trusted as our clock.
                    let _ = h.ts_ms;
                    return Ok(ConnEvent::Heartbeat);
                }
                SockFrame::Shed(s) => {
                    return Ok(ConnEvent::Shed {
                        resync_from: s.resync_from,
                    });
                }
            }
        }
    }
}

/// The printed line for a socket event — the SAME construction as the
/// in-process tail, so consumers cannot tell the transport apart
/// (BrainMsg mapping byte-identical by construction, not by review).
pub fn format_sock_line(ev: &SockEvent) -> String {
    crate::plane::format_watch_line(
        ev.kind,
        &ev.id,
        &ev.pubkey,
        &ev.content,
        &ev.tags,
        ev.created_at,
    )
}

#[cfg(test)]
mod wake_tail_tests {
    use super::*;
    use std::collections::HashSet;

    fn sample_event(id: &str) -> SockEvent {
        SockEvent {
            kind: 9,
            id: id.into(),
            pubkey: "abc".into(),
            content: "hello".into(),
            tags: vec![
                vec!["t".into(), "demo".into()],
                vec!["h".into(), "steer-adr35".into()],
            ],
            created_at: 1_790_050_000,
        }
    }

    /// Absent socket degrades LOUD: typed Absent + the reason names the
    /// fallback, never a silent hang or empty success.
    #[tokio::test]
    async fn wake_tail_absent_socket_degrades_loud() {
        let missing =
            std::env::temp_dir().join(format!("wake-tail-absent-{}.sock", uuid::Uuid::new_v4()));
        let tail = WakeSockTail::new(WakeSockConfig::new(missing.clone(), vec![9]));
        let mut seen: HashSet<String> = HashSet::new();
        let err = tail
            .run(&mut |ev| !seen.insert(ev.id.clone()))
            .await
            .expect_err("absent socket must fail loud, never silent success");
        assert!(
            matches!(err, WakeSockError::Absent(_)),
            "must be the typed Absent, got {err:?}"
        );
        let loud = err.to_string();
        assert!(
            loud.contains(&missing.display().to_string()),
            "reason names the path: {loud}"
        );
        assert!(
            loud.contains("in-process"),
            "reason names the fallback: {loud}"
        );
        assert!(
            loud.contains("never silent"),
            "reason promises loudness: {loud}"
        );
    }

    /// Socket frames map byte-identical to the in-process tail lines.
    #[test]
    fn wake_tail_socket_frames_map_identical() {
        let ev = sample_event("evt1");
        assert_eq!(
            format_sock_line(&ev),
            crate::plane::format_watch_line(
                ev.kind,
                &ev.id,
                &ev.pubkey,
                &ev.content,
                &ev.tags,
                ev.created_at
            ),
            "one mapping, two transports — byte-identical or the cutover lies"
        );
    }

    /// Discovery honors the override, else lands on wake.sock under the
    /// daemon state dir (pure core: no env mutation, no FS I/O).
    #[test]
    fn wake_tail_discover_honors_env_override() {
        let marker = format!("/tmp/wake-tail-{}", uuid::Uuid::new_v4());
        assert_eq!(discover_from(Some(marker.clone())), PathBuf::from(&marker));
        assert!(discover_from(Some(String::new())).ends_with("wake.sock"));
        assert!(discover_from(None).ends_with("wake.sock"));
    }

    async fn fake_server(listener: tokio::net::UnixListener, script: Vec<String>) {
        let (mut conn, _) = listener.accept().await.expect("accept");
        let mut reader = BufReader::new(&mut conn);
        let mut sub_line = String::new();
        reader
            .read_line(&mut sub_line)
            .await
            .expect("read subscribe");
        let req: serde_json::Value = serde_json::from_str(&sub_line).expect("subscribe parses");
        assert!(
            req.get("subscribe").is_some(),
            "first line subscribes: {sub_line}"
        );
        for frame in script {
            conn.write_all(frame.as_bytes()).await.expect("write frame");
            conn.write_all(b"\n").await.expect("newline");
        }
        // Hold briefly so the tail drains, then close (Done path).
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    fn sock_path(tag: &str) -> PathBuf {
        // Short names: macOS UDS paths must fit SUN_LEN (~104 bytes) and
        // TMPDIR is already long. Counter, not uuid.
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("wt-{tag}-{n}-{}.sock", std::process::id()))
    }

    /// Replay-then-live arrives in order on one connection; duplicates
    /// (replay/live overlap) emit once.
    #[tokio::test]
    async fn wake_tail_replay_then_live_order() {
        let path = sock_path("order");
        let listener = tokio::net::UnixListener::bind(&path).expect("bind fake wake.sock");
        let a = sample_event("A");
        let b = sample_event("B");
        let script = vec![
            serde_json::json!({"event": a}).to_string(),
            serde_json::json!({"eose": {"cursor": "c1"}}).to_string(),
            serde_json::json!({"event": b}).to_string(),
            serde_json::json!({"event": a}).to_string(), // replay/live overlap dup
        ];
        let server = tokio::spawn(fake_server(listener, script));
        let tail = WakeSockTail::new(WakeSockConfig::new(path.clone(), vec![9]));
        let mut order: Vec<String> = vec![];
        let mut seen: HashSet<String> = HashSet::new();
        let outcome = tail
            .run(&mut |ev| {
                // Dedup lives in the caller's gate; the tail only stops
                // on explicit false (the `--once` contract).
                if seen.insert(ev.id.clone()) {
                    order.push(ev.id.clone());
                }
                true
            })
            .await
            .expect("scripted tail runs");
        assert_eq!(
            outcome,
            TailOutcome::Done {
                last_cursor: Some("c1".into())
            }
        );
        assert_eq!(
            order,
            vec!["A".to_string(), "B".to_string()],
            "replay then live, dup once"
        );
        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// Per-frame control: `connect` + `next` yields frames one at a time
    /// so callers can `await` between them (the `--once`/`wait` contract).
    /// Heartbeats surface (liveness proof), never deliveries.
    #[tokio::test]
    async fn wake_tail_conn_yields_frames_one_at_a_time() {
        let path = sock_path("frames");
        let listener = tokio::net::UnixListener::bind(&path).expect("bind fake wake.sock");
        let a = sample_event("A");
        let script = vec![
            serde_json::json!({"heartbeat": {"ts_ms": 7}}).to_string(),
            serde_json::json!({"event": a}).to_string(),
            serde_json::json!({"eose": {"cursor": "c9"}}).to_string(),
        ];
        let server = tokio::spawn(fake_server(listener, script));
        let tail = WakeSockTail::new(WakeSockConfig::new(path.clone(), vec![9]));
        let mut conn = tail.connect().await.expect("scripted connect");
        assert_eq!(conn.next().await.expect("frame 1"), ConnEvent::Heartbeat);
        let ConnEvent::Event(got) = conn.next().await.expect("frame 2") else {
            panic!("frame 2 must be the event");
        };
        assert_eq!(got.id, "A");
        assert_eq!(conn.next().await.expect("frame 3"), ConnEvent::Eose);
        assert_eq!(conn.last_cursor.as_deref(), Some("c9"));
        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// Shed surfaces the resync cursor: replay via the store, then
    /// resubscribe — zero loss, zero duplicates.
    #[tokio::test]
    async fn wake_tail_shed_frame_surfaces_resync_cursor() {
        let path = sock_path("shed");
        let listener = tokio::net::UnixListener::bind(&path).expect("bind fake wake.sock");
        let script = vec![serde_json::json!({"shed": {"resync_from": "cursor-42"}}).to_string()];
        let server = tokio::spawn(fake_server(listener, script));
        let tail = WakeSockTail::new(WakeSockConfig::new(path.clone(), vec![9]));
        let mut emitted = 0;
        let outcome = tail
            .run(&mut |_| {
                emitted += 1;
                true
            })
            .await
            .expect("scripted tail runs");
        assert_eq!(
            outcome,
            TailOutcome::Shed {
                resync_from: "cursor-42".into()
            },
            "shed must carry the cursor the store replay resumes from"
        );
        assert_eq!(emitted, 0, "nothing emitted before the shed");
        server.abort();
        let _ = std::fs::remove_file(&path);
    }

    /// The shared emit gate dedups by id and enforces project + channel —
    /// the socket path cannot drift from the in-process path.
    #[test]
    fn wake_tail_gate_dedups_and_filters() {
        let project = "demo".to_string();
        let channel: Option<String> = Some("steer-adr35".into());
        let tags = sample_event("G").tags;
        let mut seen: HashSet<String> = HashSet::new();
        assert!(crate::plane::watch_gate(
            &mut seen,
            "G",
            &tags,
            &project,
            channel.as_ref()
        ));
        assert!(
            !crate::plane::watch_gate(&mut seen, "G", &tags, &project, channel.as_ref()),
            "dup id emits once"
        );
        assert!(
            !crate::plane::watch_gate(&mut HashSet::new(), "H", &tags, "other", channel.as_ref()),
            "foreign project filtered"
        );
        assert!(
            !crate::plane::watch_gate(
                &mut HashSet::new(),
                "H",
                &tags,
                &project,
                Some("other-room".to_string()).as_ref()
            ),
            "foreign channel filtered"
        );
        // Un-scoped events (no t tag) stay project-agnostic, like in-process.
        let bare: Vec<Vec<String>> = vec![vec!["h".into(), "steer-adr35".into()]];
        assert!(crate::plane::watch_gate(
            &mut HashSet::new(),
            "I",
            &bare,
            &project,
            channel.as_ref()
        ));
    }
}
