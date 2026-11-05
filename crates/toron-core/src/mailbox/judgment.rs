// governed-by: ADR-0033
//! Judgment bridge (bd-ncpt): ask flywheel's judgment math as a child process,
//! journal the typed `JudgmentEffect`, return answers + effect.
//!
//! Boundary discipline: toron never duplicates flywheel's validation math —
//! it shells out to `flywheel judgment-ask` (contract posted to crew A in
//! test-channel), validates the echoed `effect_key`/`pack_id` (corr authority:
//! a mismatched echo is a loud error, never an accepted answer), coerces
//! non-finite confidence to 0 (mirroring flywheel `fin`), and journals one
//! row per `effect_key` (idempotent replay: a recorded key returns the stored
//! row without re-invoking). No index pool → loud error, never silent.

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Default child-process budget (overridable per call, capped).
pub const JUDGMENT_DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Hard cap on child timeout (a judgment is interactive-scale, not batch).
pub const JUDGMENT_MAX_TIMEOUT_SECS: u64 = 300;
/// State-JSON cap: flywheel slices state itself, but the argv must stay sane.
pub const JUDGMENT_MAX_STATE_BYTES: usize = 65_536;

/// Resolve the flywheel binary: `$FLYWHEEL_BIN`, else `flywheel` on PATH.
pub fn flywheel_bin() -> PathBuf {
    std::env::var("FLYWHEEL_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("flywheel"))
}

/// argv for `flywheel judgment-ask` (no shell — passed to Command directly).
pub fn judgment_argv(
    effect_key: &str,
    pack_id: &str,
    state_json: &str,
    transport: &str,
) -> Vec<String> {
    vec![
        "judgment-ask".to_string(),
        "--effect-key".to_string(),
        effect_key.to_string(),
        "--pack-id".to_string(),
        pack_id.to_string(),
        "--state-json".to_string(),
        state_json.to_string(),
        "--transport".to_string(),
        transport.to_string(),
    ]
}

/// One journaled judgment (mirrors flywheel `JudgmentEffect` + answers).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JudgmentEffectRecord {
    pub effect_key: String,
    pub pack_id: String,
    pub pack_version: u32,
    pub model_id: String,
    pub confidence: f64,
    pub usage: String,
    pub fallback_flag: bool,
    pub escalated: bool,
    pub note: String,
    pub answers: serde_json::Value,
    /// Stable content hash of the judged state (ADR-0035 Spec 3: journal
    /// rows gain pack_version + state_hash). Defaulted so pre-bd-8yj0 rows
    /// parse; the TEXT effect_json column carries it, no migration needed.
    #[serde(default)]
    pub state_hash: String,
}

/// Outcome of `ask_judgment`: the record + whether it was replayed.
#[derive(Debug, Clone, PartialEq)]
pub struct JudgmentOutcome {
    pub record: JudgmentEffectRecord,
    pub replayed: bool,
}

/// Validate raw `judgment-ask` stdout into a record. Loud on: bad JSON,
/// echoed-key/pack mismatch, missing fields. Non-finite confidence coerces
/// to 0.0 with a note suffix (flywheel `fin` parity).
pub fn parse_judgment_stdout(
    expected_effect_key: &str,
    expected_pack_id: &str,
    stdout: &str,
) -> anyhow::Result<JudgmentEffectRecord> {
    let v: serde_json::Value = serde_json::from_str(stdout.trim()).map_err(|e| {
        anyhow::anyhow!(
            "judgment-ask returned bad JSON ({e}); stdout tail: {}",
            stdout_tail(stdout)
        )
    })?;
    record_from_value(expected_effect_key, expected_pack_id, &v)
}

/// Validate an already-parsed `judgment-ask` object (split for testability:
/// JSON text cannot spell non-finite floats, but a hostile transport could).
pub fn record_from_value(
    expected_effect_key: &str,
    expected_pack_id: &str,
    v: &serde_json::Value,
) -> anyhow::Result<JudgmentEffectRecord> {
    let obj = v
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("judgment-ask output must be one JSON object"))?;
    let str_field = |name: &str| {
        obj.get(name)
            .and_then(|x| x.as_str())
            .ok_or_else(|| anyhow::anyhow!("judgment-ask output missing string field `{name}`"))
            .map(str::to_string)
    };
    let effect_key = str_field("effect_key")?;
    if effect_key != expected_effect_key {
        anyhow::bail!(
            "judgment-ask echoed effect_key `{effect_key}` for request `{expected_effect_key}` \
             (corr authority — never accept a mismatched answer)"
        );
    }
    let pack_id = str_field("pack_id")?;
    if pack_id != expected_pack_id {
        anyhow::bail!(
            "judgment-ask echoed pack_id `{pack_id}` for request `{expected_pack_id}` \
             (never accept a mismatched answer)"
        );
    }
    let pack_version = obj
        .get("pack_version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|x| u32::try_from(x).ok())
        .ok_or_else(|| anyhow::anyhow!("judgment-ask output missing u32 field `pack_version`"))?;
    let confidence = obj
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| anyhow::anyhow!("judgment-ask output missing number field `confidence`"))?;
    let note = str_field("note").unwrap_or_default();
    let answers = obj
        .get("answers")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("judgment-ask output missing object field `answers`"))?;
    if !answers.is_object() {
        anyhow::bail!("judgment-ask `answers` must be an object");
    }
    Ok(JudgmentEffectRecord {
        effect_key,
        pack_id,
        pack_version,
        model_id: str_field("model_id")?,
        confidence,
        usage: str_field("usage").unwrap_or_default(),
        fallback_flag: obj
            .get("fallback_flag")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        escalated: obj
            .get("escalated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        note,
        answers,
        state_hash: String::new(),
    })
}

/// One question in a pack. IDs are for code; full meaning stays inline here.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PackQuestion {
    Choice { id: String, options: Vec<String> },
    Score { id: String, min: f64, max: f64 },
    Noul { id: String },
}

/// A versioned question pack.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Pack {
    pub id: &'static str,
    pub version: u32,
    pub questions: Vec<PackQuestion>,
}

fn choice(id: &str, options: &[&str]) -> PackQuestion {
    PackQuestion::Choice {
        id: id.to_string(),
        options: options.iter().map(|s| (*s).to_string()).collect(),
    }
}

fn score(id: &str, min: f64, max: f64) -> PackQuestion {
    PackQuestion::Score {
        id: id.to_string(),
        min,
        max,
    }
}

fn noul(id: &str) -> PackQuestion {
    PackQuestion::Noul { id: id.to_string() }
}

/// The five Spec-3 packs. Pure constructor, no globals.
pub fn pack_registry() -> Vec<Pack> {
    vec![
        Pack {
            id: "route-triage",
            version: 1,
            questions: vec![
                choice(
                    "primitive",
                    &["loop-work", "steer", "compose", "audit", "open", "none"],
                ),
                noul("risk_destructive"),
                noul("risk_security"),
            ],
        },
        Pack {
            id: "wake-priority",
            version: 1,
            questions: vec![
                score("urgency", 0.0, 1.0),
                score("actionability", 0.0, 1.0),
                noul("interrupt"),
            ],
        },
        Pack {
            id: "dispatchable",
            version: 1,
            questions: vec![noul("sufficient")],
        },
        Pack {
            id: "verify-citation",
            version: 1,
            questions: vec![choice(
                "support",
                &["supports", "contradicts", "insufficient"],
            )],
        },
        Pack {
            id: "hazard-guardrail",
            version: 1,
            questions: vec![noul("hazard"), score("severity", 0.0, 1.0)],
        },
    ]
}

/// Resolve a pack by id. `None` is unknown — the caller errors loud.
pub fn find_pack(pack_id: &str) -> Option<Pack> {
    pack_registry().into_iter().find(|p| p.id == pack_id)
}

fn sorted_value(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::with_capacity(m.len());
            for k in keys {
                out.insert(k.clone(), sorted_value(&m[k]));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(a) => {
            serde_json::Value::Array(a.iter().map(sorted_value).collect())
        }
        _ => v.clone(),
    }
}

/// Stable content hash of the judged state (FNV-1a over canonical JSON with
/// sorted object keys). Journal discrimination + replay identity; not a secret.
pub fn state_hash(state: &serde_json::Value) -> String {
    let canon = serde_json::to_string(&sorted_value(state)).unwrap_or_default();
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in canon.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{h:016x}")
}

/// Scripted hermetic answer for a known pack (offline transport). Fixed per
/// pack, so unit tests are deterministic; the state hash is still recorded
/// for journal discrimination. Noul-near-0.5 seeds escalate per the
/// confidence discipline (code routes those to human review).
pub fn offline_answer(
    effect_key: &str,
    pack: &Pack,
    state: &serde_json::Value,
) -> JudgmentEffectRecord {
    let (answers, confidence, escalated) = match pack.id {
        "route-triage" => (
            serde_json::json!({
                "primitive": "open",
                "risk_destructive": 0.1,
                "risk_security": 0.1
            }),
            0.6,
            false,
        ),
        "wake-priority" => (
            serde_json::json!({
                "urgency": 0.5,
                "actionability": 0.5,
                "interrupt": 0.5
            }),
            0.5,
            true,
        ),
        "dispatchable" => (serde_json::json!({"sufficient": 0.5}), 0.5, true),
        "verify-citation" => (serde_json::json!({"support": "insufficient"}), 0.5, true),
        "hazard-guardrail" => (
            serde_json::json!({"hazard": 0.1, "severity": 0.2}),
            0.9,
            false,
        ),
        _ => (serde_json::json!({}), 0.0, true),
    };
    JudgmentEffectRecord {
        effect_key: effect_key.to_string(),
        pack_id: pack.id.to_string(),
        pack_version: pack.version,
        model_id: "offline-seed".to_string(),
        confidence,
        usage: "0+0".to_string(),
        fallback_flag: false,
        escalated,
        note: "offline seed (hermetic, no subprocess)".to_string(),
        answers,
        state_hash: state_hash(state),
    }
}

/// Wake routing verdict for tick-side call sites (Spec 3.5 composite).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeGate {
    Inject,
    Digest,
    Queue,
}

/// Code-owned composite: 0.5*urgency + 0.3*actionability + 0.2*interrupt.
/// Thresholds are tuned on project data; changing them never re-runs inference.
pub fn decide_wake_gate(urgency: f64, actionability: f64, interrupt: f64) -> WakeGate {
    let s = 0.5_f64.mul_add(urgency, 0.3_f64.mul_add(actionability, 0.2 * interrupt));
    if s >= 0.65 {
        WakeGate::Inject
    } else if s >= 0.35 {
        WakeGate::Queue
    } else {
        WakeGate::Digest
    }
}

/// Route-triage verdict for tick-side call sites.
#[derive(Debug, Clone, PartialEq)]
pub struct RouteDecision {
    pub primitive: String,
    pub needs_human: bool,
}

/// High per-risk Noul routes to human; never auto-acts through risk.
pub fn route_triage_decision(
    primitive: &str,
    risk_destructive: f64,
    risk_security: f64,
) -> RouteDecision {
    RouteDecision {
        primitive: primitive.to_string(),
        needs_human: risk_destructive >= 0.7 || risk_security >= 0.7,
    }
}

/// Validate call args before spawning: non-empty keys, bounded state/transport/timeout.
pub fn check_judgment_args(
    effect_key: &str,
    pack_id: &str,
    state: &serde_json::Value,
    transport: &str,
    timeout_secs: Option<u64>,
) -> anyhow::Result<(String, u64)> {
    if effect_key.is_empty() {
        anyhow::bail!("ask_judgment needs a non-empty effect_key (idempotency key)");
    }
    if pack_id.is_empty() {
        anyhow::bail!("ask_judgment needs a non-empty pack_id (flywheel resolves the pack)");
    }
    if !matches!(transport, "offline" | "system-one") {
        anyhow::bail!("ask_judgment transport must be offline|system-one, got `{transport}`");
    }
    let state_json = serde_json::to_string(state)?;
    if state_json.len() > JUDGMENT_MAX_STATE_BYTES {
        anyhow::bail!(
            "ask_judgment state is {} bytes over the {}-byte cap — shrink state (flywheel slices it too)",
            state_json.len(),
            JUDGMENT_MAX_STATE_BYTES
        );
    }
    let timeout = timeout_secs.unwrap_or(JUDGMENT_DEFAULT_TIMEOUT_SECS);
    if timeout == 0 || timeout > JUDGMENT_MAX_TIMEOUT_SECS {
        anyhow::bail!(
            "ask_judgment timeout_seconds must be 1..={JUDGMENT_MAX_TIMEOUT_SECS} (got {timeout})"
        );
    }
    Ok((state_json, timeout))
}

fn stdout_tail(s: &str) -> String {
    const N: usize = 300;
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= N {
        return s.to_string();
    }
    format!("…{}", chars[chars.len() - N..].iter().collect::<String>())
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Insert-or-keep one judgment row (effect_key PK: races keep the first row).
pub async fn journal_insert(
    pool: &sqlx::SqlitePool,
    project: &str,
    record: &JudgmentEffectRecord,
) -> anyhow::Result<()> {
    let answers_json = serde_json::to_string(&record.answers)?;
    let effect_json = serde_json::to_string(record)?;
    sqlx::query(
        "INSERT OR IGNORE INTO judgment_effects \
         (effect_key, project, pack_id, pack_version, model_id, confidence, \
          escalated, fallback_flag, answers_json, effect_json, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind(&record.effect_key)
    .bind(project)
    .bind(&record.pack_id)
    .bind(i64::from(record.pack_version))
    .bind(&record.model_id)
    .bind(record.confidence)
    .bind(i64::from(record.escalated))
    .bind(i64::from(record.fallback_flag))
    .bind(answers_json)
    .bind(effect_json)
    .bind(now_micros())
    .execute(pool)
    .await?;
    Ok(())
}

/// Read-your-write: the stored row for an effect_key, if any.
pub async fn journal_get(
    pool: &sqlx::SqlitePool,
    effect_key: &str,
) -> anyhow::Result<Option<JudgmentEffectRecord>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT effect_json FROM judgment_effects WHERE effect_key = ?1")
            .bind(effect_key)
            .fetch_optional(pool)
            .await?;
    row.map(|(s,)| {
        serde_json::from_str(&s)
            .map_err(|e| anyhow::anyhow!("stored judgment row for `{effect_key}` is corrupt ({e})"))
    })
    .transpose()
}

/// Wake-priority input scores (travel together; one struct, not three args).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WakeScores {
    pub urgency: f64,
    pub actionability: f64,
    pub interrupt: f64,
}

impl super::Mailbox {
    /// Host-owned judgment kernel model (bd-uhkr): every clone shares one
    /// instance, so duplicate keys dedupe across callers (never per-call new).
    pub const fn judge_model(&self) -> &Arc<parking_lot::Mutex<JudgeModel>> {
        &self.judge_model
    }

    /// `ask_judgment`: idempotent replay → claim → child `flywheel judgment-ask` under
    /// a timeout → validate → journal → return. Loud typed errors at every
    /// stage (binary missing, timeout, non-zero, bad JSON, key mismatch).
    pub async fn ask_judgment(
        &self,
        project_slug: &str,
        effect_key: &str,
        pack_id: &str,
        state: &serde_json::Value,
        transport: &str,
        timeout_secs: Option<u64>,
    ) -> anyhow::Result<JudgmentOutcome> {
        let (state_json, timeout) =
            check_judgment_args(effect_key, pack_id, state, transport, timeout_secs)?;
        let pool = self.index_pool.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "ask_judgment journal unavailable (no index open) — \
                 refusing rather than returning an unjournaled answer"
            )
        })?;
        let expected_version = find_pack(pack_id).map(|p| p.version);
        let expected_hash = state_hash(state);
        if let Some(record) =
            journal_get_final_for(pool, effect_key, expected_version, &expected_hash).await?
        {
            return Ok(JudgmentOutcome {
                record,
                replayed: true,
            });
        }
        if !claim_judgment(pool, project_slug, effect_key, pack_id, expected_version).await? {
            if let Some(record) =
                journal_get_final_for(pool, effect_key, expected_version, &expected_hash).await?
            {
                return Ok(JudgmentOutcome {
                    record,
                    replayed: true,
                });
            }
            anyhow::bail!(
                "ask_judgment `{effect_key}` inflight under another caller (claim-before-invoke dedup) — retry after it settles, never spawn twice"
            );
        }
        if transport == "offline"
            && let Some(pack) = find_pack(pack_id)
        {
            let record = offline_answer(effect_key, &pack, state);
            if !journal_settle(pool, project_slug, &record).await? {
                let record =
                    journal_get_final_for(pool, effect_key, expected_version, &expected_hash)
                        .await?
                        .unwrap_or(record);
                return Ok(JudgmentOutcome {
                    record,
                    replayed: true,
                });
            }
            return Ok(JudgmentOutcome {
                record,
                replayed: false,
            });
        }
        let bin = flywheel_bin();
        let argv = judgment_argv(effect_key, pack_id, &state_json, transport);
        let mut cmd = tokio::process::Command::new(&bin);
        cmd.kill_on_drop(true);
        let child = cmd
            .args(&argv)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| {
                anyhow::anyhow!(
                    "ask_judgment cannot run `{}` ({e}): install flywheel \
                     (mbx +nightly-aarch64-apple-darwin install --path . --force) \
                     or set FLYWHEEL_BIN",
                    bin.display()
                )
            })?;
        let out = tokio::time::timeout(
            std::time::Duration::from_secs(timeout),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "ask_judgment timed out after {timeout}s (effect_key `{effect_key}`, child killed): \
                 raise timeout_seconds (max {JUDGMENT_MAX_TIMEOUT_SECS}) or shrink state"
            )
        })?
        .map_err(|e| anyhow::anyhow!("ask_judgment child I/O failed ({e})"))?;
        if !out.status.success() {
            anyhow::bail!(
                "ask_judgment child exited {}: {}",
                out.status,
                stdout_tail(&String::from_utf8_lossy(&out.stderr))
            );
        }
        let stdout = String::from_utf8_lossy(&out.stdout);
        let mut record = parse_judgment_stdout(effect_key, pack_id, &stdout)?;
        record.state_hash = state_hash(state);
        if !journal_settle(pool, project_slug, &record).await? {
            let record = journal_get_final_for(pool, effect_key, expected_version, &expected_hash)
                .await?
                .unwrap_or(record);
            return Ok(JudgmentOutcome {
                record,
                replayed: true,
            });
        }
        Ok(JudgmentOutcome {
            record,
            replayed: false,
        })
    }

    /// Production seam (bd-sd9j): tick → journal → interpret → route for
    /// wake priority. Builds the deferred `Effect::Judge`, runs it through
    /// `ask_judgment` (journal-before-ask inside), then applies code-owned
    /// confidence routing plus the wake-gate composite. Returns the gate,
    /// the route, and the journaled outcome (proof).
    pub async fn judge_wake_priority(
        &self,
        project_slug: &str,
        effect_key: &str,
        scores: WakeScores,
        transport: &str,
        timeout_secs: Option<u64>,
    ) -> anyhow::Result<(WakeGate, JudgeRoute, JudgmentOutcome)> {
        let state = serde_json::json!({
            "urgency": scores.urgency,
            "actionability": scores.actionability,
            "interrupt": scores.interrupt,
        });
        let effect = JudgeEffect::request(
            effect_key,
            "wake-priority",
            &state,
            transport,
            timeout_secs.unwrap_or(JUDGMENT_DEFAULT_TIMEOUT_SECS),
        );
        let emitted: Option<JudgeEffect> = {
            let mut guard = self.judge_model.lock();
            let (next, effects) = tick(&guard, &JudgeMsg::Request { effect });
            *guard = next;
            drop(guard);
            effects.into_iter().next().map(|fx| {
                let Effect::Judge(effect) = fx;
                effect
            })
        };
        let Some(effect) = emitted else {
            let pool = self.index_pool.as_ref().ok_or_else(|| {
                anyhow::anyhow!("judge_wake_priority journal unavailable (no index open)")
            })?;
            let expected_version = find_pack("wake-priority").map(|p| p.version);
            let expected_hash = state_hash(&state);
            if let Some(record) =
                journal_get_final_for(pool, effect_key, expected_version, &expected_hash).await?
            {
                let answers = &record.answers;
                let num = |name: &str| {
                    answers
                        .get(name)
                        .and_then(serde_json::Value::as_f64)
                        .ok_or_else(|| {
                            anyhow::anyhow!("wake-priority answer missing number `{name}`")
                        })
                };
                let gate =
                    decide_wake_gate(num("urgency")?, num("actionability")?, num("interrupt")?);
                let route = route_outcome(&record);
                return Ok((
                    gate,
                    route,
                    JudgmentOutcome {
                        record,
                        replayed: true,
                    },
                ));
            }
            anyhow::bail!(
                "judge_wake_priority `{effect_key}` inflight under another caller (host-model dedup) — retry after it settles, never spawn twice"
            );
        };
        let outcome = self
            .ask_judgment(
                project_slug,
                &effect.effect_key,
                &effect.pack_id,
                &effect.state,
                &effect.transport,
                Some(effect.timeout_secs),
            )
            .await;
        self.judge_model.lock().inflight.remove(effect_key);
        let outcome = outcome?;
        let answers = &outcome.record.answers;
        let num = |name: &str| {
            answers
                .get(name)
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| anyhow::anyhow!("wake-priority answer missing number `{name}`"))
        };
        let gate = decide_wake_gate(num("urgency")?, num("actionability")?, num("interrupt")?);
        let route = route_outcome(&outcome.record);
        Ok((gate, route, outcome))
    }
}

use std::collections::{BTreeMap, VecDeque};

/// Typed judgment failures. Version/state drift on replay is a loud
/// mismatch, never a silent first-row accept (bd-fbgi).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JudgeError {
    /// Stored row names another pack version than the asking call.
    VersionMismatch {
        effect_key: String,
        stored: u32,
        asked: u32,
    },
    /// Stored row judged different state than the asking call.
    StalePack {
        effect_key: String,
        stored_hash: String,
        asked_hash: String,
    },
    /// No local pack and no way to resolve one.
    UnknownPack(String),
}

impl std::fmt::Display for JudgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VersionMismatch {
                effect_key,
                stored,
                asked,
            } => write!(
                f,
                "judgment replay version_mismatch for `{effect_key}`: stored pack v{stored}, asked v{asked} — re-ask under a fresh effect_key, never replay across versions"
            ),
            Self::StalePack {
                effect_key,
                stored_hash,
                asked_hash,
            } => write!(
                f,
                "judgment replay stale_pack for `{effect_key}`: stored state {stored_hash}, asked {asked_hash} — the world moved; re-ask, never replay stale judgment"
            ),
            Self::UnknownPack(pack) => write!(
                f,
                "judgment pack `{pack}` is not in the local registry and no resolver was supplied"
            ),
        }
    }
}

impl std::error::Error for JudgeError {}

/// Deferred judgment: data only, no I/O. Built by `tick`, run once by the
/// interpreter (`interpret_judge` / `Mailbox::ask_judgment`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JudgeEffect {
    pub effect_key: String,
    pub pack_id: String,
    /// Registry version at construction; `None` when the pack is resolved
    /// downstream (version check then defers to the child, hash still binds).
    pub pack_version: Option<u32>,
    pub state: serde_json::Value,
    pub state_hash: String,
    pub transport: String,
    pub timeout_secs: u64,
}

impl JudgeEffect {
    /// Construct + hash in one step: callers never hand-assemble the fields.
    pub fn request(
        effect_key: &str,
        pack_id: &str,
        state: &serde_json::Value,
        transport: &str,
        timeout_secs: u64,
    ) -> Self {
        Self {
            effect_key: effect_key.to_string(),
            pack_id: pack_id.to_string(),
            pack_version: find_pack(pack_id).map(|p| p.version),
            state: state.clone(),
            state_hash: state_hash(state),
            transport: transport.to_string(),
            timeout_secs,
        }
    }
}

/// Closed interpreter effects. One Judge variant with one interpreter;
/// adding a variant without an interpreter arm is a compile error at the
/// match in `interpret_effect`.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    Judge(JudgeEffect),
}

/// Judgment kernel model: in-flight keys only. Clone-and-return keeps
/// `tick` pure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JudgeModel {
    pub inflight: BTreeMap<String, u64>,
}

impl JudgeModel {
    pub fn new() -> Self {
        Self::default()
    }
}

/// All kernel state changes enter as messages.
#[derive(Debug, Clone, PartialEq)]
pub enum JudgeMsg {
    /// Ask for judgment (idempotent per key: a duplicate emits nothing).
    Request { effect: JudgeEffect },
    /// The interpreter's answer landed; drop the in-flight key.
    Settled {
        effect_key: String,
        record: JudgmentEffectRecord,
    },
}

/// Pure decision: `(Model, Msg) -> (Model, Effects)`.
pub fn tick(model: &JudgeModel, msg: &JudgeMsg) -> (JudgeModel, Vec<Effect>) {
    let mut next = model.clone();
    match msg {
        JudgeMsg::Request { effect } => {
            if next.inflight.contains_key(&effect.effect_key) {
                return (next, Vec::new());
            }
            next.inflight.insert(effect.effect_key.clone(), 1);
            (next, vec![Effect::Judge(effect.clone())])
        }
        JudgeMsg::Settled { effect_key, .. } => {
            next.inflight.remove(effect_key);
            (next, Vec::new())
        }
    }
}

/// Machine config JSON for the visualizer (dispatch_core pattern).
pub fn machine_config() -> serde_json::Value {
    serde_json::json!({
        "machine": "judge_kernel",
        "states": ["idle", "inflight"],
        "transitions": [
            {"on": "Request", "guard": "key_not_inflight", "effects": ["Judge"]},
            {"on": "Settled", "guard": null, "effects": []},
        ],
        "guards": ["key_not_inflight"],
    })
}

/// Confidence routing (code-owned thresholds; changing them never re-runs
/// inference). Escalated answers and sub-threshold confidence route to a
/// human; everything else may act.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JudgeRoute {
    Act,
    EscalateHuman,
}

/// Noul-near-0.5 seeds escalate at the source, so 0.55 also catches bare
/// low-confidence answers without a flag.
pub fn route_outcome(record: &JudgmentEffectRecord) -> JudgeRoute {
    if record.escalated || record.confidence < 0.55 {
        JudgeRoute::EscalateHuman
    } else {
        JudgeRoute::Act
    }
}

/// Discriminating replay: a stored row replays ONLY on full identity
/// (effect_key + pack_version + state_hash). Drift is a loud typed error
/// (bd-fbgi), never a silent first-row accept. Rows journaled before
/// state hashes existed carry an empty hash: the hash check is skipped
/// for those (documented legacy), the version check still binds.
pub fn replay_check(
    stored: &JudgmentEffectRecord,
    expected_version: Option<u32>,
    expected_hash: &str,
) -> Result<(), JudgeError> {
    if let Some(asked) = expected_version
        && stored.pack_version != asked
    {
        return Err(JudgeError::VersionMismatch {
            effect_key: stored.effect_key.clone(),
            stored: stored.pack_version,
            asked,
        });
    }
    if !stored.state_hash.is_empty() && stored.state_hash != expected_hash {
        return Err(JudgeError::StalePack {
            effect_key: stored.effect_key.clone(),
            stored_hash: stored.state_hash.clone(),
            asked_hash: expected_hash.to_string(),
        });
    }
    Ok(())
}

/// Journal read with identity: `None` (no row) or the checked row.
pub async fn journal_get_for(
    pool: &sqlx::SqlitePool,
    effect_key: &str,
    expected_version: Option<u32>,
    expected_hash: &str,
) -> anyhow::Result<Option<JudgmentEffectRecord>> {
    let Some(record) = journal_get(pool, effect_key).await? else {
        return Ok(None);
    };
    replay_check(&record, expected_version, expected_hash).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Some(record))
}

/// Placeholder model id marking an unsettled claim (never a final answer).
pub const CLAIM_MODEL_ID: &str = "__claim__";

/// True when the row is an unsettled claim, not a judged answer.
pub fn is_claim_record(record: &JudgmentEffectRecord) -> bool {
    record.model_id == CLAIM_MODEL_ID
}

/// Insert a Prepared claim (INSERT OR IGNORE: the first claimant wins).
/// Returns true when this caller owns the claim (may spawn), false when a
/// row already exists (another caller owns — never spawn).
pub async fn claim_judgment(
    pool: &sqlx::SqlitePool,
    project: &str,
    effect_key: &str,
    pack_id: &str,
    pack_version: Option<u32>,
) -> anyhow::Result<bool> {
    let claim = JudgmentEffectRecord {
        effect_key: effect_key.to_string(),
        pack_id: pack_id.to_string(),
        pack_version: pack_version.unwrap_or(0),
        model_id: CLAIM_MODEL_ID.to_string(),
        confidence: 0.0,
        usage: String::new(),
        fallback_flag: false,
        escalated: false,
        note: "claim (bd-uhkr, unsettled)".to_string(),
        answers: serde_json::json!({}),
        state_hash: String::new(),
    };
    let answers_json = serde_json::to_string(&claim.answers)?;
    let effect_json = serde_json::to_string(&claim)?;
    let res = sqlx::query(
        "INSERT OR IGNORE INTO judgment_effects \
         (effect_key, project, pack_id, pack_version, model_id, confidence, \
          escalated, fallback_flag, answers_json, effect_json, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind(&claim.effect_key)
    .bind(project)
    .bind(&claim.pack_id)
    .bind(i64::from(claim.pack_version))
    .bind(&claim.model_id)
    .bind(claim.confidence)
    .bind(0_i64)
    .bind(0_i64)
    .bind(answers_json)
    .bind(effect_json)
    .bind(now_micros())
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// Journal read that skips unsettled claims: `None` on no row OR claim row,
/// the checked final row otherwise (drift still loud).
pub async fn journal_get_final_for(
    pool: &sqlx::SqlitePool,
    effect_key: &str,
    expected_version: Option<u32>,
    expected_hash: &str,
) -> anyhow::Result<Option<JudgmentEffectRecord>> {
    let Some(record) = journal_get(pool, effect_key).await? else {
        return Ok(None);
    };
    if is_claim_record(&record) {
        return Ok(None);
    }
    replay_check(&record, expected_version, expected_hash).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Some(record))
}

/// Settle a claim into the final answer (one UPDATE guarded on the claim
/// marker: a settled-by-other row is never overwritten). Returns true when
/// this caller settled, false when another caller already did.
pub async fn journal_settle(
    pool: &sqlx::SqlitePool,
    project: &str,
    record: &JudgmentEffectRecord,
) -> anyhow::Result<bool> {
    let answers_json = serde_json::to_string(&record.answers)?;
    let effect_json = serde_json::to_string(record)?;
    let res = sqlx::query(
        "UPDATE judgment_effects SET project = ?2, pack_id = ?3, pack_version = ?4, \
          model_id = ?5, confidence = ?6, escalated = ?7, fallback_flag = ?8, \
          answers_json = ?9, effect_json = ?10, created_at = ?11 \
         WHERE effect_key = ?1 AND model_id = '__claim__'",
    )
    .bind(&record.effect_key)
    .bind(project)
    .bind(&record.pack_id)
    .bind(i64::from(record.pack_version))
    .bind(&record.model_id)
    .bind(record.confidence)
    .bind(i64::from(record.escalated))
    .bind(i64::from(record.fallback_flag))
    .bind(answers_json)
    .bind(effect_json)
    .bind(now_micros())
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// Crash-resume: reap unsettled claims older than `older_than_micros`
/// (age 0 reaps all). Returns the reaped count — the keys are claimable
/// again, and no silent pay ever existed for them.
pub async fn reconcile_judgment_claims(
    pool: &sqlx::SqlitePool,
    older_than_micros: i64,
) -> anyhow::Result<u64> {
    let cutoff = now_micros().saturating_sub(older_than_micros.max(0));
    let res = sqlx::query(
        "DELETE FROM judgment_effects WHERE model_id = '__claim__' AND created_at <= ?1",
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Scripted substitute for the interpreter (tests + dry runs): per-key
/// answer/fail/hang scripts with millisecond runtimes — no subprocess,
/// no sleeps. A drained script replays its last answer (stable under
/// retry); unknown keys escalate loudly.
#[derive(Debug, Clone, PartialEq)]
pub enum FakeStep {
    Answer(JudgmentEffectRecord),
    Fail(String),
    Hang,
}

#[derive(Debug, Default)]
pub struct FakeJudge {
    script: BTreeMap<String, VecDeque<FakeStep>>,
    pub calls: u64,
}

impl FakeJudge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Script one key: fail-twice-then-succeed is
    /// `[Fail, Fail, Answer]`; hang is `[Hang]`; storm is N `Answer`s.
    pub fn script(&mut self, effect_key: &str, steps: Vec<FakeStep>) {
        self.script.insert(effect_key.to_string(), steps.into());
    }

    /// Run one deferred effect (sync, ms-scale). Hang surfaces as the
    /// same timeout shape the live interpreter reports.
    pub fn answer(&mut self, effect: &JudgeEffect) -> anyhow::Result<JudgmentOutcome> {
        self.calls += 1;
        let queue = self.script.get_mut(&effect.effect_key).ok_or_else(|| {
            anyhow::anyhow!(
                "fake judge has no script for `{}` — script every key under test",
                effect.effect_key
            )
        })?;
        let step = if queue.len() > 1 {
            queue.pop_front().expect("len checked")
        } else {
            queue.front().cloned().expect("script never empty")
        };
        match step {
            FakeStep::Answer(record) => Ok(JudgmentOutcome {
                record,
                replayed: false,
            }),
            FakeStep::Fail(reason) => anyhow::bail!("fake judge failure: {reason}"),
            FakeStep::Hang => anyhow::bail!(
                "fake judge hang on `{}` (timeout shape, no sleep taken)",
                effect.effect_key
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_out(key: &str, pack: &str) -> String {
        serde_json::json!({
            "effect_key": key,
            "pack_id": pack,
            "pack_version": 2,
            "model_id": "test-model",
            "confidence": 0.8,
            "usage": "1+1",
            "fallback_flag": false,
            "escalated": false,
            "note": "ok",
            "answers": {"model_id": "m", "pack_id": pack, "pack_version": 2,
                        "by_question": {}, "usage": "1+1"}
        })
        .to_string()
    }

    #[test]
    fn test_stdout_tail_multibyte_safe_bd_7qcw() {
        let s = "é".repeat(400);
        let tail = stdout_tail(&s);
        assert!(tail.starts_with('…'), "truncated marker");
        assert_eq!(tail.chars().count(), 301, "300 chars + marker");
        assert!(tail.ends_with('é'), "char intact");
        assert_eq!(stdout_tail("short"), "short");
    }

    #[test]
    fn test_argv_shape_bd_ncpt() {
        let argv = judgment_argv("k1", "p1", "{\"a\":1}", "offline");
        assert_eq!(
            argv,
            vec![
                "judgment-ask",
                "--effect-key",
                "k1",
                "--pack-id",
                "p1",
                "--state-json",
                "{\"a\":1}",
                "--transport",
                "offline",
            ]
        );
    }

    #[test]
    fn test_parse_ok_bd_ncpt() {
        let r = parse_judgment_stdout("k", "p", &sample_out("k", "p")).expect("parse");
        assert_eq!(r.effect_key, "k");
        assert_eq!(r.pack_version, 2);
        assert!((r.confidence - 0.8).abs() < 1e-9);
    }

    #[test]
    fn test_parse_key_mismatch_loud_bd_ncpt() {
        let e = parse_judgment_stdout("k", "p", &sample_out("OTHER", "p")).unwrap_err();
        assert!(e.to_string().contains("OTHER"), "names the echo: {e}");
    }

    #[test]
    fn test_parse_pack_mismatch_loud_bd_ncpt() {
        let e = parse_judgment_stdout("k", "p", &sample_out("k", "OTHER")).unwrap_err();
        assert!(e.to_string().contains("OTHER"), "names the echo: {e}");
    }

    #[test]
    fn test_parse_bad_json_loud_bd_ncpt() {
        let e = parse_judgment_stdout("k", "p", "not json{{").unwrap_err();
        assert!(e.to_string().contains("bad JSON"), "{e}");
    }

    #[test]
    fn test_parse_confidence_passthrough_bd_ncpt() {
        let r = parse_judgment_stdout("k", "p", &sample_out("k", "p")).expect("parse");
        assert!((r.confidence - 0.8).abs() < 1e-9);
        assert_eq!(r.note, "ok");
    }

    #[test]
    fn test_args_reject_empty_keys_bd_ncpt() {
        assert!(check_judgment_args("", "p", &serde_json::json!({}), "offline", None).is_err());
        assert!(check_judgment_args("k", "", &serde_json::json!({}), "offline", None).is_err());
        assert!(check_judgment_args("k", "p", &serde_json::json!({}), "bogus", None).is_err());
        assert!(check_judgment_args("k", "p", &serde_json::json!({}), "offline", Some(0)).is_err());
        let (sj, t) = check_judgment_args("k", "p", &serde_json::json!({"a": 1}), "offline", None)
            .expect("ok");
        assert_eq!(t, JUDGMENT_DEFAULT_TIMEOUT_SECS);
        assert_eq!(sj, "{\"a\":1}");
    }

    #[test]
    fn test_args_reject_oversize_state_bd_ncpt() {
        let big = serde_json::json!({"blob": "x".repeat(JUDGMENT_MAX_STATE_BYTES)});
        let e = check_judgment_args("k", "p", &big, "offline", None).unwrap_err();
        assert!(e.to_string().contains("cap"), "{e}");
    }

    #[test]
    fn test_pack_registry_has_five_packs_bd_8yj0() {
        let packs = pack_registry();
        let ids: Vec<&str> = packs.iter().map(|p| p.id).collect();
        assert_eq!(
            ids,
            vec![
                "route-triage",
                "wake-priority",
                "dispatchable",
                "verify-citation",
                "hazard-guardrail"
            ]
        );
        for p in &packs {
            assert!(p.version >= 1, "{} unversioned", p.id);
            assert!(!p.questions.is_empty(), "{} questionless", p.id);
        }
    }

    #[test]
    fn test_find_pack_unknown_is_none_bd_8yj0() {
        assert!(find_pack("nope").is_none());
        assert!(find_pack("route-triage").is_some());
    }

    #[test]
    fn test_offline_seed_deterministic_bd_8yj0() {
        let s = serde_json::json!({"goal": "x"});
        let pack = find_pack("wake-priority").expect("seeded");
        let a = offline_answer("k", &pack, &s);
        let b = offline_answer("k", &pack, &s);
        assert_eq!(a, b);
        assert_eq!(a.effect_key, "k");
        assert_eq!(a.pack_version, pack.version);
        assert!(!a.state_hash.is_empty(), "seed carries state hash");
    }

    #[test]
    fn test_state_hash_stable_and_order_insensitive_bd_8yj0() {
        let a = serde_json::json!({"x": 1, "y": {"b": 2, "a": 1}});
        let b = serde_json::json!({"y": {"a": 1, "b": 2}, "x": 1});
        assert_eq!(state_hash(&a), state_hash(&b));
        assert_ne!(
            state_hash(&a),
            state_hash(&serde_json::json!({"x": 2})),
            "content change moves the hash"
        );
    }

    #[test]
    fn test_wake_gate_thresholds_bd_8yj0() {
        assert_eq!(decide_wake_gate(0.9, 0.9, 0.9), WakeGate::Inject);
        assert_eq!(decide_wake_gate(0.5, 0.5, 0.5), WakeGate::Queue);
        assert_eq!(decide_wake_gate(0.1, 0.1, 0.1), WakeGate::Digest);
    }

    #[test]
    fn test_route_triage_high_risk_needs_human_bd_8yj0() {
        let d = route_triage_decision("steer", 0.9, 0.1);
        assert!(d.needs_human, "destructive risk routes to human");
        let calm = route_triage_decision("steer", 0.1, 0.1);
        assert_eq!(calm.primitive, "steer");
        assert!(!calm.needs_human);
    }

    #[test]
    fn test_old_row_without_state_hash_still_parses_bd_8yj0() {
        let full = serde_json::to_value(
            parse_judgment_stdout("k", "p", &sample_out("k", "p")).expect("parse"),
        )
        .expect("to value");
        let mut obj = full.as_object().expect("object").clone();
        obj.remove("state_hash");
        let r: JudgmentEffectRecord =
            serde_json::from_value(serde_json::Value::Object(obj)).expect("old row parses");
        assert_eq!(r.state_hash, "");
    }

    fn judge_effect(key: &str) -> JudgeEffect {
        JudgeEffect::request(
            key,
            "wake-priority",
            &serde_json::json!({"urgency": 0.9}),
            "offline",
            5,
        )
    }

    #[test]
    fn test_tick_request_emits_one_judge_and_dedupes() {
        let (m1, fx1) = tick(
            &JudgeModel::new(),
            &JudgeMsg::Request {
                effect: judge_effect("k"),
            },
        );
        assert_eq!(fx1.len(), 1);
        assert!(matches!(fx1[0], Effect::Judge(_)));
        let (_, fx2) = tick(
            &m1,
            &JudgeMsg::Request {
                effect: judge_effect("k"),
            },
        );
        assert!(fx2.is_empty());
        let record = offline_answer(
            "k",
            &find_pack("wake-priority").unwrap(),
            &serde_json::json!({}),
        );
        let (m3, _) = tick(
            &m1,
            &JudgeMsg::Settled {
                effect_key: "k".to_string(),
                record,
            },
        );
        let (_, fx4) = tick(
            &m3,
            &JudgeMsg::Request {
                effect: judge_effect("k"),
            },
        );
        assert_eq!(fx4.len(), 1);
    }

    #[test]
    fn test_tick_deterministic_bd_sd9j() {
        let log = vec![
            JudgeMsg::Request {
                effect: judge_effect("a"),
            },
            JudgeMsg::Request {
                effect: judge_effect("a"),
            },
            JudgeMsg::Request {
                effect: judge_effect("b"),
            },
        ];
        let replay = |log: &[JudgeMsg]| {
            let mut model = JudgeModel::new();
            let mut all = Vec::new();
            for msg in log {
                let (next, fx) = tick(&model, msg);
                model = next;
                all.extend(fx);
            }
            (model, all)
        };
        let (ma, fa) = replay(&log);
        let (mb, fb) = replay(&log);
        assert_eq!(format!("{fa:?}"), format!("{fb:?}"));
        assert_eq!(ma, mb);
        assert_eq!(fa.len(), 2, "two distinct keys, one dup suppressed");
    }

    #[test]
    fn test_replay_check_binds_version_and_hash_bd_fbgi() {
        let pack = find_pack("wake-priority").unwrap();
        let state = serde_json::json!({"urgency": 0.9});
        let record = offline_answer("k", &pack, &state);
        replay_check(&record, Some(pack.version), &state_hash(&state)).expect("match replays");
        let err = replay_check(&record, Some(pack.version + 1), &state_hash(&state)).unwrap_err();
        assert!(matches!(err, JudgeError::VersionMismatch { .. }), "{err}");
        assert!(err.to_string().contains("version_mismatch"));
        let err = replay_check(&record, Some(pack.version), "deadbeef").unwrap_err();
        assert!(matches!(err, JudgeError::StalePack { .. }), "{err}");
        assert!(err.to_string().contains("stale_pack"));
        let mut legacy = record;
        legacy.state_hash = String::new();
        replay_check(&legacy, Some(pack.version), "anything").expect("legacy hash skipped");
        assert!(replay_check(&legacy, Some(pack.version + 1), "anything").is_err());
    }

    #[test]
    fn test_route_outcome_thresholds_bd_sd9j() {
        let pack = find_pack("hazard-guardrail").unwrap();
        let calm = offline_answer("c", &pack, &serde_json::json!({}));
        assert_eq!(route_outcome(&calm), JudgeRoute::Act);
        let mut hot = calm.clone();
        hot.escalated = true;
        assert_eq!(route_outcome(&hot), JudgeRoute::EscalateHuman);
        let mut low = calm;
        low.escalated = false;
        low.confidence = 0.1;
        assert_eq!(route_outcome(&low), JudgeRoute::EscalateHuman);
    }

    #[test]
    fn test_machine_config_serializes_bd_sd9j() {
        let cfg = machine_config();
        assert_eq!(cfg["machine"], "judge_kernel");
    }

    #[test]
    fn test_fake_judge_fail_twice_then_succeed() {
        let pack = find_pack("dispatchable").unwrap();
        let state = serde_json::json!({});
        let answer = offline_answer("k", &pack, &state);
        let mut fake = FakeJudge::new();
        fake.script(
            "k",
            vec![
                FakeStep::Fail("boom-1".to_string()),
                FakeStep::Fail("boom-2".to_string()),
                FakeStep::Answer(answer.clone()),
            ],
        );
        let effect = judge_effect("k");
        assert!(fake.answer(&effect).is_err());
        assert!(fake.answer(&effect).is_err());
        let out = fake.answer(&effect).expect("third succeeds");
        assert_eq!(out.record, answer);
        assert_eq!(fake.calls, 3);
    }

    #[test]
    fn test_fake_judge_hang_is_timeout_shape() {
        let mut fake = FakeJudge::new();
        fake.script("h", vec![FakeStep::Hang]);
        let err = fake.answer(&judge_effect("h")).unwrap_err();
        assert!(err.to_string().contains("hang"), "{err}");
    }

    #[test]
    fn test_fake_judge_unknown_key_escalates_loudly() {
        let mut fake = FakeJudge::new();
        assert!(fake.answer(&judge_effect("ghost")).is_err());
    }

    #[tokio::test]
    async fn test_judge_wake_priority_seam_offline_bd_sd9j() {
        use std::sync::Arc;
        let tag = format!("seam-{}", uuid::Uuid::new_v4());
        let db_path = std::env::temp_dir().join(format!("toron-judge-{tag}.sqlite"));
        let pool = crate::index::schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");
        let bus = crate::bus::in_process::InProcessBus::open(
            pool.clone(),
            crate::bus::in_process::InProcessBusConfig::default(),
        )
        .await
        .expect("bus");
        let data_dir = std::env::temp_dir().join(format!("toron-judge-data-{tag}"));
        let mailbox = super::super::Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_index_pool(pool);
        let scores = WakeScores {
            urgency: 0.9,
            actionability: 0.9,
            interrupt: 0.9,
        };
        let (gate, route, outcome) = mailbox
            .judge_wake_priority("toron", "seam-k1", scores, "offline", Some(10))
            .await
            .expect("seam answers");
        assert_eq!(gate, WakeGate::Queue);
        assert_eq!(route, JudgeRoute::EscalateHuman);
        assert!(!outcome.replayed);
        assert_eq!(outcome.record.effect_key, "seam-k1");
        let (gate2, route2, outcome2) = mailbox
            .judge_wake_priority("toron", "seam-k1", scores, "offline", Some(10))
            .await
            .expect("replay answers");
        assert!(outcome2.replayed);
        assert_eq!((gate2, route2), (gate, route));
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_journal_get_for_discriminates_bd_fbgi() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("mem pool");
        crate::index::schema::MIGRATOR
            .run(&pool)
            .await
            .expect("migrate");
        let pack = find_pack("wake-priority").unwrap();
        let state = serde_json::json!({"urgency": 0.9});
        let record = offline_answer("k", &pack, &state);
        journal_insert(&pool, "toron", &record)
            .await
            .expect("insert");
        let hit = journal_get_for(&pool, "k", Some(pack.version), &state_hash(&state))
            .await
            .expect("get")
            .expect("row");
        assert_eq!(hit, record);
        assert!(
            journal_get_for(&pool, "k", Some(pack.version + 1), &state_hash(&state))
                .await
                .is_err()
        );
        assert!(
            journal_get_for(&pool, "k", Some(pack.version), "other")
                .await
                .is_err()
        );
        assert!(
            journal_get_for(&pool, "nope", Some(1), "x")
                .await
                .expect("get")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_journal_roundtrip_bd_ncpt() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("mem pool");
        crate::index::schema::MIGRATOR
            .run(&pool)
            .await
            .expect("migrate");
        assert!(
            journal_get(&pool, "nope").await.expect("get").is_none(),
            "empty first"
        );
        let r = parse_judgment_stdout("k", "p", &sample_out("k", "p")).expect("parse");
        journal_insert(&pool, "toron", &r).await.expect("insert");
        journal_insert(&pool, "toron", &r)
            .await
            .expect("re-insert keeps first");
        let back = journal_get(&pool, "k").await.expect("get").expect("row");
        assert_eq!(back, r);
    }

    async fn uhkr_pool() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("mem pool");
        crate::index::schema::MIGRATOR
            .run(&pool)
            .await
            .expect("migrate");
        pool
    }

    async fn uhkr_mailbox(pool: sqlx::SqlitePool) -> super::super::Mailbox {
        use std::sync::Arc;
        let tag = format!("uhkr-{}", uuid::Uuid::new_v4());
        let bus = crate::bus::in_process::InProcessBus::open(
            pool.clone(),
            crate::bus::in_process::InProcessBusConfig::default(),
        )
        .await
        .expect("bus");
        let data_dir = std::env::temp_dir().join(format!("toron-uhkr-data-{tag}"));
        super::super::Mailbox::new(Arc::new(bus), data_dir, Some(crate::keys::generate()))
            .with_index_pool(pool)
    }

    #[tokio::test]
    async fn test_second_caller_never_spawns_bd_uhkr() {
        let pool = uhkr_pool().await;
        let pack = find_pack("wake-priority").unwrap();
        let state = serde_json::json!({"urgency": 0.9});
        assert!(
            claim_judgment(&pool, "toron", "k-dup", "wake-priority", Some(pack.version))
                .await
                .expect("claim"),
            "first claim owns"
        );
        let mailbox = uhkr_mailbox(pool).await;
        let err = mailbox
            .ask_judgment(
                "toron",
                "k-dup",
                "wake-priority",
                &state,
                "system-one",
                Some(5),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("inflight"),
            "no second spawn: {err}"
        );
    }

    #[tokio::test]
    async fn test_crash_leaves_claim_reconcile_clears_bd_uhkr() {
        let pool = uhkr_pool().await;
        let pack = find_pack("wake-priority").unwrap();
        let state = serde_json::json!({"urgency": 0.9});
        let hash = state_hash(&state);
        assert!(
            claim_judgment(
                &pool,
                "toron",
                "k-crash",
                "wake-priority",
                Some(pack.version)
            )
            .await
            .expect("claim")
        );
        assert!(
            journal_get_final_for(&pool, "k-crash", Some(pack.version), &hash)
                .await
                .expect("get")
                .is_none(),
            "crash leaves claim, never a final row"
        );
        assert_eq!(
            reconcile_judgment_claims(&pool, 0)
                .await
                .expect("reconcile"),
            1
        );
        assert!(
            claim_judgment(
                &pool,
                "toron",
                "k-crash",
                "wake-priority",
                Some(pack.version)
            )
            .await
            .expect("reclaim"),
            "key reclaimable after reconcile"
        );
    }

    #[tokio::test]
    async fn test_host_model_shared_and_dedupes_bd_uhkr() {
        use std::sync::Arc;
        let pool = uhkr_pool().await;
        let m1 = uhkr_mailbox(pool).await;
        let m2 = m1.clone();
        assert!(
            Arc::ptr_eq(m1.judge_model(), m2.judge_model()),
            "one host model"
        );
        let fx = {
            let model = m1.judge_model();
            let mut guard = model.lock();
            let (next, fx1) = tick(
                &guard,
                &JudgeMsg::Request {
                    effect: judge_effect("k-host"),
                },
            );
            *guard = next;
            let (_, fx2) = tick(
                &guard,
                &JudgeMsg::Request {
                    effect: judge_effect("k-host"),
                },
            );
            assert!(fx2.is_empty(), "duplicate dedupes on the host model");
            drop(guard);
            fx1
        };
        assert_eq!(fx.len(), 1);
    }

    #[tokio::test]
    async fn test_success_settles_claim_no_leftovers_bd_pgvf() {
        let pool = uhkr_pool().await;
        let mailbox = uhkr_mailbox(pool.clone()).await;
        let state = serde_json::json!({"urgency": 0.9});
        let out = mailbox
            .ask_judgment(
                "toron",
                "k-settle",
                "wake-priority",
                &state,
                "offline",
                Some(5),
            )
            .await
            .expect("ask settles");
        assert!(!out.replayed);
        let (rows,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM judgment_effects")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(rows, 1, "one journal row, never two");
        let (claims,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM judgment_effects WHERE model_id = '__claim__'")
                .fetch_one(&pool)
                .await
                .expect("count claims");
        assert_eq!(claims, 0, "no unsettled claim survives success");
        let replay = mailbox
            .ask_judgment(
                "toron",
                "k-settle",
                "wake-priority",
                &state,
                "offline",
                Some(5),
            )
            .await
            .expect("replay");
        assert!(replay.replayed);
        assert_eq!(replay.record, out.record);
    }
}
