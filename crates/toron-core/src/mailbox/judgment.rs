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
    // JSON cannot spell non-finite floats (serde_json rejects them at parse),
    // so no fin-mirror is needed at this boundary — a present number is finite.
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
    })
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
    // Char-boundary safe: byte slicing can split a multibyte char (bd-7qcw).
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

impl super::Mailbox {
    /// `ask_judgment`: idempotent replay → child `flywheel judgment-ask` under
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
        if let Some(record) = journal_get(pool, effect_key).await? {
            return Ok(JudgmentOutcome {
                record,
                replayed: true,
            });
        }
        let bin = flywheel_bin();
        let argv = judgment_argv(effect_key, pack_id, &state_json, transport);
        let mut cmd = tokio::process::Command::new(&bin);
        // Timeout drop kills the child (no orphaned flywheel ask).
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
        let record = parse_judgment_stdout(effect_key, pack_id, &stdout)?;
        journal_insert(pool, project_slug, &record).await?;
        let record = journal_get(pool, effect_key).await?.unwrap_or(record);
        Ok(JudgmentOutcome {
            record,
            replayed: false,
        })
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
        // 400 × U+00E9 (800 bytes): a byte slice at len-300 splits a char.
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
        // JSON cannot spell non-finite floats, so any present number is finite.
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
}
