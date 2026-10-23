// governed-by: ADR-0041
//! `toron inbox msg <producer:epoch:seq>` through the real binary
//! (bd-read-by-producer-seq-uozq).
//!
//! Hermetic: the environment is cleared (so `TORON_MCP_URL`, the daemon on
//! :18080, is NOT set and cannot be used) and HOME, XDG and the data dir are a
//! throwaway directory per test. The only writes are to that directory.

use std::path::{Path, PathBuf};
use std::process::Output;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "toron-inbox-msg-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn toron(dir: &Path, args: &[&str]) -> Output {
    let mut cmd = toron_core::testkit::toron_command_clean(env!("CARGO_BIN_EXE_toron"));
    cmd.env("HOME", dir)
        .env("XDG_DATA_HOME", dir.join("xdg-data"))
        .env("XDG_STATE_HOME", dir.join("xdg-state"))
        .env("XDG_CONFIG_HOME", dir.join("xdg-config"))
        .env("TORON_DATA_DIR", dir.join("data"))
        .env("TORON_COMMS_INBOX", "1")
        .current_dir(dir)
        .args(args)
        .arg("--json");
    assert!(
        cmd.get_envs().all(|(k, _)| k != "TORON_MCP_URL"),
        "the daemon URL must not reach the child"
    );
    cmd.output().expect("run toron")
}

fn json(o: &Output) -> serde_json::Value {
    serde_json::from_slice(&o.stdout).unwrap_or_else(|_| {
        panic!(
            "not JSON (exit {:?}): {} | {}",
            o.status.code(),
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        )
    })
}

fn envelope_file(dir: &Path) -> PathBuf {
    use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
    let env = Envelope {
        v: 1,
        msg: MsgId {
            producer: A.into(),
            epoch: 0,
            seq: 0,
        },
        from: A.into(),
        to: Recipient::Npub(B.into()),
        class: Class::Task,
        contract: ContractId::new("task", 1),
        context: ContextId("ctx-msg".into()),
        task: Some(TaskId("t-msg".into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "t", "brief": "read me", "scope_paths": ["src/**"]}),
    };
    let p = dir.join("env.json");
    std::fs::write(&p, serde_json::to_string(&env).unwrap()).unwrap();
    p
}

fn seeded(tag: &str) -> PathBuf {
    let d = scratch(tag);
    let f = envelope_file(&d);
    let o = toron(&d, &["inbox", "append", "--file", f.to_str().unwrap()]);
    assert!(
        o.status.success(),
        "seed append failed: {} | {}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    d
}

fn msg(dir: &Path, key: &str, who: &str) -> Output {
    toron(dir, &["inbox", "msg", key, "--project", "p", "--as", who])
}

#[test]
fn the_sender_and_the_recipient_read_the_stored_envelope() {
    let d = seeded("both");
    let key = format!("{A}:0:0");
    for who in [A, B] {
        let o = msg(&d, &key, who);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        let v = json(&o);
        assert_eq!(v["found"], true, "{who:.4}: {v}");
        assert_eq!(v["message"]["msg_key"], key);
        assert_eq!(v["message"]["fenced"], false);
        assert_eq!(v["message"]["envelope"]["body"]["brief"], "read me");
    }
}

#[test]
fn a_stranger_and_an_unknown_key_get_the_same_answer() {
    let d = seeded("stranger");
    let stranger = json(&msg(&d, &format!("{A}:0:0"), C));
    let unknown = json(&msg(&d, &format!("{A}:7:7"), A));
    assert_eq!(stranger["found"], false);
    assert_eq!(unknown["found"], false);
    assert!(stranger["message"].is_null() && unknown["message"].is_null());
}

#[test]
fn a_malformed_key_is_refused_with_exit_3() {
    let d = seeded("badkey");
    let o = msg(&d, "not-a-key", A);
    assert_eq!(
        o.status.code(),
        Some(3),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
    assert_eq!(json(&o)["refused"], "bad_key");
}

/// bd-reject-badenvelope-panic-bhls: a file that is not an envelope is a
/// typed refusal with exit 3, not a panic (exit 101).
#[test]
fn append_of_a_non_envelope_is_refused_with_exit_3_not_a_panic() {
    let d = scratch("notenv");
    let f = d.join("bad.json");
    std::fs::write(&f, r#"{"not":"an envelope"}"#).unwrap();
    let o = toron(&d, &["inbox", "append", "--file", f.to_str().unwrap()]);
    assert_eq!(
        o.status.code(),
        Some(3),
        "stdout {} stderr {}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    let v = json(&o);
    assert_eq!(v["refused"]["reject"], "bad_envelope");
    assert!(v["refused"]["why"].as_str().unwrap().len() > 3);
    assert!(!String::from_utf8_lossy(&o.stderr).contains("panicked"));
}
