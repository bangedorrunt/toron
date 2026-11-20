// governed-by: ADR-0039

//! bd-6bgrd U2: CLI operator-signed token minting (no key material in output).
//! Each test gets a temp HOME/XDG + data dir and never touches a live daemon.

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn sandbox(tag: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("toron-events-{}-{}-{tag}", std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn toron(dir: &PathBuf) -> Command {
    let mut cmd = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    cmd.env_remove("TORON_MCP_URL")
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_STATE_HOME", dir.join("state"))
        .env("TORON_DATA_DIR", dir.join("toron-data"))
        .env("HOME", dir);
    cmd
}

fn run(dir: &PathBuf, args: &[&str]) -> Output {
    toron(dir).args(args).output().expect("run toron events")
}

fn parse(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).expect("stdout must be JSON")
}

fn write_operator_key(dir: &PathBuf) {
    let cfg_dir = dir.join("config/toron");
    std::fs::create_dir_all(&cfg_dir).unwrap();
    let keys = toron_core::keys::generate();
    let path = cfg_dir.join("operator_nsec");
    toron_core::keys::write_encrypted_key(&path, &keys, "toron", toron_core::keys::TEST_LOG_N)
        .unwrap();
}

#[test]
fn mint_without_an_operator_key_fails_loud() {
    let dir = sandbox("no-key");
    let out = run(
        &dir,
        &["events", "token", "mint", "--pin", "AmberProbe", "--project", "flywheel"],
    );
    assert!(!out.status.success(), "mint without the operator key must fail");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("operator key missing"), "{err}");
    assert!(out.stdout.is_empty(), "no token printed on failure");
}

#[test]
fn a_pin_token_is_printed_once_and_only_its_hash_is_listed() {
    let dir = sandbox("mint");
    write_operator_key(&dir);
    let out = run(
        &dir,
        &["events", "token", "mint", "--pin", "AmberProbe", "--project", "flywheel"],
    );
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let v = parse(&out);
    let token = v["token"].as_str().expect("plaintext token once");
    assert_eq!(token.len(), 64, "32 bytes hex");
    assert_eq!(v["principal"]["kind"], "pin");
    assert_eq!(v["principal"]["pin"], "AmberProbe");

    let listed = parse(&run(&dir, &["events", "token", "list"]));
    let rows = listed["tokens"].as_array().expect("token list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["hash"], toron_core::events::principal::hash_token(token));
    let dump = format!("{:?}", listed);
    assert!(!dump.contains(token), "the plaintext token must never be listed");
}

#[test]
fn revoke_makes_the_token_unusable() {
    let dir = sandbox("revoke");
    write_operator_key(&dir);
    let minted = parse(&run(
        &dir,
        &["events", "token", "mint", "--operator"],
    ));
    assert_eq!(minted["principal"]["kind"], "operator");
    let hash = minted["token"].as_str().unwrap();
    let token_hash = toron_core::events::principal::hash_token(hash);
    let out = run(&dir, &["events", "token", "revoke", "--hash", &token_hash]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let listed = parse(&run(&dir, &["events", "token", "list"]));
    assert_eq!(listed["tokens"][0]["revoked"], true);
}

#[test]
fn no_key_material_ever_appears_in_output() {
    let dir = sandbox("no-secret");
    write_operator_key(&dir);
    for args in [
        vec!["events", "token", "mint", "--pin", "P", "--project", "f"],
        vec!["events", "token", "list"],
    ] {
        let out = run(&dir, &args);
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        assert!(!text.to_lowercase().contains("nsec"), "secret leaked: {text}");
        assert!(!text.to_lowercase().contains("ncryptsec"), "secret leaked: {text}");
    }
}

#[test]
fn a_bad_ttl_is_refused() {
    let dir = sandbox("ttl");
    write_operator_key(&dir);
    let out = run(
        &dir,
        &["events", "token", "mint", "--pin", "P", "--project", "f", "--ttl-secs", "999999"],
    );
    assert!(!out.status.success(), "a >24h pin TTL must be refused");
}
