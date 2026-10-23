// governed-by: ADR-0041
//! Roles and authority at append, through the real binary (Phase 4.1): an
//! out-of-role envelope is a typed `unauthorized` refusal with exit 3 and the
//! expected set on stdout, never a panic or a bare error.
//!
//! Hermetic: the environment is cleared (`TORON_MCP_URL`, the daemon on
//! :18080, is NOT set and cannot be used); HOME, XDG and the data dir are a
//! throwaway directory per test, and the operator key is generated into it.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use toron_core::authority::Role;
use toron_core::contracts::role_v1::RoleV1;
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};

const CAP: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc01";
const CREW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
const CREW2: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02";
const PEER: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee01";
const CTX: &str = "ctx-cli-auth";

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "toron-comms-auth-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// `toron` with a cleared environment. `flag` = the value of
/// `TORON_COMMS_INBOX` (None = unset).
fn toron_with(dir: &Path, op_key: Option<&Path>, flag: Option<&str>, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_toron"));
    cmd.env_clear()
        .env("HOME", dir)
        .env("XDG_DATA_HOME", dir.join("xdg-data"))
        .env("XDG_STATE_HOME", dir.join("xdg-state"))
        .env("XDG_CONFIG_HOME", dir.join("xdg-config"))
        .env("TORON_DATA_DIR", dir.join("data"))
        .current_dir(dir)
        .args(args)
        .arg("--json");
    if let Some(f) = flag {
        cmd.env("TORON_COMMS_INBOX", f);
    }
    if let Some(k) = op_key {
        cmd.env("TORON_OPERATOR_KEY", k);
    }
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

/// A scratch root with its own operator key. Returns (dir, key path, the
/// operator's npub as 64 hex).
fn rig(tag: &str) -> (PathBuf, PathBuf, String) {
    let dir = scratch(tag);
    let key = dir.join("operator.ncryptsec");
    let o = toron_with(
        &dir,
        None,
        None,
        &[
            "key",
            "generate",
            "--role",
            "operator",
            "--out",
            key.to_str().unwrap(),
        ],
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let npub = toron_core::keys::load_encrypted_key(&key, "toron")
        .expect("load the operator key")
        .public_key()
        .to_hex();
    (dir, key, npub)
}

fn write_env(dir: &Path, name: &str, env: &Envelope) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, serde_json::to_string(env).unwrap()).unwrap();
    p
}

fn msg(producer: &str, seq: u64) -> MsgId {
    MsgId {
        producer: producer.into(),
        epoch: 0,
        seq,
    }
}

fn role_fact(from: &str, seq: u64, subject: &str, role: Role) -> Envelope {
    RoleV1 {
        npub: subject.into(),
        role,
        generation: 0,
    }
    .into_envelope(msg(from, seq), ContextId(CTX.into()))
}

fn shaped(
    from: &str,
    seq: u64,
    to: &str,
    class: Class,
    contract: &str,
    task: Option<&str>,
    body: serde_json::Value,
) -> Envelope {
    Envelope {
        v: 1,
        msg: msg(from, seq),
        from: from.into(),
        to: Recipient::Npub(to.into()),
        class,
        contract: ContractId::new(contract, 1),
        context: ContextId(CTX.into()),
        task: task.map(|t| TaskId(t.into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body,
    }
}

fn append(dir: &Path, key: &Path, flag: Option<&str>, name: &str, env: &Envelope) -> Output {
    let f = write_env(dir, name, env);
    toron_with(
        dir,
        Some(key),
        flag,
        &["inbox", "append", "--file", f.to_str().unwrap()],
    )
}

/// Operator makes CAP captain; CAP makes CREW a crew and PEER a peer.
fn governed(tag: &str) -> (PathBuf, PathBuf) {
    let (dir, key, op) = rig(tag);
    for (name, env) in [
        ("r0.json", role_fact(&op, 0, CAP, Role::Captain)),
        ("r1.json", role_fact(CAP, 0, CREW, Role::Crew)),
        ("r2.json", role_fact(CAP, 1, PEER, Role::Peer)),
    ] {
        let o = append(&dir, &key, Some("1"), name, &env);
        assert!(
            o.status.success(),
            "seed {name}: {} | {}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
    }
    (dir, key)
}

fn refused(o: &Output) -> serde_json::Value {
    assert_eq!(
        o.status.code(),
        Some(3),
        "stdout {} stderr {}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(!String::from_utf8_lossy(&o.stderr).contains("panicked"));
    let v = json(o);
    assert_eq!(v["refused"]["reject"], "unauthorized", "{v}");
    v
}

#[test]
fn a_crew_task_is_refused_with_the_expected_set_and_exit_3() {
    let (dir, key) = governed("crewtask");
    let t = shaped(
        CREW,
        0,
        CREW2,
        Class::Task,
        "task",
        Some("t-1"),
        serde_json::json!({"title": "t", "brief": "b"}),
    );
    let v = refused(&append(&dir, &key, Some("1"), "t.json", &t));
    assert_eq!(v["refused"]["role"], "crew");
    assert_eq!(v["refused"]["class"], "task");
    assert_eq!(v["refused"]["contract"], "task@1");
    // A refusal carries the role's WHOLE allow-list, not the slice for the
    // attempted class (bd-5cyi). Crew holds one control verb,
    // `escalate@1` - the upward handoff, added in Phase 4a - so the set
    // names it. The in-crate table test moved with the grant (bd-ocpn);
    // this CLI-level expectation has to move with it.
    assert_eq!(
        v["refused"]["expected"],
        serde_json::json!([
            "notify/*",
            "request/*",
            "reply/*",
            "control/escalate",
            "done"
        ])
    );
    assert!(
        v["error"]
            .as_str()
            .unwrap()
            .contains("role crew may not send task@1")
    );
}

#[test]
fn a_crew_verdict_and_a_peer_task_are_refused() {
    let (dir, key) = governed("verdict");
    let verdict = shaped(
        CREW,
        0,
        CAP,
        Class::Control,
        "verdict",
        Some("t-1"),
        serde_json::json!({"pass": true}),
    );
    let v = refused(&append(&dir, &key, Some("1"), "v.json", &verdict));
    assert_eq!(v["refused"]["role"], "crew");
    assert_eq!(v["refused"]["contract"], "verdict@1");
    assert_eq!(v["refused"]["class"], "control");

    let t = shaped(
        PEER,
        0,
        CREW,
        Class::Task,
        "task",
        Some("t-2"),
        serde_json::json!({"title": "t", "brief": "b"}),
    );
    let v = refused(&append(&dir, &key, Some("1"), "p.json", &t));
    assert_eq!(v["refused"]["role"], "peer");
    assert_eq!(
        v["refused"]["expected"],
        serde_json::json!(["notify/*", "request/*", "reply/*"])
    );
}

#[test]
fn a_role_fact_from_a_non_operator_without_rank_is_refused_with_who_could_grant() {
    let (dir, key) = governed("rolerefused");
    let f = role_fact(CREW, 0, CREW, Role::Captain);
    let v = refused(&append(&dir, &key, Some("1"), "self.json", &f));
    assert_eq!(v["refused"]["role"], "crew");
    assert_eq!(v["refused"]["contract"], "role@1");
}

#[test]
fn an_unroled_sender_in_a_governed_context_is_refused_with_no_role() {
    let (dir, key) = governed("unroled");
    let n = shaped(
        CREW2,
        0,
        CAP,
        Class::Notify,
        "notify",
        None,
        serde_json::json!({"text": "hi"}),
    );
    let v = refused(&append(&dir, &key, Some("1"), "n.json", &n));
    assert!(v["refused"]["role"].is_null(), "{v}");
    assert_eq!(v["refused"]["expected"], serde_json::json!([]));
}

#[test]
fn what_the_role_allows_still_goes_through() {
    let (dir, key) = governed("allowed");
    let n = shaped(
        CREW,
        0,
        CAP,
        Class::Notify,
        "notify",
        None,
        serde_json::json!({"text": "done soon"}),
    );
    let o = append(&dir, &key, Some("1"), "ok.json", &n);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
    assert_eq!(json(&o)["replay"], false);
}

/// Opt-out values still refuse before any evaluation. Unset and empty are
/// on (the default), so they are not in this list.
#[test]
fn with_the_flag_off_the_same_sequence_is_unchanged() {
    let (dir, key, op) = rig("flagoff");
    let boot = role_fact(&op, 0, CAP, Role::Captain);
    let t = shaped(
        CREW,
        0,
        CREW2,
        Class::Task,
        "task",
        Some("t-1"),
        serde_json::json!({"title": "t", "brief": "b"}),
    );
    for flag in [Some("0"), Some("off"), Some("false"), Some("nope")] {
        for (name, env) in [("a.json", &boot), ("b.json", &t)] {
            let o = append(&dir, &key, flag, name, env);
            assert_eq!(o.status.code(), Some(2), "flag {flag:?}");
            assert!(
                String::from_utf8_lossy(&o.stderr).contains("comms inbox is off"),
                "{}",
                String::from_utf8_lossy(&o.stderr)
            );
            assert!(
                o.stdout.is_empty(),
                "nothing is rendered while the flag is off"
            );
        }
    }
}

/// CONTROL: nothing set, and an empty env value, open the gate. A role fact lands.
#[test]
fn with_nothing_set_the_gate_is_open() {
    for (tag, flag) in [("unset", None), ("empty", Some(""))] {
        let (dir, key, op) = rig(tag);
        let boot = role_fact(&op, 0, CAP, Role::Captain);
        let o = append(&dir, &key, flag, "a.json", &boot);
        assert!(
            o.status.success(),
            "flag {flag:?}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }
}

/// Exit 3 with the typed refusal `tag`, never a panic.
fn refused_as(o: &Output, tag: &str) -> serde_json::Value {
    assert_eq!(
        o.status.code(),
        Some(3),
        "stdout {} stderr {}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    assert!(!String::from_utf8_lossy(&o.stderr).contains("panicked"));
    let v = json(o);
    assert_eq!(v["refused"]["reject"], tag, "{v}");
    v
}

fn steer_at(from: &str, seq: u64, sup_epoch: u64) -> Envelope {
    shaped(
        from,
        seq,
        CREW,
        Class::Control,
        "steer",
        Some("t-1"),
        serde_json::json!({"op": "pause", "priority": "urgent", "sup_epoch": sup_epoch}),
    )
}

/// F7 through the real binary: `no_lead` and `not_lead_holder` are typed
/// exit-3 refusals; the operator with no lead, and the holder at the current
/// epoch, still land.
#[test]
fn a_steer_is_fenced_to_the_lead_holder_with_exit_3() {
    let (dir, key, op) = rig("steerfence");
    let seed = [
        ("r0.json", role_fact(&op, 0, CAP, Role::Captain)),
        ("r1.json", role_fact(CAP, 0, CREW, Role::Crew)),
    ];
    for (name, env) in seed {
        let o = append(&dir, &key, Some("1"), name, &env);
        assert!(
            o.status.success(),
            "seed {name}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
    }

    // No lead@1 row: the captain may steer by role, and is still refused;
    // only the operator key steers with no lead.
    let v = refused_as(
        &append(&dir, &key, Some("1"), "s0.json", &steer_at(CAP, 1, 1)),
        "no_lead",
    );
    assert_eq!(v["refused"]["context"], CTX);
    assert!(
        v["error"].as_str().unwrap().contains("no lead in context"),
        "{v}"
    );

    let o = append(&dir, &key, Some("1"), "s0op.json", &steer_at(&op, 1, 0));
    assert!(
        o.status.success(),
        "operator steer with no lead: {}",
        String::from_utf8_lossy(&o.stdout)
    );
    assert_eq!(json(&o)["replay"], false);

    // The operator opens the lease for the captain at epoch 1.
    let lead = shaped(
        &op,
        2,
        CAP,
        Class::Control,
        "lead",
        None,
        serde_json::json!({"holder": CAP, "sup_epoch": 1}),
    );
    let o = append(&dir, &key, Some("1"), "lead.json", &lead);
    assert!(
        o.status.success(),
        "lead: {}",
        String::from_utf8_lossy(&o.stderr)
    );

    // The operator at the current epoch is not the holder.
    let v = refused_as(
        &append(&dir, &key, Some("1"), "s1.json", &steer_at(&op, 3, 1)),
        "not_lead_holder",
    );
    assert_eq!(v["refused"]["actor"], op.as_str());
    assert_eq!(v["refused"]["holder"], CAP);
    assert_eq!(v["refused"]["sup_epoch"], 1);

    // Stale epoch from the holder.
    let v = refused_as(
        &append(&dir, &key, Some("1"), "s2.json", &steer_at(CAP, 1, 0)),
        "stale_sup_epoch",
    );
    assert_eq!(v["refused"]["current"], 1);

    // The holder at the current epoch lands.
    let o = append(&dir, &key, Some("1"), "s3.json", &steer_at(CAP, 1, 1));
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
    assert_eq!(json(&o)["replay"], false);
}
