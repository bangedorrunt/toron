// governed-by: ADR-0041
//! Parity of `InboxStore::unread_head` with the real `toron next`: on one
//! fixture root, the head read in-process is the first item the binary
//! serves, before and after the binary commits the cursor; and
//! `pending_urgent` says urgent exactly when `toron next --peek --urgent` does.
//!
//! Hermetic: the environment is cleared (`TORON_MCP_URL`, the daemon on
//! :18080, is NOT set and cannot be used); HOME, XDG and the data dir are a
//! throwaway directory per test, and the operator key is generated into it.
//! No daemon and no port: the comms verbs run in-process on the data dir.

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::process::Output;

use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{HeadRow, InboxStore};
use toron_core::wake::Urgency;

const SYS: &str = "5959595959595959595959595959595959595959595959595959595959595901";
const CREW_PIN: &str = "AmberFox";
const CTX: &str = "proj-head-cli";

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "toron-head-cli-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn toron(dir: &Path, key: Option<&Path>, args: &[&str]) -> Output {
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
    if let Some(k) = key {
        cmd.env("TORON_OPERATOR_KEY", k);
    }
    assert!(
        cmd.get_envs().all(|(k, _)| k != "TORON_MCP_URL"),
        "the daemon URL must not reach the child"
    );
    cmd.output().expect("run toron")
}

fn ok_json(o: &Output) -> serde_json::Value {
    assert!(
        o.status.success(),
        "exit {:?}: {} | {}",
        o.status.code(),
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    serde_json::from_slice(&o.stdout).expect("JSON on stdout")
}

fn env(
    from: &str,
    epoch: u64,
    seq: u64,
    class: Class,
    name: &str,
    to: &str,
    body: serde_json::Value,
) -> Envelope {
    Envelope {
        v: 1,
        msg: MsgId {
            producer: from.into(),
            epoch,
            seq,
        },
        from: from.into(),
        to: Recipient::Npub(if name == "lead" { SYS } else { to }.into()),
        class,
        contract: ContractId::new(name, 1),
        context: ContextId(CTX.into()),
        task: (name == "steer").then(|| TaskId("t-1".into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body,
    }
}

fn append(dir: &Path, key: &Path, n: usize, e: &Envelope) {
    let f = dir.join(format!("e{n}.json"));
    std::fs::write(&f, serde_json::to_string(e).unwrap()).unwrap();
    ok_json(&toron(
        dir,
        Some(key),
        &["inbox", "append", "--file", f.to_str().unwrap()],
    ));
}

/// The head of a `toron next` batch, in `HeadRow` terms.
fn served(batch: &serde_json::Value) -> (u64, Option<HeadRow>) {
    let before = batch["cursor_before"].as_u64().unwrap();
    let head = batch["items"].as_array().unwrap().first().map(|it| {
        let e: Envelope = serde_json::from_value(it["envelope"].clone()).unwrap();
        HeadRow {
            offset: NonZeroU64::new(it["offset"].as_u64().unwrap()).unwrap(),
            msg_key: it["msg_key"].as_str().unwrap().into(),
            urgency: toron_core::inbox::urgency_of(&e),
            contract: e.contract,
            class: e.class,
        }
    });
    (before, head)
}

async fn head_in_process(dir: &Path, reader: &str, after: u64) -> Option<HeadRow> {
    let db = dir.join("data").join(toron_core::config::INDEX_SQLITE);
    let pool = toron_core::index::schema::connect(db.to_str().unwrap())
        .await
        .expect("open the fixture root");
    InboxStore::new(pool)
        .unread_head(reader, after)
        .await
        .unwrap()
}

async fn urgent_in_process(dir: &Path, reader: &str, after: u64) -> Option<HeadRow> {
    let db = dir.join("data").join(toron_core::config::INDEX_SQLITE);
    let pool = toron_core::index::schema::connect(db.to_str().unwrap())
        .await
        .expect("open the fixture root");
    InboxStore::new(pool)
        .pending_urgent(reader, after)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_head_is_what_toron_next_serves() {
    let dir = scratch("parity");
    let key = dir.join("operator.ncryptsec");
    ok_json(&toron(
        &dir,
        None,
        &[
            "key",
            "generate",
            "--role",
            "operator",
            "--out",
            key.to_str().unwrap(),
        ],
    ));
    let op = toron_core::keys::load_encrypted_key(&key, "toron")
        .unwrap()
        .public_key()
        .to_hex();
    let boot = ok_json(&toron(
        &dir,
        Some(&key),
        &["agent", "bootstrap", "--project", CTX, "--as", CREW_PIN],
    ));
    let crew = boot["npub"].as_str().expect("crew npub").to_string();
    let next = |peek: bool| {
        let mut a = vec!["next", "--project", CTX, "--as", CREW_PIN];
        if peek {
            a.push("--peek");
        }
        ok_json(&toron(&dir, Some(&key), &a))
    };

    let urgent_cli = || {
        let o = toron(
            &dir,
            Some(&key),
            &[
                "next",
                "--project",
                CTX,
                "--as",
                CREW_PIN,
                "--peek",
                "--urgent",
            ],
        );
        match o.status.code() {
            Some(0) => {
                assert_eq!(String::from_utf8_lossy(&o.stdout).trim(), "urgent");
                true
            }
            Some(1) => false,
            other => panic!(
                "exit {other:?}: {} | {}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        }
    };

    let (c, s) = served(&next(true));
    assert_eq!(s, None);
    assert_eq!(head_in_process(&dir, &crew, c).await, None);
    assert!(!urgent_cli());
    assert_eq!(urgent_in_process(&dir, &crew, c).await, None);

    let lead = env(
        &op,
        0,
        0,
        Class::Control,
        "lead",
        &crew,
        serde_json::json!({"holder": SYS, "sup_epoch": 1}),
    );
    append(&dir, &key, 0, &lead);
    let notice = env(
        &op,
        1,
        0,
        Class::Notify,
        "notify",
        &crew,
        serde_json::json!({"text": "urgent, says the text"}),
    );
    append(&dir, &key, 1, &notice);
    assert!(!urgent_cli());
    assert_eq!(urgent_in_process(&dir, &crew, 0).await, None);
    let steer = env(
        SYS,
        0,
        0,
        Class::Control,
        "steer",
        &crew,
        serde_json::json!({"op": "pause", "priority": "urgent", "sup_epoch": 1}),
    );
    append(&dir, &key, 2, &steer);

    let batch = next(true);
    let (c, s) = served(&batch);
    let h = head_in_process(&dir, &crew, c).await;
    assert_eq!(h, s);
    let h = h.unwrap();
    assert_eq!(h.contract, ContractId::new("notify", 1));
    assert_eq!(h.urgency, Urgency::Normal);
    assert!(urgent_cli());
    let u = urgent_in_process(&dir, &crew, c)
        .await
        .expect("pending urgent");
    assert_eq!(
        u.offset.get(),
        batch["items"][1]["offset"].as_u64().unwrap(),
        "the first urgent item of the batch"
    );
    assert_eq!(u.contract, ContractId::new("steer", 1));

    let read = next(false);
    let after = read["cursor_after"].as_u64().unwrap();
    assert_eq!(read["items"].as_array().unwrap().len(), 2);
    let steer_msg = read["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|it| it["envelope"]["contract"]["name"] == "steer")
        .and_then(|it| it["msg_key"].as_str())
        .expect("steer in the batch")
        .to_string();
    ok_json(&toron(
        &dir,
        Some(&key),
        &[
            "ack",
            &steer_msg,
            "accept",
            "--project",
            CTX,
            "--as",
            CREW_PIN,
        ],
    ));
    let (c, s) = served(&next(true));
    assert_eq!(c, after);
    assert_eq!(s, None);
    assert_eq!(head_in_process(&dir, &crew, after).await, None);
    assert!(!urgent_cli(), "read: not pending");
    assert_eq!(
        urgent_in_process(&dir, &crew, after).await,
        None,
        "after={after} read={read} batch={batch}"
    );

    let steer2 = env(
        SYS,
        0,
        1,
        Class::Control,
        "steer",
        &crew,
        serde_json::json!({"op": "resume", "priority": "urgent", "sup_epoch": 1}),
    );
    append(&dir, &key, 3, &steer2);
    let (c, s) = served(&next(true));
    let h = head_in_process(&dir, &crew, c).await;
    assert_eq!(h, s);
    assert_eq!(h.clone().unwrap().urgency, Urgency::Urgent);
    assert!(urgent_cli());
    assert_eq!(urgent_in_process(&dir, &crew, c).await, h);
}
