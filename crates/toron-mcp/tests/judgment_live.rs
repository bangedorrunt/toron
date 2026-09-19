// governed-by: ADR-0033
//! Live probe for `ask_judgment` (bd-ncpt): stub `flywheel judgment-ask`,
//! real mailbox + journal. Sole reader of `FLYWHEEL_BIN` in this binary, so
//! the env set is race-free (bd-6wfv: no other test here touches it).

use std::os::unix::fs::PermissionsExt;

fn stub_bin(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("flywheel-stub");
    std::fs::write(
        &path,
        r#"#!/bin/sh
EK=""; PK=""
while [ $# -gt 0 ]; do
  case "$1" in
    --effect-key) EK="$2"; shift 2;;
    --pack-id) PK="$2"; shift 2;;
    *) shift;;
  esac
done
printf '{"effect_key":"%s","pack_id":"%s","pack_version":1,"model_id":"stub","confidence":0.5,"usage":"0+0","fallback_flag":true,"escalated":true,"note":"stub","answers":{"model_id":"stub","pack_id":"%s","pack_version":1,"by_question":{},"usage":"0+0"}}' "$EK" "$PK" "$PK"
"#,
    )
    .expect("write stub");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod stub");
    path
}

#[tokio::test]
async fn ask_judgment_live_stub_journals_and_replays() {
    let tmp = std::env::temp_dir().join(format!(
        "toron-judgment-live-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&tmp).expect("tmp");
    // bd-6wfv: explicit data dir (no process-global env for the mailbox).
    unsafe {
        std::env::set_var("TORON_MCP_DATA_DIR", tmp.join("data"));
    }
    let old_bin = std::env::var("FLYWHEEL_BIN").ok();
    unsafe {
        std::env::set_var("FLYWHEEL_BIN", stub_bin(&tmp));
    }

    let mb = toron_mcp::demo_mailbox().await.expect("mailbox");
    let state = serde_json::json!({"goal": "probe"});
    let out = mb
        .ask_judgment(
            "toron",
            "live-key-1",
            "stub-pack",
            &state,
            "offline",
            Some(10),
        )
        .await
        .expect("first ask answers");
    assert!(!out.replayed, "first ask invokes");
    assert_eq!(out.record.effect_key, "live-key-1");
    assert_eq!(out.record.pack_id, "stub-pack");
    assert!(out.record.escalated, "stub escalates");
    assert!(out.record.fallback_flag, "stub falls back");

    let again = mb
        .ask_judgment(
            "toron",
            "live-key-1",
            "stub-pack",
            &state,
            "offline",
            Some(10),
        )
        .await
        .expect("second ask replays");
    assert!(again.replayed, "same key replays without re-invoke");
    assert_eq!(again.record, out.record);

    match old_bin {
        Some(v) => unsafe {
            std::env::set_var("FLYWHEEL_BIN", v);
        },
        None => unsafe {
            std::env::remove_var("FLYWHEEL_BIN");
        },
    }
    let _ = std::fs::remove_dir_all(&tmp);
}
