// governed-by: ADR-0033
//! Missing-binary probe for `ask_judgment` (bd-ncpt): separate test binary
//! from judgment_live so the two never share process env (bd-6wfv).

#[tokio::test]
async fn ask_judgment_missing_binary_is_typed() {
    let tmp = std::env::temp_dir().join(format!(
        "toron-judgment-nobin-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&tmp).expect("tmp");
    unsafe {
        std::env::set_var("TORON_MCP_DATA_DIR", tmp.join("data"));
    }
    let old_bin = std::env::var("FLYWHEEL_BIN").ok();
    unsafe {
        std::env::set_var("FLYWHEEL_BIN", tmp.join("no-such-flywheel"));
    }

    let mb = toron_mcp::demo_mailbox().await.expect("mailbox");
    let e = mb
        .ask_judgment(
            "toron",
            "nobin-key-1",
            "stub-pack",
            &serde_json::json!({}),
            "offline",
            Some(10),
        )
        .await
        .unwrap_err();
    let msg = e.to_string();
    assert!(
        msg.contains("FLYWHEEL_BIN") || msg.contains("install flywheel"),
        "actionable missing-binary error, got: {msg}"
    );

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
