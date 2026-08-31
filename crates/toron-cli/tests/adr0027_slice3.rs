// governed-by: ADR-0027
//! Slice 3 S5 — wait --corr unparks on receipt, watch --live without 1s snapshot
//! RED draft, GREEN when bd-rkom (E7/E9) closes

use std::path::PathBuf;

#[test]
fn adr0027_s3_s5_wait_corr_unparks_on_receipt() {
    let cli = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/cli.rs");
    let content = std::fs::read_to_string(&cli).expect("read cli.rs");
    assert!(
        content.contains("WaitArgs")
            || content.contains("WaitCorr")
            || content.contains("wait --corr"),
        "S5: toron wait --corr verb must exist"
    );
    assert!(
        content.contains("live") && content.contains("WatchArgs"),
        "S5: toron watch --live must exist"
    );
}

#[test]
fn adr0027_s3_s5_watch_live_no_snapshot() {
    let cli = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/cli.rs");
    let content = std::fs::read_to_string(&cli).expect("read cli.rs");
    assert!(
        content.contains("live") && content.contains("WatchArgs"),
        "watch --live must exist"
    );
    assert!(
        !content.contains("1s snapshot") || content.contains("live"),
        "watch --live must not be 1s snapshot"
    );
}
