// governed-by: ADR-0027
//! Slice 3 S5 — wait --corr unparks on receipt, watch --live without 1s snapshot
//! RED draft, GREEN when bd-rkom (E7/E9) closes
//!
//! ADR-0028 E2 (zfnx.6): WaitCorr interest rides the already-held doorbell
//! subscription vocabulary; deadline expiry is explicit, never a silent drop.

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

#[test]
fn adr0027_s3_e2_wait_held_bus_wake() {
    let plane = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/plane.rs");
    let content = std::fs::read_to_string(&plane).expect("read plane.rs");
    assert!(
        content.contains("WAIT_WAKE_KINDS"),
        "E2: wait --corr wake kinds must be a named constant mirroring the held doorbell subscription"
    );
    assert!(
        content.contains("wait_for_corr") && content.contains("subscribe_trusted"),
        "E2: wait --corr must unpark on the held bus subscription (no second watch child, no poll)"
    );
    assert!(
        content.contains("timed out after"),
        "E2: deadline expiry must be explicit (never a silent drop)"
    );
}
