// governed-by: ADR-0036
//! Slice B surface (bd-981p): socket tail behind `--watch-sock`, loud
//! fallback, no deletions (wave 3 deletes, not this bead).

use std::path::PathBuf;

fn src(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|_| panic!("read {name}"))
}

#[test]
fn wake_tail_flag_exists_behind_live() {
    let cli = src("cli.rs");
    assert!(
        cli.contains("watch_sock"),
        "WatchArgs must carry --watch-sock"
    );
    assert!(
        cli.contains("watch-sock"),
        "flag must be spelled --watch-sock"
    );
}

#[test]
fn wake_tail_absent_socket_is_loud_never_silent() {
    let tail = src("wake_sock.rs");
    for needle in [
        "Absent",
        "degrading to in-process replay+live",
        "never silent",
    ] {
        assert!(tail.contains(needle), "tail must be loud about {needle}");
    }
    let plane = src("plane.rs");
    assert!(
        plane.contains("eprintln!(\"{e}\")") || plane.contains("eprintln!(\"wake.sock"),
        "plane must print the socket reason before falling back"
    );
}

#[test]
fn wake_tail_no_deletions_wave3() {
    // Slice B adds the tail; the pump and the in-process live tail stay
    // until wave 3. Deleting them here strands embedded mode.
    let plane = src("plane.rs");
    assert!(
        plane.contains("Re-snapshot sqlite each tick"),
        "the 1s snapshot pump stays (wave 3 deletes)"
    );
    assert!(
        plane.contains("subscribe_trusted"),
        "the in-process live tail stays (wave 3 deletes)"
    );
}

#[test]
fn wake_tail_speaks_spec6_frames() {
    let tail = src("wake_sock.rs");
    for needle in [
        "subscribe",
        "since_cursor",
        "Heartbeat",
        "Shed",
        "resync_from",
        "TailOutcome",
    ] {
        assert!(tail.contains(needle), "tail must speak Spec 6 ({needle})");
    }
}

#[test]
fn wake_tail_mapping_is_shared_not_copied() {
    // Byte-identical mapping BY CONSTRUCTION: one formatter, two
    // transports. A second json! shape in production code is drift
    // (test scripts below #[cfg(test)] build fake server frames, not
    // mapping shapes).
    let tail = src("wake_sock.rs");
    let prod = tail.split("#[cfg(test)]").next().unwrap_or(&tail);
    assert!(
        !prod.contains("json!"),
        "wake_sock production code must reuse format_watch_line, never its own json! shape"
    );
    assert!(
        prod.contains("format_sock_line") && prod.contains("format_watch_line"),
        "the shared formatter must be wired"
    );
}
