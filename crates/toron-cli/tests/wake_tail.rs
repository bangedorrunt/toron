// governed-by: ADR-0036
//! Wave 4 surface (bd-ud7k; waves 2/3 cut over): the socket tail is the
//! path with no flag, loud fallback, pump deleted.

use std::path::PathBuf;

fn src(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|_| panic!("read {name}"))
}

#[test]
fn wake_tail_flag_removed_socket_is_the_path() {
    // Wave 4 (bd-ud7k): the cutover flag is removed — the socket tail is
    // unconditional, the in-process subscribe stays as the loud fallback.
    // Assert the absence, not a spelling: any reintroduction fails loud.
    let cli = src("cli.rs");
    assert!(
        !cli.contains("watch_sock"),
        "the --watch-sock flag must be removed (wave 4)"
    );
    let plane = src("plane.rs");
    assert!(
        !plane.contains("args.watch_sock"),
        "no flag gate may remain in the watch path (wave 4)"
    );
    assert!(
        plane.contains("wake.sock") || plane.contains("wake_sock"),
        "the socket tail must stay the live path"
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
fn wake_tail_wave3_pump_deleted_socket_live() {
    // Wave 3 (bd-qg6w): the 1s snapshot pump is deleted, the socket tail
    // is the live path. Pump and socket are never both live (slice C
    // tripwire contract).
    let plane = src("plane.rs");
    assert!(
        !plane.contains("Re-snapshot sqlite each tick"),
        "the 1s snapshot pump must be deleted (bead VERIFY)"
    );
    assert!(
        !plane.contains("sleep(Duration::from_secs(1))"),
        "no 1s poll sleep may remain in the watch path"
    );
    assert!(
        plane.contains("wake.sock") || plane.contains("wake_sock"),
        "the socket tail must be the live path"
    );
    assert!(
        plane.contains("subscribe_trusted"),
        "the in-process subscribe stays as the loud embedded fallback (not the pump)"
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
