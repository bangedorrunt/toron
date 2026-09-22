// governed-by: ADR-0036
//! Wave 3 surface (bd-qg6w; slices B+C before it): socket tail behind
//! `--watch-sock`, loud fallback, pump deleted, socket is the live path.

use std::path::PathBuf;

fn src(name: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(name);
    std::fs::read_to_string(&p).unwrap_or_else(|_| panic!("read {name}"))
}

#[test]
fn wake_tail_flag_exists_behind_live() {
    // clap derives `--watch-sock` from the field name + `#[arg(long)]`;
    // assert the source pair, not the derived spelling.
    let cli = src("cli.rs");
    assert!(
        cli.contains("pub watch_sock: bool"),
        "WatchArgs must carry the watch_sock flag"
    );
    let field = cli.find("pub watch_sock: bool").unwrap();
    assert!(
        cli[..field].rfind("#[arg(long)]").is_some(),
        "watch_sock must be a long flag (clap derives --watch-sock)"
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
