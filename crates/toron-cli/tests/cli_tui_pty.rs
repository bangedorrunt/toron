// governed-by: ADR-0003
// governed-by: ADR-0031
// No unsafe here and no ambient mutation: unlike cli_tui.rs this file passes
// `TORON_MCP_DATA_DIR` to the child process directly, so it never calls
// `std::env::set_var`.

//! The narrow-PTY witness: the REAL binary on a REAL terminal at the geometry
//! a phone SSH client reports.
//!
//! Everything else about phone width is proven at 45 columns with ratatui's
//! `TestBackend`, which is the geometry but not the binary. The process path is
//! what this file covers, and nothing else does: the TTY gate, raw mode, the
//! alternate screen, the blocking input thread, the first real frame and a
//! clean exit — all at 45x24, where a regression would show up as a resize card
//! instead of a console.
//!
//! `script(1)` is the PTY (no new dependency); `stty` inside the child forces
//! the size, because `script` otherwise copies the caller's terminal and a test
//! runner has none.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

/// Per-test-run data dir, matching cli_tui.rs: the durable production root
/// (`~/.local/share/toron`) holds 500+ agent keys and rebuilding it in a test
/// re-runs the argon2 KDF per key.
fn data_dir() -> String {
    let d = std::env::temp_dir().join(format!("toron-cli-tui-pty-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&d);
    d.display().to_string()
}

/// Is `script(1)` on PATH? Its absence is reported, never silently passed.
fn have_script() -> bool {
    Command::new("script")
        .arg("-h")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.code().is_some() || s.success())
        .unwrap_or(false)
}

/// Run the console on a 45x24 PTY, send `q` once it is up, and return
/// everything the terminal drew plus the exit code.
///
/// The read is bounded: a console that does not exit is a failure with a
/// message, not a hung test run.
fn console_on_a_45_col_pty(program: &std::path::Path) -> (String, i32) {
    let scripted = format!(
        "stty cols 45 rows 24; exec '{}' tui --project tuipty --as PtyAgent --refresh-ms 250",
        program.display()
    );
    // macOS script: [file [command ...]]; util-linux: -c <command> [file].
    let mut command = Command::new("script");
    if cfg!(target_os = "macos") {
        command.args(["-q", "/dev/null", "sh", "-c", &scripted]);
    } else {
        command.args(["-q", "-c", &scripted, "/dev/null"]);
    }
    let mut child = command
        .env_remove("TORON_MCP_URL")
        .env("TORON_MCP_DATA_DIR", data_dir())
        .env("TERM", "xterm-256color")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn script(1)");

    // Quit once the alternate screen is up. A newline rides along in case the
    // pty is still in canonical mode when the bytes arrive (it is delivered
    // with the line, then ignored as a key).
    let mut stdin = child.stdin.take().expect("pty stdin");
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        let _ = stdin.write_all(b"q\n");
        let _ = stdin.flush();
    });

    let mut stdout = child.stdout.take().expect("pty stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = stdout.read_to_string(&mut buf);
        let _ = tx.send(buf);
    });

    let drawn = match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(drawn) => drawn,
        Err(_) => {
            let _ = child.kill();
            panic!("the console did not exit within 30s on a 45x24 pty (wedged narrow path)");
        }
    };
    let status = child.wait().expect("console exit status");
    (drawn, status.code().unwrap_or(-1))
}

/// The console works at phone width: one pane, the compact chrome, no resize
/// card, and `q` exits clean.
#[test]
fn the_console_runs_at_45_columns_on_a_real_terminal() {
    if !have_script() {
        eprintln!(
            "SKIPPED narrow-PTY witness: script(1) not found. The 45-column geometry is still \
             covered by TestBackend in crates/toron-cli/src/tui/tests.rs; the process path is not."
        );
        return;
    }
    let program = std::path::Path::new(env!("CARGO_BIN_EXE_toron"));
    let (drawn, code) = console_on_a_45_col_pty(program);

    assert_eq!(
        code, 0,
        "the console must exit 0 on `q` at 45 columns; it drew:\n{drawn}"
    );
    assert!(
        drawn.contains("Inbox"),
        "the list pane drew at 45 columns:\n{drawn}"
    );
    assert!(
        !drawn.contains("too small"),
        "45 columns is a console, not a resize card:\n{drawn}"
    );
    assert!(
        drawn.contains("1M"),
        "the phone-width chrome compacted the tab bar:\n{drawn}"
    );
}
