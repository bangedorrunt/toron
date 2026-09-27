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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// Run the console on a 45x24 PTY, wait for `until` to appear on screen, then
/// send `q`. Returns everything the terminal drew plus the exit code.
///
/// Waiting for CONTENT rather than a fixed sleep is the point: the console
/// opens its store (and, in a real deployment, its keys) before the first data
/// frame, so a guessed delay either races it or wastes seconds. Both waits are
/// bounded, so a wedged console fails with a message instead of hanging.
///
/// `until` must be a needle that only the SETTLED pane draws. The header is
/// already on screen in the first frame, so a needle it satisfies would let a
/// stuck "loading…" pane pass this witness.
fn console_on_a_45_col_pty(program: &std::path::Path, until: &str) -> (String, i32) {
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

    let drawn = Arc::new(Mutex::new(String::new()));
    let mut stdout = child.stdout.take().expect("pty stdout");
    let reader = {
        let drawn = Arc::clone(&drawn);
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            while let Ok(n) = stdout.read(&mut buf) {
                if n == 0 {
                    break;
                }
                drawn
                    .lock()
                    .expect("drawn buffer")
                    .push_str(&String::from_utf8_lossy(&buf[..n]));
            }
        })
    };

    let shown = |needle: &str| drawn.lock().expect("drawn buffer").contains(needle);
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && !shown(until) {
        std::thread::sleep(Duration::from_millis(50));
    }

    // `q` leaves the alternate screen; holding stdin open until now (rather
    // than closing it) is what keeps the pty session alive to draw at all.
    let mut stdin = child.stdin.take().expect("pty stdin");
    let _ = stdin.write_all(b"q\n");
    let _ = stdin.flush();

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut code = None;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("console exit status") {
            code = Some(status.code().unwrap_or(-1));
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if code.is_none() {
        let _ = child.kill();
        panic!(
            "the console did not exit after `q` on a 45x24 pty (wedged narrow path). It drew:\n{}",
            drawn.lock().expect("drawn buffer")
        );
    }
    let _ = reader.join();
    let text = drawn.lock().expect("drawn buffer").clone();
    (text, code.unwrap_or(-1))
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
    // Wait for the onboarding TEXT, not the header: `--project tuipty` is only
    // ever printed by a command the empty pane named. The header already says
    // `tuipty/PtyAgent`, so waiting on that would pass before the pane settled.
    let (drawn, code) = console_on_a_45_col_pty(program, "--project tuipty");

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
    // The data dir is fresh and `PtyAgent` is not registered in it, so the
    // inbox fetch is REFUSED. The pane must still name a real, pasteable
    // command at this width — `agent bootstrap` is the fix for exactly that
    // refusal, and `mail send` is what a loaded-but-empty inbox names. Either
    // one settles the pane; `loading the index…` forever is the failure this
    // witness exists to catch.
    assert!(
        drawn.contains("toron agent bootstrap --project tuipty")
            || drawn.contains("toron mail send --project tuipty"),
        "the empty pane names the next command on a real pty:\n{drawn}"
    );
}
