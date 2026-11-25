// governed-by: ADR-0003
// governed-by: ADR-0031
//! Real-PTY smoke witness for `toron tui` (`dashboard` / `ui`) at 45 columns.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn have_script() -> bool {
    Command::new("script")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
        || std::path::Path::new("/usr/bin/script").exists()
}

fn shared_data_dir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("toron-cli-tui-pty-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&d);
    d
}

fn run_on_45_col_pty(program: &std::path::Path, keys: &str) -> (String, i32) {
    let mut cmd = toron_core::testkit::toron_command(program);
    cmd.env_remove("TORON_MCP_URL");
    cmd.env("TORON_MCP_DATA_DIR", shared_data_dir());
    cmd.env("TORON_TUI_NO_FLYWHEEL", "1");
    cmd.env("TORON_TUI_NO_HTTP_PROBE", "1");
    let shell_cmd = format!(
        "stty cols 45 rows 24; exec '{}' dashboard --project tuipty --as PtyAgent --refresh-ms 250",
        program.display()
    );
    let mut child = Command::new("script")
        .args(["-q", "/dev/null", "sh", "-c", &shell_cmd])
        .env_remove("TORON_MCP_URL")
        .envs(cmd.get_envs().filter_map(|(k, v)| v.map(|val| (k, val))))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn script(1)");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut stdout = child.stdout.take().expect("stdout");

    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let reader = std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        let mut all = Vec::new();
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 {
                break;
            }
            let chunk = buf[..n].to_vec();
            all.extend_from_slice(&chunk);
            let _ = tx.send(chunk);
        }
        String::from_utf8_lossy(&all).to_string()
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = String::new();
    while Instant::now() < deadline {
        if let Ok(chunk) = rx.recv_timeout(Duration::from_millis(100)) {
            seen.push_str(&String::from_utf8_lossy(&chunk));
            if seen.contains("Roster") && seen.contains("Posts") {
                break;
            }
        }
    }
    let _ = stdin.write_all(keys.as_bytes());
    let _ = stdin.flush();

    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => break s,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("PTY timed out; drawn:\n{seen}");
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    let drawn = reader.join().unwrap_or(seen);
    (drawn, status.code().unwrap_or(-1))
}

#[test]
fn swarm_board_runs_and_exits_cleanly_on_45_col_pty() {
    if !have_script() {
        return;
    }
    let program = std::path::Path::new(env!("CARGO_BIN_EXE_toron"));
    let (drawn, code) = run_on_45_col_pty(program, "q");
    assert_eq!(code, 0, "must exit 0 on q; drew:\n{drawn}");
    assert!(drawn.contains("Swarm"), "missing header:\n{drawn}");
    assert!(drawn.contains("Roster"), "missing roster:\n{drawn}");
    assert!(drawn.contains("Posts"), "missing posts:\n{drawn}");
    assert!(!drawn.contains("too small"), "45 cols must fit:\n{drawn}");
}
