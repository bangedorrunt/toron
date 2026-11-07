//! Long-lived toron services exit when their own executable changes on disk.
//! The launchd/systemd unit (KeepAlive) restarts the new build, so a reinstall
//! does not leave a stale daemon serving the old protocol.

use std::path::Path;
use std::time::{Duration, SystemTime};

const INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Stamp {
    len: u64,
    modified: Option<SystemTime>,
}

fn stamp_of(path: &Path) -> Option<Stamp> {
    let m = std::fs::metadata(path).ok()?;
    Some(Stamp {
        len: m.len(),
        modified: m.modified().ok(),
    })
}

/// Watch this process's executable and exit 1 after a reinstall.
pub fn spawn(label: &str) {
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let stamp = stamp_of(&exe);
    let label = label.to_string();
    let _ = std::thread::Builder::new()
        .name(format!("toron-watch-{label}"))
        .spawn(move || {
            loop {
                std::thread::sleep(INTERVAL);
                if stamp_of(&exe) != stamp {
                    eprintln!(
                        "toron {label}: binary changed on disk; exiting 1 for the supervisor restart"
                    );
                    std::process::exit(1);
                }
            }
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_changed_stamp_is_detected() {
        let dir = std::env::temp_dir().join(format!("toron-watch-{}", std::process::id()));
        let file = dir.join("bin");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&file, b"a").unwrap();
        let first = stamp_of(&file).expect("stamp");
        std::fs::write(&file, b"a longer body").unwrap();
        let second = stamp_of(&file).expect("stamp");
        assert_ne!(first, second, "a reinstall must change the watch stamp");
        assert_eq!(
            stamp_of(&file),
            Some(second),
            "an unchanged file keeps its stamp"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn serve_installs_the_watch() {
        let main = include_str!("main.rs");
        assert!(
            main.contains("self_watch::spawn(\"serve\")"),
            "toron serve must install the binary watch"
        );
    }
}
