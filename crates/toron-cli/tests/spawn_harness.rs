// governed-by: ADR-0007
//! Every test that runs a toron-linking binary does it through the harness
//! (`toron_core::testkit::toron_command` / `toron_command_clean` /
//! `apply_test_env`), so every such child carries the under-test marker and
//! an explicit data dir and config home. A spawn site that bypasses it fails
//! this test, naming the file and line.
//!
//! The rules, over every `.rs` file under `crates/*/tests/` and `crates/*/src/`:
//! 1. No `Command::new(...)` (std or tokio) may take the binary directly:
//!    `CARGO_BIN_EXE_*`, `toron_bin()`, or a literal `"toron"` / `"flywheel"` /
//!    `"doorbell"`. Those go through the harness, which builds the command.
//! 2. Any other mention of `CARGO_BIN_EXE_` (a path used for PATH, or handed
//!    to `script(1)`) must say why it is safe: a `spawn-harness:` comment on
//!    the same line or within the three lines above it.
//!
//! Production code that spawns the operator's tools (launchctl, git, herdr)
//! is not a test spawn and is not matched. The checker is tested on fixtures
//! below, so a checker that matches nothing cannot pass for a clean tree.

use std::path::{Path, PathBuf};

const BINARIES: &[&str] = &["\"toron\"", "\"flywheel\"", "\"doorbell\""];

/// `(line number, line)` for every violation in one file's source.
fn violations(source: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let code = line.split("//").next().unwrap_or("");
        let mut bypass = false;
        let mut rest = code;
        while let Some(at) = rest.find("Command::new(") {
            let arg = &rest[at + "Command::new(".len()..];
            let arg = arg.split(')').next().unwrap_or("");
            // `apply_test_env(&mut Command::new(..))` is the harness itself.
            let harnessed = rest[..at]
                .trim_end_matches("std::process::")
                .trim_end_matches("tokio::process::")
                .ends_with("apply_test_env(&mut ");
            if !harnessed
                && (arg.contains("CARGO_BIN_EXE_")
                    || arg.contains("toron_bin(")
                    || BINARIES.iter().any(|b| arg.trim() == *b))
            {
                bypass = true;
            }
            rest = &rest[at + 1..];
        }
        if bypass {
            out.push((i + 1, line.trim().to_string()));
            continue;
        }
        if code.contains("CARGO_BIN_EXE_") && !code.contains("toron_command") {
            let justified = lines[i.saturating_sub(3)..=i]
                .iter()
                .any(|l| l.contains("spawn-harness:"));
            if !justified {
                out.push((i + 1, line.trim().to_string()));
            }
        }
    }
    out
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn every_spawn_of_a_toron_binary_goes_through_the_harness() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for krate in std::fs::read_dir(&crates).expect("crates dir").flatten() {
        for sub in ["tests", "src"] {
            rust_files(&krate.path().join(sub), &mut files);
        }
    }
    assert!(
        files.len() > 100,
        "the scan must see the workspace, saw {} files",
        files.len()
    );
    let this_file = Path::new(file!()).file_name().unwrap();
    let mut found = Vec::new();
    for file in files {
        if file.file_name() == Some(this_file) {
            continue;
        }
        let source = std::fs::read_to_string(&file).unwrap_or_default();
        for (line, text) in violations(&source) {
            found.push(format!("{}:{line}: {text}", file.display()));
        }
    }
    assert!(
        found.is_empty(),
        "spawn sites that bypass toron_core::testkit::toron_command (use it, or \
         justify a non-spawn mention with a `spawn-harness:` comment):\n{}",
        found.join("\n")
    );
}

#[test]
fn the_checker_catches_each_kind_of_bypass() {
    for bypass in [
        "let mut c = Command::new(env!(\"CARGO_BIN_EXE_toron\"));",
        "    std::process::Command::new(toron_bin())",
        "let child = tokio::process::Command::new(env!(\"CARGO_BIN_EXE_toron\"))",
        "Command::new(\"toron\").arg(\"serve\")",
        "let out = std::process::Command::new(\"flywheel\")",
        "let p = std::path::Path::new(env!(\"CARGO_BIN_EXE_toron\"));",
    ] {
        assert_eq!(
            violations(bypass).len(),
            1,
            "the checker must flag: {bypass}"
        );
    }
}

#[test]
fn the_checker_passes_the_harnessed_and_justified_forms() {
    for ok in [
        "let mut c = toron_core::testkit::toron_command(env!(\"CARGO_BIN_EXE_toron\"));",
        "    toron_core::testkit::toron_command(toron_bin())",
        "let c = toron_command_clean(env!(\"CARGO_BIN_EXE_toron\"));",
        "// spawn-harness: PATH only\nlet bin = Path::new(env!(\"CARGO_BIN_EXE_toron\"));",
        "Command::new(\"git\").args([\"status\"])",
        "let out = apply_test_env(&mut std::process::Command::new(\"flywheel\"))",
        "// Command::new(env!(\"CARGO_BIN_EXE_toron\")) in a comment",
    ] {
        assert!(violations(ok).is_empty(), "the checker must accept: {ok}");
    }
}
