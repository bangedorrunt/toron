// governed-by: ADR-0040
//! Import fence for the comms kernel port (ADR-0040 §4): `kernel.rs` is the
//! only file under `crates/` that names an elm-effect crate, and toron-core
//! is the only crate manifest that depends on one. The needle is built at
//! runtime so this file does not trip its own fence. A fixture tree proves
//! the scan finds an offender (negative control) and allows `kernel.rs`.

use std::fs;
use std::path::{Path, PathBuf};

fn needle_rs() -> String {
    ["elm", "_effect"].concat()
}

fn needle_toml() -> String {
    ["elm", "-effect"].concat()
}

const KERNEL: &str = "crates/toron-core/src/kernel.rs";
const CORE_MANIFEST: &str = "crates/toron-core/Cargo.toml";

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if path.is_dir() {
            if name != "target" && !name.starts_with('.') {
                files(&path, out);
            }
        } else {
            out.push(path);
        }
    }
}

/// Offending files, as paths relative to `root`.
fn offenders(root: &Path) -> Vec<String> {
    let mut all = Vec::new();
    files(&root.join("crates"), &mut all);
    let mut out = Vec::new();
    for path in all {
        let rel = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        let code: String = text
            .lines()
            .filter(|l| {
                let t = l.trim_start();
                !(t.starts_with("//") || t.starts_with('#'))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let rust_offender = rel.ends_with(".rs") && rel != KERNEL && code.contains(&needle_rs());
        let manifest_offender =
            rel.ends_with("Cargo.toml") && rel != CORE_MANIFEST && code.contains(&needle_toml());
        if rust_offender || manifest_offender {
            out.push(rel);
        }
    }
    out.sort();
    out
}

#[test]
fn only_the_kernel_port_names_the_framework() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(offenders(&root), Vec::<String>::new());
    let kernel = fs::read_to_string(root.join(KERNEL)).unwrap();
    assert!(kernel.contains(&format!("pub use {}_journal", needle_rs())));
}

#[test]
fn negative_control_fence_catches_an_offender() {
    let tmp = std::env::temp_dir().join(format!("toron-kernel-fence-{}", std::process::id()));
    let _ = fs::remove_dir_all(&tmp);
    let write = |rel: &str, body: &str| {
        let p = tmp.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, body).unwrap();
    };
    write(
        KERNEL,
        &format!("pub use {}_journal::EffectState;\n", needle_rs()),
    );
    write(
        CORE_MANIFEST,
        &format!("{}-journal = {{ path = \"x\" }}\n", needle_toml()),
    );
    write(
        "crates/toron-core/src/mailbox/sneaky.rs",
        &format!("use {}_journal::durable_log::DurableLog;\n", needle_rs()),
    );
    write(
        "crates/toron-workflow/Cargo.toml",
        &format!("{}-elm = {{ path = \"x\" }}\n", needle_toml()),
    );
    write(
        "crates/toron-cli/src/ok.rs",
        &format!(
            "// mentions {} in a comment only\nuse toron_core::kernel::Loop;\n",
            needle_rs()
        ),
    );
    let found = offenders(&tmp);
    let _ = fs::remove_dir_all(&tmp);
    assert_eq!(
        found,
        vec![
            "crates/toron-core/src/mailbox/sneaky.rs".to_string(),
            "crates/toron-workflow/Cargo.toml".to_string(),
        ]
    );
}
