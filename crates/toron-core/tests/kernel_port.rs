// governed-by: ADR-0040
//! The comms kernel port (ADR-0040): the vendored elm-effect pin is intact,
//! the pin in code agrees with VENDORED_FROM, and the upstream laws the
//! redesign relies on are reachable and green THROUGH `toron_core::kernel`.
//! Negative controls prove the manifest check catches a tampered, missing
//! or extra vendored file.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use toron_core::index::workflow_effects as toron_fx;
use toron_core::kernel::{self, DispatchPlan, EffectState, EffectView, MemLog, ProviderCapability};

fn vendor_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vendor/elm-effect")
}

fn walk(dir: &Path, base: &Path, out: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            walk(&path, base, out);
        } else {
            let rel = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            let digest = Sha256::digest(fs::read(&path).unwrap());
            out.insert(rel, hex::encode(digest));
        }
    }
}

/// Every difference between MANIFEST.sha256 and the bytes under `crates/`.
fn manifest_mismatches(root: &Path) -> Vec<String> {
    let manifest = fs::read_to_string(root.join("MANIFEST.sha256")).unwrap();
    let mut want = BTreeMap::new();
    for line in manifest.lines().filter(|l| !l.trim().is_empty()) {
        let (hash, path) = line.split_once("  ").expect("sha256sum line");
        want.insert(path.to_string(), hash.to_string());
    }
    let mut have = BTreeMap::new();
    walk(&root.join("crates"), root, &mut have);
    let mut out = Vec::new();
    for (path, hash) in &want {
        match have.get(path) {
            None => out.push(format!("missing {path}")),
            Some(h) if h != hash => out.push(format!("changed {path}")),
            Some(_) => {}
        }
    }
    for path in have.keys().filter(|p| !want.contains_key(*p)) {
        out.push(format!("extra {path}"));
    }
    out
}

fn vendored_rev(root: &Path) -> String {
    fs::read_to_string(root.join("VENDORED_FROM"))
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("rev = ").map(str::to_string))
        .expect("rev line")
}

#[test]
fn vendored_framework_matches_its_manifest() {
    let root = vendor_root();
    let manifest = fs::read_to_string(root.join("MANIFEST.sha256")).unwrap();
    assert!(
        manifest.lines().count() > 50,
        "manifest lists the vendored files"
    );
    assert_eq!(manifest_mismatches(&root), Vec::<String>::new());
}

#[test]
fn kernel_rev_is_the_vendored_rev() {
    let rev = vendored_rev(&vendor_root());
    assert_eq!(rev.len(), 40);
    assert!(rev.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(kernel::ELM_EFFECT_REV, rev);
}

fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let path = entry.unwrap().path();
        let to = dst.join(path.file_name().unwrap());
        if path.is_dir() {
            copy_dir(&path, &to);
        } else {
            fs::copy(&path, &to).unwrap();
        }
    }
}

#[test]
fn negative_control_tampered_vendor_is_caught() {
    let tmp = std::env::temp_dir().join(format!("toron-kernel-vendor-{}", std::process::id()));
    let _ = fs::remove_dir_all(&tmp);
    copy_dir(&vendor_root(), &tmp);
    assert!(
        manifest_mismatches(&tmp).is_empty(),
        "a faithful copy passes"
    );

    let lib = tmp.join("crates/elm-effect-journal/src/durable_log.rs");
    let mut bytes = fs::read(&lib).unwrap();
    bytes.extend_from_slice(b"\n// tampered\n");
    fs::write(&lib, bytes).unwrap();
    fs::remove_file(tmp.join("crates/elm-effect-journal/src/laws.rs")).unwrap();
    fs::write(tmp.join("crates/elm-effect-journal/src/extra.rs"), "").unwrap();

    let found = manifest_mismatches(&tmp);
    let _ = fs::remove_dir_all(&tmp);
    assert!(
        found.contains(&"changed crates/elm-effect-journal/src/durable_log.rs".to_string()),
        "{found:?}"
    );
    assert!(
        found.contains(&"missing crates/elm-effect-journal/src/laws.rs".to_string()),
        "{found:?}"
    );
    assert!(
        found.contains(&"extra crates/elm-effect-journal/src/extra.rs".to_string()),
        "{found:?}"
    );
}

#[test]
fn stale_epoch_is_fenced_through_the_kernel_port() {
    kernel::durable_log_laws::stale_epoch_is_fenced(&MemLog::new()).unwrap();
}

#[test]
fn every_upstream_durable_log_law_holds_through_the_kernel_port() {
    let log = MemLog::new();
    for (name, outcome) in kernel::durable_log_laws::all(&log) {
        assert!(outcome.is_ok(), "{name}: {outcome:?}");
    }
}

#[test]
fn kernel_effect_table_is_toron_golden_table() {
    // ADR-0002 forbids a third spelling: the journal's table must be the
    // table toron already runs, pair for pair, before Phase 1 migrates.
    let names = [
        "prepared",
        "in_flight",
        "accepted",
        "settled",
        "failed",
        "unknown",
        "reconcile_pending",
        "cancelled",
        "parked",
    ];
    for from in names {
        for to in names {
            let ours = toron_fx::can_transition(
                toron_fx::EffectState::parse(from).unwrap(),
                toron_fx::EffectState::parse(to).unwrap(),
            );
            let theirs = kernel::can_transition(from.parse().unwrap(), to.parse().unwrap());
            assert_eq!(ours, theirs, "{from} -> {to}");
        }
    }
}

#[test]
fn mail_append_capability_reconciles_and_never_refuses_blind() {
    // Redesign §2.4: the log is the provider, so mail.append offers both an
    // idempotency key and a reconcile query. An uncertain append after an
    // expired lease is ReconcileFirst, never RefuseBlindRetry.
    let cap = ProviderCapability {
        supports_idempotency_key: true,
        supports_reconcile_query: true,
    };
    for state in [
        EffectState::InFlight,
        EffectState::Accepted,
        EffectState::Unknown,
    ] {
        let view = EffectView {
            state,
            attempt: 1,
            lease_live: false,
        };
        assert!(
            matches!(
                kernel::plan_attempt(Some(&view), &cap),
                DispatchPlan::ReconcileFirst { .. }
            ),
            "{state}"
        );
    }
    // Negative control: a provider with no contract is refused.
    let none = ProviderCapability {
        supports_idempotency_key: false,
        supports_reconcile_query: false,
    };
    let view = EffectView {
        state: EffectState::Unknown,
        attempt: 1,
        lease_live: false,
    };
    assert!(matches!(
        kernel::plan_attempt(Some(&view), &none),
        DispatchPlan::RefuseBlindRetry { .. }
    ));
}
