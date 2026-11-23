//! bd-u6o2l: toron's own commit-fence witness. The shell script clones THIS
//! repo into a temp dir, so no live checkout, daemon or `../toron` sibling is
//! needed. Kept in toron so it runs in toron's proof, not flywheel's.

use std::process::Command;

#[test]
fn toron_commit_fence_refuses_a_warning() {
    let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = crate_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("toron repo root");
    let status = Command::new("bash")
        .arg(root.join("scripts/test-fence.sh"))
        .current_dir(root)
        .env_remove("AGENT_NAME")
        .env_remove("FLYWHEEL_COMMIT_TOKEN")
        .env_remove("FLYWHEEL_COMMIT_OPERATOR")
        .status()
        .expect("run scripts/test-fence.sh");
    assert!(
        status.success(),
        "toron's staged-index fence must refuse the warning and prove the clean commit"
    );
}
