#!/usr/bin/env bash
# governed-by: ADR-0003
#
# S10 workspace-hygiene gate: no new top-level dirs in the workspace root
# during `cargo test` runs.
#
# 2026-08-15 observed: ~40 scratch dirs (`fw-gate-*`, `s2*`, `s3*`,
# `s66667`, `toron.jsonl`) created in ~/workspace by gate tests booting
# toron mailboxes with cwd-derived roots (`data_dir.join(absolute_slug)`
# escapes via PathBuf::join; flywheel `{project}.jsonl` state logs escaped
# the state dir the same way). Both escapes are fixed; this gate proves it
# stays fixed.
#
# Usage: scripts/verify-workspace-hygiene.sh [--workspace-root PATH]
#   Runs `cargo test --workspace` for the toron repo AND the sibling
#   flywheel repo (when present), snapshotting the workspace root's
#   top-level entries before/after. Exits non-zero on any NEW top-level
#   dir or file that appeared during the runs.
#
# Call from the repo root. Skip with `S10_SKIP=1` (e.g. sandboxed CI
# without a real workspace).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WS="${WORKSPACE_ROOT:-$(dirname "$ROOT")}"

if [ "${S10_SKIP:-0}" = "1" ]; then
    echo "ok: S10 workspace-hygiene gate skipped (S10_SKIP=1)"
    exit 0
fi
if [ ! -d "$WS" ]; then
    echo "FAIL: workspace root $WS missing" >&2
    exit 1
fi

snapshot() {
    ls -A "$WS" | sort
}

BEFORE="$(snapshot)"
echo "workspace root: $WS ($(echo "$BEFORE" | wc -l | tr -d ' ') top-level entries)"

# Run the toron suite (the mailbox-escape fix lives in toron-core).
(cd "$ROOT" && cargo test --workspace) || {
    echo "FAIL: toron cargo test failed" >&2
    exit 1
}

# Run the flywheel suite when it is a sibling (the state-log escape fix
# lives in flywheel/src/state.rs).
if [ -d "$WS/flywheel" ] && [ -f "$WS/flywheel/Cargo.toml" ]; then
    (cd "$WS/flywheel" && cargo test --workspace) || {
        echo "FAIL: flywheel cargo test failed" >&2
        exit 1
    }
fi

AFTER="$(snapshot)"
NEW="$(comm -13 <(echo "$BEFORE") <(echo "$AFTER") || true)"
if [ -n "$NEW" ]; then
    echo "FAIL: new top-level entries appeared in $WS during cargo test:" >&2
    echo "$NEW" >&2
    echo "  A test booted a mailbox with a cwd-derived root or wrote a" >&2
    echo "  state log outside its dir. Root causes to check:" >&2
    echo "  - toron: mailbox::aliases::project_root (absolute slug escape)" >&2
    echo "  - flywheel: state::project_log_path (absolute project escape)" >&2
    exit 1
fi

echo "gates pass: no new workspace dirs from cargo test"
exit 0
