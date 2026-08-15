#!/usr/bin/env bash
# governed-by: toron.dev ADR-0002 D3 (test hygiene — S10, bd-test-tmp-hygiene-pq5b)
#
# Workspace-clean gate: fail if the test matrix polluted the workspace root
# with scratch dirs. 2026-08-15: ~40 dirs (`fw-gate-*`, `s*`, `toron.jsonl`)
# appeared under the workspace because callers passed RELATIVE project keys,
# which `project_dir_str` resolves under the workspace root.
#
# Debris signatures (mailbox-archive scratch): top-level entries whose name
# matches the known scratch families, or that contain `.archive.lock` +
# `messages/` and are not a real repo.
set -euo pipefail

# The workspace root is the parent of the repo (scripts/../.. = repo/..).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKSPACE_ROOT="$ROOT"
cd "$WORKSPACE_ROOT"

# Explicit allowlist: real repos + known runtime dirs (add deliberately).
ALLOW='^\.(DS_Store|git|agents|config|local|omp)$|^(toron|flywheel|chiebukuro|qmd|hachiko|nix|dotfiles|jfskills|firstmate|fast-rlm|demo|docker-stack|notetoself|noagents|jobseeker|proj|proj-tmf)$'

fail=0
shopt -s nullglob
for entry in * .[^.]*; do
    [ -e "$entry" ] || continue
    if echo "$entry" | grep -qE "$ALLOW"; then
        continue
    fi
    # Scratch mailbox root: .archive.lock + messages/
    if [ -d "$entry" ] && [ -f "$entry/.archive.lock" ] && [ -d "$entry/messages" ]; then
        echo "FAIL: $entry looks like a mailbox-archive scratch root (relative project key leaked into the workspace)" >&2
        fail=1
        continue
    fi
    # Known scratch families
    if echo "$entry" | grep -qE '^(fw-gate-|s[0-9]{4,}|toron(-[a-z0-9]+)*\.jsonl|verify-scratch|label-scratch|orphan-test|steer-fresh|omp-repro|rv-[a-z0-9-]*|gate-[0-9]+)$'; then
        echo "FAIL: $entry matches a known scratch family" >&2
        fail=1
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "WORKSPACE-CLEAN GATE FAILED — test artifacts must live in TMPDIR (temp_dir / mktemp -d), never the workspace root." >&2
    echo "Callers passing a relative project key resolve it under the workspace root (flywheel src/main.rs project_dir_str)." >&2
    exit 1
fi
echo "workspace-clean pass"
exit 0
