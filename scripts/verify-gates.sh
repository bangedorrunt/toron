#!/usr/bin/env bash
# governed-by: ADR-0003
#
# CI gate: ADR-0003 hard constraints.
#   - zero asupersync in live code + Cargo (runtime dropped by captain Tokio override)
#   - zero fastmcp-rust (asupersync-based; forbidden)
#   - every crate root has #![forbid(unsafe_code)]
#
# Scope = live workspace code + manifests (crates/**, root Cargo.toml, Cargo.lock).
# Historical ADR/beads text and dead legacy files are excluded per ADR-0003
# verification ("prefer zero in code+Cargo").
#
# Exits non-zero on any violation. Call from the repo root.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail=0

# 1. no asupersync in crates/ code + manifests + lockfile.
if rg -n --hidden 'asupersync' crates/ Cargo.toml Cargo.lock >/dev/null 2>&1; then
    echo "FAIL: asupersync references in live code/Cargo:" >&2
    rg -n --hidden 'asupersync' crates/ Cargo.toml Cargo.lock >&2 || true
    fail=1
else
    echo "ok: no asupersync in live code/Cargo"
fi

# 2. no fastmcp-rust in crates/ code + manifests + lockfile.
if rg -n --hidden 'fastmcp' crates/ Cargo.toml Cargo.lock >/dev/null 2>&1; then
    echo "FAIL: fastmcp references in live code/Cargo:" >&2
    rg -n --hidden 'fastmcp' crates/ Cargo.toml Cargo.lock >&2 || true
    fail=1
else
    echo "ok: no fastmcp in live code/Cargo"
fi

# 3. every crate root declares #![forbid(unsafe_code)].
#    Crate roots: crates/*/src/{lib.rs,main.rs}.
missing=0
for crate_root in crates/*/src/lib.rs crates/*/src/main.rs; do
    [ -f "$crate_root" ] || continue
    if ! rg -q '^#!\[forbid\(unsafe_code\)\]' "$crate_root"; then
        echo "FAIL: $crate_root missing #![forbid(unsafe_code)]" >&2
        missing=$((missing + 1))
        fail=1
    fi
done
if [ "$missing" -eq 0 ]; then
    echo "ok: all crate roots forbid unsafe_code"
fi

# 4. ADR <-> code linkage (bd-p6-verify-adr-links-script-xhi2): every
#    governed-by marker references an existing ADR; every forbid(unsafe_code)
#    file (ADR-0003 constraint) carries a marker.
if ! bash docs/decisions/_scripts/verify-adr-links.sh; then
    fail=1
fi

# 5. Workspace-clean (bd-test-tmp-hygiene-pq5b): the test matrix must not
#    pollute the workspace root with mailbox-archive scratch dirs. Relative
#    project keys resolve under the workspace root — tests/scripts must pass
#    absolute TMPDIR paths (temp_dir / mktemp -d).
if ! bash scripts/verify-workspace-clean.sh; then
    fail=1
fi

# 6. Vendored elm-effect pin (ADR-0040): manifest integrity, kernel rev
#    agreement, and provenance when an elm-effect-rs checkout is reachable.
if ! bash scripts/verify-vendor-elm-effect.sh; then
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    echo "GATE FAILED" >&2
    exit 1
fi

echo "gates pass"
exit 0
