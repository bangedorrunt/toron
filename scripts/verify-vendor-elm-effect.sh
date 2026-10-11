#!/usr/bin/env bash
# governed-by: ADR-0040
#
# Vendored elm-effect pin gate (ADR-0040 §5).
#   1. integrity: every file under vendor/elm-effect/crates matches
#      MANIFEST.sha256, and no file is missing or extra;
#   2. pin agreement: toron_core::kernel::ELM_EFFECT_REV == VENDORED_FROM rev;
#   3. provenance: when an elm-effect-rs checkout holding that rev is
#      reachable (ELM_EFFECT_RS_DIR, default ../elm-effect-rs), the vendored
#      bytes equal `git archive <rev>` of the same paths. Skipped (and said
#      so) when upstream is absent, e.g. on CI until elm-effect-rs has a
#      remote; ELM_EFFECT_REQUIRE_PROVENANCE=1 turns the skip into a failure.
#
#   --regen  rewrite MANIFEST.sha256 from the vendored files (after a
#            deliberate re-vendor only).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
V=vendor/elm-effect

if command -v sha256sum >/dev/null 2>&1; then
    hash_files() { sha256sum "$@"; }
else
    hash_files() { shasum -a 256 "$@"; }
fi

manifest() {
    (cd "$1" && find crates -type f | LC_ALL=C sort | while IFS= read -r f; do hash_files "$f"; done)
}

if [ "${1:-}" = "--regen" ]; then
    manifest "$V" > "$V/MANIFEST.sha256"
    echo "regenerated $V/MANIFEST.sha256 ($(wc -l < "$V/MANIFEST.sha256" | tr -d ' ') files)"
    exit 0
fi

fail=0
rev="$(sed -n 's/^rev = \([0-9a-f]\{40\}\)$/\1/p' "$V/VENDORED_FROM")"
if [ -z "$rev" ]; then
    echo "FAIL: $V/VENDORED_FROM has no 40-hex rev line" >&2
    exit 1
fi

if diff -u "$V/MANIFEST.sha256" <(manifest "$V") >/dev/null; then
    echo "ok: vendored elm-effect matches MANIFEST.sha256 ($(wc -l < "$V/MANIFEST.sha256" | tr -d ' ') files, rev ${rev:0:12})"
else
    echo "FAIL: vendored elm-effect drifted from MANIFEST.sha256:" >&2
    diff -u "$V/MANIFEST.sha256" <(manifest "$V") >&2 || true
    fail=1
fi

if grep -q "ELM_EFFECT_REV: &str = \"$rev\"" crates/toron-core/src/kernel.rs; then
    echo "ok: kernel::ELM_EFFECT_REV pins $rev"
else
    echo "FAIL: crates/toron-core/src/kernel.rs ELM_EFFECT_REV != VENDORED_FROM rev $rev" >&2
    fail=1
fi

UP="${ELM_EFFECT_RS_DIR:-$ROOT/../elm-effect-rs}"
if git -C "$UP" cat-file -e "$rev^{commit}" 2>/dev/null; then
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    crates="$(sed -n 's/^crates = //p' "$V/VENDORED_FROM")"
    paths=()
    for c in $crates; do paths+=("crates/$c/Cargo.toml" "crates/$c/src"); done
    git -C "$UP" archive "$rev" "${paths[@]}" | tar -x -C "$tmp"
    if diff -u "$V/MANIFEST.sha256" <(manifest "$tmp") >/dev/null; then
        echo "ok: provenance verified against $UP@${rev:0:12}"
    else
        echo "FAIL: vendored bytes differ from $UP@$rev:" >&2
        diff -u <(manifest "$tmp") "$V/MANIFEST.sha256" >&2 || true
        fail=1
    fi
elif [ "${ELM_EFFECT_REQUIRE_PROVENANCE:-0}" = "1" ]; then
    echo "FAIL: provenance required but $UP does not hold $rev" >&2
    fail=1
else
    echo "skip: provenance (no elm-effect-rs checkout holding $rev at $UP)"
fi

exit "$fail"
