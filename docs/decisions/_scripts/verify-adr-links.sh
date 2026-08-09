#!/usr/bin/env bash
# governed-by: ADR-0008
#
# ADR <-> code linkage gate (bd-p6-verify-adr-links-script-xhi2).
# AGENTS.md: "Code <-> ADR Linkage: you MUST add `// governed-by: ADR-XXXX` to
# the top of any file you create or modify that implements an ADR constraint.
# This is verified by docs/decisions/_scripts/verify-adr-links.sh".
#
# Checks:
#   1. Every file that mentions an ADR constraint marker (`governed-by: ADR-`)
#      references an EXISTING ADR file (docs/decisions/000N-*.md). Dangling
#      references fail.
#   2. Every crate-root / file carrying `#![forbid(unsafe_code)]` (an ADR-0003
#      constraint) carries a `governed-by:` marker. Missing markers fail.
#   3. Report-only: files under crates/ that mention `ADR-` but carry no
#      marker are listed (informational — a future tightened rule).
#
# Scope = live workspace code + tests (crates/**). Excludes target/, .git/.
# Exits non-zero on any violation. Call from the repo root.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

fail=0

# Existing ADR files, as "0003" "0004" ... (numeric prefix only).
mapfile -t ADR_FILES < <(ls docs/decisions/ | sed -n 's/^\(000[0-9]\)\-.*/\1/p' | sort -u)

if [ "${#ADR_FILES[@]}" -eq 0 ]; then
    echo "FAIL: no ADR files found under docs/decisions/" >&2
    exit 1
fi

# Build a set of valid ADR numbers.
valid=""
for a in "${ADR_FILES[@]}"; do
    valid="$valid $a"
done

echo "valid ADRs:${valid}"

# --- 1. dangling references -------------------------------------------
dangling=0
while IFS=: read -r file line adr rest; do
    if ! echo "$valid" | tr ' ' '\n' | grep -qx "$adr"; then
        echo "FAIL: $file:$line references non-existent ADR-$adr" >&2
        dangling=$((dangling + 1))
        fail=1
    fi
done < <(rg -n --no-heading -o 'governed-by: ADR-([0-9]+)' crates/ -g '*.rs' 2>/dev/null \
        | sed -E 's/^([^:]+):([0-9]+):governed-by: ADR-([0-9]+)$/\1:\2:\3/' || true)

if [ "$dangling" -eq 0 ]; then
    echo "ok: no dangling governed-by references"
fi

# --- 2. forbid(unsafe_code) files must carry a marker -------------------
missing=0
while IFS= read -r f; do
    if ! rg -q 'governed-by: ADR-' "$f"; then
        echo "FAIL: $f has #![forbid(unsafe_code)] (ADR-0003 constraint) but no governed-by marker" >&2
        missing=$((missing + 1))
        fail=1
    fi
done < <(rg -l '#!\[forbid\(unsafe_code\)\]' crates/ -g '*.rs' 2>/dev/null | sort)

if [ "$missing" -eq 0 ]; then
    echo "ok: every forbid(unsafe_code) file carries a governed-by marker"
fi

# --- 3. report-only: ADR mention without marker -------------------------
unmarked=$(rg -l 'ADR-000[0-9]' crates/ -g '*.rs' 2>/dev/null \
    | while read -r f; do rg -q 'governed-by: ADR-' "$f" || echo "$f"; done || true)
if [ -n "$unmarked" ]; then
    echo "info: files mentioning ADR- but lacking a governed-by marker (report-only):"
    echo "$unmarked" | sed 's/^/  /'
else
    echo "ok: no ADR-mentioning file lacks a governed-by marker"
fi

if [ "$fail" -ne 0 ]; then
    echo "ADR-LINK GATE FAILED" >&2
    exit 1
fi

echo "adr-links pass"
exit 0
