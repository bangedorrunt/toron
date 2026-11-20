#!/usr/bin/env bash
# governed-by: ADR-0003
#
# toron's commit fence (bd-u6o2l): prove the tree a commit would land, in a
# throwaway export, so a dirty worktree cannot change the answer. This is what
# "fence green" means for toron — scripts/verify-gates.sh alone only greps
# policy and cannot catch a dead_code warning.
#
#   scripts/fence.sh [--index] [--sha <rev>] [--repo <dir>] [--keep]
#
# Both modes run the same steps on an exported tree:
#   RUSTFLAGS=-Dwarnings build --workspace --all-targets ·
#   clippy --workspace --all-targets -- -D warnings ·
#   rustfmt --check on staged .rs (--index only) · verify-gates
#
# --index fences exactly what a commit would land: `git write-tree` over the
# staging area, a throwaway commit off HEAD, then export that commit. A caller
# staged without its callee dies before HEAD moves. The pre-commit hook and
# `flywheel commit --repo ../toron` (private GIT_INDEX_FILE) both use this mode.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$ROOT"
REV="HEAD"
KEEP=0
INDEX=0
CARGO="${FENCE_CARGO:-mbx}"
# skip_children: checking a staged lib.rs must not recurse into unstaged
# modules another lane is still editing.
RUSTFMT="${FENCE_RUSTFMT:-rustfmt --edition 2024 --config skip_children=true}"

while [ $# -gt 0 ]; do
    case "$1" in
        --sha) REV="$2"; shift ;;
        --repo) REPO="$2"; shift ;;
        --index) INDEX=1 ;;
        --keep) KEEP=1 ;;
        -h | --help) sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "fence: unknown arg: $1" >&2; exit 2 ;;
    esac
    shift
done

STAGED_RS=""
if [ "$INDEX" = 1 ]; then
    TREE="$(git -C "$REPO" write-tree)"
    SHA="$(printf '%s\n' 'fence-index' | git -C "$REPO" \
        -c user.name='fence' -c user.email='fence@localhost' \
        commit-tree "$TREE" -p HEAD)"
    STAGED_RS="$(git -C "$REPO" diff --cached --name-only --diff-filter=ACM -- '*.rs')"
else
    SHA="$(git -C "$REPO" rev-parse "$REV")"
fi

git -C "$REPO" cat-file -e "${SHA}^{commit}" 2>/dev/null \
    || { echo "fence: rev $SHA not found in $REPO" >&2; exit 1; }
TMP="$(mktemp -d "${TMPDIR:-/tmp}/toron-fence.XXXXXX")"
cleanup() { [ "$KEEP" -eq 1 ] || rm -rf "$TMP"; }
trap cleanup EXIT
DEST="$TMP/toron"
mkdir -p "$DEST"
git -C "$REPO" archive "$SHA" | tar -x -C "$DEST"
# Path deps outside this repo (elm-effect-rs) come from the pins in the fenced
# revision's fence.toml, not from a neighbour's dirty working tree.
FENCE_TOML="$(git -C "$REPO" show "$SHA:fence.toml" 2>/dev/null || true)"
[ -n "$FENCE_TOML" ] || FENCE_TOML="$(cat "$REPO/fence.toml" 2>/dev/null || true)"
while IFS=$'\t' read -r rel pin; do
    [ -n "${rel:-}" ] || continue
    sib_repo="$(cd "$REPO/$rel" && pwd)"
    git -C "$sib_repo" cat-file -e "${pin}^{commit}" 2>/dev/null \
        || { echo "fence: sibling pin $pin not found in $sib_repo" >&2; exit 1; }
    sib_dest="$TMP/$(basename "$rel")"
    mkdir -p "$sib_dest"
    git -C "$sib_repo" archive "$pin" | tar -x -C "$sib_dest"
done < <(
    printf '%s\n' "$FENCE_TOML" | awk '
        /^\[\[sibling\]\]/ { if (path != "") print path "\t" rev; path=""; rev=""; in_block=1; next }
        /^\[/ { if (path != "") print path "\t" rev; path=""; rev=""; in_block=0; next }
        in_block && /^[[:space:]]*path[[:space:]]*=/ { v=$0; sub(/^[^=]*=[[:space:]]*"/,"",v); sub(/".*/,"",v); path=v }
        in_block && /^[[:space:]]*rev[[:space:]]*=/ { v=$0; sub(/^[^=]*=[[:space:]]*"/,"",v); sub(/".*/,"",v); rev=v }
        END { if (path != "") print path "\t" rev }
    '
)
echo "fence: $SHA exported to $DEST"
cd "$DEST"

step() { printf 'fence: %s\n' "$1"; }
step "build (-D warnings, workspace, all targets)"
# shellcheck disable=SC2086
RUSTFLAGS=-Dwarnings $CARGO build --workspace --all-targets
step "clippy (-D warnings, workspace, all targets)"
# shellcheck disable=SC2086
$CARGO clippy --workspace --all-targets -- -D warnings
if [ "$INDEX" = 1 ]; then
    for f in $STAGED_RS; do
        [ -f "$f" ] || continue
        step "fmt ($f)"
        # shellcheck disable=SC2086
        $RUSTFMT --check "$f"
    done
fi
step "verify-gates"
bash scripts/verify-gates.sh
echo "fence: $SHA GREEN"
