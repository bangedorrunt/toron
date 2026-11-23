#!/usr/bin/env bash
# bd-u6o2l: the commit fence in a throwaway repo built from SOURCE FILES, so it
# also runs from a git-archive export (no .git), as in clean-proof. Two commits:
# one with a dead_code field must be refused at commit time; the fixed one must
# pass, and --sha must then prove that clean commit. No live repo is touched.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/toron-fence-test.XXXXXX")"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# Build a real git repo at DEST from SRC. Where SRC has git objects we archive
# HEAD — the committed tree, never another lane's dirty WIP. An archive export
# has no .git, so there we copy its files. Either way DEST gets a fresh HEAD.
make_repo() {
    local src="$1" dest="$2"
    mkdir -p "$dest"
    if git -C "$src" rev-parse --git-dir >/dev/null 2>&1; then
        git -C "$src" archive HEAD | tar -x -C "$dest"
    else
        tar -C "$src" --exclude=.git --exclude=target --exclude=.lavish -cf - . |
            tar -C "$dest" -xf -
    fi
    git -C "$dest" init -q
    git -C "$dest" config user.name FenceTest
    git -C "$dest" config user.email fence-test@localhost
}

make_repo "$ROOT" "$TMP/toron"
cd "$TMP/toron"
# Overlay the fence under test so this script also works before it is committed.
mkdir -p scripts/hooks
cp "$ROOT/scripts/fence.sh" scripts/fence.sh
cp "$ROOT/scripts/hooks/pre-commit" scripts/hooks/pre-commit
cp "$ROOT/scripts/hooks/install.sh" scripts/hooks/install.sh
cp "$ROOT/fence.toml" fence.toml
chmod +x scripts/fence.sh scripts/hooks/pre-commit scripts/hooks/install.sh

# The fence exports ../elm-effect-rs from git at the pin in fence.toml. An
# archive export has no git objects for that sibling either, so build it from
# its files and repoint the pin at the fresh commit.
SIB_SRC="$ROOT/../elm-effect-rs"
[ -d "$SIB_SRC" ] || { echo "test-fence: $SIB_SRC missing" >&2; exit 1; }
if git -C "$SIB_SRC" rev-parse --git-dir >/dev/null 2>&1; then
    git clone -q --shared "$SIB_SRC" "$TMP/elm-effect-rs"
else
    make_repo "$SIB_SRC" "$TMP/elm-effect-rs"
    (cd "$TMP/elm-effect-rs" && git add -A && git commit -qm 'sibling fixture')
    sib_pin="$(git -C "$TMP/elm-effect-rs" rev-parse HEAD)"
    python3 - "$TMP/toron/fence.toml" "$sib_pin" <<'PY'
import re, sys
path, pin = sys.argv[1], sys.argv[2]
text = open(path).read()
text, n = re.subn(r'(?m)^rev\s*=\s*"[^"]*"', f'rev = "{pin}"', text, count=1)
if n != 1:
    raise SystemExit("test-fence: no sibling rev in fence.toml")
open(path, "w").write(text)
PY
fi

git add -A
git commit -qm 'base'
bash scripts/hooks/install.sh
hp="$(git config --get core.hooksPath)"
[ "$hp" = "scripts/hooks" ] || { echo "test-fence: hooksPath=$hp" >&2; exit 1; }

probe=crates/toron-core/src/fence_probe.rs
cat >"$probe" <<'EOF'
pub struct Probe(std::fs::File);
pub fn probe() -> Probe {
    Probe(std::fs::File::open("/dev/null").expect("open"))
}
EOF
printf 'pub mod fence_probe;\n' >>crates/toron-core/src/lib.rs
git add "$probe" crates/toron-core/src/lib.rs
head_before="$(git rev-parse HEAD)"
if FLYWHEEL_COMMIT_OPERATOR=1 git commit -m 'probe: unused field must be refused' >"$TMP/bad.log" 2>&1; then
    echo "test-fence: commit with an unused field passed the fence" >&2
    cat "$TMP/bad.log" >&2
    exit 1
fi
grep -q 'never read' "$TMP/bad.log" || { echo "test-fence: wrong failure:" >&2; cat "$TMP/bad.log" >&2; exit 1; }
[ "$(git rev-parse HEAD)" = "$head_before" ] || { echo "test-fence: HEAD moved on a red fence" >&2; exit 1; }
echo "test-fence: unused field refused at commit time"

cat >"$probe" <<'EOF'
pub struct Probe {
    _file: std::fs::File,
}
pub fn probe() -> Probe {
    Probe {
        _file: std::fs::File::open("/dev/null").expect("open"),
    }
}
EOF
git add "$probe" crates/toron-core/src/lib.rs
FLYWHEEL_COMMIT_OPERATOR=1 git commit -m 'probe: named field is clean' >"$TMP/good.log" 2>&1 || {
    echo "test-fence: clean commit was refused" >&2
    cat "$TMP/good.log" >&2
    exit 1
}
[ "$(git rev-parse HEAD)" != "$head_before" ] || { echo "test-fence: HEAD did not move" >&2; exit 1; }
echo "test-fence: clean commit passed the hook"

bash scripts/fence.sh --sha HEAD >"$TMP/sha.log" 2>&1 || {
    echo "test-fence: --sha fence red on a clean commit" >&2
    cat "$TMP/sha.log" >&2
    exit 1
}
echo "test-fence: --sha GREEN on the clean commit"

# flywheel commit --repo must drive the same hook on its private index, never
# sweeping another lane's staged file (bd-aberm + bd-u6o2l).
if command -v flywheel >/dev/null 2>&1 && flywheel commit --help 2>/dev/null | grep -q -- '--repo'; then
    printf 'foreign\n' >foreign.txt
    git add foreign.txt
    printf 'named\n' >named.txt
    flywheel commit named.txt -m 'flywheel commit private index' --repo "$TMP/toron" \
        --no-reservation-check \
        >"$TMP/fwcommit.log" 2>&1 || {
        echo "test-fence: flywheel commit --repo failed" >&2
        cat "$TMP/fwcommit.log" >&2
        exit 1
    }
    tree_files="$(git ls-tree -r --name-only HEAD)"
    printf '%s\n' "$tree_files" | grep -qx named.txt \
        || { echo "test-fence: named path missing from commit" >&2; exit 1; }
    printf '%s\n' "$tree_files" | grep -qx foreign.txt \
        && { echo "test-fence: flywheel commit swept the foreign staged file" >&2; exit 1; }
    git diff --cached --name-only | grep -qx foreign.txt \
        || { echo "test-fence: foreign staged file was stripped" >&2; exit 1; }
    echo "test-fence: flywheel commit --repo fenced the private tree"
fi
