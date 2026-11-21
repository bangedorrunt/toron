#!/usr/bin/env bash
# bd-ll7bs: model-check the lease waitlist with the pinned native checker.
# Positive models pass; each broken .cfg must fail naming exactly one
# invariant. No Java, no TLC jar.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
MODELS="$REPO/.agents/skills/verify-lease-resolution/models"
CHECKER="${EVERYSTATE:-everystate}"

command -v "$CHECKER" >/dev/null 2>&1 || {
  echo "verify-lease-waitlist: everystate not found (pin 0.4.1)" >&2
  exit 2
}

WORK="$(mktemp -d "${TMPDIR:-/tmp}/lease-waitlist-formal.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
cp "$MODELS/ToronLeaseResolution.tla" "$WORK/"
for cfg in "$MODELS"/ToronLeaseResolution*.cfg; do
  cp "$cfg" "$WORK/"
done

fail=0
run() { # cfg expected-status expected-invariant
  local cfg="$1" want="$2" name="${3:-}" got out
  out="$(cd "$WORK" && "$CHECKER" -json -deadlock -config "$cfg.cfg" ToronLeaseResolution.tla 2>&1)" || true
  got="$(printf '%s' "$out" | head -1 | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status","?"))' 2>/dev/null || echo '?')"
  if [ "$got" != "$want" ]; then
    echo "FAIL $cfg: expected $want, got $got" >&2
    fail=$((fail + 1))
    return
  fi
  if [ -n "$name" ] && ! printf '%s' "$out" | grep -q "\"name\":\"$name\""; then
    echo "FAIL $cfg: expected invariant $name in $out" >&2
    fail=$((fail + 1))
    return
  fi
  echo "PASS $cfg ($got${name:+ $name})"
}

run ToronLeaseResolution.fixed ok
run ToronLeaseResolution.waitlist ok
run ToronLeaseResolution.waitlist.broken-double-holder invariant OneHolder
run ToronLeaseResolution.waitlist.broken-lifo invariant FifoHandoff
run ToronLeaseResolution.waitlist.broken-drop-waiter invariant NoLostWaiter
run ToronLeaseResolution.waitlist.broken-no-ttl invariant HandoffOnEveryRelease

[ "$fail" -eq 0 ] || { echo "verify-lease-waitlist: $fail failed" >&2; exit 1; }
echo "verify-lease-waitlist: all checks passed"
