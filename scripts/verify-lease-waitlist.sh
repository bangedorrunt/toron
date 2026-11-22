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
# Every broken fixture is discovered by file name; its FIRST line names the
# invariant it must violate. No registry to edit when a new invariant lands.
for cfg_file in "$MODELS"/ToronLeaseResolution*.broken*.cfg; do
  [ -f "$cfg_file" ] || continue
  name="$(command head -1 "$cfg_file" | sed -n 's/^\\\* \([A-Za-z0-9]*\) broken:.*/\1/p')"
  [ -n "$name" ] || { echo "FAIL $cfg_file: first line must name its invariant" >&2; fail=$((fail + 1)); continue; }
  run "$(basename "$cfg_file" .cfg)" invariant "$name"
done

conform() { # trace expected-rc step
  local trace="$1" expected="$2" step="$3" out rc
  out="$(cd "$WORK" && "$CHECKER" conform -trace "$trace" \
    -config ToronLeaseResolution.waitlist.cfg ToronLeaseResolution.tla 2>&1)" || rc=$?
  rc="${rc:-0}"
  if [ "$rc" = "$expected" ] && { [ -z "$step" ] || printf '%s' "$out" | grep -q "$step"; }; then
    echo "PASS conform $trace (exit $rc${step:+ $step})"
  else
    echo "FAIL conform $trace: expected $expected $step, got $rc: $out" >&2
    fail=$((fail + 1))
  fi
}
TRACES="$REPO/.agents/skills/verify-lease-resolution/traces"
cp "$TRACES"/ToronLeaseResolution.*.jsonl "$WORK/"
conform ToronLeaseResolution.recorded.jsonl 0 ""
conform ToronLeaseResolution.no-holder-release.jsonl 20 "HandoffRelease"
conform ToronLeaseResolution.pre-edit.jsonl 0 ""

[ "$fail" -eq 0 ] || { echo "verify-lease-waitlist: $fail failed" >&2; exit 1; }
echo "verify-lease-waitlist: all checks passed"
