#!/usr/bin/env bash
# governed-by: ADR-0030
#
# Presence-gated chokepoints — acceptance harness (ADR-0030).
#
# RED BY DESIGN until the ADR's waves ship. Each section maps to one wave;
# a section flips to PASS when its surface lands. Green = ADR-0030
# implemented. Run from the toron repo root (or anywhere with `toron`, `br`,
# and `flywheel` on PATH).
#
# Checks per wave:
#   W1  toron whois --fresh        (presence freshness query)
#   W2  strict chokepoint refusals (br claim/gate/close, toron reserve)
#   W3  gates.jsonl presence/briefed fields + br doctor check
#   W4  doorbell suspect banner    (surface exists; full proof is e2e)
#   W5  flywheel session-start     (one-command door)
#
# Read-only against live systems except W2's probe bead, which is created
# and deleted in a throwaway TORON_BEADS_DIR. Exits non-zero listing every
# unshipped wave.
set -uo pipefail

pass=0; red=0; failed=()

section() { printf '\n== %s ==\n' "$1"; }
ok()      { printf '  PASS  %s\n' "$1"; pass=$((pass+1)); }
missing() { printf '  RED   %s\n' "$1"; red=$((red+1)); failed+=("$1"); }

have() { command -v "$1" >/dev/null 2>&1; }

section "W1 presence freshness primitive"
if have toron && toron whois --help 2>&1 | grep -q -- "--fresh"; then
  ok "W1 whois --fresh exists"
else
  missing "W1 whois --fresh (toron whois freshness query)"
fi

section "W2 chokepoint refusals under TORON_PROTOCOL_STRICT=1"
if ! have br; then
  missing "W2 br not on PATH"
else
  PROBE_DIR="$(mktemp -d)"
  export TORON_BEADS_DIR="$PROBE_DIR"
  if br create --title "adr0030 probe" --type task -p 3 >/dev/null 2>&1; then
    PROBE_ID="$(br list --json 2>/dev/null | python3 -c 'import json,sys;print(json.load(sys.stdin)["issues"][0]["id"])' 2>/dev/null || true)"
    if [ -n "${PROBE_ID:-}" ]; then
      if TORON_PROTOCOL_STRICT=1 br update "$PROBE_ID" --status in_progress >/dev/null 2>&1; then
        missing "W2 claim refuses unarrived pin in strict mode"
      else
        ok "W2 claim refusal"
      fi
      if TORON_PROTOCOL_STRICT=1 br close "$PROBE_ID" --commit-sha 0000000000000000000000000000000000000000 >/dev/null 2>&1; then
        missing "W2 close refuses unarrived pin in strict mode"
      else
        # refusal must be PRESENCE-attributable, not the pre-existing
        # gate/sha refusal (fail-closed close refuses everything today)
        ERR="$(TORON_PROTOCOL_STRICT=1 br close "$PROBE_ID" --commit-sha 0000000000000000000000000000000000000000 2>&1 || true)"
        if printf '%s' "$ERR" | grep -qiE "presence|unarrived|arrive"; then
          ok "W2 close refusal (presence-attributed)"
        else
          missing "W2 close refusal presence-attributed (today refuses on gate/sha grounds instead)"
        fi
      fi
    else
      missing "W2 probe bead id unreadable"
    fi
  else
    missing "W2 probe bead creation (br create in throwaway dir)"
  fi
  rm -rf "$PROBE_DIR" 2>/dev/null
  unset TORON_BEADS_DIR
fi

section "W3 gates.jsonl session metadata + doctor check"
if have br && br doctor --help 2>&1 | grep -qi "unarrived"; then
  ok "W3 br doctor unarrived-closes check"
else
  missing "W3 br doctor unarrived-closes check"
fi
if have br && br gate --help 2>&1 | grep -qiE "presence|briefed"; then
  ok "W3 gate rows carry presence/briefed metadata"
else
  missing "W3 gate rows presence/briefed fields"
fi

section "W4 doorbell suspect banner surface"
if have flywheel && flywheel doorbell --help 2>&1 | grep -qi "suspect"; then
  ok "W4 doorbell suspect-banner surface documented"
else
  missing "W4 doorbell suspect-banner (full e2e proof per ADR §Wave 4)"
fi

section "W5 one-command session door"
if have flywheel && flywheel help 2>&1 | grep -q "session-start"; then
  ok "W5 flywheel session-start verb"
else
  missing "W5 flywheel session-start"
fi

printf '\n== summary ==\nPASS %d / RED %d\n' "$pass" "$red"
if [ "$red" -gt 0 ]; then
  printf 'ADR-0030 not fully implemented. Unshipped waves:\n'
  for f in "${failed[@]}"; do printf '  - %s\n' "$f"; done
  exit 1
fi
printf 'ADR-0030 acceptance: GREEN\n'
