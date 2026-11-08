#!/usr/bin/env bash
# governed-by: ADR-0003
#
# Daemon-free end-to-end behavior matrix. It uses only a temp store and temp
# Flywheel state: no :18080, no :18980, no live toron store, no live panes.
#
# Covers the live-test failure modes:
#   1. non-member channel post refuses
#   2. --mention without @Name refuses before publishing
#   3. @Name + --mention creates one job; claim/result reaches done
#   4. urgent peer mail is acked, replied to, and receipted
#   5. done mail lands in the thread
#   6. optional: supervisor view exposes queued input / not_started
#   7. optional: doorbell why sees canonical flywheel-<project>* scope
#
# Usage:
#   bash scripts/e2e-behavior.sh
#   FLYWHEEL_BIN=../flywheel/target/debug/flywheel bash scripts/e2e-behavior.sh
#
# The Flywheel section is skipped when no flywheel binary is found.

set -euo pipefail

ROOT="${TORON_E2E_ROOT:-$(mktemp -d "${TMPDIR:-/tmp}/toron-e2e-behavior.XXXXXX")}"
cleanup() {
    if [[ "${TORON_E2E_KEEP:-0}" == "1" ]]; then
        printf 'kept test root: %s\n' "$ROOT" >&2
    else
        rm -rf "$ROOT"
    fi
}
trap cleanup EXIT

PROJECT="e2e-behavior"
A="AmberFox"
B="BlueMarsh"
C="CoralFinch"
CH="behavior-$$"
CORR="e2e-$CH"
DATA="$ROOT/data"

die() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

step() {
    printf '\n== %s\n' "$*"
}

TORON="${TORON_BIN:-}"
if [[ -z "$TORON" ]]; then
    if [[ -x target/debug/toron ]]; then
        TORON="$PWD/target/debug/toron"
    else
        TORON="$(command -v toron || true)"
    fi
fi
[[ -n "$TORON" && -x "$TORON" ]] || die "toron binary not found (build toron-cli or set TORON_BIN)"
command -v jq >/dev/null 2>&1 || die "jq is required"

mkdir -p "$DATA"
export TORON_MCP_DATA_DIR="$DATA"
export TORON_DATA_DIR="$DATA"
export TORON_WAKE_SOCK="$ROOT/no-wake.sock"
unset TORON_MCP_URL

run() {
    "$TORON" "$@" --json
}

assert_jq() {
    local file="$1"
    shift
    if ! jq -e "$@" "$file" >/dev/null 2>&1; then
        printf 'assertion failed: jq -e %s %s\n' "$*" "$file" >&2
        cat "$file" >&2
        exit 1
    fi
}

expect_code() {
    local want="$1"
    local label="$2"
    shift 2
    local out="$ROOT/${label}.out"
    local err="$ROOT/${label}.err"
    set +e
    "$TORON" "$@" --json >"$out" 2>"$err"
    local got=$?
    set -e
    if [[ "$got" -ne "$want" ]]; then
        printf 'expected exit %s for %s, got %s\n' "$want" "$label" "$got" >&2
        cat "$out" >&2
        cat "$err" >&2
        exit 1
    fi
}

step "register three pins"
run agent bootstrap --project "$PROJECT" --as "$A" >/dev/null
run agent bootstrap --project "$PROJECT" --as "$B" >/dev/null
run agent bootstrap --project "$PROJECT" --as "$C" >/dev/null

step "private room: non-member is refused"
run channel create "$CH" --name "e2e behavior" --private --project "$PROJECT" --as "$A" >/dev/null
run channel join "$CH" --project "$PROJECT" --as "$B" >/dev/null
expect_code 1 "nonmember-post" channel post "$CH" --body "not a member" --project "$PROJECT" --as "$C"
grep -qi 'member' "$ROOT/nonmember-post.err" || die "non-member refusal must name membership"

step "mention flag without body marker refuses and publishes nothing"
expect_code 2 "mention-guard" channel post "$CH" \
    --body "handoff without the marker" --mention "$B" \
    --project "$PROJECT" --as "$A"
grep -q "requires @$B" "$ROOT/mention-guard.err" || die "refusal must name @$B"

run channel read "$CH" --project "$PROJECT" --as "$A" >"$ROOT/read-after-guard.json"
assert_jq "$ROOT/read-after-guard.json" 'length == 0'
run job list --project "$PROJECT" --as "$B" --state open >"$ROOT/jobs-after-guard.json"
assert_jq "$ROOT/jobs-after-guard.json" 'length == 0'

step "mention job: request -> claim -> result -> done"
run channel post "$CH" --body "@$B take the repro" --mention "$B" \
    --project "$PROJECT" --as "$A" >"$ROOT/post.json"
assert_jq "$ROOT/post.json" '.kind == 9 and (.mentions | length) == 1'
run job list --project "$PROJECT" --as "$B" --state open >"$ROOT/jobs.json"
assert_jq "$ROOT/jobs.json" 'length == 1 and .[0].assignee == $b' --arg b "$B"
JOB="$(jq -r '.[0].job_id' "$ROOT/jobs.json")"
[[ -n "$JOB" && "$JOB" != "null" ]] || die "job id missing"

run job claim "$JOB" --project "$PROJECT" --as "$B" >"$ROOT/claim-b.json"
assert_jq "$ROOT/claim-b.json" '.won == true and .claimer == $b' --arg b "$B"
expect_code 1 "claim-second" job claim "$JOB" --project "$PROJECT" --as "$A"
assert_jq "$ROOT/claim-second.out" '.won == false'

run job result "$JOB" --status ok --body "receipt=$CORR" \
    --project "$PROJECT" --as "$B" >"$ROOT/result.json"
run job show "$JOB" --project "$PROJECT" >"$ROOT/show.json"
assert_jq "$ROOT/show.json" '.state == "done"'

step "urgent peer mail: send -> inbox -> ack -> receipt"
run mail send --to "$B" --subject "urgent $CH" --body "@$B urgent handoff" \
    --project "$PROJECT" --as "$A" --importance urgent --ack \
    --wake tasked --corr "$CORR" >"$ROOT/send.json"
MSG="$(jq -r '.deliveries[0].payload.event_id' "$ROOT/send.json")"
[[ -n "$MSG" && "$MSG" != "null" ]] || die "message id missing"

run mail inbox --project "$PROJECT" --as "$B" --bodies >"$ROOT/inbox-b.json"
assert_jq "$ROOT/inbox-b.json" 'map(select(.id == $id and .importance == "urgent" and .ack_required == true)) | length == 1' --arg id "$MSG"

run mail ack --project "$PROJECT" --as "$B" --id "$MSG" >"$ROOT/ack.json"
assert_jq "$ROOT/ack.json" '.acknowledged == true'
run receipt list --state acked >"$ROOT/receipts-acked.json"
assert_jq "$ROOT/receipts-acked.json" '.[] | select(.corr == $corr and .state == "acked")' --arg corr "$CORR"

step "threaded reply and done mail"
run mail reply --to "$A" --subject "re: urgent $CH" --body "ack and reply" \
    --thread "$MSG" --project "$PROJECT" --as "$B" --corr "$CORR" >"$ROOT/reply.json"
run mail inbox --project "$PROJECT" --as "$A" --bodies --thread-id "$MSG" >"$ROOT/thread-a.json"
assert_jq "$ROOT/thread-a.json" 'map(select(.body_md == "ack and reply")) | length == 1'

run mail send --to "$A" --subject "[$CH] done" --body "done: $JOB" \
    --thread "$MSG" --project "$PROJECT" --as "$B" >"$ROOT/done.json"
run mail inbox --project "$PROJECT" --as "$A" --bodies --thread-id "$MSG" >"$ROOT/thread-done.json"
assert_jq "$ROOT/thread-done.json" 'map(select(.subject | contains("done"))) | length >= 1'

run receipt set --corr "$CORR" --state resulted >"$ROOT/receipt-resulted.json"
assert_jq "$ROOT/receipt-resulted.json" '.state == "resulted"'

step "flywheel supervisor queue and doorbell scope"
FLYWHEEL="${FLYWHEEL_BIN:-}"
if [[ -z "$FLYWHEEL" || ! -x "$FLYWHEEL" ]]; then
    printf 'SKIP: set FLYWHEEL_BIN to a rebuilt flywheel to test supervisor/doorbell\n'
else
    FW="$ROOT/flywheel"
    mkdir -p "$FW/home" "$FW/config" "$FW/state" "$FW/data" "$FW/state/flywheel/journals"
    (
        export HOME="$FW/home"
        export XDG_CONFIG_HOME="$FW/config"
        export XDG_STATE_HOME="$FW/state"
        export XDG_DATA_HOME="$FW/data"
        unset XDG_RUNTIME_DIR || true

        "$FLYWHEEL" supervisor fact "$PROJECT" --child "$A" --kind progress --generation 1 >/dev/null
        "$FLYWHEEL" supervisor view "$PROJECT" --json >"$FW/view.json"
        jq -e '.host_started == false and .queued_inputs >= 1 and .status == "not_started"' \
            "$FW/view.json" >/dev/null || {
            cat "$FW/view.json" >&2
            die "supervisor view must expose queued input and not_started"
        }

        printf '%s\n' \
            "{\"actions\":[],\"msg\":{\"rt_kind\":\"Watch\"},\"pins\":[\"$A\"],\"seq\":0,\"subscriber\":\"workspace:flywheel-$PROJECT-rk\",\"ts\":\"2026-11-09T00:00:00Z\",\"v\":2,\"workspace\":\"flywheel-$PROJECT-rk\"}" \
            >"$FW/state/flywheel/journals/doorbell-journal-flywheel-$PROJECT-rk.jsonl"
        "$FLYWHEEL" doorbell why "$PROJECT" "$A" >"$FW/why.out" 2>"$FW/why.err"
        grep -q "flywheel-$PROJECT-rk" "$FW/why.out" || {
            cat "$FW/why.out" >&2
            cat "$FW/why.err" >&2
            die "doorbell why did not find the canonical workspace row"
        }
        grep -q 'Watch' "$FW/why.out" || {
            cat "$FW/why.out" >&2
            die "doorbell why did not render the journaled Watch row"
        }
        ! grep -q 'flywheel-other' "$FW/why.err" || {
            cat "$FW/why.err" >&2
            die "doorbell why admitted an out-of-scope workspace"
        }
    )
fi

printf '\nPASS: toron e2e behavior matrix\n'
printf '  channel=%s job=%s mail=%s corr=%s\n' "$CH" "$JOB" "$MSG" "$CORR"
