#!/usr/bin/env bash
# Live fixture for comms Phase 4.1 (roles + authority at append).
# Temp XDG, spare-port daemon (never :18080), every other TORON_*/FLYWHEEL_* var unset (env -i).
set -u
WT=$(/usr/bin/git -C "$(dirname "$0")" rev-parse --show-toplevel)
TORON=$WT/target/debug/toron
LOGS="$(dirname "$0")"
PORT=19873
T=$(mktemp -d /tmp/toron-fx-roles.XXXXXX)
export_env() { :; }
base_env=(PATH="$PATH" HOME="$T" XDG_DATA_HOME="$T/xdg-data" XDG_STATE_HOME="$T/xdg-state" XDG_CONFIG_HOME="$T/xdg-config" TORON_DATA_DIR="$T/data" TORON_OPERATOR_KEY="$T/operator.ncryptsec" TORON_MCP_URL="http://127.0.0.1:$PORT/mcp")
on=(env -i "${base_env[@]}" TORON_COMMS_INBOX=1)
off=(env -i "${base_env[@]}")
compact() { python3 -c 'import sys,json
raw=sys.stdin.read()
try: print(json.dumps(json.loads(raw)))
except Exception: print(raw.strip())'; }
run() { # run LABEL ENVARRAYNAME args...
  local label="$1" which="$2"; shift 2
  local -n e="$which"
  echo; echo "### $label"
  echo "\$ toron $* --json   [flag: $([ "$which" = on ] && echo TORON_COMMS_INBOX=1 || echo unset)]"
  local out; out=$("${e[@]}" "$TORON" "$@" --json 2>"$T/stderr"); local rc=$?
  echo "$out" | compact
  [ -s "$T/stderr" ] && sed 's/^/stderr: /' "$T/stderr"
  echo "exit: $rc"
}
env_file() { python3 "$LOGS/mkenv.py" "$@"; }

echo "# toron comms Phase 4.1 live fixture"
echo "# build: $($TORON --version 2>&1 | head -1) @ $(/usr/bin/git -C $WT rev-parse --short HEAD)"
echo "# root: $T (temp XDG)   daemon: 127.0.0.1:$PORT (spare; :18080 is never contacted)"
"${off[@]}" "$TORON" key generate --role operator --out "$T/operator.ncryptsec" --json > "$T/key.json" 2>/dev/null
OP=$(python3 -c "import json;print(json.load(open('$T/key.json'))['npub'])")
echo "# operator npub (hex): $OP"
CAP=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc01
SUP=5555555555555555555555555555555555555555555555555555555555555501
W1=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01
W2=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02
P=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee01
V=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01
X=9999999999999999999999999999999999999999999999999999999999999901
CTX=ctx-swarm

# The spare-port daemon (single, throwaway). The comms verbs open the durable root directly;
# the daemon is here so TORON_MCP_URL points at it and the real :18080 daemon cannot be reached.
("${off[@]}" "$TORON" serve --mcp-http "127.0.0.1:$PORT" > "$T/daemon.log" 2>&1 &) ; DPID=""
for i in $(seq 1 40); do [ "$(curl -s -o /dev/null -m 1 -w '%{http_code}' "http://127.0.0.1:$PORT/mcp")" != 000 ] && break; sleep 0.5; done
echo "# spare-port daemon: GET :$PORT/mcp -> HTTP $(curl -s -o /dev/null -m 2 -w '%{http_code}' "http://127.0.0.1:$PORT/mcp") (axum answers 4xx on a bare GET: it is up)"

echo; echo "## A. comms flag ON"
env_file $T/e1.json role $X 0 $X $CTX crew 0
run "A1 bootstrap: a non-operator cannot start a context (first fact must be the operator's captain)" on inbox append --file $T/e1.json
env_file $T/e2.json role $OP 0 $CAP $CTX captain 0
run "A2 operator signs role{captain} (bootstrap)" on inbox append --file $T/e2.json
env_file $T/e3.json role $CAP 0 $SUP $CTX supervisor 0
run "A3 captain makes SUP supervisor" on inbox append --file $T/e3.json
n=0
for spec in "$W1:crew" "$W2:crew" "$P:peer" "$V:verifier"; do
  who=${spec%%:*}; role=${spec##*:}
  env_file $T/s$n.json role $SUP $n $who $CTX $role 0
  run "A4.$n supervisor makes ${role} ${who:0:6}…" on inbox append --file $T/s$n.json; n=$((n+1))
done
env_file $T/e5.json role $W1 0 $W1 $CTX captain 9
run "A5 a crew cannot mint itself captain (role fact from a non-issuer is refused)" on inbox append --file $T/e5.json
env_file $T/e6.json role $SUP 4 $X $CTX supervisor 0
run "A6 a supervisor cannot mint a supervisor" on inbox append --file $T/e6.json
env_file $T/e7.json task $W1 0 $W2 $CTX t-x
run "A7 crew TASK refused with typed Unauthorized + expected set" on inbox append --file $T/e7.json
env_file $T/e8.json verdict $W1 1 $CAP $CTX t-x
run "A8 crew VERDICT refused" on inbox append --file $T/e8.json
env_file $T/e9.json task $P 0 $W1 $CTX t-x
run "A9 peer TASK refused" on inbox append --file $T/e9.json
env_file $T/e10.json verdict $P 1 $CAP $CTX t-x
run "A10 peer CONTROL (verdict) refused" on inbox append --file $T/e10.json
env_file $T/e11.json notify $V 0 $CAP $CTX
run "A11 out-of-role verb: verifier NOTIFY refused, expected set = its only verb" on inbox append --file $T/e11.json
env_file $T/e12.json notify $X 0 $CAP $CTX
run "A12 sender with no role in a governed context refused (role: null)" on inbox append --file $T/e12.json
env_file $T/e13.json task $SUP 4 $W1 $CTX t-1
run "A13 supervisor TASK allowed" on inbox append --file $T/e13.json
env_file $T/e14.json verdict $SUP 5 $CAP $CTX t-1
run "A14 supervisor cannot VERDICT its own task" on inbox append --file $T/e14.json
env_file $T/e15.json verdict $V 0 $CAP $CTX t-1
run "A15 verifier VERDICT allowed" on inbox append --file $T/e15.json
env_file $T/e16.json notify $W1 0 $CAP $CTX
run "A16 crew NOTIFY allowed (its earlier refusals A5/A7/A8 consumed no seq: seq 0 is still next)" on inbox append --file $T/e16.json
env_file $T/e17.json task $CAP 1 $V $CTX t-v
run "A17 captain tasks the verifier" on inbox append --file $T/e17.json
run "A18 verifier reads its inbox" on next --project fx --as $V
VKEY="$CAP:0:1"
run "A19 verifier accepts the task" on ack "$VKEY" accept --project fx --as $V
run "A20 verifier DONE refused (verifier may never done)" on done t-v --evidence x --project fx --as $V
run "A21 crew reads its inbox" on next --project fx --as $W1
run "A22 crew accepts the supervisor's task" on ack "$SUP:0:4" accept --project fx --as $W1
run "A23 crew DONE allowed" on done t-1 --evidence cargo-test --project fx --as $W1
env_file $T/e18.json task $W1 1 $W2 $CTX t-y
run "A24 outbox path: 'inbox send' of a refused crew task" on inbox send --project fx --as $W1 --file $T/e18.json

echo; echo "## role projection (sqlite, read-only)"
if command -v sqlite3 >/dev/null; then
  sqlite3 -readonly -header -column "$T/data/index.sqlite" "select substr(context,1,9) context, substr(npub,1,8) npub, role, gen, substr(issuer,1,8) issuer from inbox_role order by role, npub" 2>&1 | sed 's/^/  /' || true
fi

echo; echo "## B. same refused sequence with the comms flag OFF (unset, then =0): today's behaviour"
run "B1 role{captain} append, flag unset" off inbox append --file $T/e2.json
run "B2 crew task append, flag unset" off inbox append --file $T/e7.json
echo; echo "### B3 flag =0"
echo "\$ TORON_COMMS_INBOX=0 toron inbox append --file e7.json --json"
env -i "${base_env[@]}" TORON_COMMS_INBOX=0 "$TORON" inbox append --file $T/e7.json --json 2>&1 | head -3; echo "exit: ${PIPESTATUS[0]}"

echo; echo "## C. ungoverned context is untouched (flag ON)"
env_file $T/c1.json task $X 0 $W1 ctx-legacy t-l
run "C1 a role-less sender tasks in a context with no role fact" on inbox append --file $T/c1.json

pkill -f "toron serve --mcp-http 127.0.0.1:$PORT" 2>/dev/null
echo; echo "# done; temp root $T removed"
rm -rf "$T"
