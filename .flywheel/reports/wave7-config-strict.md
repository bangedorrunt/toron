# bd-wave7-config-strict-dck6 — toron parse-or-refuse config + freshness triple

status: done
commit: (filed at close; bead id in message)
verdict: worker-receipt (gate rows below)
terminal: done

## Knob inventory (31 names found; migrated / left)

Migrated (garbage → loud `ConfigError`, one helper per shape):
- `Config::load` (startup, all invocations): TORON_DATA_DIR, TORON_BACKEND
  (`auto|rex|superlogical|hc` via `PlaneChoice`), TORON_OPERATOR_KEY,
  TORON_ENVELOPE_TTL_DAYS (u64), TORON_PROOF_GATE_ENABLED (bool),
  TORON_PROOF_SCHEME (`schnorr|ed25519`). TORON_CONFIG / TORON_MCP_ALLOWED_HOSTS
  unchanged (path passthrough / infallible split).
- Startup pre-flight (`validate_startup_env`, in `run()`): TORON_GUARD_TIMEOUT_MS,
  TORON_COALESCE_WINDOW_MS, TORON_COALESCE_SUBWINDOW_MS, TORON_TOOL_TIMEOUT_MS,
  TORON_TUI_TIMEOUT_MS, TORON_PRESENCE_TTL_SECS, TORON_WAKE_SOCK_HANDSHAKE_MS,
  TORON_WAKE_SOCK_IDLE_MS (u64); TORON_GIT_BINARY (path);
  TORON_MCP_URL (http/https); TORON_COMMS_INBOX, TORON_GUARD_STRICT (bool).
- Use-site guards (same helpers; startup pre-empts them in the CLI, they guard
  embeddings/other binaries): `try_enabled_with` (comms), `try_daemon_url_from_env`
  + `try_tool_call_timeout` (mailbox-source), strict three-way GUARD_STRICT
  (robot), `try_new`/`try_discover` (wake_sock), `resolve_data_dir` via
  `parse_env_path` (kills the empty-string second interpretation).
- TORON_OPERATOR_NSEC: verified dead (zero code reads; README clean; catalog
  README-refusal test kept) + no-effect integration test. No code change needed.

Left (documented, not migrated):
- TORON_DEMO (presence semantics = fail-safe isolation; no valid/garbage split).
- TORON_KEY_PASSPHRASE / _FILE / TORON_OPERATOR_KEY value (secrets; wrong values
  already refuse loud at decrypt/load — refuse-at-use by nature).
- TORON_MCP_DATA_DIR, TORON_UNDER_TEST, TORON_FAULT, TORON_CRASH_CHILD_DB,
  TORON_REGEN_SHAPES (pins / test-only markers).
- TORON_PROJECT / TORON_AGENT / TORON_METRICS_FILE (already `nonblank`-strict
  at use; display-only TTL echo in tui/host.rs:391 untouched deliberately).
- TORON_WAKE_MAX_SUBSCRIBERS (toron-mcp server reader; different binary, out of
  this bead's toron-cli+core scope).
- Deep infallible readers (workflow guard/coalesce, TUI effect timeout, presence
  projection) keep signatures; garbage is pre-empted at startup, valid values
  flow byte-identical.

## Parser

`toron_core::config::{ConfigError, parse_env_opt, parse_env_bool, parse_env_u64,
parse_env_path, parse_env_string, parse_env_url}` — unset/empty → `Ok(None)`
(caller default applies); set-but-garbage → `Err(ConfigError{name, value,
accepted})` rendered as `TORON_X="v" is not valid (accepted: …)`. No new
dependency (thiserror was already in core).

## Doctor triple

New `freshness` check: `head=<sha|undiscoverable> binary=<exe>@<version>
store=<path> schema applied=<n> resolved=<m>`. Store newer than the binary
fails with the upgrade remedy; unreadable store fails with the check/restore
remedy; green triple is silent (no remedy text). Binary-vs-HEAD staleness is
deliberately NOT judged (mtimes lie — see `serve_build` docs); a live stale
daemon stays the `daemon_build` check's job.

## Tests

- `toron-cli/tests/config_strict.rs` (5 tests): 15-knob refuse/accept table via
  `doctor --json`, per-choice backend acceptance, dead-NSEC no-effect, triple
  structure, stale-store remedy (python-built sqlite with applied = known+1000).
- Mutation proof: pre-split installed binary exits 0 on `TORON_BACKEND=bogus`;
  new binary exits 1 naming accepted forms.
- Full VERIFY: workspace check clean; config_strict 5/5; doctor filter 23 suites
  ok, exit 0. Plus: toron-core lib 914/914; toron-cli full 174 passed + 1
  PRE-EXISTING failure (`config_show_emits_effective_defaults` expects default
  backend `"hc"`, code says `"auto"` since 0da17ac — stale test from the
  rex-default lane, untouched, not mine).

## Deviations

- `TORON_PROOF_GATE_ENABLED=1` now enables (was silently ignored → off). The old
  Rust-`bool`-parse accepted only `true`/`false`; the bead kills exactly this
  silencer class. Same direction for `=0|no|off` variants (now explicit).
- `enabled()`/`enabled_with` (fail-closed bool wrappers) deleted; callers use
  `try_enabled_with`. `resolve_enabled` pure table kept + tested.
- `WakeSockConfig::{new}` is now defaults-only (tests) + `try_new`/`try_discover`
  (prod); `discover` and `env_ms` removed.
- `Config::load` is now fallible (`anyhow::Result`); 6 call sites updated.

## Residual

- None from this bead. Adjacent (not acceptance): `config_show_emits_effective_defaults`
  stale default assertion (owner: rex-default lane); TORON_WAKE_MAX_SUBSCRIBERS
  strictness (toron-mcp follow-up).

PRINCIPLES: prove-it-works — VERIFY triple green + mutation proof against the
old binary, no close without the ledger row; fix-root-causes — one helper per
shape at a single interpretation site, silencers removed (not wrapped); sequence-verifiable-units —
check before tests before report, each leg evidenced in the receipt.
