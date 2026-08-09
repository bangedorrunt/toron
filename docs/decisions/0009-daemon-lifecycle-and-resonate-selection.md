# ADR-0009: Daemon Lifecycle, Autostart, and Resonate Server Selection

**Status:** Accepted (2026-08-09)
**Scope:** toron-cli operator surface, toron-workflow engine bootstrap, ops
**Supersedes:** the ad-hoc `com.flywheel.toron` launchd agent (2026-08-09)
**Implements:** bead-driven request "toron daemon start/stop/restart + autostart + resonate default server"

## Context

The toron mailbox is a **durable coordination substrate** — agents, flywheel,
and the operator all depend on the `serve --mcp-http` daemon being up. Until
now the daemon was booted ad-hoc: a hand-written launchd plist under the
flywheel label (`com.flywheel.toron`) with the passphrase **in plaintext in
the plist**, a debug-binary path, and flywheel-owned log paths. There was no
CLI to manage the lifecycle (`launchctl` only), and no autostart story on
non-macOS hosts.

Separately, the workflow engine ran **embedded** (`WorkflowEngine::local`) in
production — cross-process crash survival (SC-11) was only exercisable in
tests. A durable Resonate server exists both locally
(`http://localhost:8001`, the `com.flywheel.resonate` launchd service) and on
a Tailscale node (`https://resonate.alpine-tilapia.ts.net`). Requirement:
default to the **remote** server, fall back to **local**, and pin via env when
an operator wants a specific target.

## Decision

### 1. `toron daemon` subcommand group (toron-cli)

`toron daemon start|stop|restart|status|install|uninstall` — one surface for
daemon lifecycle on every host. Supervision strategy, per host:

| Host | Mechanism | Unit location |
| :--- | :--- | :--- |
| macOS | launchd LaunchAgent (`launchctl bootstrap/bootout/kickstart`) | `~/Library/LaunchAgents/<label>.plist` |
| Linux | systemd user unit (`systemctl --user`) | `~/.config/systemd/user/<label>.service` |
| other | pidfile + detached `serve --mcp-http` spawn | `~/.local/state/toron/toron.pid` |

- Default label `com.toron.serve`; default addr `127.0.0.1:18080` (the
  flywheel `[mail.toron]` URL — the daemon keeps serving the same endpoint
  after the migration).
- `status` detects via launchctl/systemctl first, then pidfile+TCP probe,
  and distinguishes a **managed** instance from an **unmanaged** one
  (`DaemonState::RunningUnmanaged`). `stop` refuses to kill an unmanaged
  instance (no orphaned-mailbox kills).
- `install` renders the unit from the **current binary** (`current_exe`),
  sets `RunAtLoad`/`KeepAlive`/`Restart=always`, and writes logs to
  `~/.local/state/toron/serve.{out,err}.log` (toron-owned, not flywheel's).
- `restart` = stop + start; all operations idempotent.

### 2. Secret handling: passphrase file, never the unit

The passphrase must not appear in plist/systemd files (plists are
user-readable; `launchctl print` surfaces EnvironmentVariables). Resolution
order in `toron_core::config::operator_passphrase()` (new):

1. `TORON_KEY_PASSPHRASE` (dev/CI pin; unchanged)
2. `TORON_KEY_PASSPHRASE_FILE` (a path; trimmed file content)
3. first existing `$XDG_CONFIG_HOME/toron/passphrase` / `~/.config/toron/passphrase`
4. `"toron"` dev default (backward compatible)

`toron daemon install` writes the passphrase file (mode **0600**) and the unit
sets only `TORON_KEY_PASSPHRASE_FILE` — the secret never reaches launchd's
environment. `key generate` and `serve` share the same resolution, so a
freshly-generated operator key decrypts at serve time.

### 3. Resonate server selection (toron-workflow)

Default to the remote durable server, fall back to local, then embedded:

1. `RESONATE_URL` env — explicit pin (operator override)
2. `https://resonate.alpine-tilapia.ts.net` — TCP-probed (2s budget)
3. `http://localhost:8001` — TCP-probed
4. `None` — embedded `WorkflowEngine::local` (dev/tests; unchanged semantics)

Resolution is a pure `resonate_server_url_with(probe)` core (unit-tested with
fake probes) + a `OnceLock`-cached `resonate_server_url()`. Production
`WorkflowHost::new` uses remote-when-available; `over_bus` (test embedding)
stays local. Cross-host crash survival is now the default posture, not a
test-only feature.

### 4. Migration

`com.flywheel.toron` (old label, inline passphrase, flywheel-owned logs) is
retired: `toron daemon install` + bootout of the old label + removal of the
stale plist. The endpoint (127.0.0.1:18080) and therefore flywheel's
`[mail.toron]` config are unchanged.

## Consequences

- **Operators** get one command for lifecycle + autostart on macOS/Linux;
  `toron doctor` and `toron status` remain the health surfaces.
- **Secrets**: the passphrase is no longer readable via `launchctl print`;
  the file is 0600 and owned by the operator user (single-operator,
  local-first threat model — ADR-0007 D3).
- **Durability**: workflow runs survive daemon restarts and can resume
  cross-host via the remote Resonate server. A dead remote degrades to local
  (same guarantees as before this ADR) or embedded.
- **No unsafe**: host supervision shells out to `id -u` / `kill -0` /
  `kill` instead of raw libc — the crate-level `#![forbid(unsafe_code)]`
  (ADR-0003) is preserved.

## Verification (executable)

```bash
cargo test -p toron-cli daemon::                     # plist/systemd/pidfile/state units
cargo test -p toron-core --lib config::passphrase    # resolution-order units
cargo test -p toron-workflow --lib server_url        # probe/parse units
toron daemon install && toron daemon start           # launchd install + boot
toron daemon status                                  # → running (launchd com.toron.serve)
lsof -nP -iTCP:18080 -sTCP:LISTEN                    # daemon owns the port
flywheel mail agents toron                           # privileged surface still serves
ls -la ~/.config/toron/passphrase                    # 0600, no secret in plist
```

All verified GREEN 2026-08-09.
