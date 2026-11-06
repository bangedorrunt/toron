# wave8-daemon-discovery report — bd-wave8-daemon-discovery-nxzx

status: done
terminal: done

## Resolver (crates/toron-cli/src/mailbox_source.rs)
- `SourceDecision::{DaemonExplicit, DaemonDefault, EmbeddedPinned,
  EmbeddedXdg}` — the old `RefuseSecondStore` arm is gone; a live default
  port DISCOVERS the daemon instead of refusing it (the reported bug).
- `ResolveInputs{env_url, data_dir_pinned, config_url, default_probe}`
  + `pub resolve_with(inputs)` — the injectable seam (fake probe, no env,
  no live daemon in tests). `resolve()` gathers real inputs and delegates.
- Materialize: explicit URL → `DaemonClient::connect` or loud typed error
  (unchanged); `DaemonDefault` → connect, and on the probe-to-connect race
  falls back to embedded with a loud stderr note (never refuses);
  quiet port → embedded (unchanged).
- Root fix inside `connect` (fix-root-causes): rmcp wraps the transport
  error message-only so the `is_refused` chain walk never saw the RST —
  now a bounded first-hand TCP open (`refused_now`, 200ms) classifies a
  definite refusal at the origin. Only definite-RST short-circuits;
  timeouts/DNS fall through to the normal handshake path. `is_refused`
  stays as a second net for other transports.
- All `resolve()` callers (robot, plane, mail inbox, reserve/slot/contact,
  tui, with_lease) inherit the behavior — no call-site edits needed.
  Loopback/remote guards (bd-d41q) and the contradiction warning unchanged.

## Tests
- In-crate (`mailbox_source::selector_tests`, 4 new): explicit-dead URLs
  (env + config arms) → `DaemonUnreachable{Refused}` naming the endpoint
  with the remedy; quiet probe → embedded with no socket touched;
  pin beats live probe; discovered-default-that-dies → embedded fallback.
- New `toron-cli/tests/daemon_discovery.rs` (4, shell-out, per-child env):
  env wins + typed, config wins + typed (sandbox HOME), contradiction
  warns-but-daemon-wins, no-config never refuses (exit 0).
- Each new test proven to fail (typed downcast missed pre-fix; no-refusal
  arm reproduced the live refusal pre-fix; count/shape assertions).
- Full VERIFY green: workspace check clean; `test -p toron-cli --test
  daemon_discovery` 4 pass; mailbox_source 10 pass; operator probe
  `env -u TORON_MCP_URL toron reserve list --project flywheel` exit 0
  (was the refusal error before the fix).
- Pre-existing, untouched: `cli_operator` backend_status/backend_list
  (fail at clean HEAD too — environment, proven via stash), `robot.rs:56`
  clippy lint (not my paths).

## Commit
sha=d705525a35f017b7539770f02d76db6eef330907 (1 commit, bead id in message)

## Close
- `br gate report --gate unit-test-verified` (leading `sha=` + `cmd/exit/tail/receipt`; `tail` points at the receipt file per the command-proof rule) + `br close`.
- `flywheel ledger check bd-wave8-daemon-discovery-nxzx`: LEGAL (sha d705525a…0907).
- Lesson (same class as wave2): `tail=` must be an existing non-empty file, not a prose slug — first gate row recorded, ledger rejected, re-recorded, LEGAL.

## Residual
- None.

## PRINCIPLES cited
- fix-root-causes — reproduced the live refusal first; traced the missed
  classification to rmcp's message-only wrap and fixed it with a
  first-hand probe instead of silencing or string-matching.
- prove-it-works — fake-prober matrix + in-crate typed assertions + the
  live operator probe at the commit sha; gate+ledger close below.
