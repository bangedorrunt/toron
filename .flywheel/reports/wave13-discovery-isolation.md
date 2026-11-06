# wave13-discovery-isolation report — bd-wave13-discovery-isolation-zt1z

status: done
terminal: done

## Rule (store identity before discovery)
The default `:18080` probe is allowed ONLY when the CLI's resolved store
root matches the durable default the daemon serves. An explicitly pinned
root that resolves elsewhere skips discovery entirely (embedded + loud
note) — never cross-store, even with a live probe. Explicit
`TORON_MCP_URL` still wins unconditionally.
- CLI root precedence (mirrors embedded `build_mailbox`): `TORON_DEMO` >
  `TORON_MCP_DATA_DIR` > `TORON_DATA_DIR` > durable default. Only the
  long name existed before; the short name bypassed the pin entirely.
- The harness-injected `TORON_DATA_DIR=<sandbox root>` is the sandbox
  default, not an operator pin: it counts as explicit only when it names
  something other than the sandbox root (otherwise no test could ever
  discover).
- No daemon query needed: the default daemon serves the default root by
  construction; a same-root explicit pin is allowed through and a
  non-default explicit pin can never be the daemon's store (fail closed).
- Config `data_dir` deliberately excluded: embedded ignores it, so it
  cannot divert embedded away from the daemon's store; blocking on it
  would wrongly refuse a same-store default.

## Clean-export repro (mechanism)
`toron_command` injects short `TORON_DATA_DIR=<harness root>` into every
spawned child and the with-lease test pins long `TORON_MCP_DATA_DIR=<temp>`.
The old check read only the long name for the pin... and more importantly
the probe never compared store identity at all: any explicit temp root
with a live `:18080` discovered the operator daemon and talked to the
wrong store. The with-lease zero-match test then exits 0 (acquire succeeds
on the wrong store / refusal skipped) instead of the embedded 3.
Fix: any explicit non-default root skips discovery; embedded temp store
acquires locally and the zero-match preflight refuses with exit 3.

## Tests
- In-crate (`mailbox_source::selector_tests`, 12 pass): decide matrix —
  env wins over isolation, isolated temp skips live probe, matching root
  discovers, config wins on default store, quiet embeds; `same_store_root`
  (canonical + lexical-fallback + missing-dir cases); typed
  `DaemonUnreachable` on refused env/config/dead-default; fallback.
- New shell matrix (`daemon_discovery.rs`, 6 pass): temp root (+live
  daemon) embeds — proved by `{tmp}/index.sqlite` existing (daemon path
  never touches the temp dir) + the never-cross-store note; same for the
  short pin name; explicit env still beats isolation (typed).
- Each new test proven to fail (isolation test first asserted the old
  shape; the config test caught a shared temp-dir name between parallel
  tests — renamed to unique dirs).
- Full VERIFY green at commit: workspace check clean; daemon_discovery 6
  pass; `cli_robot reserve_with_lease_..._zero_match...` passes;
  `mailbox_source` 12 pass; operator `reserve list` (no env) exit 0.
- Pre-existing, untouched: `robot.rs:56` clippy lint (empty doc line,
  fails `-D warnings`; VERIFY does not run clippy).

## Commit
sha=607781181d867098d182080976023eee20072f9e (1 commit, bead id in message)

## Residual
- None.

## PRINCIPLES cited
- fix-root-causes — reproduced the cross-store shape first (short-name
  bypass + no identity check), then fenced the resolver by store identity
  instead of silencing the probe or trusting the port alone.
- prove-it-works — the exact failing test + fake-prober matrix + live
  operator probe at the commit sha; gate+ledger close below.
