# wave5-timespec report — bd-wave5-timespec-errors-euqa

status: done
terminal: done

## Type / API
- New `toron-core::time_spec`: `TimeSpec::{Rfc3339(u64), Relative(Duration),
  Last}`, `TimeSpecError::{Unparseable, NoHighWater}` (Display names the
  accepted forms), `parse` (RFC3339 | `30s/15m/6h/7d` | `last`, trimmed,
  lowercase-exact), `resolve_since(now, water)` (saturating; `Last` without
  water is an explicit error, never 0), `resolve_since_arg` (None→None,
  `last`→`"last"`, RFC3339 byte-identical, relative→resolved RFC3339).
- Unit tests in-module: accepted/rejected matrix, resolve semantics,
  round-trip shapes (4 tests green).

## Edges adopted
- `plane.rs` `parse_since` (channel read): TimeSpec + `resolve_since(now,
  None)` — relative works; `last` fails loudly (no cursor on this path).
- `robot.rs` inbox embedded + daemon arms: `resolve_inbox_since` helper;
  `last` passes through for the daemon cursor, RFC3339 byte-identical,
  relative resolved client-side, garbage fails before any store is touched.
- Help text updated (inbox + channel read) to document relative forms and
  the `last` limitation.
- Examined, left unchanged: `MailSearchArgs.since` (already numeric µs),
  `ThreadSummarizeArgs.since` (flag exists but robot never forwards it —
  pre-existing gap, not this bead), `AckSlaArgs.sla_secs` (a duration, not
  a time point), `NextArgs` (no --since).

## Typed errors at origin
- `StoreSkew { binary: u32, store: u32 }` (`index/schema.rs`, `Error` impl,
  Display keeps `toron-error=store_skew`): `map_migrate_error` wraps it
  UNDER the legacy message via `.context()` — CLI text byte-identical,
  `downcast::<StoreSkew>()` proven by new test. Existing skew tests green.
- `DaemonUnreachable { endpoint, kind: Refused | Timeout{tool?, secs} }`
  (`mailbox_source.rs`, Display keeps today's remediation verbatim):
  connect-timeout, connect-refused (classified by `io::ErrorKind` in the
  anyhow chain — type match, never prose grep), call-timeout. Unit tests:
  classifier true/false/negative, Display byte-equality ×3.

## Tests
- `test -p toron-core time_spec`: 4 passed.
- `test -p toron-cli --test time_spec` (new, 5 tests): inbox + channel-read
  accept `6h`, reject `6hours` with the accepted forms; `last` rejected on
  the channel path with the cursor reason. All pass.
- `test -p toron-cli --bins`: 145 passed. `cli_robot` (inbox shapes): 48
  passed. `mailbox::reservations`: 62 passed.
- Pre-existing flake (not mine): one reservations heartbeat test failed
  once under full-workspace parallel load, passes alone and in-suite;
  no shared code with this diff.

## Commit
sha=7919751056be338eca1702918d5268e2e30e739c (1 commit, bead id in message)

## Residual
- None. Flywheel's string matching on daemon/store text is a later bead
  (text preserved byte-identical, so nothing breaks meanwhile).

## PRINCIPLES cited
- fix-root-causes — one temporal type, typed failures at the origin
  (TimeSpec centralizes parsing; StoreSkew/DaemonUnreachable ride the
  anyhow chain instead of grep-able strings).
- prove-it-works — accepted/rejected matrix + typed assertions
  (unit matrices, downcast proof, CLI accept/reject integration tests,
  ledger LEGAL to follow).
