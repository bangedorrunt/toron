# Graceful toron daemon stop: design (bd-slkn slice 1)

## Current state (measured 2026-09-29)

- Daemon stop is SIGTERM via bootout/systemctl/pidfile (`toron-cli/src/daemon.rs`
  `stop`). No `tokio::signal` handler, no graceful hook, no JoinHandle
  retention anywhere in `toron-mcp/src/server.rs`.
- Trigger loops (`spawn_trigger_loop`, `spawn_mail_trigger_loop`) are
  fire-and-forget: handles dropped at the spawn sites (`server.rs:459-460`).
  SIGTERM kills them mid-flight.
- HTTP serves via bare `axum::serve` (no graceful mode).

## Already built (enablers, landed)

- Both loop exits persist delivery cursors (bd-8x2o): subscription close ends
  `run_trigger_loop`, channel close ends the mail loop; tails call
  dirty-gated `persist_delivery_cursors` (warn-only).
- So today a *clean* loop end is fully covered. Only SIGTERM death loses the
  post-tick window (duplicates only; loss already impossible).

## Proposed design

1. `tokio::signal` for SIGTERM/SIGINT in the serve entrypoint.
2. On signal: close the trigger subscription first (run loop returns →
   persists), drop the mail tx (mail loop ends → persists), then
   `axum::serve(...).with_graceful_shutdown` with a bounded timeout.
3. Retain the two JoinHandles (spawn fns already return them) and join with
   a budget shorter than the supervisor kill timeout (launchd ExitTimeOut /
   TimeoutStopSec); second signal exits immediately.
4. No in-flight publish completion: at-least-once on transport ambiguity
   stays (documented in `CoalescePublishError::Transport`).

## Risks

- Supervisor SIGKILL after its own timeout makes any budget above it a lie;
  keep the join budget strictly below.
- Double-signal must exit fast, never deadlock on a hung persist (persist is
  warn-only, but the join still needs its own timeout).

## Acceptance (for implementation slices)

Stop-then-reload round trip: marks set, SIGTERM, reboot on same store, no
redelivery of pre-signal marks. Existing `cursor_stop_persist` stays green.

## Non-goals

No new MCP surface, no flags, no in-flight completion guarantees.
