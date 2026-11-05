---
status: accepted
date: 2026-11-06
accepted: 2026-11-06
decision-makers: bangedorrunt (operator)
consulted: ""
informed: ""
supersedes: [ADR-0044 §1 (hc is the only multiplexer backend)]
related: [ADR-0044 §4-6, ADR-0003 §G9, ADR-0031, flywheel ADR-0012, rex, hc]
---

# ADR-0045: rex Is the Default Pane Plane, hc the Transport Fallback

> **Revision 1 (2026-11-06).** Supersedes only **§1** of
> [ADR-0044](0044-hc-is-the-only-multiplexer-backend.md) ("hc is the only
> multiplexer backend"). ADR-0044 §2 (tmux removed, not aliased), §3 (herdr TUI
> client deleted), §4 (a workspace is a root terminal plus descendants), §5
> (OSC 7501 is the status source), and §6 (mail identity rides labels) all
> stand. Operator word: "review proposal … then implement your plan".

## Context

ADR-0044 collapsed the backend surface to one implementation because the other
two had no caller. That was right for tmux and the herdr TUI, and wrong about
**which** single backend: the pane plane on this machine is rex (Superlogical),
with hc as the transport that remains when rex is not installed. Flywheel
already ran that shape — `auto` = rex primary, `FallbackBackend` on hc — but the
selection logic lived **above** toron, in `../flywheel/src/backend/`.

That placement is backwards for the dependency rule
(`flywheel → toron-workflow → toron-core`). toron's own surfaces (mailbox
`whois`/`list_agents`, MCP `resolve_pane_identity`, `toron doctor`, `toron agent
bootstrap`) each hard-coded `HcBackend::connect(None)`, so the default plane and
the callers disagreed: flywheel could be running on rex while toron's identity
reads asked an hc socket that was not there.

The proposal was reviewed against the effect-oriented law, the elm-effect
carrier law, and the Rust API guidelines before implementation. Three findings
changed the plan:

1. **A decision that reads the world is not a pure decision.** Flywheel's
   `resolve_plane(cfg)` called `rex_cli_present(cfg)` inline — a filesystem and
   `PATH` read buried in the decision. The resolver could not be tested without
   also testing the host. The plane table must be a pure function of a probed
   **snapshot**.
2. **`auto` must not build a primary it already knows is absent.** The old
   shape built a rex primary unconditionally and fell back per call, so a host
   without rex paid a failed spawn on every call and learned why only from a
   one-line warning. Resolving `auto → hc` up front removes the dead path and
   names the reason once.
3. **A fallback that replays a write after a timeout double-applies it.** The
   old `FallbackBackend` replayed **every** method on `Connect | Io | Timeout`.
   A timed-out `pane.split`, `workspace.create`, or `agent.prompt` may already
   have committed at the provider; replaying it on a second plane creates a
   second pane or sends the prompt twice, and these calls carry no idempotency
   key. This is the effect law's "record intent before the world changes"
   boundary, and the old policy crossed it.

## Decision

1. **`rex` is the primary pane plane; `hc` is the transport fallback.** The
   default is `auto`: rex when its CLI is present, hc otherwise, with the
   reason recorded. `TORON_BACKEND` (and `.toron.toml`'s `backend`) select
   `auto | rex | superlogical | hc`; the default when unset is `auto`.
   `superlogical` stays as an alias of `rex` because flywheel spells it that
   way.
2. **The resolver lives in `toron_core::backend::plane`.** It is a pure
   function `resolve(&PlaneEnv) -> ResolvedPlane` over two inputs: the parsed
   [`PlaneChoice`] and a [`RexCli`] probe snapshot. `RexCli::detect` is the only
   filesystem/`PATH` read, and it runs at the edge. No new dependency edge is
   created; flywheel may delete its copy and re-export this one.
3. **One construction site.** `plane::connect(&PlaneEnv)` builds the backend
   the table names and is the only place that decides between
   `HcBackend`, `SuperlogicalBackend`, and `FallbackBackend`.
   `plane::connect_default()` is the same function over the process
   environment, for callers that hold no `Config` (mailbox, MCP).
4. **The fallback contract is per effect, not per method list.** Attached only
   to `auto` on the rex plane:
   - **read** (`workspace_list`, `pane_list`, `agent_list`, `agent_read`,
     `subscribe`): replay on `Connect | Io | Timeout`;
   - **bounded wait** (`agent_wait`, `pane_wait_output`): replay on
     `Connect | Io`, because its timeout is the caller's answer and a replay
     would spend the budget twice;
   - **write** (`workspace_create`, `workspace_close`, `pane_split`,
     `pane_close`, `pane_run`, `agent_start`, `agent_rename`, `prompt`,
     `prompt_coded`, `agent_send_keys`): replay on `Connect` **only** — the one
     code that proves nothing was dispatched.
   `Unsupported`, semantic rejects, and malformed replies never replay: they
   are verdicts about the call, not transport faults.
5. **An explicit pin never falls back.** `TORON_BACKEND=rex` (or
   `superlogical`) with no rex CLI is an error at construction, naming the
   missing path. An unknown name (`tmux`, a typo) is an `Unsupported` error
   naming `auto | rex | hc`; it never becomes a silent hc ride.
6. **Identity reads ride the plane, not hc.** `resolve_pane_identity`,
   `cleanup_pane_identities`, the live pin→pane map, and the MCP pane tools take
   `&dyn Backend` and construct through `plane::connect_default()`. hc keeps
   `terminal.label` tokens; rex carries the pin as the block label from
   creation, so a bootstrap stamp is a no-op there instead of a failed socket
   open.
7. **The probe is presence-only.** `RexCli::detect` never executes rex.
   `--autostart` defaults to true, so even `rex whoami` would start a server;
   a status probe must not have that effect.
8. **`hc` remains a first-class selection.** `TORON_BACKEND=hc` bypasses the
   probe entirely (no rex attempt, no fallback line). The fallback is a
   transport contract, not a demotion of hc.

## Consequences

- **toron and flywheel derive the same plane from the same table.** The
  flywheel migration is done: its `src/backend/fallback.rs` and local
  `Plane`/`ResolvedPlane`/probe are deleted, and `src/backend/factory.rs`
  re-exports the toron-core types, supplies only its configured name and
  `[backend] socket_path` (via `PlaneEnv::with_hc_socket`), and constructs
  through `plane::connect`. `FLYWHEEL_BACKEND` accepts
  `auto|rex|superlogical|hc`.
- **A write never silently lands twice.** The cost is that a `Timeout` on a
  write surfaces instead of retrying on hc. That is the honest reading: the
  caller cannot know whether the pane exists, so a retry is a guess with a
  duplicate as its failure mode.
- **A host without rex behaves exactly as before** (hc serves), minus the
  per-call failed spawn and with the reason printed once.
- **`toron backend status` reports the derivation**, not just the configured
  name: `serving`, `rex_reachable`, `fallback`, `reason`. `toron backend list`
  reports both planes with their role.
- **The rex gaps ADR-0044 recorded are closed or explicitly refused.**
  `subscribe` resolves each pane's session and passes `-s`, so a crew's stream
  follows the crew's session; a pane set spanning sessions (or an empty set) is
  refused with a pointer to `subscribe_session`. `workspace_close` refuses
  `close_group=false` because `session kill` has one scope — widening it
  silently would violate the same don't-do-more-than-asked rule as a replayed
  write. `agent_list` is still N+1 subprocess calls, but is now bounded:
  `AGENT_SCAN_CAP` (512 blocks) and a 3 s status budget, checked between
  blocks; past either bound the block is listed with `Unknown`, which cannot
  prove completion.

## Alternatives considered

- **Keep selection in flywheel and re-export from toron.** Rejected: it inverts
  the dependency rule and leaves toron's own surfaces unable to name the plane.
- **Resolve `auto` by spawning `rex whoami`.** Rejected: `--autostart` defaults
  true, so a probe would start a server. Presence of an executable is the
  contract.
- **Replay writes on `Io`.** Rejected: a mid-stream `Io` may follow a committed
  request. Only `Connect` is provably pre-dispatch.
- **Make `hc` the `auto` primary and rex the fallback.** Rejected: the pane
  plane on this machine is rex, and hc is the compatibility transport.

## Verification

- Code: `crates/toron-core/src/backend/plane.rs` (pure table + probe +
  single construction site), `crates/toron-core/src/backend/fallback.rs`
  (per-effect replay policy), `crates/toron-core/src/mailbox/{identity,surface,
  messaging}.rs`, `crates/toron-mcp/src/tools/system.rs`,
  `crates/toron-cli/src/{operator,config,robot}.rs`.
- Tests: `backend::plane::plane_tests` (`auto` both ways, explicit pin never a
  fallback, unknown → invalid, total parsing, probe honors `REX_BIN_DIR`),
  `backend::fallback::fallback_tests` (read replays a timeout, write does not,
  write replays a refused connection, wait does not replay, unsupported never
  replays, absent fallback preserves the error, `name() == "auto"`),
  `backend_selection_tests` (unknown name refused, `hc` and `auto` validate),
  `config::tests::default_backend_is_auto`.
- Bound and scope: `pane_identity_tests::the_roster_sweep_is_bounded_by_cap_and_time`,
  `a_pane_set_must_name_exactly_one_session`,
  `a_narrow_workspace_close_is_refused_not_widened`, plus the adapter's
  label→token tests.
- Live (opt-in): `pane_identity_tests::live_rex_round_trip_labels_a_block_and_resolves_identity`
  (`--ignored`); it creates a throwaway rex session, proves the label→pin
  mapping and the roster entry, and asserts the session is gone afterward.
- flywheel: `mbx build` and `mbx test --bin flywheel backend::` (the factory
  tests cover `auto` with and without a CLI, a pinned missing CLI refusing,
  the typo refusal naming the choices, and the spawn gate).
- Guide: [`docs/guides/backend.mdx`](../guides/backend.mdx).

// governed-by: ADR-0045
