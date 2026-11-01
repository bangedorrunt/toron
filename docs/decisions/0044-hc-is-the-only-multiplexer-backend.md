---
status: accepted
date: 2026-11-02
accepted: 2026-11-02
decision-makers: bangedorrunt (operator)
consulted: ""
informed: ""
supersedes: [ADR-0003 §G9 (backend clause), ADR-0007 (multiplexer row)]
related: [flywheel, herdr-core, hc, osc7501]
---

# ADR-0044: herdr-core (`hc`) Is the Only Multiplexer Backend

> **Revision 1 (2026-11-02).** Supersedes the backend clause of
> [ADR-0003](0003-nostr-mailbox-fabric.md) ("herdr default; tmux on hold") and
> the multiplexer row of
> [ADR-0007](0007-post-implementation-hardening-and-api-redesign.md)
> (`herdr | superlogical stub | tmux hold`). Everything else in those ADRs
> stands.

## Context

ADR-0003 G9 introduced a `Backend` trait over three process-supervision
surfaces: the **herdr** terminal multiplexer (TUI + socket), a **tmux** stub
that returned `Unsupported`, and a mock. Herdr's TUI owned workspaces, tabs,
and panes; its socket carried agent lifecycle.

Two things changed.

1. **herdr-core (`hc`) replaced the herdr TUI as the supervisor.** `hc` runs
   programs in persistent terminals, exposes them over one OpenRPC socket, and
   folds program status from OSC 7501. It has no workspaces, no tabs, no
   agent-kind enum, and no integration-install surface. Its unit is the
   **terminal** (`t7`), and terminals form a parent tree.
2. **The tmux stub never had a caller.** It returned `Unsupported` on every
   method; nothing in toron or flywheel ever selected it. A stub nobody can
   select is not optionality, it is a maintenance surface that reads as a
   supported backend in docs and in `backend list`.

Keeping two dead backends cost real correctness: two error vocabularies, two
"workspace" models, a `hc|tmux` selector, fixtures, and guide text that named
commands (`herdr agent prompt`, `hc integration install`) that do not exist.

## Decision

1. **`hc` is the only multiplexer backend.** The trait is
   `toron_core::backend::Backend`; the only real implementation is
   `toron_core::backend::hc::HcBackend` (plus `mock` for tests).
2. **tmux is removed, not aliased.** `backend/tmux.rs`, `BackendArg::Tmux`,
   the `hc|tmux` selector, and every fixture/guide reference are deleted. An
   old `backend = "tmux"` config is an error naming `hc`, never a silent
   mapping.
3. **The herdr TUI client is deleted.** `backend/herdr.rs` is gone;
   flywheel's `src/herdr.rs` and `ClientExt` are gone. There is exactly one
   `fn agent_prompt` and one socket client, in toron-core.
4. **A "workspace" is a root terminal plus its descendants.** The trait keeps
   the workspace/pane shape for its callers, mapped as:
   - `workspace_create` → `terminal.run` with empty `argv` (the machine's own
     shell); the workspace id **is** the root terminal address.
   - `pane_list(Some(ws))` → that terminal plus every descendant (walk
     `parent`).
   - `workspace_close` → kill and remove **every** pane in the tree,
     deepest-first. Flywheel creates crew children `outlive_parent`, so a
     root-only kill leaves the crew alive; that was a real teardown bug, not a
     cosmetic one.
   - `pane_split` → a child `terminal.run` (hc has no splits or directions).
   - `agent_start` → type the launch line into the existing pane; it does not
     create a second terminal.
5. **Status comes from OSC 7501.** `toron_core::osc7501::fold` is the single
   fold, shared with flywheel's rex backend (`FoldPolicy::Attention` or
   `RootWins`). hc's own `status`/`records` are inputs to that fold, never a
   second vocabulary.
6. **Mail identity rides terminal labels.** There are no per-pane identity
   files. A pane is stamped `toron_pin` / `toron_project` / `toron_npub`
   through `terminal.label`; `toron_pin` is the preferred mail name, with the
   terminal name as fallback.

## Consequences

- **Fewer moving parts.** One socket, one error taxonomy
  (`BackendErrorCode`), one status fold, one client shared by toron and
  flywheel.
- **hc's absences are stated, not papered over.**
  - No integration surface: flywheel's H3 preflight reads every kind
    `not_installed` and warns; it never fails a spawn. Lifecycle state comes
    from the harness's own OSC 7501 report, else process heuristics.
  - No agent sessions: `agent_session` is `None`, so `flywheel agent tail`
    bails loud instead of guessing a session file.
  - No `agent.kind`: `unsupported_agent_kind` and the kind-based prompt gate
    are gone. flywheel resolves a kind to a launch line; hc types it.
- **The TUI is a separate product.** `herdr` (the multiplexer UI) may still be
  installed, but nothing in this stack drives it. `hc` is the supervisor;
  `herdr-app` arranges windows.
- **Flywheel's plane selection is unchanged.** `auto` is still rex-primary
  with hc as the transport fallback; this ADR removes tmux and the herdr TUI
  client, it does not change orchestration.

## Alternatives considered

- **Keep tmux as an alias for hc.** Rejected: it would make a config written
  for a dead backend silently mean something else.
- **Keep the herdr TUI client behind a feature.** Rejected: two clients would
  drift on protocol pin, error mapping, and prompt semantics, and the TUI is
  no longer the supervisor.
- **Model workspaces as real hc objects.** Rejected: hc has none. Inventing a
  workspace registry would duplicate the parent tree hc already reports.

## Verification

- Code: `crates/toron-core/src/backend/hc.rs` (client, `close_workspace`,
  event decode, OSC fold wiring), `crates/toron-core/src/backend/mod.rs`
  (trait), `crates/toron-core/src/osc7501.rs` (shared fold).
- Guide: [`docs/guides/backend.mdx`](../guides/backend.mdx).
- Tests: `backend::hc` unit suite (tree close, event decode, wait semantics)
  plus the ignored live probes (`live_hc_attention_roundtrip`,
  `live_events_decode_a_status_report`). Flywheel: `hc_support::` suite,
  `wait_output_live`, `real_pane_reaches_a_detected_prompt`.
- Absence check: `rg -n 'tmux' crates/` returns only this ADR's own history
  notes and `backend/mod.rs`'s pointer here.

// governed-by: ADR-0044
