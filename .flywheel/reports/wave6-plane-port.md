# Wave 6 / Lane B report — invert the pane-plane dependency (port)

bead: bd-wave6-plane-port-8udj (toron tracker)
status: done
terminal: done
toron commit: a1fb7d9ff287d85d398627e46542c87b10307d18 (plane_port.rs new + 7 files; bead id in message)
receipt: .flywheel/receipts/bd-wave6-plane-port-8udj-b245f72f8669.verify.txt (VERIFY green)

## Reach inventory (6 sites, 4 files — all converted)

| Site | Fact used | Port member |
|------|-----------|-------------|
| mailbox/messaging.rs authorize_ack | `connect_default` + `agent_list` (same-pane arm) | `agent_list`, via injected `Mailbox.plane` |
| mailbox/identity.rs live_pin_pane_map | `connect_default` + `pane_list` + conditional `agent_list` | both lists, via new `plane` param |
| mailbox/identity.rs resolve_pane_identity | `&dyn Backend` param, `pane_list` + `agent_list` | `&dyn PanePlane` |
| mailbox/identity.rs cleanup_pane_identities | `&dyn Backend` param, same two calls | `&dyn PanePlane` |
| mailbox/surface.rs ×2 wrappers | per-call `connect_default` | per-call `LivePanePlane::connect_default` (timing identical) |
| osc7501.rs | `AgentStatus` type import only | re-export via `plane_port` |
| toron-mcp tools/system.rs ×2 | per-call `connect_default` + `&dyn Backend` args | per-call `LivePanePlane::connect_default` |

No site needed anything broader: the kernel only ever lists panes/agents.

## Port surface (crates/toron-core/src/plane_port.rs)

- `pub trait PanePlane: Send + Sync` — `pane_list()`, `agent_list()` over
  `anyhow::Result` (no backend error-type leak).
- Vocabulary re-exports: `AgentInfo`, `PaneInfo`, `TOKEN_*`, `AgentStatus`
  (kernel names `crate::plane_port`, never `crate::backend`).
- `LivePanePlane` (wrap a `Box<dyn Backend>`; `connect_default()` mirrors the
  old call sites incl. degrade-as-absent) + `MockPanePlane` (cfg(test) vecs).
- Backend is now a leaf: the only core-internal edges into `crate::backend`
  originate in `src/backend/**` itself (implementation) and `plane_port.rs`
  (the seam). `rg crate::backend` outside those: zero.

## Injection plan (as built)

- `Mailbox` gains `plane: Option<Arc<dyn PanePlane>>`; `new()` keeps its
  signature and defaults to live-at-construction (`None` when unreachable —
  same degrade as the old per-call failures); `with_plane()` overrides for
  tests/embedders. All ~20 existing `Mailbox::new` callers compile unchanged
  (flywheel untouched — it never called the three changed methods directly;
  its facade goes through `MailSurface`, whose shapes are unchanged).

## Tests

- `plane_port::port_tests` (3): mock serves canned data; unreachable fails
  both lists; real `LivePanePlane` is wired behind the port (compile proof,
  zero ambient dependence — the live-rex round-trip stays in the existing
  ignored tests).
- `identity::tests` (2): stamped mock pane enriches `live_pin_pane_map`;
  unreachable/`None` map nothing; `resolve_pane_identity` resolves/misses
  from the injected plane (also proves `with_plane` stores the instance).
- `messaging::tests` (1): `authorize_ack` admits same-pane pair and refuses a
  stranger on another pane with no thread role, both through the mock.
- Full `test -p toron-core plane_port|injected_plane|authorize_ack_reads`:
  3 + 3 + 1 green. Workspace check (all targets) green, zero warnings.
  `rustfmt --edition 2024` applied to touched files only.

## What ran

- VERIFY at HEAD before work: red (no port, 6 reaches) → proceeded.
- Full bead VERIFY after work (receipt): check exit 0, rg gate clean,
  3 + 3 + 1 tests green.

## Deviations

1. `system.rs` (toron-mcp, not listed in scope but in-repo and covered by the
   workspace check): same 2-spot retarget, required — the `&dyn Backend`
   args would not compile otherwise. Behavior identical (per-call connect).
2. `MockPanePlane` lives under `#[cfg(test)]` (not a runtime global);
   `LivePanePlane::wrap` is currently used by no production path — kept as
   the documented embedder seam for the crate-split bead.
3. `authorize_ack` test needed agent-roster entries, not just panes (the arm
   reads `agent_list`): mock carries both, which the test documents.

## Residual inventory

- Physical crate split (backend → own crate): port is the enabler, split
  mechanical — enabler: none (follow-up bead as briefed).
- `responses.rs` `*Out` mirrors + `backend/mock.rs` untouched — enabler: none.
- Flywheel's `Box<dyn Backend>`-typed call sites, if any appear later — enabler:
  none (`LivePanePlane::wrap` exists for exactly that).

recommendation: close (port injected, mock proves kernel, suites green).

## Principles cited

- fix-root-causes — invert the reach-through so the plane can become a leaf
  (kernel takes facts through the port; backend owns construction again).
- prove-it-works — mock port + real impl tests, behavior identical (per-call
  semantics preserved line-for-line; 7 new tests green).
