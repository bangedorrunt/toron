# toron-mcp

rmcp server over the mailbox. Two transports, one `ToronServer`.

## WHERE TO LOOK

| Concern | Path |
|---------|------|
| Server / transports | `src/server.rs` (`serve_stdio`, `serve_http`, `router`) |
| 38 tools | `src/tools/{system,mail,reservation,product,contact,slot,workflow}.rs` |
| 25 resources | `src/resources.rs` |
| Response shapes | `src/responses.rs` |
| Workflow host | `src/workflow_host.rs` (trigger ingress + coalescer flush) |
| Blossom | `src/blob_serve.rs` — mount `/.blossom`, never collide `/mcp` |

HTTP: `/mcp` + `/.blossom`. Protocol `V_2026_07_28` (fallback `V_2025_11_25`).

## CONVENTIONS

- `#[tool]` / `#[resource]` + schemars-derived schemas. No hand-rolled schemas.
- Frozen names: 38 tools, 25 resources. Intentional shape diffs live in
  `docs/parity/known-divergences.md` — do not chase AM-Rust byte parity.
- `demo_mailbox` is the production serve constructor (plus robot/tui/bench).
- Workflow host sender is `workflow` / `workflow-{project}` until a
  `ProfileResolver` is injected here (P4 seam; flywheel owns the live catalog).
- Mutex poison must not brick the process.

## ANTI-PATTERNS

- New tool names without an ADR.
- Legacy HTTP+SSE two-endpoint transport.
- Blossom `PUT /upload` mutating the binary body.
- Workflow-emitted events re-triggering the host ingress loop.
- Passphrase or nsec in MCP responses/logs.
