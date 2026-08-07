# Architecture Decision Records (ADR)

This directory contains records of the major architectural decisions made for the `toron` project.

| ID | Title | Status |
| :--- | :--- | :--- |
| [0001](0001-project-foundation.md) | Project Foundation and Core Stack | **Superseded** by [ADR-0003](0003-nostr-mailbox-fabric.md) |
| [0002](adopt-agent-adapter-architecture.md) | Adopt Agent Adapter Architecture | **Partially superseded** by [ADR-0003](0003-nostr-mailbox-fabric.md) (MCP adapter retained; Sidecar → `Backend` trait) |
| [0003](0003-nostr-mailbox-fabric.md) | Pivot to a Nostr-Native Multi-Backend Agent Mailbox | **Accepted** |
| [0004](0004-operational-hardening.md) | Operational Hardening & Agent-Automation Efficiency (Post-Parity) | **Accepted** |
| [0005](0005-orchestration-layer.md) | Orchestration Tier — Profiles, Workflows, Stages, Smart Cron, and a Flexible Workflow Engine | **Accepted** (2-round adversarial review + 3 captain amendments) |

## Parity catalogs

| Catalog | Scope |
| :--- | :--- |
| [`docs/parity/am-rust-tool-catalog.md`](../parity/am-rust-tool-catalog.md) | AM-Rust 38-tool / 25-resource surface (names, args, counts — the frozen contract) |
| [`docs/parity/known-divergences.md`](../parity/known-divergences.md) | Intentional response-shape divergences (npub vs id, ts_us vs ISO, event-id returns) |

// governed-by: ADR-0003
