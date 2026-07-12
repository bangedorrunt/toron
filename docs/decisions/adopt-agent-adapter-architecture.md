---
status: accepted
date: 2026-05-17
decision-makers: Gemini CLI
---

# Adopt Agent Adapter Architecture

## Context and Problem Statement
Existing coding agents (Gemini CLI, OpenCode, Pi, etc.) already possess sophisticated reasoning and tool-use capabilities. Rather than building custom agents from scratch, `toron` needs a way to bridge these existing agents into its durable, multi-protocol P2P fabric. This will allow specialized agents to collaborate seamlessly while leveraging `toron` for durability, discovery, and secure transport.

## Decision
We will implement an **Adapter Architecture** that treats external agents as first-class citizens in the `toron` network. This architecture will support two primary integration patterns:

1.  **MCP Adapter (Model Context Protocol)**: Expose Toron Hub topics and tools as an MCP server. This targets agents like Gemini CLI and Claude that can consume MCP tools and resources.
2.  **Sidecar Adapter**: Spawn standalone agent binaries as child processes, piping Hub messages to their `stdin` and reading actions from `stdout`. This targets agents that run as CLI tools without built-in protocol support.

**Non-Goals**:
*   Modifying the internal reasoning or prompt logic of bridged agents.
*   Enforcing a single agent-side communication format (translation is handled by the adapter).

## Consequences

*   **Good**: Rapid ecosystem expansion by supporting existing, powerful agents.
*   **Good**: Preservation of agent-specific capabilities (e.g., Gemini's large context window).
*   **Good**: Separation of concerns between the communication fabric (`toron`) and the reasoning engine (the agent).
*   **Bad**: Complexity in translating varied agent outputs into consistent Toron messages.
*   **Bad**: Overhead of managing external process lifecycles in the Sidecar model.

## Implementation Plan

- **Affected paths**: `src/adapter/`, `src/lib.rs`, `src/protocol/`
- **Dependencies**: `mcp-sdk-rs` (for MCP support), `asupersync` (for process management).
- **Patterns to follow**: Trait-based polymorphic adapters (`AgentAdapter` trait).
- **Patterns to avoid**: Hard-coding agent-specific logic into the Hub core.

### Verification

- [ ] Successful registration of an MCP server that exposes the Hub.
- [ ] Ability to launch a Sidecar adapter that spawns a dummy CLI and routes messages.
- [ ] Message round-trip: External Agent -> Adapter -> Hub -> Peer Agent.

## Alternatives Considered

*   **Custom SDK**: Requiring agents to use a `toron-sdk` directly. Rejected because most popular agents are closed or hard to modify at the source level.
*   **Pure Proxying**: Just piping raw sockets to agents. Rejected because it lacks the semantic intent mapping (`Intent::Propose`, `Intent::Query`) required by the Toron Protocol.

## More Information
// governed-by: ADR-0001
// governed-by: ADR-0002
