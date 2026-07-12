# ADR-0001: Project Foundation and Core Stack

## Status
Accepted

## Context
AI agents (Gemini, OpenCode, Pi, Codex, etc.) need a reliable, decentralized, and durable way to communicate over specific topics. Existing solutions are either too rigid or lack the durability required for long-running agentic workflows.

## Decision
We will build `toron`, a standalone framework-like runtime in Rust with the following core stack:

1.  **Runtime**: `asupersync` for spec-first, cancel-correct, and capability-secure async execution.
2.  **Durability**: `Resonate` to treat conversations as durable "promises," ensuring agents can resume state after failures.
3.  **Discovery**: `mDNS` for zero-config local network discovery.
4.  **Transport**: `WebSockets` and `QUIC` with transparent bridging between protocols.
5.  **Security**: Pre-shared Key (PSK) for securing the local P2P network.
6.  **Topology**: Global Hub with Active-Passive failover for high availability.
7.  **Extensibility**: Trait-based (compile-time) integration for agents and extensions.

## Consequences

### Positive
*   **Reliability**: `Resonate` + `asupersync` provides industry-leading durability and concurrency safety.
*   **Flexibility**: Support for multiple protocols allows heterogeneous agents to talk seamlessly.
*   **Ease of Use**: mDNS ensures zero-config setup in local environments.

### Negative
*   **Complexity**: `asupersync` and `Resonate` have steeper learning curves than standard `tokio` or simple JSON-RPC.
*   **Overhead**: Maintaining durable state and protocol bridging adds runtime overhead.

## Implementation Blueprint

### Context
*   The system must run as a standalone daemon (Hub) or be integrated as a library.

### Target
*   `src/hub`: The main coordination logic.
*   `src/protocol`: Multi-protocol bridging and serialization.
*   `src/durability`: Resonate integration.

### Forbidden
*   **No `tokio`**: Direct dependency on `tokio` is forbidden in the core logic (use `asupersync`).
*   **No Unsecured discovery**: mDNS must be coupled with PSK.

## Verification
*   Bash script to verify mDNS discovery.
*   Rust tests using `asupersync` LabRuntime for deterministic concurrency testing.
*   Resonate recovery tests (kill and resume hub).

// governed-by: ADR-0001
