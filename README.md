# toron

A durable, multi-protocol P2P agent communication fabric.

`toron` is the "Semantic Glue" for the Agentic Era. It provides a durable, asynchronous, and decentralized fabric where agents (Gemini, OpenCode, Pi, Codex, etc.) can collaborate on long-running tasks without losing state.

---

## 🚀 Vision

In a world of specialized agents, communication is the bottleneck. `toron` solves this by providing:
*   **Durability**: Conversations are durable "promises" that survive crashes.
*   **Discovery**: Zero-config local network discovery via mDNS.
*   **Bridging**: Seamless integration for popular agents like Gemini and Claude.
*   **Security**: Secured by Pre-shared Keys (PSK) and encrypted transport.

## 🛠️ Core Stack

- **Runtime**: [asupersync](https://github.com/asupersync) (Spec-first Structured Concurrency)
- **Durability**: [Resonate](https://resonatehq.io) (Durable Execution)
- **Transport**: WebSockets & QUIC (Multi-protocol bridging)
- **Discovery**: mDNS (Zero-config local P2P)

## 🧩 Agent Bridging (Adapter Architecture)

Rather than forcing agents to adopt a new SDK, `toron` bridges them using two patterns:

1.  **MCP Adapter**: Exposes Toron as a [Model Context Protocol](https://modelcontextprotocol.io) server. This allows **Gemini CLI** or **Claude** to see other agents as tools and resources.
2.  **Sidecar Adapter**: Spawns agent binaries (like **OpenCode**) as child processes, translating their stdio into semantic Toron messages.

## 🏃 Getting Started

### 1. Start the Hub
The Hub is the primary coordinator for local agents.
```bash
cargo run --bin toron
```

### 2. Connect a Peer
Use the provided examples to see agents in action.
```bash
cargo run --example agent_ping_pong
```

## 🏗️ Project Structure

- `docs/decisions`: [Architecture Decision Records (ADRs)](docs/decisions/README.md)
- `src/hub`: Core coordination and active-passive failover
- `src/protocol`: Semantic message definitions and serialization
- `src/durability`: Resonate workflow integration
- `src/adapter`: Bridging logic for external agents (MCP/Sidecar)

## ⚖️ License

MIT

// governed-by: ADR-0001
// governed-by: ADR-0002
