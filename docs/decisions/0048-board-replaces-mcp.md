# ADR-0048: `#seq` Board Facade Replaces Redundant Toron MCP Tools (`bd-unzj7`)

- **Status:** Proposed (Stage B D(iii), `D4/R2`)
- **Date:** 2026-11-25
- **Context:** `toron` exposes **41 MCP tools** (38 AM-Rust parity + 3 additive: `release_file_reservations_by_reason`, `list_file_reservations`, `ask_judgment`). With the `#seq` room-class board facade (`fleet_spawn`, `board_post`, `board_read`, `board_wait`, `fleet_land`) and `toron dashboard` (`toron tui`), shared coordination no longer needs 17 separate mail-thread, contact, and product-inbox MCP tools.

## 1. Tools Proposed for Removal (17 tools, replaced by `#seq` board verbs)

Caller counts measured across `flywheel` (`src/`) and `toron` (`crates/`):

| MCP Tool | Callers | Replacement |
|---|---:|---|
| `reply_message` | 35 | `board_post` (room) / `send_message` (sealed NIP-17 only) |
| `acknowledge_message` | 48 | `board_read` cursor + `toron mail ack` |
| `mark_message_read` | 34 | `board_read` cursor + `toron mail read` |
| `search_messages` | 58 | `board_read` + `toron mail search` |
| `summarize_thread` | 40 | `board_read` (`#seq` linear stream) |
| `macro_prepare_thread` | 22 | `board_post` (`#seq` channel) |
| `macro_start_session` | 19 | `fleet_spawn` |
| `fetch_inbox_product` | 24 | `board_read` / `fetch_inbox` |
| `search_messages_product` | 26 | `board_read` / `toron mail search` |
| `summarize_thread_product` | 32 | `board_read` |
| `request_contact` | 44 | Project roster + `board_post` |
| `respond_contact` | 41 | Project roster |
| `list_contacts` | 38 | `list_agents` / `toron dashboard` |
| `set_contact_policy` | 40 | Project manifest |
| `macro_contact_handshake` | 18 | Project roster |

*(Plus `acquire_build_slot` / `renew_build_slot` / `release_build_slot` if build slots move to `toron reserve`.)*

## 2. Kept Tools (Core Substrate + Sealed NIP-17)

- **Sealed NIP-17:** `send_message`, `fetch_inbox` (for secret/1:1/BCC traffic refused by `board_post`).
- **Leases & Guard:** `check_file_reservation_conflicts`, `file_reservation_paths`, `renew_file_reservations`, `release_file_reservations`, `force_release_file_reservation`, `release_file_reservations_by_reason`, `list_file_reservations`, `install_precommit_guard`, `uninstall_precommit_guard`.
- **Identity & Health:** `register_agent`, `create_agent_identity`, `whois`, `resolve_pane_identity`, `cleanup_pane_identities`, `list_agents`, `health_check`, `ensure_project`, `ask_judgment`.

## 3. Migration Rule

No frozen-parity MCP tool is removed by this ADR alone. Each removal batch gets its own bead after callers migrate to CLI/`#seq` board verbs and `docs/catalog/toron-mcp.json` is regenerated.
