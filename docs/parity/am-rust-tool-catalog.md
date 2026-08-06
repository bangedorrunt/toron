# AM-Rust Tool & Resource Catalog (Canonical)

// governed-by: ADR-0003

> First-party parity catalog for toron. Source: the **public interface** of
> `Dicklesworthstone/mcp_agent_mail_rust` (README tool/resource tables + the
> `#[tool(...)]`/`#[resource(...)]` attributes in `crates/mcp-agent-mail-tools/src/*.rs`).
> Interface metadata (names, args, return shapes) only — **no test code vendored**
> (AM-Rust is `LicenseRef-MIT-Rider`; `bd-77u.1` fixtures are first-party artifacts).
> Source of truth for `bd-led.7.2` (wire the 38 tools) and `bd-77u.1` (golden fixtures).
> Phase-1 mailbox beads MUST reference tool names verbatim from this file.

Counts: **38 tools / 9 clusters** and **25 MCP resources**.

## 1. Tool clusters (38 tools, 9 clusters)

| # | Cluster | Count | Tools |
|---|---------|-------|-------|
| 1 | Infrastructure | 4 | `health_check`, `ensure_project`, `install_precommit_guard`, `uninstall_precommit_guard` |
| 2 | Identity | 6 | `register_agent`, `create_agent_identity`, `whois`, `resolve_pane_identity`, `cleanup_pane_identities`, `list_agents` |
| 3 | Messaging | 5 | `send_message`, `reply_message`, `fetch_inbox`, `acknowledge_message`, `mark_message_read` |
| 4 | Contacts | 4 | `request_contact`, `respond_contact`, `list_contacts`, `set_contact_policy` |
| 5 | File Reservations | 5 | `check_file_reservation_conflicts`, `file_reservation_paths`, `renew_file_reservations`, `release_file_reservations`, `force_release_file_reservation` |
| 6 | Search | 2 | `search_messages`, `summarize_thread` |
| 7 | Macros | 4 | `macro_start_session`, `macro_prepare_thread`, `macro_contact_handshake`, `macro_file_reservation_cycle` |
| 8 | Product Bus | 5 | `ensure_product`, `products_link`, `search_messages_product`, `fetch_inbox_product`, `summarize_thread_product` |
| 9 | Build Slots | 3 | `acquire_build_slot`, `renew_build_slot`, `release_build_slot` |
| **Total** | | **38** | |

## 2. Infrastructure (4 tools)

### `health_check`

_Return basic readiness information for the Agent Mail server._

- **Required:** — (none)
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

### `ensure_project`

_Idempotently create or ensure a project exists for the given human key._

- **Required:** `human_key`
- **Optional:** `identity_mode`
- **Returns:** dict Minimal project descriptor: { id, slug, human_key, created_at }.

### `install_precommit_guard`

__

- **Required:** `project_key`, `code_repo_path`
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

### `uninstall_precommit_guard`

__

- **Required:** `code_repo_path`
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

## 3. Identity (6 tools)

### `register_agent`

_Create or update an agent identity within a project and persist its profile to Git._

- **Required:** `project_key`, `program`, `model`
- **Optional:** `name`, `task_description`, `attachments_policy`, `reaper_exempt`, `pane_id`, `registration_proof`
- **Returns:** dict { id, name, program, model, task_description, inception_ts, last_active_ts, project_id }

### `create_agent_identity`

_Create a new, unique agent identity and persist its profile to Git._

- **Required:** `project_key`, `program`, `model`
- **Optional:** `name_hint`, `task_description`, `attachments_policy`, `pane_id`, `registration_proof`
- **Returns:** dict { id, name, program, model, task_description, inception_ts, last_active_ts, project_id }

### `whois`

_Return enriched profile details for an agent, optionally including recent archive commits._

- **Required:** `project_key`, `agent_name`
- **Optional:** `include_recent_commits`, `commit_limit`
- **Returns:** dict Agent profile augmented with { recent_commits: [{hexsha, summary, authored_ts}] } when requested.

### `resolve_pane_identity`

_Resolve the agent name for a tmux pane from the canonical per-pane identity file._

- **Required:** `project_key`
- **Optional:** `pane_id`
- **Returns:** dict { agent_name, pane_id, identity_path }

### `cleanup_pane_identities`

_Remove stale per-pane identity files for tmux panes that no longer exist._

- **Required:** — (none)
- **Optional:** `project_key`
- **Returns:** dict { removed_count, removed_paths }

### `list_agents`

_List registered agents in a project, most-recently-active first._

- **Required:** `project_key`
- **Optional:** `limit`, `active_within_days`
- **Returns:** str (JSON) Array of agent objects with fields: name, program, model, task_description, inception_ts, last_active_ts, contact_policy. Ordered by last_active_ts descending.

## 4. Messaging (5 tools)

### `send_message`

_Send a Markdown message to one or more recipients and persist canonical and mailbox copies to Git._

- **Required:** `project_key`, `sender_name`, `to`, `subject`, `body_md`
- **Optional:** `cc`, `bcc`, `attachment_paths`, `convert_images`, `importance`, `ack_required`, `thread_id`, `topic`, `broadcast`, `auto_contact_if_blocked`, `sender_token`
- **Returns:** dict { \"deliveries\": [ { \"project\": str, \"payload\": { ... message payload ... } } ], \"count\": int, \"verified_sender\": bool }

### `reply_message`

_Reply to an existing message, preserving or establishing a thread._

- **Required:** `project_key`, `message_id`, `sender_name`, `body_md`
- **Optional:** `to`, `cc`, `bcc`, `subject_prefix`, `importance`, `ack_required`, `attachment_paths`, `convert_images`, `sender_token`
- **Returns:** dict Message payload including `thread_id` and `reply_to`.

### `fetch_inbox`

_Retrieve recent messages for an agent and mark returned messages read._

- **Required:** `project_key`, `agent_name`
- **Optional:** `urgent_only`, `since_ts`, `limit`, `include_bodies`, `unread_only`, `ack_overdue_only`, `topic`, `mark_read`
- **Returns:** list[dict] Each message includes: { id, subject, from, created_ts, read_ts?, ack_ts?, importance, ack_required, kind, [body_md] }

### `acknowledge_message`

_Acknowledge a message addressed to an agent (and mark as read)._

- **Required:** `project_key`, `agent_name`, `message_id`
- **Optional:** — (none)
- **Returns:** dict { message_id, acknowledged: bool, acknowledged_at: iso8601 | null, read_at: iso8601 | null }

### `mark_message_read`

_Mark a specific message as read for the given agent._

- **Required:** `project_key`, `agent_name`, `message_id`
- **Optional:** — (none)
- **Returns:** dict { message_id, read: bool, read_at: iso8601 | null }

## 5. Contacts (4 tools)

### `request_contact`

_Request contact approval to message another agent._

- **Required:** `project_key`, `from_agent`, `to_agent`
- **Optional:** `to_project`, `reason`, `ttl_seconds`, `register_if_missing`, `program`, `model`, `task_description`
- **Returns:** (see tool description for shape)

### `respond_contact`

_Approve or deny a contact request._

- **Required:** `project_key`, `to_agent`, `from_agent`, `accept`
- **Optional:** `from_project`, `ttl_seconds`
- **Returns:** (see tool description for shape)

### `list_contacts`

_List contact links for an agent in a project._

- **Required:** `project_key`, `agent_name`
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

### `set_contact_policy`

_Set contact policy for an agent: open | auto | contacts_only | block_all._

- **Required:** `project_key`, `agent_name`, `policy`
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

## 6. File Reservations (5 tools)

### `check_file_reservation_conflicts`

_Check project-relative paths against authoritative active exclusive file reservations without mutating Agent Mail._

- **Required:** `project_key`, `agent_name`, `paths`
- **Optional:** — (none)
- **Returns:** dict { conflict_free, conflicts, clear_paths, checked_paths, total_conflicting_reservations, output_truncated, project, snapshot_ts, authoritative_source, read_only }

### `file_reservation_paths`

_Request advisory file reservations (leases) on project-relative paths/globs._

- **Required:** `project_key`, `agent_name`, `paths`
- **Optional:** `ttl_seconds`, `exclusive`, `reason`
- **Returns:** dict { granted: [{id, path_pattern, exclusive, reason, expires_ts}], conflicts: [{path, holders: [...]}] }

### `renew_file_reservations`

_Extend expiry for active file reservations held by an agent without reissuing them._

- **Required:** `project_key`, `agent_name`
- **Optional:** `extend_seconds`, `paths`, `file_reservation_ids`
- **Returns:** dict { renewed: int, file_reservations: [{id, path_pattern, old_expires_ts, new_expires_ts}] }

### `release_file_reservations`

_Release active file reservations held by an agent._

- **Required:** `project_key`, `agent_name`
- **Optional:** `paths`, `file_reservation_ids`
- **Returns:** dict { released: int, released_at: iso8601 }

### `force_release_file_reservation`

_Force-release a stale file reservation held by another agent after inactivity heuristics._

- **Required:** `project_key`, `agent_name`, `file_reservation_id`
- **Optional:** `note`, `notify_previous`
- **Returns:** (see tool description for shape)

## 7. Search (2 tools)

### `search_messages`

_Search over subject and body for a project using the unified Search V3 service._

- **Required:** `project_key`, `query`
- **Optional:** `limit`, `offset`, `cursor`, `ranking`, `sender`, `from_agent`, `sender_name`, `importance`, `thread_id`, `date_start`, `date_end`, `date_from`, `date_to`, `after`, `before`, `since`, `until`, `explain`, `include_body_md`
- **Returns:** dict { result: [{ id, subject, importance, ack_required, created_ts, thread_id, from }], assistance?, guidance?, explain?, next_cursor?, diagnostics? }

### `summarize_thread`

_Extract participants, key points, and action items for one or more threads._

- **Required:** `project_key`, `thread_id`
- **Optional:** `include_examples`, `llm_mode`, `llm_model`, `per_thread_limit`
- **Returns:** (see tool description for shape)

## 8. Macros (4 tools)

### `macro_start_session`

_Macro helper that boots a project session: ensure project, register agent, optionally file_reservation paths, and fetch the latest inbox snapshot._

- **Required:** `human_key`, `program`, `model`
- **Optional:** `agent_name`, `task_description`, `file_reservation_paths`, `file_reservation_reason`, `file_reservation_ttl_seconds`, `inbox_limit`, `reaper_exempt`, `pane_id`, `registration_proof`
- **Returns:** (see tool description for shape)

### `macro_prepare_thread`

_Macro helper that aligns an agent with an existing thread by ensuring registration, summarising the thread, and fetching recent inbox context._

- **Required:** `project_key`, `thread_id`, `program`, `model`
- **Optional:** `agent_name`, `task_description`, `register_if_missing`, `include_examples`, `include_inbox_bodies`, `llm_mode`, `llm_model`, `inbox_limit`, `registration_proof`
- **Returns:** (see tool description for shape)

### `macro_contact_handshake`

_Request contact permissions and optionally auto-approve plus send a welcome message._

- **Required:** `project_key`
- **Optional:** `requester`, `target`, `agent_name`, `to_agent`, `to_project`, `reason`, `auto_accept`, `ttl_seconds`, `welcome_subject`, `welcome_body`, `thread_id`, `register_if_missing`, `program`, `model`, `task_description`
- **Returns:** (see tool description for shape)

### `macro_file_reservation_cycle`

_Reserve a set of file paths and optionally release them at the end of the workflow._

- **Required:** `project_key`, `agent_name`, `paths`
- **Optional:** `ttl_seconds`, `exclusive`, `reason`, `auto_release`
- **Returns:** (see tool description for shape)

## 9. Product Bus (5 tools)

### `ensure_product`

_Ensure a Product exists. If not, create one._

- **Required:** — (none)
- **Optional:** `product_key`, `name`
- **Returns:** (see tool description for shape)

### `products_link`

_Link a project into a product (idempotent)._

- **Required:** `product_key`, `project_key`
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

### `search_messages_product`

_Search across all projects linked to a product using the unified Search V3 service._

- **Required:** `product_key`, `query`
- **Optional:** `limit`, `cursor`, `project`, `project_key_filter`, `project_slug`, `proj`, `sender`, `from_agent`, `sender_name`, `importance`, `thread_id`, `ranking`, `date_start`, `date_end`, `date_from`, `date_to`, `after`, `before`, `since`, `until`, `include_body_md`
- **Returns:** dict { result: [{ id, subject, importance, ack_required, created_ts, thread_id, from, project_id }], assistance?, next_cursor?, diagnostics? }

### `fetch_inbox_product`

_Retrieve recent messages for an agent across all projects linked to a product (non-mutating)._

- **Required:** `product_key`, `agent_name`
- **Optional:** `limit`, `urgent_only`, `include_bodies`, `since_ts`
- **Returns:** (see tool description for shape)

### `summarize_thread_product`

_Summarize a thread (by id or thread key) across all projects linked to a product._

- **Required:** `product_key`, `thread_id`
- **Optional:** `include_examples`, `llm_mode`, `llm_model`, `per_thread_limit`
- **Returns:** (see tool description for shape)

## 10. Build Slots (3 tools)

### `acquire_build_slot`

_Acquire a build slot (advisory), optionally exclusive. Returns conflicts when another holder is active._

- **Required:** `project_key`, `agent_name`, `slot`
- **Optional:** `ttl_seconds`, `exclusive`
- **Returns:** (see tool description for shape)

### `renew_build_slot`

_Extend expiry for an existing build slot lease. No-op if missing._

- **Required:** `project_key`, `agent_name`, `slot`
- **Optional:** `extend_seconds`
- **Returns:** (see tool description for shape)

### `release_build_slot`

_Mark an active slot lease as released (non-destructive; keeps JSON with released_ts)._

- **Required:** `project_key`, `agent_name`, `slot`
- **Optional:** — (none)
- **Returns:** (see tool description for shape)

## 11. MCP Resources (25 URIs, read-only)

| # | URI template | Scope | Summary |
|---|--------------|-------|---------|
| 1 | `resource://agents/{project_key}` | project | List registered agents (name, program, model, task, timestamps). |
| 2 | `resource://config/environment` | global | Server environment + HTTP settings (host/port/path). |
| 3 | `resource://file_reservations/{slug}` | project | Reservations; `active_only=true` (default) filters released. |
| 4 | `resource://identity/{project}` | project | Identity resolution: slug, mode, canonical path, git identity. |
| 5 | `resource://inbox/{agent}` | project(q) | Inbox; `project` (req), `limit`, `since_ts`, `urgent_only`, `include_bodies`. |
| 6 | `resource://mailbox-with-commits/{agent}` | project(q) | Mailbox + commit metadata incl. explicit unavailable markers. |
| 7 | `resource://mailbox/{agent}` | project(q) | Mailbox w/ commit metadata; explicit unavailable markers. |
| 8 | `resource://message/{message_id}` | project(q) | Single message body/metadata by id. |
| 9 | `resource://outbox/{agent}` | project(q) | Messages sent by agent + commit metadata. |
| 10 | `resource://product/{key}` | global | Product + linked projects. |
| 11 | `resource://project/{slug}` | project | Project + its agents (LDAP-like directory). |
| 12 | `resource://projects` | global | All projects in creation order. |
| 13 | `resource://thread/{thread_id}` | project(q) | Thread messages; numeric message-id seed accepted. |
| 14 | `resource://tooling/capabilities/{agent}` | project(q) | Tooling capabilities for an agent. |
| 15 | `resource://tooling/diagnostics` | global | Comprehensive diagnostic report + recommendations. |
| 16 | `resource://tooling/directory` | global | Clustered tool directory w/ playbooks. |
| 17 | `resource://tooling/locks` | global | Lock metadata from shared archive storage. |
| 18 | `resource://tooling/metrics` | global | Aggregated tool call/error counts. |
| 19 | `resource://tooling/metrics_core` | global | Core system metrics (HTTP/DB/Storage). |
| 20 | `resource://tooling/recent/{window_seconds}` | global | Recent tool activity. |
| 21 | `resource://tooling/schemas` | global | Parameter schemas for tools/macros (drift guard). |
| 22 | `resource://views/ack-overdue/{agent}` | project(q) | Ack-required messages older than ttl_minutes. |
| 23 | `resource://views/ack-required/{agent}` | project(q) | Messages requiring ack where ack pending. |
| 24 | `resource://views/acks-stale/{agent}` | project(q) | Ack-required messages past TTL still unacked. |
| 25 | `resource://views/urgent-unread/{agent}` | project(q) | Urgent+high unread messages. |

**Scoping rule:** `agents`/`file_reservations`/`project`/`identity` take the project in the path segment; `inbox`/`mailbox`/`outbox`/`thread`/`message`/`views` take agent-or-id in the path and `?project=` in the query. `...?{query}` variants are the same resource parameterized by query string.

## 12. ADR-0003 abbreviation gaps flagged

- **Macros (ADR §Tool→Nostr mapping says only "Macros (4): compose the above").** Concrete names now canonical: `macro_start_session`, `macro_prepare_thread`, `macro_contact_handshake`, `macro_file_reservation_cycle`.
- ADR order vs README order: ADR lists Contacts before File Reservations; README lists Search before Macros, Product Bus before Build Slots. Order is cosmetic; names/counts are the contract.
- `fetch_inbox` ADR summary omits the read-marking default; the tool marks returned messages read unless `mark_read=false`.
- `summarize_thread` accepts a numeric message id as thread seed (ADR says thread key only).
- `search_messages` exposes cursor + date-alias args (`date_from/date_to/after/before/since/until`) not enumerated in the ADR mapping; aliases are first-party, not parity-mandated.

---

*Generated by `bd-0qg` (oc-c) from AM-Rust public interface, 2026-08-06.*
