# Parity Shape Catalog — Known Divergences (toron vs AM-Rust)

// governed-by: ADR-0003, ADR-0004, ADR-0005

> **Intentional-but-catalogued** parity shape divergences between toron (Nostr-native)
> and AM-Rust (`mcp_agent_mail_rust`, integer-id/sqlite model). These are **BY DESIGN**,
> not gaps: toron is crypto-native per ADR-0003 (Schnorr identity, NIP-17 E2E, kind-based
> dispatch). Integration/parity tests must assert **structurally-equivalent-where-required**,
> not byte-identical. Do NOT chase byte-parity on Nostr-vs-sqlite models.
>
> Source: the 3-agent real-e2e opencode probe (2026-08-06, bead bd-96a), cross-checked
> against `crates/toron-mcp/src/tools.rs` `ToJson` impls and `crates/toron-core/src/mailbox/*`
> on 2026-08-07. Companion catalog: `docs/parity/am-rust-tool-catalog.md` (the tool surface
> contract — names/counts are the frozen 38-tool contract; this file documents response
> SHAPE differences only).
>
> Canonical bead: **bd-96a** / **bd-3mp**. Unblocks bd-jgd (outputSchema on the 38 tools)
> which should reference these shapes instead of free-text notes.

## 1. Identity model (the biggest divergence — EXPECTED, do NOT change)

| Axis | toron | AM-Rust |
|---|---|---|
| Agent identity | `npub` (Schnorr pubkey hex) + `key_path` + `profile_event_id` | integer `id` + `registration_token` (no npub) |
| Timestamps | `*_ts_us` (int microseconds) or `created` (epoch) | ISO-8601 strings (`inception_ts`, `last_active_ts`, `created_at`) |
| Profile event | kind-30013 on the bus (crypto-signed) | sqlite `agents` row |
| Naming policy | arbitrary names accepted (MORE permissive) | enforces adjective+noun (rejects `PiAmA`) |

Every identity op differs in response shape. toron is crypto-native per ADR-0003.

## 2. Field-shape divergences (per op, toron side verified)

| Tool | toron shape | AM-Rust shape | Notes |
|---|---|---|---|
| `whois` | `{name, npub, program, model, task_description, last_active_ts_us}` | `{...} + id/capabilities/recent_commits/inception_ts`, ISO ts | no `id`/`capabilities`/`recent_commits` on toron |
| `list_agents` | array w/ `npub`, `last_active_ts_us` | array w/ `id`, `capabilities`, ISO `last_active_ts` | timestamp unit differs (int-us vs ISO) |
| `register_agent` / `create_agent_identity` | `{name, project_slug, npub, profile_event_id, key_path, created}` | `{id, name, program, model, task_description, inception_ts, last_active_ts, project_id}` | no integer id / registration_token |
| `resolve_pane_identity` | no-pane → empty success `{agent_name: null, pane_id}` | no-pane → hard error | toron is lenient |
| `cleanup_pane_identities` | `[]` (bare list) | `{removed_count, removed_paths}` | shape differs |
| `request_contact` | stores `reason` as a NIP-17 tag on the control message; response `ContactLink` has no `reason`/`expires_ts` | stores + returns `reason`; has `expires_ts` | reason IS persisted on toron (tag), just not surfaced in the response; `expires_ts` absent |
| `set_contact_policy` | returns bare kind-30014 **event-id hex string** | returns `{agent, policy}` | policy invisible after set (no read API surfaces it on toron) |
| `summarize_thread` | flat `message_count` at top level | nested `summary.total_messages` | nesting differs |
| `release_file_reservations` | `{released: N}` | `{released: N, released_at}` | no timestamp on toron |
| `send_message` | `deliveries[].payload = {recipient, event_id}` (wrap-id/Nostr event) | numeric delivery id | field names differ |
| `fetch_inbox` | entries lack AM's `read_ts`/`kind` shape | has `read_ts?`, `kind` | read-receipt shape differs |
| `reply_message` | `subject` REQUIRED | `subject_prefix` optional | arg semantics differ |
| `search_messages` | params `since`/`until` | params `date_start`/`date_end` | alias both directions in tests |
| `health_check` | 2-field `{archive, bus}` | deep `{status, health_level, verdicts, queues, disk}` | toron is minimal |

## 3. AM-side behaviors (NOT toron faults)

- `acquire_build_slot` / `renew_build_slot` / `release_build_slot`: AM disabled in the
  probe env (needs `WORKTREES_ENABLED`); toron works.
- `search_messages` on AM runs degraded (`search_index_stale`); toron returns `[]`
  (no test data) — not a parity failure.
- AM conflict-check requires normalized `/private/tmp` path; toron accepts both
  `/tmp` and `/private/tmp` (toron advantage).

## 4. Test guidance

- Parity tests assert structural equivalence: for identity ops require `npub` +
  `*_ts_us` on toron and `id` + ISO on AM — never byte-identity.
- Timestamp aliasing: `since`/`until` (toron) ≡ `date_start`/`date_end` (AM).
- toron's more-permissive naming is an advantage; tests must not require rejection.
- `set_contact_policy` returns an event id — tests read the kind-30014 event back
  off the bus to verify the policy, not the tool response.

## 5. Open thread

- ~~bd-jgd (outputSchema on the 38 MCP tools) should derive schemars structs from the
  toron shapes above and REPLACE this file's free-text notes as the schema-level source
  of truth. This catalog stays the human-readable divergence record.~~
- **DONE (bd-jgd, 2026-08-08):** every one of the 38 tools now carries a schemars-derived
  `outputSchema` on its definition (and over the MCP wire). Schema structs live in
  `crates/toron-mcp/src/responses.rs`, mirroring the toron-side shapes in section 2
  (e.g. `WhoIsOut` = `{name, npub, program, model, task_description, last_active_ts_us}`,
  `SendResultOut.deliveries[].payload = {recipient, event_id}`, `ReleaseResultOut = {released}`).
  The schemas are the machine-readable source of truth for response shapes; this catalog
  stays the human-readable divergence record. Divergence notes above remain accurate —
  they are what the structs encode.
- Acceptance gate: `all_tools_declare_output_schema` (lib test) + wire-level
  `output_schema.is_some()` check in `crates/toron-mcp/tests/acceptance.rs`.

---

*Maintained by lane OC-C (BlackMarsh). Created 2026-08-07 from the bd-96a real-e2e probe
+ source cross-check. Updated per code changes; re-verify toron shapes on any response
change (ADR-0004: "changes a response shape" gates on this catalog). outputSchema
structs: `crates/toron-mcp/src/responses.rs` (bd-jgd).*
