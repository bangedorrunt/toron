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
| `summarize_thread` | flat `message_count` at top level; `participants` are NAMES (AM-parity) | nested `summary.total_messages` | nesting differs |
| `release_file_reservations` | `{released: N}` | `{released: N, released_at}` | no timestamp on toron |
| `send_message` | `deliveries[].payload = {recipient, event_id}` (wrap-id/Nostr event) | numeric delivery id | field names differ |
| `fetch_inbox` | `sender` = the sender's display NAME (resolved from the agents index; hex npub fallback for unregistered senders); entries lack AM's `read_ts`/`kind` shape | `sender` = display name; has `read_ts?`, `kind` | read-receipt shape differs; sender NAME is now AM-parity (fixed 2026-08-08) |
| `reply_message` | `subject` REQUIRED; `message_id` NOT taken — replies via `thread_id` (the NIP-10/`thread`-tag anchor) | `message_id` REQUIRED (anchors the reply-to chain); `subject_prefix` optional | arg semantics differ (toron: thread-anchored; AM: message-anchored) |
| `search_messages` | params `since`/`until` | params `date_start`/`date_end` | alias both directions in tests |
| `health_check` | 2-field `{archive, bus}`; **no required args** (project_key optional, ignored — fixed 2026-08-08) | deep `{status, health_level, verdicts, queues, disk}`; no required args | toron is minimal |

## 2b. Id-type args (toron hex vs AM u64) — FIXED 2026-08-08

> **Driver 5 / C3 exception (ADR-0007):** tool **names** are frozen, but arg types may
> break pre-1.0 — recorded here per the dual-accept + divergence-note posture.

`acknowledge_message` / `mark_message_read` `message_id` and `force_release_file_reservation`
`file_reservation_id` were typed `i64` (copied from AM's integer-id model), but toron's ids are
hex event ids (the `id` field `fetch_inbox` / `file_reservation_paths` return). An agent could
NOT acknowledge/read/force-release over the MCP wire — the schema rejected the hex string.
All three args are now `String`, matching toron's own id model (the mailbox layer always
matched by hex; only the MCP arg type was wrong). Verified by `am_parity_e2e`.

**Residual (F11-class, tracked with F11):** `renew_file_reservations` / `release_file_reservations`
still accept `file_reservation_ids: Option<Vec<i64>>`, which the handlers silently ignore
(only `paths` is honored). No silent-0 corruption on the id itself (ids are never parsed to
`i64`), but accepted-and-ignored args are an honesty gap to close.

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

## 6. Free-form thread ids (FIXED 2026-08-08)

AM thread ids are free-form strings (`"br-123"`, `"deploy-xyz"`). toron previously
encoded `thread_id` ONLY as a NIP-10 `e` tag, which requires a valid hex event id — so
any free-form thread id was silently dropped (inbox `thread_id` came back `null`).
`build_visible_rumor` now emits a literal `thread` tag for ALL thread ids (and an `e`
root tag too when the id IS a hex event id, for strict NIP-10 clients). Both
`inbox_from` (live fetch_inbox) and the unwrap projection (`messages.thread_id` column,
which search/summarize read) prefer the `thread` tag with `e` fallback. Verified by
`am_parity_e2e` (send with `thread_id="parity-thread-1"` → inbox + summarize round-trip).

## 7. Daemon data-dir isolation (FIXED 2026-08-08)

`demo_mailbox` used a FIXED data dir (`toron-mcp-demo`), so every server boot
accumulated agent keys there. The unwrap loop runs the argon2 KDF (~1s/key) over ALL
keys on every pipeline rebuild — after dozens of boots the rebuild stalls for minutes
(observed: 80+ keys, search projection never lands within test timeouts). The data dir
is now per-invocation (`toron-mcp-demo-<pid>-<seq>`); `TORON_MCP_DATA_DIR` overrides
for tests that need a data dir to persist ACROSS processes (cli_robot registers
in-process then shells out). Verified: `am_parity_e2e` dropped from 400s+ (with search
failing) to 56s green.

### KDF scan bound (R2.0 / F2.KDF, 2026-08-08 — gates durable root)

The unwrap-loop pipeline rebuild no longer re-derives every key. `Mailbox` keeps a
content-keyed unlock cache (`crates/toron-core/src/mailbox/mod.rs`, `key_cache` +
`cached_load_agent_key`): rebuilds re-run the argon2 KDF ONLY for key files whose
ncryptsec CONTENT changed — O(Δkeys) per rebuild, never O(N).

- **Cold path bound:** N × per-key KDF (~1s/key at log-n 16), each key unlocked
  exactly once per process.
- **Warm rebuild bound:** KDF = number of added/changed key files; unchanged keys
  are memory cache hits (pure file reads, no argon2).
- **Machine-checked by** `unwrap_loop_kdf_cached_between_rebuilds_many_keys`
  (fixture: 50 keys + 1 added + 1 changed; asserts KDF counts 50 → 51 → 51 → 52).
- Durable XDG root (R2.2) may land only with this gate green.

## 8. AM-parity e2e (NEW 2026-08-08)

`crates/toron-mcp/tests/am_parity_e2e.rs` boots the REAL daemon (axum router on a live
TCP socket + operator key) and drives all 38 tools over the Streamable HTTP wire as a
real agent would. This is the "is toron a trustworthy agent-mail replacement" gate: it
INVOKES tools (sibling `acceptance.rs` only lists them). Cluster assertions:
infra/identity (health, ensure_project idempotency, register upsert, whois), messaging
(send → inbox → ack → read → reply → search → summarize round-trip), contacts
(policy → request → respond → list), reservations (acquire → conflict → renew → release
→ force-release staleness gate), build slots (acquire → renew → release), macros +
product bus (session, reservation cycle, contact handshake, product link fan-out).

Run: `cargo test -p toron-mcp --test am_parity_e2e`.

**Flake root cause + fix (2026-08-09):** the harness never isolated the daemon's
mailbox root. After the R2 durable default (ADR-0007 D3) replaced per-pid temp roots,
`build_mailbox` fell back to the DURABLE XDG root (`~/.local/share/toron`), which
accumulates agent keys across every test run. The unwrap-loop pipeline rebuild
KDF-loads EVERY key in the root (~1s/key cold) — after a few runs the projection
(search/summarize/product fan) stalled for minutes while the per-agent inbox
(single-key load) stayed fast, so `cluster_messaging_round_trip` +
`cluster_macros_and_product_bus` failed on search. Fixes: (a) `LiveDaemon::boot` sets
`TORON_MCP_DATA_DIR` to a per-invocation temp dir (restores the "56s green" isolation
that per-pid roots used to provide); (b) the messaging cluster now polls the search
until the projection catches up (bounded 20s) instead of one immediate racy search —
the projection is async by design, so the test must await it (the product cluster
already polled). Suite green 6/6 in ~58s.

## 9. Unified MailSurface consumption (NEW 2026-08-09, ADR-0007 se2.3)

flywheel's local `MailBackend` trait is DELETED; both backends implement
`toron_core::mailbox::surface::MailSurface` (async, `&self`, `MessageId` string ids,
`MailError` machine codes). Consequences for the parity catalog:

- **Send receipts carry the RUMOR id (D8.2 fix).** `send_message`/`reply_message`
  `deliveries[].payload.event_id` is now the rumor id — the SAME id the projection
  stores and `fetch_inbox`/`search` rows show — never the transport-internal gift-wrap
  id. A receipt id is usable for ack/mark_read/summarize on the toron path.
- **Projection self-threads head messages.** A message with no `thread`/`e` tag gets
  `thread_id = own rumor id` in the `messages` table, so `summarize_thread(head_id)`
  finds the head (AM parity: every message belongs to a thread, the head IS its own).
- **AM adapter fills Nostr-only fields with `""`** (`npub`, `key_path`,
  `profile_event_id`, `manifest_event_id`, `from_pubkey`, `to_pubkey`,
  `agent_pubkey`): agent-mail has no Schnorr identity. Name-based ops (whois, list,
  contact handshake) are unaffected (G2); the fields are documented-empty, never
  fabricated.
- **`MailItem.sender` accepts both `sender` (toron) and `from` (agent-mail)** via a
  serde alias — one typed row shape for both backends.
- **Pane write/clear are `Unsupported` on toron** (no per-pane identity files; live
  herdr mapping, G9); agent-mail writes/clears the identity files it serves.
- **AM message ids are decimal strings** on the typed surface (`MessageId("42")`);
  a non-decimal id on the AM edge is a parse ERROR (se2.2), never `unwrap_or(0)`.
- The `renew_file_reservations`/`release_file_reservations` `file_reservation_ids`
  accepted-and-ignored gap (F11 residual) is GONE from the shared surface — the
  superset trait is paths-only; ids were never honored on either backend.

---

*Maintained by lane OC-C (BlackMarsh). Created 2026-08-07 from the bd-96a real-e2e probe
+ source cross-check. Updated per code changes; re-verify toron shapes on any response
change (ADR-0004: "changes a response shape" gates on this catalog). outputSchema
structs: `crates/toron-mcp/src/responses.rs` (bd-jgd).*
