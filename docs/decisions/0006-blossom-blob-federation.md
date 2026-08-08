# ADR-0006 — Blossom / NIP-B7 Federated Blob Serving

// governed-by: ADR-0003 (federation, content-addressing)

**Status: Accepted** (bd-esh.5)

**Date: 2026-08-08**

## Context

toron stores file attachments locally, content-addressed, per ADR G4:
`crates/toron-core/src/archive/blob.rs` writes blobs to `blobs/sha256/<hash>`
and a kind-15 file message carries a `toron-blob://<hash>` reference with
`x`/`file-type`/`decryption-key` tags. This is **local-first only**: a peer on
another instance cannot fetch a referenced blob — the URL scheme is
non-resolvable outside the owning host.

`bd-esh.5` (this ADR) asks: how do we serve these blobs **federated**, so an
external Nostr client or peer instance can `GET` a blob by its sha256 over
HTTP, and toron users can upload/attach large payloads to a reachable URL?

## The spec landscape (researched 2026-08-08)

Two related standards exist for Nostr blob federation:

1. **NIP-96 (HTTP File Storage Integration)** — `draft` `optional` but
   **`unrecommended`: explicitly deprecated in favor of NIP-B7** as of the
   current NIPs master. It defined `POST $api_url` (multipart form), a
   `/.well-known/nostr/nip96.json` server descriptor, kind-10096 server
   preference, and NIP-98 auth with a base64-sha256 payload.
2. **NIP-B7 (Blossom media)** + the **Blossom BUDs** (`hzrd149/blossom`) —
   the replacement. Blossom is "blobs stored simply on mediaservers": HTTP
   servers storing blobs addressed by their sha256, using Nostr keys for
   identity.

**Decision: target NIP-B7 / Blossom BUDs, not NIP-96.** Reasons:

- NIP-96 is marked `unrecommended` and deprecated by its own spec.
- NIP-B7 is the active, simpler contract and shares toron's exact
  content-addressing model (sha256-addressable blobs).
- The BUDs are minimal: `BUD-01` (get/has), `BUD-02` (upload), `BUD-03`
  (user server list), `BUD-04` (mirror), `BUD-12` (list/delete).

## Blossom BUD contract (the wire shapes toron must honor)

### BUD-01 — retrieval (`GET /<sha256>` and `HEAD /<sha256>`)

- `GET /<sha256>` MUST return the blob bytes; `Content-Type` SHOULD be the
  blob's MIME, defaulting to `application/octet-stream`. An optional file
  extension on the URL (`.pdf`, `.png`) is accepted but the server MUST still
  return the real MIME type.
- `HEAD /<sha256>` — same headers as GET, no body.
- CORS: `Access-Control-Allow-Origin: *` on **all** responses; preflight
  (`OPTIONS`) MUST allow headers `Authorization, *` and methods
  `GET, HEAD, PUT, DELETE`; `Access-Control-Max-Age: 86400` MAY be set.
- Errors: any `>=400` response MAY include a human-readable `X-Reason` header.
- Status codes: `200` (exists), `206` (range), `307/308` (redirect to a URL
  containing the same sha256), `404` (absent), `416` (bad range), etc.

### BUD-02 — upload (`PUT /upload`)

- `PUT /upload` MUST accept **binary** data in the body, MUST NOT modify it,
  and MUST compute sha256 over the exact bytes.
- Response: `201 Created` (newly stored) or `200 OK` (already exists), body =
  a **Blob Descriptor**:
  ```json
  {
    "url": "https://cdn.example.com/<sha256>.pdf",
    "sha256": "<64-hex>",
    "size": 184292,
    "type": "application/pdf",
    "uploaded": 1725105921
  }
  ```
  `url` MUST include a file extension for embeddability.
- Optional client header `X-SHA-256`: if present and mismatched → `409`.
- Status codes include `413` (too large), `415` (unsupported type), `411`
  (missing Content-Length).

### BUD-03 — user server list

- A replaceable `kind:10063` event with one or more `["server", "<url>"]`
  tags advertises the user's Blossom servers (most-reliable first). Clients
  upload to at least the first; may mirror to the rest.

### Client hash extraction (BUD-03)

- When resolving a Blossom URL, take the **last 64-char hex string** in the
  URL as the sha256 — this also works for non-Blossom CDN URLs.

## toron integration points

### 1. Serve the local G4 store over Blossom (this bead's slice)

`crates/toron-mcp/src/blob_serve.rs` (new) mounts a Blossom router over the
**existing** `BlobStore` (`toron_core::archive::blob`):

- `GET /<sha256>(.ext)` → `store.get(hash)` → 200 + bytes; `Content-Type`
  from a MIME-extension map (octet-stream default); 404 + `X-Reason` when
  absent. CORS headers on every response.
- `HEAD /<sha256>` → same headers, empty body.
- `PUT /upload` → read body bytes, `store.put(bytes)` (idempotent,
  content-addressed), compute descriptor, return `201`/`200` + JSON.
- `OPTIONS` preflight per BUD-01.
- Mounted at `/.blossom` (a distinct path so it never collides with the MCP
  `/mcp` transport) on the same axum router the daemon already serves.

The `BlobStore` needs a small extension: a MIME-type hint on `put` (or a
sidecar `.type` file) so BUD-02's descriptor `type` and BUD-01's
`Content-Type` are accurate rather than always `octet-stream`. The G4
`FileRef` already carries `file-type`; this ADR records that serving should
persist it alongside the blob.

### 2. Resolve `toron-blob://` to an HTTP URL (federation client side)

`FileRef::http_url(base)` — given a configured public base URL, map
`toron-blob://<hash>` → `<base>/.blossom/<hash>.<ext>`, and **extract the hash
by the last-64-hex rule** so external Blossom/CDN URLs resolve too. Kind-15
messages then carry a reachable URL.

### 3. Advertise servers (BUD-03)

A `kind:10063` event publisher: `toron-cli` operator command
(`toron blob servers add|list`) writing `["server", url]` tags. Optional in
this slice — recorded as the federation surface for a follow-up.

## Scope of bd-esh.5 (this slice)

- ADR (this document).
- `crates/toron-mcp/src/blob_serve.rs`: BUD-01 GET/HEAD + BUD-02 PUT + CORS,
  mounted at `/.blossom`, backed by `BlobStore`.
- MIME hint persistence on `BlobStore::put` (additive; default
  octet-stream keeps existing behavior).
- `FileRef::http_url` + `BlossomUrl::parse` (last-64-hex hash extraction).
- Tests: GET round-trip, HEAD no-body, PUT 201/200 idempotence, X-SHA-256
  409 mismatch, 404 absent, CORS headers, URL parse/extract.

Not in this slice: BUD-04 mirror, BUD-12 list/delete, kind-10063 publisher,
range requests (206) — each a clean follow-up bead.

## Decision outcome (bd-esh.5, 2026-08-08)

- **NIP-96 is deprecated → targeted NIP-B7 / Blossom BUDs** (see Context).
- Shipped `crates/toron-mcp/src/blob_serve.rs` implementing BUD-01 (GET/HEAD
  `/<sha256>`, CORS, `X-Reason`) + BUD-02 (PUT `/upload`, Blob Descriptor,
  `X-SHA-256` 409) over the existing G4 `BlobStore`.
- `BlobStore` gained additive `put_with_type`/`get_type` (MIME sidecar) +
  `exists`; default behavior unchanged.
- `http_url` maps `toron-blob://<hash>` → `<base>/.blossom/<hash>.<ext>`
  (BUD-03 last-64-hex extraction for arbitrary Blossom/CDN URLs).
- Mounted on the daemon router at `/.blossom` (never collides with `/mcp`).
- Verification: 8 unit tests (blob_serve) + 7 blob-store tests + 1 doctest +
  1 acceptance test (`blossom_route_serves_upload_and_retrieval` round-trip)
  green; `verify-gates.sh` pass.

## Consequences

- **Positive:** blobs become fetchable by any Nostr client / peer over plain
  HTTP; toron's content-addressing is preserved (sha256 = identity + URL);
  small vs large inline policy (G4) is untouched; no new Nostr kind.
- **Negative:** serving blobs over HTTP exposes them publicly (they are
  content-addressed, so unguessable-but-immutable); operator must set the
  public base URL deliberately. Auth (BUD-11) is out of scope — the slice is
  unauthenticated read + write, matching the "content-addressed =
  authorization-free" Blossom model.
- **Risks:** none to the frozen 38-tool MCP surface (new HTTP route is
  additive, under `/.blossom`); no dependency-inversion (blob_serve is
  surface code in toron-mcp over toron-core).

## Verification

- `cargo test -p toron-mcp blob_serve` green.
- `cargo test --workspace` green (subject to sibling lane WIP).
- `scripts/verify-gates.sh` pass.
