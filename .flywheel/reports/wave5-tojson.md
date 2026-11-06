# Wave 5 / Lane C report — move ToJson trait to toron-core

bead: bd-wave5-tojson-core-x7kj (toron tracker)
status: done
terminal: done
toron commit: 551441cd13dd90a604b4a428e36d4671f7fe21a9 (json.rs new, lib.rs, tools/mod.rs, tools/judgment.rs; bead id in message)
receipt: .flywheel/receipts/bd-wave5-tojson-core-x7kj-49f7b28da598.verify.txt (VERIFY green)

## Trait home

`pub trait ToJson` now defined once in `crates/toron-core/src/json.rs`
(exported as `toron_core::json`; `pub mod json` in core lib.rs). The file
carries the moved trait doc + all moved impls + a `to_json_tests` module.

## Impl split (core vs mcp)

Moved to core (39 impls — every impl whose type lives in toron-core, plus the
blanket `Vec<T>`/`Option<T>` and tuples over core types):
- primitives/containers: `bool`, `String`, `()`, `Vec<T>`, `Option<T>`
- mailbox DTOs (33): HealthComponent, HealthReport, ProjectState,
  RegisteredAgent, WhoIs, ContactLink, DeliveryPayload, Delivery, SendResult,
  RequestReleaseResult, TransferResult, InboxMessage, ReceiptResult,
  GrantedReservation, PathConflict, AcquireResult, ConflictReport, RenewResult,
  ReleaseResult, SearchMessage, (String, SearchMessage), (String, InboxMessage),
  ThreadSummary, ProductInfo, JudgmentOutcome (from tools/judgment.rs),
  ContactHandshakeResult, PrepareThreadResult, ReservationCycleResult,
  StartSessionResult, GrantedSlot*, SlotConflict*, SlotStatus*, BuildSlot*,
  AcquireBuildSlotResult* (*index::slots types)
- The move was forced, not stylistic: once the trait lives upstream, impls for
  upstream types cannot legally stay downstream (E0117).

Kept in toron-mcp (local type, foreign-but-ours trait — orphan-legal):
- `impl ToJson for LeaseBoardOut` (tools/reservation.rs, mcp-local type).

## Re-export

- `crates/toron-mcp/src/tools/mod.rs`: trait + 38 impls replaced by
  `pub use toron_core::json::ToJson;` — `super::ToJson`,
  `crate::tools::ToJson`, and toron-mcp's `pub use tools::{ToJson,
  ToolServer}` (lib.rs:27) all keep resolving; `ToolOutput::result<T: ToJson>`
  unchanged.
- toron-cli (`robot.rs`) keeps `use toron_mcp::ToJson` — compiles through the
  re-export, untouched per lane boundary.
- Pruned now-unused imports (mod.rs DTO block, slots block, `json` macro
  re-scoped to the tests module, judgment.rs trait/type imports).

## Tests

- New `json::to_json_tests` (6 fns, all green): one assertion per moved impl —
  exact shapes for primitives/containers, key assertions per DTO, plus two
  parity-guard negatives (`GrantedReservation` omits internal `epoch`;
  `InboxMessage` omits internal `bead`).
- `toron-mcp` suite green through the re-export (120 + 5 + 9 + … passed,
  0 failed), incl. `catalog_shapes` (no JSON change on the wire).

## What ran

- VERIFY at HEAD before work: trait in toron-mcp only → proceeded.
- Full bead VERIFY after work: `check --workspace --all-targets` clean,
  single definition in core, none in tools/mod.rs, `test -p toron-core
  to_json` 6/6 green (receipt).
- `rustfmt` (repo toolchain) applied to the four touched files.
- Flywheel untouched (only pre-existing .beads churn, not mine).

## Deviations

1. Count correction: 38 impls moved from tools/mod.rs (not 36 — the earlier
   list output was truncated), +1 from judgment.rs = 39 total.
2. `mbx fmt` normalized one hunk in another lane's
   `crates/toron-core/src/index/schema.rs` (their file was already dirty and
   unformatted). Disclosed, not reverted — reverting would mean editing their
   file a second time. (Side observation: that lane is building a `StoreSkew`
   type — wave4 follow-up in flight.)
3. Bare `rustfmt` (edition 2015 default) was tried first and failed without
   writing; repo `mbx fmt` used instead.

## Residual inventory

- Flywheel's `toron-mcp` dev-dependency for `ToJson` (the dep this move
  enables dropping) — enabler: none (captain sequences the dep drop after the
  br-client lane commits; this lane was forbidden to touch flywheel).
- `responses.rs` `*Out` Serialize mirrors + comments referencing old paths —
  enabler: none (comments still true; a doc-sync pass can reword).

recommendation: close (single definition, shapes proven, suites green).

## Principles cited

- fix-root-causes — the shared projection trait belongs in the core, not the
  MCP server (orphan rule made the placement mandatory, not preferential).
- prove-it-works — one definition, impls where the types live, check green
  (workspace check + 6/6 new tests + full toron-mcp suite through the re-export).
