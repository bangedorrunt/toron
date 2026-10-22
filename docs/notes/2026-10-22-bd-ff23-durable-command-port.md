<!-- governed-by: ADR-0040, ADR-0041 -->
# bd-ff23: the comms port's durable commands — status and the two open decisions

Date: 2026-10-22 · Lane: `elm-comms-kernel` · Agent: BlueRidge

## What the bead asks

`bd-ff23` (ADR-0018 §3 step 1, toron's half of the elm-effect port) closes with
two "Done means" and the port's five-item checklist
(`elm-effect-rs/skills/elm-effect/references/migrate-toron.md`):

1. a mail send is a durable command keyed by its event id;
2. a reservation acquire is a claim with a generation, a lease and `may_settle`;
3. one persisted `EffectState`, the core one;
4. `plan_attempt` table tests pass against toron's function and the framework's on
   the same vectors;
5. `PARK_MAX_WAIT_SECS` is still 86400, and a longer park is a typed refusal;
6. a replay digest mismatch does not mint a second effect key;
7. `cargo test` in `toron-core` and `toron-workflow` for the journal and park
   modules is green.

## Status of each item (2026-10-22)

| # | Item | Status | Evidence |
|---|------|--------|----------|
| 3 | one persisted `EffectState` | done for the toron crates | `crates/toron-workflow/src/engine/boundary.rs:49` imports `toron_core::index::workflow_effects::EffectState`; the only other spelling is the vendored framework's, whose transition table is proven equal by `crates/toron-core/tests/kernel_port.rs` (81 pairs) |
| 4 | `plan_attempt` parity | done | `crates/toron-workflow/src/engine/reconcile.rs:463` compares toron's function with the framework's over 216 vectors |
| 5 | `PARK_MAX_WAIT_SECS == 86400` | done | `crates/toron-workflow/src/engine/elm.rs:61` |
| 6 | replay digest mints no second key | done | `check_replay` re-export + commit `8d4e0d2` |
| 7 | journal and park modules green | done | `mbx test -p toron-core` (906 lib + all integration) and `-p toron-workflow` (326 + all integration) pass |
| 1 | mail send is a durable command keyed by its event id | **landed in this increment** (see below) | `crates/toron-core/src/mailbox/idempotent.rs`, `.../messaging.rs`, migration `0030_idempotent_send_publish_state.sql` |
| 2 | reservation acquire is a claim (generation, lease, `may_settle`) | **open, not started** | see "the reserve half" below |

### Why item 3 is not a type swap

`workflow_effects::EffectState` and the framework's `elm_effect_journal::EffectState`
are two identical enums (nine variants, the same wire strings). Swapping toron's for
the framework's would change the **serde** shape — the framework derives
`Serialize` without `rename_all`, so the variant name (`Prepared`) would replace the
snake_case form (`prepared`) in the `wfx` change log. 448 live
`workflow_effect_attempts` rows in the live `index.sqlite` carry the snake_case form,
and the projection's `insert()` drops a row it cannot parse
(`crates/toron-core/src/index/workflow_effects.rs:686`). A swap would silently empty
the projection's attempt history. The vendored crate is hash-pinned, so the alias
cannot be added there. The core enum stays the persisted one; the framework's copy is
upstream's, and the golden-table test is what keeps them equal.

## Item 1, landed: the send is a durable command keyed by its event id

### The hole it closes

The legacy send claimed its idempotency key BEFORE the bus publish and wrote the
**intended** delivery set into the claim row. A crash between the claim and the
publish therefore left a claim row for a rumor that never hit the bus, and every
retry inside the 24h TTL **suppressed** the send: the mail was lost. That residual
risk was written down in the module doc of
`crates/toron-core/src/mailbox/idempotent.rs` ("a crash between the claim insert and
the publish leaves a claim row for a rumor that never hit the bus; retries within
the TTL suppress instead of resending").

### The shape it takes now

The claim row **is** the durable command. It carries:

- `rumor_created_at` — the rumor's `created_at` (unix seconds). It is part of the
  event id, so a retry that re-publishes an unfinished claim rebuilds the **same**
  event id. The key IS the event id.
- `published` — 1 once the publish completed.

The send therefore has three outcomes instead of two:

| claim state | what the retry does |
|---|---|
| `published = 1` | replay the stored deliveries, publish nothing, ring nothing (the crash-after-publish case, unchanged) |
| `published = 0`, `rumor_created_at` present | **re-publish** the same event (rebuilt from the stored timestamp): the mail was never sent, and the recipient still sees one message because the projection dedupes on the event id (`crates/toron-core/src/mailbox/unwrap.rs:891`) |
| `published = 0`, no stored timestamp | a row written before this change: the event cannot be rebuilt, so it keeps the old suppress behaviour, bounded by the TTL |

A partial publish (some recipients confirmed, some not) leaves the claim
unpublished, so the next retry re-publishes the whole set: the recipients that
already got it dedupe on the event id, the ones that did not get it now do.

### Proof

- `crates/toron-core/src/mailbox/messaging.rs` —
  `idempotent_send_retry_after_crash_before_publish_still_sends`: seeds a claim
  with no publish, then asserts the retry **sends** (count 1, one rumor on the bus)
  and that the retry after that converges on one event id with no second rumor.
  Inverting the fix (the re-publish arm made unreachable) turns it red:
  `assertion left == right failed: a crash before the publish must not lose the mail / left: 0 / right: 1`.
- `crates/toron-core/src/mailbox/idempotent.rs` —
  `idempotent_send_claim_first_wins_then_duplicate`: the claim carries the rumor's
  timestamp and the `published` flag, and `mark_published` settles it.
- `mbx test -p toron-core` → 906 lib tests + every integration test pass;
  `mbx test -p toron-workflow` → 326 + every integration test pass.
- `mbx clippy -p toron-core --all-targets` and `mbx fmt --check` are clean on the
  touched files.

### What it does NOT do

- It does not use the **effect journal** (`toron_core::journal::JournalDb`, the
  `kernel::JournalStore` over `effect_journal`) as the carrier. The claim row
  already gives the same guarantee (intent before the provider, one settled
  outcome, a key that is the event's identity) in the store the send already owns,
  with no new table and no extra write on the send's hot path. Porting the carrier
  to the journal is a separate, larger change; see the open decision below.
- It does not change the legacy plane's other behaviour: the receipts store, the
  wake ring, the thread/ack machinery and the recipient side are untouched.

## Item 2, open: the reserve half

The lease board already fences: a per-coordinate monotonic epoch
(`crates/toron-core/src/mailbox/reservations.rs:119-166`), a presented-epoch check
(`check_claim_epoch`), and an atomic conflict floor inside the acquire (removing it
is a named anti-pattern). What the port asks for is the framework's claim shape —
`generation`, a lease deadline, and `may_settle` — carried by the **effect journal**
(`crates/toron-core/src/journal.rs`).

That is an architecture decision, not a mechanical port, because it changes **which
store owns the claim truth**:

1. The 30031 lease board is what the commit guard reads
   (`crates/toron-core/src/mailbox/guard.rs`), what `toron reserve list` shows, and
   what every agent's reservation contract is written against. It is a bus
   projection (lossless, replayable), not a table.
2. The effect journal is a SQLite table with a generation and a lease — the right
   shape for a claim, but a **second** source of truth for "is this path leased".

### The decision

> Which store is authoritative for a reservation acquire: the 30031 lease board
> (today) or the effect journal (the port's claim)?

- **Option A — the journal claims, the board announces.** The acquire writes a
  journal claim (generation + lease) keyed by the lease coordinate, then publishes
  the 30031 event. The board stays the guard's view; the journal is the claim
  record. Two stores to keep consistent, and the guard still reads the board, so a
  journal-only claim would not fence a commit.
- **Option B — the board keeps the claim, the journal records the effect.** The
  acquire's existing epoch IS the generation, its expiry IS the lease, and
  `check_claim_epoch` IS the `may_settle` fence; the journal row then records the
  acquire as a durable command so a crash between the claim and the publish is
  reconcilable. One truth, one carrier, and the port's law is satisfied by the
  journal rather than by the board.

Option B is the smaller change and keeps the commit guard's floor intact. It is
still the captain's call, because it decides whether the journal is the claim or
the record of the claim.

## The other open decision

Whether the send's carrier moves to the effect journal (item 1's "the journal
carrier" step). It is not required for the guarantee — the claim row is a durable
command with the event id as its key — but the port's letter says the journal.
