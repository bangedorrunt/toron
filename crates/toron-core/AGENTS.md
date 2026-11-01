# toron-core

Mailbox fabric: signed events on a `Bus`, projected to SQLite + Git archive.

## WHERE TO LOOK

| Concern | Path |
|---------|------|
| Public mailbox trait | `src/mailbox/surface.rs` (`MailSurface`, `&self`) |
| Tool impls (send/inbox/ack) | `src/mailbox/messaging.rs` |
| String ids | `src/mailbox/ids.rs` (`MessageId` — never u64) |
| Reservations / intent | `src/mailbox/reservations.rs` (30031 `intent→held→released`; largest file — acquire/renew/force + request-release) |
| Gift-wrap | `src/nostr/gift_wrap.rs` |
| Kind constants | `src/nostr/kinds.rs` |
| Identity / 30013 | `src/nostr/identity.rs` |
| InProcessBus | `src/bus/in_process.rs` |
| RelayBus | `src/bus/relay_bus.rs` |
| Project `#t` scoping | `src/bus/scoping.rs` |
| Git archive | `src/archive/git.rs` |
| Effect journal | `src/journal.rs` (fallible `JournalStore` over SQLite; sync bridge) |
| Durable engine store | `src/index/durable_engine.rs` (fallible `EngineStorage`; additive `durable_engine_*` tables) |
| Projection / FTS | `src/index/` |
| Keys (ncryptsec) | `src/keys/` |

Effect law for every core and its tests is the root `AGENTS.md` section of that name. Load `~/.agents/skills/effect-oriented/SKILL.md` before editing `dispatch_core`, receipts, or judgment.

## CONVENTIONS

- `Mailbox` is the concrete type; `MailSurface` is the embedder trait.
- One dedicated SQLite write connection. WAL. i64-µs timestamps.
- Gift wraps: no project tags. Timeline orders by rumor time, not wrap time.
  Unwrap verifies the envelope Schnorr sig before NIP-44 decrypt. `gift_wrap`
  is sync (`GiftWrapBuilder`, rust-nostr 0.45).
- Presence (kind-20001) is ephemeral — never persist to SQLite or Git.
- `GitArchive` stages worktree + pending bytes; `commit` builds an in-memory
  tree from fresh HEAD + pending. Never write the live libgit2 index.
- Force-release is operator revocation. Projection priority: operator > agent.

## ANTI-PATTERNS

- Wrap pubkey as sender identity.
- BCC on the visible rumor. Public acks/reads as replaceable events.
- `t` tag on metadata-hiding kinds. harness/model as `t` tags (they live in 30013 content).
- Raw `bus.publish(kind-14)` counted as NIP-17 mail.
- Replay that duplicates a Git commit or index row.
- nsec outside `keys/`. Never `{:#?}` a `SecretKey`.
- Removing the atomic reservation conflict-check floor.
