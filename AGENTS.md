# toron

Nostr-native agent mailbox: one `toron` binary (daemon, MCP server and robot
CLI). Flywheel embeds toron, never the reverse. Dependencies run one way only:
**flywheel → toron-workflow → toron-core**.

Ops crib: `~/.agents/skills/toron/SKILL.md`. ADRs: `docs/decisions/` (they are
history; `0001-*` is superseded). One root `AGENTS.md` only, enforced by
`scripts/verify-gates.sh`.

## Structure

```
crates/toron-core/      mailbox, bus, nostr, archive, index, events, pane types + port, keys
crates/toron-plane/     pane plane: rex/hc backends, OSC 7501 board
crates/toron-workflow/  YAML engine, cron, channels/jobs
crates/toron-mcp/       rmcp server (stdio + HTTP /mcp), MCP Events
crates/toron-cli/       bin `toron` (+ TUI)
crates/toron-e2e/       SC-* scenarios (feature-gated)
docs/catalog/           generated MCP surface (toron.dev)
workflows/              product YAML
```

## Where to look

| Task | Location |
|------|----------|
| Mail send/inbox/ack | `crates/toron-core/src/mailbox/` (`MailSurface`, `MessageId`) |
| Reservations | `crates/toron-core/src/mailbox/reservations.rs`; CLI `toron reserve …` |
| Receipts / send tags / identity | `crates/toron-core/src/mailbox/{receipts,send_tags,identity}.rs` |
| Gift wrap / kinds / identity | `crates/toron-core/src/nostr/` |
| Bus / federation | `crates/toron-core/src/bus/` |
| OSC 7501 board | `crates/toron-plane/src/osc7501.rs` (`fold` + `FoldPolicy`) |
| Multiplexer backends | `crates/toron-plane/src/backend/` (`plane.rs` resolves rex first, hc fallback), injected through `toron_core::plane_port::PanePlane` |
| Workflow engine | `crates/toron-workflow/src/{schema,engine}/`; guide `docs/guides/workflow-engine.mdx` |
| Cron | `crates/toron-workflow/src/cron.rs` (not `trigger: schedule`) |
| Effect journal / durable engine | core index migrations, `RunStateStore`, `crates/toron-core/src/index/durable_engine.rs` |
| MCP tools / resources | `crates/toron-mcp/src/{tools,resources}.rs`, `catalog.rs` |
| MCP Events (SEP-3415) | `crates/toron-core/src/events/`, `crates/toron-mcp/src/events/`, `events_stream.rs` |
| TUI (Elm core) | `crates/toron-cli/src/tui/` (pure `tick`) |
| Daemon / CLI / build identity | `crates/toron-cli/src/{cli,daemon,main}.rs`, `crates/toron-core/src/serve_build.rs` |

## Crate rules

- **toron-core:** one SQLite write connection, WAL, i64-µs. Gift wraps carry no
  project tags; unwrap verifies Schnorr before NIP-44. Presence (20001) is
  ephemeral and never persisted. Never write the live libgit2 index in
  `GitArchive::commit`. Force-release is operator revocation. Never `{:#?}` a
  `SecretKey`.
- **toron-workflow:** never imports flywheel. The embedder injects
  `ProfileResolver` / `WorkflowResolver` (default `None`, which fails closed).
  DAG `edges:` are forbidden; derive them from `needs`. `Enforced` refuses a
  live effect with no runner; `Recording` is the dry arm.
- **toron-mcp:** `rmcp` + Tokio, `#[tool]`/`#[resource]` + schemars only. Tool
  and resource names are frozen unless an ADR says otherwise; the published
  catalog is the AM-Rust parity subset (`catalog.rs`, `catalog_freshness`).
  MCP Events follows the Final spec (`io.modelcontextprotocol/events`). Blossom
  lives at `/.blossom` and never collides with `/mcp`. No passphrase or nsec in
  any response. Intentional shape diffs: `docs/parity/known-divergences.md`.

## Conventions

- Edition 2024. `#![forbid(unsafe_code)]` crate roots and ADR markers are
  enforced by `scripts/verify-gates.sh`.
- No new kinds or MCP tools without an ADR, a catalog regen and tests.
- CLI flags: the acting agent is always `--as`; project-scoped commands require
  `--project`. Channel posts are kind 9.
- `MessageId` is hex `String`, never `u64`. The `t` tag is the project slug only.
- Agent pins are CamelCase AdjectiveNoun (`nostr::identity::validate_agent_pin`),
  one key per pin.
- `corr` is minted by the sender; sqlite owns `owed → acked → resulted`.
- Published MCP strings stay free of `bd-*` ids (`catalog_no_internal_ids`).
- Beads: `br --json` / `--robot-*`; close through the br skill ceremony.
- TDD. No benches. Cite a principle with `flywheel principles list`.

## Working in the shared tree

Flywheel builds toron-core from this tree, so a broken file here breaks every
flywheel build. Crews follow the rules in `../flywheel/AGENTS.md`:

- Reserve the files you edit (`toron reserve acquire --project flywheel --paths
  ../toron/<path>`), keep the tree compiling between steps, and commit only with
  `flywheel commit <paths> --repo ../toron`.
- Never stage by hand, and never `git add -A`, `stash`, `checkout --`,
  `restore`, `reset`, `rebase` or `--amend`.
- Run cargo through `mbx` with no `CARGO_TARGET_DIR`.
- Before you mail a sha: `bash scripts/fence.sh --sha <sha>` plus the targeted
  tests for what you touched. A toron API change also needs a `proof.toml` bump
  in flywheel; the captain's flywheel proof covers it.

## Commands

```bash
mbx test -p <crate> <filter>
bash scripts/fence.sh --sha <rev>     # -Dwarnings build, clippy, rustfmt, verify-gates on a clean export
bash scripts/fence.sh --index         # the pre-commit hook (scripts/hooks/install.sh once per clone)
bash scripts/verify-gates.sh
bash scripts/e2e-behavior.sh          # daemon-free channel/job/mail/supervisor matrix
PATH="$PWD/target/debug:$PATH" toron catalog --json && mbx test catalog_freshness
mbx test -p toron-e2e --features p1   # also p2, p4
toron daemon status --json
```

"Fence green" means `scripts/fence.sh`, never `verify-gates.sh` alone.
`mbx test --exclude-ignored` is invalid.

## Effect law

Binding skill: `~/.agents/skills/effect-oriented/SKILL.md`. A core is
`tick(&Model, &Msg) -> (Model, Vec<Effect>)` with no clock, env or network.
Live effects journal before the provider runs. Prove each test can fail.

## Test sandbox (hard)

1. `launchctl`/`systemctl` deny-shims first on `PATH`; their logs stay empty.
2. Temp `HOME` and XDG dirs, with an explicit `TORON_DATA_DIR` for mailbox tests.
3. Spare ports only: never live `:18080` / `:18980`, the live store
   `~/.local/share/toron/`, a live hc session or `~/.config/herdr-core`. Never
   rewrite the operator passphrase or nsec.
4. Never print `operator_nsec`.

## Skill sync

CLI/MCP change → `Command` enum + tool attrs + this file +
`~/.agents/skills/toron/SKILL.md` + the catalog (`toron catalog --json`,
`catalog_freshness`) when the surface grows.

## Anti-patterns

- `asupersync` / `fastmcp`. Nested `block_on`. Silent-success stubs.
- Cross-key NIP-09 delete. Per-pid temp mailbox roots.
- Removing the atomic reservation conflict check.
- Headless code faking pane lifecycle kinds.

## Notes

- Daemon label `com.toron.serve`, HTTP `127.0.0.1:18080`. `toron serve` needs
  `--mcp-stdio` or `--mcp-http`. Passphrase `~/.config/toron/passphrase` and
  operator key `~/.config/toron/operator_nsec` (both 0600).
- Bare `toron` may be a stale install; use `target/debug` for catalog work.
- Sibling orchestrator: `../flywheel` (pane plane rex, hc fallback). Rex docs:
  https://www.superlogical.com/rex/docs. hc docs: https://herdr.dev/core/
