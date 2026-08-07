# Nightly features: what they'd buy toron, measured (2026-08-07)

> Probe: direct `rustc` compile of each candidate on the **pinned** toolchain
> (`rust-toolchain.toml` → nightly 1.97.0, 52b6e2c20 2026-04-27), edition
> 2021 vs 2024, with and without `#![feature]` gates. Compile = ground truth,
> not the unstable-book (whose pages for let-chains / if-let-guard /
> async-closures now 404 — i.e. those features are no longer tracked as
> unstable).

## Feature map (verified on pinned nightly 1.97.0)

| Feature | Status | Evidence |
|---|---|---|
| `let_chains` | **STABLE**, edition 2024 only | no-gate compile clean in ed2024; "only allowed in Rust 2024" in ed2021 |
| `if_let_guard` | **STABLE** | no-gate compile clean in ed2024 |
| async fn in traits (AFIT) | **STABLE** (since 1.75) | no-gate compile clean |
| async closures (`async \|\|`) | **STABLE** | parses/compiles no-gate |
| `let_else` | STABLE | sanity probe |
| `gen_blocks` / `yield_expr` | **unstable** | E0658, needs gates |
| `return_type_notation` | **unstable** | E0575 |
| `exhaustive_patterns` | **unstable** | unstable-book page still live |
| `type_changing_struct_update` | **unstable** | unstable-book page still live |

## What this means for toron

toron pins **nightly** and uses **zero `#![feature]` gates** (verified:
no `#![feature]` anywhere in `crates/`). So the nightly pin currently buys
nothing, while exposing every build to nightly-only codegen behavior (we
just chased the `strip=debuginfo` release bug — a cargo default, but the
class of surprise is real).

Every feature that would genuinely improve this codebase is **already
stable** — the catch is edition: `let_chains` / `if_let_guard` require
**edition 2024** (stable since rustc 1.85; toron is on edition 2021).

The still-unstable features have **no real toron surface**:

- `gen_blocks` — sync generators; toron's pipelines are async (tokio
  streams/broadcast). `mock_relay.rs` is the only iterator-ish site; a
  sync gen would not improve it.
- `return_type_notation` — only relevant for generic Send-bounded AFIT
  usage; toron uses the `async-trait` crate (boxes futures) today, so RTN
  is moot until/unless that migration happens — and the pragmatic
  migration path (`trait-variant` crate) doesn't need RTN.
- `exhaustive_patterns` — toron's enums (ContactPolicy, GuardOutcome,
  ProjectOutcome…) are already exhaustively matched; no `!`/uninhabited
  variants to exploit.
- `type_changing_struct_update` — zero struct-update (`..base`) sites in
  the codebase.

## The improvement path (all stable)

### 1. Edition 2021 → 2024 (highest leverage)
Unlocks `let_chains` + `if_let_guard` everywhere, plus edition-2024
semantics (RPIT lifetime capture, match-ergonomics ref bindings, `gen`
keyword reserved, `unsafe_op_in_unsafe_fn` — the latter N/A: toron has
`#![forbid(unsafe_code)]`).

Concrete rewrite targets (nested `if let` → `if let … && let …`):
- `toron-workflow/src/coalesce.rs:303` — `if let Some(buf) = … { if buf.mentions.is_empty() { … } }`
- `toron-core/src/mailbox/unwrap.rs` — tag scans in `is_overseer_bypass`
  (t-tag check + importance check), `tag_text`/`ts_ms_tag` find_map chains
- `toron-workflow/src/guard.rs`, `trigger_filter.rs`, `lint.rs`,
  `mention.rs`, `jobs.rs` — the `if let Some(x) = … &&` patterns
- `toron-cli/src/operator.rs`, `config.rs` — nested option handling

### 2. Replace `async-trait` crate with native AFIT (stable) — DONE, with a correction

Measured correction (probe, nightly 1.97): **both** native AFIT and RPITIT
(`-> impl Future + Send`) traits are **non-dyn-compatible** (E0038) —
`trait-variant`'s Send-variant adds Send bounds, NOT dyn-compat. So the
plan's mechanism was wrong for the dyn traits.

Shipped (commit 2a617a5):
- `Backend` (never dyn-used) → native AFIT. Removed async_trait from the
  trait + 3 impls (herdr/tmux/mock) + the Box<dyn Future> boxing.
  `#[expect(async_fn_in_trait)]` justified (futures Send by construction;
  no caller spawns cross-thread). MockBackend's trait-object test converted
  to a generic static-dispatch proof.
- `Bus` (Arc<dyn Bus>, 16 sites) + `ProjectionSink` → **keep async-trait**
  (dyn-compat requires boxed futures); rationale documented on the trait.
- async_trait annotation sites 10 → 6; dep retained for the dyn traits.

### 3. Drop the nightly pin → stable — DONE
`rust-toolchain.toml` now `channel = "stable"` (resolves to rustup stable
1.97.1). Verified on stable: 37 workspace test binaries green, clippy
--workspace --all-targets -D warnings clean, release build green, gates
pass. Note: the local shell's nix-profile `cargo-clippy` (nightly driver)
shadows rustup's on PATH — use `cargo +stable clippy` or the rustup binary
path; CI (rustup-based) is unaffected.

## Recommendation

Do (1) now — edition 2024 + a let-chains/if-let-guard pass — then (2)
AFIT, then (3) drop to stable. The user-visible payoff of "nightly
features" for toron is real but it is delivered by **stable, modern
Rust + edition 2024**, not by `#![feature]` gates. Filed as bead (bd-3wc).
