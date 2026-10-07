# ADR-0038: The operator key lives in the config home, one resolution for everyone

**Status:** accepted (2026-09-30)
**Supersedes:** the implicit data-root default in ADR-0007 D3 for the operator key only (the data root itself is unchanged).
**Governs:** `crates/toron-core/src/config/mod.rs`, `crates/toron-cli/src/operator.rs`

## Context

Two conventions for the operator key had grown up together, and nothing joined them:

- `ToronPaths::operator_key_path()` resolved `<data root>/operator/operator.ncryptsec`,
  and the MCP host's implicit fallback (`TORON_OPERATOR_KEY` unset) **minted** a key
  there on first use.
- `toron key generate --role operator` resolved `~/.config/toron/operator_nsec`, and
  the quick-start guide and the toron skill tell an operator to
  `export TORON_OPERATOR_KEY=$HOME/.config/toron/operator_nsec`.

Both are reasonable in isolation. Together they produced a live defect: on the machine
this was written for, the two paths held **different keys** (`d5c811a8…` in the config
home, `76e3fb4b…` in the data root), so which identity signed an operator-gated action —
`reserve release-by-reason`, force-release, overseer send — depended on the entry point.
The running daemon's unit pointed at the data root while the documentation an agent
follows pointed at the config home. The unit's `ensure_operator_key` made it worse: when
its path was empty it silently minted a *new* identity rather than reporting the
disagreement.

This is the same class as the agent-pin collision that ADR-0037 addresses: one name, two
keys, and a send that reports success against a key the reader never resolves.

## Decision

1. **The config home is canonical.** The operator key resolves
   `$XDG_CONFIG_HOME/toron/operator_nsec` > `~/.config/toron/operator_nsec` >
   `operator_nsec`. It is a secret, so it belongs beside the passphrase (same directory,
   same 0600 discipline), not in the data root with the bus and the search index.
2. **One resolution, one name.** `config::operator_key_path()` is the only derivation.
   The daemon unit, the mailbox host fallback, `toron key generate --role operator`, and
   `doctor` all read it, so producer and consumers cannot disagree by construction.
   `ToronPaths::operator_key_path()` remains as a delegating accessor so existing callers
   (including the sibling `flywheel` repo) do not move, and documents that it deliberately
   ignores `self.root`.
3. **Explicit config still wins.** `operator_key` / `TORON_OPERATOR_KEY` outranks the
   default, unchanged. The default is the *fallback*, not an override.
4. **Never mint a second identity.** `operator_key_source(canonical_exists,
   legacy_exists)` is a pure decision with three outcomes: load, mint, or REFUSE. A key at
   the legacy data-root path with none at the canonical path refuses loudly and hands over
   the exact `mv`. Explicit `toron key generate --out` is untouched — an operator asking
   for a new key may have one; only an *implicit* side effect must never create a second
   identity.
5. **The old location is reported, never loaded.** `legacy_operator_key_path()` exists so
   the mint guard and `doctor` can name it. `doctor`'s `keys` check reports which source
   resolved (configured vs canonical default), and a legacy-only key is a red that names
   both paths instead of a bare "not configured".

## Why not the data root

The data root is where non-secret durable state lives (the bus store, the index, project
roots), and it is the directory most likely to be relocated, backed up, or synced. A
secret does not belong there, and the passphrase precedent already put the sibling secret
in the config home. Choosing the data root would also have required rewriting the
quick-start guide and the skill, which are the two surfaces an agent actually reads.

## Consequences

- A machine with a key only at the old data-root path fails loudly with a `mv` until the
  operator chooses. That is the intended cost: the alternative is picking an identity for
  them.
- A daemon unit installed before this ADR still names the old path in its
  `TORON_OPERATOR_KEY`. Re-run `toron daemon install` to re-point it; until then the
  unit keeps its own explicit pin, which is honored (rule 3). This ADR does not rewrite a
  live unit behind the operator's back.
- `ToronPaths = { root }` still hands back a config-home path through
  `operator_key_path()`. That is a documented wart kept for API compatibility with
  callers outside this repo; the honest shape is the free function.
- The agent and anchor key roles still resolve through `~/.config/toron/<role>_nsec`
  derived from `HOME` alone, so they ignore `XDG_CONFIG_HOME`. Out of scope here; noted so
  the asymmetry is deliberate rather than forgotten.

## Verification

- `config::resolve_operator_key_path` (pure, env passed in) — XDG wins, HOME falls back,
  neither keeps the relative name, and the result is never under the data root.
- `config::operator_key_source` — all four existence combinations; mint only when no key
  exists anywhere.
- `cli_operator::doctor_keys_check_resolves_the_canonical_config_home_key` — a real
  subprocess against an isolated `HOME`: absent ⇒ red, canonical ⇒ green naming the path,
  legacy-only ⇒ red naming both paths.
