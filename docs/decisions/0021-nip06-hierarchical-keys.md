# ADR-0021: NIP-06 Hierarchical Agent Keys — One scrypt Total?

**Status:** Rejected (2026-08-18; decision recorded by flywheel-toron-coder,
open for captain overrule)
**Related:** bd-epic-nip06-keys-kjsz, bd-unwrap-ptag-lazy (p-tag lazy unwrap,
wave-1), bd-harvest-thread-filter-yp9r (content-keyed KDF cache, R2.0),
ADR-0017 (identity continuity across roots), ADR-0007 R2 (durable XDG root),
ADR-0018 (identity naming grammar)
**Spec:** NIP-06 (BIP-39 mnemonic + BIP-32 derivation), NIP-49 `ncryptsec`

## Context

The 2026-08-17 optimization inspection measured one scrypt per cold unlock
(~0.2s release, log_n 16) per `key.ncryptsec` file. The p-tag lazy-unwrap
bead (wave-1, landed: `RecipientKeyResolver` in
`crates/toron-core/src/mailbox/unwrap.rs`) removed per-wrap amplification —
the unwrap path resolves the ONE recipient key per wrap, not a keyring. The
content-keyed KDF cache (bd-harvest-thread-filter-yp9r,
`cached_load_agent_key` in `mailbox/identity.rs`) removed warm-path scrypt:
per key file, the first unlock pays the KDF, subsequent unlocks hit the
cache.

Residual cost after both: **one scrypt per DISTINCT agent whose mail is
unwrapped by a fresh process** — the first delivery to each recipient after
process start. Production fleet: 119 `key.ncryptsec` files across the durable
root (`projects/`: toron 53, flywheel 14, Users-path roots 48, misc 4 —
measured 2026-08-18; the inspection's 117 was the same set one day earlier).

NIP-06 would replace per-key files with one encrypted master + BIP-32-style
HMAC-SHA512 child derivation (microseconds per child, `nostr-sdk` ships a
feature-gated `nip06` module). The literature framing (paperclip 2026-08-17):
Qatsi (arx_2510.18614) shows stacked memory-hard hierarchies stay expensive
per layer — the win comes specifically from switching layers 2+ to HMAC;
cost-asymmetric MHF analysis (arx_2206.12970) frames the
derive-once/verify-cheap asymmetry that makes the current per-key design
defensible too. This is a tradeoff ADR, not a one-sided win.

## Decision

### D1 — REJECT full adoption: identity continuity is unmaintainable

BIP-32-style derivation produces keys **deterministically** from
(master, path). The 119 production keys were randomly generated; no master
can reproduce them. "Adoption" of an existing identity is impossible without
re-registration under a NEW npub.

What re-registration breaks (the exact failure classes ADR-0017 repaired):

- **Mail history projection**: unwraps are keyed to the recipient npub; a
  re-registered agent cannot unwrap its own history, and thread
  authorization (ADR-0017 D4) loses its recipient anchor.
- **Contacts / social graph**: AgentLink rows reference npubs.
- **Reservations / build slots**: `agent_pubkey` is the holder key.
- **Ack authorization**: the (a) recipient-keys path dies.
- **30013 profiles / agents rows / agent-names.json pane bindings**: all
  keyed by npub or identity name.
- **Archive provenance**: the permanent Git archive records events under the
  original npub.

A "re-register once" migration converts a one-time optimization into a
fleet-wide identity break with no honest winner — the same merge-safety
argument ADR-0017 D3 used to reject row movement applies, one level down.

### D2 — REJECT the mixed model (NIP-06 for new agents only)

The one-scrypt win applies ONLY to newly-created identities. Existing keys
are identities that never retire — the production fleet's cold-start cost
would stay at 119 × scrypt forever, minus only the handful of agents created
after the flip. The mixed model pays: a derivation-path registry (name →
index), a derivation-aware key resolver alongside the file resolver, dual
at-rest models, and index-reuse risk (key file deleted/recreated → path
collision) — for a bounded, near-zero win. `subtract-before-you-add` and
`foundational-thinking`: a second key-management model is a permanent
tax, not a migration.

### D3 — The residual cost is small and already shrinking

The win under the best case (full adoption) is one scrypt (~0.2s) at process
cold start instead of N_active × 0.2s. N_active is the set of agents whose
mail a fresh process actually unwraps — for the current fleet, single
digits. The KDF cache means the cost is paid once per process lifetime, not
per operation. 0.2s vs ~1s on a daemon that runs for hours is not an
operational concern; the inspection's headline (per-wrap amplification) was
already closed by wave-1.

### D4 — Security regression: one master is total + permanent

All agent keys already share one passphrase today (`agent_passphrase`,
default `toron-dev`), so per-key compartmentalization is already nominal —
but a NIP-06 master makes compromise **structural**: one leaked master
(backup, memory dump, supply chain) exposes EVERY current AND FUTURE derived
key, and there is no rotation — changing the master changes every derived
npub, i.e. the entire fleet must re-register. Per-key files bound a
compromise to one identity and rotate independently. arx_2206.12970's
asymmetry (derive-once/verify-cheap) makes the per-key design defensible
precisely because the verify side is cheap and the compromise side is
bounded.

### D5 — Recorded dimensions (moot under rejection, decided for the record)

- **At-rest model**: under adoption, one encrypted master PER PROJECT (not
  per data dir) — matches the existing per-project key layout and limits a
  master leak to one project. Moot: rejected.
- **Passphrase chain**: reuse the existing `TORON_KEY_PASSPHRASE` →
  `agent_passphrase` chain (one chain, no new secret). Moot: rejected.
- **Hardened derivation only**: required under adoption — non-hardened
  children expose the parent chain code; hardened children still expose
  siblings on master compromise. Moot: rejected.
- **Interplay with lazy-unwrap**: the `RecipientKeyResolver` stays — a
  NIP-06 resolver would be one more backend behind the same trait. The
  resolver is NOT the problem the epic set out to fix; the key store is.
  Moot: rejected.

### D6 — Revisit trigger

Revisit NIP-06 (new-agents-only) if and only if measured cold-start
unlock cost becomes operationally significant: fresh serve processes
unwrapping mail for ≥50 DISTINCT agents, or the per-key KDF is re-parameterized
to ≥1s. Evidence-gated, not scheduled.

## Consequences

- The per-key NIP-49 store remains the at-rest model; no code change.
- The one-scrypt-per-process ideal is NOT pursued; the actual residual cost
  is bounded by lazy-unwrap + KDF cache and is a cold-start, once-per-process
  event for a small active set.
- Identity continuity (npubs, history, contacts, reservations, acks) is
  preserved — the rejection is the migration-safety decision, not a
  performance verdict.
- Rejection is a valid close for this epic per its acceptance criteria; the
  ADR records the captain-visible decision and the revisit trigger. No
  implementation beads are spawned (the epic's criterion #2 fires only on
  acceptance).

## Verification

```bash
ls docs/decisions | grep -i nip06
```

Accepts the ADR file. No behavior changed — the repo's full suite is
unaffected by this ADR (documentation-only decision):

```bash
cargo test -p toron-core --lib keys      # key store untouched, still green
```
