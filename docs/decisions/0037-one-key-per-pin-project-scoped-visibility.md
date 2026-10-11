<!-- governed-by: ADR-0003, ADR-0014, ADR-0017, ADR-0018, ADR-0033 -->
---
status: accepted
date: 2026-09-29
decision-makers: bangedorrunt
consulted: bd-j5sk live measurement (2026-09-28) + bd-0tbg live board measurement (2026-09-29)
informed: flywheel x toron x beads maintainers
---

# ADR-0037: One Key per Pin — Project-Scoped Visibility, Not Project-Scoped Keys

**Status:** Accepted — captain word 2026-10-11 18:44 AEDT, relayed by the flywheel bot for the stack-comms build (factory task `23283d6f`), as a hard prerequisite of the comms redesign (redesign §5, §11 Phase 0). The operator accepts the §6.1 blast-radius trade. Acceptance cuts the §7.1 waves as beads; bd-j5sk stays open until W1–W4 land and the §5 migration runs. Implementation status at acceptance: W1–W5 not started (see §10).

**Decision in one sentence:** An agent pin resolves to exactly ONE key across every project it is registered in, and the project becomes a property of the *message* (already carried on the rumor's `t` tag) rather than of the *key*, so `mail send --to <pin>` is deliverable in any project without the recipient ever needing an npub.

**Extends:**

- [ADR-0003](0003-nostr-mailbox-fabric.md): NIP-17 gift-wrap encryption is unchanged. This changes which key a name resolves to, not how mail is sealed.
- [ADR-0014](0014-kind-registry-specification.md): no new kinds. `t` (project slug) and the existing `p`/`#p` tags already carry everything visibility needs.
- [ADR-0017](0017-project-root-canonicalization.md): project *slugs* keep canonicalising. This ADR removes project from the *identity* axis, not from the *routing* axis.
- [ADR-0018](0018-agent-identity-naming-grammar.md): the naming grammar, the CamelCase Adjective+Noun rule, and the `agent_name_aliases` fold table are all unchanged. This ADR makes the "one identity" that ADR-0018 already assumes actually true at the key layer.
- [ADR-0033](0033-additive-tools-kinds-discipline.md): the frozen 40-tool / 25-resource surface is untouched. No new tool is required — `whois`, `mail send`, `register`, and `agent prune` gain behaviour, not surface.

**Amends:** nothing. It closes bd-j5sk's stated open half ("it is an ADR not a patch") and is the design fix bd-j5sk names.

**Does not amend:** cross-key NIP-09 delete stays forbidden; the `captain` alias stays a per-project fold; `identity_mode` (swarm/open) keeps its per-project meaning; the durable XDG root and the "never a second store" rule are untouched.

**Does not wire:** key escrow or recovery (a lost key is still a lost key); multi-device per-pin key rotation; a HSM or signer tier.

**Tracker:** bd-j5sk (this ADR's rationale and live evidence). Migration beads are cut on acceptance — see §7.3.

---

## 1. Scope and fixed point

Goals:

1. **A name is deliverable.** `toron mail send --project A --to <pin>` reaches that agent when they read under project B, and the send either delivers or fails loud. "Reported `ok`, visible to nobody" must not be a reachable state.
2. **One pin, one key, enforced.** A second key for a taken pin becomes impossible to mint by accident, not merely discouraged in prose. `docs/guides/identity.mdx` already says "One pin, one key"; this ADR makes the code agree with the sentence.
3. **Project scope survives as visibility.** A message still records the project it was sent in, so per-project views, retention, and audit keep working. Nothing becomes globally visible by accident.
4. **The warning retires.** `ambiguous_name_warning` (identity.rs:332) exists to replace an unanswerable inference with a fact. Once the fact is structural, the warning has nothing left to say and is deleted rather than left to rot.

The fixed point is: **one key per pin; project is a filter on the message, not a partition of the keyspace.** Not one key per human, not a global directory, not a relay.

### 1.1 Non-goals

- Not one key per *person*. One pane, one pin, one key — a person running two panes is two pins and two keys, by design (that is what makes revocation per-agent possible).
- Not federated identity. No cross-machine key discovery; a pin's key is a local fact plus a published profile event.
- No change to what a *folder*, channel, or job visibility means. This ADR is about mail addressing.
- No retroactive re-keying of the 766 historical identities from bd-yegs without the per-identity decision in §5.2.

---

## 2. Context (evidence, with file pointers)

The defect was found by live test, not by reading. bd-j5sk, 2026-09-28:

```
whois flywheel  Buffy -> 6778eb30...
whois toron.dev Buffy -> 71998e0b...     both report pane_id w1W:p1
```

One pane, two keys. The send reported `count=1` OK and the target's inbox did not contain the message. Not lost in transit — **invisible**, which is the worst failure mode available: a success report with no reader.

bd-0tbg then found the same split *inside one project* on the same words: `BrashOtter` → `d335c51c…` and `brash-otter` → `93bb5e31…`, both live, holding 4 and 5 reservations respectively. `agent bootstrap` refuses the kebab spelling outright, so the pin rule bites on new registrations and has never been applied to the old ones.

What the code does today:

| Layer | Shape | Evidence |
|---|---|---|
| Key material | keyed by **(project, name)** — `<project_root>/agents/<name>/key.ncryptsec` | `identity.rs:954-959` (`agent_key_path`), called from `messaging.rs:1618` (`agent_keys_for`) |
| Identity table | `agents(project_slug, name, pubkey)` — no uniqueness across projects | `migrations/0001_schema.sql:5-15` |
| Send resolution | name → key, *through the sender's project* | `messaging.rs:1638-1646` (`agent_keys_for_resolved`) |
| Name ambiguity | detected and **warned**, never blocked | `nostr/identity.rs:291` (`name_across_projects`), `:332` (`ambiguous_name_warning`) |
| **Inbox read** | gift wraps filtered by **`.pubkey(npub)` only** — no project term | `messaging.rs:730-735` (`.pubkey(npub)` at `:732`) |
| Rumor payload | project already recorded as `Tag::parse(["t", &project_slug])` | `messaging.rs:621`, `:1099` |

**The last two rows are the whole decision.** The read side is already keyed by pubkey alone, and the project is already on the wire. Project scope is therefore *not* enforced by the protocol or the read path — it is an emergent property of where the key file happens to live. That is why the fix is small and why nothing downstream needs to move.

The existing warning already names the failure precisely, in its own docstring (identity.rs:337-345): *"from toron.dev, 'Buffy' resolves to the toron.dev key, yet the person at that pane may be the flywheel Buffy and never see the message."* It was written by someone who had already seen this and had no structural fix to point at.

There is a partial escape hatch today — `flywheel mail register <project> --name <pin> --from-project <other>` reuses an npub across projects — but it is opt-in, undocumented as an invariant, and unverified: nothing checks that a pin already registered elsewhere ended up with one key.

---

## 3. Options considered

**Option A — One key per pin (this ADR).** `agents` gains a uniqueness constraint on the key, not the name; registration in a second project reuses the key and records a second `(project, name) → same pubkey` row. Project scope moves entirely to the message.

- For: makes the documented invariant true; deletes the invisible-delivery class outright; read path already matches; migration is additive (extra rows, no row rewrites).
- Against: a key compromised in one project is now readable in all of them. This is a real loosening, not a formality.

**Option B — Keep project-scoped keys, make the send fail loud.** Refuse `mail send --to <name>` whenever the name is bound in another project, naming the npub to use instead.

- For: zero security change, zero migration, smallest diff.
- Against: it does not make cross-project mail *work*, it only makes the failure honest. It leaves the pin namespace split, leaves bd-0tbg unfixed, and leaves `docs/guides/identity.mdx` claiming an invariant the code does not have. It is a mitigation, and this bead asked for a fix.

**Option C — Fan out to every key bound to the name.** Send to all N bindings.

- For: "just works" for the human.
- Against: **it is a confidentiality breach.** If two different agents independently pick the same pin, every message to that pin is delivered to both. The whole CamelCase-pin namespace exists precisely because names collide. Rejected outright; recorded here so it is not re-proposed.

**Option D — NIP-06 hierarchical keys.** Already evaluated as [ADR-0021](0021-nip06-hierarchical-keys.md) and **Rejected** there, for 119 production keys. This ADR does not reopen it; the per-key storage cost of the flat one-key-per-pin model is the same either way.

**Option E — A qualified address form (`pin@project`) as the deliverable.** Keep project-scoped keys; let the sender disambiguate.

- For: tiny; deterministic; no migration; no security change.
- Against: it is a real ergonomic win and it is **not** an alternative to this ADR so much as its compatibility shim — it is what makes mixed-state fleets (some pins migrated, some not) addressable during the transition. **Adopted as part of Option A**, not as a competitor. See §4.3.

---

## 4. Decision

### 4.1 One key per pin, enforced

- A pin is registered **once**. `register_agent` and `create_agent_identity` in a project where the pin already exists elsewhere **reuse** the existing key and add a `(project, name) → pubkey` row. They never mint a second key.
- Reuse requires **proof of possession**: the registering surface signs a challenge with the existing key and the signature is verified before the row is written. This is what stops an unrelated agent from claiming a pin it does not own, which is the entire hazard of a shared namespace.
- The `agents` table gains a partial unique index on `pubkey` per `(project, name)` pair plus a repository-level assertion that one `name` never maps to two `pubkey`s **within** a project. That is the bd-0tbg shape, and it is a straight bug fix independent of this ADR's cross-project half.
- `agent bootstrap` refusing a non-CamelCase pin stays. What is new is that a *legacy* kebab-case holder is detected and reported with its reservations, not silently left to be reclassified as a folded-slug leftover. (`agent prune` already refuses to absorb a holder that still holds leases — bd-0tbg's fix, `e143876` — and this ADR gives that report a first-class home.)

### 4.2 Send resolves by pin, not by project

- `mail send --to <name>` resolves the name to a **key**, not to a `(project, name)` key. The sender's project determines the *routing* (the `t` tag, the d-tag) and nothing about who the recipient is.
- A name that is not registered anywhere fails loud (today's behaviour, kept).
- A name that resolves to exactly one key anywhere in the local store sends. No warning needed; there is no second candidate to warn about.
- `fetch_inbox` needs **no change**. It already filters by pubkey (§2), and a single key now receives mail from every project.

### 4.3 `pin@project` and npub remain valid

Both are strictly more explicit than a bare name and neither becomes illegal:

- `<npub>` — unchanged, always works, still the right answer when you are deliberately reaching one key that is not a registered pin.
- `<pin>@<project>` — the disambiguation form. **Required during the migration window**, when some pins in the store are one-key and some are still per-project. It resolves exactly as today: name in *that* project.
- A bare `<pin>` that matches a `pin@project`-shaped string is parsed as the qualified form only when the suffix names a project that exists; otherwise the whole string is treated as a literal name and fails the "not registered" path. Never guess.

### 4.4 Visibility is a message property

- The `t` project tag is load-bearing under this ADR, where before it was mostly informational. Per-project inbox views filter on it; the default inbox is the whole key.
- Retention, thread grouping, and audit keep their project dimension. Nothing is deleted by this ADR.

---

## 5. Migration

### 5.1 Two-key pins are the hard case

A pin registered in N projects holds N keys today. Collapsing to one is a **per-identity decision with a human in the loop**, not a batch job: for each pin, someone must say which key survives. Guessing wrong either strands the other key's history or silently hands a live holder's mail to an identity it was never addressed to. This is bd-lkjf, and it is the reason §5 is not automated.

### 5.2 Per-identity re-key sequence

1. **Enumerate.** For each project, list pins whose `name` maps to more than one `pubkey`, and pins whose `name` appears in more than one project with different keys. Report; do not act.
2. **Choose the surviving key** (human). The other key's key file is retained, not deleted, until the pin is confirmed delivered under the survivor.
3. **Repoint the extra rows** in `agents` to the survivor. The old key file stays on disk; nothing is `rm`-ed.
4. **Re-key live reservations** so no holder loses addressability, then re-run `toron agent prune` to absorb the now-empty leftover rows. bd-0tbg's `held` bucket is what makes step 4 safe to run at all.
5. **Drain and verify.** Read the survivor's inbox and confirm the old key's mail is now visible; only then release the old key.

### 5.3 Historical debris is out of scope

The 766 identities and the 4 real keys under the path-derived `Users/` tree (bd-yegs) are **not** migrated by this ADR. They are residue from a pre-ADR-0017 fixture era. Touching them is a separate decision with its own blast radius; this ADR only forbids making the problem larger.

---

## 6. Consequences

### 6.1 The security trade, stated plainly

Today, compromising a key in project A exposes only project A's mail. After this ADR, it exposes that pin's mail in **every** project. That is a genuine loosening and it is the price of the ergonomic fix. Mitigations available later, none of them in scope now: per-pin signer tiers, key rotation, and the ability to scope an inbox by `t` tag to reduce blast radius on the *read* side.

This paragraph is the reason the ADR is **proposed** and not accepted.

### 6.2 What gets better

- `mail send --to <pin>` is deliverable in any project. The invisible-delivery class from bd-j5sk is gone, not warned about.
- The pin namespace stops splitting. bd-0tbg's two-keys-one-name shape becomes unrepresentable.
- `ambiguous_name_warning` is deleted: the inference it was built to replace becomes structural.
- `docs/guides/identity.mdx`'s "One pin, one key" stops being an aspiration.

### 6.3 What gets harder

- **Registration is a two-party operation.** Reuse needs proof of possession, so `register` gains a round trip. A caller that can no longer mint silently now has to prove who it is.
- **Mixed-state fleets are real during migration.** `pin@project` (§4.3) exists for exactly this window and must not be removed at the end of it — some agents will always be stragglers.
- **A lost key is still a lost key.** No escrow, no recovery. This ADR makes one key per pin load-bearing; it does not make it safe to lose.
- **Every per-project identity assumption in tooling must be re-read.** The guard's identity fold, the reservation board, and the contact list all key on holder name *and* project today. They keep working — they now see one key in several places, which is the point — but the tests that asserted distinct keys per project are asserting the old world and will need rewriting, not deleting.

### 6.4 Reversibility

Reverting is a matter of re-minting per-project keys from the retained key files (§5.2 step 3 never deletes them). The migration is therefore **reversible by construction**, which is the main reason to allow the §6.1 trade at all.

---

## 7. Implementation plan

### 7.1 Waves (cut as beads on acceptance)

**W1 — the same-project bug, independent of this ADR.** Enforce one `name` → one `pubkey` within a project. This is bd-0tbg's shape and needs nothing from W2. Highest value per line, ship it first.

**W2 — key reuse at registration.** `create_agent_identity` / `register_agent`: on a pin already known in any project, reuse the key behind a proof-of-possession check instead of minting. `agents` gains the partial unique index. New `// governed-by: ADR-0037` at the top of the touched files.

**W3 — send resolves by pin.** `agent_keys_for_resolved` and the send path resolve a name to a key independent of the sender's project. `ambiguous_name_warning` and `name_across_projects` are deleted **in the same change**, so there is no window where the code both splits and warns.

**W4 — `pin@project`.** The qualified form, for the migration window. Parser in the messaging entry, resolution in the existing per-project path.

**W5 — docs, guide, skill.** `AGENTS.md` identity section, `docs/guides/identity.mdx`, `docs/guides/mail.mdx`, `~/.agents/skills/toron/SKILL.md`, and the Skill Sync Contract surfaces, since `mail send`'s addressing rules change for every agent that reads the skill.

### 7.2 Files

| Path | Change |
|---|---|
| `crates/toron-core/src/mailbox/identity.rs` | uniqueness constraint; reuse-with-proof at registration; legacy kebab holder reporting |
| `crates/toron-core/src/mailbox/messaging.rs` | `agent_keys_for_resolved` resolves by pin; `pin@project` parse |
| `crates/toron-core/src/nostr/identity.rs` | delete `name_across_projects`, `ambiguous_name_warning` (W3) |
| `crates/toron-core/migrations/` | partial unique index on `agents`; new migration number, never edit `0001` |
| `crates/toron-cli/src/robot.rs` | `whois` reports the pin's key and its projects; `agent prune` surfaces held holders |
| `crates/toron-mcp/src/tools/mail.rs` | same addressing on the MCP surface — parity is frozen, so behaviour changes, names do not |
| `crates/toron-core/src/mailbox/guard.rs` | identity fold: one key in several projects is now the *normal* case, not a collision |

### 7.3 Tests

- **Same-project split (W1).** Registering one name twice in one project with two keys is refused. Today it succeeds — that is the bug.
- **Reuse (W2).** Registration in a second project yields the SAME pubkey and adds a row. Prove it by asserting the npub, not the row count.
- **Proof of possession (W2).** A registration claiming someone else's pin without a valid signature is refused, and the refusal names the pin. Both directions (missing signature, wrong signature).
- **Cross-project delivery (W3).** A send from project A to a pin registered in B is decryptable in B's inbox. This is the end-to-end proof bd-j5sk's live test failed to get.
- **Negative control (W3).** A send to a genuinely unregistered name still fails loud, and a send to a *different* pin's key is never delivered to ours.
- **`pin@project` (W4).** Resolves to the named project's key; an unknown project suffix falls through to the literal-name path and fails; the bare form is unchanged.
- **Migration (W5-adjacent).** A re-keyed pin's mail is readable under the survivor, and the released key's file still exists on disk.

Per the repo's effect law, each of these is a decision core with a fake, not a live-daemon test. The one end-to-end live check belongs in `toron-e2e`, and it must be proven in **both** directions — a send that arrives, and a send that is refused — so the happy path cannot pass for the wrong reason.

---

## 8. Verification

Acceptance is mechanical. Every box must be checkable by a command or a test name, not by reading.

- [ ] W1: one `name` → one `pubkey` per project is enforced by a DB constraint, not only by code (`agents_same_name_one_key`).
- [ ] W2: registering a known pin in a new project returns the SAME npub (`register_reuses_a_known_pins_key`).
- [ ] W2: registration without valid proof of possession is refused in both failure modes (`register_without_possession_is_refused`, `register_with_wrong_signature_is_refused`).
- [ ] W3: `mail send --project A --to <pin>` is readable in project B (`cross_project_send_is_readable_by_the_recipient`).
- [ ] W3: `grep -rn 'ambiguous_name_warning\|name_across_projects' crates/` returns nothing.
- [ ] W4: `pin@project` resolves to the named project's key and an unknown suffix fails loud (`qualified_address_resolves_and_unknown_project_fails`).
- [ ] W3/W4: a send to an unregistered name still fails loud (`unknown_recipient_still_fails_loud`).
- [ ] `mbx test --workspace` (per package) green; `mbx clippy --workspace --all-targets -- -D warnings` clean; `mbx fmt --check` clean.
- [ ] `mbx test catalog_freshness` green after any catalog regen.
- [ ] No new MCP tool and no new kind (`docs/catalog/toron-mcp.json` tool count still 40 / 38-parity + 2 additive; kind registry unchanged).
- [ ] Every touched ADR-constrained file carries `// governed-by: ADR-0037`.
- [ ] AGENTS.md, `docs/guides/identity.mdx`, `docs/guides/mail.mdx`, and the toron SKILL.md all state the one-key rule; `docs/guides/check.mjs` passes.

---

## 9. Revisit triggers

Reopen this ADR if any of these becomes true:

- A real incident from the §6.1 blast radius: a key compromised in one project exposing mail in another. That is the trade failing, and the answer would be a signer tier or rotation, not a retraction.
- More than ~10% of the fleet is still multi-key after the migration window closes — `pin@project` has stopped being a shim and become the interface, which means the ergonomic promise was not kept.
- A pin legitimately needs per-project keys for isolation (a tenant boundary). If that demand is real, Option A is wrong for that class and the ADR needs an explicit opt-out with a stated blast radius.
- ADR-0021 is ever reopened. Hierarchical keys would change the storage cost that this ADR's flat model assumes.

---

## 10. Status log

- **2026-09-29 — proposed.** Drafted from bd-j5sk's live measurement and bd-0tbg's board measurement. Not accepted: §6.1 is an operator decision. bd-j5sk stays open.
- **2026-10-11 — accepted.** Captain word, relayed (stack-comms build, Phase 0). Verified at acceptance on `main` (`dd643ff`): W1–W5 are **not implemented** — `ambiguous_name_warning` and `name_across_projects` are still in `nostr/identity.rs` and `mailbox/messaging.rs`; none of the §8 test names exist (`agents_same_name_one_key`, `register_reuses_a_known_pins_key`, `register_without_possession_is_refused`, `cross_project_send_is_readable_by_the_recipient`, `qualified_address_resolves_and_unknown_project_fails`, `unknown_recipient_still_fails_loud`). Sequencing in the comms build plan: W1 (same-project uniqueness) is Phase 0 tail; W2–W4 land in Phase 1 beside the envelope, and are a hard gate for Phase 2 (`send --via-inbox` addresses npubs only); the §5.2 per-identity re-key needs the operator per pin and is scheduled, not automated.
