# ADR-0030: Presence-Gated Chokepoints — Session Entry Enforced at Tool Boundaries

**Status:** Proposed (awaiting captain word)
**Date:** 2026-09-09
**Decision-makers:** Captain (bangedorrunt)
**Extends:** [ADR-0011](0011-coalition-orchestration.md) (session door: arrive → briefing → argv), [ADR-0017](0017-project-root-canonicalization.md) (pins as acting identity), [ADR-0019](0019-bead-verified-execution-and-principle-playbooks.md) (ledger-gated close), [ADR-0028](0028-jido-behavior-first-on-wheelhouse-nouns.md) (presence trust)
**Does not amend:** frozen MCP tool surface (40), kind registry (ADR-0014), one-way deps, the legal-close table (it adds a precondition to entry, not to close validity), herdr protocol.
**Tracker:** beads. Never pull requests.
**Incident of record:** 2026-09-09 — an agent worked the herdr-0.9 fix (bd-myab/bd-jqgh) entirely outside the session: never `arrive`d, never briefed, while a **live fleet was running on toron** (dispatcher up, pins=1, 31 held reservations). The exit ceremony (gate rows, legal close, captain mail) ran normally and made the lane *look* compliant. Post-incident `flywheel briefing` confirmed the classifier would have ruled **join** (ADR-0011 D5). The agent's own post-mortem: every rule that failed was prose; every rule that held was a tool-boundary check.

---

## Context

The stack has two classes of rules and only one of them works.

| Rule | Enforcement | Held under live test? |
| :--- | :--- | :--- |
| Commit only inside your reservation | `toron` pre-commit guard (hard fail) | **Yes** — blocked a commit until reserved |
| Bead tokens in staged diff must be in the message | strict guard (hard fail) | **Yes** — forced an amend |
| Close requires gate row + SHA citing the bead | `br close` fail-closed | **Yes** — ceremony followed it |
| Catalog freshness | `mbx test catalog_freshness` | **Yes** |
| **Arrive before work** | prose ("skip and you are not in the session") | **No** — skipped, nothing noticed |
| **Briefing decides topology** | prose ("do not self-route") | **No** — agent self-routed solo |
| **Exit ceremony valid only if entry happened** | nothing | **No** — mail made it look legitimate |

The failure is not discipline; it is architecture. Ceremonies attached to an unavoidable action (committing → guard; closing → gate) are followed by every agent, because the tool refuses otherwise. Ceremonies the agent must initiate voluntarily (arrive, brief) are dropped whenever the agent rationalizes the task as "too small to dispatch" — and the rationalization is always available, because nothing downstream consults the session state. Three aggravators:

1. **Zero detection.** No dispatcher, doorbell, captain, or doctor check reconciles a closed bead against "did the owning pin have a live arrival during the bead's open window?" The only audit artifact (gates.jsonl) records gates, not session legitimacy.
2. **Ceremony asymmetry.** Exit is loud (mail, gate rows); entry is silent. Compliance theater on the exit path masks absence on the entry path.
3. **Prologue scatter.** Session start demands ~six mandatory regimes at once (skills, routing, tools, arrive, brief, workflow discipline). The voluntary two-step (arrive, brief) is the most droppable.

> **Out of scope:** hard-blocking every interactive one-liner behind a dispatcher join (breaks "fix this typo" workflows); new Nostr kinds or MCP tools (presence + beads + guard already exist); punishing already-closed historical beads (audit flags forward only); changing `WorkspaceModel`/`tick` (ADR-0027) beyond one loud suspect-banner surface.

---

## Decision

Gate the chokepoints that are already audit moments on **presence freshness** — the pin has a live `arrive` for this project within the freshness window. Roll out warn-first, ratchet to strict behind an operator flag. Five waves, each independently shippable.

```
Wave 1  presence freshness primitive (query + expose)
Wave 2  chokepoint gating: warn → strict (br claim/gate/close, toron reserve)
Wave 3  audit trail: gates.jsonl session metadata + br doctor flags
Wave 4  captain-side reconciliation of [bd-*] done reports
Wave 5  one-command session door: flywheel session-start
```

### Option B — Prose only, tighter wording (REJECTED)

That is the status quo that failed. ADR-0022's lesson, quoted by 0029 and re-proven here: "anything that exists only as convention silently breaks. Mechanics … fail loudly." No amount of capital letters in AGENTS.md would have stopped the 2026-09-09 lane; the pre-commit guard would have.

### Option C — Captain-side detection only (REJECTED as primary)

Audit-not-prevention. The captain learns about solo lanes after the work is committed, when reverting costs the most. Detection ships as Wave 3/4 anyway — as visibility, not as the enforcement layer.

### Option D — Hard-fail everything on day one (REJECTED)

Interactive one-liners ("fix this typo in the current pane") would demand a dispatcher join or force operators to run with gates disabled — the boy-who-cried-bypass. Hence warn-first with a ratchet (Wave 2), and the strictness flag is **operator-owned** (`TORON_PROTOCOL_STRICT=1`), never agent-owned.

---

## Spec 1 — Presence freshness primitive (Wave 1)

`toron presence` already records state (ADR-0020/0028). Add a pure, testable predicate and expose it:

- `presence_fresh(project, pin, max_age_secs) -> Result<Freshness>` — fresh / stale(age) / absent, backed by the existing presence store (30013 / registry; no new kind).
- Freshness window default **900s** (presence re-assert cadence is well under a minute for live panes; 15 min tolerates a long headless turn). Operator-overridable per project; config may only narrow.
- Surfaced via `toron whois --fresh` and consumed by every later wave. No new MCP tool (CLI noun per the ADR-0027 skill-sync rule).

**Avoid:** a second presence store; requiring heartbeat writes from agents (reads stay cheap; arrival + normal activity already touch it).

## Spec 2 — Chokepoint gating (Wave 2)

Four chokepoints, all already audit moments, check `presence_fresh` for the acting pin + project and take the configured stance:

| Chokepoint | Warn mode (default) | Strict mode (`TORON_PROTOCOL_STRICT=1`, operator-set) |
| :--- | :--- | :--- |
| `br update <id> --status in_progress` (claim) | print `PROTOCOL: unarrived lane claiming <id>` | refuse |
| `br gate report` | stamp row `unarrived` | refuse |
| `br close` | stamp close reason prefix `[UNARRIVED-LANE]` | refuse |
| `toron reserve acquire` | proceed + warn | refuse |

- The flag is read from **operator env/config only**; repo config may narrow nothing here (ADR-0029 Spec 4 trust boundary applies: agents can never loosen).
- Escape hatches (`TORON_GUARD_STRICT=0`, `--no-verify`) are operator-only in the strict sense already documented; an agent using one to evade these gates is a protocol violation, logged by the existing guard audit trail.

**Avoid:** gating read-only verbs (whois, list, read) — the stack must stay inspectable from anywhere; gating `arrive` itself.

## Spec 3 — Audit trail + doctor (Wave 3)

- `br gate report` and `br close` rows in gates.jsonl gain additive fields: `presence: fresh|stale|absent`, `briefed: true|false` (briefing = a briefing event for this project by this pin within the bead's open window — flywheel already emits briefing state; absence is `false`, never fabricated).
- `br doctor` new check `protocol.unarrived_closes`: closed beads whose provider pin had no fresh arrival during the bead's open window → listed, loud, forward-looking only.
- Nothing historical is re-opened; the check exists so the pattern is visible the first time it recurs.

## Spec 4 — Captain-side reconciliation (Wave 4)

The doorbell tick (ADR-0027) already consumes `[{id}] done` mail. On a done-report whose sender pin has `presence_fresh == absent` for the project:

- banner the wake line: `SUSPECT: [bd-x] done from unarrived pin <pin> (no session entry)`;
- include the bead id in the captain digest's suspect section.

No message is dropped, no close is invalidated — the captain decides. This kills the "mail made it look compliant" asymmetry.

## Spec 5 — One-command session door (Wave 5)

`flywheel session-start <project> --goal "<goal>" [--as <pin>]` = resolve pin (bootstrap if needed) + `arrive` + `briefing --json` in **one** call, printing the classifier verdict and argv. One command cannot be half-skipped; six can. AGENTS.md's session-start section collapses to: run `session-start`, then follow argv. The two-command path (`arrive` + `briefing`) remains valid for compatibility.

---

## Implementation blueprint

**Wave 1** — **Target:** toron `crates/toron-core/src/nostr/presence.rs` + `crates/toron-cli/src/cli.rs` (`whois --fresh`), `toron guard` surface. **Patterns:** ADR-0028 presence trust; pure predicate + integration test against the registry store. **Avoid:** new kinds; MCP growth. **Tests:** fresh/stale/absent boundaries at exactly 900s; per-project isolation.

**Wave 2** — **Target:** beads repo `br` (claim/gate/close verbs), toron `reserve acquire`. **Patterns:** the pre-commit guard's strict-flag shape; fail-loud error naming the resolved project (bd-flag-vocab precedent). **Avoid:** gating read verbs; repo-config loosening. **Tests:** warn mode stamps + proceeds; strict mode refuses with exit 2 + typed error; operator flag wins over repo config.

**Wave 3** — **Target:** beads gates.jsonl writer (additive fields, schema v-next minor), `br doctor`. **Patterns:** additive-field schema discipline (v19 precedent); doctor check registry. **Avoid:** rewriting historical rows; fabricating `briefed` (absent ≠ false-positive). **Tests:** new fields present on fresh rows, old rows parse unchanged; doctor lists unarrived closes.

**Wave 4** — **Target:** flywheel `doorbell_tick.rs` (suspect banner on done-from-absent-pin), awareness-digest suspect section. **Patterns:** ADR-0027 tick (banner is a rendered view, not a new Msg). **Avoid:** suppressing or auto-invalidating closes. **Tests:** absent-pin done → banner present; fresh-pin done → unchanged bytes.

**Wave 5** — **Target:** flywheel CLI `session-start` verb composing the existing three calls. **Patterns:** `toron agent bootstrap` (one-call precedent). **Avoid:** new state; skipping the classifier (session-start MUST call briefing, never shortcut it). **Tests:** end-to-end on a scratch project: cold pin → registered + arrived + briefed + argv printed in one invocation.

**Skill Sync:** Waves 2/5 add CLI nouns/flags (`whois --fresh`, `session-start`) → run the Skill Sync Contract in the same cut (toron + br skill files, tool-count constant unchanged — no new MCP tools).

---

## Verification (acceptance for this ADR)

Harness: `docs/decisions/_scripts/verify-presence-gates.sh` — **RED today by design**; each gate flips a section to PASS as its wave ships. Green requires:

- [ ] `toron whois --fresh` reports fresh/stale/absent; 900s boundary exact.
- [ ] Strict mode: claim, gate, close, and reserve each refuse an unarrived pin with exit 2 + typed error naming project + pin.
- [ ] Warn mode: all four proceed but stamp `unarrived` on the row/reason/output.
- [ ] `TORON_PROTOCOL_STRICT` is honored from operator env; a repo-level attempt to unset it fails at load.
- [ ] gates.jsonl fresh rows carry `presence:` and `briefed:`; pre-existing rows still parse.
- [ ] `br doctor` lists unarrived closes; historical beads are flagged forward-only, never re-opened.
- [ ] Doorbell banners a done-report from an absent pin; fresh-pin output byte-identical to today.
- [ ] `flywheel session-start` cold-starts a scratch project in one invocation: pin + arrival + briefing verdict + argv.

---

## Consequences

**Positive**
- The 2026-09-09 class of bypass — solo lane with a legitimate-looking exit ceremony — becomes impossible to commit silently in strict mode and loud everywhere in warn mode.
- Enforcement posture unifies: every rule that matters is a tool-boundary check; prose rules shrink to definitions and operator policy.
- The classifier regains its purpose: topology is decided by state (live fleet, ready beads), not by the agent's confidence about task size.

**Negative / costs**
- A dead presence store (daemon down) would warn-stamp every lane in warn mode and block claims in strict mode — correct (you cannot be in a session the daemon cannot see), but the operator owns that failure mode; warn is the default ratchet stage for exactly this reason.
- gates.jsonl grows two fields (schema minor bump); old rows parse unchanged.
- `session-start` is a fourth spelling of "get into a session" — mitigated by AGENTS.md collapsing to exactly this verb.

**Revisit if:** presence proves too coarse (a pin arriving once then vanishing for hours passes 900s checks only at claim time — consider activity-coupled freshness if incidents show that gap); strict mode false-positives on long headless turns (raise the operator window rather than special-casing verbs).

## Principles cited (ADR-0019 registry)

| Name | Decision it changed |
| :--- | :--- |
| `fix-root-causes` | The gap is entry enforcement, not exit ceremony; gates go on the chokepoints where all agents already pass. |
| `prove-it-works` | Acceptance is a runnable RED harness (`verify-presence-gates.sh`), not prose intent. |
| `encode-lessons-in-structure` | The 2026-09-09 incident becomes presence gates + doctor checks, not a sharperly-worded AGENTS.md paragraph. |
| `boundary-discipline` | Strictness is operator-owned (`TORON_PROTOCOL_STRICT`); repo config may only narrow, agents may never loosen. |
| `make-operations-idempotent` | `session-start` composes existing verbs; no new session state, no second presence store. |

## Related

- Incident post-mortem + memory record, 2026-09-09 (agent SlateOtter; correction event in the chiebukuro memory plane).
- ADR-0011 §"join by default" (D5) — the decision the bypassed lane should have received.
- ADR-0029 Spec 4 — the trust-boundary pattern this ADR reuses for `TORON_PROTOCOL_STRICT`.
