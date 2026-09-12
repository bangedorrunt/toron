# Stack friction log — ADR-0032 swarm dispatch (2026-09-12)

Captain: AmberVellum. Context: /swarm-mode, 4 opencode crews, compose token wave-swarm.
Engine dispatch failed end-to-end; manual steer used. Every item below was hit, not hypothesized.
Ordered by severity. "Fix" = suggested owner.

## Sev1 — engine compose never dispatched

**F7/F8. `compose --run` hangs in foreground, dies half-state, no resume.**
- Stdout stops after the crew-packing lines; >15 min silent (spawn settle 30s × 4 ≈ 2 min, so ~13 min unaccounted — likely dispatch-stage send loop with no timeout).
- Kill the CLI → panes stay spawned + registered, but ZERO run rows (`workflow history <id>` empty), no dispatch, no artifact. `flywheel runs pending` empty too. Half-state has no resume verb.
- Second attempt with same `--idempotency-key` + `--reuse w13` created a NEW run id and spawned a SECOND crew into the same workspace (`--reuse` binds the workspace id, does not adopt existing panes). 4 orphan panes had to be hand-closed (`herdr pane close`).
- No `--timeout` flag on compose. Fix: compose should (a) persist the run row BEFORE spawning, (b) adopt-or-refuse on idempotency-key hit when panes are alive, (c) take a timeout that fails loud instead of hanging.

**F9. `send --wait` gives false-ambiguous receipts for opencode panes.**
- Two of three lane dispatches returned `backend error timeout` / `agent_prompt_stalled ... current status is idle` while the pane was visibly working seconds later (spinner + token counter advancing; bead claimed).
- The ambiguous-exit design is honest (never invents green) but the oc-kind observation channel lags the 5s/45s windows, so every oc send reads as ambiguous. Captains learn to ignore the signal — that trains exactly the reflex GS-037 exists to prevent.
- Fix: oc status observation needs a longer/adaptive settle, or `send` should treat herdr `agent_status=working` transitions within N seconds as landed.

## Sev2 — identity plane split

**F1/F4. `flywheel mail register` does not register the pin in toron's local identity store.**
- `flywheel mail register toron --name P` succeeds, `flywheel mail whois` resolves, but `flywheel arrive --as P` fails presence publish ("unknown agent name") and `toron mail whois toron P` says "unknown agent name (register first)".
- Meanwhile `toron reserve acquire --as P` WORKS (resolves via the key file) — so reserve and whois use different resolution paths.
- Downstream: the strict guard blocked my own lease ("held by 989d0ee9") because whois couldn't map AmberVellum → that npub. Three debug rounds to find.
- Fix: `flywheel mail register` should call toron's register/upsert (or arrive should fall back to key-file identity). Docs should say `toron agent bootstrap` is the canonical register door (it upserts on the same key — verified `created: false`, npub preserved).

**F10. `arrive` does not rebind a dead captain alias.**
- `captain` folded onto QuietAnvil, a prior session's pin (stale=false only because last-active threshold is generous; its pane is gone). Crew `mail --to captain` would deliver to nobody live.
- Manual fix: `toron contact captain --project <p> --pin <new pin>`. Fix: arrive should offer/perform FCFS rebinding when the fold holder has no live pane/dispatcher.

## Sev3 — guard friction (working as designed, but sharp edges)

**F2. Strict guard inspects ALL staged paths, ignores `git commit -- <pathspec>`.**
- `git commit -m ... -- .beads` was blocked by a staged, unrelated `docs/decisions/**` file. Semantics defensible (guard the index, not the pathspec) but undocumented; cost one failed commit.

**F5. Guard vs br skill ceremony disagree on tracker commits.**
- Guard demands every bead id in the staged `.beads/issues.jsonl` diff appear in the commit message. A sync commit's diff contains every touched issue → message must enumerate unrelated bead ids (5 in my case; grows with board size).
- br skill still documents `git commit -m "Update issues"` — that command now fails the gate. Fix: guard should exempt `.beads/**`-only diffs from per-token citation (the sync commit IS the attribution), or br skill must be updated and the message format standardized.

**F6. No per-run harness override on compose; profile help advertises a kind that doesn't exist.**
- User-level profiles pinned `harness=pi`; the run needed opencode. compose exposes `--role-model` only — no harness flag — so honoring "4 opencode crews" required mutating `~/.config/flywheel/profiles.toml` via `profile create`.
- `profile create --harness oc` accepted the value; spawn then failed `unsupported interactive agent kind oc` (herdr kind is `opencode`). Validate harness values against the herdr kind registry at profile-create time.

## Process gap (not a tool bug)

**F3. Canonical ADR left uncommitted by the prior session.**
- Working tree held ADR-0032 rev 3 (the version the beads were re-cut against); HEAD held the superseded rev-2 text that rev 3 explicitly corrects. Any crew reading git history implements the wrong contract. No end-of-session gate covers doc-only changes (bead gates only witness code commits). Fix: captain close-out checklist includes "commit docs working tree" or a dirt-preflight on briefing that names uncommitted decision files.

## Wave-1 execution addenda (same session, ~05:30)

**F11. Bead close does not release reservations; the release-on-bead-close primitive is MCP-only.**
- SwiftBadger closed bd-i6kc green but its broad `crates/toron-workflow/src/engine/**` lease stayed held; BrightCoast (bd-o1hl scope: control_flow.rs + template.rs) was blocked, sent request-release into the void (holder pane already idle — finished agents don't answer), and mailed the captain.
- `release_file_reservations_by_reason` exists as an MCP tool but has NO CLI noun; the crews' close ceremony (br skill + ce-execute stage) never mentions releasing reservations. TTL expiry (2h) is the only automatic backstop.
- Fix: add `toron reserve release-by-reason <bead>` (or auto-release offer on `br close` detection), and add one RELEASE line to the close ceremony in br skill + ce-execute standing orders.

**F12. `flywheel send --to <pin>` does not resolve for spawned crew agents — only registry slugs.**
- Crew panes post mail FROM their pins (SwiftBadger, BrightCoast, WindyGlen on the steer channel), but `send --to SwiftBadger` fails "no agents match ... available: crew-toro091203590020-coder,...". The skill says `--to <pin>` resolves via spawn-registry mail_identity; in practice only the `crew-<run>-<role>` slugs address. First two operator replies (to pins) silently missed — caught only because the asx3 dispatch failed loud with the available-names list.
- Fix: make --to resolve pins through the same mail_identity the panes post with, or make the failure generic (it was loud here — good — but the two earlier sends exited clean-looking).

**F13. STEP-0 cross-project register mints a nameless key that can hold leases but never commit.**
- WindyGlen ran `flywheel mail register flywheel --name crew-toro...-coder3 --from-project toron` as briefed; the flywheel-side lease then landed on a fresh nameless npub (17cb8b3f). Transfer rejected, expiry 2h, only operator force-release cleared (×3 paths). The commit guard correctly refused the mismatch — but the create-a-ghost-holder path shouldn't exist.
- Fix: --from-project register should reuse the AGENT's existing key (env/injected ncryptsec), and lease acquisition under a key with no registered name should warn loud at acquire time.

**F14. Lease board is invisible: no CLI noun, MCP tool returns no structured content.**
- `toron reserve` has acquire/conflicts/renew/release/force-release/request-release/respond — no list/show. `toron_list_file_reservations` MCP errors "has an output schema but did not return structured content". I reconstructed holder state with `reserve conflicts` against guessed paths; WindyGlen's and BrightCoast's mails disagreed with the actual board.
- Fix: `toron reserve list --project <p>` (CLI) and fix the MCP tool's structured output.

**F15. `send --wait` false-ambiguity confirmed systemic (F9 generalizes).**
- All four --stage ce-execute dispatches: dispatched=true, then "no settled observation ... treat as ambiguous". Healthy panes were working within seconds. De-facto protocol is now --fire-and-forget + eyeball the pane, which is exactly the receipt-skepticism the receipt system was built to prevent.

**F16. Epic/parent-child structure deadlocks the ready set — and inverts cleanly-wrong.**
- `br ready`/`br next` count `parent-child` deps as blocking: bd-v139.1 had its only real blocker (htoo) closed, verify+principles legal, yet not dispatchable because the epic bd-v139 was open — and the epic only closes when its children close. Second structural deadlock this swarm (after the wave-label one, review fix).
- The epic's own deps were INVERTED at cut time (bd-v139 depends_on bd-htoo/bd-i6kc, i.e. epic blocked by children), and the open epic itself appeared in `br ready` output as dispatchable work.
- Fix options: ready() should ignore parent-child type (epics are grouping, not ordering), epics should never surface in ready, and `br create --parent` should validate direction. Worked around by removing the three parent-child links (grouping lives in the effect-journal label) — epic closes at drain citing children.

## Clean (worth keeping)

- Strict guard's blocked[] JSON is precise (agent, path, reason) — fast to act on once identity resolved.
- `toron reserve acquire --dry-run`, `br dep cycles`, `br ready` dispatchability gates (VERIFY fence + principles) all worked first try.
- Channel join-before-post fails loud with the remedy named; kind-9 room + `--file` chunking worked.
- `toron agent bootstrap` upsert preserving the existing npub is exactly the right semantics (one pin, one key held).
