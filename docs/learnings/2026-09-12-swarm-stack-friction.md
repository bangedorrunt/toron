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

## Clean (worth keeping)

- Strict guard's blocked[] JSON is precise (agent, path, reason) — fast to act on once identity resolved.
- `toron reserve acquire --dry-run`, `br dep cycles`, `br ready` dispatchability gates (VERIFY fence + principles) all worked first try.
- Channel join-before-post fails loud with the remedy named; kind-9 room + `--file` chunking worked.
- `toron agent bootstrap` upsert preserving the existing npub is exactly the right semantics (one pin, one key held).
