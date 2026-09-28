---
title: Can herdr classify a Freebuff pane?
date: 2026-09-29
status: measured, negative
---

# Can herdr classify a Freebuff pane?

**Answer: not without a change to herdr itself, and not yet with the fields herdr
needs.** herdr already sees a Freebuff pane but cannot name it, which is the one
thing `--wait` and `agent list` are built on. Freebuff 0.1.2 exposes nothing that
would let a detector classify it.

Everything below was measured from inside a Freebuff pane running in herdr
(`HERDR_ENV=1`, workspace `w1W`, tab `w1W:t4`, pane `w1W:p4`). Command outputs are
quoted verbatim.

## The pane is visible but unclassified

```console
$ herdr pane current --current
{"result":{"pane":{"agent_status":"unknown","display_agent":"BoardAgent",
 "terminal_title":"Freebuff: continue","pane_id":"w1W:p4", ...
```

`agent_status` is `unknown`. A working opencode pane in the same workspace carries
three fields this one does not:

| field | opencode pane | Freebuff pane |
|---|---|---|
| `agent` | set | absent |
| `agent_session` (`source: "herdr:opencode"`) | `ses_f1eb7dd…` | absent |
| `state_change_seq` | present | absent |

Those three are what `herdr agent list` and `--wait` read. Without them the pane
cannot be enumerated, waited on, or steered. The absence is the whole problem in
one line.

## Freebuff is not a herdr kind, and kinds are compiled in

`herdr integration install` enumerates 18 named kinds (`pi`, `omp`, `claude`,
`codex`, `copilot`, `devin`, `droid`, `kimi`, `opencode`, `kilo`, `hermes`,
`qodercli`, `qwen`, `letta`, `cursor`, `mastracode`, `antigravity-cli`, `grok`).
There is no `freebuff` and no generic custom-kind escape hatch.

`herdr` is a single compiled binary at `~/.local/bin/herdr`, so the kind registry
is compiled in. **Adding a kind is a herdr source change**, and no herdr source
tree exists on this machine (`~/workspace/*` has no herdr checkout). That is the
first hard blocker: the integration cannot be made from this repo.

## Freebuff exposes no machine-readable surface

- **It is a thin npm launcher.** Freebuff 0.1.2 downloads a checksummed
  platform binary. The shipped `http.js` exports exactly two things:
  `createReleaseHttpClient` and `evaluateRedirect` — a download helper with a
  redirect-host allowlist. That is not a control API.
- **No headless mode.** `freebuf --help` offers one subcommand, `login`, plus
  `--version`, `--continue [conversation-id]`, `--cwd`, `--trust-agents`,
  `--help`. There is no `--json`, no `exec`, no `print`.
- **No state, no config, no local API.** `~/.freebuff`, `~/.config/freebuff`,
  `~/.local/share/freebuff` and `~/.codebuff` are all absent. The terminal buffer
  is the only channel.

## The terminal buffer is the only viable channel, and it has three blockers

It is workable in principle. The pane's own buffer renders a usable status line:

```console
$ herdr pane read w1W:p4 --source detection --lines 30
 working...                      1m 13s  ■ Esc
Enter a coding task
Space Bunny Alpha • max · ~/workspace/toron
```

Three blockers stand between that and a working detector:

**1. Advertisements render inline in the same buffer.** Copy such as
`Get a .cv domain at 50% off…`, `Stand out with a .cv domain. GEO Opti…` and
`Learn more ola.cv` appears in the detection source. These are load-bearing per
the Freebuff README, not incidental noise, so a text-grepping detector will
false-positive on ad copy rather than on agent state. Any detector must
distinguish ad copy from status by position or shape, not by keyword.

**2. Session identity has no source.** `--continue [conversation-id]` proves a
conversation id exists, but the TUI never surfaces it. There is nothing to build
the `ses_…` value from, so an `agent_session` equivalent cannot be derived even
in principle.

**3. Only the working state was ever observed.** Every capture above was taken
while this agent was mid-turn. The idle rendering is a **hypothesis**, not a
measurement. Since the idle/working distinction is the entire `--wait` contract,
building on the unobserved half is how a detector silently becomes a
one-state detector.

## What already works

The toron half is done and live. `crates/toron-core/src/backend/herdr.rs`
(1522 lines, `HDR_PROTOCOL = 22`, `MAIL_IDENTITY_SOURCE = "toron:mail-identity"`,
`TOKEN_PIN` / `TOKEN_PROJECT` / `TOKEN_NPUB`, socket `HERDR_SOCKET_PATH` or
`~/.config/herdr/herdr.sock`) stamps mail identity onto a pane via
display-only `pane.report_metadata`. That stamp is on this very pane, in a
program herdr cannot classify:

```json
"tokens":{"toron_npub":"12bc7285…","toron_pin":"BoardAgent","toron_project":"demo"}
```

So toron does not need herdr's kind registry to work. It needs `pane.list`,
`pane.read` and `pane.report_metadata`, all of which work regardless of
classification. **Nothing in toron needs to change for this.** A
`freebuff` kind is worth having for `agent list` and `--wait`, which is a
convenience, not a correctness dependency.

## What it would take

1. A herdr source tree, and a `freebuff` kind registered in the compiled registry.
2. A session-id source in Freebuff. Without one, `agent_session` stays unset and
   `--wait` falls back to classification alone.
3. An idle-state capture, recorded rather than assumed, before any state machine
   is written against it.

Steps 1 and 3 are upstream work. Step 2 is blocked on Freebuff shipping
something. None of the three is decidable from this repository, and none of them
is on the critical path for toron.

## Re-checking this

The conclusions above are falsifiable, so the check is cheap:

```console
$ herdr pane current --current   # expect agent_status: "unknown"
$ herdr integration list          # expect no freebuff entry
$ freebuff --help                 # expect no --json / exec / print
```

If any of those change, the answer above is stale and this note should be
rewritten rather than amended.
