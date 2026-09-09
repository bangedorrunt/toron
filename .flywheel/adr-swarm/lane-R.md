# Lane R — reliability / failure taxonomy (flywheel repo)

You are Lane R of the ADR-0024/0025/0026 implementation swarm. Captain: TealDune.
Your repo: `/Users/bangedorrunt/workspace/flywheel`. Baseline HEAD: 186a13e (checkpoint), `cargo check` green.

## Specs (read first)
- `/Users/bangedorrunt/workspace/toron/docs/decisions/0026-failure-taxonomy-funded-preflight-fallback-chains-mail-health-escalation.md` — Specs 0–3 (Wave 1), Spec 4 (Wave 2, later bead).
- The bead: `bd-adr26-w1-failure-taxonomy-afa7` ("cargo test failure_classify").

## Scope (ADR-0026 Wave 1)
1. `src/failure.rs` NEW: `FailureKind` with variants EXACTLY `Auth | RateLimit | Timeout | Blocked | Provider | Cancelled | Unknown | Billing` + a cross-ref test freezing that full spelling (ADR-0024 Spec 1 set + this record's two additions). `ClassifiedFailure { kind, retryable, reroute_eligible, evidence }`. Pure `classify(stderr_tail, exit_code, body)` per the ADR signature table — extendable by pattern only, no-match → `Unknown` with raw tail as evidence, never guesses.
2. Classifier call sites in `src/main.rs`: spawn paths (`start_agent_with_retry`, `start_agent_with_retry_dyn`, plain spawn), `cmd_send` first-dispatch, headless invoke. On reroute-eligible failure: mark attempt FAILED with evidence, then fallback chain (next profile); ONE reroute per class per task; second same-class failure terminal with evidence. rate_limit/timeout → `{ok:false, kind}` per ADR-0024's rule (subsumes the pane-path 429 copy). `billing`/`auth` never retried.
3. `src/profile.rs`: `fallback: Vec<String>` + catalog-load validation — unknown fallback name fails AT LOAD naming profile + name.
4. `src/preflight.rs`: provider-funded checks per distinct provider in the resolved crew manifest — funded endpoint returning zero/insufficient FAILS the mode naming provider+profile; no endpoint → `skip` with reason "no funded endpoint — classifier+fallback cover it". Loop-identity-key check as a `fail` (not skip) for loop modes.
5. `src/steer_brief.rs`: funded column from the same source. Non-TTY `--json` output shape unchanged.
6. Tests: per-row classifier table; chain-bound; unknown-fallback-name fails load; reroute audit written (spawn registry row `(attempt, profile, kind, evidence)`; bead-owned tasks also `br comments add`).
- Do NOT implement Spec 4 (mail-health) in this bead — it is `bd-adr26-w2-mail-health-zg5f`, your third bead.

## Hard constraints
- `// governed-by: ADR-0026` on new/changed enforcing files.
- No new CLI nouns, no new kinds in the Nostr registry, no new MCP tools, no second retry policy (0024's retry rule governs rate_limit/timeout; billing/auth reroute-or-terminal).
- Work-quality (test red) is NEVER a FailureKind.
- `harness_missing` keeps its existing recipe fallback first (ADR-0005 H1); classifier only sees it when recipes fail.
- `Blocked` is carried for set parity; spawn/send classifier emits it only where a reservation-conflict surface feeds it — otherwise never.
- TDD; `cargo clippy --all-targets -- -D warnings` + `cargo fmt --check` clean; bead VERIFY `cargo test failure_classify` must pass.
- **You own `flywheel/src/main.rs` in Wave 0.** Lane S is under orders not to touch it.
- Build slot: acquire `cargo` slot before test runs, release after (`flywheel mail slot acquire --slot cargo`).
- Reserve before edit: `flywheel mail locks reserve --project flywheel --reason bd-adr26-w1-failure-taxonomy-afa7 src/main.rs src/failure.rs src/profile.rs src/preflight.rs src/steer_brief.rs` (renew if you outlive the TTL).

## Process
- Claim: `br update bd-adr26-w1-failure-taxonomy-afa7 --status in_progress --assignee <your herdr name>` (in /Users/bangedorrunt/workspace/flywheel).
- Mail captain on start / done (`[<bead-id>] done` + sha + test tail) / blocked >15min (then STOP).
- Commits: conventional + bead id, explicit paths only, `.beads/` included after `br sync --flush-only`.
- Queue after this bead: `bd-d363` (ADR-0024 W1a: runner-site kind classification at pane/headless/cron, after 26-w1), then `bd-adr26-w2-mail-health-zg5f` (Spec 4; by then Lane S has released `src/durable.rs`). Await captain's go per bead.
