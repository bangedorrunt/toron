# Adversarial review — in-flight toron work

Reviewer: OliveDove (arrived, briefing classifier proposed `interrogate` crew of 3; I am the
reviewer that classifier asked for, so vehicle = this crew pane, no second swarm composed).
Date: 2026-09-27. Branch `main` @ `53d7cb0` + dirty tree.

Scope cast: the uncommitted working tree, the three most recent peer commits
(`53d7cb0` bd-5z0l, `cbc0afc` bd-qpm0, `dc1d084` bd-cqo2), the two landed-but-unclosed
bd-4dpq commits (`f41953d`, `75ac551`), and the ledger state of the open P0-P2 board.

**Verdict: DISAGREE.** Two P0 defects. One is proven by execution on this machine; the other is
a gate that provably cannot fail. bd-cqo2 and bd-qpm0 are individually sound.

## Findings (JSON)

```json
{"findings": [
  {
    "id": "F1",
    "severity": "P0",
    "condition": "A `toron` command runs with BOTH TORON_MCP_DATA_DIR (an explicit durable-root pin, the variable the CLI's own error text tells operators to set) and TORON_MCP_URL present in the environment. TORON_MCP_URL is machine-wide ambient state on any crew pane: it is set in flywheel/crates/doorbell-cli/units/com.doorbell.serve.plist, and it is set in the reviewer's own live env (TORON_MCP_URL=http://127.0.0.1:18080/mcp).",
    "evidence": "crates/toron-cli/src/mailbox_source.rs:207-226 — `decide_source` returns `SourceDecision::DaemonUrl(url)` on the `env_url` arm at :213-215, BEFORE the `data_dir_pinned` arm at :216-218 is ever read. Its own doc at :236-239 ranks 'explicit wins over implicit' with 1. TORON_MCP_URL and 2. TORON_MCP_DATA_DIR, i.e. it deliberately ranks two explicit signals and lets the loser die silently. PROVEN BY EXECUTION on this machine, same command, one fresh temp pin: (A) with both set the error came back as an MCP wire envelope — `tool list_contacts returned an error result: [Text(TextContent { text: \"agent OliveDove has no local key in project demo...\"` — and the pinned dir was left EMPTY, no sqlite created; (B) with TORON_MCP_URL unset the same command created index.sqlite + -wal + -shm inside the pin and returned the local embedded error with no envelope. bd-5z0l's commit message names this exact root cause ('mailbox_source.rs:207-226 checks the daemon URL first and returns before the data-dir pin is ever read') and then fixed it in TEN TEST FILES, leaving the selector untouched and the defect unfiled in any bead (`br ready` shows no such bead). crates/toron-cli/src/robot.rs:548 instructs the operator to 'set TORON_MCP_DATA_DIR to run against an embedded mailbox' — advice that silently does nothing when the URL is ambient. bd-5z0l's own new comment in crates/toron-cli/tests/cli_robot.rs:2926-2931 states the consequence: 'the guard would judge the wrong lease table'.",
    "autofix": "gated_auto",
    "confidence": "high — quoted code at the named lines plus a two-case execution diff on this machine"
  },
  {
    "id": "F2",
    "severity": "P0",
    "condition": "Anyone tries to close bd-4dpq (P2, 'Run-state checkpoint store + time-travel replay/fork in toron-core') by running its declared VERIFY.",
    "evidence": "bd-4dpq.verify = `mbx +nightly-aarch64-apple-darwin test --lib toron-core`. `--lib` selects the lib target and the trailing bare `toron-core` is a NAME FILTER, not a package selector — the `-p` is missing, so the command runs the lib target of every workspace member filtering on the substring 'toron-core'. Ran it verbatim: toron-mcp `0 passed; 0 failed; 113 filtered out`, toron-workflow `0 passed; 0 failed; 305 filtered out`, toron-core `0 tests`, EXIT=0. The bead's own 12 tests live in the INTEGRATION target crates/toron-core/tests/run_state_conformance.rs, which `--lib` structurally cannot reach; the correct target runs them: `mbx test -p toron-core --test run_state_conformance` => 12 passed / 0 failed. A cargo name filter that matches nothing exits 0, so the gate CANNOT fail. Meanwhile bd-4dpq is still status=open with assignee=None, commit_sha=None, close_verdict=None, close_reason=None and no PRINCIPLES field, while TWO commits cite it (f41953d feat, 75ac551 fix) and a live durable migration is on disk (crates/toron-core/migrations/0013_run_state_checkpoints.sql, CREATE TABLE run_state_checkpoints). Sibling beads get the form right — bd-qpm0 `mbx test -p toron-core --lib bus::wake`, bd-kkid `mbx test -p toron-core --lib mailbox` — which is why this one slipped.",
    "autofix": "gated_auto",
    "confidence": "high — the VERIFY was executed verbatim and its 0-test exit-0 recorded; the correct target was executed and its 12/0 recorded"
  },
  {
    "id": "F3",
    "severity": "P1",
    "condition": "The dirty tree's deletion of live_watch_wait_wake_kinds_cover_doorbell (crates/toron-cli/src/plane.rs:2127-2138) is committed. A wake kind is then added to flywheel's doorbell list, or the two toron copies drift apart.",
    "evidence": "The wake-kind vocabulary is hand-copied THREE times with no witness. Canonical: flywheel/src/main.rs:9728 `const DOORBELL_WATCH_KINDS: &str = \"9,14,20001,30013,43001,30031,39002\";`. Server emit list: crates/toron-core/src/bus/wake.rs:26 `pub const WAKE_KINDS: &[u16] = &[9, 14, 1059, 20001, 30013, 43001, 43004, 30031, 39002];` used at wake.rs:232, :350, :439. Client ask list: crates/toron-cli/src/plane.rs:1624 `pub const WAIT_WAKE_KINDS: &[u16] = &[9, 14, 1059, 20001, 30013, 43001, 43004, 30031, 39002];` used at plane.rs:1663 and :1725. Diffed the two toron lists: IDENTICAL, and both are supersets of the doorbell mirror — so the invariant holds today and restoring the test is free. The coupling is load-bearing and silent: WakeConfig (wake.rs:210-224) has NO kinds field, so the sidecar always emits WAKE_KINDS while `wait --corr` filters client-side on WAIT_WAKE_KINDS; if the client asks for a kind the server never emits, the request is accepted and `wait --corr` sits until its 30s default timeout with no diagnostic. `grep -rn DOORBELL_WATCH_KINDS` over the toron tree returns exactly one hit, the doc comment at plane.rs:1619 — so the deleted test was the ONLY cross-repo witness. The dirty diff removes it. This is the same defect class bd-5uwt (P1, closed at e0ee9bc) was opened to eliminate; its close_reason reads 'restored the catalog tamper negative control and the default key-path contract test'.",
    "autofix": "gated_auto",
    "confidence": "high — both constants quoted, list diff computed, grep for the canonical name across the repo reported"
  },
  {
    "id": "F4",
    "severity": "P1",
    "condition": "Reviewers read `git diff` on the current tree to decide what changed.",
    "evidence": "`git diff --stat` on the dirty tree: 7 files, 56 insertions(+), 50 deletions(-). Of that, crates/toron-workflow/src/engine/control_flow.rs, crates/toron-workflow/src/engine/tests.rs and crates/toron-core/src/index/schema.rs are PURE rustfmt reflow (verified by reading every hunk — no token changed, only line wrapping). `mbx fmt --check` exits 0, so the fmt part is already correct. The only semantic content in the whole diff is two test deletions. The other deletion, conformance_scripts_are_deterministic in crates/toron-core/tests/wake_conformance.rs, is a CORRECT prune: it builds `(0..20).map(|i| format!(\"flood-{i}\"))` twice, asserts the two vectors are equal, and checks `HashMap::insert` returns None — it exercises `format!` and std HashMap, not toron. So one good prune and one bad one are shipped in the same noise diff, and the fmt reflow is what makes the bad one hard to see.",
    "autofix": "manual",
    "confidence": "high — every hunk read; fmt-cleanliness measured"
  },
  {
    "id": "F5",
    "severity": "P1",
    "condition": "bd-qpm0 is treated as closed with its bead's analysis fully dispositioned.",
    "evidence": "bd-qpm0's own description names a SECOND defect and says 'Related, and worth fixing in the same pass because both are per-connection costs that accumulate: :461 creates a brand new subscription on EVERY poll tick (WAKE_POLL_MS = 1000, wake.rs:42) whenever a wake.sock consumer is present. Each tick costs two full store scans (the subscribe-time snapshot at in_process.rs:367 plus the delta re-read) and spawns a detached replay task... it is 2 store scans per second per consumer forever.' Still present and untouched: crates/toron-core/src/bus/wake.rs:493 `let polled = match bus.subscribe_trusted(poll_filter.clone()).await {` inside the `tokio::select!` poll arm at :489, gated only by `if bus_tx.receiver_count() == 0 { continue; }` at :490. The commit touches wake.rs stats only (258 insertions, of which the whole wake.rs hunk is the stats split); the close_reason mentions only the Vec retention and the aggregate. No bead carries the churn. Note the two defects interact: bd-cqo2's Drop hook now removes each of those per-tick registrations instead of letting them pin, so bd-cqo2 quietly mitigates bd-qpm0's churn — which is exactly why the omission reads as deliberate and needs to be recorded rather than left implicit.",
    "autofix": "manual",
    "confidence": "high — the bead's own words quoted, the surviving line read at its current number"
  },
  {
    "id": "F6",
    "severity": "P2",
    "condition": "A reader trusts crates/toron-core/tests/bus_subscriber_leak.rs, or looks for coverage of the fanout_to TrySendError::Closed arm after dc1d084.",
    "evidence": "Line 17-18 of the module doc still reads 'This test asserts the correct behavior (count returns to zero). It is expected to FAIL against the current code; that failure is the evidence.' The file passes: `mbx test -p toron-core --test bus_subscriber_leak` => 2 passed / 0 failed. Line 5-6 also still claims the Closed arm in fanout_to is 'the ONLY removal path', which dc1d084 made false. Second, dropped_scoped_subscription_is_released (line 84-107) carries the comment at :88-91 'This is the control case that shows the removal path works when access_gate lets a publish through', but line 96 `drop(sub)` runs before line 97's publish on the same thread, so the Drop hook has already removed the entry and the publish is a no-op. The test can no longer exercise the Closed arm it names. Both tests now assert the same thing, and in_process.rs:259-262 / :272-275 have lost their witness.",
    "autofix": "gated_auto",
    "confidence": "high — text quoted from the file; suite executed green"
  },
  {
    "id": "F7",
    "severity": "P2",
    "condition": "The RSS assertion in crates/toron-core/tests/bus_wake_stats_bound.rs is read as the memory fence for bd-qpm0.",
    "evidence": "rss_kb() at :29-38 shells out to `ps -o rss= -p <pid>` and ends in `.parse().unwrap_or(0)`, so if `ps` is absent or the output is unparseable the sample is 0 and `grew_kb` at :184 computes `after.saturating_sub(before)` as 0 — the assertion at :185-188 (`grew_kb < 32_768`) then passes unconditionally. Measured: the whole 1500-subscriber sequence in serving_many_subscribers_in_sequence_does_not_retain_them_all completes in 0.17s, so the before/after window is ~170ms. SubscriberStats is 4x u64 + Option<String> = 56-64 B (:372-386 of wake.rs), so 1500 retained records is ~96 KB — about 340x under the 32 MB threshold. The threshold is two orders of magnitude above the leak it is named for, so it cannot fail for that reason. A silent-success stub is an explicit anti-pattern in crates/toron-cli AGENTS.md. The real fence in that test is the retained-count pair at :167-177, which is what bd-qpm0's close_reason actually reports as RED ('detail view is bounded: retained 1500 of 1500').",
    "autofix": "gated_auto",
    "confidence": "high — test executed (0.17s), struct layout read, arithmetic shown"
  },
  {
    "id": "F8",
    "severity": "P2",
    "condition": "Anyone follows the consumption guidance on the Subscription type after dc1d084.",
    "evidence": "crates/toron-core/src/bus/subscription.rs:53-55 still reads 'Consume with [`next()`](Self::next) or `Stream::next` via [`tokio_stream`]'s `ReceiverStream` wrapper.' After dc1d084 `into_inner` returns `DetachedSubscription` (subscription.rs:100), not `mpsc::Receiver<SubscriptionEvent>`, so `ReceiverStream::new(sub.into_inner())` no longer compiles. `grep -rn 'impl Stream'` over the bus modules returns no `impl Stream for Subscription`, so the doc was already describing a path that does not exist; dc1d084 made it actively wrong rather than merely aspirational.",
    "autofix": "gated_auto",
    "confidence": "high — doc text and signature both quoted from the file; no Stream impl in tree"
  },
  {
    "id": "F9",
    "severity": "P2",
    "condition": "dc1d084's added surface is weighed against what the leak fix actually required.",
    "evidence": "dc1d084 added `DetachedSubscription` (subscription.rs:117-131), its own `Drop` impl (:141-145), and converted `Subscription.rx` from `mpsc::Receiver` to `Option<mpsc::Receiver>` (:50) plus a `release: Option<ReleaseOnDrop>` field (:53) — roughly 40 added lines and a second Drop impl across the file. `grep -rn 'into_inner()'` over crates/ returns no caller of `Subscription::into_inner`; the only `into_inner` hits are `PoisonError::into_inner` and `ids.rs` String accessors. The leak is fixed by `Subscription`'s own Drop (:133-139) alone. AGENTS.md states 'No backwards-compatibility debt. Early development, no users', so there is no external-consumer justification for the public signature change from `mpsc::Receiver<SubscriptionEvent>` to `DetachedSubscription`.",
    "autofix": "manual",
    "confidence": "high — grep for callers returned empty; the struct and both Drop impls read at the cited lines"
  },
  {
    "id": "F10",
    "severity": "P2",
    "condition": "bd-cqo2's or bd-5z0l's evidence lines are relied on as reproducible.",
    "evidence": "dc1d084's message claims 'Measured 50k subscribe+drop cycles: 8,562 B -> 12 B per subscription, subscriber_count returns to 0.' The committed fence does 10 cycles in each of 2 tests (bus_subscriber_leak.rs:72-75 and :94-100) and asserts only `subscriber_count() == 0`; there is no byte measurement and no 50k cycle anywhere in the tree. bd-5z0l's message claims '12 tests failed' and 'seven that were red', but the per-suite deltas it gives account for 9 (cli_robot 37-was-28) + 1 (cli_operator 12-was-11) + 1 (adr0027_slice1 8-was-7) = 11 failing tests across 3 named suites, and sc27-sc33 is reported with no 'was' count. Four red suites and one failing test are unaccounted for.",
    "autofix": "manual",
    "confidence": "high — the committed test bodies read line by line; the commit messages quoted verbatim"
  }
]}
```

## Verified clean (checked, no finding)

- **bd-cqo2's Drop cannot deadlock.** `fanout` (in_process.rs:546-549) clones the subscriber
  `Vec` under the read guard and drops the guard BEFORE calling `fanout_to` (:551-558), so the
  write lock taken in Drop is never acquired while a read guard is held on the same thread.
- **bd-cqo2's `Closed` arms are reachable, not dead.** A publisher that cloned the `Arc` at
  in_process.rs:546-548 before the subscriber dropped still calls `fanout_to`, gets
  `TrySendError::Closed`, and reaches :259-262 / :272-275. The `remove` there is a redundant
  no-op, not a bug. The commit's "parking_lot locks synchronously, so it is safe outside a
  runtime" is true.
- **bd-qpm0's fold ordering is race-free.** totals.fold → recent.push_back → `live.fetch_sub`
  (wake.rs:543-556), so when `subscribers_live == 0` the window already holds the record. That is
  what makes `wait_for_finished` (bus_wake_stats_bound.rs:59-71) a sound gate for the
  aggregate-vs-detail agreement assertions. Correctly done.
- **No mutex-poison finding.** `stats()`/`aggregate_stats()` use `if let Ok` / `unwrap_or_default`,
  which would silently freeze a metric on a poisoned lock — but the only holders are saturating
  arithmetic and `VecDeque::push_back`, which cannot unwind. Retracted as unreachable.
- **Both reviewed fences are green.** `bus_wake_stats_bound` 2 passed, `bus_subscriber_leak`
  2 passed, `run_state_conformance` 12 passed, `mbx fmt --check` exit 0.

## Open decisions (report-only escalation, not decided here)

**OD-1 — collapse `WAIT_WAKE_KINDS` into `toron_core::bus::wake::WAKE_KINDS` (F3 structural).**

1. Original criterion: F3's detection condition is silent `wait --corr` timeout on wake-kind
   drift, witnessed by a deleted test and three hand copies.
2. Proposed expansion: delete the toron-cli copy outright; toron-cli already depends on
   toron-core, so plane.rs:1624 becomes a re-export or is deleted and its two call sites
   (:1663, :1725) import from core.
3. Smallest compliant alternative: F3's `gated_auto` fix — restore the test and add one
   assertion that `WAIT_WAKE_KINDS ⊆ WAKE_KINDS`. That catches the silent-timeout mode without
   touching the constant's identity, and it is what I recommend if the operator wants the
   narrowest change.
4. Accept vs decline: accept removes a copy and makes drift unrepresentable; decline leaves three
   copies and relies on a test to keep them in sync. Either is defensible; the choice is the
   operator's because it touches two call sites in a crate whose CLI surface is governed by the
   Skill Sync Contract.
5. Recommendation: take the narrow fix (F3) now, file the collapse as a P3 cleanup. The
   invariant is what protects the wake path, not the deduplication.

**OD-2 — should `decide_source` prefer the data-dir pin, or refuse on conflict (F1)?**

This is a settled-adjacent call. ADR-0035 Spec 4.5 / bd-nsr7 chose "an explicit daemon URL
connects or errors — never a silent second store" and mailbox_source.rs:231-234 documents it.
Both candidate fixes honor that contract, so I am not choosing between them: (a) rank
`data_dir_pinned` above `env_url`, treating the durable-root pin as the more specific signal, or
(b) keep the ranking and fail loudly when both are set, naming both. (b) is the smaller change
and adds no ordering argument; (a) is what the ten patched test files implicitly assume.
Recommend (b) first, then (a) if the conflict proves common. The operator picks; the reviewer
does not self-answer this.

## Changes required for my agreement

1. **F1** — fix `decide_source` (mailbox_source.rs:207-226) so an explicit `TORON_MCP_DATA_DIR`
   is not silently discarded, and file the bead. The ten test-file patches in 53d7cb0 are
   belt-and-braces, not the fix. Until then, any `toron` command in a crew pane that believes it
   is store-isolated is not.
2. **F2** — repair bd-4dpq's VERIFY to `mbx +nightly-aarch64-apple-darwin test -p toron-core
   --test run_state_conformance`, then close the bead with a gate row, `commit_sha` bound to
   `75ac551` (the newest citing commit), and a PRINCIPLES entry carrying `name — decision`
   (required on P0-P2; the field is currently absent).
3. **F3** — restore `live_watch_wait_wake_kinds_cover_doorbell` and extend it with
   `WAIT_WAKE_KINDS ⊆ WAKE_KINDS`. The invariant holds today, so this is free.
4. **F4** — split the dirty diff: fmt reflow in one commit, the `conformance_scripts_are_deterministic`
   prune in a second, and do not let a semantic change ride a formatting diff again.
5. **F5** — record bd-qpm0's unfixed per-tick resubscribe churn (wake.rs:493) as a bead, or
   amend the close_reason to say it was consciously deferred. Right now it is silently dropped.
6. **F6-F10** — the doc corrections and the evidence reconciliation. F6 and F8 are one-line edits.

Nothing above is applied. This review is report-only per ADR-0009.
