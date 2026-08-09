# ADR-0008 Round-1 Counter — Adversary 1 (Round-1 code-grounded counter)

## Verdict: AGREE-WITH-CHANGES

All 11 P0 evidence paths verified in code (2 need path/arg corrections, none missing).
8 gate tests executed: all PASS. 2 audit gates (S4, S8) pass with notes. 1 claim
(bd-XXX ×3) is code-GREEN but only 1 of 3 fixes is regression-pinned by a test.
No RED claims. Four track-level defects found in P1–P6 (dead-code list is wrong,
D8.2 names a buildable that does not exist, unsafe-checker is a mismatch for a
zero-unsafe tree, NIP-40 expiration has no ingest probe). All amendable.

## P0 claim verdicts (Adversary 1 only)

| Claim | Evidence verified | Gate real | Gate result | Verdict |
|-------|-------------------|-----------|-------------|---------|
| **S1** NIP-17 DM triggers `message_posted` | ✓ `unwrap.rs:213` `pub trait ProjectionSink`; `unwrap.rs:226` `TriggerNotifySink`; host wiring `server.rs:463–469` (`with_trigger_tx`) + `server.rs:510–511` (`WorkflowHost::new` + `spawn_mail_trigger_loop`) | ✓ REAL e2e: `serve_wire_gift_wrap_fires_message_posted_once` (`server.rs:446`) — gift-wrapped `Mailbox::send_message` (not raw kind-14); asserts exactly 2 kind-1059 wraps, stamp present on reply (`:565`), no 3rd wrap in 4s grace (`:573–589`); can FAIL on self-loop or stamp loss | **PASS** (50.3s) | GREEN |
| **S2** Workflow cannot re-trigger itself | ✓ `trigger_filter.rs:86` `WORKFLOW_EMITTED_TAG`, `:121–133` `should_evaluate_trigger_event` (kind + tag belt-and-suspenders); all 3 production emit paths stamped: `workflow_host.rs:109` (SendMessage), `:137` (SendDm), `:202` (coalesced 43001 via `publish_coalesced_job`); runner cap-flush `:161` + host tick flush `:441–461` both route through the stamped `publish_coalesced_job`; `engine.rs` has **zero** `bus.publish` (pure) | ✓ REAL: 19 `trigger_filter` tests pass (`all_workflow_run_kinds_excluded`, workflow-emitted kind-14 rejection at `:235–267`) + S1 gate's self-loop grace window | **PASS** | GREEN |
| **S3** Shared durable mail state across processes | ✓ `server.rs:283` `TORON_MCP_DATA_DIR`; `server.rs:292–304` default = `ToronPaths::resolve()` durable root + `ensure_operator_key_mode`; demo-mode per-pid temp only under `TORON_DEMO` (`:275–282`) | ✓ REAL but **indirect**: `cli_robot.rs` (toron-cli/tests) — in-process `register_test_agent` then shelled-out `toron` binary, both on one `TORON_MCP_DATA_DIR`; empirically fails closed: isolated root → `exit=1` "agent GhostAgent has no local key" (verified by running the binary). Proves shared identity/keys, NOT mail-content sharing | **PASS** (5/5, 14s) | GREEN (gate wording amend, agenda #3) |
| **S4** toron mail ids round-trip as String | ✓ `tools.rs:1512+` `AcknowledgeMessageArgs.message_id: String`; `tools.rs:1523+` `MarkMessageReadArgs.message_id: String`; `tools.rs:1602+` `ForceReleaseFileReservationArgs.file_reservation_id: String`; `ids.rs:17` `MessageId(String)` newtype with `FromStr` rejecting empty/whitespace (`:31–43`); `surface.rs:658/671` coercion `MessageId::from` | ✓ (audit) — claim's wording "force_release `message_id`" misnames the arg: it is `file_reservation_id` (kind-30031 hex, `tools.rs:1607`). Substance holds: all String, no i64/u64 | **PASS** | GREEN (naming amend, agenda #1) |
| **S5** `foreach` binds loop variable | ✓ `engine.rs:650` `run_foreach`; template injection path `engine.rs:1139–1146`; `{{item}}` documented at `engine.rs:2036–2039` | ✓ REAL: `foreach_over_list_collects` (`engine.rs:2041`) asserts `bodies == ["a","b","c"]` (`:2089`) — fails if `let _ = item;` regresses | **PASS** (`cargo test -p toron-workflow --lib foreach_over_list_collects` → `test engine::tests::foreach_over_list_collects ... ok`) | GREEN |
| **S6** `call_workflow` composes | ✓ `engine.rs:738` `run_call_workflow`; `engine.rs:96` `WorkflowResolver` type + `:266–268` `default_workflow_resolver`; DI via `ctx.get_dependency` (`:746`) | ✓ REAL: `call_workflow_nested_subrun_returns_child_output` (`engine.rs:2136`) asserts child output nested under `sub` (`:2197–2200`) + exactly 1 child action (`:2201–2205`) — not a `{ok:false}` stub | **PASS** (`... call_workflow_nested_subrun_returns_child_output ... ok`) | GREEN |
| **S7** Mention burst → one job at host | ✓ `workflow_host.rs:81` `Arc<std::sync::Mutex<MentionCoalescer>>`; flush paths `workflow_host.rs:148–179` (runner cap-flush) + `:441–461` (host tick) | ✓ REAL: `coalesced_burst_publishes_one_stamped_43001` (`workflow_host.rs:824`) — 3 mentions with cap-3 coalescer → `assert_eq!(published, 1)` (`:875`) + host-path batch publish | **PASS** (`... coalesced_burst_publishes_one_stamped_43001 ... ok`) | GREEN |
| **S8** 25 resources readable | ✓ `resources.rs:11–129` exactly 25 RESOURCES entries; `:177` metrics_core stub confirmed `"database": "not wired", "storage": "not wired"`; 11 data-projection arms (`:209–257`), 7 static arms (`:163–186`), 2 generic `live_via_tools` stubs (recent, capabilities) | ✓ REAL tests: `exactly_25_resources` (`:589`), `static_resources_serve_json` (`:625`), `data_backed_resources_project_mailbox_data` (`:634`) | **PASS** (7 tests, 30.6s) | GREEN-with-decision (metrics_core allowlist, agenda #2) |
| **S9** AM superset at flywheel boundary | ✓ `docs/parity/known-divergences.md` §KDF (`:123–135`, current with F2.KDF gate) + flywheel MailBackend note (`:168`) | STUB by design — captain decision 3, out of scope | N/A | GREEN (batch updates the doc) |
| **F2.KDF** Mailbox startup bound | ✓ `mailbox/mod.rs:175` `cached_load_agent_key` (content-keyed cache); `:196–198` `kdf_load_count`; loop rebuild at `:229–244` | ✓ REAL: `unwrap_loop_kdf_cached_between_rebuilds_many_keys` (`mailbox/mod.rs:540`) — 50-key fixture, asserts KDF counts 50 → 51 → 51 → 52 (`:562–598`); can FAIL if cache misses | **PASS** (7.7s) | GREEN |
| **bd-XXX ×3** | ✓ comments at cited line numbers — **in `crates/toron-core/src/mailbox/`, not `toron-mcp/src/` (charter gives bare names)**; all 3 document FIXED bugs: `build_slots.rs:117` (renew base = existing lease expiry `:120–121`, not `now`); `identity.rs:73` (create fails closed on existing key `:80–84`); `reservations.rs:239` (renewal = old absolute expiry + extend `:240–247`, no 2x) | ✗ **audit-only, no named gate test**; only identity has a regression pin: `test_create_always_different_npub_or_refuses_collision` (asserts refusal + no npub rotation). `build_slots.rs:271` `renew_and_release_lifecycle` and `reservations.rs:735` `test_renew_and_release` assert only `Some`/count — a re-introduced under-extension/2x-expiry bug would PASS both | **PASS** by inspection; 2 of 3 fixes UNPINNED | YELLOW (agenda #4) |

**Gate-execution log:** `foreach_over_list_collects` ok · `call_workflow_nested_subrun_returns_child_output` ok · `coalesced_burst_publishes_one_stamped_43001` ok · `serve_wire_gift_wrap_fires_message_posted_once` ok (50.3s) · `unwrap_loop_kdf_cached_between_rebuilds_many_keys` ok (7.7s) · `trigger_filter` 19/19 · `resources` 7/7 · `cli_robot` 5/5 (14s). All from the live tree, no modifications.

## Change Agenda (low → high leverage)

1. **change:** P0 table — give full evidence paths for bd-XXX (crates/toron-core/src/mailbox/{build_slots,identity,reservations}.rs) and correct S4's arg name (force_release_file_reservation takes `file_reservation_id`, not `message_id`).
   - `posture:` narrow
   - `evidence_of_defect:` charter §P0 table rows bd-XXX + S4; `tools.rs:1602–1607` (actual arg); the three comments live in `crates/toron-core/src/mailbox/` — bare names mislocate them.
   - `recommended_change:` charter text fix — full paths + correct arg name. Code-grounded review cannot chase wrong paths.
   - `confidence:` High

2. **change:** S8 gate — add an explicit decision rule for honest stubs: `metrics_core` (`resources.rs:176–178`, "not started"/"not wired" **field values**) is an acceptable honest stub and must not be "fixed" by faking metrics; the 2 generic `live_via_tools` stubs (recent, capabilities) are honest per `resources.rs:139–160`. Gate wording "not 'not yet wired'" is ambiguous — it cannot distinguish the honest stub (allowed) from a placeholder (not allowed).
   - `posture:` narrow
   - `evidence_of_defect:` `resources.rs:176–178` — the charter's own cited evidence line contains the strings its gate bans; without a rule the crew either rubber-stamps the stub or churns it.
   - `recommended_change:` charter S8 gate: "each of the 25 resolves to non-empty JSON (test `static_resources_serve_json`/`data_backed_resources_project_mailbox_data` extended to cover all 25 URIs); stubs allowed only with honest field values + a `status` field naming the live tool."
   - `confidence:` Medium

3. **change:** S3 gate wording — state that `cli_robot.rs` proves cross-process sharing of agent identity/keys (not mail-content). If the claim is "both processes see the same inbox", the gate needs a send-in-process → inbox-in-subprocess assertion (currently only exit-0 + JSON-shape are asserted, `cli_robot.rs:83–94`).
   - `posture:` narrow
   - `evidence_of_defect:` `cli_robot.rs:71–94` — `inbox_parity_emits_valid_json` asserts exit 0 + array shape only; mail-content sharing is untested. The empirical fail-closed test (missing key → exit 1) saves the gate today, but only for identity sharing.
   - `recommended_change:` charter S3 gate: add "cli_robot asserts the subprocess sees a message sent in-process" (batch-fix RED test) OR reword the claim to "shared durable agent state".
   - `confidence:` Medium

4. **change:** bd-XXX ×3 — add regression tests pinning the fixed renewal semantics for build_slots and reservations (absolute-expiry extension, not now-relative).
   - `posture:` narrow-structural
   - `evidence_of_defect:` `build_slots.rs:271–283` (`renewed.is_some()` only — under-extension bug would pass); `reservations.rs:735–750` (`renewed == 1` only — 2x-expiry bug would pass). The code is fixed (`build_slots.rs:120–121`, `reservations.rs:240–247`) but unpinned; a future refactor re-introduces the ADR-0007 FAIL cells silently.
   - `recommended_change:` charter bd-XXX gate: "for each of the 3 comments, a regression test asserts the fixed behavior" — identity already has one; add renew-expiry assertions in the batch fix (RED first per AGENTS.md).
   - `confidence:` High

5. **change:** P2 method — add a silent-Result-drop pass (`let _ =`, `.ok();`, ignored `Result`/`Option`) plus a count-conservation rule: every one of the 219 `unwrap()` / 984 `expect()` / 67 `unwrap_or(0)` sites classified invariant-true or risk (no cherry-picking).
   - `posture:` narrow
   - `evidence_of_defect:` the silent-drop class is invisible to the `$X.unwrap()`/`$X.expect($MSG)` ast-grep. Canonical deliberate case needing triage: `unwrap.rs:251` `let _ = self.tx.send(mail.clone())` (best-effort by design); accidental siblings would not surface. 1203 sites is a large population — without count conservation a partial pass can be rubber-stamped.
   - `recommended_change:` charter P2 method: add ast-grep `let _ = $X` + `.ok()` patterns; require "classified N = audited N = baseline N" in the findings table.
   - `confidence:` Medium

6. **change:** P1 method — add two explicit probes: (a) `RecvError::Lagged` handling (broadcast-lag swallow), (b) std-Mutex guard held across `block_on_async`/thread-blocking publish (the `.lock().await` ast-grep cannot see it).
   - `posture:` narrow
   - `evidence_of_defect:` `relay_bus.rs:196` `Err(RecvError::Lagged(_)) => continue` — lag events silently dropped on the federation bus; the default in-process bus is per-subscriber mpsc by design (`in_process.rs:13` comment), so the risk is relay-only but real. `workflow_host.rs:441–461` holds `self.coalescer.lock().expect("coalescer lock")` while `publish_coalesced_job` blocks on a scoped thread + `rt.block_on(bus.publish)` (`:207–218`) — a std-guard-across-blocking-publish stall candidate; `.lock().await` greps never match either site. Also the charter's own S7 row flags `workflow_host.rs:156` poison risk — the method must actually find that class.
   - `recommended_change:` charter P1 method: add `Lagged` grep + `lock().unwrap()/lock().expect(` within ±10 lines of `.await`/`block_on`/thread-scope probe; classify stall-vs-cancel per site.
   - `confidence:` High

7. **change:** P6 — `docs/decisions/_scripts/verify-adr-links.sh` does not exist (checked: `docs/decisions/_scripts/` absent). The charter's "if present, else enumerate" escape is honest; make the enumeration the named gate and add the script to the batch.
   - `posture:` narrow
   - `evidence_of_defect:` `ls docs/decisions/_scripts/` → no such dir. AGENTS.md mandates the script; the review will be the enforcement point.
   - `recommended_change:` charter P6 gate: "enumerate `// governed-by` markers across src + tests; the batch adds verify-adr-links.sh and wires it".
   - `confidence:` Medium

8. **change:** P4 dead-code target — the 5 clippy "dead code" items are LIVE test fixtures; the charter's "remove or wire" must be "wire", never "remove".
   - `posture:` narrow
   - `evidence_of_defect:` `noop_runner` used by `engine.rs:2211` (call_workflow_unknown test), `toron-cli/src/workflow.rs` (2 sites), `sc11/sc18/sc19/sc20` e2e; `next_tick` in `cron.rs` + `tests/common/mod.rs`; `housekeeper_workflow` in `tests/common/mod.rs` (via `housekeeper_resolver`, used by sc18–sc20); `fixture_registration` in `sc24_cron_catchup.rs` + `common/mod.rs`; `cron_registry` in `common/mod.rs`. Following the charter literally deletes fixtures the e2e suite compiles against → the batch fix would break sc11/sc18/sc19/sc20/sc24.
   - `recommended_change:` charter P4 target: "reclassify as test-support fixtures; wire behind `#[cfg(any(test, feature = \"e2e-fixtures\"))]` or move to toron-e2e common; removal is forbidden unless a test references zero sites."
   - `confidence:` High

9. **change:** P5 D8.2 — name the agent-mail artifact. There is no agent-mail backend in this tree (`crates/toron-core/src/backend/` = herdr, tmux, mock, error, types). "agent-mail must build + pass D8.2 parity" names a buildable that does not exist here.
   - `posture:` narrow-structural
   - `evidence_of_defect:` `ls crates/toron-core/src/backend/` — no agent-mail impl; the parity surface is `crates/toron-mcp/tests/am_parity_e2e.rs` (live daemon, 38-tool functional parity vs the documented agent-mail contract) + `cli_robot.rs`. As written, P5 cannot execute its own gate.
   - `recommended_change:` charter P5: define D8.2 as "am_parity_e2e + cli_robot pass" against `docs/parity/am-rust-tool-catalog.md` + `known-divergences.md`; state explicitly that agent-mail (external) is out of repo and its "build" is covered by flywheel CI, or cite the artifact.
   - `confidence:` High

10. **change:** P3 skill mapping — drop `unsafe-checker` (the tree is `#![forbid(unsafe_code)]`, 0 real unsafe — the baseline itself proves this) and anchor the track on named crypto invariants, including a NIP-40 expiration probe on the mail ingest path.
    - `posture:` narrow-structural
    - `evidence_of_defect:** (a) skill mismatch: unsafe-checker reviews unsafe blocks; there are none. (b) Real hole risk: seal verify exists (`gift_wrap.rs:100` `verify_event(&seal)`, `:114–117` SenderMismatch), but **no `expiration` handling found on the ingest path** — grep across `mailbox/` shows NIP-40 only in `reservations.rs` (leases) and slots; the unwrap pipeline (`unwrap.rs`/`mailbox/mod.rs:223–244`) has no TTL check. The charter's P3 target "NIP-40 expiration TTL honored" has no probe and no gate test — an expired-but-never-seen gift wrap would be accepted, voiding the replay/expiry claim.
    - `recommended_change:` charter P3: skill list → m02-resource + m13-domain-error (keep) + method probes: (1) verify-before-project per ingest path (named fn + line), (2) NIP-40 expiration honored on unwrap → if absent, file as P0-level finding + RED test in the batch, (3) replay = `is_projected` dedup confirmed per sink.
    - `confidence:` High

**Highest-leverage items: #10 (P3 expiration probe — voids a Parity Superiority claim if red), #9 (D8.2 unexecutable as written), #8 (dead-code list would break the batch), #6 (P1 method misses its own named risk class).**

## Notes

- No P0 claim is RED; no gate is a tautology. The two honest negatives that most improve the charter: bd-XXX unpinned renewals (#4) and the NIP-40 ingest absence (#10).
- Flywheel stays out of scope (captain decision 3): `relay_bus.rs:196` Lagged-swallow and the S9 boundary are documented here and in `known-divergences.md`, not edited.
- Baseline sanity: engine.rs 2401 / unwrap.rs 1114 / workflow_host.rs 1076 / reservations.rs 1007 LOC all confirmed at read time.

---

# ADR-0008 Round-1 Counter — Adversary 2 (charter-coherence)

## Verdict: AGREE-WITH-CHANGES

The charter is substantially sound: the Measured Baseline reproduces (LOC,
per-crate, unwrap/expect/clone/lock/await/test counts all exact), the six
captain decisions are encoded, the named gate tests exist and are
falsifiable, and the debate protocol has no hard deadlock. It is NOT
rubber-stampable: several gates are honest audits that will plausibly go RED
(S8 resources still say "not wired"; clippy dead code is real). Four
coherence defects and three risk-gap omissions require amendment before
acceptance.

## Baseline reproduction (3+ greps)

| Baseline row | Charter | Re-run | Match |
|---|---|---|---|
| Total LOC (src+tests) | 45,609 | 45,609 | exact |
| `#![forbid(unsafe_code)]` | every crate root + 23+ files | all 5 crate roots + test files confirmed | yes |
| Real `unsafe` blocks in src | 0 | 5 non-forbid hits all doc comments; zero real `unsafe {}` | yes |
| `panic!`/`todo!`/`unreachable!` in src | 0 (35 hits all `#[cfg(test)]`) | 35 hits all inside `mod tests` | exact |
| `unwrap()` | 219 | 219 | exact |
| `expect()` | 984 | 984 | exact |
| `unwrap_or(0)`/`unwrap_or_default` | 67 | 67 | exact |
| `Mutex`/`RwLock` decls | 102 | 102 = word mentions; real instantiations `Mutex<`/`RwLock<` = 52 | label drift (C2) |
| `.lock()` sites | 122 | 122 | exact |
| `.await` sites | 1,324 | 1,324 | exact |
| `clone()` | 548 | 548 | exact |
| Tests | 574 (335+239), 2 `#[ignore]` | 574; 2 `#[ignore]` | exact |
| Mega-files | 9 listed | all 9 verify | exact (src only; am_parity_e2e.rs tests=1059 excluded — C3) |
| bd-XXX x3 | 3 comments | 3 present at exact cites | exact |

**Verdict:** baseline honest. 3+ greps reproduce exactly. Two label/method
drifts (C2, C3) are presentation-level, not fabrication.

## Change Agenda (low -> high leverage)

1. **Fix "Mutex/RwLock decls" label + method** — 102 = mentions (incl imports/docs); real instantiations = 52. Relabel + add 52 as P1 working number. (High)
2. **Pin exact grep for "80 unsafe"** — `rg -n 'unsafe' crates/*/src | rg -v 'forbid\(unsafe_code\)'` = 5 hits all comments; assert 0 real `unsafe {}`. (High)
3. **Mega-file table excludes test files without saying so** — add column "scope: src (tests excluded)" or add am_parity_e2e.rs as 10th row marked tests/. (High)
4. **Make S8 gate falsifiable** — "honest structured stub" escape hatch lets resources.rs:177 "not wired" pass. Gate = JSON with `"stub": true` + owner-documented; any remaining "not wired" string = RED. (High)
5. **Fix phantom "captain decision 7"** — charter cites "decision 7 (debate)" but enumerates 6. Promote debate-governance to 7th decision OR rewrite citation. (High)
6. **P5 gate method wrong** — `cargo test --workspace` does NOT run feature-gated e2e (sc9-sc25 under p1/p2/p4/p5, no default). Method = `--all-features` or per-gate matrix. (High)
7. **S1 gate needs e2e-level gift-wrap witness named** — in-tree workflow_host.rs:926 uses raw kind-14 (the C7 trap). Gate should state witness must exist or be authored RED-first. (High)
8. **Debate REJECT path single-blind** — captain is sole judge; add bounded second loop: crew flags ACCEPT-MODIFIED/REJECT rulings that change a P0 gate; flagged = two-line captain justification in ADR. (High)
9. **Add R8 — baseline/gate staleness mid-execution** — re-run 5 cheap greps before batch-fix; file:line cites re-verified at P6. Precedent: ADR-0007 stale-cite failure. (Medium-High)
10. **Add R9 — counter independence** — captain is author AND judge; add cross-check: oc verifies oc2's cites (and vice versa) before captain rules. (Medium)

## Charter-coherence findings

**Omitted track?** None fully omitted. Performance (decision 6) is encoded
but has no owner row in deliverable tables — exists only as risk mitigation
(R5). Acceptable (R5 honest) but P1 deliverable should name "backpressure +
latency measured baseline" so performance has a table.

**Captain's 6 decisions fidelity:** all 6 encoded (lines 51-67). Caveat:
charter cites "decision 7 (debate)" (line 291) but enumerates 6 — C5.

**Gates falsifiable?** S1 falsifiable but currently unsatisfiable-in-tree
(C7). S2/S3/S5/S6/S7/F2.KDF/bd-XXX all falsifiable. S8 can fail but escape
hatch makes RED winnable-away (C4). No tautological gates.

**Debate protocol soundness?** No hard deadlock (REJECT -> respawn). Real
gap: rubber-stamp undetected — captain is author + judge + sole rater (C8, C10).

**Risks R1-R7 complete?** Covered. Missing R8 (staleness, C9) + R9
(independence, C10) + R10 (e2e feature-gate skew, folded into C6).

## Bottom line

Charter executable + mostly honest. Fix S8 escape hatch, P5 feature-gate
method, phantom decision 7, add two governance gaps (captain-single-judge,
baseline staleness) — then accept.
