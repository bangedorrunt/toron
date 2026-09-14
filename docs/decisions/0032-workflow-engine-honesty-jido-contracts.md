<!-- governed-by: ADR-0024, ADR-0026, ADR-0027, ADR-0028, ADR-0014, ADR-0003 -->
---
status: accepted
date: 2026-09-12
decision-makers: Captain (bangedorrunt)
consulted: Jido v3-spike branch study (2026-09-12); jido_action release/v3 branch study (2026-09-12); Jido v2.3.x and jido.run study (2026-09-11); grilling interview and 25-defect audit (2026-09-11); toron ADR review (0024, 0026, 0027, 0028); fresh-eyes contract review (2026-09-12)
informed: flywheel x toron x beads maintainers
---

# ADR-0032: Workflow-Engine Honesty, Jido Action Contracts, and Truthful Effect Lifecycle

**Status:** Accepted — captain word on this file, 2026-09-13 (stack-reset grilling session, QuietHeron, bead bd-2o1v). Implementation state at acceptance: workflow_effects store + reconcile policy + EffectDispatcher live (toron-core migration 0010, toron-workflow engine/reconcile.rs, toron-mcp workflow_host); engine run-path journaling remains host-owned per boundary.rs (follow-up beads sequence it).

**Implementation state as of 2026-09-14:** journaling is still host-owned by design, but it is no longer silent. The engine marks the gap instead of falling through to the compat runner unflagged — an effectful leaf with no `EffectRunner` carries `EFFECT_JOURNAL_BYPASSED` (`bypassed-no-effect-runner`), and a sink an injected runner does not cover carries `EFFECT_JOURNAL_SINK_UNCOVERED` (`bypassed-sink-uncovered`), both with a loud reason. The first embedding adopted the contract: flywheel injects a host-owned `EffectRunner` (`flywheel/src/effect_journal.rs`) for every run that really dispatches, landing in the same `workflow_effects` tables keyed by the engine boundary's own derivation.

**Implementation state as of 2026-09-14 (the Spec 6 flip, `bd-engine-run-path-journaling-fwlx`):** the marking half landed first (warn-and-tag), and its own precondition — an embedding that injects a runner — is now met, so the fail-closed half landed too. The engine carries an explicit `EffectJournalMode`: `Recording` (default) keeps the bypass/uncovered markers for arms that record instead of perform (dry arms: `workflow run` without `--execute`, `cron run` without `--live`), and `Enforced` REFUSES an effectful host sink with no injected runner. Both embeddings construct `Enforced` on the arms that really dispatch and `Recording` on the dry arms: flywheel (`flywheel/src/pane.rs`, `flywheel/src/cron.rs`) and the toron host (`toron-mcp/src/workflow_host.rs`). `verify` now routes through the normalized boundary's host-sink path like every other effectful leaf (it previously had its own direct-to-`VerifySink` arm that never reached an injected runner); the engine executes the embedding's `VerifySink` first and hands the already-executed result to the runner via `RunContext.provider_result`, so the journal settles AROUND an effect that already happened rather than re-performing it. Both runners journal that observation (flywheel's `effect_journal.rs`, toron-mcp's `mailbox_effect_runner`), and a host with no `VerifySink` reports the sink's own `{ok:false, error:"verify sink missing"}` (journaled as a proven failure) instead of a generic "no sink" error. Because verify moved onto the shared dispatch path it now also takes the shared RENDER, which its private arm had skipped: a templated `command: "{{inputs.verify}}"` — the spelling the builtin repair loop uses — reaches the sink resolved instead of verbatim. That makes verify's journal row **observational, not a pre-commit fence** — the one deliberate exception to Spec 5's intent-before-dispatch ordering, sound because `verify` is ADR-0024's synchronous, embedding-owned check (the engine cannot commit intent before a sink it hands the command to). A crash between the sink returning and the row committing leaves no row: a visible gap in the ledger, never a row claiming an effect that did not happen. The last follow-up landed (`bd-mi9n`): **engine-side run-lifecycle rows** (`Started` → `Running` → terminal) now travel through an injected `RunJournal` — the engine reports its own phases, so an engine-driven run has a `workflow_runs` row without the embedding calling `create_run`/`cas_run_status`, and the run row and its effect rows agree on `run_id` by construction. `apply_run_status` (toron-mcp) is the ONE write path: the host's own lifecycle calls and the injected journal both go through it, idempotent by construction (create-if-absent, CAS-only-when-different, terminal rows never move), so a replay cannot double-write or resurrect a finished run. Absent journal = no run rows, exactly as before.

**Decision in one sentence:** Keep Resonate, the closed `Action` enum, the DAG engine, the mailbox, and the existing kind taxonomy; add a single normalized action boundary, a real host run lifecycle, and a workflow-specific effect journal so that action contracts, failures, parked work, and external-effect uncertainty are explicit rather than inferred from mailbox delivery receipts.

**Extends:**

- [ADR-0024](0024-graph-first-repair-loops-bead-workflow.md): graph-canonical execution, runtime failure classification, VerifySink, parkable control-flow actions, and bounded repair as named graph nodes. Its generic retry rule is narrowed here: a `rate_limit`/`timeout` is retryable only when the effect is known safe or the provider honors the stable effect key; otherwise it enters reconciliation.
- [ADR-0026](0026-failure-taxonomy-funded-preflight-fallback-chains-mail-health-escalation.md): the closed runtime `FailureKind` set and its retry/reroute/escalation semantics. Its `rate_limit`/`timeout` retry rule is subject to the same effect-safety qualification.
- [ADR-0027](0027-one-workspace-tick-control-plane.md): effects as data, one control-plane tick, and explicit receipt ownership.
- [ADR-0028](0028-jido-behavior-first-on-wheelhouse-nouns.md): behavior-first contracts on local nouns, sender-owned `corr`, monotonic mailbox receipts, and refusal to port the Jido/OTP runtime.

**Amends:** ADR-0024 Spec 1 and ADR-0026 Spec 0 only to qualify `rate_limit`/`timeout` retry with effect-safety, stable-key, and reconciliation requirements. The kind names, spelling, and auth/billing/provider/blocked/cancelled routing remain unchanged; this is a soundness clarification, not a second retry policy.

**Does not amend:** the one-way dependency `flywheel → toron-workflow → toron-core`; the frozen kind registry; the frozen runtime `FailureKind` spelling `Auth | RateLimit | Timeout | Blocked | Provider | Cancelled | Unknown | Billing`; MCP tool count; beads as the work graph; `thread_id ≠ corr`; `workspace::tick` and the Tick Law; `br gate report`/`br close` ownership; or the worktree prohibition.

**Does not wire:** Jido's AgentServer, OTP supervision topology, Pods, CloudEvents, Phoenix PubSub, Jido Flow/Topology runtime, Splode or Zoi as dependencies, a general action registry, a whole-run transactional Turn, an engine shell, or a general compensation DSL.

**Tracker:** existing beads are retained but must be re-cut against this record before implementation: `bd-i6kc` (toron honesty gate), `bd-himk` and `bd-40d1` (flywheel router and run-ledger truth), `bd-htoo` (toron action/effect contracts), and `bd-3pti` (bounded reduce). Bead ID remains the mail `thread_id` and reservation `reason`.

> **Important correction:** the existing mailbox `receipts` table is a message-lifecycle store, not a workflow-effect journal. `owed → acked → resulted` cannot prove that a workflow effect was dispatched, cannot distinguish “dispatch never began” from “dispatch succeeded and the process crashed,” and cannot safely provide exactly-once behavior. The implementation must add workflow-specific journal state in the existing SQLite persistence boundary; it must not reuse mailbox receipt state by implication.

---

## 1. Scope and fixed point

This ADR has five goals:

1. **Honest execution:** every live action either performs its documented effect, returns a typed failure, or is removed/deprecated with a migration path. No catch-all `Ok(Null)`, silent unsupported action, swallowed output, or fictional security guarantee remains.
2. **One execution contract:** rendered actions pass through one boundary that normalizes input, validates output, carries run/node/effect context, applies one execution policy, and exposes the failure stage without importing Jido.
3. **Truthful durability:** every external effect has a stable logical identity and durable intent before dispatch. The system explicitly models `unknown_external_outcome`; it does not claim exactly-once execution where the provider cannot prove it.
4. **Real run lifecycle:** parked promises are drained by a host-owned lifecycle manager; pending and terminal events are published through existing kinds; resolutions are correlated and idempotent; run history records the real status, project, artifacts, and terminal evidence.
5. **Composable authoring:** the closed action enum gains Jido's useful contract discipline—static per-variant schemas, normalized invocation values, explicit output envelopes, canonical graph validation, bounded map/reduce/iterate behavior, stable node identity, and loud ambiguity—without porting Jido's runtime.

The fixed point is **truthful durable orchestration under at-least-once external effects, with explicit reconciliation where the external outcome is uncertain**. It is not “exactly-once workflow execution,” “rollback of completed side effects,” or “successful `engine.run` means business completion.”

### 1.1 Non-goals

- Do not make a workflow run a database transaction spanning Nostr, mailbox, subprocesses, Resonate, and external providers.
- Do not infer an external effect's absence from a timeout, process exit, missing receipt, or missing terminal event.
- Do not make the engine call `br`, shell commands, or provider APIs directly.
- Do not turn parse/lint validation codes into new runtime `FailureKind` variants.
- Do not replace the existing mailbox receipt store; link to it when useful.
- Do not add new Nostr kinds or MCP tools in this ADR.

---

## 2. Ground truth at the review boundary

### 2.1 Current toron seams that constrain the design

The following are implementation facts, not target behavior:

3. `toron-core` mailbox receipts track message lifecycle fields such as `corr`, state, wake, due time, result path, and timestamps. They do not contain workflow node identity, action digest, dispatch-attempt identity, external id, or an unknown-outcome state.
4. Mailbox idempotency and mailbox receipts are separate concerns. A mailbox send may use `corr` as a transport correlation/idempotency projection, but the mailbox receipt does not become a workflow journal merely because the same string appears in both places.
5. `mailbox_runner` currently has a catch-all arm that returns `Ok(Null)` for actions such as `react` and `call_webhook`. The current comments and public docs are ahead of the implementation.
6. The production trigger loop starts `engine.run`, logs or returns its outcome, but does not yet provide a complete lifecycle for draining `ApprovalSink`, `SpawnSink`, and `VerifySink`, publishing pending events, subscribing to resolutions, and publishing one semantic terminal record.
7. The sinks currently carry bare promise IDs. A bare ID is insufficient for a host to reconstruct the run, node, effect key, action payload, generation, and publication status after a crash.
8. Linear and nested step paths do not all share one guard evaluator. Graph-node guards and branch guards exist; a linear `Step.guard` and nested `Step` values can take different semantics.
9. `Spawn.no_claim_timeout` has a serde default of zero in the action shape even though the documented/default policy is 600 seconds. Zero must be an explicit spelling for “no timeout,” not an accidental default.
10. Direct graph execution can reach a stuck empty-ready-set without a typed cycle/stall failure if lint is bypassed. Runtime validation must remain a second honesty gate.
11. `run_call_workflow` and `invoke` have historically taken different input-binding paths. Declared defaults and `{{inputs.*}}` must have one binding function.
12. Foreach and parallel paths have silent coercions (`non-list → []`, invalid parallel count → one copy). Silent coercion is not a valid workflow contract.
13. The graph retry path has had a hard-coded bound separate from the enclosing retry configuration. One retry coordinator must own attempts, backoff, deadlines, and effect-outcome rules.
14. The CLI can construct an engine without the resolvers or lifecycle host required by approval/spawn-await actions. A run must fail preflight before parking forever in a no-op host.
15. The flywheel classifier has had two topology maps, a first-match keyword ladder, a silent steer fallback, a stale `code-review` stage name, and run-history writes that can report a failed run as `done` under project `workflow`.
16. The current `ACTION_TYPES`/control-flow lists are not allowed to become another stale copy of the closed enum. Advertised names must be derived from the enum or checked exhaustively against it.
17. A replay that re-renders volatile or environment-dependent templates can derive a different action digest and accidentally create a second logical effect. The first prepared normalized action identity must be durable and reused for retry/recovery.

### 2.2 Frozen failure vocabulary

The runtime failure kinds remain exactly:

| `FailureKind` | Meaning | Default handling |
| :--- | :--- | :--- |
| `rate_limit` | Provider/relay throttling, possibly with retry-after | bounded retry when the effect policy makes retry safe; otherwise blocked/reconcile |
| `timeout` | An operation-owned deadline | retry only when dispatch/effect state is known safe or the provider honors the stable effect key |
| `auth` | Credentials or authorization failure | terminal or bounded reroute; never blind retry |
| `billing` | Provider funding/account exhaustion | terminal or bounded reroute; never blind retry |
| `provider` | Harness, transport, unsupported-agent, or provider execution failure | terminal or bounded reroute |
| `blocked` | Reservation, build slot, dependency, or operator gate | escalate; never repair by blind retry |
| `cancelled` | Operator or explicit cancellation | terminal; no repair |
| `unknown` | Unclassified or insufficiently evidenced runtime failure | explicit failure/reconciliation; never silently continue |

`invalid_input`, `invalid_output`, `unresolved_ref`, `cycle`, `unknown_action`, `missing_sink`, and similar contract errors are **error codes/stages**, not new `FailureKind` values. They may appear in a structured action error with no runtime kind. This preserves ADR-0026's closed set and still gives Jido-style programmatic error decisions.

### 2.3 Jido v3-spike and `jido_action` release/v3 ground truth

The branch-specific study changes the plan in material ways:

| Branch pattern | Evidence from the branch | What toron takes | What toron refuses |
| :--- | :--- | :--- | :--- |
| Evaluation is separate from commit | `jido/v3-spike` Turn design preserves source Signal, fixes executable/input, validates a candidate, and gives evaluation no commit, persistence, dispatch, or settlement authority | One normalized action/evaluation boundary and explicit stage names | A Jido Turn runtime or BEAM process model |
| Commit, directive handling, settlement, and business completion differ | `jido/v3-spike` commit/effects design names separate completion points and says external business completion is not implied by Turn settlement | Separate `intent committed`, `dispatch accepted`, `effect settled`, and `external outcome unknown` states | A claim that one receipt or `engine.run` proves external completion |
| Recoverable work needs a stable ID and portable intent | The same design requires stable work ID, portable input, acknowledgement, retry/cancel/retention policy, and duplicate/conflict rules | `effect_key`, journal intent, provider idempotency/reconciliation contract | A universal exactly-once/outbox promise without provider support |
| Persistence distinguishes conflict and indeterminate writes | `jido/v3-spike` persistence design uses exact-byte CAS and treats unknown adapter results as indeterminate | Explicit `unknown`/reconcile state and fail-closed writes | Treating an error or timeout as proof that no write occurred |
| Action contracts validate both sides | `jido_action/release/v3` `Jido.Action` supports optional static input/output schemas and generated validators; `Jido.Exec.Action.Runner` separates input, execution, and output phases | One input/output contract table, normalized invocation, output validation, phase-bearing failures | Zoi/NimbleOptions/Splode dependencies |
| Execution policy is outside the Action | `jido_action/release/v3` says Exec does not retry; timeout, concurrency, continuation, and task supervision are explicit options | One host/engine policy object, no retry hidden in an action callback | The earlier “Jido Exec defaults include retries” assumption |
| Executable resolution and validation are one boundary | `Jido.Executable` resolves the exact target and validates required callbacks; `Jido.Instruction` normalizes target, params, context, and metadata | `ResolvedAction`/`ActionInvocation` normal form | An open runtime registry for YAML action names |
| Flows validate topology before execution | `Jido.Flow.Validation` checks unknown dependencies, duplicate names, cycles, output requirements, target kinds, and dispatch placement | Compile/normalize workflow once; lint and runtime both reject bad graphs | Jido Flow/Runic as a dependency or second scheduler |
| Flow identity is canonical and item/iteration identities are deterministic | `Jido.Flow.Identity` hashes canonical flow data and derives item/iteration IDs; `Jido.Exec.Work` tokens are revision-bound and stale-safe | Workflow digest, stable node path, deterministic iteration key, stale-resolution checks | Copying an in-memory step-token API into Resonate |
| Map/Reduce/Iterate are explicit and bounded | `jido_action/release/v3` has separate components, ordered map collection, serial reduce, and bounded `max_iterations`; execution bounds concurrency and continuations | Explicit bounded composites, stable order, fail-fast/collect-errors policy | Unbounded fan-out, silent coercion, implicit retry/continuation |
| The release/v3 tree has no old Catalog module | The branch tree contains `Executable`, `Instruction`, `Flow`, and validation modules, but not the previously studied v2 Catalog surface | Keep a flywheel-owned static routing catalog and loud ambiguity rules | Claiming current Jido v3 Catalog semantics as a dependency |

The v3 design documents are target designs and explicitly say code remains canonical until approval. They are useful design evidence, not a reason to claim that toron is implementing a stable Jido protocol.

---

## 3. Decision drivers and invariants

### 3.1 Decision drivers

1. **Honesty precedes leverage.** Contracts on top of no-ops are validated lies.
2. **One owner per semantic boundary.** The engine sequences; the host executes effects and owns run publication; the mailbox owns message delivery receipts; beads owns legal close.
3. **Stable identity before retry.** No dedupe, reconciliation, or crash matrix is meaningful without a stable logical effect key.
4. **At-least-once truth beats exactly-once theater.** A provider-specific idempotency key can improve the guarantee; it cannot retroactively make every provider transactional.
5. **Closed vocabulary, open evidence.** Action and failure names remain exhaustively checked; failures carry structured stage, code, and evidence.
6. **Loud boundaries, bounded work.** Unknown fields, unresolved refs, invalid collections, cycles, missing sinks, invalid limits, and ambiguous routes fail before silent degradation.
7. **No second control plane.** Resonate remains the durable run substrate; `workspace::tick` remains the flywheel control plane; the host lifecycle manager is an embedding adapter, not a new agent runtime.
8. **Preserve existing legal ownership.** The engine never calls `br`; a workflow run never writes `command-verified`; only the armed loop can produce the legal close evidence.

### 3.2 Invariants

- **I1 — Action completeness:** every `Action` variant is either handled by the engine, handled by a named host sink, or rejected/deprecated at lint. A live variant cannot fall through to success.
- **I2 — Normal-form execution:** a rendered action is normalized and validated exactly once before its execution policy and effect lifecycle are selected.
- **I3 — Context-bearing invocation:** every effectful invocation carries `project` (when scoped), `run_id`, canonical workflow digest, stable node path, iteration key, logical effect key, attempt number, a fencing/claim token, and policy/deadline context. Pure/control actions may omit effect identity; an effectful action with no effect identity is rejected before dispatch.
- **I4 — Intent precedes dispatch:** an effectful dispatch cannot begin until its workflow-effect intent is durably committed.
- **I5 — Unknown is visible:** a crash, timeout, or ambiguous provider response cannot be represented as a successful or absent effect without reconciliation evidence.
- **I6 — Mailbox receipts stay mailbox receipts:** `owed → acked → resulted` describes message lifecycle only; it is linkable to, but cannot replace, workflow-effect state.
- **I7 — Settlement is not business completion:** a successful action result, a terminal workflow event, and external business completion are separate claims.
- **I8 — One retry coordinator:** retries, backoff, deadlines, and effect-safety checks are selected by one engine/host policy path. `ActionRunner` never retries on its own.
- **I8a — Replay identity:** once an effect reaches `prepared`, its normalized action identity and effect key are fixed for that logical node; retry/recovery cannot re-render a different logical effect under the same run/node.
- **I9 — Guard parity:** top-level, graph, branch, retry-nested, foreach-nested, and called-workflow steps use the same guard/ref resolver.
- **I10 — No implicit fallback:** unresolved templates, non-list foreach input, invalid parallel counts, missing output, missing sink, and graph stalls are errors, never `0`, `[]`, `1`, else-branch, or `Ok(Null)`.
- **I11 — Park lifecycle:** every parked promise has a structured pending record, a published pending event, a validated resolution path, and a terminal/reconciliation path.
- **I12 — Terminal idempotency:** a run has one semantic terminal transition identified by `run_id + terminal_digest`; event delivery may be at-least-once, and consumers dedupe that identity.
- **I13 — Compensation is not rollback:** cleanup attempts are idempotent, separately journaled, and never erase the original failure or claim to undo an already completed external effect.
- **I14 — Close ownership:** workflow execution can produce evidence paths; it cannot produce a legal bead close.

---

## 4. Considered options

### Option A — Contract-first port of Jido

Rejected. It would validate actions before fixing silent runners, leave parked promises unowned, and make a validated no-op look more trustworthy.

### Option B — Port Jido's runtime

Rejected. AgentServer, OTP supervision, Pods, Signals, and Jido Flow/Topology would create a second runtime/control plane and weaken the existing Resonate/mailbox/tick model. ADR-0028 already rejects this class of port.

### Option C — Reuse mailbox `receipts` as Turn/effect receipts

Rejected. The table lacks workflow identity, action digest, attempt state, dispatch marker, external ID, and unknown outcome. `owed` is not “effect intent committed”; `resulted` is not “external business completed.” A distinct workflow-effect table in the same SQLite persistence boundary is the smallest truthful fix.

### Option D — Add an open action registry

Rejected. A name lookup adds an environment-dependent miss path to every dispatch and removes the Rust exhaustive-match witness that prevents silent arms. Third-party action authoring is a future ADR requiring signed/versioned contracts and a migration story.

### Option E — Keep the current `anyhow::Result<Value>` runner and document conventions

Rejected. The runner has no context with which to derive an effect key, no phase-bearing error boundary, and no way to distinguish effect uncertainty. A compatibility adapter may exist during migration, but production execution must use the context-bearing contract.

### Option F — Chosen: local contracts plus host-owned effect lifecycle

Chosen. Normalize and validate actions in the engine, pass a context-bearing request into an embedding-owned executor, durably record effect intent before dispatch, model uncertainty and reconciliation explicitly, and keep Jido's runtime out.

---

## 5. Normative contract

The words **MUST**, **MUST NOT**, and **MAY** are normative in this section.

### Spec 0 — Terminology and ownership

The following terms are deliberately distinct:

| Term | Owner | Meaning |
| :--- | :--- | :--- |
| `Workflow` | schema/engine | Authored YAML definition |
| canonical graph | engine | Normalized DAG used for all execution paths |
| `node_path` | engine | Stable authored location, including composite parents |
| `iteration_key` | engine | Stable item/iteration identity for foreach/map/iterate work |
| `workflow_digest` | engine | Hash of canonical workflow identity and schema version |
| `ResolvedAction` | engine | Rendered and normalized action with validated fields |
| `ActionInvocation` | engine/embedding boundary | Resolved action plus run/node/effect/policy context |
| `effect_key` | engine/host boundary | Stable identity of one logical external effect; excludes retry attempt |
| `attempt_id` | effect journal | Identity of one dispatch attempt for evidence |
| `corr` | mailbox transport | Sender-owned transport correlation; may project `effect_key` for mailbox effects |
| mailbox receipt | toron-core | Message lifecycle (`owed`, `acked`, `resulted`, `degraded`) |
| workflow effect journal | host/core persistence | Intent, dispatch, settlement, and uncertainty for a logical workflow effect |
| run ledger | host/flywheel | Workflow run lifecycle, terminal status, project, artifacts, and evidence |
| terminal event | host/bus | Existing kind-46012 notification of a semantic run terminal |
| bead close | beads/armed loop | Legal tracker transition; never an engine effect |

A “Turn” is an internal name for one action/effect boundary, not a serialized Jido Turn and not a transaction covering a whole run. A durable run is a sequence of turns separated by durable sleeps, parked promises, or external work.

### Spec 1 — Normalized action boundary

Replace the production-only use of the context-free runner with a context-bearing boundary. The exact Rust names may vary, but the semantic shape is fixed:

```rust
struct ActionInvocation {
    action: Action,                 // rendered, owned action
    run: RunContext,
    execution: ExecutionPolicy,
}

struct RunContext {
    run_id: String,
    workflow_digest: String,
    node_path: String,
    iteration_key: Option<String>,
    effect_key: Option<String>,
    attempt: u32,
}

struct ExecutionPolicy {
    deadline: Option<Duration>,
    max_concurrency: Option<u32>,
    retry: RetryPolicy,
}

struct ActionOutcome {
    value: serde_json::Value,
    effect: EffectSettlement,
}

struct ActionFailure {
    stage: ActionStage,             // normalize | input | execute | output | effect | park
    code: ActionErrorCode,
    message: String,
    kind: Option<FailureKind>,      // only the frozen runtime set
    retryable: bool,
    resumable: bool,
    disposition: Disposition,
    effect_state: EffectState,
    evidence: EvidenceRef,
}
```

The local contract vocabulary is closed even when the concrete Rust representation is private:

- `ActionStage` is `normalize | input | execute | output | effect | park`.
- `EffectState` is `none | prepared | in_flight | accepted | settled | failed | unknown | reconcile_pending | cancelled`.
- `Disposition` is `continue | retry | reroute | blocked | terminal_failed | cancelled | reconcile`.
- `ActionErrorCode` is an owned, documented code such as `invalid_input`, `invalid_output`, `unresolved_ref`, `unknown_action`, `missing_sink`, `cycle`, `stalled_graph`, `provider_error`, or `effect_unknown`; these codes do not expand `FailureKind`.
- `EvidenceRef` is a bounded projection of paths, hashes, external IDs, and safe provider/status details. It never contains a stack trace, secret, process handle, closure, or unbounded callback payload.
- `EffectSettlement` is `none` for pure/control actions and otherwise names the effect key, current effect state, and safe result/evidence projection.

The engine MAY represent these as enums and structs or as a private serde value, but every public/ durable projection MUST preserve the same meanings.
```

The old `Arc<dyn Fn(&Action) -> anyhow::Result<Value>>` type may remain behind a test/compatibility adapter for pure actions during migration. It MUST NOT be the production path for an effectful action after Spec 1 lands.

The boundary is split into explicit semantic sub-boundaries, even if the first implementation keeps them in one Rust module:

1. **Evaluation/normalization:** preserve the immutable trigger/source payload, render and bind inputs, normalize, validate input, and derive the canonical identity. This stage has no authority to publish, retry, resolve, or settle an external effect. Once the workflow route is selected, input preparation cannot trigger a second route lookup.
2. **Effect admission:** atomically insert/load the workflow-effect intent and claim an attempt in the existing persistence boundary.
3. **Effect execution:** the embedding invokes the named sink/provider once for that attempt with the stable effect key.
4. **Settlement:** the embedding validates the result, records settled/failed/unknown state, and returns bounded evidence to the graph.

The implementation MUST preserve this ownership order even if an adapter combines calls for efficiency. A callback that performs external I/O during evaluation remains subject to the effect journal; Jido's evaluation boundary does not imply rollback of that I/O.

The boundary order is:

```text
raw Step/Action
  → render templates and bind inputs
  → normalize ResolvedAction
  → validate action contract/input
  → derive canonical identity and policy
  → commit effect intent when effectful
  → execute exactly once for this attempt
  → normalize callback/transport result
  → validate output
  → settle effect journal or mark unknown
  → return ActionOutcome or ActionFailure
```

The engine owns action selection, graph sequencing, guard evaluation, and retry policy. The embedding owns mailbox/provider/subprocess effects, effect-journal access, provider idempotency/reconciliation, and host publication. Neither layer may treat a missing result as success.

### Spec 2 — Closed action contracts

The closed `Action` enum remains the authoring vocabulary. No runtime string registry is added.

Each variant has one exhaustive `ActionContract` entry co-located with the enum/schema code. It defines:

- canonical YAML type name;
- required, optional, and forbidden fields;
- field types and cross-field constraints;
- template-capable fields and their expected runtime shapes;
- normalized rendered form;
- output shape or explicit unit/output-envelope rule;
- whether the action is pure, durable-control, parked, read-only external, or effectful external;
- required host sink and effect policy;
- allowed execution limits and retry/unknown-outcome rules.

The same contract is consumed at three gates:

1. **Parse/normalize:** reject malformed YAML and unknown fields.
2. **Lint:** reject authoring errors, missing required policy, invalid graph/composite use, and unsupported host modes.
3. **Invoke:** validate the fully rendered action and its output, because a valid template can render an invalid runtime value.

Do not add a JSON-schema dependency merely to imitate Jido. A serde-native contract table and narrow validators are sufficient. If a standard JSON-shaped schema is emitted for tooling, it is a projection of the closed Rust contract, not a second source of truth.

Action success MUST be explicit. `null` is permitted only for an action whose contract declares unit output; it is not a universal success fallback. An action callback that returns an unsupported shape, throws/panics, or returns an unclassified failure crosses the boundary as a structured failure.

The current advertised action lists MUST be generated or exhaustively checked against `Action::type_name()`. Adding an action requires the enum variant, contract entry, normalization/rendering arm, engine arm, lint coverage, host/sink arm, and tests. A compiler error is preferred to a runtime lookup miss.

#### 2.1 Existing action migration

- `send_message`, `send_dm`, and `dispatch_job` remain only when their host effects return honest outcomes and carry `effect_key`/transport correlation.
- `request_approval`, `delay`, and `spawn await: true` remain engine-owned parked controls, but their pending records are structured and host-drained under Spec 5.
- `verify` is an embedding-owned verification effect. The engine never runs a shell command.
- `react` MUST be wired through a named reaction sink with tests or be deprecated and removed after an inventory/deprecation window. It MUST NOT fall through to success.
- `call_webhook` MUST be wired through a named webhook sink with an explicit security contract or be deprecated and removed. If retained, the sink MUST enforce scheme/host policy, block loopback/private/link-local/CGNAT and IPv4-mapped private addresses, control redirects and DNS rebinding, bound response/body size and duration, and redact credentials from evidence. A docstring is not an SSRF guard.
- Control-flow actions MUST NOT be passed to a mailbox effect runner. An accidental control-action invocation returns a typed execution error, never `Ok(Null)`.

### Spec 3 — Canonical graph, identity, and composite semantics

Before a run executes, `Workflow::compile`/`to_graph` MUST produce one canonical graph representation. `steps:` is sugar for a named chain; `graph:` is the general form. Both paths use `run_graph`; the old linear scheduler is not a second semantic engine.

Compilation/normalization MUST check:

- duplicate node names;
- unknown `needs` references;
- cycles and empty-ready-set stalls;
- invalid terminal/output shape;
- unsupported action contracts and nested control-flow combinations;
- bounded parallelism, iteration, continuation, and delay policy;
- stable source locations for errors;
- canonical serialization for `workflow_digest`.

Use a deterministic identity helper inspired by `Jido.Flow.Identity`:

```text
workflow_digest = H(schema_version, canonical_workflow_definition)
node_path       = authored_name + composite ancestry
iteration_key   = H(parent_collection_digest, authored index) or stable iteration identity
effect_key      = H(workflow_digest, run_id, node_path, iteration_key, action_digest)
```

The trigger/source payload is immutable for a run. Effective input may add declared defaults or bounded context, but cannot mutate the source or select a second route. Collection inputs used for iteration MUST be durable/replay-stable; an index alone is not enough if the collection can change during replay.

`attempt_id` is separate and changes per dispatch attempt. `effect_key` does not change merely because an action is retried. Compute `action_digest` from the full canonical rendered action in memory, including values needed to distinguish logical effects; persist only the digest and a redacted projection. Never persist secret values solely to make the digest. If a provider cannot accept the secret-safe stable key, its capability is recorded as weaker and unknown outcomes remain reconcilable rather than silently deduped.

For the first `prepared` transition, persist the normalized action identity/digest and use that identity for all retries and recovery. A replay that would render a different action digest fails with a version/replay error or enters explicit reconciliation; it must not mint a new logical effect silently.

Composite rules:

- `foreach` requires a proper list; non-list and improper-list inputs fail with an action error, not `[]`.
- `parallel` requires a valid positive count or declared crew role; invalid values fail, not `1`. Concurrency is bounded by an explicit policy.
- Collection results preserve authored/item order unless the action contract explicitly declares unordered output.
- Nested steps in `branch`, `retry`, `foreach`, `parallel`, and `call_workflow` enter the same `run_step`/normalization/guard/error path as top-level steps.
- `call_workflow` binds declared defaults and provided inputs through the same function used by `invoke`; provided values win over defaults.
- Iteration and reduce operations are bounded. A future `reduce` action follows Jido's useful separation of collection, ordered fold, initial value, reducer contract, and output validation. It is not implemented by an unbounded loop or an open action registry.
- Stale work/resolution tokens are rejected or ignored by generation/run identity before action work begins. A stale resolution MUST NOT resume a different node or later attempt.

### Spec 4 — Guards and templates

There is one `evaluate_step_guard` path for graph nodes, linear-desugared steps, branch guards, and nested steps.

- A false guard produces an explicit skipped outcome with node identity and reason.
- A syntax/type/unresolved-reference error is an action/contract failure. It MUST NOT become false, take an `else` branch, or render a placeholder.
- Template namespaces are explicit: `trigger`, `inputs`, `steps`, and the documented item/iteration context. The resolver binds declared defaults once and does not invent aliases such as `trigger_until` to compensate for missing `inputs` support.
- Missing refs, wrong shapes, and unrenderable values name the workflow, node, field, and reference path.
- Guards and templates are bounded by the existing evaluator policy. No unbounded expression evaluation or prompt-injected stdout is allowed.

### Spec 5 — Workflow effect journal and crash/reconciliation contract

#### 5.1 Storage boundary

Add workflow-specific tables to the existing SQLite persistence boundary. Do not create a new database or reuse the mailbox receipt table for a different meaning.

The logical `workflow_effects` record contains at least:

```text
 effect_key             primary key; stable logical effect identity
 run_id                 durable workflow run
 project                optional project scope
 workflow_digest        canonical workflow identity
 node_path              authored/composite location
 iteration_key          stable item/iteration identity, nullable
 action_type            closed action name
 action_digest          canonical rendered-action digest
 redacted_action        optional safe diagnostic projection
 state                  prepared | in_flight | accepted | settled |
                        failed | unknown | reconcile_pending | cancelled
 attempt                latest attempt number
 attempt_id             latest dispatch-attempt identity
 claim_token            fencing token for the current in-flight owner
 lease_expires_at       bounded recovery/fencing deadline
 transport_corr         mailbox/job correlation, nullable
 external_id            provider/job/message identifier, nullable
 outcome_json           bounded safe result projection, nullable
 failure_json           bounded safe failure projection, nullable
 reconcile_after        optional retry/reconciliation time
 retention_until        bounded evidence-retention deadline
 created_at / updated_at
```

The journal MAY use an append-only transition/attempt table in addition to the current-state row. It MUST retain enough bounded evidence to explain every transition without storing secrets, unbounded payloads, process handles, or stack traces. Redacted action projections and hashes are preferred to raw rendered actions. An in-flight claim MUST have a bounded lease/recovery deadline; an expired claim is reconciled as uncertain, never treated as proof of non-dispatch. A stale owner cannot settle an attempt after its fencing token is replaced.

A separate `workflow_runs` record is required for host-visible lifecycle state:

```text
 run_id, project, workflow, workflow_digest,
 status: started | running | suspended | reconcile_pending | succeeded |
        failed | blocked | cancelled | unknown,
 started_at, suspended_at, finished_at,
 current_node, failed_node, terminal_digest,
 artifact_paths, failure_projection,
 terminal_publication_state
```

A durable `workflow_pending` record (or an equivalent table with the same uniqueness constraints) is required for parked controls:

```text
 run_id, promise_id, park_generation, project, workflow, node_path,
 effect_key, kind, pending_payload, deadline,
 publication_state, created_at, updated_at
 unique(run_id, promise_id, park_generation)
```

`pending_payload` is schema-validated, bounded, redacted, and path-oriented. It contains no secret, stack trace, process handle, or unbounded action result.

`reconcile_pending` is nonterminal; `succeeded`, `failed`, `blocked`, `cancelled`, and `unknown` are terminal run claims. `terminal_digest` is a canonical hash of the run ID, workflow digest, terminal status, failed/current node, and bounded evidence references. It is the idempotency key for terminal publication, not proof that a bus subscriber received the event.

Resonate remains the execution/durable-function substrate. These records are the host's evidence and publication boundary; they do not duplicate Resonate's internal scheduler.

#### 5.2 Effect state machine

The host MUST use the following meaning:

```text
prepared
  → in_flight       (dispatch lease/attempt committed before the call)
  → accepted         (provider accepted the request, if that distinction exists)
  → settled          (the action returned a contract-valid result)

in_flight → unknown                  (process loss, timeout, ambiguous provider result)
accepted  → unknown                  (acceptance known, completion/evidence lost)
unknown   → reconcile_pending       (a reconciler or operator owns the next check)
reconcile_pending → settled|failed|unknown
prepared → failed                    (durably proven no dispatch / validation failure)
any nonterminal → cancelled          (only when the action/provider policy permits)
```

`settled` means that the action/provider boundary recorded a contract-valid outcome. It does not mean that a human read a message, a downstream job completed, or a business process reached its final state. `unknown` is a first-class state. A timeout is not proof of absence. A process crash after the `in_flight` commit is not proof that the provider was not called.

#### 5.3 Dispatch algorithm

For an effectful action:

1. Render and validate the action.
2. Derive `workflow_digest`, `node_path`, `iteration_key`, `action_digest`, and stable `effect_key`.
3. In one SQLite transaction, insert or load the effect row. A duplicate terminal `effect_key` returns its existing outcome. A duplicate nonterminal key attaches to or resumes the existing attempt/reconciliation record; it MUST NOT start a second logical effect.
4. Commit `prepared`, then claim/commit `in_flight` with a fresh `attempt_id` before invoking the provider.
5. Dispatch with `effect_key` as the provider idempotency key whenever the provider supports one. For mailbox actions, the host MAY set transport `corr = effect_key` in `extra_tags`; it MUST also retain the workflow-effect row.
6. On a provider-confirmed result, write `accepted`/`settled` and the bounded outcome. Link any mailbox/job receipt or external ID.
7. On an ambiguous result, write `unknown` and expose reconciliation. Do not silently return success or blindly create a second logical key.
8. On resume, a `settled`/`failed` row is authoritative for that logical action; `in_flight`/`unknown` goes through the action's reconciliation/idempotency policy.

This gives **intent-before-effect** and safe same-key retry where the provider supports it. It does not give universal exactly-once dispatch. The implementation MUST include the crash matrix in §7.4.

#### 5.4 Relationship to mailbox receipts

- `corr` is sender-owned transport correlation. For a workflow-originated mailbox effect, `corr` MAY equal `effect_key` as a deliberate projection.
- The mailbox `ReceiptStore` continues to record message lifecycle and may transition `owed → acked → resulted` or `degraded`.
- A mailbox `owed` row does not prove that the workflow effect was dispatched; a workflow `settled` row does not prove that a human read or business process completed the message.
- `thread_id` remains a work/conversation identity and is never substituted for `corr` or `effect_key`.
- Any reconciliation query MUST join through the explicit `effect_key`/`corr` link rather than infer identity from subject, body, or thread text.

### Spec 6 — Run lifecycle, sinks, and existing Nostr kinds

The existing `WorkflowHost` becomes the host-owned lifecycle manager. This is an adapter/event loop, not a Jido AgentServer or a second scheduler.

#### 6.1 Structured sink entries

Replace bare sink IDs with structured pending entries containing at least:

```text
run_id, workflow, node_path, effect_key, promise_id,
park_generation, kind: approval | spawn,
pending_payload, created_at, deadline
```

A `VerifySink` request is not a parked promise in the current design: it receives the same action/run/effect context and returns bounded verification evidence synchronously. If an embedding later makes verification asynchronous, it MUST use the same structured pending-entry contract and explicit generation. The entry is emitted durably or recoverably before the host relies on it. In particular, the implementation must close the crash gap between promise creation, sink observation, pending-row persistence, and pending-event publication: either the durable function's replay reconstructs the same entry, or a persistent pending row is committed before the function is treated as parked. Duplicate sink observations are idempotent on `run_id + promise_id + park_generation` (or `run_id + effect_key + attempt_id` for verification requests). A pending row is recoverable independently of the in-memory sink.

#### 6.2 Host loop responsibilities

The host lifecycle manager MUST:

1. Start a run and write `started/running` with the correct project before dispatch; if that write is not confirmed, do not dispatch.
2. Treat a Resonate suspended result as `suspended`, not failed or done.
3. Drain approval and spawn sinks promptly and on restart.
4. Publish existing kind-46011 approval-pending data with run/node/effect/evidence fields, or publish the existing kind-43001 job request for spawn-await. No new kind is introduced.
5. Subscribe to and validate approval/spawn resolution events. Resolve/reject the exact promise only when run, project, promise, generation, and action identity match. Duplicate resolutions are harmless; stale resolutions are recorded/ignored.
6. Convert normal completion, typed failure, cancellation, timeout, and unreconciled unknown outcome into one run-ledger terminal state using a compare-and-swap from the current nonterminal status.
7. Commit the terminal row and `terminal_digest` before publishing kind-46012. If publication is interrupted, retry the same event/digest; a duplicate terminal with the same digest is a no-op, while a different digest is a conflict requiring evidence.
8. Continue draining pending work independently of one parked or failed run. A single `engine.run` await MUST NOT be the only lifecycle driver.
9. On shutdown/restart, recover pending rows and terminal-publication rows before accepting new work.

The CLI must either use this host lifecycle or reject workflows requiring an unavailable sink/resolver before starting. It MUST NOT start an approval/spawn-await workflow with a no-op runner that can park forever without an operator-visible pending event.

#### 6.3 Pending and terminal payloads

Approval pending payloads carry bounded evidence paths rather than blobs:

```json
{
  "run_id": "wf-…",
  "workflow": "ship-toron",
  "node": "gate",
  "promise_id": "…",
  "park_generation": 1,
  "question": "Release toron?",
  "artifacts": [{"node":"verify-2","path":".flywheel/receipts/…"}],
  "ledger_path": ".flywheel/receipts/…-progress-ledger.json",
  "evidence_hash": "…"
}
```

A changed evidence hash is a new approval question; an old approval cannot be applied to a changed artifact set. Rejection is a decision with terminal/status evidence, not an engine panic.

### Spec 7 — Failure normalization and one retry coordinator

Jido v3's useful error lesson is staged normalization, not Splode. The toron action boundary MUST preserve:

- stage: `normalize`, `input`, `execute`, `output`, `effect`, or `park`;
- stable contract `code` for program decisions;
- human message separate from code;
- optional frozen runtime `FailureKind`;
- bounded evidence and identifiers (`run_id`, node, effect key, attempt, external ID);
- `effect_state` and `disposition`.

Raw `anyhow` strings, stack traces, secret-bearing inputs, process handles, and arbitrary provider terms MUST NOT be the durable/public error projection.

The engine/host has one retry coordinator:

- `ActionRunner`/sink callbacks do not retry.
- `Action::Retry` contributes an explicit policy to the coordinator; normalization MUST reject or flatten nested retry wrappers so one logical node cannot multiply attempts accidentally.
- `rate_limit` and `timeout` are retry candidates only when the effect state is known safe or the provider honors the same stable `effect_key`. If outcome is unknown and no reconciliation/idempotency contract exists, the result is `unknown/reconcile_pending`, not a blind retry. This qualifies ADR-0024/0026's generic retry candidate rule; it does not add a second retry policy.
- `auth`, `billing`, `provider`, `blocked`, and `cancelled` are not blindly retried. ADR-0026 fallback/reroute rules remain authoritative.
- Work-quality failure (test red, review rejection, failed VerifySink without runtime kind) is not a `FailureKind`. It is an explicit graph result and may enter the authored, bounded repair subgraph.
- A `Continue` policy preserves `ok:false` and failure metadata on the node; continuing graph admission is not the same as reporting a green run.
- Failure after a completed external effect does not claim rollback. The run may be failed while the effect remains settled.

### Spec 8 — Bounded, visible repair and compensation

#### 8.1 Repair is a graph pattern

Repair remains an authored/unrolled graph pattern, not a hidden `StepOnError::Repair` mode:

```text
repair-1 (fresh context)
  → evaluate-1 (fresh context, structured result)
  → verify-1 (VerifySink, embedding-owned)
  → repair-2 ...
```

Each cycle has a distinct node identity and ledger entry. The ledger stores paths and bounded structured fields (`done`, `remaining_work`, `failures`, `validation_evidence`); the next prompt receives the ledger path, not injected stdout. A failed runtime dispatch retries the same logical node according to Spec 7; it does not create a repair cycle.

A repair cycle cannot produce a legal `command-verified` or `reviewer-signed` bead gate. The armed loop remains the only writer of `command-verified`; the engine never calls `br gate report` or `br close`.

#### 8.2 Compensation is cleanup, not rollback

On terminal failure, the embedding MAY execute the fixed lease cleanup already supported by the stack:

1. release reservations by the owning bead reason using the operator-signed primitive;
2. send bounded captain evidence through the existing mail path;
3. add a bead comment when the owning bead is known;
4. record each cleanup attempt with its own effect key/outcome.

The original failure, effect uncertainty, and compensation failure remain separately visible. No user-authored general `on_error` undo DSL is added. No cleanup path is allowed to claim that an already completed external action was undone.

### Spec 9 — Flywheel catalog router and run-ledger truth

This is flywheel-owned metadata and routing; it is not added to `toron-workflow::Workflow` and does not reverse the dependency direction.

- The workflow catalog is the one routing table for `decide_trigger`, matcher, primitive, argv, and topology metadata.
- Matchers are declarative and lintable. Duplicate or overlapping claims that can produce two routes fail catalog load; first-match-wins is deleted.
- `decide` and `briefing` consume the same compiled table. A private primitive→topology map is deleted.
- No match returns an explicit `primitive: "none"` with a reason. It never silently becomes `steer` or `swarm`.
- `wave_shaped` substring sniffing is deleted. A route is selected only by a declared matcher.
- Fix stale stage references such as `code-review → ce-review`; resolve project-tier stage prompts through the correct catalog/tier; do not silently render an unknown `--param`.
- A headless leaf failure returns a failed typed outcome to the workflow runner. `workflow history` records `failed`, `blocked`, `suspended`, or `succeeded` truthfully, with the correct project and artifact paths. `done` is not a synonym for success.
- Run ledger status is updated at the host terminal boundary, not inferred from a successful process exit after a failed child step.

No Jido v3 Catalog is assumed here. The static flywheel catalog is our own deterministic routing contract, informed by the old ambiguity lesson but not a port of a branch API.

---

## 6. Implementation blueprint and sequencing

The former two-wave plan is reordered because effect identity and host lifecycle are prerequisites for any honest receipt claim.

### Gate 0 — Contract characterization and schema migration design

Before behavior changes:

- inventory every `Action` arm and every production runner/host path;
- write the action/failure/effect state tables and crash matrix as tests/spec fixtures;
- define the SQLite migrations for `workflow_runs`, `workflow_effects`, and pending/terminal evidence;
- define canonical workflow/node/iteration identity and redaction limits;
- decide the explicit retain/remove outcome for `react` and `call_webhook` after inventory;
- re-cut beads so no bead still says “reuse mailbox receipts for exactly-once.”

### Wave 1 — Honesty and lifecycle

**Toron:**

- replace production catch-all success arms with exhaustive engine/host/sink behavior;
- introduce normalized `ActionInvocation`/`ActionOutcome`/`ActionFailure` boundary and compatibility adapter;
- make control-flow and effectful action partitions explicit;
- drain structured approval/spawn/verify sinks through `WorkflowHost`;
- publish pending kind-46011/43001 and terminal kind-46012 with stable correlation/evidence;
- make CLI run refuse unavailable parked-action hosts;
- fix `Step.guard` parity, graph stall errors, input-default parity, loud composite coercion, and `no_claim_timeout` default/explicit-zero semantics;
- normalize failure predicate to `ok == false` and remove duplicate retry loops.

**Flywheel:**

- make run ledger status/project/artifact writes truthful;
- make headless leaf failures fail the parent run;
- route `decide` and `briefing` through one catalog compiler with explicit ambiguity/no-match behavior;
- fix stage-tier resolution and stale stage names.

**Wave 1 exit condition:** no parked run can be invisible, no live action can silently succeed by falling through, and a failed run cannot be recorded as successful merely because the outer process returned.

### Wave 2 — Contracts and canonical composites

- implement the closed per-variant action contract table and three validation gates;
- compile/normalize all workflows to one graph with canonical digest/source locations;
- apply one resolver to all guards/templates/nested steps;
- add bounded and explicit collection/parallel/reduce/iterate semantics, preserving output order and failure visibility;
- add output validation and structured error projection;
- adopt fresh-context repair graph conventions and evidence-by-path approvals;
- ensure advertised action lists are checked against the enum.

**Wave 2 exit condition:** a workflow author cannot use schema/graph/template/composite ambiguity to obtain a silent fallback, and every action result has a named stage and contract shape.

### Wave 3 — Effect journal, reconciliation, and fixed cleanup

- add workflow-effect, run-ledger, and pending-row migrations in the existing SQLite persistence boundary;
- wrap mailbox/job/webhook/verify effects with intent-before-dispatch, stable keys, claim fencing, and bounded retention;
- link mailbox `corr` and mailbox receipt lifecycle without conflating stores;
- implement the crash/reconciliation matrix and provider-specific idempotency/reconcile adapters;
- add terminal publication dedupe and restart recovery;
- execute lease cleanup as separately journaled, idempotent host effects;
- document residual at-least-once/unknown-outcome behavior for operators.

**Wave 3 exit condition:** the system can explain every effect as settled, proven failed, or unknown/reconciliation-pending after a crash, without claiming universal exactly-once behavior.

### 6.1 Target zones

| Zone | Intended responsibility |
| :--- | :--- |
| `crates/toron-workflow/src/schema.rs` | closed action contracts, canonical graph normalization, defaults, stable identity inputs |
| `crates/toron-workflow/src/engine/control_flow.rs` | one graph scheduler, guard parity, bounded composites, retry coordinator, park metadata |
| `crates/toron-workflow/src/engine/template.rs` | explicit ref resolution and loud missing/shape errors |
| `crates/toron-workflow/src/engine/mod.rs` | normalized invocation/result types, DI registration, run status handoff |
| `crates/toron-workflow/src/engine/sinks.rs` | structured pending entries and sink request/result types |
| `crates/toron-mcp/src/workflow_host.rs` | host effects, effect journal wrapper, sink drain, resolution subscriber, terminal publication |
| `crates/toron-core/src/index/schema.rs` / mailbox persistence boundary | migrations and storage primitives; mailbox receipt meaning unchanged |
| `crates/toron-cli/src/workflow.rs` | host/sink preflight and truthful run status surface |
| flywheel `src/decide.rs`, `src/briefing.rs`, `src/workflow_catalog.rs` | one deterministic router and no-match/ambiguity semantics |
| flywheel run/history/artifact paths | lifecycle status, project attribution, failed artifacts, terminal evidence |

**Forbidden zones:** kind registry, MCP tool names/count, `FailureKind` spelling, `workspace.rs` Tick Law surfaces, gift-wrap/archive semantics, `br` legal-close code, and `unsafe`.

### 6.2 Patterns to follow

- Jido v3's separation of evaluation, commit, settlement, and business completion.
- `jido_action` v3's input/output validation, explicit execution options, phase-bearing failures, canonical graph validation, static executable descriptors, stable identity, bounded composites, and revision/stale-token discipline.
- Existing `send_tags.rs` hard errors, receipt monotonicity, distinct sink newtypes, Resonate durable promises, VerifySink DI, and operator-signed reservation cleanup.

### 6.3 Patterns to avoid

- `Ok(Null)` catch-alls and error strings as the only contract.
- Reusing mailbox `owed/acked/resulted` as a workflow intent journal.
- Blind rerun after timeout/process death.
- A second retry policy inside the runner.
- Hidden repair loops in `on_error`.
- Prompt-stuffed stdout, secret-bearing durable projections, or unbounded ledgers.
- First-match route ladders, silent steer fallbacks, stale topology maps, and environment-dependent action validity.
- Engine shelling out, calling `br`, or writing legal-close evidence.

---

## 7. Verification and acceptance

### 7.1 Honesty and contract tests

- Every current `Action` variant appears in the exhaustive contract/dispatch test; no production catch-all can return success.
- Unsupported `react`/`call_webhook` behavior is either fully exercised through its sink or rejected/deprecated with an explicit error and migration warning.
- A control action accidentally passed to the effect runner fails with a typed error.
- Parse, lint, and invoke reject malformed fields, unknown fields, unresolved refs, wrong rendered shapes, invalid output, and missing sinks.
- `ok: true` with an informational `error` field does not fail the run; `ok: false` does.
- Error projections carry stage/code/effect state and bounded identifiers but no stack trace, secret, process handle, or unbounded payload.

### 7.2 Graph, guard, and composite tests

- `steps:` compiles to a chain graph and named step outputs remain compatible.
- Direct invoke of a cyclic/unknown-dependency graph fails with named nodes; it never returns partial success.
- A false guard skips explicitly; guard syntax/type/ref errors fail or follow explicit error policy, never else/placeholder.
- Top-level, graph, branch, retry, foreach, and call-workflow guards use the same resolver.
- `call_workflow` defaults/provided inputs have parity with direct invoke.
- Non-list foreach, invalid parallel count, improper collection, unknown reduce operation, missing reduce key/template, and over-limit iterate all fail loudly.
- Parallel fan-out is bounded and output order is deterministic.
- A retryable runtime failure retries the same logical node/effect key within one coordinator; it does not create a repair cycle. A work-quality failure enters only the authored repair graph.
- `Spawn` absent `no_claim_timeout` uses 600 seconds; explicit `0` is the only no-timeout spelling.

### 7.3 Host lifecycle tests

- Approval and spawn-await runs emit structured pending entries and existing pending events.
- A pending run survives host/process restart through Resonate plus recoverable pending records.
- Matching approval/spawn resolution resumes exactly the intended promise generation.
- Duplicate resolution is idempotent; stale/wrong-project/wrong-run resolution is rejected or ignored with evidence.
- A suspended run is not recorded as failed or done.
- Normal completion, failure, cancellation, timeout, and reconciliation-pending each produce the correct run-ledger status and one semantic terminal digest.
- Kind-46012 publication can be retried without creating duplicate semantic terminals.
- CLI execution with unavailable sinks fails before parking and names the required host/mode.

### 7.4 Effect journal crash matrix

The following cases are mandatory. The expected result is deliberately conservative:

| Crash/failure point | Required state on recovery | Allowed next action |
| :--- | :--- | :--- |
| Before `prepared` transaction commits | no effect intent; no dispatch | start fresh intent |
| After `prepared`, before `in_flight` commits | prepared | claim/commit in-flight, then dispatch |
| After `in_flight` commits, before provider call | in-flight/unknown boundary | reconcile; same-key retry only under declared policy |
| During provider call | unknown | reconcile or same-key idempotent retry; never new logical key |
| Provider accepted, before settled row | unknown/accepted if evidence exists | query/reconcile by effect key/external ID |
| Provider returned failure with proven no-send | failed | obey failure policy; no dispatch retry unless policy says so |
| Settled row committed, before run continuation | settled | resume from durable outcome; no duplicate logical effect |
| Mailbox receipt written but workflow journal not settled | workflow unknown, mailbox lifecycle linked | reconcile workflow row; do not infer from receipt alone |
| Terminal event publish interrupted | run terminal row committed, publication retryable | republish same semantic terminal digest |
| Compensation interrupted | original failure retained; cleanup pending/failed | retry idempotent cleanup and report separately |

- `settled` and `failed` effect rows do not trigger duplicate dispatch after replay;
- an expired/stale claim cannot overwrite a newer attempt;
- a changed replay render cannot silently mint a new effect key;
- terminal row CAS and repeated kind-46012 publication produce one semantic terminal;
- retention/compaction bounds prevent effect, pending, and run evidence from growing without limit.

### 7.5 Router and run-ledger tests

- Two catalog matchers with the same/overlapping claim fail load with both entries named.
- One match produces the same primitive/topology in `decide` and `briefing`.
- No match returns explicit `primitive: none` and never silently routes to steer/swarm.
- `code-review` cannot be emitted when the registered stage is `ce-review`.
- A failed headless leaf marks the parent run failed, writes failure artifacts, and preserves the correct project.
- Run history distinguishes suspended, blocked, unknown, failed, and succeeded.

### 7.6 Existing gates

- `cargo fmt --check` and workspace tests/clippy for touched toron crates using the repository's required `mbx +nightly-aarch64-apple-darwin` prefix.
- `catalog_freshness`, flag conformance, kind-registry checks, MCP tool-count checks, and ADR link checks remain green.
- No new Nostr kind, MCP tool, Tick Law path, or legal-close writer is introduced.

---

## 8. Documentation and skill sync

Docs describe shipped behavior only; each batch lands after the wave it documents.

| Surface | Required update |
| :--- | :--- |
| `toron/references/workflow-engine.md` | Action contract table, normalization stages, guard/ref rules, structured sinks, effect journal vs mailbox receipts, unknown/reconciliation semantics, `no_claim_timeout`, bounded composites, and host-mode requirements |
| `toron/docs/guides/workflow-engine.mdx` | Repository-facing authoring and operator guide kept aligned with the shipped wave; it must not document pending lifecycle behavior before its gate is green |
| `toron/SKILL.md` | One concise pointer to the workflow-engine contract; no command dump and no claim that approval/terminal publication exists before Wave 1 is green |
| flywheel skill/reference docs | Catalog router, explicit no-match, ambiguity failure, run-ledger status/artifact truth, and project attribution |
| flywheel `AGENTS.md` | Catalog/router and workflow lifecycle invariants if the flywheel implementation changes |
| `docs/learnings/jido-port-grilling-2026-09-11.md` | Mark the old `corr`/receipt and “attach sinks” claims corrected; retain it as historical evidence rather than rewriting the audit history |
| `toron/docs/decisions/README.md` | Keep ADR-0032 listed as Proposed until acceptance; update status only when the captain accepts it |
| `docs/catalog/toron-mcp.json` | Regenerate only if the CLI/MCP surface actually changes; this ADR plans no new tool |

Glossary for agents:

| Jido term | Local analogue | Boundary warning |
| :--- | :--- | :--- |
| Action | closed `Action` variant | local contract, not a Jido module |
| Instruction | normalized `ActionInvocation` | execution options stay outside authored action data |
| Exec | engine/host action boundary | no automatic retry or rollback |
| Flow | canonical workflow graph | Resonate remains the scheduler/durability substrate |
| Turn | one local action/effect cycle | not a whole-run transaction |
| Commit | workflow intent/evidence commit | not external business completion |
| Settlement | action/run/effect terminal evidence | distinct from provider delivery/read status |
| Catalog | flywheel routing catalog | not a dependency on Jido v3 Catalog APIs |
| Compensation | fixed host cleanup | not universal rollback |

---

## 9. Consequences and residual risks

### Good

- A workflow can explain what it intended, what it attempted, what the provider accepted, and what remains unknown after a crash.
- The most dangerous prior claim—“mailbox `corr` receipts provide per-step exactly-once”—is removed and replaced with a testable effect/reconciliation contract.
- Jido's strongest v3 lessons become local invariants: fixed executable/input, explicit phases, complete output validation, canonical graph identity, stable item identity, bounded execution, and separate commit/settlement meanings.
- Parked approval/spawn work becomes a host lifecycle responsibility rather than an unowned vector of promise IDs.
- Existing beads, mailbox receipts, Resonate durability, and legal-close protections remain in their current ownership lanes.

### Costs

- A workflow-specific journal and run ledger add SQLite schema and retention work. This is a real cost, but a second table in the existing persistence boundary is smaller and more truthful than pretending the mailbox table has missing columns.
- The context-bearing runner is a compatibility-sensitive boundary change. A temporary adapter reduces blast radius; it must not become a permanent context-free production path.
- Conservative unknown outcomes can block a run until a provider reconciliation or operator decision exists. That is visible friction replacing an unsafe silent duplicate.
- Canonical identities, structured error projections, bounded composites, and sink lifecycle code add explicit state. Each state is justified by a crash or ambiguity that the current implementation cannot witness.
- `react` and `call_webhook` may require a deprecation/removal path if their safe host implementations are not ready. A feature break is preferable to a successful no-op or false security claim.

### Residual risks

| Risk | Residual truth | Mitigation |
| :--- | :--- | :--- |
| Provider ignores idempotency key | Duplicate external work remains possible | classify capability, reconcile by external query where possible, surface unknown otherwise |
| Provider accepts but never reports business completion | `settled` means action/transport settlement only | document business completion separately; use provider events if available |
| Host dies between sink observation and publication | pending row remains recoverable | durable pending entry, restart drain, publication dedupe |
| Journal write is indeterminate | effect may or may not be recorded | fail closed, remove write authority for the activation where required, reconcile storage/effect |
| Action contract evolves | old run replay may see different schema | persist workflow digest/schema version and reject incompatible replay or provide migration |
| Large evidence or secrets leak | diagnostics can become sensitive/unbounded | redaction, bounded fields, artifact paths, hashes, no raw stack/callback payloads |
| Repairer edits while original agent holds a lease | two writers corrupt the tree | inherit reservation or interrupt original first; never concurrent ownership |

---

## 10. Explicit refuse list

1. Jido AgentServer/OTP/Pods/supervision runtime.
2. Jido Flow/Topology/Runic dependency or a second DAG scheduler.
3. Whole-run transactional Turn or “rollback all effects” claim.
4. Reusing mailbox `receipts` as a workflow-effect journal.
5. Universal exactly-once or blind retry after unknown external outcome.
6. Splode/Zoi/NimbleOptions as dependencies; use local structured Rust contracts.
7. Open action registry or environment-dependent action validity.
8. New runtime failure kinds for parse/lint/input errors.
9. Hidden `on_error: repair` sequencing; repair is a visible bounded graph.
10. General user-authored compensation/undo DSL.
11. Engine shelling out, calling `br`, writing `command-verified`, or closing beads.
12. LLM-driven `decide` routing, first-match ladders, silent steer fallback, or duplicate topology maps.
13. New MCP tools, Nostr kinds, plane nouns, or Tick Law exceptions.
14. Persisting raw secrets, unbounded stdout, stack traces, process handles, or callback closures in the workflow journal.

---

## 11. Revisit triggers

Revisit this ADR only when evidence changes one of these boundaries:

- Resonate or toron-core gains a verified workflow-effect/idempotency primitive that can replace the proposed journal without weakening the crash matrix.
- A provider contract supplies durable query/reconciliation and idempotency guarantees strong enough to narrow `unknown` behavior.
- A third-party action authoring requirement appears, together with signed/versioned contract and capability isolation requirements.
- Jido v3 and its design seams are accepted/released in a way that materially changes the evaluation/commit/recoverable-work contracts; re-evaluate the patterns, not the runtime port.
- The action enum becomes an external plugin surface; revisit the closed-enum decision in a separate ADR.

---

## 12. Source map

### Local sources

- `crates/toron-workflow/src/schema.rs`: closed action enum, workflow graph/schema, spawn timeout, action lists.
- `crates/toron-workflow/src/engine/mod.rs`: `ActionRunner`, Resonate engine, resolvers, run/invoke, promise resolution.
- `crates/toron-workflow/src/engine/control_flow.rs`: graph/step execution, guards, retries, composites, sinks, terminal helpers.
- `crates/toron-workflow/src/engine/sinks.rs`: approval/spawn/verify DI seams.
- `crates/toron-workflow/src/engine/template.rs`: template/ref rendering.
- `crates/toron-mcp/src/workflow_host.rs`: production runner and trigger host.
- `crates/toron-core/src/mailbox/receipts.rs`, `messaging.rs`, `send_tags.rs`: mailbox receipt/correlation semantics.
- `crates/toron-core/src/index/schema.rs`: existing SQLite migration boundary.
- `crates/toron-cli/src/workflow.rs`: CLI host/invoke path.
- flywheel `src/decide.rs`, `src/briefing.rs`, `src/workflow_catalog.rs`, run/history/artifact paths: routing and run-ledger seams.
- `docs/learnings/jido-port-grilling-2026-09-11.md`: historical defect inventory and live failure evidence.
- ADRs 0024, 0026, 0027, and 0028: accepted local constraints.

### Branch-specific Jido sources reviewed on 2026-09-12

- `agentjido/jido` `v3-spike`: `docs/design/04_turn-evaluation/design.md`, `06_commit-and-effects/design.md`, `07_persistence/design.md`, `12_errors-and-contracts/design.md`, `lib/jido/agent/turn.ex`, `lib/jido/agent_server/runtime_checkpoint.ex`, `lib/jido/persistence.ex`.
- `agentjido/jido_action` `release/v3`: `lib/jido_action.ex`, `lib/jido_executable.ex`, `lib/jido_instruction.ex`, `lib/jido_exec/action/runner.ex`, `lib/jido_exec/options.ex`, `lib/jido_flow/compiler.ex`, `lib/jido_flow/validation.ex`, `lib/jido_flow/identity.ex`, `lib/jido_flow/iterate.ex`, `lib/jido_flow/reduce.ex`, `lib/jido_exec/work.ex`, and `guides/execution.md`.

The branch study is evidence for boundaries and design patterns. It is not a dependency, API compatibility promise, or claim that pending Jido design documents are shipped contracts.

// governed-by: ADR-0032
