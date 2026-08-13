# ADR-0012: Standalone Cron Type — a scheduled primitive distinct from workflow

**Status:** Accepted (2026-08-13; implemented + live; captain audit flipped Proposed → Accepted)
**Date:** 2026-08-11
**Decision-makers:** Captain (bangedorrunt) — 2-round grilling
**Extends:** [ADR-0005](0005-orchestration-layer.md) Spec 3 (which had cron = workflow-with-schedule).
**Amends:** [ADR-0005](0005-orchestration-layer.md) Spec 3 — cron is NO LONGER a workflow-with-schedule-trigger; it is a distinct type.
**Supersedes (partially):** the `CronEntry { template: <workflow-name> }` single-workflow-binding model (cron now composes workflows via a body).

> **Scope:** cron becomes a FIRST-CLASS scheduled primitive with its own multi-step body that
> COMPOSES workflows (`call_workflow`) + actions. It is a distinct YAML type, distinct CLI, distinct
> lint — NOT "a workflow with a schedule trigger" (which collapsed cron to running one workflow).
> It runs on the SHARED toron-workflow engine (one engine, two input types), Resonate-native.

---

## Context

### The captain's critique (grilling 2026-08-11)
"cron is treated as workflow and it's not right — it must be standalone, composing workflows as part
of cron." Plus: cron should be Resonate-native (not systemd), YAML-single-source, with a user dir.

### Reality found during the grilling
- **Cron is ALREADY Resonate-native.** `cron.rs:6/388`: the P4.3 cron manager is a "Resonate durable-
  sleep loop." systemd is the LEGACY being migrated FROM (`cron.rs:717`: "the old systemd timers stay
  until the workflows replace them"). So "use Resonate instead of systemd" is already the design;
  what remains is COMPLETING the migration (retiring systemd sagas), not a substrate switch.
- **The real defect:** `CronEntry { schedule, owner, policy, scope, template: <workflow-name> }` — a
  cron binds to ONE workflow template; on tick it runs that single workflow. It CANNOT compose
  multiple workflows or carry its own multi-step body. This is a simplification of ADR-0005 Spec 3
  (which described schedule + owner + policy + a step body). The captain's "cron = workflow, wrong"
  lands here: cron can only fire ONE workflow.

---

## Decision

### Spec 1 — Cron is a distinct TYPE (not a workflow)
A **cron** is a first-class scheduled primitive. Distinct from `Workflow`:
- distinct YAML schema (schedule + owner + scope + policy + steps body),
- distinct CLI (`flywheel cron lint|run|list|show|pause|resume|history|remove`), distinct lint,
- cron-specific policy that does NOT belong on a plain workflow (`owner`, `scope`, `on_failure`,
  `repeat_threshold`, `no_claim_timeout`, catch-up `latest`).

It is **NOT** `trigger: schedule` on a workflow. A workflow is invoke/reactive; a cron is scheduled +
owns its policy. Both can `call_workflow` (compose).

### Spec 2 — Cron runs on the SHARED toron-workflow engine (one engine, two types)
The cron's `steps:` body uses the SAME step DSL as workflows (`call_workflow`, `delay`, `branch`,
`call_webhook`, …). The engine parses the cron YAML → a Resonate durable function with the schedule
as a durable-sleep loop + the body as durable steps. **One engine, two input types** (workflow YAML +
cron YAML). "Standalone" = type/schema/CLI/lint distinction, NOT a separate runtime. Rationale: a
separate cron engine would double maintenance + two DSLs; the shared engine delivers the standalone
type cheaply (the body's `call_workflow` composes workflows on the same engine).

### Spec 3 — The cron YAML schema (single source; engine interprets)
```yaml
# ~/.config/flywheel/crons/weekly-upgrade.yaml  (or <project>/.flywheel/crons/)
name: weekly-upgrade
schedule: "30 3 * * 0"          # 5-field Vixie cron
owner: housekeeper              # the owning profile (the fixer's identity)
scope: host                     # host (operator identity) | project (scoped agent)
mode: headless                  # headless (default; no churn) | pane
policy:
  on_failure: bead              # log | bead | escalate
  on_repeat_failure: escalate   # after repeat_threshold consecutive failures
  repeat_threshold: 3
  on_success: log
  max_retries: 1
  no_claim_timeout: 600
  catchup: latest               # latest (run most-recent due tick on wake) | skip
steps:                          # the body — composes workflows + actions
  - { name: probe,  action: call_workflow, workflow: health-check }
  - { name: upgrade, action: call_workflow, workflow: docker-upgrade, if: "{{steps.probe.healthy}}" }
  - { name: verify, action: call_workflow, workflow: health-check }
```
The YAML IS the source (no Jinja/template-expansion layer); the engine reads/interprets/transforms it
into a Resonate durable function. Agents learn the syntax from the skill; `flywheel cron lint` validates
(schedule parse, step resolution, owner-profile resolution, policy fields).

### Spec 4 — User dirs (global + per-project, with precedence)
- **Built-ins:** compiled-in (read-only; override by same name in a user dir).
- **Global user:** `~/.config/flywheel/crons/<name>.yaml` (cross-project maintenance).
- **Per-project user:** `<project>/.flywheel/crons/<name>.yaml` (project-scoped automation).
- **Precedence:** per-project > global > built-in. `flywheel cron list` shows source + scope.

### Spec 5 — Resonate-native (confirmed, not changed)
The cron manager is the Resonate durable-sleep loop (ADR-0005 Spec 3, already implemented P4.3).
`ctx.sleep` until next tick; catch-up `latest` on wake. **No systemd dependency** for the cron
runtime. systemd sagas (docker-auto-upgrade, agent-reach-upgrade) are LEGACY — left running (they
work), migrated opportunistically to cron YAMLs; NOT retired by this ADR.

---

## Consequences

### Positive
- **Cron is genuinely standalone** — distinct type/CLI/lint + cron-specific policy; composes
  multi-workflow bodies (not bound to one workflow).
- **One engine** — no separate cron runtime to maintain; the body DSL is shared.
- **YAML single-source** — the cron YAML IS the source; engine interprets; lint validates.
- **User dirs** — clear global + per-project authoring with precedence.
- **Resonate-native confirmed** — no systemd dependency in the cron runtime.

### Negative
- **Migration of `CronEntry { template }` → cron YAML body** — existing crons (crons.toml) +
  schedule-triggered-workflows migrate to the new schema.
- **Two types on one engine** — the engine gains a cron-input path (parse cron YAML → durable fn);
  the workflow + cron types share the step DSL but diverge at schedule/policy.
- **systemd sagas NOT retired** (deferred) — two scheduling systems coexist until the sagas migrate.

### Neutral
- One ADR, amends ADR-0005 Spec 3.

---

## Verification

- **Type distinct:** `flywheel cron lint <file>` validates a cron YAML (schedule + owner + policy +
  steps); `flywheel workflow lint` rejects a cron-YAML (no `schedule`/`owner` on a workflow) — the
  two types are distinct at lint.
- **Body composes:** a cron with `steps: [call_workflow health-check, call_workflow docker-upgrade]`
  runs BOTH on a tick (multi-workflow body), not just one.
- **Engine shared:** the cron body's `call_workflow` runs on the same toron-workflow engine as an
  on-demand workflow invoke.
- **User dirs + precedence:** a per-project cron shadows a global same-name cron shadows a built-in;
  `cron list` shows the resolved source.
- **Resonate-native:** a cron's next tick is a `ctx.sleep` (durable); the cron survives a flywheel
  restart + catch-up (`latest`) on wake.

---

## Implementation Blueprint

| Phase | What |
|---|---|
| 1 | `CronSpec` schema (schedule + owner + scope + policy + steps) in toron-workflow; `parse_cron` + `cron lint`. Distinct from `Workflow`. |
| 2 | Engine: cron YAML → Resonate durable function (durable-sleep schedule + body steps). Shared engine, cron input path. |
| 3 | flywheel `cron` CLI rewrite: `lint/run/list/show/pause/resume/history/remove` over the new cron YAMLs (global + per-project dirs + precedence). |
| 4 | Migrate `Crons` (crons.toml) + schedule-triggered-workflows → cron YAMLs. |
| 5 | Update the cron-authoring skill reference to the standalone model. |

### Deferred
- **systemd saga migration** (docker-auto-upgrade etc.) — left running; opportunistically migrated.
- **Dynamic cron routing** (cost/load-aware schedule steering) — follow-on.

// governed-by: ADR-0005, ADR-0010
