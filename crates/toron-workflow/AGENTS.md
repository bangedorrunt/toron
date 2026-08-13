# toron-workflow

Resonate-backed YAML engine. Schema + lint + durable run. Never imports flywheel.

## WHERE TO LOOK

| Concern | Path |
|---------|------|
| YAML models | `src/schema.rs` (`Workflow`, `CronSpec`, `Action`) |
| Engine + DI | `src/engine/mod.rs` (`ActionRunner`, `ProfileResolver`, `WorkflowResolver`) |
| `spawn` / graph | `src/engine/control_flow.rs` (`run_spawn`) |
| Lint | `src/lint.rs` |
| Cron manager | `src/cron.rs` (ADR-0012 — distinct type, not `trigger: schedule`) |
| Cron failure policy | `src/cron_failure.rs` |
| Job kinds 43001–43006 | `src/jobs.rs` |
| @mention coalesce | `src/coalesce.rs` |
| Trigger filter | `src/trigger_filter.rs` (skip workflow-emitted loops) |

## CONVENTIONS

- Engine never resolves profiles or named workflows itself. Embedder injects
  `ProfileResolver` / `WorkflowResolver`. Default of either is `None` →
  `{ok:false}` (fail-closed). That default is correct. Flywheel must inject
  a real catalog (`ProfileCatalog::resolve_for_engine`).
- `action: spawn` parks on a promise + `SpawnSink`. `dispatch_job` is the
  live runner path used by built-in flywheel workflows.
- `ActionRunner` returns `Result<Value>`. `on_error: fail_run` exists;
  default is `continue`.
- DAG `edges:` forbidden — derive from `needs`.
- Cron YAML and workflow YAML are two input types on one engine.
  Missed ticks never fire (`catchup: latest`).
- Job `t` tags forbidden — `#t` is project scoping (core).

## ANTI-PATTERNS

- Importing flywheel catalogs from this crate.
- Treating `default_resolver` as a bug — it is the DI fail-closed.
- Silent `call_workflow` success when `WorkflowResolver` is None.
- Workflow-emitted kind-14 re-entering the trigger path.
- Changing `triage.yaml` without keeping ADR-0004 suite green.
