# P4.5 — Migrate docker-auto-upgrade + agent-reach-upgrade sagas → declarative workflows

> governed-by: ADR-0005 Spec 3 (smart cron), bd-p4-migrate-sagas-7rmo
> Status: **DESIGN** — the definitions below lint clean against the unified
> engine (toron-workflow) + flywheel tier. Execution (cron registration +
> runs) is gated on P4.3 (cron manager, durable-sleep) + P4.4 (bead-on-fail),
> per the bead's dependency graph.

## Goal

Convert the captain's two hand-built self-reporting cron sagas (systemd
timers + bash) into declarative workflows owned by the `housekeeper` profile.
This is the proof the framework replaces hand-built sagas: same health-gates,
same failure semantics, expressed as `workflows/*.yaml` consumed by the
unified engine.

## Sequencing rule (from the bead)

The new workflow path must pass the SAME health-gates as the saga BEFORE the
old systemd-timer path is removed. Old timers remain until the workflow is
verified, then are disabled (not deleted).

## The three workflows

### 1. `docker-upgrade` — weekly Docker image upgrade (housekeeper)

Replaces the systemd `docker-auto-upgrade` timer (Sun 03:30). Saga semantics:
snapshot → pull/rebuild → recreate → health-gate 90s → auto-rollback-on-fail.

```yaml
name: docker-upgrade
description: "Weekly health-gated Docker image upgrade with auto-rollback (saga replacement, ADR-0005 Spec 3)."
trigger:
  type: schedule
  cron: "30 3 * * 0"
  owner: housekeeper
  scope: host
policy:
  on_failure: bead
  on_repeat_failure: escalate
  repeat_threshold: 3
  on_success: log
  max_retries: 1
  no_claim_timeout: 600
steps:
  - name: snapshot
    action: spawn
    profile: "@housekeeper"
    workflow: housekeeper-snapshot
    await: true
    no_claim_timeout: 600
    on_error: fail_run
  - name: upgrade
    action: spawn
    profile: "@housekeeper"
    workflow: housekeeper-docker-upgrade
    await: true
    no_claim_timeout: 600
    on_error: fail_run
  - name: health-gate
    action: branch
    if: "{{steps.upgrade.healthy}} == true"
    then:
      - name: notify-ok
        action: send_dm
        to: housekeeper
        body: "docker-upgrade OK: {{steps.upgrade.containers}}"
    else:
      # A failed spawn-await FAILS the run (Spec 4a), which triggers
      # `on_failure: bead` — no synthetic abort action needed. If rollback
      # itself fails, the run fails and the failure bundle carries the
      # rollback error + last-known-good.
      - name: rollback
        action: spawn
        profile: "@housekeeper"
        workflow: housekeeper-rollback
        await: true
        no_claim_timeout: 600
      - name: notify-fail
        action: send_dm
        to: housekeeper
        body: "docker-upgrade FAILED — rolled back to snapshot"
```

### 2. `agent-reach-upgrade` — weekly agent-reach CLI upgrade + verify (housekeeper)

Replaces the systemd `agent-reach-upgrade` timer (Sun 03:45).

```yaml
name: agent-reach-upgrade
description: "Weekly agent-reach CLI upgrade + verify (saga replacement, ADR-0005 Spec 3)."
trigger:
  type: schedule
  cron: "45 3 * * 0"
  owner: housekeeper
  scope: host
policy:
  on_failure: bead
  on_repeat_failure: escalate
  repeat_threshold: 3
  on_success: log
  max_retries: 1
  no_claim_timeout: 600
steps:
  - name: upgrade
    action: spawn
    profile: "@housekeeper"
    workflow: housekeeper-reach-upgrade
    await: true
    no_claim_timeout: 600
    on_error: fail_run
  - name: verify
    action: branch
    if: "{{steps.upgrade.verified}} == true"
    then:
      - name: notify-ok
        action: send_dm
        to: housekeeper
        body: "agent-reach-upgrade OK"
    else:
      - name: notify-fail
        action: send_dm
        to: housekeeper
        body: "agent-reach-upgrade FAILED — verify step did not pass"
      - name: fail-closed
        action: call_webhook
        url: "http://127.0.0.1:9/none"
        on_error: fail_run
```

### 3. `housekeeper-diagnose` — the defined fixer (bd-p4-bead-on-fail)

On a failure bead, housekeeper reads the failure bundle (run-id, workflow,
owner, error, last-known-good, retry history) + proposes a patch (fix-bead /
PR / config edit). Registered as the owner's diagnosis workflow; the smart
cron loop invokes it after `on_failure: bead` creates the failure bead.

```yaml
name: housekeeper-diagnose
description: "Defined fixer (ADR-0005 Spec 3): reads a failure bead bundle and proposes a patch."
trigger:
  type: webhook
inputs:
  - name: bundle
steps:
  - name: triage
    action: spawn
    profile: "@housekeeper"
    workflow: housekeeper-diagnose-run
    await: true
    no_claim_timeout: 600
    on_error: fail_run
  - name: propose
    action: branch
    if: "{{steps.triage.patch}} != null"
    then:
      - name: fix-bead
        action: dispatch_job
        assignee: housekeeper
        spec: { profile: "@housekeeper", task: "apply patch: {{steps.triage.patch}}", run_id: "{{inputs.bundle.run_id}}" }
    else:
      - name: escalate
        action: send_dm
        to: housekeeper
        body: "housekeeper-diagnose could not propose a patch for {{inputs.bundle.workflow}} — human-in-the-loop"
```

## Lint / parse status

The three YAMLs parse against the unified schema (engine lint) and pass the
flywheel tier (profile `@housekeeper` resolves to a built-in profile; no
undeclared `{{inputs.*}}` / `{{steps.*}}` refs). Proven by
`tests/smart_cron_design.rs` (parse + lint clean, no cron-mgr needed).

## Not in this design (documented follow-ups)

- **Execution wiring**: registering these crons via `flywheel cron add` and
  the P4.3 durable-sleep loop driving ticks. Gated on P4.3/P4.4.
- **health-gate implementation**: the `housekeeper-docker-upgrade` /
  `housekeeper-health-check` / `housekeeper-rollback` sub-workflows (or the
  housekeeper's own scripts behind the spawn). The saga's 90s health-gate
  becomes a sub-workflow step (`delay: 90` + health probe) — same gate, now
  declarative and replayable.
- **Old systemd timers**: disabled (not deleted) once the workflow path
  passes the same health-gates.
