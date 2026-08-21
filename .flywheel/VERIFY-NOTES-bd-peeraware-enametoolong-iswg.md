# VERIFY notes — bd-peeraware-enametoolong-iswg (SandySparrow, 2026-08-21)

Durable record for the peer-aware spawn ENAMETOOLONG regression. The code fix
lives in the flywheel repo (commit 13a26f5, "fix(spawn): never pass launch text
as agent.start args"); this bead's tracker repo is toron, and the ADR-0019 SHA
rule derives the close sha from THIS repo — so this anchor commit carries the
bead id here. Full evidence: `.flywheel/receipts/` (gitignored, per-run).

## Root cause + fix

Peer-aware spawn put the launch text (peer template / first prompt) into
agent.start args; opencode reads a positional as a directory, oversized argv
died ENAMETOOLONG / "Failed to change directory". Fix: primes are typed via
pane.send_text after TUI readiness; agent_args carry flags only
(`needs_in_arg_prime` is the documented dead secondary path).

## Fence drift found while verifying (durable operational knowledge)

1. `herdr pane list --json` no longer parses: current herdr rejects `--json`
   ("unknown option"); pane list emits JSON by default and takes only
   `--workspace`. Bead fence corrected accordingly.
2. `flywheel spawn <dir> --oc 1` refuses to run while a stale workspace for
   the same dir lingers (orphan guard). Unattended VERIFY runs must pass
   `--retire-orphans` or pre-close stale workspaces, else the fence wedges.
   Corrected fence: `flywheel spawn /tmp/probe-enametoolong --oc 1 --no-focus
   --retire-orphans && herdr pane list | grep -v ENAMETOOLONG` → exit 0
   (probe opencode booted at /private/tmp/probe-enametoolong, zero
   ENAMETOOLONG anywhere).

## Board/projection mismatch observed (reservations class)

During close sequencing, the live reservation board reported an exclusive
CoralFinch lease on `.beads/issues.jsonl` (id 375a9189...), but the durable
projection (`~/.local/share/toron/index.sqlite`, table `file_reservations`,
post-5173276) contains NO row for that lease — the guard's read model does not
see it. Request-release to CoralFinch went unanswered. Flagged for whoever
works the reservations-projection class next: board state and projection can
disagree for leases that pre-date the projection fix.

## Build blocker note (cross-bead)

`cargo test -p flywheel` was blocked by toron-mcp failing to compile against
TanFern's in-flight bd-dispatch-spec-untyped-0iud WIP (workflow_host.rs:155,
expected Value found DispatchSpec). Verification above is behavioral against
the installed flywheel binary (built from post-fix HEAD 472e4ca).
