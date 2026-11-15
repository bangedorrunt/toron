# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# A replay that cannot complete must REFUSE the claim (bd-0uzrq.12).
#
# MEASURED BEFORE: two acquires on one exclusive path in one task both read an
# empty board and both were granted. The board read took 10.0s and returned
# Ok(0) — the wait was `current_mutations`'s pool acquire timeout, swallowed by
# `.unwrap_or(0)`, while the scan itself legitimately found no rows. A truncated
# or timed-out history is NOT an empty history, and the acquire treated the two
# as the same thing.
#
# Two layers, one row each:
#   R1 the BOARD read must be the strict one. Swapping it back to the permissive
#      `collect_replay` makes a `Closed`-before-EOSE subscription read as short
#      history, the board comes back empty, and the claim is granted.
#   R2 the DECISION must not swallow the failure. Even with a strict read, an
#      `unwrap_or_default()` on the acquire path turns the refusal into an empty
#      board and grants — which is the original defect wearing a different line.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(a_replay_that_cannot_read_refuses_the_acquire)

mutation "R1  the board read accepts a truncated history as short" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    find='let events = super::collect_replay_to_eose(self.bus.as_ref(), filter).await?;' \
    replace='let events = super::collect_replay(self.bus.as_ref(), filter).await?;' \
    expect=a_replay_that_cannot_read_refuses_the_acquire

mutation "R2  the acquire swallows the read failure into an empty board" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    find='let (mut model, meta) = self.try_kernel_model_for(project_slug).await?;' \
    replace='let (mut model, meta) = self.try_kernel_model_for(project_slug).await.unwrap_or_default();' \
    expect=a_replay_that_cannot_read_refuses_the_acquire
