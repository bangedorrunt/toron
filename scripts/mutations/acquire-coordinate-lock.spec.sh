# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034

# The per-coordinate acquire lock (bd-0uzrq.7).
#
# MEASURED BEFORE: two `file_reservation_paths` on one exclusive path, started in
# the same tick, BOTH returned `granted` with epoch 0. Each acquire read a
# snapshot, decided, and published; the ledger resolved one winner afterwards,
# but the API had already told both callers they held the path. The decision is
# not linearizable without serialising it.
#
# Two pieces make the serialisation real, and each gets its own row:
#
#   1. The lock FILE is per (project, path). A path that is unique per call
#      locks nothing: both callers take their own lock, both decide on an empty
#      board, both publish. That is the pre-fix behaviour, restored.
#   2. The WAIT yields. Both acquires here are one task (`join!`), so a
#      synchronous sleep in the waiter starves the HOLDER out of its own poll:
#      the lock is never released and the waiter times out. Measured at 14s and
#      60s deadlines before the yield landed.
#
# The yield row costs a full 30s deadline before it goes red. It is kept because
# it is the half that a reader would otherwise delete as a style choice.

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(same_tick_double_acquire a_staggered_same_path_claim)

mutation "A1  the lock path is unique per call (serialisation gone)" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    find='let lock_path = dir.join(format!("reserve-{}.lock", reserve_lock_key(path)));' \
    replace='let lock_path = dir.join(format!("reserve-{}-{}.lock", reserve_lock_key(path), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)));' \
    expect=same_tick_double_acquire_grants_exactly_one

mutation "A2  the wait blocks the runtime instead of yielding" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    find='tokio::time::sleep(std::time::Duration::from_millis(2)).await;' \
    replace='std::thread::sleep(std::time::Duration::from_millis(2));' \
    expect=same_tick_double_acquire_grants_exactly_one
