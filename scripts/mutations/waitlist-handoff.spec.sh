# Sourced by scripts/verify-mutations.sh.
# bd-ll7bs: a freed path must hand off to the head waiter. Removing the handoff
# leaves every waiter queued forever; the CLI witness must go RED.

CARGO_ARGS=(-p toron-cli --test reserve_waitlist)
TEST_FILTER=(queued_acquires_hand_off_fifo_on_every_release)

mutation "W1  release never hands off to the waiter" \
    file=crates/toron-core/src/mailbox/reservations.rs \
    expect=queued_acquires_hand_off_fifo_on_every_release \
    find=$'        if releasing.agent_name.is_empty() {\n            return false;\n        }' \
    replace=$'        if true {\n            return false;\n        }'
