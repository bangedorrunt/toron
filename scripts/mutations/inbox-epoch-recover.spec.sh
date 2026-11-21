# shellcheck shell=bash
# Sourced by scripts/verify-mutations.sh.
# shellcheck disable=SC2034
#
# bd-gj5vn: a reader must RECOVER from a duplicate-writer stale epoch, not
# degrade. Remove the re-sync arm and the reader returns Err (degraded).

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(a_stale_producer_epoch_recovers_and_the_pending_mail_is_returned)

MSG=crates/toron-core/src/mailbox/messaging.rs

mutation "E1  a stale producer epoch degrades instead of recovering" \
    file=$MSG \
    expect=a_stale_producer_epoch_recovers_and_the_pending_mail_is_returned \
    find='                    Err(InboxError::Append(AppendError::StaleEpoch { .. })) => {
                        let (fresh_epoch, fresh_seq) =
                            store.resync_ingest_seq(&sender, &reader).await?;
                        env.msg.epoch = fresh_epoch;
                        env.msg.seq = fresh_seq;
                        continue;
                    }' \
    replace='                    Err(InboxError::Append(AppendError::StaleEpoch { .. })) => {
                        return Err(InboxError::Append(AppendError::StaleEpoch {
                            producer: sender.clone(), sent: env.msg.epoch, current: env.msg.epoch + 1,
                        }).into());
                    }'
