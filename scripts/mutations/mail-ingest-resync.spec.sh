# Sourced by scripts/verify-mutations.sh.
# bd-bn0ko: resync must hand each ingest scope a DISTINCT epoch. Reverting to
# a shared epoch makes two readers collide at (producer, epoch, seq) and the
# ingest loop bails with "8 tries lost the producer race".

CARGO_ARGS=(-p toron-core --lib)
TEST_FILTER=(mail_ingest_stale_epoch_resync_lands_the_pending_rumor)

mutation "B1  resync pins every scope to the same shared epoch" \
    file=crates/toron-core/src/inbox.rs \
    expect=mail_ingest_stale_epoch_resync_lands_the_pending_rumor \
    find='        self.ingest_seq_for(producer, recipient).await' \
    replace='        { let _ = &scope; Ok((1, 1)) }'

mutation "B2  retryable fence retries the same triple instead of resyncing" \
    file=crates/toron-core/src/mailbox/messaging.rs \
    expect=mail_ingest_stale_epoch_resync_lands_the_pending_rumor \
    find='''                    Err(InboxError::Append(AppendError::SeqGap { .. }))
                    | Err(InboxError::PayloadConflict { .. })
                    | Err(InboxError::Append(AppendError::StaleEpoch { .. })) => {
                        (epoch, seq) = store.resync_ingest_seq(&sender, &reader).await?;
                    }''' \
    replace='''                    Err(InboxError::Append(AppendError::SeqGap { .. }))
                    | Err(InboxError::PayloadConflict { .. })
                    | Err(InboxError::Append(AppendError::StaleEpoch { .. })) => continue,'''
