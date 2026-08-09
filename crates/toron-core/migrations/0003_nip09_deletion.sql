-- governed-by: ADR-0003
-- F19.NIP09 (bd-p3-nip09-deletion-projection-uzgy, batch-fix bd-batch-fix-pass-mip4):
-- NIP-09 kind-5 tombstone support on the messages projection.
-- Deleted mail must not stay readable on the index; search/inbox/thread
-- queries filter `deleted = 0`.

ALTER TABLE messages ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE messages ADD COLUMN deleted_ts_us INTEGER;
