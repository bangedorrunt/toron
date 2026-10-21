-- bd-uc8m: the workflow-effect projection's hot path reads ONE effect by key
-- (`get_effect`, reached from `claim_attempt` and `settle_with_fence`), and the
-- receipts' hot path reads ONE receipt by corr. Both keys live in the event
-- PAYLOAD, so a scoped read needs an index or it is still a full scan of a
-- table that is never pruned.
--
-- `events_kind` serves the kind-scoped folds (the receipts' fold and the
-- workflow-effect fold each read only their own event kinds instead of the
-- whole log). `events_wfx_key` mirrors the existing `events_corr` expression
-- index (migration 0014) for the effect key.
--
-- `json_extract` returns NULL for a non-JSON payload and `NULL = NULL` is
-- never true, so a malformed row is simply absent rather than an error.
CREATE INDEX IF NOT EXISTS events_kind ON events (kind);
CREATE INDEX IF NOT EXISTS events_wfx_key ON events (json_extract(payload, '$.key'));
