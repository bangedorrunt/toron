-- governed-by: ADR-0003
-- S3 (bd-ts-unit-audit-pmn5): created_ts_us unit audit.
--
-- Contract: every `*_ts_us` column is int-microseconds (ADR G7). Old-era
-- rows (pre-2026-08 normalize) may carry SECONDS in the microseconds field
-- (e.g. 1786508238 instead of 1786508238000000), which breaks since_ts
-- filtering and recency ordering.
--
-- Plausibility rule: post-2001 dates in µs are >= 1e12 (2001-09-09 ≈
-- 1,000,000,000,000 µs). Any value in (0, 1e12) is seconds-scale and is
-- scaled to µs. Zero and negative values are untouched (no meaning to
-- scale).

UPDATE messages
   SET created_ts_us = created_ts_us * 1000000
 WHERE created_ts_us > 0 AND created_ts_us < 1000000000000;
