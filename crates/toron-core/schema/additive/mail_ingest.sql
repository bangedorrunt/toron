-- bd-next-envelope-ingest-yl0cx: exact dedupe for mail ingest into the
-- envelope log. One row per delivered DM per recipient, keyed by the rumor
-- id (stable across reads); msg_key records where it landed so a stale
-- claim (crash between claim and append) heals by re-appending instead of
-- going invisible. No older binary reads this table; its absence only means
-- mail never ingested (next reads the log, never this table).
CREATE TABLE IF NOT EXISTS mail_ingest (
    recipient TEXT NOT NULL,
    rumor_hex TEXT NOT NULL,
    msg_key   TEXT NOT NULL,
    PRIMARY KEY (recipient, rumor_hex)
);
