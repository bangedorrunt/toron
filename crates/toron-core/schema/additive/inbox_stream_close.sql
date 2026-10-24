-- elm-effect 0.4.0 gives DurableLog an explicit, monotonic close: an append
-- after it is refused (AppendError::Closed), and bytes before it stay
-- readable. One row per closed stream; absence means open and appendable.
CREATE TABLE IF NOT EXISTS inbox_stream_close (
    stream       TEXT PRIMARY KEY,
    -- The msg key of the append that sealed the stream, so its own retry
    -- still answers with the original offset. NULL for a bare close.
    closed_by    TEXT,
    closed_ts_us INTEGER NOT NULL
);
