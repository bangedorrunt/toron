CREATE TABLE IF NOT EXISTS event_subscriptions (
  id TEXT PRIMARY KEY,
  principal TEXT NOT NULL,
  delivery TEXT NOT NULL,
  name TEXT NOT NULL,
  args_hash TEXT NOT NULL,
  cursor INTEGER NOT NULL DEFAULT 0,
  created_us INTEGER NOT NULL,
  expires_us INTEGER,
  state TEXT NOT NULL DEFAULT 'active',
  UNIQUE (principal, delivery, name, args_hash)
);

CREATE TABLE IF NOT EXISTS event_deliveries (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  subscription_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_us INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS event_deliveries_sub_seq
  ON event_deliveries (subscription_id, seq);
