-- ADR-0037 W2: one pin, one key ACROSS projects, enforced by the database
-- for every new write (Mailbox::refuse_second_key is the first line and
-- gives the operator-facing message).
--
-- A new binding of a name may only carry a key that name already holds
-- somewhere (reuse), or be the name's first binding anywhere (mint).
-- Re-registering an existing row keeps its own key, so legacy splits
-- (ADR-0037 §5, ~149 pins on the live board at acceptance) keep upserting;
-- they just cannot grow a further key. Collapsing a split (§5.2) repoints
-- rows to a key the name already holds, which this allows.
--
-- Triggers, not an index: existing splits must not stop the database from
-- opening (the same reasoning as 0018).
CREATE TRIGGER IF NOT EXISTS agents_one_key_per_pin_insert
BEFORE INSERT ON agents
WHEN EXISTS (SELECT 1 FROM agents a WHERE a.name = NEW.name AND a.project_slug <> NEW.project_slug)
 AND NOT EXISTS (SELECT 1 FROM agents a WHERE a.name = NEW.name AND a.pubkey = NEW.pubkey)
BEGIN
    SELECT RAISE(ABORT, 'agents_one_key_per_pin: this pin already holds a key in another project; reuse it with proof of possession (ADR-0037 W2)');
END;

CREATE TRIGGER IF NOT EXISTS agents_one_key_per_pin_update
BEFORE UPDATE OF name, pubkey ON agents
WHEN EXISTS (SELECT 1 FROM agents a WHERE a.id <> NEW.id AND a.name = NEW.name AND a.project_slug <> NEW.project_slug)
 AND NOT EXISTS (SELECT 1 FROM agents a WHERE a.id <> NEW.id AND a.name = NEW.name AND a.pubkey = NEW.pubkey)
 AND NOT (OLD.name = NEW.name AND OLD.pubkey = NEW.pubkey)
BEGIN
    SELECT RAISE(ABORT, 'agents_one_key_per_pin: this pin already holds a key in another project; reuse it with proof of possession (ADR-0037 W2)');
END;
