-- ADR-0037 W1 (bd-0tbg shape): one name -> one pubkey per project, enforced
-- by the database and not only by `Mailbox::name_collision_in_project`.
--
-- Two spellings of one pin (`BrashOtter`, `brash-otter`) fold to the same
-- words. `UNIQUE (project_slug, name)` (0001) cannot see that, so a project
-- could hold both rows with DIFFERENT keys, and reservations/mail addressed to
-- one spelling were invisible to the other.
--
-- The fold below is a sound under-approximation of `name_collision_key`
-- (ASCII alphanumerics, lowercased): it strips the separators pins and slugs
-- actually use (`-`, `_`, `.`, space) and ASCII-lowercases. Whenever these
-- SQL keys are equal, the Rust keys are equal too, so the trigger never
-- refuses something the Rust check would allow; the Rust check stays the
-- first line and gives the operator-facing message.
--
-- A trigger, not a unique index: live boards still carry legacy splits
-- (bd-0tbg measured one). A unique index would fail this migration on those
-- databases and refuse to open them. Triggers guard every NEW write and leave
-- existing rows for the operator (`doctor` near-duplicates, bd-lkjf).
--
-- A second spelling holding the SAME key is not a split and stays allowed.
CREATE TRIGGER IF NOT EXISTS agents_same_name_one_key_insert
BEFORE INSERT ON agents
WHEN EXISTS (
    SELECT 1 FROM agents a
    WHERE a.project_slug = NEW.project_slug
      AND a.name <> NEW.name
      AND a.pubkey <> NEW.pubkey
      AND lower(replace(replace(replace(replace(a.name, '-', ''), '_', ''), '.', ''), ' ', ''))
        = lower(replace(replace(replace(replace(NEW.name, '-', ''), '_', ''), '.', ''), ' ', ''))
)
BEGIN
    SELECT RAISE(ABORT, 'agents_same_name_one_key: another spelling of this name holds a different key in this project (ADR-0037 W1)');
END;

CREATE TRIGGER IF NOT EXISTS agents_same_name_one_key_update
BEFORE UPDATE OF project_slug, name, pubkey ON agents
WHEN EXISTS (
    SELECT 1 FROM agents a
    WHERE a.id <> NEW.id
      AND a.project_slug = NEW.project_slug
      AND a.pubkey <> NEW.pubkey
      AND lower(replace(replace(replace(replace(a.name, '-', ''), '_', ''), '.', ''), ' ', ''))
        = lower(replace(replace(replace(replace(NEW.name, '-', ''), '_', ''), '.', ''), ' ', ''))
)
BEGIN
    SELECT RAISE(ABORT, 'agents_same_name_one_key: another spelling of this name holds a different key in this project (ADR-0037 W1)');
END;
