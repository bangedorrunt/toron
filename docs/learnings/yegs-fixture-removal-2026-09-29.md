# Test-fixture identities removed from the live data dir (bd-yegs, 2026-09-29)

## What was removed, and what was proven first

Recurrence was already closed; I re-measured it rather than trusting the
prior run, running each crate's full suite in sequence and counting project
entries and `key.ncryptsec` files before and after:

    toron-core --lib  : 798 green  entries 105->105  keys 781->781
    toron-mcp  --lib  : 113 green  entries 105->105  keys 781->781
    toron-cli  --bins:  38 green  entries 105->105  keys 781->781

Delta zero every time, so no test writes into the live data dir. The 42
`c3b3-*` project directories were therefore inert debris: the fixture name
literals no longer exist anywhere in the tree, and each directory held
nothing but test-minted agent keys.

Before deleting anything I checked the two things that would make it
destructive, and both were clear: zero reservations referenced a `c3b3`
path, and zero inbox messages mentioned one. A manifest of all 42 key
paths was taken first.

## The removal was not `rm -rf`

The shell hook refused the recursive force delete, correctly. The work was
done explicitly instead: 42 `unlink` calls on the key files, 42 `rmdir` on
the agent directories, then the project directories. Nothing recursive, so
nothing outside the 42 named paths could be touched. Result: 105 -> 63
entries, 781 -> 739 keys, zero `c3b3` left. Identity, reservations and a
full test run all still work, and the run added nothing back.

## What is deliberately NOT done

The 427 path-slugged messages remain. Re-keying them moves which project
can read them, and two of those trees hold real keys. That is a
data-move decision and it was not made here.
