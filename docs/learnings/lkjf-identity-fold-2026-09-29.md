# The two-keys-one-name split is folded, not guessed (bd-lkjf, 2026-09-29)

The bead asked a human to decide: are `BrashOtter` and `brash-otter` one
agent or two, and is the same true of `SwiftOtter` and `swift-otter`? The
operator answered by ruling the kebab-case pin invalid (it is the form
`toron agent bootstrap` refuses on registration), which settles the
question: the CamelCase identity is the real one and the kebab one is the
duplicate to remove.

## Removing the duplicate without destroying a holder

Deleting a key that still holds leases would make those leases
unaddressable, which is how a cleanup becomes an outage. So the leases
moved first, and only identities that held nothing were removed.

`toron reserve transfer` is release plus re-acquire, so a lease keeps its
path and reason and only changes hands:

    flywheel   brash-otter -> BrashOtter   4 leases (bd-s5ng, the 0014
                                           operator order, bd-8kn9, bd-3o5t)
    beads      swift-otter -> SwiftOtter   4 leases
    chiebukuro swift-otter -> SwiftOtter   3 leases
    toron.dev  swift-otter -> SwiftOtter  13 leases

Every one of those 24 leases includes an OPERATOR ORDER (ADR-0014,
"operator order: jev-latest + xdg pack truth"). Moving a lease is how that
instruction stays addressable; deleting the identity would have silently
dropped it.

Five stale holds for the closed bead `bd-lt77` were released outright, and
the standing "principle clear escape hatch" lease was left alone: it is
deliberate, not debris.

## What was removed, and the proof

`index.sqlite` was copied first, so the change is reversible. Then the
five registrations that held nothing were deleted. `toron doctor` went
from `5 project(s) hold one name across several keys` to
`no identity holds a second key for the same name`, and identity,
reservations and a full 798-test run all still work.

## The one that is deliberately still there

`beads|brash-otter` is NOT removed. It still holds the four escape-hatch
leases, and there is no `BrashOtter` in the `beads` project at all, so
that key is the only identity that agent has there. Deleting it would
leave a live holder unable to sign, which is the destruction this bead was
opened to prevent. It needs a rename or a fold into a real pin, decided
by whoever owns that identity, and it will keep showing up in doctor until
then. That is the correct state for a known-and-owned problem, and worse
than a silently deleted key.
