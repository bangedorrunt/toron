# An agent pin is a namespace, not a label

## The rule

`IvoryRidge`, `EmberOtter`, `TealMarlin` — exactly two capitalised words, letters
only, no two capitals in a row, length ≥ 3. Enforced at the registration door
(`toron agent bootstrap`), never at resolution.

## Why it is a namespace rule and not a style rule

Identity is **per-project**. The same pane holds a *different key* in every
project it is registered in. Measured on this host, 2026-09-28:

```
toron mail whois flywheel  Buffy  ->  6778eb30f5bb7b08ac2a4704...
toron mail whois toron.dev Buffy  ->  71998e0bcfb5dac4a226bff0...
                                 both report  pane_id w1W:p1
```

Same pane. Two keys. So a name that is *not distinctive* is a name two unrelated
agents can pick independently — and when they do, it resolves to two different
keys, and mail sent to it from one project is encrypted to a key the recipient
never reads, while `mail send` reports `ok`. The message is not lost. It is
**invisible**, which is strictly worse because nothing reports a problem.

`Buffy` is the live counter-example: one ordinary word, two projects, two npubs.

## A resolved name is not a route

The shape generalises past naming. In a per-scope-key system, resolving a
recipient proves the *name* is known; it does not prove the resolved key is one
the recipient reads. The mailbox validates the name and never checks the route —
the same gap the doorbell closed with `has_route` (bd-bk9t).

Working recipe, proven live in both directions:

```shell
toron mail send --project <sender-project> --to <npub> --thread ... --subject ... --body ...
```

Npub addressing crosses projects. Name addressing does not.

## Enforce at create, never at resolve

Refusing to *read* a legacy name would orphan every agent already registered, and
the failure this rule prevents is a send-time problem. So: refuse at creation,
warn at send time, migrate nothing. The existing non-conforming pins stay
registered — renaming a live identity means re-registering its key in every
project it appears in, which is an operator decision, not a drive-by.

## Scope the rule to the vocabulary it governs

Project slugs are a *different* vocabulary and stay kebab-case: `demo`, `forge`,
`parity-product` are all legitimate. A rule that reads as obviously wrong gets
ignored, so the validator's refusal explicitly says it does not cover slugs, and a
fence asserts that.

## Building the validator: four bugs, three caught by its own fences

- The word counter only counted a capital that *follows* a lowercase, so a word's
  own initial capital was invisible and it scored `EmberOtter` as one word —
  refusing the pin I use myself.
- No adjacent-capital check, so `IVoryRidge` scored as exactly two words.
- Alphanumeric rather than alphabetic, so `Ivory2Ridge` passed: a digit makes the
  first "word" not a word.
- The first fixture hand-built the `Event` and invented a d-tag shape the wire
  never carries, so the detector it exercised was silently empty and "passed" by
  never firing.

The fixture now goes through the production builder and derives the npub from the
event, so the fence cannot certify a fiction. A validator's first fixtures should
be the values you actually use, and its fixture should call the builder production
calls.

## Ledger note

This work was committed citing `bd-pinrule`, which was never a real bead; the real
one is `bd-jvpq`. The commit guard checks that a `bd-*` token appears in the
message, not that the bead resolves, so the gap was invisible to it. A citation
that does not resolve is worse than none, because the ledger reads as
authoritative and is not.
