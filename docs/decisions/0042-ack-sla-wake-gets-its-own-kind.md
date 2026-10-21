<!-- governed-by: ADR-0003, ADR-0033 -->
---
status: accepted
date: 2026-10-22
decision-makers: bangedorrunt
consulted: flywheel stack audit 2026-10-21 §1 (measured live on the toron store)
informed: flywheel x toron x beads maintainers
---

# ADR-0042: The Ack-SLA Reminder Gets Its Own Kind, One Per Conversation, And A Budget

**Status:** Accepted — bead bd-4v1a, the toron-side half of the stack audit's
S1 finding.

**Decision in one sentence:** The "you have not acked this" reminder is a WAKE,
not a chat message: it rides its own kind (30015) instead of the kind-9 room
plane, it is owed once per CONVERSATION rather than once per message, it has a
budget after which it is terminally muted, and one scan is bounded by a
publication cap and a deadline.

**Builds on:** [ADR-0003](0003-nostr-mailbox-fabric.md) (the frozen kind
registry) and [ADR-0033](0033-additive-tools-kinds-discipline.md) §2 (additive
kinds are allowed with an ADR, a free block, and the registry test extended).

**Does not change:** the kind-9 room plane and its membership rules; the frozen
41-tool / 25-resource MCP surface; the doorbell's own prompt path (flywheel's,
`toron ack-sla` → `SlaFired` → inbox fetch).

---

## 1. What was wrong

Measured on the live store on 2026-10-21 (flywheel
`docs/analysis/stack-audit-2026-10-21.md` §1):

- `bus_events` held 100,000 rows, **95,077 of them one reminder plane**
  (`[doorbell] ack-sla-expired`), against 88 real room posts.
- 2,300 posts were overdue for project `flywheel`. Three threads held 714
  posts each — the same request re-posted every two minutes — and every one of
  them was its own reminder obligation.
- The reminder was published on `toron_kind(9)`, the ROOM plane. The doorbell
  forwards every kind-9 event into its wake stream, so 95,077 reminders
  saturated the single-serial wake brain: 1,879 real wake lines were dropped
  (45 events lost upstream of the cursor) and `/health` reported
  `degraded: ["wake_brain_dropping"]`.
- `toron ack-sla --project flywheel` took over two minutes and was killed at
  the caller's 15-second timeout every minute, so it never converged: the
  scan re-read the head of the list forever and `ack_sla_wakes` grew without
  bound.

## 2. The rules

1. **Its own kind.** `ACK_SLA_WAKE = 30015` (a free block: 30010-30014 are
   manifests and profiles, 30031/30032 are reservations and build slots; no
   live value is renumbered — ADR-0033 §3). The reminder keeps its
   `toron-kind=ack-sla-expired` tag, so the classifier that reads the tag is
   unchanged.
2. **The obligation is the conversation.** The dedupe key is
   `(scope, audience)`, where the scope is the post's thread (the post id when
   it has no thread). A thread with 714 posts is ONE obligation, not 714. The
   audience stays part of the key: one reader's ack must not silence the
   reminder for a co-recipient who has not read it.
3. **A budget is the terminal state.** A post nobody acks stays overdue
   forever, so an unbounded reminder re-nags forever — that was the flood. Each
   obligation gets `ACK_SLA_WAKE_BUDGET` (3) reminders; after that it is
   terminally muted. A muted obligation is not silently dropped: it stays in
   the reader's inbox, and the counter that muted it is durable in
   `ack_sla_wakes.wakes`.
4. **Terminal posts are never reminded.** A tombstoned post (`deleted = 1`) is
   unreadable, so it can never be acked; an un-addressed post (`to_list =
   '[]'`) has no reader, so the reminder can never reach anyone who could act.
   Both are excluded from the candidate set.
5. **One scan is bounded.** At most `ACK_SLA_SCAN_MAX_EMIT` (500) reminders
   are published per scan, and the scan stops at `ACK_SLA_SCAN_DEADLINE_MS`
   (10s) — the doorbell kills the scan at 15s, so a scan that outruns that
   never reports anything at all.
6. **One signer per scan.** The operator (or first agent) key is resolved once
   per scan, not once per candidate: resolving it per candidate re-read and
   re-decrypted the key file thousands of times, and that was the two-minute
   scan.

## 3. Why the reminder stays reachable

The doorbell's prompt path never was the bus event. It is
`toron ack-sla --project <p>` → the `expired` ids in its JSON → the brain's
`SlaFired` → `Effect::FetchInbox` → the inbox fold, which rings the pin that
holds an unacked high/urgent message. The kind-9 event was a SECOND ring path
for the same fact, and it was the one that saturated the brain. Moving the
reminder off kind 9 removes the duplicate; the doorbell's own scan keeps
prompting, once per SLA window, exactly as before.

## 4. What this does not settle

The reminder is a nag about an obligation, not the obligation: a post nobody
acks is still unacked after the budget is spent. Tightening that (an ack that
covers a whole thread, say) is a mailbox-contract change, not a reminder
change, and it is not decided here.
