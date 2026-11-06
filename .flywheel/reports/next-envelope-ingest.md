# bd-next-envelope-ingest-yl0cx — LANE A report

status: done
done:
  - mail ingest into the envelope log + per-message `next` (receipt
    `toron/.flywheel/receipts/bd-next-envelope-ingest-yl0cx-6860f1818eec.verify.txt`,
    commit `6860f1818eec5491c07a9805bb62f76e4c0a2374` in toron)

## Root

Two planes, one gap: `mail inbox` projects Nostr DMs off the bus;
`next` reads `inbox_log`, which only `inbox append` fed. Ordinary
`mail send` mail never entered the log, so `next` was empty while the
inbox was full — and agents fell back to `inbox --since last`, whose
timestamp high-water skips every message after the answered one.

## Fix (producer / ingest / routing)

- Producer: the author's npub (`from` == `msg.producer`, the log's
  identity rule); per-sender gapless seq with bounded SeqGap retry.
- Ingest: `Mailbox::ingest_delivered_mail` (toron-core) — collect the
  reader's DMs (extracted `collect_unwrapped_mail`, shared with
  `fetch_inbox`), one `request@1` envelope per message. Request, not
  notify: the door ends each mail with an ack and only obligation
  classes admit one (`allowed_now` already suggests `toron ack`).
- Routing: `to` = reader npub; context = project (ungoverned → authority
  passes). Exact dedupe by rumor id (`mail_ingest` additive table; a
  stale claim heals by re-appending, never invisible).
- `next` is now per-message, not high-water: `STRAGGLER_SQL` serves
  live-but-behind-cursor rows ahead of fresh ones, so an ack advances
  exactly the acked message. Terminal = rank-6 exits + each class's
  success terminal (spelled out in SQL; no migration, old stores read
  identically). `UNREAD_SQL`, head parity, and the 100k plan test are
  untouched.
- CLI `next` ingests before reading (one call site); ingest trouble
  warns loud and serves logged rows — the door degrades, never blinds.
- `mail inbox --since last` semantics intentionally unchanged.

## Tests (all green)

- `next_ingest_two_mails_ack_one_returns_other`: blind-next empty →
  ingest 2 → next returns both Delivered → ack one → next returns the
  other → re-ingest adds 0 → ack other → next empty.
- VERIFY: `test -p toron-core --lib` 919/0, `cli_robot` 49/0,
  `check --workspace --all-targets` exit 0 (all on the final tree).
- Live probe (project flywheel, no ack performed): WindyCreek
  `items: []` → 6, cursor 0→532 then stable; SunnyMarsh 0→547,
  15 items; `allowed_now` offers `toron ack … accept`.

## Residual inventory

- None from this bead. Urgent-via-next (`--urgent --peek`) still only
  sees steer-contract rows — mail urgency routing was not asked for.
- Note: Task-Acted (done, unverified) repeats in `next` until Proved —
  pending verification reads as still open, by the same rule.

recommendation: close (task=verdict binding)

PRINCIPLES: fix-root-causes — the per-message door now sees the mail
(DM→envelope ingest, not a second cursor); prove-it-works — bead-exact
test + full VERIFY + live probe on the reported pins; no silencers —
stale claims heal visibly, ingest failure warns loud.
