# A Symbol Stopped Updating

Use this when one or more underlyings stop producing data while the rest keep
going — the "nothing but SPY is updating" shape.

`zerogex-oa-ingestion` is **one parent process that forks one child per
symbol** (plus VIX, VXN, and the futures display feeds — 8 children for the
default four-symbol config). The parent supervises them; each child owns its
own TradeStation streams and DB writes for exactly one symbol.

That topology is the whole reason this runbook exists. A single child dying
does not stop the unit, so **every process-level check reports healthy**:
systemd shows the unit `active` (the parent is alive), `Restart=always` never
triggers (the parent never exited), `OnFailure` never dispatches, and the
per-minute liveness watchdog — which only runs `systemctl is-active` — stays
green. The only signal that something is wrong is that one symbol's rows stop
appearing.

`zerogex-oa-ingestion-freshness.timer` now catches this within ~15 minutes.
This runbook is for when you are ahead of the alert, or diagnosing after the
fact.

## 1. Find the gap's real edges

```sql
SELECT symbol, max(timestamp) FROM underlying_quotes
WHERE timestamp > now() - interval '2 days' GROUP BY symbol ORDER BY symbol;
```

Healthy looks like: equities (SPY, QQQ) current to the minute during
04:00–20:00 ET; cash indices (SPX, NDX) parked at their 16:00 ET close, since
they print no underlying level outside the regular cash session. **A cash
index sitting at 16:00 in the evening is correct, not a gap.**

For a suspect symbol, list the raw timestamps to see where it stops:

```sql
SELECT timestamp FROM underlying_quotes
WHERE symbol='QQQ' AND timestamp > now() - interval '3 days' ORDER BY timestamp;
```

**Read where the gap starts, not just that one exists.** A stop at the session
edge (20:00 ET) that never resumes at the next 04:00 open is a different
failure from one that stops mid-session. Timestamps are UTC; subtract 4 (EDT)
or 5 (EST) for ET.

## 2. Dead child, or stalled child?

This is the fork in the road, and the two look identical from the database.
Each child logs under its own PID, so the journal answers it directly:

```bash
journalctl -u zerogex-oa-ingestion --since "-6h" -o short-iso \
 | awk '{match($0,/ingestion\[[0-9]+\]/); p=substr($0,RSTART,RLENGTH);
         if(p!=""){if(!(p in first))first[p]=$1; last[p]=$1; n[p]++}}
        END{for(p in last) printf "%-22s %s .. %s  (%d lines)\n", p, first[p], last[p], n[p]}' \
 | sort
```

Children are forked in `INGEST_UNDERLYINGS` order, then VIX, VXN, futures — so
they occupy **consecutive PIDs**. Map them by position: with
`INGEST_UNDERLYINGS=SPY,$SPXW.X,QQQ,$NDXP.X`, the 3rd child PID is QQQ. Confirm
the mapping against a line that names its symbol (`Initialized IngestionEngine
for QQQ`).

- **A PID missing from the sequence entirely, or one that stopped logging while
  its siblings continued** → the child is **dead**. The supervisor should have
  restarted it; check for `exited unexpectedly` / `GIVING UP` below.
- **A PID still logging but writing no bars** → the child is **stalled**. Its
  streams are up but starved. Look at the underlying staleness ladder in
  `stream_manager.py` (`STALE`, `Restarting underlying bar stream`).

Then read what the supervisor did:

```bash
journalctl -u zerogex-oa-ingestion --since "-6h" \
  | grep -iE "exited unexpectedly|respawning|GIVING UP|Re-attempting|supervisor crashed|Fatal error"
```

Note `--since "-6h"`: the `--since "-5min"` shorthand parses unreliably and
silently returns `No entries`, which reads exactly like "nothing was logged."
Prefer `-n 200 --no-pager` when in doubt.

## 3. What the supervisor does on its own

Understanding this tells you whether to intervene:

| Journal line | Meaning | Action |
|---|---|---|
| `exited unexpectedly … respawning in Ns` | Normal recovery, backoff doubling 5s→120s | None; confirm it came back |
| `GIVING UP on ingest-<name>` | Budget spent (5 deaths/15 min). That symbol writes nothing | Investigate the child's error |
| `Re-attempting abandoned … after 900s` | Slow retry; recovers if the cause cleared | None if it then stays up |
| `Every ingestion worker is currently abandoned` | Total outage, unit deliberately stays up retrying | Check upstream (TradeStation, DB) |
| `Ingestion supervisor crashed` | Parent-level bug; traceback follows | Read the traceback |

Abandonment is never permanent, and the unit never takes itself down over a
bad worker — that would kill the healthy symbols too, and `StartLimitBurst`
would fail the unit permanently.

## 4. Restart

```bash
sudo systemctl reset-failed zerogex-oa-ingestion   # only if it shows failed
sudo systemctl restart zerogex-oa-ingestion
sleep 15 && systemctl status zerogex-oa-ingestion | grep -E "Tasks|Main PID"
ps --ppid $(systemctl show -p MainPID --value zerogex-oa-ingestion) -o pid,etime,cmd | cat
```

**The `sleep 15` is not optional.** `Type=simple` reports started as soon as
exec happens, and the engine imports numpy/scipy/pandas before forking — so
`Tasks: 1` immediately after a restart is the import phase, not a failure.
Expect `Tasks:` in the dozens and one child per configured feed.

## 5. Backfill the gap

`underlying_quotes` is recoverable from TradeStation's barchart history. It is
idempotent on `(symbol, timestamp)`, so a wider range than the gap is safe.

```bash
venv/bin/python -m src.tools.underlying_backfill \
  --symbols QQQ --start 2026-08-18 --end 2026-08-18 \
  --session-template "$(grep '^SESSION_TEMPLATE=' .env | cut -d= -f2)" --dry-run
```

**Pass the same `--session-template` as `.env`.** The tool defaults to
`Default` (09:30–16:00 ET); production runs `USEQ24Hour` (04:00–20:00). Using
the default against a 24-hour deployment silently backfills only the regular
session and leaves every pre/post-market bar missing — the bar count is how you
catch it (~390/day under `Default` vs ~900 under `USEQ24Hour`). Drop
`--dry-run` to write.

Only backfill what was actually lost: if the outage began after 16:00 ET, the
cash indices lost nothing.

A residual first-bar warning (`first bar 07:35 ET, 215 min after the session
open`) means the vendor did not serve that window — not a tool bug. Compare
another symbol on the same date to confirm, and re-run the next day to see
whether it fills in.

## 6. Confirm

```bash
make ingestion-freshness-healthcheck
```

Exit 0 with every in-window symbol `fresh`. Symbols reported `outside its
delivery window` are not being checked — expected overnight, at weekends, and
for cash indices before 09:30 / after 16:00 ET.

## Known limits

- **Journal retention is the binding constraint on post-mortems.** The cap
  (`SystemMaxUse` in `setup/systemd/zerogex-oa-journald.conf`, mirrored by the
  nightly vacuum, which reads that same file) is shared across all four
  services. In the 2026-08-17 incident it held only minutes of history, so the
  evening's traceback was gone by morning and the root cause was never
  recovered — only the fact of the death, inferred from PID sequence. If you
  are diagnosing something intermittent, raise the cap **first**, then wait for
  a recurrence. Check `df -h /` before raising it; the root volume runs hot.
- The freshness check pages every 10 minutes while a symbol is stale. That is
  intentional for data loss, but it means a long known outage is noisy.
- Nothing here detects a child that is alive, streaming, and writing
  *wrong* data — only absence.
