# Gamma Regime Session History

The Gamma Shift page (`/gamma-shift`) reads dealer gamma as a **change**, and
two of its four surfaces depend on one table: `gex_regime_session`, which holds
one stored read per trading day.

- The **Session History** strip renders one bar per stored session. With no
  rows it renders "No stored sessions for {symbol} yet" — indefinitely, and
  with no error anywhere, because an empty history is a legitimate state for a
  symbol whose first session has not closed.
- The **Gamma Regime Shift** card normalizes its magnitude against the trailing
  standard deviation of those same rows. Under `MIN_SESSIONS_FOR_SIGMA` (10)
  stored sessions it bootstraps off the chain instead and labels the magnitude
  provisional ("Magnitude is provisional — only N stored sessions…").
- The **Expiry Roll-off** panel's "is that a lot?" percentile is a rank against
  `rolloff_share` on the same rows, and is `null` until the window fills.

Nothing writes those rows as a side effect of serving traffic. They exist only
because `src/tools/regime_session_refresh.py` ran.

## Wiring

| Piece | What it does |
| --- | --- |
| `zerogex-oa-regime-session-refresh.timer` | Fires every 15 min, 09:00–16:45 ET Mon–Fri |
| `zerogex-oa-regime-session-refresh.service` | Runs `make regime-session-refresh` |
| `src/tools/regime_session_refresh.py` | Computes today's since-open read and upserts it |
| `src/analytics/regime_session.py` | The read itself — shared with the live endpoint |

The tool self-gates to 09:45–16:20 ET on non-holiday weekdays, so the fires
either side of the session are cheap no-ops. **Inside** the window a run that
writes nothing exits non-zero and the unit shows in `systemctl --failed`: an
empty history strip is exactly the sort of silent, exit-0 failure that goes
unnoticed for months, so it is made loud.

Today's row is rewritten through the session and freezes into "what that
session was" once the analytics engine stops writing after the close. There is
deliberately no separate end-of-day job — one that computed the read even
slightly differently would put a bar in the strip that disagreed with the
headline directly above it, every day.

## Standing it up

```bash
make regime-session-refresh-install   # enable the 15-min cash-session timer
make regime-session-backfill          # seed 60 weekdays (do this once)
make regime-session-refresh-status    # timer state + last/next fire + recent log
```

The backfill is what takes a symbol from the provisional bootstrap magnitude to
the strong trailing claim, so run it whenever a new underlying is added to
`$BULLETIN_TWEET_SYMBOLS`. It walks sessions **oldest first**, so each row
normalizes against the sessions before it exactly as it would have on the day.

Useful overrides:

```bash
make regime-session-backfill SYMBOLS="SPY QQQ SPX"
make regime-session-backfill REGIME_SESSION_DAYS=120
make regime-session-backfill DRY_RUN=1          # compute and log, write nothing
make regime-session-refresh REGIME_SESSION_FORCE=1   # run outside the session
```

## Checking it

```sql
SELECT underlying, COUNT(*) AS sessions, MIN(session_date), MAX(session_date)
FROM gex_regime_session
GROUP BY underlying
ORDER BY underlying;
```

Then the endpoints the page actually calls:

```bash
curl -sS -H "X-API-Key: $API_KEY" \
  "https://YOUR_API_HOST/api/gex/regime-history?symbol=SPY&limit=30" | python -m json.tool
curl -sS -H "X-API-Key: $API_KEY" \
  "https://YOUR_API_HOST/api/gex/regime-shift?symbol=SPY&lookback=session" | python -m json.tool
```

`read.normalization` tells you which regime the card is in:

- `trailing` — ≥10 stored sessions; the magnitude is this symbol's own history.
- `proxy` — bootstrapping off the chain's near-spot gamma stock. Real and
  directionally meaningful, but labelled provisional on the card. Run the
  backfill.
- `none` — the snapshot carries no near-spot gamma at all (a degraded chain).
  The card renders the direction and makes no magnitude claim.

## Symptoms and causes

**"No stored sessions for SPY yet" that never fills.** The timer is not
installed or not enabled. `systemctl list-timers 'zerogex-oa-regime-session-*'`;
if it is absent, `make regime-session-refresh-install`.

**The strip fills but every session reads QUIET.** Check
`read.normalization`. On `proxy` with a handful of sessions this is expected to
be soft — run the backfill. On `trailing`, check the chain's tail weight before
concluding the market was quiet:

```bash
make regime-scale-report          # read-only; add SYMBOLS="SPY SPX" to narrow
```

It reports, per symbol and per axis, the shape of the stored shift
distribution and the QUIET rate each candidate z-score denominator would have
produced over that history — so the cut is chosen from measurement rather than
from an argument about estimators. Two such arguments have already been wrong.

`sd/mean|x|` is `sqrt(pi/2)` = **1.2533** for any Gaussian-shaped chain; above
that, a few violent sessions are larger than the ordinary ones and a
squared-moment scale is being set by them.

`skew` (`mean|x| / median|x|`) is the column that decides between the robust
candidates, and the one that is easy to miss. Where it is well above 1, the
MEDIAN session is much smaller than the mean one — so a scale built from a
mean still reports the median session as nothing happening, which is what the
card shows as "always QUIET". `regime_shift.robust_scale` is mean-based, so a
high `skew` is the signal that it is not enough on its own.

`drift` (`|mean| / mean|x|`) is how much of an axis is a persistent, repeated
shift rather than variation around zero. The denominator is measured about
zero, so a high-drift axis is charged for that drift: its typical day becomes
its own yardstick. That is deliberate — a chain that sheds gamma every session
IS deteriorating every session, and the state's SIGN comes from the raw score,
so mean-centring would report a below-average shedding day as FIRMING — but it
does push a high-drift symbol toward QUIET, and it is worth knowing which
effect you are looking at before changing anything.

`quiet_rate` should land near **25%** — the fraction of a bivariate normal
inside the `QUIET_Z` = 0.75 ring. A symbol far above that whose `tail_weight`
is normal is a genuine market observation. A symbol far above it WITH a high
`tail_weight` is a measurement artifact, and the cross-check that settles it is
SPY against SPX: they track the same index, so a large gap between their
`quiet_rate`s is the read miscalibrated, not the market.

**A single session is missing from the middle of the strip.** That day had no
usable snapshot pair (a market holiday, or an ingestion outage). The refresh
skips rather than storing a read built on one frame. Repair with
`make regime-session-backfill REGIME_SESSION_DAYS=N` — the upsert is idempotent
and recomputes from immutable `gex_by_strike` / `gex_summary` history.

**The unit is failing but the table looks fine.** A run inside the window that
wrote zero rows exits 1. Read the reason with
`journalctl -u zerogex-oa-regime-session-refresh -n 50`: usually every symbol
skipped because the analytics engine was not writing.
