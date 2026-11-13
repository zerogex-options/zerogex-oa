# TradeWorkz forward tests — run the bots, tweak, find the edge

**Status:** shipped 2026-11-12 · **Modules:** `src/strategies/` (the switch),
`src/tradeworkz/chain.py`, `src/tradeworkz/structures.py`,
`src/tradeworkz/forward_test.py` · **Scorecard:** `make tradeworkz-forward-test`

## Why

The fleet has been parked since the 2026-08-09 shelving. A bot could only get a
paper sleeve with a validated, bot-screened edge, and the history to screen on
is 60–90 days of chain archive: most candidates produced 0–14 trades, too few
to call anything (see each catalog entry's research log). Waiting for deeper
history means waiting indefinitely. A forward test settles it the other way:
the bot trades live on paper and its own trades become the record.

## What runs

Ten bots, set by `forward_test=True` on their catalog entries — the nine
CANDIDATE bots whose screens looked good on too few trades, plus the bot for
`gex_gradient_trend`, the strategy with the platform's one measured edge:

`charm_close_magnet`, `climax_flow_fade`, `dual_flip_dislocation`,
`gamma_regime_shift_rider`, `profile_shelf_breaker`,
`put_capitulation_credit_fade`, `settlement_flow_snap`,
`vanna_vol_crush_rider`, `weekly_charm_grind`, `gex_gradient_trend`.

`gex_gradient_trend` trades options 2 weekdays out (a `bot_params`
override), not its pattern's 5: production ingests each symbol's nearest 3
expirations (`INGEST_EXPIRATIONS=3`) and the bot only fires on the
daily-listing signal symbols (SPY/SPX/QQQ/NDX), so a 5-weekday contract never
exists live and the bot could not open at all.

They are visible to members, marked as tests: each card's tagline starts
"Forward test:", and the API returns `forward_test: true` on `/bots`,
`/bots/{id}` and `/leaderboard` for the website to badge. Followers get trade
alerts as with any bot.

Each gets an even slice of `TRADEWORKZ_FLEET_CAPITAL` ($100K of the default
$1M). Every risk control still applies — the daily loss kill, the 0.5× size
throttle on a losing profit factor, the adaptive entry bar, the bias veto, the
RTH gate — except the auto-disable circuit breaker: switching a test off after
twenty trades ends it before it says anything (a long-premium bot can be
profitable at a 30% hit rate).

**The flag is the switch.** On every API start, provisioning funds and enables
every flagged bot and disables and zeroes everything that shipped and is no
longer live. To stop a test, clear its flag. To start one, set
`forward_test=True` and `provisioned_history=True` (the catalog check refuses
the first without the second, so clearing the flag later always disables the
row).

## Any symbol

`TRADEWORKZ_UNIVERSE=*` (now the default) is every symbol in
`ANALYTICS_UNDERLYINGS`, minus futures. Stocks used to be kept out because a
bot names its contract by arithmetic — today's date, a $1 grid — and a stock
lists neither. Now:

* **The snapshot reads the listed strike grid** near the money from
  `option_chains_latest` (an operator's `TRADEWORKZ_STRIKE_INCREMENTS` entry
  still wins), so widths come out right ($2.50 on most MAG 7 names).
* **Every open fits its legs onto listed contracts** (`chain.fit_legs`): the
  earliest listed expiration on or after the one asked for (up to 7 days out,
  never earlier) and the nearest listed strike (at most one step away).
  A fit that would merge two strikes or two expirations, or land off the
  ingested band, is refused rather than turned into a different trade.
  Already-listed legs come back unchanged, so SPY/QQQ/SPX behave as before.
* **A rolled trade still closes the day the bot asked for.** The time stop is
  capped at 15:55 ET on the expiration the bot requested, not the one it got,
  so a same-day bot on NVDA trades Friday's contract but is flat by today's
  close.

Each symbol adds one snapshot (~30 reads) per 5-second tick, inside the API
process. Run `make capacity-check` during a session after widening the
universe; narrow it with an explicit list if the database feels it.

## Any (capped-risk) structure

`structure_override` in a bot's params re-expresses its signal without
touching the bot (`structures.reexpress`), in the live engine, the research
harness and product bot replay alike:

| Signal | Structures |
|---|---|
| bullish / bearish | `long_single`, `debit_vertical`, `credit_vertical`, `butterfly` (body on the bot's target) |
| neutral, native trade sells premium | `iron_condor`, `iron_fly`, `long_butterfly` |
| neutral, native trade buys premium | `long_straddle`, `long_strangle` |

Geometry is in listed strike steps around the money:
`structure_width_increments` (default 2) and `structure_offset_increments`
(default 0; a condor's short strikes and a strangle's legs default to one
width out). The bot's own expiration, spot target and stop, time stop and
conviction carry over. A structure from the wrong family is refused (the
signal is dropped and counted in the bot's miss reasons).

Capped risk only: every open — re-expressed or native — must have each short
option covered by a long of the same right (`sizing.is_capped_risk`), or it is
refused. No naked shorts, short straddles or strangles: sizing divides the
heat budget by the structure's max loss, and theirs is unbounded.

## The tweak loop

1. **Change a bot's tuning** in its catalog `bot_params` (thresholds,
   `dte_target`, `structure_override`, widths, stops). Merge and restart the
   API; provisioning re-syncs params from the catalog.
2. **Every entry records its tuning** as `params_version` (a fingerprint of the
   bot's params) in the trade's `components_at_entry`.
3. **Read the scorecard:** `make tradeworkz-forward-test` scores each bot
   overall and split by symbol, structure and tuning against the catalog's
   promotion bar: at least 20 trades, profit factor at least 1.10, positive
   expectancy. A tuning is judged only on its own trades.
4. **When the current tuning clears the bar**, the scorecard says so. Record a
   `ResearchRun(harness="tradeworkz-live", verdict=Verdict.EDGE, ...)` on the
   catalog entry and set its stage to VALIDATED: the bot is then funded on
   evidence, and its card drops the "Forward test:" label at the next start.

Optional, before a tweak goes live: `make tradeworkz-backtest ARGS="--bots
<id>"` screens the same bot and structure over history. The harness applies the
same contract fit and structure override.
