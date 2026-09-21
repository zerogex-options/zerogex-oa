# Market Data Provider Abstraction

**Status:** implemented (branch `claude/market-data-provider-abstraction`) · **Date:** 2026-09-10
**Companions:** `docs/compliance/market-data-remediation-runbook.md` (steps 13–15, the parallel run
and cutover) · `docs/design/realtime-market-data-vendors.md` (which vendor, and why)

---

## Why

The remediation runbook's longest pole is not the licensing conversation, it's the engineering: until
a second feed can run, TradeStation switching the account off takes the product dark. The runbook
assumes the ingestion layer can host two feeds at once ("run the new feed alongside the old one",
"check the numbers actually match"). It could not: `stream_manager`, `volatility_index_ingester` and
`futures_underlying_ingester` each spoke TradeStation directly, and the vendor's shape leaked as far
as capitalised `"Bid"` / `"DailyOpenInterest"` dict keys.

This change adds the seam. A second vendor is now one new module plus two lines in a registry.

## What it does not do

**It does not rewire the live path.** `StreamManager` still constructs `OptionStreamAccumulator`
directly and still reads raw TradeStation dicts. `MARKET_DATA_PROVIDER` defaults to `tradestation`.
Production behaviour on this branch is unchanged, and that is deliberate: the abstraction has to be
reviewable on its own before it carries a cutover.

Moving the hot path onto the interface is a separate change, made once a vendor is contracted.

## Shape

```
src/ingestion/providers/
├── base.py           interface + normalised records + capability model
├── tradestation.py   the incumbent, as an adapter over existing accumulators
├── stub.py           template for the next vendor
└── __init__.py       registry; get_provider() reads MARKET_DATA_PROVIDER
```

Six operations, matching what the engine actually consumes:

| Operation | Replaces |
| --- | --- |
| `stream_option_quotes` | the chain stream |
| `stream_underlying_bars` | SPY / QQQ / SPX / NDX 1-minute bars |
| `stream_index_bars` | the VIX / VXN ingesters |
| `stream_futures_bars` | the @ES / @NQ ingester |
| `get_option_expirations` / `get_option_strikes` | chain discovery |
| `snapshot_option_quotes` | the REST open-interest and IV seed |

### Three decisions worth stating

**Normalised records, not vendor dicts.** `OptionQuote` and `Bar` carry exactly the fields
`option_chains` and `underlying_quotes` persist, under the DB's own column names.

Every field is `Optional`, and absent fields normalise to `None` rather than `0`. This is not
pedantry: a missing bid and a zero bid are different facts, and collapsing them presents a fabricated
two-sided quote to the IV solver and the fill model. The same rule applies to `up_volume`, where
`None` means "this feed cannot report a signed split" and `0` means "nothing traded".

**Streams are objects, not generators.** The existing accumulators are long-lived background readers
that reconnect underneath the caller, keep open interest and IV sticky, and are sampled at whatever
cadence the main loop wants. A generator cannot express that, so the protocols mirror the lifecycle
that already works: `start` / `stop` / `is_alive` / `drain` / `snapshot` / `updates_received`.

**Capabilities are declared, not discovered.** No evaluated vendor covers all six families:

| Vendor | Gap |
| --- | --- |
| ThetaData | no CME futures product at all |
| Databento | no index feed in the catalogue; `MAIN.CGIF` 404s, so SPX / NDX / VIX / VXN values are unavailable at any price |
| IQFeed | protocol-enforced symbol cap, and no redistribution |

`ProviderCapabilities` makes each gap explicit at construction, and every flag defaults to `False`,
so a forgotten declaration denies rather than silently allows. Asking a provider for a feed it does
not carry raises `ProviderCapabilityError` at startup, instead of writing empty tables that surface
hours later as "why is VIX stale".

An unrecognised `MARKET_DATA_PROVIDER` is likewise fatal. A typo that quietly fell back to the
default would be a cutover that appears to succeed while still reading the old feed.

## The comparison harness

`make feed-compare CANDIDATE=<name>` implements runbook step 14.

It samples both feeds over the **same contract list at the same instant** (the candidate reuses the
incumbent's spot, so a small price difference cannot select different strikes and manufacture a
divergence), pushes both through the **same** IV, Greeks and analytics code, and diffs the six
published numbers: spot, net GEX, call wall, put wall, gamma flip, max pain.

Comparing analytics rather than raw quotes is the point. Two feeds never agree tick for tick, so a
quote diff reports thousands of meaningless differences. What subscribers see is the derived layer,
and a wall is an argmax over a strike ladder, far more robust than the quotes beneath it. Holding the
analytics constant and varying only the feed makes a surviving difference attributable to the vendor.

Tolerances are split, because the metrics are not alike. Price-space metrics land on a strike ladder,
so anything above 0.05% means the feeds picked different strikes. Dollar exposure scales with open
interest and moves more, so it gets 2%.

Coverage is reported alongside every metric: contracts, two-sided quotes, and **contracts carrying
open interest**. That last one matters because GEX is OI-weighted, so a candidate that quotes the
whole chain but seeds no OI produces a plausible-looking empty gamma profile that a spot check would
never catch.

`--persist` writes both feeds to `option_chains_shadow` / `underlying_quotes_shadow` and the run to
`feed_comparisons` (`setup/database/shadow_tables.sql`, or `make feed-compare-schema`). These are
separate tables rather than a `source` column on the live ones, because the live tables feed the
analytics engine, every API route and every signal: a candidate under evaluation must have no path
into any of that, and a shared table with a discriminator column is one forgotten `WHERE` clause away
from serving a subscriber an unvetted number.

The runbook's bar is that differences are written down and *explained*, not that they are zero. The
`feed_comparisons` table exists so that can be met from SQL weeks later rather than from scrollback.

## Adding a vendor

1. Copy `stub.py`, implement the six operations against the vendor's client.
2. Declare `ProviderCapabilities` honestly, including the gaps.
3. Register the factory in `providers/__init__.py`.
4. `make feed-compare CANDIDATE=<name> --persist` for at least five sessions, one an OPEX Friday.
5. Write down the differences and why they occur.
6. Only then flip `MARKET_DATA_PROVIDER`, keeping the old feed warm for a week.

Four things the stub's docstring warns about, each learned from the incumbent path:

* **Sticky fields.** Only overwrite open interest and volume on a positive value, or a mid-session
  zero erases the accumulated figure.
* **Timestamps.** Drop an unparseable one rather than stamping `now()`; a misdated bar overwrites the
  current minute and corrupts the spot the Greeks are computed against.
* **Reconnects.** Exponential backoff with jitter, reset only after a healthy connection. Flat retry
  across many readers is what exhausted the per-account stream cap.
* **Signed volume.** Only TradeStation ships it on the bar. Otherwise leave it `None` and declare
  `signed_underlying_volume=False`.
