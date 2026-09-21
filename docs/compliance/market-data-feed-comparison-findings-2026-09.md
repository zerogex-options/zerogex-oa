# Feed comparison findings — ThetaData Market Value, September 2026

**Runbook step:** 14, "Check the numbers actually match".
**Question:** would a subscriber see a different number if we served ThetaData's Market Value
feed instead of TradeStation's realtime feed?
**Answer:** no, on every level they read. The one metric that moves is explained below and moves
less than the incumbent feed already moves against itself.
**Status:** step 14's *evidence* bar is met; its *calendar* bar is not. See "Where this falls
short" — that gap is deliberate and recorded, not overlooked.

Not legal advice. Engineering findings, written so the question "are the differences written down
and explained" can be answered from this file instead of from a chat transcript.

---

## What Market Value is

ThetaData sells a derived product that takes each contract's real-time bid and ask and randomises
each by up to a penny. Exhibit A of the executed agreement calls it "This adjusted bid and ask
value … derived from the real-time bid and ask prices of the option where each is randomized by a
penny." ThetaData characterises it as a derived product carrying no exchange fees, which is the
entire reason we are moving to it.

The penny adjustment is a **fixed absolute perturbation**. Its effect on anything computed
downstream therefore scales inversely with option premium, and that single fact predicts every
result below.

**We do not attempt to reverse it.** Not by inverting the adjustment, and not by averaging repeated
polls of a static quote to average the noise out — those are the same act under two names, and both
would reconstruct licensed exchange data. That constraint is also written into §1.2 of the executed
agreement, which excludes from "Derived Data" anything from which the underlying quotes "can be
reverse-engineered, reconstructed or substantially recovered."

---

## Verdict

**116 paired samples** across three underlyings, four sessions. Every sample compares Market Value
against realtime on the *same terminal*, over the *same contract list*, pushed through the *same*
IV, Greeks and analytics code, so the feed is the only variable.

| Metric | Result |
|---|---|
| `spot` | **0.00% apart on every sample** |
| `call_wall` | **0.00% apart on every sample** |
| `put_wall` | **0.00% apart on every sample** |
| `max_pain` | **0.00% apart on every sample** |
| `gamma_flip` | 0.01%–0.11% apart, against 0.06%–0.31% self-variance |
| `net_gex` | moves; always below each feed's own self-variance — see below |

For the strike-quantised metrics that 0.00% is not "inside tolerance". Those are compared in
strikes, so it means the *same strike* came back, 116 times out of 116.

---

## Sample coverage

| Date | Underlying | Paired samples | Persisted | Note |
|---|---|---|---|---|
| 2026-09-15 | SPY | 28 | yes | |
| 2026-09-15 | `$SPXW.X` | 30 | yes | flip unresolved throughout — see F3 |
| 2026-09-15 | `$SPXW.X` | 3 | no | post-close, market frozen — the cleanest measurement we have |
| 2026-09-16 | QQQ | 31 | yes | |
| 2026-09-16 | `$SPXW.X` | 8 | no | 3 vs 6 expirations, 3 seconds apart |
| 2026-09-21 | `$SPXW.X` | 16 | no | 3/6/9/12 expirations |
| **Total** | | **116** | | |

A separate 58-sample SPY run compared TradeStation against ThetaData realtime (vendor migration,
not the Market Value question): spot and put_wall identical 58/58, call_wall 49/58, max_pain 32/58,
net_gex 2.67% average against 20.39% self-variance. The residual divergence traced to intermittent
open-interest *delivery* on the TradeStation side (210.0 vs 232.8 contracts carrying OI per poll),
which favours ThetaData.

`$NDXP.X` could not be tested — see F5.

---

## F1 — net_gex moves, and the size of the move is a function of premium

`net_gex` is the only metric that differs, and the differences are small relative to the noise the
metric already carries.

| Underlying | feeds apart | incumbent self-variance | candidate self-variance |
|---|---|---|---|
| SPY | 25.22% | 33.30% | 27.93% |
| SPX | 2.79% | 16.66% | 17.83% |
| QQQ | see F2 | see F2 | see F2 |

**On both underlyings the cross-feed difference is smaller than each feed's own minute-to-minute
variance.** A subscriber watching net_gex on the realtime feed already sees it move by a third
between consecutive polls.

The SPY/SPX ratio is **9.04×**. The SPX/SPY spot ratio is **10×**. That is the mechanism falling out
of the data: a fixed half-cent mid adjustment is a ~10× smaller *relative* perturbation on SPX
contracts than on SPY contracts, it propagates through the IV solve into gamma, and out into
net_gex proportionally. The prediction — QQQ behaves like SPY, NDX like SPX or better — held.

Two further confirmations:

- **Chain depth dilutes it.** On SPX the cross-feed gap halved as the chain doubled: 3.11% at three
  expirations, 1.16% at six (2026-09-16); 2.43% / 0.59% / 0.93% / 0.68% across 3/6/9/12
  expirations (2026-09-21). More contracts, same penny, smaller share.
- **The harness figure is an upper bound, not a measurement.** See F6.

**Note on which net_gex this is.** The harness compares `total_net_gex` — the whole-chain sum from
`_calculate_gex_by_strike`. The `/levels` endpoint serves `net_gex_at_spot`, read off the smoothed
spot-shift profile, which is the less quote-sensitive of the two. We measured the harsher figure.

---

## F2 — a metric that changes sign has no meaningful percentage

The QQQ run (2026-09-16, 31 samples) printed **535.04% self-variance and 25.58% feeds apart** on
net_gex. Both figures are artefacts and neither describes the feed.

QQQ's chain-sum GEX crossed zero during the run — **-605.86M to +399.38M and back to -405.71M**.
Once a series changes sign its mean sits near zero, and every percentage divides by almost nothing.

In dollars, over the same 31 samples:

- mean difference between the feeds: **7.07M**
- worst single difference: **22.21M**
- range the metric travelled by itself: **1,005M** → the worst gap is **2.2%** of it
- between two consecutive polls the incumbent moved **461M in one minute**, 65× the typical gap

`feed_compare` now detects the sign change and prints the absolute comparison alongside the
percentages, labelling them as non-measurements (commit `6259d85`).

---

## F3 — the gamma flip is regime-dependent, and that is not a feed finding

On 2026-09-15 SPX reported `gamma_flip` unresolved on all 30 samples — **on both feeds
identically**. It resolved cleanly on 2026-09-16 (7,695.50 at three expirations) and was again
unresolved at every chain depth on 2026-09-21.

The cause is market structure, not vendor. On the 16th SPX net_gex ran -1,753M to -4,662M; on the
21st it ran **+44,220M to +74,782M**. A chain that long-gamma has no dealer-gamma zero crossing
anywhere near spot. Reading back from the profile's sign distribution, the crossing sat roughly
**26–39% above spot** against `GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT = 0.08`. The engine returning
NULL is correct behaviour.

`feed_compare` previously printed a bare `-` for this, indistinguishable from the two feeds
agreeing. It now emits the engine's unresolved-flip diagnostic naming which gate rejected
(`369aa31`), extended to report how much of the chain was excluded and why (`8898957`).

**This is a pre-existing property of the metric.** It is unchanged by the migration and would have
behaved identically on TradeStation.

---

## F4 — Exhibit A does not cover two endpoints the chain ingestion depends on

Verified against a live response on 2026-09-17:

| Endpoint | Columns returned | Market Value? | In Exhibit A? |
|---|---|---|---|
| `option_snapshot_market_value` | `market_bid`, `market_ask`, `market_price` | yes | yes |
| `option_snapshot_ohlc` | `open`, `high`, `low`, `close`, `volume` | **no** | **no** |
| `option_snapshot_open_interest` | `open_interest` | **no** | **no** |

`thetadata.py` routes quotes through `_endpoint()` to the Market Value call but fetches OHLC and
open interest directly (`:888`, `:891`). Both are plain OPRA data, outside the penny randomisation
the fee exemption rests on, and **open interest is what every gamma exposure figure is weighted
by** — the product cannot operate without it.

ThetaData stated by email (Bill Tompkins, 2026-09-17): *"all three data feeds are derived
produts [sic]"*, and on 2026-09-21: *"My management team considers the Market Value data stream a
derived data feed."* **Neither statement is in the executed agreement.** Exhibit A was amended to
read "This adjusted bid and ask value is derived from…" on the options and stocks lines, which
strengthens the characterisation of the quote streams only.

**Open risk.** §3.1's termination right is scoped to "the Exhibit A data". If an exchange asserts a
fee on open interest, that right may not reach it. Accepted knowingly; the correspondence above is
the record.

---

## F5 — NDX needed a separate entitlement, now contracted

`$NDXP.X` failed on every sample, both feeds:

```
Requesting data for NASDAQ GIDs symbols: [NDX] without proper permissions.
```

Exhibit A's Indices line covered the **Cboe CGIF** feed. NDX is a Nasdaq index under **Nasdaq
GIDS**, a different entitlement. Options were unaffected — a probe on 2026-09-17 returned 240 of
240 contracts, all two-sided, under the OPRA root `NDXP`, in 0.21s. Only the index *level* was
blocked.

Resolved in the executed agreement: NDX added to the **Indices – Market Value** line of Exhibit A,
so it inherits the derived characterisation. ThetaData confirmed by email that NDX is supplied as
Market Value, meaning no separate Nasdaq GIDS exchange fee falls to us under §3.1. Priced at
+$250/month for months 1–6 and +$500/month for 7–12 (Exhibit B: $1,250 / $2,500).

**Still to verify:** VIX and VXN sit inside the existing CGIF coverage. Asked three times, never
answered. VXN is the one to check — a Cboe-published index on a Nasdaq underlying is exactly the
shape that produced this finding.

---

## F6 — "feeds apart" is an upper bound, not a measurement

The harness polls the two feeds **sequentially**: the incumbent's chain snapshot completes, then
the candidate's. Measured gaps ran **0.2s to 1.0s**. While the market is moving, part of every
cross-feed difference is that gap rather than the vendor.

The post-close SPX run on 2026-09-15 removes the term entirely. With quotes frozen, the only
remaining difference is the penny adjustment itself:

| | live SPX (30 samples) | post-close SPX (3 samples) |
|---|---|---|
| net_gex, feeds apart | 2.79% | **0.08%** |
| everything else | 0.00% | **0.00%** |

Two variables changed at once (frozen market, and 0DTE dropping out of the profile), so the whole
35× reduction cannot be charged to sampling skew. And post-close quotes are wider, which makes a
fixed penny adjustment proportionally smaller — so **0.08% is a lower bound as surely as 2.79% is
an upper one.** The truth sits between two numbers that are both far inside "no subscriber could
tell."

`feed_compare` now records when each snapshot landed and labels the column accordingly
(`be28638`).

---

## F7 — contract coverage, which the runbook says to watch

> *"Watch the with-OI contract count, not just the metrics. GEX is open-interest weighted, so a
> candidate that quotes the whole chain but seeds no OI produces a plausible-looking empty gamma
> profile that a spot-price check would never catch."*

Market Value and realtime returned **identical counts on every sample** — same contracts, same
two-sided count, same with-OI count, every time. Representative figures:

| Session | contracts | two-sided | with-OI |
|---|---|---|---|
| SPX 2026-09-15 (post-close) | 240 | 201 | 238 |
| QQQ 2026-09-16 | 240 | 237–240 | 231–233 |
| SPX 2026-09-21, depth 3 | 240 | 232–234 | 211 |
| SPX 2026-09-21, depth 12 | 960 | 953 | 862–865 |

No coverage difference between the two feeds was observed in any run.

---

## F8 — SPY and QQQ spot is single-venue, by design

Exhibit A, Stocks – Market Value: *"Derived from Real Time Nasdaq Basic Data; 15-Min Delayed CTA &
UTP SIP Data."*

The real-time path for SPY and QQQ is **Nasdaq Basic — one venue, not the consolidated tape.** The
consolidated CTA/UTP feed is available only at a 15-minute delay. There is no real-time
consolidated option in this product.

`thetadata.py:37-42` already documented and handled this before the agreement confirmed it: the
provider declares `signed_underlying_volume=False` so callers cannot mistake single-venue volume
for a full-market figure. Nasdaq Basic tracks SPY within pennies, which is all the Black-Scholes
spot input needs.

**Customer-visible consequence:** the website candlestick charts for SPY and QQQ are single-venue
and will not tick-for-tick match a consolidated chart elsewhere. SPX and NDX are unaffected (CGIF
and GIDS, both real-time).

---

## Findings that turned out not to be about the feed

Recorded because each cost investigation time and each looked like a vendor problem first.

1. **SPX gamma_flip unresolved** — market regime (F3). Identical on both feeds.
2. **The $29.91 gamma-flip shift** between three and six expirations — `INGEST_EXPIRATIONS`, a
   configuration set under a TradeStation streaming constraint that does not apply to a polling
   deployment. Open; see "Carried forward".
3. **QQQ's 535% net_gex self-variance** — a zero-crossing artefact (F2).
4. **64 QQQ contracts with no solved IV, constant at every chain depth** — one expiration's worth
   of *expired* contracts. The run was 19 minutes after the close; past the settlement instant
   `calculate_time_to_expiration` returns exactly 0.0 and the IV solver cannot touch them.
5. **IV p90 falling with chain depth** (0.356 → 0.129 across 3 → 12 expirations) — compositional.
   A three-expiration chain on a daily-expiry underlying is entirely 0–2 DTE, where the smile is
   steepest; the median barely moved. Term structure flattening, not data quality.

---

## Where this falls short of the runbook

Stated plainly so nobody later mistakes what was done for what was asked.

| Runbook asks | Delivered |
|---|---|
| At least five days | **three** — 15, 16 and 21 September |
| One OPEX Friday | **none.** 18 September was quarterly triple-witching; no data captured |
| One volatile day | arguable — SPX went from short gamma to +$75bn between the 16th and 21st |
| `MINUTES=390` (full session) | longest single run was 30 minutes |
| `PERSIST=1` | SPY and SPX Market Value runs only; the A/B and depth sweeps persisted nothing |

The evidence bar is met by volume and by breadth. The calendar bar is not. The one genuinely
untested regime is **OPEX**, where 0DTE walls dominate and the flip is least stable — and it is
also, by F1's premium-scaling argument, where the penny adjustment should matter *least*, since
OPEX-day near-the-money premiums are not unusually small. Next monthly OPEX is the third Friday of
October; next quarterly is December.

**Recommendation:** proceed to step 15. Run one OPEX Friday session at `MINUTES=390 PERSIST=1`
afterwards and append the result here rather than holding the cutover for it.

---

## Carried forward

- **`INGEST_EXPIRATIONS = 3`.** Measured to move the SPX flip $29.91 (0.39% of spot) versus six
  expirations, against near-flip gate bands 0.6% and 0.8% wide. On a daily-expiry underlying the
  `min(1, DTE/5)` ramp can never reach weight 1.0 at this setting. Whether the flip *converges*
  with depth is unanswered — three attempts, all defeated by regime or by running post-close.
  `make chain-depth-sweep` exists to settle it. Production config unchanged.
- **250%+ solved IVs.** Present in 9 of 16 samples on 2026-09-21, at every chain depth, flickering
  between consecutive polls. Solver range is [0.01, 5.0] so it is not clipping. Depth-independent;
  not investigated.
- **IV-less contracts reach one published figure and not the other.** `GreeksCalculator` prices a
  failed IV solve against a 0.20 fallback without writing it back, so the contract carries a gamma
  into `_calculate_gex_by_strike` while `_gamma_exposure_profile` skips it on `sigma <= 0`.
  `net_gex` and `gamma_flip` are therefore not always computed over the same chain.
- **VIX / VXN CGIF coverage** — unconfirmed (F5).
- **F4's Exhibit A gap** — accepted; correspondence is the record.

---

## Reproducing this

```bash
# paired feed comparison, persisted to the shadow tables
INCUMBENT=thetadata CANDIDATE=thetadata_mv make feed-compare MINUTES=390 PERSIST=1
UNDERLYING='$SPXW.X' INCUMBENT=thetadata CANDIDATE=thetadata_mv make feed-compare MINUTES=390 PERSIST=1

# one fetch, sizing and coverage only
PROVIDER=thetadata_mv UNDERLYING='$NDXP.X' make feed-probe

# does the flip converge as the chain deepens (read-only)
UNDERLYING=QQQ PROVIDER=thetadata_mv DEPTHS=3,6,9,12 ROUNDS=6 make chain-depth-sweep
```

Index symbols must reach `make` through the **environment**, not as a make argument: make expands
`$S` as a variable of its own, so `make … UNDERLYING='$SPXW.X'` delivers `PXW.X`. Both tools now
reject that in the first second rather than failing slowly.

Persisted runs land in `feed_comparisons`, `option_chains_shadow` and `underlying_quotes_shadow`
(`setup/database/shadow_tables.sql`), keyed by `provider`.
