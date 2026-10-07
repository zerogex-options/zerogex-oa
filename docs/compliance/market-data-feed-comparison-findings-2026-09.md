# Feed comparison findings — ThetaData Market Value, September 2026

**Runbook step:** 14, "Check the numbers actually match".
**Question:** would a subscriber see a different number if we served ThetaData's Market Value
feed instead of TradeStation's realtime feed?
**Answer, chain levels:** no. Spot, both walls, max pain, the gamma flip and net GEX are settled;
the one metric that moves moves less than the incumbent feed already moves against itself.
**Answer, flow: yes, and materially.** Buy/sell classification was never measured at all until
2026-09-28, because the harness meant to measure it was broken and said so in language that read
like a quiet market. Once fixed, the published net imbalance came back with the **opposite sign in
12 of 30 paired samples**. The cause is a defect in ThetaData's Market Value calculation, which
they have since reproduced and acknowledged. See F9.
**Status:** step 14's evidence bar is **met for chain metrics and failed for flow**. Cutover is
deferred with no date, pending F9. The *calendar* bar was never met either — see "Where this falls
short"; that gap is deliberate and recorded, not overlooked.

**Amended 2026-09-29.** Everything above the F9 section was written 2026-09-22 and stands as
written; the verdict below was always a verdict on the chain metrics, which is all the harness
could measure at the time. Nothing in it has been retracted. What changed is that a class of
output it never covered turned out to be affected.

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

**Scope.** This verdict covers the CHAIN metrics and nothing else. Flow classification is not in
the table below and was not measured until 2026-09-28; see F9.

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

## F9 — the Market Value calculation returns crossed quotes, and Lee-Ready cannot survive them

*Added 2026-09-29. This is the finding that deferred the cutover.*

### What was measured

`option_snapshot_market_value` returns quotes with the **bid above the ask** on penny-wide
spreads. Two windows on 2026-09-28, SPY, 240 contracts per sample (three nearest expirations,
strikes within 3% of spot), one sample a minute:

| Window (ET) | Samples | Contract-quotes | Crossed | Per sample | Share |
|---|---|---|---|---|---|
| 11:25:50–11:56:46 | 30 | 7,200 | 1,640 | 38–67 | 15.8%–27.9%, mean 22.8% |
| 15:02:31–15:32:59 | 31 | 7,440 | 1,493 | 33–63 | 13.8%–26.3%, mean 20.1% |

Two different tools, so the provenance is worth stating. The morning row is `feed-compare`'s
`crossed_candidate` counter, which counts among *compared* contracts; compared was 240 in every
sample, so the base is the same 240. The afternoon row is `crossed-capture`, which counts among
all quoted contracts and writes one CSV row per crossed contract per sample.

In the afternoon window 112 distinct contracts crossed at some point, so it moves around the chain
rather than sticking to the same few. **Every one of the 1,493 was crossed by exactly one cent.**
No exceptions — that uniformity is the finding, not the count.

The incumbent returned **zero** crossed quotes, on the identical contract list, sampled 0.3–0.8s
either side of each Market Value call, in all 30 morning samples.

### Mechanism

Consistent with the penny adjustment being applied **independently to each side** of a penny-wide
spread. A bid moved up one cent and an ask moved down one cent on a one-cent spread lands crossed
by exactly one cent, which is what all 1,493 observations show and why none of them are wider.

ThetaData reproduced it on 2026-09-28 (Anthony, support), saw ~18% on their own sample, pulled the
raw NBBO for the same contracts at the same timestamps and found **none of those crossed** —
placing the defect in the Market Value calculation rather than in the underlying OPRA quote. Raised
with their team; no fix date given.

This **contradicts their statement of 2026-09-14** that the adjustment does not introduce crossed
quotes, which is recorded in `thetadata.py`'s `_endpoint()` docstring and was relied on.

### Why it blocks the cutover

`_classify_volume_chunk` decides buyer- from seller-initiated by where the trade price sits
relative to the prevailing quote (Lee-Ready, band 0.70 × half-spread). Against a crossed quote
that decision is arbitrary — there is no coherent bid/ask relationship to grade against.

Measured on the same trades, same instant, over 30 paired samples on 2026-09-28:

| | Result |
|---|---|
| contracts classified differently | 44.6%–55.8% |
| volume classified differently | 55.5%–92.5% |
| **net imbalance sign reversed** | **12 of 30 samples** |
| crossed quotes introduced | +38 to +67 per sample, incumbent always 0 |

The net imbalance is what reaches a subscriber: it is the input to `order_flow_imbalance` and
`tape_flow_bias`. A figure that reverses sign 40% of the time cannot ship unexplained. Note the
chain metrics in the same 30 samples were as clean as ever — spot, both walls and max pain
identical, net GEX 2.25% apart against 11.17% incumbent self-variance. The defect is confined to
flow, but flow and GEX come off the same chain and cannot be sourced separately.

### The harness was broken, and said nothing

`compare_flow_classification` looked the candidate's quote up **by the incumbent's vendor symbol
string**. Each feed resolves its own chain through its own `build_option_symbol`, so the incumbent
keys a contract `SPY 260928C763` and the candidate keys the identical contract
`SPY   260928C00763000`. The lookup missed on every contract of every sample.

The miss was silent: the loop skipped ahead before reaching the no-trade counter, and the report
keys off `contracts_compared` alone, so a run in which nothing *matched* printed as a run in which
nothing *traded*. Thirty samples of live SPY 0DTE carrying six-figure volume per contract reported
`no contract traded in this sample`.

Fixed 2026-09-28 (`90b84a4`): the join is on `(expiration, strike, right)` from the harness's own
metadata, unmatched contracts are counted and reported, and an empty result now names which of the
three reasons it hit. The existing tests had given both feeds the *same* made-up symbol keys and no
metadata, so symbol equality always matched and a join that could never work against two real
feeds passed all ten of them; their fixture now derives a contract identity per symbol.

**Consequence for this record: no flow number produced before 2026-09-28 is evidence.** Any such
figure quoted in a chat transcript or an earlier draft should be disregarded.

### The vendor's interim workaround, and why it is not yet adopted

ThetaData suggested classifying against the raw NBBO from `option/snapshot/quote` instead, noting
it is "on the same Standard subscription".

That answers **access tier**, not **fee treatment**, and the two are not the same question. Per F4
above, `option_snapshot_market_value` is the only quote endpoint in Exhibit A, and Exhibit A's
options line was amended to read "This adjusted bid and ask value is derived from…" — the derived
characterisation attaches to the **adjusted** quote. Raw NBBO sits outside that language, and
consuming it would reinstate precisely the OPRA exposure this migration exists to remove.

`thetadata.py`'s `_endpoint()` already refuses to fall back to the realtime endpoint on a Market
Value stage, deliberately, for exactly this reason. It has not been weakened and should not be.

Asked in writing 2026-09-28: does consuming raw NBBO purely as a classification input — never
displayed, never redistributed — carry OPRA exchange fees or redistribution obligations we do not
have today?

**ANSWERED 2026-10-07, and the answer closes the route.** ThetaData's commercial team: using the
raw NBBO as an internal input, *even never displayed and never redistributed*, counts as
**non-display use** under OPRA; that sits **outside the Market Value exemption** in the executed
agreement and would carry **a separate monthly OPRA fee**, to be quoted. Their own recommendation
was not to go down that road.

The reading recorded above was right as far as it went, and understated the problem. The concern
was that raw NBBO sits outside Exhibit A's "This adjusted bid and ask value is derived from—"
language. It does — and OPRA's non-display category means the exposure does not depend on never
showing a user the quote, which was the entire premise of the workaround. "Never displayed" is
not a defence against the fee; it is the name of the fee.

Consequences, in order of how easy each is to get wrong later:

1. `_endpoint()`'s refusal to fall back to the realtime endpoint on a Market Value stage is now
   **permanent rather than provisional**. It was written against an open question; the question
   is closed against us. Do not weaken it and do not add a flag that bypasses it.
2. `option_snapshot_quote` stays unwired. It is already reachable as the non-MV path, which is
   exactly why this needs to be written down rather than left to memory.
3. The flow decision no longer has three routes, it has two: the quote test on the repaired
   Market Value feed, if the post-fix measurement supports it, or the tick test, which reads no
   quote at all.

This is the precondition for the WORKAROUND, not the blocker itself. The blocker is the defect:
fix the crossing and the workaround, this question and the tick test all become unnecessary. Kept
straight here because it is easy to mistake the thing we can act on ourselves for the thing that
actually resolves the problem.

### Paths

| If | Then |
|---|---|
| ThetaData fixes the crossing | re-measure with `make feed-compare`. Vendor says fixed 2026-10-07; our measurement pending |
| ~~raw NBBO is fee-clean for us~~ | **DEAD 2026-10-07** — non-display use, fee-bearing. `option_snapshot_quote` stays unwired |
| raw NBBO is fee-bearing | **This is the case.** Tick test: grade against the PREVIOUS TRADE PRICE, needs no quote at all, and **shrinks** the OPRA surface relative to both alternatives |

Whichever lands, the published flow numbers change, and that has to be a deliberate and understood
change rather than a side effect of a cutover.

### Also open

The auth response carries `isProfessional: true` **and** `isRetail: true` simultaneously.
ThetaData is confirming which governs entitlements and billing. They note professional status
follows registration (FINRA, SEC, a state agency, an exchange or a futures market) and does not
change accessible data tiers. Billing and OPRA fee treatment are the reason it matters.

**By that test this account is non-professional.** ZeroGEX is a sole proprietorship and none of
those registrations apply, so `isProfessional: true` is wrong on its face. It matters because the
professional and non-professional OPRA fee schedules are not close to each other, and whether any
OPRA fee attaches at all is the open licensing question above. If a fee ever lands against a record
that reads professional, it lands at the expensive rate.

**When it started is not answerable from the record.** It was first observed 2026-09-29. Nothing
captured the auth response during the September trial, and nothing in the codebase read those
fields at all, so there is no evidence either way as to whether the flags were correct before
cutover to production and changed, or were always this way. That gap is now closed going forward
rather than retroactively: `thetadata.log_entitlement_flags()` logs both flags on every real login
(not on a forked worker's adoption, which would repeat them per underlying), distinguishes a
`false` flag from an absent one, and logs **the two flags only** — the auth response also carries
the session token and the account email, and neither belongs in a log file. Asked of ThetaData
2026-10-07 as a question, not an assertion, for exactly this reason.

**Vendor says corrected 2026-10-07**, to non-professional and commercial. UNVERIFIED on our
side.

**RESOLVED 2026-10-07 by the vendor, and the resolution is that we cannot observe it.**
ThetaData (Anthony): the professional/non-professional and retail/commercial classification
is an **account-level attribute held on their side**. It is not part of any data response,
the Python client does not surface it, and there is no endpoint for it. He corrected the
double flag on 2026-10-07; the record now reads **non-professional and commercial**.

The terminal's startup line is **not** that classification:

```
INFO: Subscriptions: Stock: PROFESSIONAL Options: PROFESSIONAL Index: PROFESSIONAL Rate: FREE
```

That `PROFESSIONAL` is the **data subscription tier** — their Pro tier — confirmed by the
vendor. Seeing it is expected and says nothing about OPRA fee treatment. `Rate: FREE` is also
not a finding: `RISK_FREE_RATE` comes from `src/config.py`, never from the vendor.

**What this cost, recorded because the mistake is repeatable.** Two separate places were
searched for a value that traverses neither. A probe was added to the provider on 2026-10-06
(`log_entitlement_flags`), reported "absent from the auth response" on its first real login,
and was then kept on the reasoning that a later client version might expose the fields. The
vendor's answer refutes that reasoning rather than deferring it, so the probe was **removed**
on 2026-10-07 along with its seven tests. An earlier revision of this paragraph also told a
reader to look in `make feed-compare` output; that was wrong and is corrected here rather
than deleted. The general lesson is the specific one: before instrumenting for a value,
establish that the value crosses the boundary being instrumented.

**Verification, such as it is.** There is nothing to read and nothing to assert. The vendor
invited the only available check: if anything downstream ever behaves as though the
professional flag were still set — an unexpected fee line, a refused entitlement, a billing
change — report it with what was seen and they will re-check the record. Until then this rests
on their written statement of 2026-10-07, which is what the correspondence file is for.
`make theta-entitlements` still shows the subscription tier and the terminal's startup block,
which are useful operationally, but it does **not** answer this question and no longer claims
to.

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
   Confirmed by re-running mid-session on 2026-09-22: the no-IV count falls to **0–8 contracts**
   (QQQ 1–8 of 480–960, SPX 0–6 of 240–960) and chain usability runs **79–97%**, the remainder
   being strikes with genuinely zero open interest. There is no IV-solve problem during RTH.
5. **IV p90 falling with chain depth** (0.356 → 0.129 across 3 → 12 expirations) — compositional.
   A three-expiration chain on a daily-expiry underlying is entirely 0–2 DTE, where the smile is
   steepest; the median barely moved. Term structure flattening, not data quality.

---

## The expiration-depth question, answered — and not the way it was framed

`INGEST_EXPIRATIONS = 3` was the one open configuration question. Two facts pointed at raising it:
a measured $29.91 shift in the SPX flip between three and six expirations (2026-09-16), and the
arithmetic that on a daily-expiry underlying the `min(1, DTE/5)` ramp can never reach weight 1.0 at
this setting. The constraint the value was originally chosen under — a TradeStation streaming
symbol cap — does not apply to a polling deployment.

A mid-session sweep on 2026-09-22 (`make chain-depth-sweep`, one fetch analysed at 3/6/9/12
expirations so every depth reads the same quotes at the same instant, six rounds each) inverts it:

| Underlying | depth 3 | depth 6 | depth 9 | depth 12 |
|---|---|---|---|---|
| QQQ | **resolved 6/6** — 734.66, 1.35% below spot | 0/6 | 0/6 | 0/6 |
| `$SPXW.X` | **resolved 3/6** — 7,562.86, 2.61% below spot | 0/6 | 0/6 | 0/6 |

**Only the three-expiration chain produces an actionable flip at all.** The deeper chains are not
disagreeing about where it sits — their crossings exist but fall outside
`GAMMA_PROFILE_MAX_FLIP_DISTANCE_PCT = 0.08`, so the resolver correctly declines them. Adding
expirations made the flip *less* resolvable, not better determined.

The mechanism is the ramp working as designed, in the opposite direction from the one assumed.
Contracts at 3–11 DTE carry weights of 0.6–1.0 against 0–0.4 for the front three, so a deeper chain
is dominated by longer-dated open interest — which on index products clusters at round strikes far
from spot. That pulls the crossing away from the money. The two settings answer different
questions: three expirations asks where *near-dated* dealer gamma flips, twelve asks where the
*whole book* flips, and only the first is actionable on any trading horizon.

The 2026-09-16 measurement stands, but it was taken on one of the rarer occasions when both depths
resolved. It is not representative.

**Conclusion: leave `INGEST_EXPIRATIONS` at 3.** Raising it would very likely make `gamma_flip`
NULL most of the time on the two underlyings tested. This closes the question rather than carrying
it forward.

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

- **`INGEST_EXPIRATIONS = 3` — leave it alone.** See "The expiration-depth question, answered"
  below. Production config unchanged, and on this evidence it should stay that way.
- **250%+ solved IVs.** Present in 9 of 16 samples on 2026-09-21, at every chain depth, flickering
  between consecutive polls. Solver range is [0.01, 5.0] so it is not clipping. Depth-independent;
  not investigated.
- **IV-less contracts reach one published figure and not the other.** `GreeksCalculator` prices a
  failed IV solve against a 0.20 fallback without writing it back, so the contract carries a gamma
  into `_calculate_gex_by_strike` while `_gamma_exposure_profile` skips it on `sigma <= 0`.
  `net_gex` and `gamma_flip` are therefore not always computed over the same chain.
- **F9's licensing question** — **RESOLVED 2026-10-07, against us.** Raw NBBO as an internal
  input is non-display use under OPRA, outside the Market Value exemption, and fee-bearing.
  `option_snapshot_quote` stays unwired and `_endpoint()`'s refusal is permanent. No longer a
  thing to wait for; a constraint to hold.
- **F9's crossed quotes** — acknowledged by the vendor, no fix date. `make crossed-capture`
  reproduces the evidence on demand; a capture returning nothing is the signal that it is fixed.
- **F9's account classification** — `isProfessional: true` and `isRetail: true` both set, which by
  ThetaData's own registration test is wrong for a sole proprietorship. Unanswered. The flags are
  now logged at every login, so the next session's startup log is the record; before 2026-10-07
  there is none.
- **VIX / VXN CGIF coverage** — resolved 2026-09-28 for the operational question, not the
  contractual one. A pre-open probe returned a live VIX at 08:44:46 ET agreeing with the incumbent
  to within a penny while VIX was moving, so extended hours are served and no per-feed override is
  needed. Whether VIX and VXN sit inside the existing CGIF coverage contractually is still
  unanswered (F5).
- **F4's Exhibit A gap** — accepted; correspondence is the record.
- **Signal components whose gates the ingested chain cannot reach.** Two turned up by accident
  during this comparison, so all six basic signals, all six registered MSI components and the three
  gamma delegates were swept deliberately for the same shape. Three found, one of them a regression
  this migration introduces: signed
  underlying volume goes NULL at cutover and takes four views with it (**blocks step 15**),
  `skew_delta` has never produced a non-abstain reading on SPX or NDX, and `gex_gradient`'s wing
  damper has never fired. See
  [signal-component-inert-gate-sweep-2026-09.md](signal-component-inert-gate-sweep-2026-09.md).

---

## Reproducing this

```bash
# paired feed comparison, persisted to the shadow tables
INCUMBENT=thetadata CANDIDATE=thetadata_mv make feed-compare MINUTES=390 PERSIST=1
UNDERLYING='$SPXW.X' INCUMBENT=thetadata CANDIDATE=thetadata_mv make feed-compare MINUTES=390 PERSIST=1

# one fetch, sizing and coverage only
PROVIDER=thetadata_mv UNDERLYING='$NDXP.X' make feed-probe

# F9: capture crossed Market Value quotes to CSV (read-only, no DB writes)
make crossed-capture MINUTES=30 OUT=~/crossed-quotes.csv

# does the flip converge as the chain deepens (read-only)
UNDERLYING=QQQ PROVIDER=thetadata_mv DEPTHS=3,6,9,12 ROUNDS=6 make chain-depth-sweep
```

Index symbols must reach `make` through the **environment**, not as a make argument: make expands
`$S` as a variable of its own, so `make … UNDERLYING='$SPXW.X'` delivers `PXW.X`. Both tools now
reject that in the first second rather than failing slowly.

Persisted runs land in `feed_comparisons`, `option_chains_shadow` and `underlying_quotes_shadow`
(`setup/database/shadow_tables.sql`), keyed by `provider`.
