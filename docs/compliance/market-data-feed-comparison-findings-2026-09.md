# Feed comparison findings — ThetaData Market Value, September 2026

**Runbook step:** 14, "Check the numbers actually match".
**Question:** would a subscriber see a different number if we served ThetaData's Market Value
feed instead of TradeStation's realtime feed?
**Answer, chain levels:** no. Spot, both walls, max pain, the gamma flip and net GEX are settled;
the one metric that moves moves less than the incumbent feed already moves against itself.
**Answer, flow: yes, and materially.** Buy/sell classification was never measured at all until
2026-09-28, because the harness meant to measure it was broken and said so in language that read
like a quiet market. Once fixed, the published net imbalance came back with the **opposite sign in
12 of 30 paired samples**. See F9.

**Amended 2026-10-07 (F10): the cause was misattributed here.** This line read "the cause is a
defect in ThetaData's Market Value calculation, which they have since reproduced and
acknowledged." There was such a defect, it was real, and it is now **fixed and confirmed** at the
September 28 scope — and the flow disagreement barely moved: 43.5% of contracts still classify
differently, and the candidate's net imbalance runs above the incumbent's in 26 of 30 samples by
a mean exceeding the signal's own magnitude. The real cause is the penny randomisation itself,
which is the product and not a defect. ThetaData's engineering now states that classification
should not run against the Market Value quote at all. **The quote test is finished, not pending.**

**Status, 2026-10-08: the method question is DECIDED and the flow bar is MET, by the tick
test.** Measured against a noise floor for the first time (F11), the tick test's cross-feed
disagreement came in *below* the floor — 4.22% mean against a 5.60% floor — and both of its
net sign flips landed in samples where the incumbent flipped against ITSELF, leaving nothing
attributable to the candidate. The quote test exceeded its floor in 28 of 28. Raw NBBO is not
needed and **no OPRA non-display fee is incurred**. What remains is engineering, not evidence:
wire the tick test into the production write path, and decide how the changed flow figure is
explained to subscribers. The *calendar* bar was never met — see "Where this falls short";
that gap is deliberate and recorded, not overlooked.

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
| ~~ThetaData fixes the crossing~~ | **Fixed and measured 2026-10-07. It did not resolve flow.** Zero crossed in 30 samples, both feeds. The quote test still disagreed on 43.5% of contracts and the candidate's net imbalance ran above the incumbent's in 26 of 30 |
| ~~raw NBBO is fee-clean for us~~ | **DEAD 2026-10-07** — non-display use, fee-bearing. `option_snapshot_quote` stays unwired |
| raw NBBO is fee-bearing | **This is the case**, and the price is unquoted. Either pay it, or take the tick test |
| the quote test on Market Value | **DEAD 2026-10-07, by vendor design.** See F10 |

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

## F10 — the Market Value quote is not a classification input, by vendor design

*Added 2026-10-07. This is the finding that ends the quote test, and it is the vendor's own.*

### What ThetaData said

Engineering, relayed by support 2026-10-07, answering the question put on 2026-10-06 about
whether the crossing fix also bounds the midpoint shift on a one-cent spread:

> The fix only stops the market value quote from reporting a crossed market when the underlying
> NBBO is not crossed. It does not bound how far the adjustment can move the midpoint of a one
> cent spread. Market Value is derived from the NBBO and its price can be shifted by design, so on
> a one cent spread the midpoint moves with it.
>
> Engineering's view is that a buy or sell classification should run against the actual NBBO
> rather than the market value quote, and the fix does not change that.

### What we measured, independently, before receiving it

30 paired samples, SPY, 2026-10-07 12:12–12:43 ET, 231 near-the-money contracts per sample,
after the crossing fix shipped:

| | Result |
|---|---|
| crossed quotes | **0 of 30 samples**, both feeds (was 38–67 per sample on 09-28) |
| contracts classified differently | mean **43.5%** (35.1–51.1%); was 44.6–55.8% |
| volume classified differently | mean 66.4% (43.3–81.6%) |
| net imbalance sign reversed | 5 of 30; was 12 of 30 |
| **candidate net ABOVE incumbent** | **26 of 30 samples** |
| mean signed difference | **+1,223,415** |
| mean \|incumbent net\| for scale | 1,041,127 |

The signed mean is 90% of the mean absolute difference, so the differences nearly all point one
way, and it exceeds the typical magnitude of the signal itself. That is bias, not noise.

**The fixed poll order cannot explain it.** `compare_flow_classification` takes `volume` and
`last` from the INCUMBENT for both sides and varies only the quote, so the feed polled second
cannot contribute extra tape. The only differing input is the Market Value quote. The vendor's
statement and this measurement are the same fact arrived at from two directions.

### Consequence

The quote test on the Market Value feed is **finished**, not pending further measurement. Two
routes remain and neither is free of cost:

1. **Raw NBBO + Lee-Ready.** Reproduces today's published number exactly. Non-display use under
   OPRA per F9, fee-bearing, price unquoted as of this writing. The vendor's commercial team will
   quote it.
2. **Tick test.** Grades against the previous trade price, reads no quote, needs no licensing
   answer, and shrinks the OPRA surface rather than expanding it. Changes what the published flow
   figure means. Its cross-feed disagreement (5.89% mean, 5 of 29 sign reversals on 2026-10-07)
   has **no noise floor under it yet** — unlike the quote test it reads each feed's own trade
   sequence, and the feeds are polled 0.3–0.8s apart. `--self-control` was built for exactly this
   and the measurement is outstanding.

**That measurement is now the decision.** If the tick test clears its floor, the cutover proceeds
with no OPRA fee and a changed flow figure. If it does not, the only remaining route is the paid
one, and the question becomes what it costs.

### Note on the chain metrics

None of this touches them. Spot, both walls, max pain and the gamma flip were identical or within
self-variance in the same 30 samples, as in all 116 before them. Market Value remains sound for
everything the chain comparison covers; it is buy/sell classification, and only that, which it
cannot serve.

---

## F11 — the tick test clears its noise floor; the quote test does not

*Added 2026-10-08. This is the finding that decided the method, and with it the cutover.*

### Why a floor was needed at all

Every flow number before this one was reported without knowing what the measurement's own noise
was. The chain table has carried a self-variance column since it was written, and the line it
prints — *"the incumbent moved further from ITSELF between samples than the two feeds ever
differed"* — is the only reason those numbers are believable. Flow had no such column, and on
2026-10-07 that gap stopped the decision: the tick test's 5.89% cross-feed disagreement and 5-of-29
sign reversals could not be separated from the 0.3–0.8s gap between the two polls, because the tick
test reads each feed's OWN trade sequence and a print landing between polls flips a direction that
then carries forward.

`--self-control` takes a third snapshot, of the incumbent again, and runs both comparators over the
two incumbent samples. It is sampled THIRD, so the cross-feed pair keeps the short gap every prior
run was measured at and the floor inherits the longer one — a handicap that overstates the floor,
which is the direction that cannot manufacture a finding.

### What was measured

28 paired samples, SPY, 2026-10-08 10:02–10:32 ET, 240 contracts per sample, three nearest
expirations, strikes within 3% of spot. Cross-feed gap 0.28–0.74s; self gap 2.73–3.39s.

| | tick test | quote test |
|---|---|---|
| noise floor (incumbent vs itself), mean | **5.60%** | 8.18% |
| cross-feed disagreement, mean | **4.22%** | **39.73%** |
| samples where cross EXCEEDED the floor | **3 of 27** | **28 of 28** |
| largest excess over the floor | **+0.82 pp** | +40.71 pp |
| cross-feed NET sign flips | 2 of 27 | 14 of 28 |
| …of those, flips the floor ALSO flipped on | **2** | 7 |
| …leaving, attributable to the candidate | **0** | 7 |

**The tick test's cross-feed disagreement is smaller than the floor beneath it.** The harness
cannot distinguish TradeStation from ThetaData under the tick test because it cannot reliably
distinguish TradeStation from itself. The three samples that exceeded the floor did so by at most
0.82 percentage points. Both cross-feed sign flips occurred in samples where the incumbent flipped
against itself in the same sample, leaving **nothing** that could be the candidate.

**The quote test exceeded its floor in every sample**, by roughly five times. Independently:
the candidate's net imbalance was higher than the incumbent's in **26 of 28**, and **positive in
all 28** while the incumbent's swung both ways. That is the same one-directional bias measured on
2026-10-07 (26 of 30, mean +1,223,415), and it agrees with ThetaData engineering's own statement
in F10 that classification should not run against the Market Value quote.

### Decision

**Adopt the tick test.** It needs no quote, so the Market Value randomisation cannot reach it;
it needs no raw NBBO, so **no OPRA non-display fee is incurred** (F9); and it is now the only
classification method on record that has been measured against its own noise and passed.

The published flow figure will change. That is a product decision, not an evidence gap: switching
method on the SAME feed relabels 30–66% of contracts per sample. It has to be a deliberate,
explained change rather than a side effect of a cutover.

### A separate finding about our own metric, not the vendor's

The quote test's net imbalance flipped sign against the **same feed, same trades**, in 12 of 28
samples — the only variable being a quote 2.8s older. A single snapshot's net imbalance is a
knife-edge quantity. The production figure aggregates differently and this does not transfer to it
directly, but it should be understood before any weight is put on a one-sample reading, and it is
the reason the quote test's 14 cross-feed flips are only half a finding on their own. The 28-of-28
floor exceedance and the 28-of-28 positive bias are what settle that question.

### Methodology note

The per-sample figures above were extracted with a throwaway script that first collected cross-feed
rows and self rows into two parallel lists. **That was wrong and briefly produced a false result**
("0 of 2 overlap"): the first sample of a run emits a self row but no cross row, so the lists were
off by one and every pair compared one sample's result against another sample's floor. The
corrected version splits the output on the sample header and pairs rows only within a block.
Anything comparing a measurement to its control must pair them inside the sample, never by position
across two collections.

---

## F12 — at bucket level the method change is small on ETFs and large on indices

*Added 2026-10-09. The measurement that resolved F11's open question, and
split the answer by symbol.*

> **CORRECTED AND RE-MEASURED 2026-10-12. The figures below are the full
> session; the originals were the morning only.** The `shadow-compare-flow`
> target converted the ET cash session to UTC with
> `TZ=America/New_York date -d "$D 09:30" -u`. GNU date's `-u` sets `TZ=UTC0`
> for the whole operation, the parse of `-d` included, so the prefix was ignored
> and 09:30 ET came out as 09:30Z. The window queried was 09:30Z–16:00Z —
> 05:30–12:00 ET, the right six-and-a-half-hour duration in the wrong place.
> Data starts at 09:30 ET, so what actually bit was **09:30–12:00 ET**: 150
> one-minute buckets per symbol out of 390.
>
> It read as healthy coverage because nothing stated what complete would be:
> the originally recorded "600 of 610 buckets" is 4 symbols × 150, not 4 × 390.
> A span check would not have caught it either — the bad window really was 390
> minutes long.
>
> The rows were already in the rehearsal database, so the fixed target
> re-measured this day over the full session with no new market day needed.
> **Coverage is in fact complete: 1,560 of 1,560 buckets, zero production-only,
> zero rehearsal-only** — better than first recorded.
>
> What the morning window got wrong, for the record: it put all-symbol sign
> agreement at 71.2% against the true 70.8%, and the all-symbol share of flipped
> flow at 14.7% against the true 16.5%. The direction of every conclusion holds.
> Its largest distortion was on SPY, which matters for F14.
>
> Fixed in the target, with the computed bound converted back to ET and checked
> before any query runs, and the report now printing the window span alongside
> the bucket count.

### What was open

F11 settled the FEED and left the METHOD. Measured on 230-contract snapshots,
switching the quote test for the tick test relabelled 47.6% of contracts and
flipped the net imbalance sign 16 of 27 times — 41% sign agreement. A snapshot
is not what a subscriber sees; the site serves one-minute buckets aggregated
across the chain, and nothing could say whether aggregation absorbed that.

`--self-control` could not answer it either: it bounds the harness, not the
product. So `FLOW_CLASSIFIER=quote|tick` was added to the engine and a shadow
session run with `FLOW_CLASSIFIER=tick` on the SAME feed as production, so the
only variable was the method, with `make shadow-compare-flow` diffing the two
databases at the aggregation the site publishes.

**A rule was written down before the numbers existed**: 80%+ sign agreement
means bucketing absorbs the change and the cutover is cosmetic for flow; near
41% means it is as disruptive as snapshots suggested; anything between is
judgement and must be called as such rather than forced.

### What was measured

TradeStation on both sides, `FLOW_CLASSIFIER=quote` in production and `tick`
in the rehearsal DB. 2026-10-09, 09:30–16:00 ET. **Complete coverage: 1,560 of
1,560 buckets across 4 symbols, nothing missing on either side.**

| | all symbols | SPY |
|---|---|---|
| net imbalance sign agrees | **70.8%** (1104/1560) | **76.2%** (297/390) |
| rehearsal net higher | 723 of 1560 (46.3%) | — |
| mean \|difference\| vs mean \|production net\| | 1,100 / 1,434 = 77% | — |
| **share of session imbalance in flipped buckets** | **16.5%** | **16.5%** |

**It landed between, and was called between.** Bucketing roughly halves the
disagreement, from 59% to 29%, but does not remove it.

**No systematic bias.** The rehearsal reads higher 46.3% of the time, which is
close enough to a coin flip at this sample size. Set against the quote test's
26-of-28 one-directional feed bias (F10/F11), the tick test is differently
NOISY, not differently OPINIONATED —
the single most reassuring number in this finding.

**The flip RATE overstated the impact.** 232 of 456 flips landed on below-median
buckets where the site read "roughly balanced" either way, so 83.5% of the
session's total imbalance sits in buckets both methods agree on.

### The finding that actually matters: it splits by symbol

Full session, 390 buckets each.

| symbol | sign agrees | flips near-zero / typical / large | **share of flow flipped** |
|---|---|---|---|
| QQQ | 76.7% | 37 / 43 / **11** | **13.1%** |
| SPY | 76.2% | 32 / 43 / **18** | **16.5%** |
| SPX | 66.4% | 53 / 49 / **29** | **22.6%** |
| **NDX** | **63.8%** | 45 / 57 / **39** | **29.1%** |

A spread of better than two to one, and the two ETFs sit clear of the two cash
indices. The prediction was that thinner symbols would fare worse; the split is
cleaner than liquidity alone, since QQQ edges SPY.

**NDX is not shippable on the tick test as it stands.** Close to a third of its
published flow would point the other way, and 39 of its 141 flips land on
above-median buckets — those are not cosmetic reversals on quiet minutes but
different claims about the session. (The morning-only window put this at 36.5%
and 18 of 57; the full session is less severe but the judgement is unchanged.)

### A cause with an obvious test, and the fix

`_classify_volume_chunk_tick` routes a zero tick — a print at the same price as
the last one — to whichever direction it last saw, **with no expiry**. The quote
path has carried `FLOW_CLASSIFY_PRIOR_TICK_MAX_AGE_SECONDS` (10s) since it was
written for the symmetric reason: a stale prior tick is not a valid proxy. The
tick path had no equivalent.

NDX contracts print rarely and in size. A flat print inherits a direction that
may have been set minutes ago, and on a high-premium contract one inherited
call moves a lot of imbalance. That fits all three NDX symptoms at once: worst
agreement, most large flips, highest flow share.

`FLOW_TICK_MAX_CARRY_SECONDS` expires the carry, routing a stale zero tick to
mid instead of inheriting. Scoped to the carry alone — a stale up- or downtick
still classifies, because it compares two real prices and, unlike the quote
test, has no contemporaneous fallback to reach for.

**It defaults to 0, disabled.** The table above was measured without it, and
session 2 (ThetaData + tick) has to stay comparable with session 1. Changing
two things at once teaches nothing. **This is a hypothesis with an obvious
test, not a measured cause**, and it must not be written up as the latter
until a session with the guard on is compared against one with it off.

### What this does not cover

One day, no OPEX. The published SIGNAL was not compared — `shadow-run` runs
ingestion without analytics, so this is the input to `order_flow_imbalance` and
`tape_flow_bias`. The transform is deterministic, so matching inputs mean
matching outputs, but if a later session differs materially the next step is
pointing analytics at the rehearsal DB.

---

## F13 — NDX cannot be ingested on ThetaData at all: the index level is unentitled

Not a data-quality finding. The Nasdaq index level is refused outright, so there is no
NDX spot, and without spot there is no NDX chain. As of 2026-10-12 on the Market Value
feed, NDX is absent
rather than degraded.

### What happened

Shadow session 2 started 2026-10-12 at about 08:55 ET on `thetadata_mv` with
`FLOW_CLASSIFIER=tick`. The NDX worker failed to initialize, repeatedly, with:

```
INVALID_ARGUMENT: Requesting data for NASDAQ GIDs symbols: [NDX] without proper permissions
```

raised out of `index_snapshot_market_value` — the endpoint `_endpoint()` selects for index
spot on this feed (`src/ingestion/providers/thetadata.py:1434`). It surfaces through
`StreamManager._fetch_underlying_bar`, whose `except Exception` logs it at ERROR and returns
`None` (`src/ingestion/stream_manager.py:1595`). `_get_underlying_price` then returns `None`,
`initialize()` returns `False` (`stream_manager.py:2134`), and `run_streaming()` takes its
retry path (`main_engine.py:2454`).

So the worker never dies and the supervisor never sees a crash. It loops on failed
initialization behind `_worker_restart_delay`, 5s escalating to the 120s cap. SPY, QQQ and
SPX were unaffected and ran the full session.

Scope is the **account's NASDAQ GIDS entitlement**, not the Market Value product and not the
OPRA options data. SPX is CBOE and the ETFs are not index products, which is why only NDX
fails. The NDX *options* chain was never reached, so whether it is entitled is still unknown —
spot and chain are separately entitled on this vendor, and the failure stops at spot.

### Log volume, since this was the live concern

About 0.6 MB for the session: 229 retries at roughly 2,679 bytes each, including the
traceback `exc_info=True` attaches. The 5s→120s backoff is doing exactly the job it was
added for, and its own comment names this case — "a symbol the vendor does not serve" —
alongside the run that wrote 73 million lines and 7 GB in seven minutes. Without it this
would have been a disk-exhaustion event on the box that also runs the live service. `/` had
17 GB free and `/var/log` is a separate 10 GB volume at 55%.

### Why the comparison runs could not surface it — and what did

`make feed-compare` cannot find it, by design. The candidate is sampled with
`spot_hint=incumbent.spot` (`src/tools/feed_compare.py:1491`), and `sample_provider` takes
that hint in place of calling the feed (`feed_compare.py:565`). That is a **correct** control,
not a defect: letting each feed pick its own spot would let a small price difference select
different contracts, and the divergence would be an artefact of the harness. The cost is that
a comparison never asks the candidate for an index level.

`make feed-probe` does ask — it passes no hint and runs `require_spot=False`. And it found
this, on **2026-09-25**, seventeen days before it stopped an ingestion run. The escape
hatch added in that commit
is commented:

> A probe reports what IS reachable. Stopping at a failed spot call left "$NDXP.X" reading as
> a total outage when only the Nasdaq GIDS index level was unentitled and the OPRA chain
> underneath it was never asked for.

The signal was absorbed as harness robustness and never carried anywhere: no vendor question,
no cutover gate, no entry here. **The gap was in what happened to the finding, not in the
coverage.** A first pass at this finding, on the morning of 2026-10-12, said three weeks of
testing had never exercised the endpoint. That is true of `feed-compare` and wrong about
`feed-probe`, which exercised it and reported it; corrected here.

The corrected lesson: per-symbol reachability is a **probe** question, and a comparison
harness is structurally unable to answer it. The gate before any cutover has to be one
`feed-probe` per production symbol on the candidate feed, read for a spot error — not a
comparison run, however long.

### Consequence for cutover

NDX is blocked on ThetaData until the entitlement is added. This stacks on F12, where NDX was
already the worst symbol on the method change (63.8% sign agreement, 29.1% of session
imbalance in flipped buckets over the full session, judged not shippable on the tick test
as it stands). Two independent blockers on the same symbol.

It does not change the position on SPY, QQQ or SPX.

### Status

Raised with ThetaData 2026-10-12; Anthony confirmed he is looking into it. Open, alongside the
credit outstanding from 28 September.

A standing re-check, read for `spot_error` rather than pass/fail:

```bash
PROVIDER=thetadata_mv UNDERLYING='$NDXP.X' make feed-probe
```

**Not while a shadow run is up.** The account holds one MDDS connection and a second login
kicks the first, so probing the candidate feed during a rehearsal on that same feed ends the
rehearsal. This is also why the cutover gate below cannot be a same-day check: the
per-symbol probes have to run on a day with no rehearsal holding the connection, or before
one starts.

---

## F14 — a cross-day feed comparison, superseded by F15

*Added 2026-10-12, revised the same evening on full-session data. Arm C:
ThetaData Market Value + tick test, against production.*

> **SUPERSEDED 2026-10-13 by F15, which measured both arms on the same day.**
> This finding's SPY result — ThetaData 23.7% of flipped flow against the
> TradeStation arm's 16.5% — compared two different market days, and F15 shows
> that this measurement swings **+7.9 points between two days on the same feed**.
> The +7.2pp gap recorded here is inside that band and is not evidence of
> anything about the vendor. Read F15 for the answer; this finding is kept for
> the record of how it was reached.
>
> **REVISED TWICE on 2026-10-12, both times downward in confidence about my own
> reading.** First version: written on the truncated morning window (see the
> correction at the top of F12), it reported SPY's share of flipped flow going
> 15.8% to 34.5% — a doubling, called a possible crisis. On the full session the
> move is **16.5% to 23.7%**; the morning window had overstated that delta by a
> factor of 2.6. Second version: it named polling density as the leading cause,
> on row counts taken from an unbounded window. Measured inside the cash session
> the density is identical, and the hypothesis is refuted — see below. What
> remains is the feed or the day, with no variance estimate to tell them apart.

### What was open

F12 isolated the METHOD (TradeStation on both sides, quote vs tick). F11 said
the FEED contributes nothing detectable under the tick test: cross-feed
disagreement 4.22% against a 5.60% self-variance floor, measured on snapshots.
Arm C changes both at once, which is what a cutover actually does, and tests
whether F11's snapshot result holds at the aggregation the site publishes.

**Pre-registered**, from F12: if arm C lands close to arm B, the feed adds
nothing at bucket level and F11 is confirmed. If it is markedly worse, that is
new information about the feed and must not be explained away.

### Coverage

1,170 buckets in both, 390 production-only, zero rehearsal-only. The 390 is
exactly NDX's full session: production has it, the rehearsal cannot (F13), and
the per-symbol table reads `NDX (no overlapping buckets)`. Net of NDX, coverage
is complete — 3 symbols × 390, nothing missing on either side. **No integration
failure**: this was the first time ThetaData went through the production write
path, and it held for a whole session.

Option volume agrees to a fraction of a percent: SPY 7,586,256 against
production's 7,580,891 (+0.07%); QQQ 5,313,683 against 5,324,166 (−0.20%).

### Read the per-symbol rows, not the headline

The all-symbol figures are **not comparable between arms**: arm B covers four
symbols and arm C three, and NDX is arm B's worst row. The three shared symbols
are the comparison.

| symbol | arm B — feed held | arm C — feed changed | change |
|---|---|---|---|
| QQQ sign agrees | 76.7% | 74.4% | −2.3pp |
| QQQ share of flow flipped | 13.1% | 16.5% | **+3.4pp** |
| SPX sign agrees | 66.4% | 66.4% | **0.0pp** |
| SPX share of flow flipped | 22.6% | 25.5% | **+2.9pp** |
| SPY sign agrees | 76.2% | 69.5% | −6.7pp |
| SPY share of flow flipped | 16.5% | 23.7% | **+7.2pp** |

All-symbol, for the record and not for comparison: arm B 70.8% / 16.5% over
four symbols, arm C 70.1% / 21.6% over three.

**The feed costs something, and it is modest.** Three to seven points of flipped
flow per symbol, and overall sign agreement barely moves (70.8% to 70.1%). F11
said the feed was undetectable on snapshots; at bucket level over a full session
it is detectable but small, and nowhere near the size of the method change
itself.

**SPY pays most of it** — +7.2pp, roughly double QQQ's and SPX's. That is still
the wrong shape for the carried-direction story: SPY is the most liquid symbol
here and prints most often, so it should be the LEAST exposed to a tick test,
and F12's own prediction was that thinner symbols fare worse.

**Two further signals, both pointing the same way.** Arm C's mean absolute
difference runs at 92.7% of the mean absolute production reading, against arm
B's 76.7% — the typical disagreement is now nearly the size of the typical
figure itself. And the rehearsal reads higher in 41.1% of buckets against arm
B's 46.3%, a drift away from the coin flip. Both are consistent with the feed
adding noise rather than an opinion, but one session per arm cannot separate
that from day-to-day variation, and these buckets are autocorrelated within a
symbol, so no significance is claimed.

### The polling-density confound — RAISED AND REFUTED, same evening

The first version of this finding named polling density as the leading
explanation: the rehearsal appeared to have written 8.7% fewer SPY rows and 9.5%
fewer QQQ rows than production for near-identical volume, and since the tick
test reads a SEQUENCE of trade prices, sparser polling would leave more zero
ticks inheriting a carried direction — a property of the run, not the feed.

**Those row counts were wrong, and wrong in a way worth recording.** They came
from `make shadow-compare`, whose coverage query has **no upper time bound and
starts at midnight ET**. Production runs 24×5, so it holds overnight and
after-hours rows; the rehearsal started at 08:38. The entire 8.7% was rows
outside the cash session. The same window's own output shows it — production's
SPY `first_et` reads `00:00:00` against the rehearsal's `09:30:00` — and it was
quoted anyway.

Measured properly, inside the 09:30–16:00 ET window the comparison actually
uses, polls per contract are:

| | arm B (2026-10-09) | arm C (2026-10-12) |
|---|---|---|
| SPY, rehearsal vs production | 358.4 vs 358.2 (+0.1%) | 364.2 vs 367.0 (−0.8%) |
| QQQ, rehearsal vs production | 370.6 vs 370.6 (0.0%) | 377.6 vs 377.0 (+0.2%) |
| SPX, rehearsal vs production | 340.7 vs 336.8 (+1.2%) | 361.1 vs 361.1 (0.0%) |
| NDX, rehearsal vs production | 325.2 vs 322.3 (+0.9%) | — |

**Every symbol on both days polled within 1.2% of production.** There is no
density difference to explain anything. Two further points finish it off: arm C
polled SPY 1.6% MORE densely than arm B (364.2 against 358.4), which is the
wrong direction; and the supporting correlation claimed in the first version —
"SPX was the one symbol that polled at production density and the one symbol
whose agreement did not move" — was vacuous, because all three symbols polled at
production density. There was no variation to correlate with.

### What that leaves, and the measurement that is actually missing

With density gone, SPY's +7.2pp is the feed **or the day**, and the two cannot
be separated from what has been run. Arm B is 2026-10-09 and arm C is
2026-10-12: different market days, one measurement each.

**There is no day-to-day variance estimate for this bucket-level comparison**,
so a 7-point difference between two single-day arms is not yet interpretable in
either direction. That is the same gap F11 was built to close for snapshots —
`--self-control` exists precisely because a cross-feed number means nothing
without a floor under it — and this finding walked into it again one level up.
It is recorded here rather than resolved because resolving it needs a market
day, not an argument.

Weak evidence against a large day effect, not enough to lean on: SPX's sign
agreement was **66.4% on both days**, on two different feeds. Something that
moved SPY by 7 points day-to-day might be expected to move SPX too.

**The clean design, and it needs one day rather than two.** Run both arms
simultaneously against the same market, into two rehearsal databases, both on
the tick test:

```bash
make shadow-pgpass SHADOW_DB=zerogex_shadow_ts
make shadow-create SHADOW_DB=zerogex_shadow_ts

# TradeStation arm (method only) -- repeats arm B. SPY ONLY, see below.
SHADOW_LOG=~/shadow-ts-$(date +%F).log make shadow-run \
    SHADOW_DB=zerogex_shadow_ts SHADOW_PROVIDER=tradestation \
    FLOW_CLASSIFIER=tick SHADOW_UNDERLYINGS=SPY

# ThetaData arm (method + feed) -- repeats arm C
SHADOW_LOG=~/shadow-td-$(date +%F).log make shadow-run \
    SHADOW_DB=zerogex_shadow FLOW_CLASSIFIER=tick
```

Then `make shadow-compare-flow SHADOW_DB=<db> UNDERLYING=SPY` once per
rehearsal database. The difference between the two results is the feed, with the
day held fixed. `make shadow-stop SHADOW_DB=<db>` stops one arm without touching
the other, since it selects on `DB_NAME`.

**Why the TradeStation arm is SPY only.** TradeStation caps concurrent streams
per account at nominally 10, and production alone approaches it — roughly two
chunked option streams per underlying plus the underlying bar stream plus VIX
(`docs/design/api-gateway-architecture.md`). Cap exhaustion is **not**
header-observable on a standard key; its only fingerprint is a stream that
connects 200 OK and then dies inside 30 seconds, which is the 2026-06 incident.
Arm B ran production plus one four-symbol TradeStation engine and survived, but
this adds a THIRD TradeStation consumer, because the ThetaData arm's VIX / VXN /
futures ingesters still call TradeStation directly. A one-symbol arm costs two
to three streams instead of roughly ten, and the open question is SPY anyway.

The risk being managed is not a failed rehearsal. It is production losing a
stream it cannot reopen because a rehearsal is holding the slot.

**`INGEST_UNDERLYINGS=SPY make shadow-run` does not work, and does not say so.**
`shadow-run` re-reads `.env` in the shell and `export`s every key
unconditionally, so a value already in the environment is overwritten: the run
comes up on all four symbols while the banner's own "Underlyings:" line prints
the full list and nobody looks twice. `SHADOW_UNDERLYINGS` is applied after that
loader, which is the only place it can win, and the banner marks the list as
narrowed whenever it is set.

### Status

- **Nothing here disqualifies the feed**, and nothing here clears it either. A
  few points of flipped flow per symbol with overall sign agreement unchanged is
  a small effect, but it is measured across two different days with no estimate
  of how much this figure moves day-to-day, so its size is not established.
- **SPY: CLOSED 2026-10-13 by F15.** The +7.2pp was the day, not the feed. A
  same-day comparison puts the feed's cost at −0.4pp on the decision number.
- QQQ +3.4pp, SPX +2.9pp: small, and SPX's sign agreement did not move at all.
- NDX: no data (F13).
- First full-session end-to-end run of ThetaData through the production write
  path: clean. That much is settled.
- **Blocking the flow half of the cutover decision: one same-day two-arm
  session.** Not a vendor question and not an argument — a market day.

### What this does not cover

One session per arm. No OPEX. The published signal, as in F12 — this is the
input to `order_flow_imbalance` and `tape_flow_bias`, not the rendered figure.

---

## F15 — the feed is cleared on flow for SPY; the day was the variable all along

*Added 2026-10-13. Both arms run simultaneously against the same market, which
is what F14 said was missing. It resolves F14's open SPY question against my own
reading of it.*

### Design

Two rehearsal databases, same session, same classifier, same symbol, both
compared against production:

| arm | database | feed | classifier | symbols |
|---|---|---|---|---|
| method only | `zerogex_shadow_ts` | TradeStation | tick | SPY |
| method + feed | `zerogex_shadow` | ThetaData MV | tick | SPY, SPX, QQQ, NDX |

Production runs the quote test on TradeStation in both comparisons, so the
baseline is identical — confirmed in the output, where both runs report the same
`mean |production net|` of 3,239. The TradeStation arm was narrowed to SPY with
`SHADOW_UNDERLYINGS` to keep it clear of TradeStation's concurrent-stream cap;
see F14's plan block. Production stayed healthy throughout: 12 of 12 streams
fresh at 09:36 and 09:50, zero cap warnings in either log, zero session kicks.

**Coverage is complete on both: 390 of 390 buckets, nothing missing either
side.**

### The feed costs nothing on the number that decides this

| | TradeStation + tick | ThetaData + tick | feed cost |
|---|---|---|---|
| net imbalance sign agrees | 70.8% (276/390) | 67.7% (264/390) | **−3.1pp** |
| **share of session imbalance flipped** | **24.4%** | **24.0%** | **−0.4pp** |
| rehearsal net higher | 50.0% (195/390) | 50.8% (198/390) | — |
| mean \|difference\| vs scale | 2,717 / 3,239 = 83.9% | 3,022 / 3,239 = 93.3% | — |
| flips: near-zero / typical / large | 45 / 38 / **31** | 55 / 40 / **31** | — |

**The decision number is 24.0% against 24.4% — a 0.4 point difference, in
ThetaData's favour.** On the figure that answers "would a subscriber notice",
the feed is indistinguishable from the incumbent.

Sign agreement is 3.1 points worse on ThetaData, and the flip bands say exactly
where that goes: 12 extra flips, of which **ten land on near-zero buckets and
two on typical ones. Large-bucket flips are identical, 31 against 31.** The
extra disagreement falls entirely on minutes where the site read "roughly
balanced" either way.

Both arms are unbiased — 50.0% and 50.8% of buckets reading higher is a coin
flip either way.

The one place the feed is visible: its typical disagreement runs at 93.3% of the
typical production reading against the incumbent arm's 83.9%. The Market Value
randomisation adds noise, as F10 and F11 imply it should. It does not reach the
published decision.

### The day-to-day variance estimate, which is what F14 was actually missing

The TradeStation arm is a repeat of F12's arm B on a different day, with feed,
method and symbol all held constant. That is the first measurement of how much
this figure moves on its own:

| TradeStation + tick vs production, SPY | sign agrees | share of flow flipped |
|---|---|---|
| Friday 2026-10-09 | 76.2% | 16.5% |
| Tuesday 2026-10-13 | 70.8% | **24.4%** |
| **day-to-day swing** | **−5.4pp** | **+7.9pp** |

**That settles F14.** The gap I flagged on Monday as a possible feed problem —
ThetaData 23.7% against Friday's TradeStation 16.5%, +7.2pp — is *smaller* than
the +7.9pp this measurement moves between two days on the same feed. It was the
day, not the vendor.

Both of today's arms also sit near 24% against Friday's 16.5%, which is the same
statement from the other side: Tuesday was a harder day for the tick test than
Friday, equally for both feeds.

### What this closes, and what it does not

**Closed.** F14's open SPY question. The feed does not meaningfully change
published flow on SPY. F11's snapshot finding holds at the aggregation the site
publishes, measured the only way that can show it.

**Also closed, as a methodological matter.** A single day's bucket-level figure
carries roughly ±8 points of noise on the share-of-flow number. No future
finding should read a difference of that size between two single-day runs as
anything at all. F12, F14 and this finding should all be read with that band
around them.

**Not closed.** The METHOD still costs what it costs: switching the quote test
for the tick test flips the sign on 16–24% of SPY's session imbalance depending
on the day, and that is a deliberate product change, not a measurement artefact.
Nothing here softens it.

**Not covered.** QQQ and SPX were not in the TradeStation arm today — it was
narrowed to SPY for the stream cap — so the feed is cleared for **SPY
specifically**. Their only evidence remains cross-day, which this finding has
just shown is worth about ±8 points. NDX remains blocked outright (F13).

**Still one day.** A same-day comparison removes the day as a confound; it does
not make one session into many.

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
- **The `shadow-compare-flow` window bug — CLOSED 2026-10-12, both arms
  re-measured.** It had both F12 and F14 covering 09:30–12:00 ET only. Fixed,
  guarded, and both days re-measured over the full session from rows already in
  the rehearsal database. Coverage turned out complete on both. The morning
  window had overstated SPY's arm-B-to-arm-C degradation by 2.6x, which is the
  one conclusion it materially changed.
- **SPY's share-of-flow under the feed change — CLOSED 2026-10-13 (F15).** Both
  arms were run simultaneously against the same market: the feed's cost on the
  share-of-flow decision number is **−0.4pp** (24.0% ThetaData against 24.4%
  TradeStation), and the 12 extra sign flips land entirely on near-zero and
  typical buckets with large-bucket flips identical at 31 apiece. The +7.2pp
  that prompted this entry was day-to-day variation: the same measurement moves
  +7.9pp between two days with the feed held constant. **The feed is cleared on
  flow for SPY.** QQQ and SPX still have only cross-day evidence, and NDX
  remains blocked (F13). Superseded detail below, kept for the record:
- ~~**SPY's share-of-flow under the feed change (F14) — OPEN, and the blocker is a
  market day.**~~ 16.5% to 23.7% between arm B and arm C, sign agreement down 6.7
  points, against +3.4pp on QQQ and +2.9pp on SPX. Polling density was raised as
  the cause and is **refuted**: inside the cash session every symbol on both days
  polled within 1.2% of production, and arm C polled SPY 1.6% more densely than
  arm B. What remains is the feed or the day, and the two arms are different days
  with one measurement each, so there is no day-to-day variance estimate to tell
  them apart. **Next step: one session running both arms simultaneously into two
  rehearsal databases** (commands in F14), which holds the day fixed and fits
  inside the single ThetaData connection.
- **A single day's bucket-level figure carries about ±8 points of noise.**
  Measured 2026-10-13 (F15): TradeStation + tick vs production on SPY moved from
  16.5% to 24.4% of flipped flow between 10-09 and 10-13, with feed, method and
  symbol all held constant. No finding should read a difference of that size
  between two single-day runs as a result. F12, F14 and F15 all carry this band.
- **Row counts must come from a bounded window.** `make shadow-compare` starts at
  midnight ET with no upper bound, and production runs 24×5 while a rehearsal
  starts in the morning, so its row counts are not comparable between the two and
  must never be read as polling density. That mistake produced and then had to
  retract F14's first explanation. The cash-session query is in F14.
- **NDX is unentitled on ThetaData (F13) — OPEN, raised 2026-10-12.** The NASDAQ GIDS index
  level is refused, so there is no NDX spot and therefore no NDX chain. Absent, not degraded.
  The NDX options chain has never been reached, so its entitlement is still unknown. Stacks on
  F12, which already judged NDX not shippable on the tick test. SPY / QQQ / SPX unaffected.
- **Per-symbol reachability is a probe question, and belongs in the cutover gate.** `feed-compare`
  passes `spot_hint=incumbent.spot` as a deliberate control, so it can never ask the candidate for
  an index level. `feed-probe` can, and did on 2026-09-25 — seventeen days before F13 surfaced in
  ingestion — and the finding went into a harness comment instead of into this document. Before
  cutover: one `feed-probe` per production symbol on the candidate feed, read for `spot_error`
  — on a day with no rehearsal running, since the single MDDS connection makes the two
  mutually exclusive.
- **F9's licensing question** — **RESOLVED 2026-10-07, against us.** Raw NBBO as an internal
  input is non-display use under OPRA, outside the Market Value exemption, and fee-bearing.
  `option_snapshot_quote` stays unwired and `_endpoint()`'s refusal is permanent. No longer a
  thing to wait for; a constraint to hold.
- **F9's crossed quotes** — **CLOSED 2026-10-07.** Vendor shipped a fix; confirmed from our
  side at the September 28 scope, zero crossed in 30 samples on both feeds. `make crossed-capture`
  still reproduces on demand if it ever regresses.
- **F9's midpoint question** — **ANSWERED 2026-10-07, against us.** See F10: the fix bounds
  crossing only, not the midpoint shift, and the vendor's own engineering says classification
  should not run against the Market Value quote at all.
- **F9's account classification** — **CLOSED 2026-10-08, and VERIFIED from our side.** The
  vendor said on 2026-10-07 that the flags come back through no data response and the client
  cannot surface them. That is **not correct**: the `thetadata` library logs the entire auth
  response at INFO on every login, flags included. The 10:02:28 ET login on 2026-10-08 printed
  `"isProfessional": false, "isRetail": false` — non-professional and commercial, exactly the
  record they said they had set. Their correction is confirmed by their own client. To re-check,
  grep any run's output for `isProfessional`; no tooling is needed and none should be added
  (the removed probe failed because the fields are not client ATTRIBUTES, only log content).
- **VIX / VXN CGIF coverage** — resolved 2026-09-28 for the operational question, not the
  contractual one. A pre-open probe returned a live VIX at 08:44:46 ET agreeing with the incumbent
  to within a penny while VIX was moving, so extended hours are served and no per-feed override is
  needed. Whether VIX and VXN sit inside the existing CGIF coverage contractually is still
  unanswered (F5).
- **F4's Exhibit A gap** — accepted; correspondence is the record.
- **Signal components whose gates the ingested chain cannot reach.** Two turned up by accident
  during this comparison, so all six basic signals, all six registered MSI components and the three
  gamma delegates were swept deliberately for the same shape. Three found. See
  [signal-component-inert-gate-sweep-2026-09.md](signal-component-inert-gate-sweep-2026-09.md).
  - **S1, signed underlying volume** — **RESOLVED (`b391764` + follow-up); this entry said
    "blocks step 15" and was stale as of 2026-10-08.** Total underlying volume survives cutover;
    what is lost is the uptick/downtick SPLIT alone (`buy_pct`, `period_buy_pct`,
    `uptick_vol_pct`, `tick_bias`), which now return NULL behind a "No Tick Data" label instead
    of the fabricated `50` / neutral they used to. That is an improvement on the prior
    behaviour, accepted and documented. **Cutover is not blocked by this.**
  - `skew_delta` has never produced a non-abstain reading on SPX or NDX — open.
  - `gex_gradient`'s wing damper has never fired — open.

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
