# flow_smart_money calibration

The `flow_smart_money` table classifies large/unusual options trades.
Each row carries an `unusual_activity_score` in [0, 10].  The score is
the sum of three sub-scores: a volume tier (0-4), a premium tier (0-4),
and an IV tier (0-2).

## Why it's symbol-aware

The previous implementation used hardcoded $50k/$100k/$250k/$500k
premium thresholds and 50/100/200/500 contract volume thresholds.  At
SPX (~$5500 spot, ~$550k notional per contract), the lowest premium
tier saturated on a single contract trade — every trade scored 4 on
premium.  At SPY (~$45k notional per contract), the highest premium
tier required ~12 contracts — modest size.  The same dollar number
meant very different things across underlyings.

The current implementation scales premium tiers by `notional_per_contract
= spot * 100`, so the tiers represent multiples of one-contract notional:

| Tier | Default ENV var                         | Multiple of notional/contract |
|------|------------------------------------------|-------------------------------|
| 1    | `SMART_MONEY_PREM_T1_NOTIONAL_X=1.0`     | 1×  (single contract)         |
| 2    | `SMART_MONEY_PREM_T2_NOTIONAL_X=2.0`     | 2×                            |
| 3    | `SMART_MONEY_PREM_T3_NOTIONAL_X=5.0`     | 5×                            |
| 4    | `SMART_MONEY_PREM_T4_NOTIONAL_X=10.0`    | 10× (10 contracts of notional)|

For SPX at $5500: tier 4 ≈ $5.5M premium.
For SPY at $450:  tier 4 ≈ $450k premium.

Volume tiers stay in raw contract counts (a "large block" is a
contract-count concept, not a notional one), but are tunable for
asymmetric markets:

| Tier | Default                                  |
|------|------------------------------------------|
| 1    | `SMART_MONEY_VOL_T1=50`                  |
| 2    | `SMART_MONEY_VOL_T2=100`                 |
| 3    | `SMART_MONEY_VOL_T3=200`                 |
| 4    | `SMART_MONEY_VOL_T4=500`                 |

## Inclusion filter

A row enters `flow_smart_money` only if at least one of these holds:

- `volume_delta >= SMART_MONEY_VOL_T1`
- `premium >= SMART_MONEY_PREM_T1_NOTIONAL_X * notional_per_contract`
- `implied_volatility > 0.4 AND volume_delta >= 20` (high-IV plays)
- `ABS(delta) < 0.15 AND volume_delta >= 20` (deep-OTM plays)

The IV/deep-OTM thresholds are not yet symbol-aware — they may need
tuning when extended to symbols beyond SPX/SPY.

## Future work

The score is still tier-based, not distribution-based.  A more
defensible "unusual" definition is the upper percentile of recent
flow distribution per symbol.  The `component_normalizer_cache`
table already stores per-symbol percentiles for several other
fields; extending it to cover `volume_delta` and `premium` per
symbol would let us replace the static tiers with rolling
calibration.
