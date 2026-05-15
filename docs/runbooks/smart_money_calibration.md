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

## Distribution-based calibration (preferred)

The tiers above are the **cold-start fallback**.  When a positive
per-symbol rolling p95 of `volume_delta` / `premium` is present in
`component_normalizer_cache`, the tier breakpoints are instead derived
from that distribution — the defensible "unusual = upper percentile of
recent per-contract flow" definition.  Calibration is **per field**:
volume can be distribution-based while premium falls back to tiers (or
vice-versa) depending on which p95 rows are populated.

Populate the p95 rows with the existing nightly job:

```
python -m src.tools.normalizer_cache_refresh           # all active symbols
python -m src.tools.normalizer_cache_refresh --symbols SPY SPX
```

It samples per-contract-per-cycle `volume_delta` and `premium_delta`
from the canonical `flow_contract_facts` (so the distribution matches
exactly what the smart-money SQL scores) and writes
`smart_money_volume_delta` / `smart_money_premium` rows.

Distribution tier breakpoints are env-tunable multiples of p95
(tier 2 sits AT p95 = "unusual"):

| Tier | Volume env var (×p95)               | Premium env var (×p95)               | Default |
|------|-------------------------------------|--------------------------------------|---------|
| 1    | `SMART_MONEY_VOL_DIST_T1_P95_X`     | `SMART_MONEY_PREM_DIST_T1_P95_X`     | 0.5     |
| 2    | `SMART_MONEY_VOL_DIST_T2_P95_X`     | `SMART_MONEY_PREM_DIST_T2_P95_X`     | 1.0     |
| 3    | `SMART_MONEY_VOL_DIST_T3_P95_X`     | `SMART_MONEY_PREM_DIST_T3_P95_X`     | 2.0     |
| 4    | `SMART_MONEY_VOL_DIST_T4_P95_X`     | `SMART_MONEY_PREM_DIST_T4_P95_X`     | 4.0     |

A volume tier is floored at 1 so a tiny p95 can never produce a 0
threshold (which would admit every contract).  Each refresh logs the
resolved mode (`vol=dist,prem=tier`, etc.) and the p95 inputs at DEBUG.

## Inclusion filter

A row enters `flow_smart_money` only if at least one of these holds:

- `volume_delta >= vol_tier_1` (distribution- or tier-based)
- `premium >= prem_tier_1` (distribution- or tier-based)
- `implied_volatility > iv_incl AND volume_delta >= 20` (high-IV plays)
- `ABS(delta) < deep_otm_delta AND volume_delta >= 20` (deep-OTM plays)

`iv_incl` and `deep_otm_delta` are per-symbol env-tunable (previously
hardcoded 0.4 / 0.15), same precedence convention as the PCR
saturation knob:

1. `SMART_MONEY_IV_INCL_<SYMBOL>` / `SMART_MONEY_DEEP_OTM_DELTA_<SYMBOL>`
2. `SMART_MONEY_IV_INCL_DEFAULT` / `SMART_MONEY_DEEP_OTM_DELTA_DEFAULT`
3. `0.4` / `0.15` (legacy hardcoded defaults)

## Future work

The IV *score* sub-tiers (`> 0.6` -> 1, `> 1.0` -> 2) remain absolute
and could likewise move to a per-symbol IV-rank distribution.
