# Quantile-Regression Range Model (v2) — Roadmap & Data-Collection Plan

**Status:** parked · **First re-check:** 2027-01 (≈6 months of `daily_forecast` rows)
**Repo:** `zerogex-oa`
**Owner:** whoever picks this up next — no in-flight work

> The `daily_forecast` table (shipped 2026-06-30) records both the morning
> projection and the 4 PM receipt. Once ~120 trading days of receipts have
> accumulated we have enough labelled data to train a real range model that
> replaces `heuristic_v1`. This doc captures the intent so we don't forget it
> and can walk in cold and pick up.

## Why v1 is a heuristic, not a model

`src/jobs/forecast_range_model.py` (`heuristic_v1`) computes the projected
range by expanding the wider of `(spot − put_wall)` and `(call_wall − spot)`
by a 10% wick allowance, plus a 1.5× multiplier on FOMC/CPI/NFP days, then
clamps to a `[0.3%, 2.5%]` fraction-of-spot band. It's honest — every
component is auditable and the algorithm is deterministic — but every
constant (`WALL_EXPANSION = 1.10`, `EVENT_DAY_MULTIPLIER = 1.5`, the clamp
bounds) is a guess informed by nothing but intuition. We can do better once
we have realized data.

## What v2 looks like — quantile GBRT

**Model:** LightGBM (or scikit-learn's `GradientBoostingRegressor` with
`loss='quantile'`) trained twice per production build — once for the 5th
percentile of the day's low and once for the 95th percentile of the day's
high. The band `(P5_low, P95_high)` is calibrated to a 90% empirical coverage
target: over the trailing 90 sessions, ≥90% of realized ranges should fall
inside the predicted band. Anything less is a miscalibration alarm.

**Loss:** pinball loss (`quantile_loss` in scikit-learn / LightGBM's
`objective='quantile'` with `alpha=0.05` and `alpha=0.95`). Pinball rewards
narrow bands that still cover the target quantile — it's the honest scoring
rule for one-sided range forecasting.

**Features (all sourced from existing endpoints — no new ingestion):**

| Feature | Source | Rationale |
|---|---|---|
| `open_spot` | `/api/market/quote` | Level anchor; log-return targets |
| `call_wall - spot`, `spot - put_wall` | `/api/gex/summary` | Same signal `heuristic_v1` uses, but the model learns the shape |
| `gamma_flip_dist` | `/api/gex/summary` | Whether we open near the flip amplifies range |
| `msi_composite`, `msi_normalized` | `/api/signals/score` | Regime proxy — long-gamma days compress, short-gamma amplifies |
| `iv_rank_30d` | `/api/vol-surface` or a new snapshot col | Vol-of-vol matters as much as vol level |
| `atr_5d` | Derived from `equity_bars_intraday` | Realized-vol prior |
| `weekday` (Mon–Fri, one-hot) | `forecast_date` | Monday openings run wide; Friday pins tight |
| `days_to_opex` | Calendar | Opex Fridays compress |
| `is_event_day` | Same source `heuristic_v1` reads (FOMC/CPI/NFP) | Explicit macro shock flag |
| `open_hour_frac_realized_range` | First 30 min of `equity_bars_intraday` | If we're willing to shift the 7:10 writer to a 10:00 writer, this becomes the single strongest feature |

**Labels:** `actual_low`, `actual_high` from the 16:05 receipt writer (already
being collected — this is the reason v2 has to wait).

**Training cadence:** weekly Sunday batch that reads the trailing 250 rows
of `daily_forecast`, refits, writes the model artifact to
`models/range_quantile_v2_YYYY-MM-DD.joblib`, and updates a
`range_models_registry` row keyed by symbol. The 07:00 writer loads the most
recent artifact for the symbol at boot.

**A/B rollout:** for the first month, write BOTH `heuristic_v1` and
`quantile_v2` predictions (adding two nullable columns to `daily_forecast`)
but keep `range_model` pointing at `heuristic_v1` in the API response. At
end-of-month, compare on: (a) empirical 90%-coverage rate, (b) mean band
width, (c) Brier score on `range_respected`. Promote to primary only when
`v2` wins on both coverage and width.

## Data-collection requirements before we can train

- **≥120 receipt rows per symbol.** At 1 symbol × ~21 sessions/month, this is
  ~6 calendar months from the 2026-06-30 write-through go-live. First check:
  **2027-01-04** (Monday).
- **Zero receipt gaps.** Timers must survive box reboots (they will —
  `Persistent=true` is set). If we see a `daily_forecast` row with
  `receipt_ts IS NULL` for more than 24h, someone forgot to restart the
  timer. `SELECT date FROM daily_forecast WHERE receipt_ts IS NULL AND date
  < CURRENT_DATE - INTERVAL '1 day'` should return empty.
- **`heuristic_v1` unchanged through the collection window.** Changing the
  heuristic mid-collection contaminates the "does v2 beat v1?" comparison
  because v1's predictions on days 1-60 are from a different model than v1's
  predictions on days 61-120. If we tweak `WALL_EXPANSION`, we reset the
  clock.

## What "done" looks like

1. `src/models/range_quantile_v2.py` — training + inference wrapper.
2. `src/jobs/train_range_model.py` — weekly Sunday cron that refits.
3. `models/range_quantile_v2_*.joblib` artifact directory (gitignored,
   probably S3-backed).
4. `daily_forecast.range_model` starts writing `quantile_v2` instead of
   `heuristic_v1` for the primary prediction.
5. `/api/forecast/{date}` payload gains `model_metadata: {trained_at,
   artifact_id, feature_set_hash}` so the forecast card can render "trained
   on N sessions, last refit YYYY-MM-DD".
6. Grafana panel: rolling 30-day empirical coverage vs target 90%. Alerts
   when coverage drifts below 85%.

## Re-check reminder

Set a calendar reminder for **2027-01-04**. Query at that point:

```sql
SELECT
  COUNT(*) AS labelled_rows,
  MIN(date) AS earliest,
  MAX(date) AS latest,
  COUNT(*) FILTER (WHERE receipt_ts IS NULL) AS missing_receipts
FROM daily_forecast
WHERE symbol = 'SPY';
```

If `labelled_rows >= 120` and `missing_receipts = 0`, proceed to
implementation. Otherwise wait another month and re-check.

## References

- `src/jobs/forecast_range_model.py` — the current `heuristic_v1`
- `src/jobs/forecast_writer.py` — 07:00 morning writer
- `src/jobs/forecast_receipt.py` — 16:05 label writer
- `migrations/*_daily_forecast.sql` — table schema + immutability trigger
- `docs/design/pattern-calibration.md` — sibling doc: similar
  measure-then-model loop for playbook patterns
