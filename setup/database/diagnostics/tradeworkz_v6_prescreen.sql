-- TradeWorkz v6 pre-screen: the cheap questions to answer BEFORE a bot screen.
--
-- The v6 candidates (docs/design/tradeworkz-v6-strategies.md) sell or buy the
-- 0DTE move on this platform's confirmed MAGNITUDE signals, and one of them
-- trades the last-hour momentum the hedging-demand literature documents. Each
-- rests on a base rate that the existing tables can report in seconds, without
-- running `make tradeworkz-backtest` (~1 hour per 90-day screen). Run this
-- first; a section that comes back thin or null says the dependent bot will
-- read "insufficient" or "underpowered", and which gate to look at.
--
-- Read-only against the schema: every statement is a SELECT except one
-- session-local TEMP TABLE for section G (dropped at the end).
-- Runs on-demand via `make tradeworkz-v6-prescreen`.

\pset pager off
\pset border 2

-- The window is ANCHORED, not rolling. Two runs a day apart with a rolling
-- NOW() - 95 days window differed by one session, and that one session was
-- the whole change in the 13:30 gamma margin. Defaults below reproduce the
-- 2026-10-12 run; override from make with START=YYYY-MM-DD END=YYYY-MM-DD
-- (END is exclusive). The archive reaches back to 2026-04-20, so
-- START=2026-04-20 END=2026-07-10 is a holdout no reading here has seen.
\if :{?start_ts}
\else
\set start_ts 2026-07-10
\endif
\if :{?end_ts}
\else
\set end_ts 2026-10-13
\endif
\echo
\echo  window: :start_ts (inclusive) -> :end_ts (exclusive)

\echo
\echo ============================================================
\echo  A. Option-chain coverage per symbol (the screen window)
\echo ============================================================
\echo  The replay is clamped to hot UNION archive coverage. The archive is
\echo  read with one equality predicate per symbol so the probe uses the
\echo  (underlying, timestamp) index instead of scanning the whole table.
\echo

SELECT 'option_chains' AS tbl, underlying, MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts
FROM option_chains
WHERE underlying IN ('SPY', 'QQQ', 'SPX')
GROUP BY underlying
UNION ALL
SELECT 'option_chains_archive', 'SPY',
       (SELECT MIN(timestamp) FROM option_chains_archive WHERE underlying = 'SPY'),
       (SELECT MAX(timestamp) FROM option_chains_archive WHERE underlying = 'SPY')
UNION ALL
SELECT 'option_chains_archive', 'QQQ',
       (SELECT MIN(timestamp) FROM option_chains_archive WHERE underlying = 'QQQ'),
       (SELECT MAX(timestamp) FROM option_chains_archive WHERE underlying = 'QQQ')
UNION ALL
SELECT 'option_chains_archive', 'SPX',
       (SELECT MIN(timestamp) FROM option_chains_archive WHERE underlying = 'SPX'),
       (SELECT MAX(timestamp) FROM option_chains_archive WHERE underlying = 'SPX')
ORDER BY 1, 2;

\echo
\echo ============================================================
\echo  B. Does the morning forecast expected_vol_ratio predict the day?
\echo ============================================================
\echo  The compression-forecast gate assumes the graded daily_forecast rows
\echo  carry skill. Spearman rho between the committed expected_vol_ratio and
\echo  the realized_vol_ratio on the graded receipt, per symbol, plus the hit rate
\echo  and mean realized ratio on committed-compression days vs the rest.
\echo  rho near 0 => the forecast gate is noise; run only the ungated control.
\echo  A symbol with no rows here has no forecast => its forecast-gated bots
\echo  abstain on that symbol (the writer covers SPY, QQQ, SPX and NDX).
\echo

WITH graded AS (
    SELECT symbol, date, expected_vol_ratio, realized_vol_ratio, expected_vol_state,
           CASE WHEN realized_vol_ratio <= 0.85 THEN 'compression'
                WHEN realized_vol_ratio >= 1.15 THEN 'expansion'
                ELSE 'normal' END AS realized_state
    FROM daily_forecast
    WHERE receipt_ts IS NOT NULL
      AND expected_vol_ratio IS NOT NULL
      AND realized_vol_ratio IS NOT NULL
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY symbol ORDER BY expected_vol_ratio) AS r_expected,
           RANK() OVER (PARTITION BY symbol ORDER BY realized_vol_ratio) AS r_realized
    FROM graded
)
SELECT symbol,
       COUNT(*) AS graded_days,
       MIN(date) AS first_day,
       MAX(date) AS last_day,
       ROUND(CORR(r_expected, r_realized)::numeric, 3) AS spearman_rho,
       ROUND(100.0 * AVG((expected_vol_state = realized_state)::int), 1) AS state_hit_pct,
       COUNT(*) FILTER (WHERE expected_vol_state = 'compression') AS compression_days,
       ROUND(AVG(realized_vol_ratio) FILTER (WHERE expected_vol_state = 'compression')::numeric, 3)
           AS realized_on_compression_days,
       ROUND(AVG(realized_vol_ratio) FILTER (WHERE expected_vol_state <> 'compression')::numeric, 3)
           AS realized_on_other_days
FROM ranked
GROUP BY symbol
ORDER BY symbol;

\echo
\echo ============================================================
\echo  C. ATM IV history per symbol (the 60-day percentile)
\echo ============================================================
\echo  atm_iv_pctile_60d needs >= 20 prior sessions; below that the gate is
\echo  permissive by default (require_iv_pctile=false) and the funnel shows it.
\echo

SELECT underlying, COUNT(*) AS sessions, MIN(trading_date) AS first_day, MAX(trading_date) AS last_day,
       ROUND(AVG(atm_call_iv)::numeric, 4) AS mean_atm_iv
FROM daily_atm_iv
WHERE underlying IN ('SPY', 'QQQ', 'SPX')
GROUP BY underlying
ORDER BY underlying;

\echo
\echo ============================================================
\echo  D. Dealer-gamma sign base rates, RTH, over the window
\echo ============================================================
\echo  The fly needs positive gamma midday (then the top quartile by
\echo  gex_historical_stats, which this probe cannot rank cheaply); the
\echo  momentum bot needs NEGATIVE gamma at 15:00-15:20 ET. Shares of minutes.
\echo

WITH g AS (
    SELECT underlying, net_gex_at_spot,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM gex_summary
    WHERE underlying IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
      AND net_gex_at_spot IS NOT NULL
)
SELECT underlying,
       COUNT(*) FILTER (WHERE et BETWEEN '09:30' AND '16:00') AS rth_minutes,
       ROUND(100.0 * AVG((net_gex_at_spot > 0)::int) FILTER (WHERE et BETWEEN '10:30' AND '13:30'), 1)
           AS pct_positive_1030_1330,
       ROUND(100.0 * AVG((net_gex_at_spot < 0)::int) FILTER (WHERE et BETWEEN '15:00' AND '15:20'), 1)
           AS pct_negative_1500_1520
FROM g
GROUP BY underlying
ORDER BY underlying;

\echo
\echo ============================================================
\echo  E. Last-hour momentum, raw, from the cash tape (no options)
\echo ============================================================
\echo  The hedging_demand_close_momentum hypothesis before any option is
\echo  priced: on sessions where the 09:30 -> 15:00 cash return is at least
\echo  half the daily sigma implied by the prior-session ATM IV (daily_atm_iv;
\echo  the earlier VIX join clamped this to ~29 sessions; the default window
\echo  holds ~63), how often does 15:00 -> 15:55 go the
\echo  same way, and how many bps does it travel in the direction of the day? Split
\echo  by the dealer-gamma sign at 15:00. Baltussen et al. say the short-gamma
\echo  row should carry the effect and the long-gamma row should not. With
\echo  ~15 qualifying sessions per symbol this table can only rule OUT a large
\echo  effect; SPY and SPX are the same days, so pool them as one. The
\echo  gamma labels disagree between SPY and SPX on the same days (section D:
\echo  44% vs 60% positive midday), so a short-gamma row is not one condition,
\echo  and the 15:00 label is the structural (multi-day) gamma read, which
\echo  largely tracks whether the day is up or down. The second table splits
\echo  the same sessions by day direction so the two readings can be told apart. Index symbols open on their 09:31 bar (the
\echo  09:30 print is stale). Each symbol is scaled by its own ATM IV.
\echo

WITH bars AS (
    SELECT symbol, timestamp, close,
           (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM underlying_quotes
    WHERE symbol IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
),
opens AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_open
    FROM bars
    WHERE et >= CASE WHEN symbol IN ('SPX', 'NDX') THEN '09:31'::time ELSE '09:30'::time END
      AND et < '09:40'
    ORDER BY symbol, sess, timestamp
),
at_1500 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1500
    FROM bars WHERE et BETWEEN '14:45' AND '15:00'
    ORDER BY symbol, sess, timestamp DESC
),
at_1555 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1555
    FROM bars WHERE et BETWEEN '15:40' AND '15:55'
    ORDER BY symbol, sess, timestamp DESC
),
gamma_1500 AS (
    SELECT DISTINCT ON (underlying, sess) underlying AS symbol, sess, net_gex_at_spot
    FROM (SELECT underlying, net_gex_at_spot, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM gex_summary
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz) g
    WHERE et BETWEEN '14:00' AND '15:00' AND net_gex_at_spot IS NOT NULL
    ORDER BY underlying, sess, timestamp DESC
),
iv_scale AS (
    -- the prior session's ATM IV per symbol, as a daily 1-sigma fraction
    SELECT o.symbol, o.sess, d.atm_call_iv / SQRT(252) AS implied_daily
    FROM opens o
    JOIN LATERAL (
        SELECT atm_call_iv FROM daily_atm_iv
        WHERE underlying = o.symbol AND trading_date < o.sess AND atm_call_iv > 0
        ORDER BY trading_date DESC
        LIMIT 1
    ) d ON TRUE
),
sessions AS (
    SELECT o.symbol, o.sess,
           a.px_1500 / o.px_open - 1 AS day_ret,
           c.px_1555 / a.px_1500 - 1 AS last_ret,
           v.implied_daily,
           g.net_gex_at_spot
    FROM opens o
    JOIN at_1500 a USING (symbol, sess)
    JOIN at_1555 c USING (symbol, sess)
    JOIN gamma_1500 g USING (symbol, sess)
    JOIN iv_scale v USING (symbol, sess)
),
qualifying AS (
    SELECT *, SIGN(day_ret) * last_ret AS last_ret_in_day_direction,
           CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END AS gamma_at_1500,
           CASE WHEN day_ret > 0 THEN 'up day' ELSE 'down day' END AS day_dir
    FROM sessions
    WHERE ABS(day_ret) >= 0.5 * implied_daily AND ABS(day_ret) <= 3.0 * implied_daily
)
SELECT symbol,
       gamma_at_1500,
       COUNT(*) AS qualifying_sessions,
       ROUND(100.0 * AVG((SIGN(day_ret) = SIGN(last_ret))::int), 1) AS continuation_pct,
       ROUND((10000 * AVG(last_ret_in_day_direction))::numeric, 1) AS mean_last_55m_bps_in_day_dir,
       ROUND((10000 * PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY last_ret_in_day_direction::float8))::numeric, 1)
           AS median_last_55m_bps_in_day_dir
FROM qualifying
GROUP BY ROLLUP (symbol, gamma_at_1500)
ORDER BY symbol NULLS LAST, 2 NULLS LAST;

\echo
\echo  E2. The same qualifying sessions by direction of the day, then by gamma
\echo  sign inside each direction. If the long-gamma reversal above is only
\echo  the up-day rows reverting, the label is reading the day, not dealers.
\echo

WITH bars AS (
    SELECT symbol, timestamp, close,
           (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM underlying_quotes
    WHERE symbol IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
),
opens AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_open
    FROM bars
    WHERE et >= CASE WHEN symbol IN ('SPX', 'NDX') THEN '09:31'::time ELSE '09:30'::time END
      AND et < '09:40'
    ORDER BY symbol, sess, timestamp
),
at_1500 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1500
    FROM bars WHERE et BETWEEN '14:45' AND '15:00'
    ORDER BY symbol, sess, timestamp DESC
),
at_1555 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1555
    FROM bars WHERE et BETWEEN '15:40' AND '15:55'
    ORDER BY symbol, sess, timestamp DESC
),
gamma_1500 AS (
    SELECT DISTINCT ON (underlying, sess) underlying AS symbol, sess, net_gex_at_spot
    FROM (SELECT underlying, net_gex_at_spot, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM gex_summary
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz) g
    WHERE et BETWEEN '14:00' AND '15:00' AND net_gex_at_spot IS NOT NULL
    ORDER BY underlying, sess, timestamp DESC
),
iv_scale AS (
    SELECT o.symbol, o.sess, d.atm_call_iv / SQRT(252) AS implied_daily
    FROM opens o
    JOIN LATERAL (
        SELECT atm_call_iv FROM daily_atm_iv
        WHERE underlying = o.symbol AND trading_date < o.sess AND atm_call_iv > 0
        ORDER BY trading_date DESC
        LIMIT 1
    ) d ON TRUE
),
sessions AS (
    SELECT o.symbol, o.sess,
           a.px_1500 / o.px_open - 1 AS day_ret,
           c.px_1555 / a.px_1500 - 1 AS last_ret,
           v.implied_daily,
           g.net_gex_at_spot
    FROM opens o
    JOIN at_1500 a USING (symbol, sess)
    JOIN at_1555 c USING (symbol, sess)
    JOIN gamma_1500 g USING (symbol, sess)
    JOIN iv_scale v USING (symbol, sess)
),
qualifying AS (
    SELECT *, SIGN(day_ret) * last_ret AS last_ret_in_day_direction,
           CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END AS gamma_at_1500,
           CASE WHEN day_ret > 0 THEN 'up day' ELSE 'down day' END AS day_dir
    FROM sessions
    WHERE ABS(day_ret) >= 0.5 * implied_daily AND ABS(day_ret) <= 3.0 * implied_daily
)
SELECT symbol, day_dir, gamma_at_1500,
       COUNT(*) AS qualifying_sessions,
       ROUND(100.0 * AVG((SIGN(day_ret) = SIGN(last_ret))::int), 1) AS continuation_pct,
       ROUND((10000 * AVG(last_ret_in_day_direction))::numeric, 1) AS mean_last_55m_bps_in_day_dir
FROM qualifying
GROUP BY GROUPING SETS ((symbol, day_dir, gamma_at_1500), (symbol, day_dir), (day_dir, gamma_at_1500), (day_dir))
ORDER BY symbol NULLS LAST, day_dir, gamma_at_1500 NULLS LAST;

\echo
\echo ============================================================
\echo  F. Does positive gamma compress the realized day, on this tape?
\echo ============================================================
\echo  The fly thesis in one table: the cash-session high-low range as a
\echo  multiple of a NORMAL range (sqrt(8/pi) x the 1-sigma daily move implied
\echo  by the prior-session ATM IV), split by the dealer-gamma sign at 12:00 ET.
\echo  The LEVEL sits well under 1 on every row: the cash range excludes the
\echo  overnight gap and implied vol carries a premium, so read the long-gamma
\echo  row against the short-gamma row, not against 1 (there is no compression
\echo  share here for that reason). pm_ratio is the 12:00-16:00 range alone,
\echo  which is what the fly is exposed to and the only range the 12:00 gamma
\echo  reading does not already contain; am_ratio is the 09:30-12:00 range,
\echo  printed because the second run showed flat full-day ranges with lower
\echo  afternoons, i.e. the noon label co-varied with a BIGGER morning. Barbon
\echo  and Buraschi and the excursion studies on this platform say the
\echo  long-gamma rows should sit lower, on pm_ratio above all.
\echo

WITH bars AS (
    SELECT symbol, timestamp, high, low, close,
           (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM underlying_quotes
    WHERE symbol IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
),
opens AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_open
    FROM bars
    WHERE et >= CASE WHEN symbol IN ('SPX', 'NDX') THEN '09:31'::time ELSE '09:30'::time END
      AND et < '09:40'
    ORDER BY symbol, sess, timestamp
),
ranges AS (
    SELECT symbol, sess,
           MAX(high) - MIN(low) AS day_range,
           MAX(high) FILTER (WHERE et < '12:00') - MIN(low) FILTER (WHERE et < '12:00') AS am_range,
           MAX(high) FILTER (WHERE et >= '12:00') - MIN(low) FILTER (WHERE et >= '12:00') AS pm_range
    FROM bars WHERE et BETWEEN '09:30' AND '16:00'
    GROUP BY symbol, sess
),
gamma_1200 AS (
    SELECT DISTINCT ON (underlying, sess) underlying AS symbol, sess, net_gex_at_spot
    FROM (SELECT underlying, net_gex_at_spot, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM gex_summary
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz) g
    WHERE et BETWEEN '11:00' AND '12:00' AND net_gex_at_spot IS NOT NULL
    ORDER BY underlying, sess, timestamp DESC
),
iv_scale AS (
    -- the prior session's ATM IV per symbol, as a daily 1-sigma fraction
    SELECT o.symbol, o.sess, d.atm_call_iv / SQRT(252) AS implied_daily
    FROM opens o
    JOIN LATERAL (
        SELECT atm_call_iv FROM daily_atm_iv
        WHERE underlying = o.symbol AND trading_date < o.sess AND atm_call_iv > 0
        ORDER BY trading_date DESC
        LIMIT 1
    ) d ON TRUE
),
ratios AS (
    SELECT o.symbol, o.sess, g.net_gex_at_spot,
           r.day_range / (o.px_open * v.implied_daily * SQRT(8.0 / PI())) AS realized_over_normal,
           r.am_range / (o.px_open * v.implied_daily * SQRT(8.0 / PI())) AS am_over_normal,
           r.pm_range / (o.px_open * v.implied_daily * SQRT(8.0 / PI())) AS pm_over_normal
    FROM opens o
    JOIN ranges r USING (symbol, sess)
    JOIN gamma_1200 g USING (symbol, sess)
    JOIN iv_scale v USING (symbol, sess)
)
SELECT symbol,
       CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END AS gamma_at_1200,
       COUNT(*) AS sessions,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY realized_over_normal::float8)::numeric, 3) AS median_ratio,
       ROUND(AVG(realized_over_normal)::numeric, 3) AS mean_ratio,
       ROUND(AVG(am_over_normal)::numeric, 3) AS mean_am_ratio,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm_over_normal::float8)::numeric, 3) AS median_pm_ratio,
       ROUND(AVG(pm_over_normal)::numeric, 3) AS mean_pm_ratio
FROM ratios
GROUP BY ROLLUP (symbol, CASE WHEN net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END)
ORDER BY symbol NULLS LAST, 2 NULLS LAST;

\echo
\echo ============================================================
\echo  G. What the ATM straddle charged vs what the tape paid (measure first)
\echo ============================================================
\echo  The fly P&L before any bot runs. At 10:30, 12:00, 13:00, 13:30 and 14:30 ET, the
\echo  0DTE strike nearest spot with two-sided quotes on both legs, straddle
\echo  mid (and bid, which is what the harness sells at) against the move it
\echo  settled into, |px_1555 - strike|. Split by the dealer-gamma sign in the
\echo  hour before entry. Under fair pricing the MEAN paid/charged ratio is
\echo  1.0; the published 0DTE variance premium puts the unconditional near
\echo  0.85-0.90. The gamma gate earns its place only if the long-gamma row
\echo  sits at least 0.10 below the all-sessions row at the same slot. If it
\echo  does not, the tape-only twin is the trade to screen and the regime gate
\echo  is a diagnostic. seller_bps_of_spot is the mean gross result of the naked
\echo  seller at the straddle bid, in bps of spot, before the cost of the wings: a
\echo  row that is already negative here cannot be rescued by exits. This is a
\echo  hold-to-15:55 measurement, i.e. the gated/hold_to_settle arm, not the
\echo  default bot (take 50%, stop 0.5x, flat 15:40). Pooled rows average three
\echo  symbols on the SAME days (SPY and SPX are one index), so their effective
\echo  sample is ~52 sessions, not 156, and the wing column is a COUNT because
\echo  1 vs 4 sessions should read like 1 vs 4. Chains are read hot UNION
\echo  archive; a slot with few sessions means the 0DTE chain was not captured
\echo  in the ten minutes before it. Adjacent slots are the SAME sessions
\echo  settling to the SAME 15:55 print (13:30 and 14:30 share ~57% of their
\echo  variance), so a second slot agreeing is a third of a replication, not
\echo  a second one. Read seller_bps_of_spot before the ratio: a ratio that
\echo  falls while the bps stay flat is the straddle getting smaller, and the
\echo  case for that slot is cost structure (wings and slippage scale with the
\echo  straddle), not more premium.
\echo
\echo  The straddle set is built ONCE into a session-local temporary table
\echo  (the chain scan is the slow part; G, G2 and G3 then read it in
\echo  milliseconds). Nothing is written to the schema.
\echo

DROP TABLE IF EXISTS pg_temp.tw_v6_straddles;

CREATE TEMP TABLE tw_v6_straddles AS
WITH slots AS (
    SELECT unnest(ARRAY['10:30', '12:00', '13:00', '13:30', '14:30']::time[]) AS slot
),
bars AS (
    SELECT symbol, timestamp, close,
           (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
           (timestamp AT TIME ZONE 'America/New_York')::time AS et
    FROM underlying_quotes
    WHERE symbol IN ('SPY', 'QQQ', 'SPX')
      AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
),
opens AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_open
    FROM bars
    WHERE et >= CASE WHEN symbol IN ('SPX', 'NDX') THEN '09:31'::time ELSE '09:30'::time END
      AND et < '09:40'
    ORDER BY symbol, sess, timestamp
),
spot_at AS (
    SELECT DISTINCT ON (b.symbol, b.sess, s.slot) b.symbol, b.sess, s.slot, b.close AS spot
    FROM bars b CROSS JOIN slots s
    WHERE b.et > s.slot - INTERVAL '5 minutes' AND b.et <= s.slot
    ORDER BY b.symbol, b.sess, s.slot, b.timestamp DESC
),
at_1555 AS (
    SELECT DISTINCT ON (symbol, sess) symbol, sess, close AS px_1555
    FROM bars WHERE et BETWEEN '15:40' AND '15:55'
    ORDER BY symbol, sess, timestamp DESC
),
gamma_at AS (
    SELECT DISTINCT ON (g.underlying, g.sess, s.slot) g.underlying AS symbol, g.sess, s.slot,
           g.net_gex_at_spot
    FROM (SELECT underlying, net_gex_at_spot, timestamp,
                 (timestamp AT TIME ZONE 'America/New_York')::date AS sess,
                 (timestamp AT TIME ZONE 'America/New_York')::time AS et
          FROM gex_summary
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
            AND net_gex_at_spot IS NOT NULL) g
    CROSS JOIN slots s
    WHERE g.et > s.slot - INTERVAL '60 minutes' AND g.et <= s.slot
    ORDER BY g.underlying, g.sess, s.slot, g.timestamp DESC
),
chain AS (
    -- 0DTE rows only, within 1% of the slot's spot, in the ten minutes before it.
    SELECT c.underlying AS symbol, c.timestamp, c.strike, c.option_type, c.bid, c.ask,
           COALESCE(c.mid, (c.bid + c.ask) / 2.0) AS mid,
           sp.sess, sp.slot, sp.spot
    FROM (SELECT underlying, timestamp, strike, expiration, option_type, bid, ask, mid
          FROM option_chains
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
            AND expiration = (timestamp AT TIME ZONE 'America/New_York')::date
            AND ((timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '10:20' AND '10:30'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '11:50' AND '12:00'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '12:50' AND '13:00'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '13:20' AND '13:30'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '14:20' AND '14:30')
          UNION ALL
          SELECT underlying, timestamp, strike, expiration, option_type, bid, ask, mid
          FROM option_chains_archive
          WHERE underlying IN ('SPY', 'QQQ', 'SPX')
            AND timestamp >= :'start_ts'::timestamptz AND timestamp < :'end_ts'::timestamptz
            AND expiration = (timestamp AT TIME ZONE 'America/New_York')::date
            AND ((timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '10:20' AND '10:30'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '11:50' AND '12:00'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '12:50' AND '13:00'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '13:20' AND '13:30'
                 OR (timestamp AT TIME ZONE 'America/New_York')::time BETWEEN '14:20' AND '14:30')) c
    JOIN spot_at sp
      ON sp.symbol = c.underlying
     AND sp.sess = (c.timestamp AT TIME ZONE 'America/New_York')::date
     AND (c.timestamp AT TIME ZONE 'America/New_York')::time > sp.slot - INTERVAL '10 minutes'
     AND (c.timestamp AT TIME ZONE 'America/New_York')::time <= sp.slot
    WHERE ABS(c.strike - sp.spot) <= 0.01 * sp.spot
      AND c.bid > 0 AND c.ask > 0
),
chain_ts AS (
    -- the latest chain stamp before each slot
    SELECT symbol, sess, slot, MAX(timestamp) AS chain_ts
    FROM chain
    GROUP BY symbol, sess, slot
),
straddles AS (
    -- strike nearest spot with BOTH legs two-sided at that stamp
    SELECT DISTINCT ON (k.symbol, k.sess, k.slot) k.symbol, k.sess, k.slot, k.spot, k.strike,
           k.mid + p.mid AS straddle_mid,
           k.bid + p.bid AS straddle_bid
    FROM chain k
    JOIN chain_ts t ON t.symbol = k.symbol AND t.sess = k.sess AND t.slot = k.slot
                   AND t.chain_ts = k.timestamp
    JOIN chain p ON p.symbol = k.symbol AND p.timestamp = k.timestamp AND p.slot = k.slot
                AND p.strike = k.strike AND p.option_type = 'P'
    WHERE k.option_type = 'C'
    ORDER BY k.symbol, k.sess, k.slot, ABS(k.strike - k.spot), k.strike
)
SELECT s.symbol, s.sess, s.slot, s.spot, s.strike, s.straddle_mid, s.straddle_bid,
       ABS(c.px_1555 - s.strike) AS paid,
       g.net_gex_at_spot,
       CASE WHEN g.net_gex_at_spot < 0 THEN 'short gamma' ELSE 'long gamma' END AS gamma_sign,
       CASE WHEN s.spot >= o.px_open THEN 'up day' ELSE 'down day' END AS day_dir,
       NTILE(2) OVER (PARTITION BY s.symbol, s.slot ORDER BY s.straddle_mid / s.spot) AS richness
FROM straddles s
JOIN at_1555 c USING (symbol, sess)
JOIN gamma_at g USING (symbol, sess, slot)
JOIN opens o USING (symbol, sess)
WHERE s.straddle_mid > 0;

SELECT symbol,
       slot AS entry_et,
       gamma_sign AS gamma_before_entry,
       COUNT(*) AS sessions,
       ROUND(AVG(straddle_mid)::numeric, 2) AS mean_straddle_mid,
       ROUND(AVG(paid / straddle_mid)::numeric, 3) AS mean_paid_over_charged,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (paid / straddle_mid)::float8)::numeric, 3)
           AS median_paid_over_charged,
       ROUND((10000 * AVG(paid / spot))::numeric, 1) AS mean_paid_bps_of_spot,
       ROUND(100.0 * AVG((paid < straddle_bid)::int), 1) AS seller_win_pct_at_bid,
       ROUND((10000 * AVG((straddle_bid - paid) / spot))::numeric, 1) AS seller_bps_of_spot,
       SUM((paid > 1.25 * straddle_mid / SQRT(2.0 / PI()))::int) AS beyond_1p25_sigma_wing_n
FROM tw_v6_straddles
GROUP BY GROUPING SETS ((symbol, slot, gamma_sign), (symbol, slot), (slot))
ORDER BY symbol NULLS LAST, slot, 3 NULLS LAST;

\echo
\echo  G2. The same straddles, gamma sign x direction of the day at entry.
\echo  net_gex_at_spot is read off the structural (prior-close open interest)
\echo  profile, so its sign at 13:30 largely says which side of the multi-day
\echo  flip spot sits on, i.e. whether the day is up or down. If the long-gamma
\echo  advantage lives only in the up-day rows, the gate is reading the day
\echo  direction, not dealer hedging; a long-gamma advantage inside BOTH
\echo  direction rows is the mechanism. First pre-screen (2026-10-11) also
\echo  showed the short-gamma straddle 10-26% richer in every cell, so G3
\echo  re-cuts by straddle richness instead of gamma sign.
\echo

SELECT symbol, slot AS entry_et, day_dir, gamma_sign,
       COUNT(*) AS sessions,
       ROUND(AVG(straddle_mid)::numeric, 2) AS mean_straddle_mid,
       ROUND(AVG(paid / straddle_mid)::numeric, 3) AS mean_paid_over_charged,
       ROUND((10000 * AVG(paid / spot))::numeric, 1) AS mean_paid_bps_of_spot,
       ROUND((10000 * AVG((straddle_bid - paid) / spot))::numeric, 1) AS seller_bps_of_spot,
       SUM((paid > 1.25 * straddle_mid / SQRT(2.0 / PI()))::int) AS beyond_1p25_sigma_wing_n
FROM tw_v6_straddles
WHERE slot >= '12:00'
GROUP BY GROUPING SETS ((symbol, slot, day_dir, gamma_sign), (symbol, slot, day_dir))
ORDER BY symbol, slot, day_dir, gamma_sign NULLS LAST;

\echo
\echo  G3. The same straddles cut by straddle richness (straddle mid / spot,
\echo  above or below the symbol-slot median) instead of gamma sign. If the
\echo  cheap half pays the seller as well as the long-gamma rows do, the gate
\echo  is a proxy for a quiet (cheap-straddle) tape and twin/tail_only is the
\echo  trade. long_gamma_pct shows how much the two cuts overlap.
\echo

SELECT symbol, slot AS entry_et,
       CASE richness WHEN 1 THEN 'cheap half' ELSE 'rich half' END AS straddle_richness,
       COUNT(*) AS sessions,
       ROUND(100.0 * AVG((gamma_sign = 'long gamma')::int), 1) AS long_gamma_pct,
       ROUND(AVG(straddle_mid)::numeric, 2) AS mean_straddle_mid,
       ROUND(AVG(paid / straddle_mid)::numeric, 3) AS mean_paid_over_charged,
       ROUND((10000 * AVG(paid / spot))::numeric, 1) AS mean_paid_bps_of_spot,
       ROUND((10000 * AVG((straddle_bid - paid) / spot))::numeric, 1) AS seller_bps_of_spot,
       SUM((paid > 1.25 * straddle_mid / SQRT(2.0 / PI()))::int) AS beyond_1p25_sigma_wing_n
FROM tw_v6_straddles
WHERE slot >= '12:00'
GROUP BY symbol, slot, richness
ORDER BY symbol, slot, richness;

DROP TABLE IF EXISTS pg_temp.tw_v6_straddles;

\echo
\echo  Done. Next: python -m src.tools.tw_edge_field_probe --days 90 for the
\echo  new snapshot fields, then make tradeworkz-backtest for the v6 bots.
\echo
