-- Is the daily spread rollup a measurement, or a coin flip?
--
-- Every row in daily_spread_stats is a SNAPSHOT: the analytics writer runs
-- on a 60s cycle, and the row that survives the session is whatever the last
-- cycle before the 16:00 ET close happened to see. Every percentile on the
-- Spread Monitor's header cards ranks that snapshot against a window of
-- other snapshots.
--
-- That is sound if the snapshot is representative of its session, and it is
-- noise if it is not. We have one reason to think it may not be:
-- zero_bid_pct in the daily table alternates between exactly 0.0 and 7-9%
-- session to session, while spread_surface_stats shows EVERY session is
-- internally mixed — 6 to 11 of its 13 half-hour buckets at exactly zero and
-- the rest at 4-9%. A number that is bimodal at the session level but
-- continuous underneath is the signature of a single draw from a mixed
-- population, not of a market that changed.
--
-- This report tests that directly, using the surface table as ground truth
-- for what the whole session looked like. It is read-only.
--
-- WHAT EACH SECTION DECIDES
-- -------------------------
--   §1  Compares the session-to-session scatter of the CLOSING bucket (what
--       the daily row stores) against the same scatter for the session MEAN
--       (what an honest daily number would store). If the closing-bucket
--       column is much larger, the daily number is carrying sampling noise
--       that a session aggregate removes, and the fix is in the writer.
--       If they are close, the daily row is fine and the bimodality is real.
--
--   §2  Which half-hours of the session actually carry the no-bid contracts.
--       Concentrated at the open and the close is market structure, and the
--       daily row is then sampling a known-wide part of the day. Scattered
--       across the session with no shape is a feed artifact.
--
--   §3  Splits the intraday history at :cut and asks whether the SESSION
--       level picture — not the snapshot — changed. This is the only section
--       that can say anything about whether the market actually moved around
--       a given date, because it is the only one not built on single draws.
--
-- Widths here are median_relative_spread_pct, 100 * (ask - bid) / mid, the
-- same statistic the page shows. Contracts with no bid have no width and are
-- excluded from it, which is exactly why zero_bid_pct is tracked separately.
--
-- Variables: symbols, dte_max, band, days, cut, min_contracts.

\set ON_ERROR_STOP on
-- §1 and §2 are wide. psql hands a wide table to $PAGER, and less restores
-- the screen on exit, so the table vanishes from the scrollback the moment
-- you press q — which reads as "the section returned nothing".
\pset pager off

\if :{?symbols}       \else \set symbols       'SPX,NDX'    \endif
\if :{?dte_max}       \else \set dte_max       7            \endif
\if :{?band}          \else \set band          5            \endif
\if :{?days}          \else \set days          60           \endif
\if :{?cut}           \else \set cut           '2026-09-10' \endif
\if :{?min_contracts} \else \set min_contracts 100          \endif

-- The daily rollup pins dte_max; the surface table names the same universe
-- 'u7' / 'u30' / etc. Keep the two in step so §1 compares like with like.
\set dte_scope 'u':dte_max

\echo ''
\echo '================================================================'
\echo ' 1. THE SNAPSHOT vs THE SESSION IT CAME FROM'
\echo '================================================================'
\echo 'close_sd  is how much the closing bucket moves session to session.'
\echo 'mean_sd   is how much the whole-session average moves.'
\echo 'noise_x   is close_sd / mean_sd. Near 1 means the daily row tracks'
\echo '          its session and the percentiles are sound. Much above 1'
\echo '          means the daily row is mostly sampling noise.'
\echo 'Read the zb_ columns first: that is where the bimodality showed up.'
\echo ''

WITH bucketed AS (
    SELECT underlying,
           option_type,
           trading_date,
           bucket_start_min,
           zero_bid_pct,
           median_relative_spread_pct,
           MAX(bucket_start_min) OVER (
               PARTITION BY underlying, option_type, trading_date
           ) AS last_bucket
      FROM spread_surface_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND dte_scope = :'dte_scope'
       AND band_pct = :band::real
       AND money_bucket = 'all'
       AND contract_count >= :min_contracts
       AND trading_date >= CURRENT_DATE - :days::int
),
per_session AS (
    SELECT underlying,
           option_type,
           trading_date,
           COUNT(*) AS buckets,
           -- What the daily row stores.
           MAX(zero_bid_pct)
               FILTER (WHERE bucket_start_min = last_bucket) AS close_zb,
           MAX(median_relative_spread_pct)
               FILTER (WHERE bucket_start_min = last_bucket) AS close_width,
           -- What the session actually was.
           AVG(zero_bid_pct) AS mean_zb,
           AVG(median_relative_spread_pct) AS mean_width
      FROM bucketed
     GROUP BY underlying, option_type, trading_date
)
SELECT underlying AS sym,
       option_type AS side,
       COUNT(*) AS sessions,
       ROUND(AVG(buckets), 1) AS buckets_per_sess,
       ROUND(STDDEV_SAMP(close_zb)::numeric, 2) AS zb_close_sd,
       ROUND(STDDEV_SAMP(mean_zb)::numeric, 2) AS zb_mean_sd,
       ROUND((STDDEV_SAMP(close_zb) / NULLIF(STDDEV_SAMP(mean_zb), 0))::numeric,
             1) AS zb_noise_x,
       -- How often the stored number is exactly zero, against how often the
       -- session as a whole was. A big gap IS the artifact.
       ROUND(100.0 * COUNT(*) FILTER (WHERE close_zb = 0) / COUNT(*), 0)
           AS zb_close_pct_zero,
       ROUND(100.0 * COUNT(*) FILTER (WHERE mean_zb = 0) / COUNT(*), 0)
           AS zb_sess_pct_zero,
       ROUND(STDDEV_SAMP(close_width)::numeric, 3) AS w_close_sd,
       ROUND(STDDEV_SAMP(mean_width)::numeric, 3) AS w_mean_sd,
       ROUND((STDDEV_SAMP(close_width)
              / NULLIF(STDDEV_SAMP(mean_width), 0))::numeric, 1) AS w_noise_x
  FROM per_session
 GROUP BY underlying, option_type
 ORDER BY underlying, option_type;

\echo ''
\echo '================================================================'
\echo ' 2. WHICH HALF-HOURS CARRY THE NO-BID CONTRACTS'
\echo '================================================================'
\echo 'A hump at the open and the close is market structure. A flat line'
\echo 'with no shape is a feed artifact. sess_with_zb is how many of the'
\echo 'sessions had ANY no-bid contract in that bucket.'
\echo ''

SELECT underlying AS sym,
       option_type AS side,
       TO_CHAR(INTERVAL '1 minute' * bucket_start_min, 'HH24:MI') AS at_et,
       COUNT(*) AS sessions,
       COUNT(*) FILTER (WHERE zero_bid_pct > 0) AS sess_with_zb,
       ROUND(AVG(zero_bid_pct)::numeric, 2) AS avg_zb,
       ROUND(MAX(zero_bid_pct)::numeric, 1) AS max_zb,
       ROUND(AVG(median_relative_spread_pct)::numeric, 2) AS avg_width,
       ROUND(AVG(contract_count)) AS avg_contracts
  FROM spread_surface_stats
 WHERE underlying = ANY (string_to_array(:'symbols', ','))
   AND dte_scope = :'dte_scope'
   AND band_pct = :band::real
   AND money_bucket = 'all'
   AND contract_count >= :min_contracts
   AND trading_date >= CURRENT_DATE - :days::int
 GROUP BY underlying, option_type, bucket_start_min
 ORDER BY underlying, option_type, bucket_start_min;

\echo ''
\echo '================================================================'
\echo ' 3. DID THE SESSION-LEVEL PICTURE CHANGE AT THE CUT DATE?'
\echo '================================================================'
\echo 'Every number here is averaged over the whole session, so none of it'
\echo 'is a single draw. This is the only section that can answer "did the'
\echo 'market actually change around this date".'
\echo ''

SELECT underlying AS sym,
       option_type AS side,
       CASE WHEN trading_date < :'cut'::date THEN 'before ' || :'cut'
            ELSE 'from ' || :'cut' END AS era,
       COUNT(DISTINCT trading_date) AS sessions,
       ROUND(AVG(zero_bid_pct)::numeric, 2) AS avg_zb,
       ROUND(100.0 * COUNT(*) FILTER (WHERE zero_bid_pct = 0) / COUNT(*), 1)
           AS pct_buckets_zero,
       ROUND(AVG(median_relative_spread_pct)::numeric, 3) AS avg_width,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
                 ORDER BY median_relative_spread_pct)::numeric, 3)
           AS median_width,
       ROUND(AVG(contract_count)) AS avg_contracts
  FROM spread_surface_stats
 WHERE underlying = ANY (string_to_array(:'symbols', ','))
   AND dte_scope = :'dte_scope'
   AND band_pct = :band::real
   AND money_bucket = 'all'
   AND contract_count >= :min_contracts
   AND trading_date >= CURRENT_DATE - :days::int
 GROUP BY underlying, option_type,
          CASE WHEN trading_date < :'cut'::date THEN 'before ' || :'cut'
               ELSE 'from ' || :'cut' END
 ORDER BY underlying, option_type, era DESC;

\echo ''
\echo ''
\echo '================================================================'
\echo ' 4. IS A MOVE IN RELATIVE WIDTH REAL, OR JUST CHEAPER PREMIUM?'
\echo '================================================================'
\echo 'rel_w is 100 * (ask - bid) / mid -- premium sits in the denominator,'
\echo 'so it rises when vol falls even if the dollar spread never moved.'
\echo 'bps_w is 10,000 * (ask - bid) / spot: the same dollar spread with'
\echo 'the index, not the premium, underneath it. If rel_w moved and bps_w'
\echo 'did not, nothing happened to the market -- options just got cheaper.'
\echo ''
\echo 'Core hours only (10:00-15:00 ET). The open and the close have their'
\echo 'own structure -- see §2 -- and including them compares bucket mixes'
\echo 'rather than sessions. Each session contributes one value, so a'
\echo 'session with a missing bucket cannot outvote one without.'
\echo ''

WITH core AS (
    SELECT underlying,
           option_type,
           trading_date,
           AVG(median_relative_spread_pct) AS rel_w,
           AVG(10000.0 * median_spread / NULLIF(spot_price, 0)) AS bps_w,
           AVG(zero_bid_pct) AS zb,
           AVG(contract_count) AS contracts
      FROM spread_surface_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND dte_scope = :'dte_scope'
       AND band_pct = :band::real
       AND money_bucket = 'all'
       AND contract_count >= :min_contracts
       AND trading_date >= CURRENT_DATE - :days::int
       AND bucket_start_min BETWEEN 600 AND 900
     GROUP BY underlying, option_type, trading_date
)
SELECT underlying AS sym,
       option_type AS side,
       CASE WHEN trading_date < :'cut'::date THEN 'before ' || :'cut'
            ELSE 'from ' || :'cut' END AS era,
       COUNT(*) AS sessions,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rel_w)::numeric, 3)
           AS rel_w,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bps_w)::numeric, 2)
           AS bps_w,
       -- Whether a gap between the eras is bigger than the session-to-session
       -- scatter inside them. A shift well under one sd is not a finding.
       ROUND(STDDEV_SAMP(bps_w)::numeric, 2) AS bps_sd,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY zb)::numeric, 2)
           AS zb,
       ROUND(AVG(contracts)) AS contracts
  FROM core
 GROUP BY underlying, option_type,
          CASE WHEN trading_date < :'cut'::date THEN 'before ' || :'cut'
               ELSE 'from ' || :'cut' END
 ORDER BY underlying, option_type, era DESC;

\echo ''
\echo '================================================================'
\echo ' 5. THE WEEK-BY-WEEK PATH, CORE HOURS, IN BOTH MEASURES'
\echo '================================================================'
\echo 'An era split hides whether a move was a step, a drift, or a spike'
\echo 'that already reverted -- and a thing that reverted two weeks ago is'
\echo 'not news anyone can trade. Read bps_w down the column: that is the'
\echo 'one that is not distorted by the price of the options.'
\echo ''

WITH core AS (
    SELECT underlying,
           option_type,
           DATE_TRUNC('week', trading_date)::date AS week_of,
           trading_date,
           AVG(median_relative_spread_pct) AS rel_w,
           AVG(10000.0 * median_spread / NULLIF(spot_price, 0)) AS bps_w,
           AVG(zero_bid_pct) AS zb
      FROM spread_surface_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND dte_scope = :'dte_scope'
       AND band_pct = :band::real
       AND money_bucket = 'all'
       AND contract_count >= :min_contracts
       AND trading_date >= CURRENT_DATE - :days::int
       AND bucket_start_min BETWEEN 600 AND 900
     GROUP BY underlying, option_type, trading_date
)
SELECT week_of,
       COUNT(*) FILTER (WHERE underlying = 'SPX' AND option_type = 'P')
           AS spx_sess,
       ROUND(AVG(bps_w) FILTER
             (WHERE underlying = 'SPX' AND option_type = 'P')::numeric, 2)
           AS spx_p_bps,
       ROUND(AVG(bps_w) FILTER
             (WHERE underlying = 'SPX' AND option_type = 'C')::numeric, 2)
           AS spx_c_bps,
       ROUND(AVG(bps_w) FILTER
             (WHERE underlying = 'NDX' AND option_type = 'P')::numeric, 2)
           AS ndx_p_bps,
       ROUND(AVG(bps_w) FILTER
             (WHERE underlying = 'NDX' AND option_type = 'C')::numeric, 2)
           AS ndx_c_bps,
       ROUND(AVG(rel_w) FILTER
             (WHERE underlying = 'SPX' AND option_type = 'P')::numeric, 2)
           AS spx_p_rel,
       ROUND(AVG(rel_w) FILTER
             (WHERE underlying = 'NDX' AND option_type = 'P')::numeric, 2)
           AS ndx_p_rel
  FROM core
 GROUP BY week_of
 ORDER BY week_of;

\echo ''
