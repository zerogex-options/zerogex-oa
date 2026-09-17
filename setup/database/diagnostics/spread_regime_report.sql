-- Spread regime report — "have index put spreads actually gone wider lately?"
--
-- Written to answer that question with numbers you can put in front of
-- someone who trades the product, rather than a screenshot of a card. It
-- reads only the two rollups the Spread Monitor reads, so every figure here
-- is the same statistic the page shows, computed the same way:
--
--   daily_spread_stats    one row per session at ONE pinned scope
--                         (SPREAD_STATS_DTE_MAX / _MONEYNESS_BAND_PCT,
--                         default through-7DTE, +/-5% of spot), anchored at
--                         the last analytics cycle before the 16:00 ET close.
--   spread_surface_stats  the same statistic sliced by expiry bucket, by
--                         moneyness and by half-hour of the session.
--
-- WHICH TABLE ANSWERS WHICH QUESTION
-- ----------------------------------
-- The daily rollup is the only place "lately" lives — it is a session-level
-- series, so §1-§3 read it. It CANNOT answer anything about 0DTE: it stores
-- one scope, and that scope is the whole near-dated chain. Slicing today's
-- 0DTE book out of the chain and ranking it against a through-7DTE window
-- compares two populations, reports the widest 5% of sessions on an ordinary
-- day, and is the bug §4 exists to avoid. So the 0DTE question is answered
-- from spread_surface_stats, which stores 0DTE against 0DTE at the same time
-- of day.
--
-- WHAT THE NUMBERS ARE
-- --------------------
--   median_relative_spread_pct   100 * (ask - bid) / mid. The headline: the
--                                share of the premium that crossing costs.
--                                NOT comparable between SPX and NDX in any
--                                strong sense, and not comparable across
--                                expiries at all — 0DTE premium is small, so
--                                its relative width is large on the calmest
--                                day of the year.
--   bps of the index             10,000 * (ask - bid) / spot. The ONLY
--                                cross-symbol comparable width: SPX near
--                                6,800 and NDX near 25,000 are not on one
--                                dollar scale. §5 is in these.
--   zero_bid_pct                 contracts quoted with an offer and no bid.
--                                No width exists for them, so they are
--                                excluded from every median above and
--                                counted here instead. A chain can hold its
--                                median while a fifth of it goes untradeable.
--
-- Percentiles match src/analytics/spread_stats.percentile_rank exactly: the
-- share of prior readings AT OR BELOW today's, over a window that excludes
-- today's own row. Medians match spread_stats.percentile(values, 50), which
-- is linear-interpolated, hence percentile_cont.
--
-- Sessions below :min_contracts are dropped here as they are on the read
-- path: a median over 21 contracts and one over 684 are not the same
-- measurement, and an ingestion outage is not a quiet market.
--
-- THE CLOCK, AND WHY §1 PRINTS IT
-- ------------------------------
-- Every historical row in daily_spread_stats froze at the last analytics
-- cycle before the 16:00 ET close. TODAY's row has not frozen: it is
-- whatever the most recent cycle wrote, which at 09:40 is a reading taken
-- eleven minutes after the open. Spreads have a shape through the session
-- and the open is the wide end of it, so ranking a morning reading against
-- a window of closing readings flatters today upward.
--
-- So §1 and §3 print `as_of_et` beside `baseline_at_et` rather than
-- assuming the two agree, and `skip_today = yes` drops the unfrozen
-- session entirely — which is the honest setting for the REGIME question,
-- where a half-formed session should not get a vote. Leave it off to see
-- what the page is showing right now; the page has the same property.
--
-- Read-only. Run:
--
--   make spread-report
--   make spread-report SYMBOLS=SPX,NDX DAYS=90 RECENT=5
--
-- Variables: symbols, dte_max, band, days, recent, min_contracts,
-- min_sessions, option_type, skip_today.

\set ON_ERROR_STOP on

\if :{?symbols}       \else \set symbols       'SPX,NDX,SPY,QQQ' \endif
\if :{?dte_max}       \else \set dte_max       7                 \endif
\if :{?band}          \else \set band          5                 \endif
\if :{?days}          \else \set days          60                \endif
\if :{?recent}        \else \set recent        10                \endif
\if :{?min_contracts} \else \set min_contracts 100               \endif
\if :{?min_sessions}  \else \set min_sessions  8                 \endif
\if :{?option_type}   \else \set option_type   'P'               \endif
\if :{?skip_today}    \else \set skip_today    'no'              \endif

\echo ''
\echo '================================================================'
\echo ' 1. TODAY vs THE TRAILING WINDOW  (daily rollup, pinned scope)'
\echo '================================================================'
\echo 'The header cards, and the "N x the median width of that window"'
\echo 'sentence. pctile is where the latest session ranks among the'
\echo 'prior ones; 100 means wider than every one of them.'
\echo ''
\echo 'CHECK as_of_et AGAINST baseline_at_et FIRST. Historical rows froze'
\echo 'near the 16:00 ET close; today has not frozen. A morning as_of'
\echo 'against a 15:5x baseline ranks the time of day, not the session —'
\echo 'run with skip_today=yes for a clean read.'
\echo ''

WITH scoped AS (
    SELECT *,
           MAX(trading_date) OVER (PARTITION BY underlying) AS newest_date
      FROM daily_spread_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND dte_max = :dte_max
       AND moneyness_band_pct = :band
       AND contract_count >= :min_contracts
       AND median_relative_spread_pct IS NOT NULL
       AND trading_date > CURRENT_DATE - (:days || ' days')::interval
),
usable AS (
    -- The newest session is the only one that can still be unfrozen, and it
    -- is identified by being newest rather than by the server clock: this
    -- box runs UTC, so CURRENT_DATE rolls over at 20:00 ET and would call
    -- the evening's live row yesterday's.
    SELECT * FROM scoped
     WHERE :'skip_today' <> 'yes' OR trading_date < newest_date
),
latest AS (
    SELECT DISTINCT ON (underlying, option_type) *
      FROM usable
     ORDER BY underlying, option_type, trading_date DESC
),
ranked AS (
    SELECT l.underlying,
           l.option_type,
           l.trading_date,
           l.source_timestamp,
           l.spot_price,
           l.median_relative_spread_pct                        AS today_pct,
           l.p90_relative_spread_pct                           AS today_p90,
           l.median_spread                                     AS today_spread,
           l.median_spread_bps_underlying                      AS today_bps,
           l.zero_bid_pct + l.crossed_or_locked_pct            AS today_dead_pct,
           l.contract_count,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY w.median_relative_spread_pct)          AS window_median,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY EXTRACT(epoch FROM
                   (w.source_timestamp AT TIME ZONE 'America/New_York')::time))
                                                               AS window_clock,
           COUNT(w.*)                                          AS sessions,
           COUNT(*) FILTER (
               WHERE w.median_relative_spread_pct
                     <= l.median_relative_spread_pct)          AS at_or_below
      FROM latest l
      LEFT JOIN usable w
             ON w.underlying = l.underlying
            AND w.option_type = l.option_type
            AND w.trading_date <> l.trading_date
     GROUP BY l.underlying, l.option_type, l.trading_date, l.source_timestamp,
              l.spot_price, l.median_relative_spread_pct,
              l.p90_relative_spread_pct, l.median_spread,
              l.median_spread_bps_underlying, l.zero_bid_pct,
              l.crossed_or_locked_pct, l.contract_count
)
SELECT underlying                                   AS sym,
       option_type                                  AS side,
       trading_date                                 AS session,
       TO_CHAR(source_timestamp AT TIME ZONE 'America/New_York', 'HH24:MI')
                                                    AS as_of_et,
       TO_CHAR(INTERVAL '1 second' * window_clock, 'HH24:MI')
                                                    AS baseline_at_et,
       ROUND(today_pct::numeric, 2)                 AS width_pct,
       ROUND(today_p90::numeric, 2)                 AS p90_pct,
       ROUND(window_median::numeric, 2)             AS normal_pct,
       CASE WHEN window_median > 0
            THEN ROUND((today_pct / window_median)::numeric, 2) END
                                                    AS vs_normal_x,
       CASE WHEN sessions > 0
            THEN ROUND(100.0 * at_or_below / sessions, 1) END
                                                    AS pctile,
       sessions,
       ROUND(today_spread::numeric, 2)              AS width_dollars,
       ROUND(today_bps::numeric, 1)                 AS width_bps,
       ROUND(today_dead_pct::numeric, 1)            AS no_market_pct,
       contract_count                               AS contracts
  FROM ranked
 ORDER BY sym, side;

\echo ''
\echo '================================================================'
\echo ' 2. WHICH SIDE IS EXPENSIVE  (put width / call width, by session)'
\echo '================================================================'
\echo 'The claim is about PUTS, not about liquidity in general. Above 1'
\echo 'is a hedging bid: downside costs more to get into and out of than'
\echo 'the same-distance upside. A ratio that has climbed while both'
\echo 'sides widened is a different story from both widening together.'
\echo ''

WITH scoped AS (
    SELECT underlying, trading_date, option_type,
           median_relative_spread_pct,
           median_spread_bps_underlying
      FROM daily_spread_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND dte_max = :dte_max
       AND moneyness_band_pct = :band
       AND contract_count >= :min_contracts
       AND option_type IN ('P', 'C')
       AND median_relative_spread_pct IS NOT NULL
       AND trading_date > CURRENT_DATE - (:days || ' days')::interval
),
paired AS (
    SELECT underlying, trading_date,
           MAX(median_relative_spread_pct)
               FILTER (WHERE option_type = 'P')     AS put_pct,
           MAX(median_relative_spread_pct)
               FILTER (WHERE option_type = 'C')     AS call_pct
      FROM scoped
     GROUP BY underlying, trading_date
)
SELECT underlying                                   AS sym,
       COUNT(*)                                     AS sessions,
       ROUND(AVG(put_pct / NULLIF(call_pct, 0))::numeric, 2)
                                                    AS mean_put_call_x,
       ROUND((percentile_cont(0.5) WITHIN GROUP (
           ORDER BY put_pct / NULLIF(call_pct, 0)))::numeric, 2)
                                                    AS median_put_call_x,
       ROUND(MAX(put_pct / NULLIF(call_pct, 0))::numeric, 2)
                                                    AS worst_put_call_x,
       ROUND((MAX(put_pct / NULLIF(call_pct, 0))
              FILTER (WHERE trading_date = (SELECT MAX(trading_date)
                                              FROM paired p2
                                             WHERE p2.underlying =
                                                   paired.underlying)))::numeric, 2)
                                                    AS latest_put_call_x
  FROM paired
 WHERE call_pct IS NOT NULL AND put_pct IS NOT NULL
 GROUP BY underlying
 ORDER BY sym;

\echo ''
\echo '================================================================'
\echo ' 3. IS "LATELY" REAL?  (recent sessions vs the rest of the window)'
\echo '================================================================'
\echo 'The actual claim is a REGIME claim, not a claim about today. This'
\echo 'splits the window: the last' :recent 'sessions against everything'
\echo 'before them. shift_x near 1 means nothing has changed and the'
\echo 'complaint is about one bad session.'
\echo ''

WITH scoped AS (
    SELECT underlying, trading_date,
           median_relative_spread_pct,
           p90_relative_spread_pct,
           median_spread_bps_underlying,
           zero_bid_pct + crossed_or_locked_pct     AS dead_pct,
           ROW_NUMBER() OVER (PARTITION BY underlying
                                  ORDER BY trading_date DESC) AS recency
      FROM daily_spread_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND option_type = :'option_type'
       AND dte_max = :dte_max
       AND moneyness_band_pct = :band
       AND contract_count >= :min_contracts
       AND median_relative_spread_pct IS NOT NULL
       AND trading_date > CURRENT_DATE - (:days || ' days')::interval
       -- An unfrozen session carries more weight here than anywhere else:
       -- over a :recent of 5 it is a fifth of the "recent" side, taken at
       -- the wide end of the session. See skip_today in the header.
       AND (:'skip_today' <> 'yes'
            OR trading_date < (SELECT MAX(d2.trading_date)
                                 FROM daily_spread_stats d2
                                WHERE d2.underlying = daily_spread_stats.underlying
                                  AND d2.option_type = :'option_type'))
),
split AS (
    SELECT underlying,
           recency <= :recent                       AS is_recent,
           COUNT(*)                                 AS sessions,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY median_relative_spread_pct) AS med_pct,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY p90_relative_spread_pct)    AS med_p90,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY median_spread_bps_underlying) AS med_bps,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY dead_pct)                   AS med_dead
      FROM scoped
     GROUP BY underlying, recency <= :recent
)
SELECT underlying                                   AS sym,
       MAX(sessions) FILTER (WHERE is_recent)       AS recent_n,
       MAX(sessions) FILTER (WHERE NOT is_recent)   AS prior_n,
       ROUND((MAX(med_pct) FILTER (WHERE is_recent))::numeric, 2)
                                                    AS recent_pct,
       ROUND((MAX(med_pct) FILTER (WHERE NOT is_recent))::numeric, 2)
                                                    AS prior_pct,
       ROUND((MAX(med_pct) FILTER (WHERE is_recent)
              / NULLIF(MAX(med_pct) FILTER (WHERE NOT is_recent), 0))::numeric, 2)
                                                    AS shift_x,
       ROUND((MAX(med_bps) FILTER (WHERE is_recent))::numeric, 1)
                                                    AS recent_bps,
       ROUND((MAX(med_bps) FILTER (WHERE NOT is_recent))::numeric, 1)
                                                    AS prior_bps,
       ROUND((MAX(med_p90) FILTER (WHERE is_recent)
              / NULLIF(MAX(med_p90) FILTER (WHERE NOT is_recent), 0))::numeric, 2)
                                                    AS wings_shift_x,
       ROUND((MAX(med_dead) FILTER (WHERE is_recent))::numeric, 1)
                                                    AS recent_no_market_pct,
       ROUND((MAX(med_dead) FILTER (WHERE NOT is_recent))::numeric, 1)
                                                    AS prior_no_market_pct
  FROM split
 GROUP BY underlying
 ORDER BY sym;

\echo ''
\echo '================================================================'
\echo ' 4. 0DTE, RANKED AGAINST 0DTE  (surface rollup, time-matched)'
\echo '================================================================'
\echo 'Each expiry bucket ranked against ITS OWN history in the same'
\echo 'half-hour of the session — the only honest way to ask whether the'
\echo 'front expiry is unusual, since 0DTE is the widest book of the year'
\echo 'every day. b0 = 0DTE, b1 = 1DTE, b2_3, b4_7, b8_30. A bucket'
\echo 'with no listed expiry today (2-3 DTE over a weekend) anchors on'
\echo 'the last session that had one — read the session column. Below'
\echo :min_sessions 'comparable sessions the page publishes no rank,'
\echo 'and neither should you.'
\echo ''

WITH scoped AS (
    SELECT *
      FROM spread_surface_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND option_type = :'option_type'
       AND band_pct = :band::real
       AND money_bucket = 'all'
       -- The DISJOINT buckets only. surface_scopes stores two families
       -- under money_bucket = 'all': these, and the cumulative universes
       -- u0/u1/u7/u30 that back the summary strip. Listing both puts u0
       -- beside b0 holding the identical number by construction, and
       -- invites reading "0DTE is wide" off four rows that are the same
       -- row. The cumulative view is §1's job; this is the by-expiry cut.
       AND dte_scope IN ('b0', 'b1', 'b2_3', 'b4_7', 'b8_30')
       AND median_relative_spread_pct IS NOT NULL
       AND trading_date > CURRENT_DATE - (:days || ' days')::interval
),
anchor AS (
    -- The latest stored reading per (symbol, expiry bucket): the newest
    -- session, and within it the last time bucket written. After the close
    -- that is the 15:30-16:00 reading, which is also what the page falls
    -- back to and labels as such.
    SELECT DISTINCT ON (underlying, dte_scope) *
      FROM scoped
     ORDER BY underlying, dte_scope, trading_date DESC, bucket_start_min DESC
),
ranked AS (
    SELECT a.underlying,
           a.dte_scope,
           a.trading_date,
           a.bucket_start_min,
           a.median_relative_spread_pct                        AS current_pct,
           a.median_spread,
           a.spot_price,
           a.two_sided_pct,
           a.contract_count,
           percentile_cont(0.5) WITHIN GROUP (
               ORDER BY w.median_relative_spread_pct)          AS normal_pct,
           COUNT(w.*)                                          AS sessions,
           COUNT(*) FILTER (
               WHERE w.median_relative_spread_pct
                     <= a.median_relative_spread_pct)          AS at_or_below
      FROM anchor a
      LEFT JOIN scoped w
             ON w.underlying = a.underlying
            AND w.dte_scope = a.dte_scope
            -- Same clock time, or the comparison is against a different
            -- market: 0DTE at 15:45 behaves nothing like 0DTE at 10:00.
            AND w.bucket_start_min = a.bucket_start_min
            AND w.trading_date <> a.trading_date
     GROUP BY a.underlying, a.dte_scope, a.trading_date, a.bucket_start_min,
              a.median_relative_spread_pct, a.median_spread, a.spot_price,
              a.two_sided_pct, a.contract_count
)
SELECT underlying                                   AS sym,
       dte_scope                                    AS expiry,
       trading_date                                 AS session,
       TO_CHAR((bucket_start_min || ' minutes')::interval, 'HH24:MI')
           || ' ET'                                 AS at_time,
       ROUND(current_pct::numeric, 2)               AS width_pct,
       ROUND(normal_pct::numeric, 2)                AS normal_pct,
       CASE WHEN normal_pct > 0
            THEN ROUND((current_pct / normal_pct)::numeric, 2) END
                                                    AS vs_normal_x,
       CASE WHEN sessions >= :min_sessions
            THEN ROUND(100.0 * at_or_below / sessions, 1) END
                                                    AS pctile,
       sessions,
       CASE WHEN sessions < :min_sessions
            THEN 'insufficient history' END         AS note,
       ROUND((10000.0 * median_spread / NULLIF(spot_price, 0))::numeric, 1)
                                                    AS width_bps,
       ROUND(two_sided_pct::numeric, 1)             AS two_sided_pct,
       contract_count                               AS contracts
  FROM ranked
 ORDER BY sym,
          CASE dte_scope WHEN 'b0'   THEN 0 WHEN 'b1'   THEN 1
                         WHEN 'b2_3' THEN 2 WHEN 'b4_7' THEN 3
                         WHEN 'b8_30' THEN 4 ELSE 9 END;

\echo ''
\echo '================================================================'
\echo ' 5. SPX vs NDX ON ONE SCALE  (basis points of the index)'
\echo '================================================================'
\echo 'The only column that compares symbols. Relative width flatters the'
\echo 'expensive index and dollar width just compares index levels. Both'
\echo 'scopes: the whole near-dated chain, and the front expiry alone.'
\echo ''

WITH chain AS (
    SELECT DISTINCT ON (underlying)
           underlying,
           trading_date,
           median_spread_bps_underlying             AS chain_bps,
           median_relative_spread_pct               AS chain_pct
      FROM daily_spread_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND option_type = :'option_type'
       AND dte_max = :dte_max
       AND moneyness_band_pct = :band
       AND contract_count >= :min_contracts
       AND median_spread_bps_underlying IS NOT NULL
     ORDER BY underlying, trading_date DESC
),
front AS (
    SELECT DISTINCT ON (underlying)
           underlying,
           trading_date,
           10000.0 * median_spread / NULLIF(spot_price, 0) AS front_bps,
           median_relative_spread_pct                      AS front_pct,
           spot_price
      FROM spread_surface_stats
     WHERE underlying = ANY (string_to_array(:'symbols', ','))
       AND option_type = :'option_type'
       AND band_pct = :band::real
       AND money_bucket = 'all'
       AND dte_scope = 'b0'
       AND median_spread IS NOT NULL
     ORDER BY underlying, trading_date DESC, bucket_start_min DESC
)
SELECT COALESCE(c.underlying, f.underlying)         AS sym,
       ROUND(f.spot_price, 0)                       AS spot,
       ROUND(c.chain_bps::numeric, 1)               AS chain_bps,
       ROUND(c.chain_pct::numeric, 2)               AS chain_pct,
       ROUND(f.front_bps::numeric, 1)               AS zero_dte_bps,
       ROUND(f.front_pct::numeric, 2)               AS zero_dte_pct
  FROM chain c
  FULL JOIN front f ON f.underlying = c.underlying
 ORDER BY sym;

\echo ''
\echo '================================================================'
\echo ' 6. CAN ANY OF THIS BE RANKED?  (baseline coverage per symbol)'
\echo '================================================================'
\echo 'Run this before quoting a percentile anywhere. A symbol with four'
\echo 'stored sessions has no distribution, and "the widest of the four'
\echo 'days we have" is not a 100th percentile.'
\echo ''

SELECT d.underlying                                 AS sym,
       COUNT(*) FILTER (WHERE d.contract_count >= :min_contracts)
                                                    AS daily_sessions,
       COUNT(*) FILTER (WHERE d.contract_count <  :min_contracts)
                                                    AS daily_too_thin,
       MIN(d.trading_date)                          AS earliest,
       MAX(d.trading_date)                          AS latest,
       (SELECT COUNT(DISTINCT s.trading_date)
          FROM spread_surface_stats s
         WHERE s.underlying = d.underlying
           AND s.option_type = :'option_type'
           AND s.dte_scope = 'b0'
           AND s.band_pct = :band::real
           AND s.money_bucket = 'all'
           AND s.trading_date > CURRENT_DATE - (:days || ' days')::interval)
                                                    AS zero_dte_sessions
  FROM daily_spread_stats d
 WHERE d.underlying = ANY (string_to_array(:'symbols', ','))
   AND d.option_type = :'option_type'
   AND d.dte_max = :dte_max
   AND d.moneyness_band_pct = :band
   AND d.trading_date > CURRENT_DATE - (:days || ' days')::interval
 GROUP BY d.underlying
 ORDER BY sym;

\echo ''
\echo 'Reminder for anything quoted publicly: these are QUOTED (NBBO)'
\echo 'widths, not effective spreads — what market makers are showing,'
\echo 'not what trades filled at. The feed carries no sizes, so a tight'
\echo 'quote for one contract and for a thousand are identical here.'
\echo ''
