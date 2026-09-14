-- Which contract month is the futures feed actually on, and did it splice?
--
-- Companion to futures_feed_forensics.sql. That one asks whether bars arrived
-- ON TIME; this one asks whether they are the RIGHT PRICE — a different
-- failure, and one the timing report cannot see, because a feed quoting the
-- wrong contract month is perfectly punctual about it.
--
-- WHY THIS COMES UP
-- -----------------
-- INDEX_FUTURES_MAP points at TradeStation CONTINUOUS contracts (@ES, @NQ),
-- so the provider rolls for us (src/symbols.py). CME equity-index futures
-- roll 8 days before the third Friday of Mar/Jun/Sep/Dec, and across that
-- roll the continuous series switches from the expiring contract to the next
-- one — which trades at a cost-of-carry premium:
--
--     F = S x e^((r - q) x T)     T = time to the future's expiry
--
-- So the quote legitimately JUMPS by one quarter of carry on the roll date,
-- and for the rest of the quarter it sits that far above any platform still
-- displaying the old contract. On a Sep->Dec roll that is roughly +0.8% on
-- ES and +1.0% on NQ — NQ is larger because NDX pays less dividend than SPX,
-- so its net carry (r - q) is higher. A report that "the quote is wildly off
-- versus TradingView" is usually this, not a feed fault.
--
-- WHAT TO CONCLUDE
-- ----------------
-- §1 measures the basis ratio (future / index) per day. Its LEVEL says which
-- contract you are on: near 1.000 is an expiring contract, and roughly
-- 1.010 is one a quarter out. A step in that ratio on one day, with no
-- matching step in the cash index, is the roll splice.
--
-- §2 isolates the splice directly: an overnight jump in the FUTURE that the
-- INDEX did not make. The index never rolls, so it is the control — a real
-- market move shows in both, a splice shows only in the future.
--
-- §3 backs out the implied (r - q) from the current basis and compares it to
-- the configured RISK_FREE_RATE / DIVIDEND_YIELD_BY_SYMBOL. Agreement means
-- the price is a real contract at a sane carry. A ratio far from both 1.000
-- and the carry estimate is the back-adjusted-series case the projection
-- engine guards against with FUTURES_BASIS_MAX_DEVIATION (default 0.03) —
-- a back-adjusted quote is not a tradable level.
--
-- A splice is NORMAL and expected. What it costs you is continuity: a
-- multi-day futures chart drawn across it has a step in it that is not a
-- market move, and any change-vs-prior-day computed across it is wrong by
-- the carry. Intraday display and same-session change are unaffected.
--
-- NOTE for editors: psql's \echo runs `backticks` as a SHELL command and
-- treats an unpaired apostrophe as an unterminated string. Keep both out.
--
-- Read-only. Run via `make futures-roll-check`, or directly:
--   psql ... -v index_symbol=NDX -f setup/database/diagnostics/futures_contract_roll.sql

\pset pager off
\pset border 2

-- index_symbol  cash index to audit (NDX -> @NQ, SPX -> @ES).
-- history_days  how far back to walk. Must span the roll you care about.
-- local_tz      timezone for display.
-- jump_bps      overnight future-vs-index divergence, in basis points, that
--               counts as a splice. 25bp is far above normal overnight basis
--               drift and far below a quarterly roll (~80-100bp).
\if :{?index_symbol} \else \set index_symbol NDX          \endif
\if :{?history_days} \else \set history_days 75           \endif
\if :{?local_tz}     \else \set local_tz     America/New_York \endif
\if :{?jump_bps}     \else \set jump_bps     25           \endif
-- roll_days_before_expiry  CME equity-index futures roll this many days
--               ahead of the third Friday. Used to pick the ACTIVE
--               contract's expiry rather than the expiring one.
\if :{?roll_days_before_expiry} \else \set roll_days_before_expiry 8 \endif

\echo
\echo ================================================================
\echo  Futures contract / roll check for :index_symbol
\echo ================================================================
\echo  Looking back :history_days days. Splice threshold :jump_bps bps.
\echo

\echo
\echo ================================================================
\echo  1. Daily basis ratio (future / index) — which contract am I on?
\echo ================================================================
\echo  Ratio near 1.000 = an expiring contract. Around 1.010 = one
\echo  quarter out. ratio_step_bps is the day-over-day change: a large
\echo  step on a single day is the roll.
\echo  Measured on CONCURRENT minutes only, so it needs the cash session
\echo  (the index does not print overnight) — expect one row per
\echo  trading day, none for weekends or holidays.
\echo

WITH paired AS (
    SELECT
        (f.timestamp AT TIME ZONE :'local_tz')::date AS local_day,
        f.close::numeric / u.close::numeric          AS ratio
    FROM futures_quotes f
    JOIN underlying_quotes u
      ON u.symbol = f.index_symbol
     AND u.timestamp = f.timestamp
    WHERE f.index_symbol = :'index_symbol'
      AND f.timestamp >= now() - (:history_days * interval '1 day')
      AND u.close > 0
), daily AS (
    SELECT
        local_day,
        count(*)                                                        AS concurrent_minutes,
        round(percentile_cont(0.50) WITHIN GROUP (ORDER BY ratio)::numeric, 6) AS median_ratio
    FROM paired
    GROUP BY local_day
)
SELECT
    local_day,
    concurrent_minutes,
    median_ratio,
    round((median_ratio - 1) * 10000, 1)                                AS basis_bps,
    round((median_ratio - lag(median_ratio) OVER (ORDER BY local_day))
          * 10000, 1)                                                   AS ratio_step_bps,
    CASE
        WHEN abs((median_ratio - lag(median_ratio) OVER (ORDER BY local_day))
                 * 10000) >= :jump_bps THEN '<< STEP — likely the roll'
        ELSE ''
    END                                                                 AS flag
FROM daily
ORDER BY local_day;

\echo
\echo ================================================================
\echo  2. Splice detector — the future gapped, the index did not
\echo ================================================================
\echo  Session-over-session change in each series, side by side. The cash
\echo  index NEVER rolls, so it is the control: a real market move shows
\echo  in both columns, a contract splice shows only in the future.
\echo  divergence_bps is the difference. Expected: 0 rows between rolls.
\echo

WITH f_daily AS (
    SELECT DISTINCT ON ((timestamp AT TIME ZONE :'local_tz')::date)
        (timestamp AT TIME ZONE :'local_tz')::date AS local_day,
        close::numeric                             AS fut_close
    FROM futures_quotes
    WHERE index_symbol = :'index_symbol'
      AND timestamp >= now() - (:history_days * interval '1 day')
    ORDER BY (timestamp AT TIME ZONE :'local_tz')::date, timestamp DESC
), u_daily AS (
    SELECT DISTINCT ON ((timestamp AT TIME ZONE :'local_tz')::date)
        (timestamp AT TIME ZONE :'local_tz')::date AS local_day,
        close::numeric                             AS idx_close
    FROM underlying_quotes
    WHERE symbol = :'index_symbol'
      AND timestamp >= now() - (:history_days * interval '1 day')
    ORDER BY (timestamp AT TIME ZONE :'local_tz')::date, timestamp DESC
), joined AS (
    SELECT
        f.local_day,
        f.fut_close,
        u.idx_close,
        lag(f.fut_close) OVER (ORDER BY f.local_day) AS prev_fut,
        lag(u.idx_close) OVER (ORDER BY f.local_day) AS prev_idx
    FROM f_daily f
    JOIN u_daily u USING (local_day)
)
SELECT
    local_day,
    prev_fut, fut_close,
    round((fut_close / prev_fut - 1) * 10000, 1) AS future_move_bps,
    round((idx_close / prev_idx - 1) * 10000, 1) AS index_move_bps,
    round(((fut_close / prev_fut) - (idx_close / prev_idx)) * 10000, 1) AS divergence_bps,
    round(fut_close - prev_fut, 2)               AS future_points
FROM joined
WHERE prev_fut IS NOT NULL AND prev_idx IS NOT NULL
  AND abs(((fut_close / prev_fut) - (idx_close / prev_idx)) * 10000) >= :jump_bps
ORDER BY local_day;

\echo
\echo ================================================================
\echo  3. Current basis vs theoretical carry
\echo ================================================================
\echo  implied_r_minus_q is what the live basis says the market is
\echo  charging to carry to the assumed expiry. Compare it against
\echo  RISK_FREE_RATE minus this index DIVIDEND_YIELD_BY_SYMBOL entry
\echo  (defaults: r=0.05; q unset means 0.0, so set it per symbol or the
\echo  theoretical fallback overstates the premium).
\echo  Agreement = a real contract priced at a sane carry. A basis far
\echo  from both 0 bps and the carry estimate is the back-adjusted-series
\echo  case that FUTURES_BASIS_MAX_DEVIATION exists to reject.
\echo  expiry_assumed is the first quarterly third Friday past its roll
\echo  date, which is the contract a rolled feed is quoting. If YOUR feed
\echo  has not rolled yet, the real expiry is the nearer one and the
\echo  implied rate here reads far too LOW — cross-check against the
\echo  basis_bps level in section 1.
\echo

WITH latest_pair AS (
    SELECT f.timestamp, f.close::numeric AS fut, u.close::numeric AS idx
    FROM futures_quotes f
    JOIN underlying_quotes u
      ON u.symbol = f.index_symbol AND u.timestamp = f.timestamp
    WHERE f.index_symbol = :'index_symbol' AND u.close > 0
    ORDER BY f.timestamp DESC
    LIMIT 1
), expiry AS (
    -- Third Friday of a quarterly month (Mar/Jun/Sep/Dec), taking the first
    -- one that is PAST ITS ROLL DATE. CME equity-index futures roll 8 days
    -- before expiry, so inside that window the nearest third Friday belongs
    -- to the contract the feed has already left; using it annualises a whole
    -- quarter of carry over a few days and implies a triple-digit rate.
    SELECT min(d)::date AS exp_date
    FROM generate_series(date_trunc('day', now()), now() + interval '400 days', interval '1 day') d
    WHERE extract(month FROM d)::int IN (3, 6, 9, 12)
      AND extract(dow   FROM d)::int = 5
      AND extract(day   FROM d)::int BETWEEN 15 AND 21
      AND d > now() + (:roll_days_before_expiry * interval '1 day')
)
SELECT
    p.timestamp AT TIME ZONE :'local_tz'                     AS as_of_local,
    p.fut                                                    AS future_close,
    p.idx                                                    AS index_close,
    round(p.fut - p.idx, 2)                                  AS basis_points,
    round((p.fut / p.idx - 1) * 10000, 1)                    AS basis_bps,
    e.exp_date                                               AS expiry_assumed,
    round(((e.exp_date - now()::date) / 365.0)::numeric, 4)  AS years_to_expiry,
    round((ln(p.fut / p.idx) / ((e.exp_date - now()::date) / 365.0))::numeric * 100, 2)
                                                             AS implied_r_minus_q_pct
FROM latest_pair p CROSS JOIN expiry e;

\echo
\echo ================================================================
\echo  Done.
\echo ================================================================
\echo  A step in section 1 with a matching row in section 2 is a normal
\echo  quarterly roll: the quote is the NEW contract and is correct, and
\echo  any platform still showing the old one will sit a quarter of carry
\echo  below it until that contract expires. What it does cost you is a
\echo  step in multi-day futures charts and in any change computed across
\echo  the roll date.
\echo
