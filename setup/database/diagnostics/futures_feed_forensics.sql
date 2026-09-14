-- Futures feed forensics — "was the ES / NQ chart actually late?"
--
-- Answers a user report of a delayed futures chart AFTER the fact, from the
-- durable evidence in `futures_quotes`. Written for the case the journal
-- cannot cover: service logs are capped (see `make journal-volume`) and a
-- report that arrives hours later routinely outlives them, while these rows
-- survive for FUTURES_BARS_RETENTION_DAYS / DATA_RETENTION_DAYS.
--
-- WHY THE CHART CAN LOOK FINE NOW AND STILL HAVE BEEN LATE THEN
-- ------------------------------------------------------------
-- The ingester reconnects with `barsback` replay
-- (src/ingestion/futures_underlying_ingester.py: `_read_stream` requests
-- poll_barsback bars on every reconnect), so a stream drop BACKFILLS itself
-- the moment it recovers. Re-pull the same window an hour later and the
-- series is complete — which is exactly what "the user saw it, I don't"
-- looks like. The gap is gone; the fingerprint is not.
--
-- The fingerprint is `updated_at`. Both write paths stamp `updated_at = NOW()`
-- on conflict, so the column records WHEN EACH BAR LANDED, independently of
-- the minute it describes:
--
--   write_lag := updated_at - timestamp
--
--   `timestamp` is the bar's OPEN minute. TradeStation stamps a bar at its
--   CLOSE and the ingester re-floors it (`bucket_timestamp(ts - 1s, 60)`),
--   so a bar covering 07:00:00-07:00:59 is stored at 07:00:00 and its final
--   write arrives just after 07:01:00.
--
--   => healthy live write_lag is ~60-75s. It is a minute of bar plus
--      delivery, NOT a fault. Anything materially above that is the feed
--      running late; a block of bars sharing ONE `updated_at` is a replay
--      burst, i.e. a gap that has since healed.
--
-- READ IT IN THIS ORDER: §2 says whether the window was late at all. If it
-- was, §4 and §5 say whether it was a clean outage-then-replay or sustained
-- drift, and §6 says whether the other future was hit too (shared cause:
-- process, network, credential) or only this one (symbol-specific:
-- entitlement, contract, stream slot).
--
-- CAVEATS, both of which fake a huge write_lag:
--   * `src/tools/futures_backfill.py` upserts with `updated_at = NOW()` too.
--     A manual backfill is a replay burst spanning hours or days — §5 prints
--     the span so you can tell it from a reconnect, which spans minutes.
--   * A window older than retention is PRUNED, not late. §1 shows the
--     retained range: if the incident falls outside it, every later section
--     is empty and that is absence of evidence, not evidence of health.
--
-- Read-only: SELECTs only, no temp objects, safe against prod.
--
-- Run via `make futures-forensics` (see that target for the arguments), or
-- directly:
--   psql ... -v index_symbol=NDX -v incident_date=2026-09-14 \
--            -f setup/database/diagnostics/futures_feed_forensics.sql

-- NOTE for editors: psql's \echo runs `backticks` as a SHELL command and
-- treats an unpaired apostrophe as an unterminated string, which aborts the
-- script at that line. Keep both out of every \echo below.
\pset pager off
\pset border 2

-- Parameters. Each is overridable with `psql -v name=value`; the guards below
-- only fill in a default when the caller did not pass one.
--
-- index_symbol  cash index whose future to audit — NDX (@NQ) or SPX (@ES).
--               futures_quotes is keyed by the INDEX, not the contract.
-- peer_symbol   the other future, as a control for §6.
-- incident_date calendar date of the report, in local_tz. 'today' is valid.
-- open_local    the local cash open the user anchored the report to.
-- local_tz      timezone that open is quoted in (London cash open: 08:00
--               Europe/London — DST-correct, so this resolves to 03:00 ET in
--               BST and 02:00 ET in GMT without the caller doing the math).
-- pre_min/post_min   window padding either side of that open, in minutes.
-- lag_warn_sec  write_lag above which a bar is called late. 90s = the ~60-75s
--               healthy band plus headroom for normal delivery jitter.
-- history_days  lookback for the recurring-pattern check in §7.
\if :{?index_symbol}  \else \set index_symbol  NDX          \endif
\if :{?peer_symbol}   \else \set peer_symbol   SPX          \endif
\if :{?incident_date} \else \set incident_date today        \endif
\if :{?open_local}    \else \set open_local    08:00        \endif
\if :{?local_tz}      \else \set local_tz      Europe/London \endif
\if :{?pre_min}       \else \set pre_min       45           \endif
\if :{?post_min}      \else \set post_min      120          \endif
\if :{?lag_warn_sec}  \else \set lag_warn_sec  90           \endif
\if :{?history_days}  \else \set history_days  7            \endif

\echo
\echo ================================================================
\echo  Futures feed forensics
\echo ================================================================
\echo  Symbol under audit / control symbol:
\echo    :index_symbol  vs  :peer_symbol
\echo  Anchored on :open_local :local_tz on :incident_date,
\echo    from :pre_min min before to :post_min min after.
\echo  Late threshold: write_lag > :lag_warn_sec s.
\echo

\echo
\echo ================================================================
\echo  1. Retention + coverage: is the incident window even still here?
\echo ================================================================
\echo  Every row futures_quotes holds, per index. If the window below sits
\echo  outside [first_bar, last_bar] the data was PRUNED and the rest of
\echo  this report proves nothing.
\echo

WITH params AS (
    SELECT (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
)
SELECT
    f.index_symbol,
    string_agg(DISTINCT f.future_symbol, ', ')            AS contracts,
    count(*)                                              AS bars,
    min(f.timestamp) AT TIME ZONE :'local_tz'             AS first_bar_local,
    max(f.timestamp) AT TIME ZONE :'local_tz'             AS last_bar_local,
    round(extract(epoch FROM now() - max(f.timestamp)) / 60.0, 1) AS last_bar_age_min,
    CASE
        WHEN min(f.timestamp) > p.cash_open - (:pre_min * interval '1 minute')
            THEN 'WINDOW PARTLY/FULLY PRUNED — sections below are incomplete'
        ELSE 'window is inside retention'
    END                                                   AS verdict
FROM futures_quotes f
CROSS JOIN params p
GROUP BY f.index_symbol, p.cash_open
ORDER BY f.index_symbol;

\echo
\echo ================================================================
\echo  2. HEADLINE — write-lag distribution across the incident window
\echo ================================================================
\echo  Healthy live band is ~60-75s (one minute of bar + delivery).
\echo  A p50 in that band with a high max = a brief stall.
\echo  A p50 well above it = the whole window ran late.
\echo  bars_late / bars_expected is the share of the window the user
\echo  would have seen as stale; bars_missing never had a row written.
\echo  NOTE bars_expected counts every minute in the window, which is the
\echo  right expectation only INSIDE a CME session. Point this at the
\echo  17:00-18:00 ET maintenance break or a weekend and it will report
\echo  missing bars for minutes the market was simply shut.
\echo

WITH params AS (
    SELECT
        (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
), win AS (
    SELECT
        cash_open - (:pre_min  * interval '1 minute') AS lo,
        cash_open + (:post_min * interval '1 minute') AS hi
    FROM params
), bars AS (
    SELECT extract(epoch FROM f.updated_at - f.timestamp) AS lag_s
    FROM futures_quotes f, win w
    WHERE f.index_symbol = :'index_symbol'
      AND f.timestamp >= w.lo AND f.timestamp < w.hi
)
SELECT
    (SELECT count(*) FROM bars)                                    AS bars_present,
    (SELECT round(extract(epoch FROM (hi - lo)) / 60) FROM win)    AS bars_expected,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY lag_s)::numeric, 1) AS p50_lag_s,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY lag_s)::numeric, 1) AS p95_lag_s,
    round(max(lag_s)::numeric, 1)                                  AS max_lag_s,
    count(*) FILTER (WHERE lag_s > :lag_warn_sec)                  AS bars_late,
    (SELECT round(extract(epoch FROM (hi - lo)) / 60) FROM win) - count(*) AS bars_missing,
    CASE
        WHEN count(*) = 0 THEN 'NO BARS AT ALL — feed was down, or ' || :'index_symbol'
                                  || ' is not in INGEST_FUTURES_INDEXES'
        -- Missing bars are invisible to the lag filter (no row, no lag), so
        -- they are tested FIRST. Reporting "clean" here would be a false
        -- all-clear on a feed that dropped minutes outright.
        WHEN count(*) < (SELECT extract(epoch FROM (hi - lo)) / 60 FROM win)
             AND count(*) FILTER (WHERE lag_s > :lag_warn_sec) > 0
            THEN 'LATE *AND* MISSING BARS — see sections 3, 4, 5'
        WHEN count(*) < (SELECT extract(epoch FROM (hi - lo)) / 60 FROM win)
            THEN 'BARS MISSING (not merely late) — see section 4'
        WHEN count(*) FILTER (WHERE lag_s > :lag_warn_sec) > 0
            THEN 'LATE BARS PRESENT — see sections 3, 4, 5'
        ELSE 'clean — every minute present, none landed late'
    END                                                            AS verdict
FROM bars;

\echo
\echo ================================================================
\echo  3. Per-minute detail across the window
\echo ================================================================
\echo  One row per stored bar. gap_min is the distance back to the
\echo  previous bar: 1 is contiguous, anything higher is missing minutes.
\echo  Times are shown in :local_tz so they line up with the report.
\echo

WITH params AS (
    SELECT (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
), win AS (
    SELECT
        cash_open - (:pre_min  * interval '1 minute') AS lo,
        cash_open + (:post_min * interval '1 minute') AS hi
    FROM params
)
SELECT
    f.timestamp  AT TIME ZONE :'local_tz'                     AS bar_local,
    f.updated_at AT TIME ZONE :'local_tz'                     AS written_local,
    round(extract(epoch FROM f.updated_at - f.timestamp)::numeric, 1) AS write_lag_s,
    round(extract(epoch FROM f.timestamp
          - lag(f.timestamp) OVER (ORDER BY f.timestamp)) / 60.0)     AS gap_min,
    f.future_symbol,
    f.close,
    CASE WHEN extract(epoch FROM f.updated_at - f.timestamp) > :lag_warn_sec
         THEN '<< LATE' ELSE '' END                           AS flag
FROM futures_quotes f, win w
WHERE f.index_symbol = :'index_symbol'
  AND f.timestamp >= w.lo AND f.timestamp < w.hi
ORDER BY f.timestamp;

\echo
\echo ================================================================
\echo  4. Missing minutes (gaps that were never filled)
\echo ================================================================
\echo  Runs of absent bars. A gap still open here means the minutes were
\echo  never recovered at all; a gap that healed shows up in section 5
\echo  instead, because the bars exist but landed late.
\echo  Expected: 0 rows during a CME session.
\echo

WITH params AS (
    SELECT (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
), win AS (
    SELECT
        cash_open - (:pre_min  * interval '1 minute') AS lo,
        cash_open + (:post_min * interval '1 minute') AS hi
    FROM params
), seq AS (
    SELECT
        f.timestamp                                        AS ts,
        lead(f.timestamp) OVER (ORDER BY f.timestamp)      AS next_ts
    FROM futures_quotes f, win w
    WHERE f.index_symbol = :'index_symbol'
      AND f.timestamp >= w.lo AND f.timestamp < w.hi
)
SELECT
    ts      AT TIME ZONE :'local_tz'                        AS gap_starts_after_local,
    next_ts AT TIME ZONE :'local_tz'                        AS resumes_at_local,
    round(extract(epoch FROM next_ts - ts) / 60.0)::int - 1 AS minutes_missing
FROM seq
WHERE next_ts IS NOT NULL
  AND next_ts - ts > interval '1 minute'
ORDER BY ts;

\echo
\echo ================================================================
\echo  5. Replay bursts — gaps that healed, and when they healed
\echo ================================================================
\echo  Bars grouped by the SECOND they were written. A burst of many bars
\echo  sharing one write instant is a reconnect replaying barsback:
\echo  those minutes were MISSING from the chart until written_local, which
\echo
\echo  is the delay the user saw and the reason it is invisible now.
\echo  span_min tells a reconnect (minutes) from a manual futures_backfill
\echo  run (hours/days). Expected: 0 rows.
\echo

WITH params AS (
    SELECT (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
), win AS (
    SELECT
        cash_open - (:pre_min  * interval '1 minute') AS lo,
        cash_open + (:post_min * interval '1 minute') AS hi
    FROM params
)
SELECT
    f.updated_at AT TIME ZONE :'local_tz'                   AS written_local,
    count(*)                                                AS bars_in_burst,
    min(f.timestamp) AT TIME ZONE :'local_tz'               AS covering_from_local,
    max(f.timestamp) AT TIME ZONE :'local_tz'               AS covering_to_local,
    round(extract(epoch FROM max(f.timestamp) - min(f.timestamp)) / 60.0)::int AS span_min,
    round(max(extract(epoch FROM f.updated_at - f.timestamp))::numeric / 60.0, 1) AS worst_lag_min
FROM futures_quotes f, win w
WHERE f.index_symbol = :'index_symbol'
  AND f.timestamp >= w.lo AND f.timestamp < w.hi
GROUP BY f.updated_at
HAVING count(*) > 1
ORDER BY f.updated_at;

\echo
\echo ================================================================
\echo  6. CONTROL — the other future over the same window
\echo ================================================================
\echo  Both late  => shared cause: the ingestion process, the box, the
\echo                network, or the shared TradeStation credential.
\echo  Only one   => symbol-specific: the CME real-time entitlement on
\echo                that contract, a stream-slot cap, or the contract
\echo                itself. Note the futures feeds may run under their own
\echo                TRADESTATION_FUTURES_REFRESH_TOKEN (see
\echo                config.futures_tradestation_credentials) — an account
\echo                without the real-time CME entitlement is served
\echo                DELAYED data rather than no data, which looks exactly
\echo                like this report.
\echo

WITH params AS (
    SELECT (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
), win AS (
    SELECT
        cash_open - (:pre_min  * interval '1 minute') AS lo,
        cash_open + (:post_min * interval '1 minute') AS hi
    FROM params
)
SELECT
    f.index_symbol,
    string_agg(DISTINCT f.future_symbol, ', ')                     AS contract,
    count(*)                                                       AS bars_present,
    (SELECT round(extract(epoch FROM (hi - lo)) / 60) FROM win)    AS bars_expected,
    round(percentile_cont(0.50) WITHIN GROUP (
        ORDER BY extract(epoch FROM f.updated_at - f.timestamp))::numeric, 1) AS p50_lag_s,
    round(max(extract(epoch FROM f.updated_at - f.timestamp))::numeric, 1)    AS max_lag_s,
    count(*) FILTER (
        WHERE extract(epoch FROM f.updated_at - f.timestamp) > :lag_warn_sec) AS bars_late
FROM futures_quotes f, win w
WHERE f.index_symbol IN (:'index_symbol', :'peer_symbol')
  AND f.timestamp >= w.lo AND f.timestamp < w.hi
GROUP BY f.index_symbol
ORDER BY f.index_symbol;

\echo
\echo ================================================================
\echo  7. Is this hour ALWAYS bad? Lag by local hour, last :history_days days
\echo ================================================================
\echo  Separates a one-off from a standing pattern at this time of day.
\echo  days_with_late of days_seen is the discriminator: 1-of-7 is an
\echo  incident, 7-of-7 is a scheduled cause — token refresh, a
\echo  maintenance window, or a nightly job sharing the box.
\echo

SELECT
    date_trunc('hour', f.timestamp AT TIME ZONE :'local_tz')::time AS local_hour,
    count(*)                                                       AS bars,
    round(percentile_cont(0.50) WITHIN GROUP (
        ORDER BY extract(epoch FROM f.updated_at - f.timestamp))::numeric, 1) AS p50_lag_s,
    round(percentile_cont(0.95) WITHIN GROUP (
        ORDER BY extract(epoch FROM f.updated_at - f.timestamp))::numeric, 1) AS p95_lag_s,
    count(*) FILTER (
        WHERE extract(epoch FROM f.updated_at - f.timestamp) > :lag_warn_sec) AS bars_late,
    count(DISTINCT (f.timestamp AT TIME ZONE :'local_tz')::date)   AS days_seen,
    -- The discriminator: 1-of-N days is an incident, N-of-N is a pattern.
    count(DISTINCT (f.timestamp AT TIME ZONE :'local_tz')::date) FILTER (
        WHERE extract(epoch FROM f.updated_at - f.timestamp) > :lag_warn_sec) AS days_with_late,
    max((f.timestamp AT TIME ZONE :'local_tz')::date) FILTER (
        WHERE extract(epoch FROM f.updated_at - f.timestamp) > :lag_warn_sec) AS worst_day
FROM futures_quotes f
WHERE f.index_symbol = :'index_symbol'
  AND f.timestamp >= now() - (:history_days * interval '1 day')
GROUP BY 1
ORDER BY 1;

\echo
\echo ================================================================
\echo  8. TradeStation API pressure around the window
\echo ================================================================
\echo  Counts are summed across every ingestion process sharing the
\echo  5-minute window, so a spike here means the rate-limit governor was
\echo  throttling the whole box — which delays the futures stream without
\echo  any futures-specific fault. Flat counts rule this out.
\echo

WITH params AS (
    SELECT (:'incident_date'::date + :'open_local'::time) AT TIME ZONE :'local_tz' AS cash_open
), win AS (
    SELECT
        cash_open - (:pre_min  * interval '1 minute') AS lo,
        cash_open + (:post_min * interval '1 minute') AS hi
    FROM params
)
SELECT
    t.window_start AT TIME ZONE :'local_tz' AS window_local,
    t.call_count
FROM tradestation_api_calls t, win w
WHERE t.window_start >= w.lo AND t.window_start < w.hi
ORDER BY t.window_start;

\echo
\echo ================================================================
\echo  Done.
\echo ================================================================
\echo  If section 2 says the window was clean and section 4 is empty, the
\echo  feed was on time and the delay was downstream of the DB — the API
\echo  response, the CDN, or the browser session on their end. Ask them for
\echo  the freshness envelope from /api/v2/market/historical?symbol=NQ at
\echo  the time (source_timestamp vs evaluated_at) before digging further.
\echo
