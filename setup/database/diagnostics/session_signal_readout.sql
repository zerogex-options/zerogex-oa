-- ============================================================
-- Session signal readout: what every signal said, and when
-- ============================================================
-- Answers "could anything we ship have seen this move coming?" for
-- one session, from rows the engines already persisted.  Written
-- after the 2026-09-23 sell-off, when that question could only be
-- answered from the code.
--
--   1. The 08:30 ET morning forecast (committed before the open).
--   2. Regime + Trade Bias every 10 minutes, 09:40-13:00 ET.
--      dash_state is trade_bias_scores.market_state -- the same rule
--      the dashboard's Trade Bias panel runs client-side.
--      intraday_* is the backend's intraday tenor, which adds the
--      tactical override the dashboard panel does not have.
--   3. The individual signals every 10 minutes
--      (-100 bearish .. +100 bullish).
--   4. Playbook cards from the prior session's 14:30 ET through
--      13:00 ET, so a card fired into yesterday's close shows up.
--      STAND_DOWN cards are never persisted (src/signals/playbook/
--      cycle.py), so a stretch with no rows means no pattern matched.
--
-- A blank cell means no row within 15 minutes of that time.  A blank
-- flip is the resolver declining to publish -- "GF NA" on the ladder.
--
-- Read-only.
--
-- Invoke (defaults to today, ET):
--   make -s psql < setup/database/diagnostics/session_signal_readout.sql
-- For another session:
--   (echo '\set d 2026-09-22'; cat setup/database/diagnostics/session_signal_readout.sql) | make -s psql
-- ============================================================

\set ON_ERROR_STOP on
\pset pager off
\pset footer off

\if :{?d}
\else
SELECT to_char((now() AT TIME ZONE 'America/New_York')::date, 'YYYY-MM-DD') AS d \gset
\endif

\echo
\echo '== 1. The 08:30 ET morning forecast (committed before the open) =='
SELECT symbol AS sym,
       open_spot::numeric(10,2)          AS spot_0830,
       regime,
       expected_vol_state                AS vol_call,
       expected_vol_ratio::numeric(5,2)  AS vol_x_normal,
       projected_low::numeric(10,2)      AS proj_low,
       projected_high::numeric(10,2)     AS proj_high,
       gamma_flip::numeric(10,2)         AS flip,
       open_msi::numeric(5,1)            AS msi,
       flagship_setup->>'pattern'        AS setup,
       flagship_setup->>'direction'      AS setup_dir
FROM daily_forecast
WHERE date = :'d' AND symbol IN ('SPY', 'SPX')
ORDER BY symbol;

\echo '== 2. Regime + Trade Bias every 10 min (dash_state = what the dashboard Trade Bias panel showed) =='
WITH marks AS (
    SELECT s.sym, g AS t
    FROM (VALUES ('SPY'), ('SPX')) s(sym),
         generate_series((:'d'::date + time '09:40') AT TIME ZONE 'America/New_York',
                         (:'d'::date + time '13:00') AT TIME ZONE 'America/New_York',
                         interval '10 minutes') g
)
SELECT m.sym,
       to_char(m.t AT TIME ZONE 'America/New_York', 'HH24:MI')             AS et,
       q.close::numeric(10,2)                                              AS px,
       (g.total_net_gex / 1e9)::numeric(8,2)                               AS gex_b,
       (g.net_gex_at_spot / 1e9)::numeric(8,2)                             AS gex_spot_b,
       g.gamma_flip_point::numeric(10,2)                                   AS flip,
       s.composite_score::numeric(5,1)                                     AS msi,
       s.direction                                                         AS msi_band,
       (s.components->'__aggregation__'->>'magnitude_score')::numeric(5,1) AS mag,
       b.market_state                                                      AS dash_state,
       i.bias_code                                                         AS intraday_bias,
       i.state                                                             AS intraday_layer,
       i.bias_score::numeric(5,0)                                          AS intraday_score
FROM marks m
LEFT JOIN LATERAL (
    SELECT close FROM underlying_quotes
    WHERE symbol = m.sym AND timestamp <= m.t AND timestamp > m.t - interval '15 minutes'
    ORDER BY timestamp DESC LIMIT 1) q ON TRUE
LEFT JOIN LATERAL (
    SELECT total_net_gex, net_gex_at_spot, gamma_flip_point FROM gex_summary
    WHERE underlying = m.sym AND timestamp <= m.t AND timestamp > m.t - interval '15 minutes'
    ORDER BY timestamp DESC LIMIT 1) g ON TRUE
LEFT JOIN LATERAL (
    SELECT composite_score, direction, components FROM signal_scores
    WHERE underlying = m.sym AND timestamp <= m.t AND timestamp > m.t - interval '15 minutes'
    ORDER BY timestamp DESC LIMIT 1) s ON TRUE
LEFT JOIN LATERAL (
    SELECT market_state FROM trade_bias_scores
    WHERE underlying = m.sym AND tenor = 'swing'
      AND timestamp <= m.t AND timestamp > m.t - interval '15 minutes'
    ORDER BY timestamp DESC LIMIT 1) b ON TRUE
LEFT JOIN LATERAL (
    SELECT bias_code, state, bias_score FROM trade_bias_scores
    WHERE underlying = m.sym AND tenor = 'intraday'
      AND timestamp <= m.t AND timestamp > m.t - interval '15 minutes'
    ORDER BY timestamp DESC LIMIT 1) i ON TRUE
ORDER BY m.sym, m.t;

\echo '== 3. Individual signals every 10 min (-100 bearish .. +100 bullish) =='
-- Advanced/basic signals are deduplicated on write (a row only when the
-- score moves, or on the 5-minute heartbeat), so each one is read as its
-- own latest row rather than all at one shared timestamp.
WITH marks AS (
    SELECT s.sym, g AS t
    FROM (VALUES ('SPY'), ('SPX')) s(sym),
         generate_series((:'d'::date + time '09:40') AT TIME ZONE 'America/New_York',
                         (:'d'::date + time '13:00') AT TIME ZONE 'America/New_York',
                         interval '10 minutes') g
)
SELECT m.sym,
       to_char(m.t AT TIME ZONE 'America/New_York', 'HH24:MI') AS et,
       c.gex_grad, c.tape, c.order_flow, c.dealer_delta, c.vanna_charm, c.odte,
       c.vol_exp, c.range_break, c.range_label, c.mpi, c.mpi_label
FROM marks m
LEFT JOIN LATERAL (
    SELECT
        (100 * max(clamped_score) FILTER (WHERE component_name = 'gex_gradient'))::int                AS gex_grad,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'tape_flow_bias'))::int              AS tape,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'order_flow_imbalance'))::int        AS order_flow,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'dealer_delta_pressure'))::int       AS dealer_delta,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'vanna_charm_flow'))::int            AS vanna_charm,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'zero_dte_position_imbalance'))::int AS odte,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'vol_expansion'))::int               AS vol_exp,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'range_break_imminence'))::int       AS range_break,
        max(context_values->>'label') FILTER (WHERE component_name = 'range_break_imminence')         AS range_label,
        (100 * max(clamped_score) FILTER (WHERE component_name = 'market_pressure'))::int             AS mpi,
        max(context_values->>'label') FILTER (WHERE component_name = 'market_pressure')               AS mpi_label
    FROM (
        SELECT DISTINCT ON (component_name) component_name, clamped_score, context_values
        FROM signal_component_scores
        WHERE underlying = m.sym
          AND component_name IN ('gex_gradient', 'tape_flow_bias', 'order_flow_imbalance',
                                 'dealer_delta_pressure', 'vanna_charm_flow',
                                 'zero_dte_position_imbalance', 'vol_expansion',
                                 'range_break_imminence', 'market_pressure')
          AND timestamp <= m.t AND timestamp > m.t - interval '15 minutes'
        ORDER BY component_name, timestamp DESC
    ) latest
) c ON TRUE
ORDER BY m.sym, m.t;

\echo '== 4. Playbook trade cards, prior session 14:30 ET through 13:00 ET (no rows = no pattern matched) =='
-- Prior weekday; a market holiday just makes the window a day longer.
WITH win AS (
    SELECT ((:'d'::date - CASE extract(isodow FROM :'d'::date)::int
                              WHEN 1 THEN 3 WHEN 7 THEN 2 ELSE 1 END)
            + time '14:30') AT TIME ZONE 'America/New_York' AS t0,
           (:'d'::date + time '13:00') AT TIME ZONE 'America/New_York' AS t1
)
SELECT c.underlying AS sym,
       to_char(c.timestamp AT TIME ZONE 'America/New_York', 'Dy HH24:MI') AS et,
       c.pattern, c.action, c.direction,
       c.confidence::numeric(4,2)      AS conf,
       left(c.payload->>'rationale', 90) AS rationale
FROM signal_action_cards c, win
WHERE c.underlying IN ('SPY', 'SPX')
  AND c.action <> 'STAND_DOWN'
  AND c.timestamp >= win.t0 AND c.timestamp < win.t1
ORDER BY c.timestamp, c.underlying;
