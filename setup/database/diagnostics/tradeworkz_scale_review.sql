-- TradeWorkz scale-out ladder — session review.
--
-- A per-bot readout of the scale-out ladder's activity for the most recent
-- trading session: how many open positions armed the ladder, how many trades
-- actually scaled, the per-trade tranche ledger, and — the headline — whether
-- scaling BEAT or COST versus holding the whole position to the same final
-- exit. Complements the accounting audit in tradeworkz_invariants.sql.
--
-- Run on-demand:  make tradeworkz-review
-- See docs/design/tradeworkz-exit-strategy.md.

\pset pager off
\pset border 2

-- Pin the review to the most recent ET session that has a live (non-sim)
-- close, so it reads correctly whether you run it during, right after, or the
-- morning after a session. Falls back to today if there are no live closes.
SELECT COALESCE(
         MAX((closed_at AT TIME ZONE 'America/New_York')::date),
         (NOW() AT TIME ZONE 'America/New_York')::date
       ) AS session_day
FROM tw_trades
WHERE (components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'
  AND (components_at_entry ->> 'simulated') IS DISTINCT FROM 'true'
\gset

\echo
\echo ============================================================
\echo  TradeWorkz scale-out review
\echo ============================================================
\echo  Session under review (ET) shown below; all closed-trade sections are
\echo  scoped to it. Section 1 is a LIVE snapshot of positions open right now.
\echo

SELECT :'session_day'::date AS session_under_review;

\echo
\echo ============================================================
\echo  1. Arming snapshot — positions OPEN right now (live)
\echo ============================================================
\echo  armed = ladder geometry frozen (t1_trigger_price set). The cap is
\echo  working when avg_t1_dist_pct hugs the ~0.8% envelope while the raw
\echo  target sits further out (avg_target_dist_pct). Empty after the RTH close.
\echo

SELECT bot_id,
       COUNT(*) AS open_pos,
       COUNT(*) FILTER (WHERE t1_trigger_price IS NOT NULL) AS armed,
       COUNT(*) FILTER (WHERE COALESCE(scale_stage, 0) > 0) AS scaling_now,
       ROUND(AVG(CASE WHEN t1_trigger_price IS NOT NULL AND entry_spot > 0
                      THEN ABS(t1_trigger_price - entry_spot) / entry_spot * 100 END), 2)
         AS avg_t1_dist_pct,
       ROUND(AVG(CASE WHEN target_price IS NOT NULL AND entry_spot > 0
                      THEN ABS(target_price - entry_spot) / entry_spot * 100 END), 2)
         AS avg_target_dist_pct
FROM tw_positions
GROUP BY bot_id
ORDER BY bot_id;

\echo
\echo ============================================================
\echo  2. Per-bot ladder activity for the session (live trades only)
\echo ============================================================
\echo  scaled        = trades that took at least one tranche (T1/T2).
\echo  runner_exits  = closed on the runner stop (trail or S2 floor).
\echo  scaling_pnl_benefit = realized MINUS holding the whole position to the
\echo    same final fill. >0 = scaling helped (banked a reversal before it
\echo    round-tripped); <0 = scaling capped a trend you would have ridden.
\echo    (This isolates scale-vs-no-scale at the same exit; it is not a
\echo    counterfactual against the far structural target.)
\echo

SELECT bot_id,
       COUNT(*) AS closes,
       COUNT(*) FILTER (
         WHERE COALESCE((components_at_exit ->> 'scale_stage')::int, 0) > 0
       ) AS scaled,
       COUNT(*) FILTER (
         WHERE close_reason IN ('runner_trail_stop', 'runner_s2_stop')
       ) AS runner_exits,
       ROUND(SUM(realized_pnl)::numeric, 2) AS realized_pnl,
       ROUND(SUM(
         CASE WHEN COALESCE((components_at_exit ->> 'scale_stage')::int, 0) > 0
                   AND (components_at_exit ->> 'final_fill') IS NOT NULL
              THEN realized_pnl
                   - ((components_at_exit ->> 'final_fill')::numeric - entry_price)
                     * quantity * 100
              ELSE 0 END
       )::numeric, 2) AS scaling_pnl_benefit
FROM tw_trades
WHERE (closed_at AT TIME ZONE 'America/New_York')::date = :'session_day'::date
  AND (components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'
  AND (components_at_entry ->> 'simulated') IS DISTINCT FROM 'true'
GROUP BY bot_id
ORDER BY scaled DESC, scaling_pnl_benefit DESC NULLS LAST;

\echo
\echo ============================================================
\echo  3. Per scaled trade — the tranche ledger
\echo ============================================================
\echo  wt_exit    = size-weighted consolidated exit (what the one trade row
\echo               records). final_fill = the last runner tranche fill.
\echo  tranches   = every take, in order (stage / price / qty / reason).
\echo

SELECT (closed_at AT TIME ZONE 'America/New_York')::time(0) AS closed_et,
       bot_id, underlying, quantity,
       entry_price,
       ROUND(exit_price, 4) AS wt_exit,
       (components_at_exit ->> 'final_fill')::numeric AS final_fill,
       ROUND(realized_pnl::numeric, 2) AS realized,
       ROUND(pnl_percent * 100, 1) AS pnl_pct,
       close_reason,
       (components_at_exit ->> 'scale_stage') AS stage,
       jsonb_array_length(components_at_exit -> 'tranches') AS n_tranches,
       components_at_exit -> 'tranches' AS tranches
FROM tw_trades
WHERE (closed_at AT TIME ZONE 'America/New_York')::date = :'session_day'::date
  AND (components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'
  AND (components_at_entry ->> 'simulated') IS DISTINCT FROM 'true'
  AND COALESCE((components_at_exit ->> 'scale_stage')::int, 0) > 0
ORDER BY closed_at DESC
LIMIT 100;

\echo
\echo ============================================================
\echo  4. Scale-event tape (only populated when a bot has followers)
\echo ============================================================
\echo  The authoritative record is section 3. This fanout tape is empty when
\echo  the bots have no followers (fanout only writes rows for opted-in users).
\echo  One row per (position, stage).
\echo

SELECT DISTINCT ON (position_id, payload ->> 'stage')
       (sent_at AT TIME ZONE 'America/New_York')::time(0) AS t_et,
       bot_id, position_id,
       payload ->> 'stage'      AS stage,
       payload ->> 'reason'     AS reason,
       payload ->> 'contracts'  AS cut,
       payload ->> 'remaining'  AS remaining,
       ROUND((payload ->> 'realized_pnl')::numeric, 2) AS tranche_pnl
FROM tw_notifications_log
WHERE event_type = 'scale'
  AND (sent_at AT TIME ZONE 'America/New_York')::date = :'session_day'::date
ORDER BY position_id, payload ->> 'stage', sent_at DESC;

\echo
\echo ============================================================
\echo  End of scale-out review.
\echo ============================================================
