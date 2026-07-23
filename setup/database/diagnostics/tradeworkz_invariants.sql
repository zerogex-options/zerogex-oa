-- TradeWorkz accounting invariants.
--
-- Every SELECT in this file MUST return zero rows on a healthy DB. Any
-- non-zero result means the engine's P&L math has drifted from what the
-- persisted per-trade columns record, OR the equity rollup has failed,
-- OR a stale connection wrote a row that never reached its downstream
-- table.
--
-- Runs on-demand via `make tradeworkz-check`. Not scheduled — the
-- operator invokes this after suspicious state (a leaderboard number
-- that doesn't match what they expected, a P&L math change, an
-- overnight process crash). Rerun after every code change that touches
-- reconciler.py or simulate.py.
--
-- Each check is preceded by a `\echo` heading and terminated with a
-- trailing echo so the output reads as a report, not a wall of rows.

\pset pager off
\pset border 2

\echo
\echo ============================================================
\echo  Invariant A: realized_pnl arithmetic is self-consistent
\echo ============================================================
\echo  Every closed-trade realized_pnl must equal
\echo    (exit - entry) * quantity * 100
\echo  for LONG-DEBIT positions (every current bot; check per-leg
\echo  side if you add short structures). Live rows only - sim rows
\echo  use a synthesized pnl_pct that does not follow this formula.
\echo  Expected: 0 rows.
\echo

SELECT id, bot_id, direction, entry_price, exit_price, quantity,
       realized_pnl,
       ROUND(((exit_price - entry_price) * quantity * 100)::numeric, 2)
         AS expected_realized_pnl
FROM tw_trades
WHERE closed_at >= NOW() - interval '90 days'
  AND (components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'
  AND (components_at_entry ->> 'simulated') IS DISTINCT FROM 'true'
  AND ABS(realized_pnl - (exit_price - entry_price) * quantity * 100) > 0.50
ORDER BY closed_at DESC
LIMIT 100;

\echo
\echo ============================================================
\echo  Invariant B: current_capital reconciles to sum of live trades
\echo ============================================================
\echo  For each bot:
\echo    current_capital - starting_capital
\echo      MUST equal SUM(realized_pnl) across every LIVE closed trade
\echo  Any drift means a close() half-wrote (updated tw_trades but not
\echo  tw_bot_capital, or the other way around).
\echo  Expected: 0 rows.
\echo

SELECT c.bot_id, c.starting_capital, c.current_capital,
       COALESCE(agg.sum_realized_live_only, 0) AS sum_realized_live_only,
       ROUND((c.current_capital - c.starting_capital
              - COALESCE(agg.sum_realized_live_only, 0))::numeric, 2) AS drift
FROM tw_bot_capital c
LEFT JOIN (
  SELECT bot_id,
         SUM(CASE WHEN (components_at_entry ->> 'origin')
                        IS DISTINCT FROM 'simulate'
                    AND (components_at_entry ->> 'simulated')
                        IS DISTINCT FROM 'true'
                  THEN realized_pnl ELSE 0 END) AS sum_realized_live_only
  FROM tw_trades GROUP BY bot_id
) agg ON agg.bot_id = c.bot_id
WHERE ABS(c.current_capital - c.starting_capital
          - COALESCE(agg.sum_realized_live_only, 0)) > 0.01
ORDER BY drift DESC;

\echo
\echo ============================================================
\echo  Invariant C: outcome column matches realized_pnl sign
\echo ============================================================
\echo  outcome=win iff realized_pnl > 0; loss iff below 0; scratch iff zero.
\echo  If these ever mismatch, the leaderboard win-rate is wrong.
\echo  Expected: 0 rows.
\echo

SELECT id, bot_id, outcome, realized_pnl
FROM tw_trades
WHERE closed_at >= NOW() - interval '90 days'
  AND (
    (outcome = 'win'     AND realized_pnl <= 0)
    OR (outcome = 'loss' AND realized_pnl >= 0)
    OR (outcome = 'scratch' AND realized_pnl != 0)
  )
ORDER BY closed_at DESC
LIMIT 100;

\echo
\echo ============================================================
\echo  Invariant D: no open positions on disabled bots
\echo ============================================================
\echo  If a bot is disabled but still holds an open position, the
\echo  operator turned it off after entry but the reconciler never
\echo  closed the outstanding trade. Expected: 0 rows.
\echo

SELECT p.id, p.bot_id, p.opened_at, p.underlying, p.entry_price, p.quantity_open
FROM tw_positions p
JOIN tw_bots b ON b.id = p.bot_id
WHERE b.enabled = FALSE
ORDER BY p.opened_at DESC;

\echo
\echo ============================================================
\echo  Invariant E: no notification rows without a matching bot
\echo ============================================================
\echo  A notification row whose bot_id no longer exists in tw_bots is
\echo  orphaned - usually the result of a manual DELETE FROM tw_bots
\echo  that never cascaded. Expected: 0 rows.
\echo

SELECT n.id, n.bot_id, n.event_type, n.sent_at
FROM tw_notifications_log n
LEFT JOIN tw_bots b ON b.id = n.bot_id
WHERE b.id IS NULL
ORDER BY n.sent_at DESC
LIMIT 100;

\echo
\echo ============================================================
\echo  Invariant F: scaled-close tranche ledger sums to realized_pnl
\echo ============================================================
\echo  For a scale-out close (components_at_exit.scale_stage > 0), the sum of
\echo  every per-tranche realized in components_at_exit.tranches MUST equal the
\echo  consolidated realized_pnl. This is the ledger-vs-total check for the
\echo  one-row consolidation (see the exit-strategy design doc, sec 7): if it
\echo  drifts, a partial take was booked but not folded into the final row, or
\echo  the size-weighted exit was computed from the wrong base. Live rows only.
\echo  Expected: 0 rows.
\echo

SELECT id, bot_id, close_reason, realized_pnl,
       (SELECT COALESCE(SUM((t->>'realized')::numeric), 0)
        FROM jsonb_array_elements(
          CASE WHEN jsonb_typeof(components_at_exit->'tranches') = 'array'
               THEN components_at_exit->'tranches' ELSE '[]'::jsonb END) AS t
       ) AS sum_tranche_realized
FROM tw_trades
WHERE closed_at >= NOW() - interval '90 days'
  AND (components_at_entry ->> 'origin') IS DISTINCT FROM 'simulate'
  AND (components_at_entry ->> 'simulated') IS DISTINCT FROM 'true'
  AND COALESCE((components_at_exit ->> 'scale_stage')::int, 0) > 0
  AND ABS(
        realized_pnl
        - (SELECT COALESCE(SUM((t->>'realized')::numeric), 0)
           FROM jsonb_array_elements(
             CASE WHEN jsonb_typeof(components_at_exit->'tranches') = 'array'
                  THEN components_at_exit->'tranches' ELSE '[]'::jsonb END) AS t)
      ) > 0.50
ORDER BY closed_at DESC
LIMIT 100;

\echo
\echo ============================================================
\echo  Done. Any rows above are drift — investigate.
\echo ============================================================
