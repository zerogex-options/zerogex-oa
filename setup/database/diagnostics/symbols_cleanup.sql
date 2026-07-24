-- ============================================================
-- symbols cleanup  (DESTRUCTIVE -- deletes malformed registry rows)
-- ============================================================
-- Removes MALFORMED rows from the symbols table -- those that do not match
-- ^[A-Z]{1,6}$ (raw TradeStation forms like $SPX.X, and fragments like .X
-- or \DXW.X).  See symbols_cleanup_audit.sql for the full rationale and a
-- read-only preview of the cascade blast radius.
--
-- Deleting a symbols row CASCADES to every FK child.  By default this
-- script deletes ONLY malformed rows with zero dependent rows, so nothing
-- of value can be cascaded away.  Pass include_data=true to also delete
-- malformed rows that still hold child data (their child rows cascade).
--
-- Guarded: does nothing unless confirm=yes is passed.  Well-formed
-- (canonical) rows are never touched -- an extra WHERE guard on the DELETE
-- enforces that independently of the candidate list.
--
-- Invoke via the make wrapper (which supplies the flags):
--   make db-symbols-cleanup CONFIRM=yes
--   make db-symbols-cleanup CONFIRM=yes INCLUDE_DATA=yes
-- Or directly:
--   psql ... -v confirm=yes [-v include_data=true] \
--            -f setup/database/diagnostics/symbols_cleanup.sql
-- ============================================================

\set ON_ERROR_STOP on
\pset pager off
\pset border 2

-- Defaults when the wrapper / caller omits a flag.
\if :{?confirm}
\else
    \set confirm no
\endif
\if :{?include_data}
\else
    \set include_data false
\endif

SELECT (lower(:'confirm') = 'yes')::text AS _armed \gset

\if :_armed
\else
    \echo
    \echo '!! Refusing to delete: pass confirm=yes (make db-symbols-cleanup CONFIRM=yes).'
    \echo '   Nothing was changed.  Run  make db-symbols-audit  for a read-only preview.'
    \echo
    \quit
\endif

-- ------------------------------------------------------------
-- Build the candidate report (same catalog-driven count as the audit).
-- ------------------------------------------------------------
DROP TABLE IF EXISTS _symbols_cleanup_report;
CREATE TEMP TABLE _symbols_cleanup_report (
    symbol         text,
    dependent_rows bigint
);

DO $do$
DECLARE
    j     record;
    fk    record;
    n     bigint;
    total bigint;
BEGIN
    FOR j IN
        SELECT symbol FROM symbols WHERE symbol !~ '^[A-Z]{1,6}$' ORDER BY symbol
    LOOP
        total := 0;
        FOR fk IN
            SELECT c.conrelid::regclass::text AS child_table,
                   a.attname                  AS child_col
            FROM pg_constraint c
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid
             AND a.attnum   = c.conkey[1]
            WHERE c.confrelid = 'symbols'::regclass
              AND c.contype   = 'f'
        LOOP
            EXECUTE format('SELECT count(*) FROM %s WHERE %I = $1',
                           fk.child_table, fk.child_col)
                INTO n USING j.symbol;
            total := total + n;
        END LOOP;
        INSERT INTO _symbols_cleanup_report VALUES (j.symbol, total);
    END LOOP;
END
$do$;

\echo
\echo === Deleting malformed symbols (include_data = :include_data) ===
\echo  Rows selected for deletion:
\echo

SELECT
    symbol,
    dependent_rows,
    CASE WHEN dependent_rows = 0 THEN 'safe' ELSE 'cascades child rows' END AS note
FROM _symbols_cleanup_report
WHERE dependent_rows = 0 OR :include_data
ORDER BY dependent_rows DESC, symbol;

-- The delete. The extra !~ guard makes it structurally impossible to
-- remove a canonical (well-formed) row, whatever the report contains.
DELETE FROM symbols s
USING _symbols_cleanup_report r
WHERE s.symbol = r.symbol
  AND s.symbol !~ '^[A-Z]{1,6}$'
  AND (r.dependent_rows = 0 OR :include_data);

\echo
\echo === Registry after cleanup ===
\echo

SELECT symbol, asset_type, is_active
FROM symbols
ORDER BY is_active DESC, symbol;

-- Report any malformed rows left behind (skipped because they hold data
-- and include_data was not set).
SELECT symbol, dependent_rows AS remaining_dependent_rows
FROM _symbols_cleanup_report r
WHERE EXISTS (SELECT 1 FROM symbols s WHERE s.symbol = r.symbol)
ORDER BY symbol;

\echo
\echo '(rows still listed above were kept -- they hold data; re-run with INCLUDE_DATA=yes to cascade them.)'
\echo
