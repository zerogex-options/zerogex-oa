-- ============================================================
-- symbols cleanup audit  (READ-ONLY)
-- ============================================================
-- The symbols table is a registry: every time-series table FK-references
-- it as symbols(symbol) with ON DELETE CASCADE.  It is meant to hold only
-- CANONICAL tickers -- the user-facing alias returned by
-- get_canonical_symbol() (src/symbols.py), e.g. "SPX", "SPXW", "NDX",
-- "QQQ".  Never the raw TradeStation form ("$SPX.X"), and never a
-- truncated fragment (".X", "\DXW.X").
--
-- Over time the table accumulated debris:
--   * raw TradeStation index symbols  ($SPX.X, $SPXW.X, $NDX.X, $NDXW.X)
--     -- superseded by their canonical rows once canonicalization landed;
--   * mangled fragments               (\DXW.X, DXW.X, .X)
--     -- one-off corruption, no live code path produces these.
-- Every one of these is inactive; every canonical symbol is a bare
-- 1-6 letter uppercase ticker.  That lets us flag junk purely by shape:
-- a row is MALFORMED when it does not match  ^[A-Z]{1,6}$ .
--
-- Deleting a symbols row CASCADES to every child table.  Before removing
-- anything, this audit shows, per malformed row, exactly how many
-- dependent rows a delete would take with it.  Read-only: nothing is
-- deleted from this file.  Paste-ready cleanup commands are printed at the
-- end for use AFTER review.
--
-- Invoke via the make wrapper:
--   make db-symbols-audit
-- Or directly:
--   psql ... -f setup/database/diagnostics/symbols_cleanup_audit.sql
-- ============================================================

\set ON_ERROR_STOP off
\pset pager off
\pset border 2
\timing off

\echo
\echo ============================================================
\echo  symbols cleanup audit  (READ-ONLY -- nothing is deleted)
\echo ============================================================
\echo  MALFORMED := symbol does not match ^[A-Z]{1,6}$
\echo

\echo === [1/3] Full symbols registry, malformed rows first ===
\echo  format_check = MALFORMED marks a cleanup candidate.
\echo

SELECT
    symbol,
    name,
    asset_type,
    is_active,
    CASE WHEN symbol ~ '^[A-Z]{1,6}$' THEN 'ok' ELSE 'MALFORMED' END
        AS format_check,
    created_at,
    updated_at
FROM symbols
ORDER BY (symbol ~ '^[A-Z]{1,6}$'), symbol;

-- ------------------------------------------------------------
-- Per malformed symbol, count the rows every FK child holds for it.
-- Driven off the catalog (pg_constraint) so it stays correct as child
-- tables are added or dropped -- no hand-maintained table list.
-- ------------------------------------------------------------
DROP TABLE IF EXISTS _symbols_cleanup_report;
CREATE TEMP TABLE _symbols_cleanup_report (
    symbol                 text,
    is_active              boolean,
    dependent_rows         bigint,
    child_tables_with_data text
);

DO $do$
DECLARE
    j      record;   -- one malformed symbol
    fk     record;   -- one FK child (table, column) referencing symbols
    n      bigint;
    total  bigint;
    tbls   text;
BEGIN
    FOR j IN
        SELECT symbol, is_active
        FROM symbols
        WHERE symbol !~ '^[A-Z]{1,6}$'
        ORDER BY symbol
    LOOP
        total := 0;
        tbls  := '';
        FOR fk IN
            SELECT c.conrelid::regclass::text AS child_table,
                   a.attname                  AS child_col
            FROM pg_constraint c
            JOIN pg_attribute a
              ON a.attrelid = c.conrelid
             AND a.attnum   = c.conkey[1]
            WHERE c.confrelid = 'symbols'::regclass
              AND c.contype   = 'f'
            ORDER BY 1
        LOOP
            EXECUTE format('SELECT count(*) FROM %s WHERE %I = $1',
                           fk.child_table, fk.child_col)
                INTO n USING j.symbol;
            IF n > 0 THEN
                total := total + n;
                tbls  := tbls || format('%s(%s) ', fk.child_table, n);
            END IF;
        END LOOP;
        INSERT INTO _symbols_cleanup_report
        VALUES (j.symbol, j.is_active, total, NULLIF(trim(tbls), ''));
    END LOOP;
END
$do$;

\echo
\echo === [2/3] Cascade blast radius per malformed symbol ===
\echo  dependent_rows = rows a DELETE of this symbol would CASCADE away.
\echo  SAFE rows carry no data and can be hard-deleted with no loss.
\echo  HAS DATA rows still hold rows in the listed child tables -- review
\echo  before deleting (that data becomes unreachable once the symbol
\echo  differs from its canonical form, but confirm it is truly stale).
\echo

SELECT
    symbol,
    is_active,
    dependent_rows,
    CASE
        WHEN dependent_rows = 0 THEN 'SAFE -- no cascade'
        ELSE                        'HAS DATA -- CASCADE deletes child rows'
    END AS verdict,
    child_tables_with_data
FROM _symbols_cleanup_report
ORDER BY dependent_rows DESC, symbol;

\echo
\echo === [3/3] Totals ===
\echo

SELECT
    count(*)                                          AS malformed_rows,
    count(*) FILTER (WHERE dependent_rows = 0)        AS safe_to_delete,
    count(*) FILTER (WHERE dependent_rows > 0)        AS have_dependent_data,
    COALESCE(sum(dependent_rows), 0)                  AS total_dependent_rows
FROM _symbols_cleanup_report;

\echo
\echo ------------------------------------------------------------
\echo  Cleanup commands  (review [2/3] above first)
\echo ------------------------------------------------------------
\echo  Dry run -- list candidates, delete nothing:
\echo      make db-symbols-cleanup
\echo  Delete ONLY the SAFE (zero-dependent) malformed rows:
\echo      make db-symbols-cleanup CONFIRM=yes
\echo  Also CASCADE-delete malformed rows that still hold data:
\echo      make db-symbols-cleanup CONFIRM=yes INCLUDE_DATA=yes
\echo
