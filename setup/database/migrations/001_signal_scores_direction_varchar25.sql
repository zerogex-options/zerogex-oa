-- Migration 001: Widen signal_scores.direction from VARCHAR(10) to VARCHAR(25)
-- and update the CHECK constraint to match MSI regime labels.
--
-- Apply with: psql -d <dbname> -f setup/database/migrations/001_signal_scores_direction_varchar25.sql
--
-- Safe to re-run; all steps are guarded by existence checks.

DO $$
BEGIN
    -- Drop old constraint (bullish/bearish/neutral labels no longer used).
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_scores_direction_check'
          AND conrelid = 'signal_scores'::regclass
    ) THEN
        ALTER TABLE signal_scores DROP CONSTRAINT signal_scores_direction_check;
    END IF;

    -- Widen the column if it is still VARCHAR(10).
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'signal_scores'
          AND column_name = 'direction'
          AND character_maximum_length <= 10
    ) THEN
        ALTER TABLE signal_scores ALTER COLUMN direction TYPE VARCHAR(25);
    END IF;

    -- Add updated constraint for MSI regime labels.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'signal_scores_direction_check'
          AND conrelid = 'signal_scores'::regclass
    ) THEN
        ALTER TABLE signal_scores
            ADD CONSTRAINT signal_scores_direction_check
            CHECK (direction IN ('trend_expansion', 'controlled_trend', 'chop_range', 'high_risk_reversal'));
    END IF;
END $$;
