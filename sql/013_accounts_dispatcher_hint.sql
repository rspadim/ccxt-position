-- Account-level dispatcher worker hint for warm start.
-- Safe to run multiple times.

SET @db_name := DATABASE();

SET @add_worker_hint := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'dispatcher_worker_hint'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN dispatcher_worker_hint INT NULL AFTER reconcile_long_lookback_seconds'
  )
);
PREPARE stmt1 FROM @add_worker_hint;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @add_hint_updated_at := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'dispatcher_hint_updated_at'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN dispatcher_hint_updated_at TIMESTAMP NULL AFTER dispatcher_worker_hint'
  )
);
PREPARE stmt2 FROM @add_hint_updated_at;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;
