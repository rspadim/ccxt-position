-- Account-level reconciliation policy overrides.
-- Safe to run multiple times.

SET @db_name := DATABASE();

SET @add_reconcile_enabled := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_enabled'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_enabled BOOLEAN NOT NULL DEFAULT TRUE AFTER position_mode'
  )
);
PREPARE stmt1 FROM @add_reconcile_enabled;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @add_short_interval := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_short_interval_seconds'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_short_interval_seconds INT NULL AFTER reconcile_enabled'
  )
);
PREPARE stmt2 FROM @add_short_interval;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

SET @add_short_lookback := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_short_lookback_seconds'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_short_lookback_seconds INT NULL AFTER reconcile_short_interval_seconds'
  )
);
PREPARE stmt3 FROM @add_short_lookback;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;

SET @add_hourly_interval := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_hourly_interval_seconds'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_hourly_interval_seconds INT NULL AFTER reconcile_short_lookback_seconds'
  )
);
PREPARE stmt4 FROM @add_hourly_interval;
EXECUTE stmt4;
DEALLOCATE PREPARE stmt4;

SET @add_hourly_lookback := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_hourly_lookback_seconds'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_hourly_lookback_seconds INT NULL AFTER reconcile_hourly_interval_seconds'
  )
);
PREPARE stmt5 FROM @add_hourly_lookback;
EXECUTE stmt5;
DEALLOCATE PREPARE stmt5;

SET @add_long_interval := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_long_interval_seconds'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_long_interval_seconds INT NULL AFTER reconcile_hourly_lookback_seconds'
  )
);
PREPARE stmt6 FROM @add_long_interval;
EXECUTE stmt6;
DEALLOCATE PREPARE stmt6;

SET @add_long_lookback := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'reconcile_long_lookback_seconds'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN reconcile_long_lookback_seconds INT NULL AFTER reconcile_long_interval_seconds'
  )
);
PREPARE stmt7 FROM @add_long_lookback;
EXECUTE stmt7;
DEALLOCATE PREPARE stmt7;
