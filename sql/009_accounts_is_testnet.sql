-- Safe to run multiple times.
SET @db_name := DATABASE();

SET @add_is_testnet := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'accounts'
        AND COLUMN_NAME = 'is_testnet'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN is_testnet BOOLEAN NOT NULL DEFAULT FALSE AFTER exchange_id'
  )
);
PREPARE stmt1 FROM @add_is_testnet;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
