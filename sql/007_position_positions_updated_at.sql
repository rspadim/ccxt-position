-- Safe to run multiple times.
SET @db_name := DATABASE();

SET @add_updated_at := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_positions'
        AND COLUMN_NAME = 'updated_at'
    ),
    'SELECT 1',
    'ALTER TABLE position_positions ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER opened_at'
  )
);
PREPARE stmt1 FROM @add_updated_at;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
