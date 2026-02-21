-- Safe to run multiple times.
SET @db_name := DATABASE();

SET @add_command_id := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_orders'
        AND COLUMN_NAME = 'command_id'
    ),
    'SELECT 1',
    'ALTER TABLE position_orders ADD COLUMN command_id BIGINT NULL AFTER id'
  )
);
PREPARE stmt1 FROM @add_command_id;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @add_idx := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_orders'
        AND INDEX_NAME = 'idx_position_orders_command_id'
    ),
    'SELECT 1',
    'ALTER TABLE position_orders ADD KEY idx_position_orders_command_id (command_id)'
  )
);
PREPARE stmt2 FROM @add_idx;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;
