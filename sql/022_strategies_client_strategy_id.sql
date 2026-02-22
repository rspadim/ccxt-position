SET @exists := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'strategies'
    AND COLUMN_NAME = 'client_strategy_id'
);
SET @sql := IF(
  @exists = 0,
  'ALTER TABLE strategies ADD COLUMN client_strategy_id BIGINT NULL AFTER id',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
