SET @exists := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'oms_orders'
    AND COLUMN_NAME = 'reconciled'
);
SET @sql := IF(
  @exists = 0,
  'ALTER TABLE oms_orders ADD COLUMN reconciled BOOLEAN NOT NULL DEFAULT TRUE AFTER avg_fill_price',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE oms_orders
SET reconciled = FALSE
WHERE reason = 'external' AND strategy_id = 0;
