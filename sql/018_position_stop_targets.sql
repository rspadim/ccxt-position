SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_orders'
          AND COLUMN_NAME = 'stop_loss'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN stop_loss DECIMAL(36,18) NULL AFTER price'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_orders'
          AND COLUMN_NAME = 'stop_gain'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN stop_gain DECIMAL(36,18) NULL AFTER stop_loss'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_positions'
          AND COLUMN_NAME = 'stop_loss'
    ),
    'SELECT 1',
    'ALTER TABLE oms_positions ADD COLUMN stop_loss DECIMAL(36,18) NULL AFTER avg_price'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_positions'
          AND COLUMN_NAME = 'stop_gain'
    ),
    'SELECT 1',
    'ALTER TABLE oms_positions ADD COLUMN stop_gain DECIMAL(36,18) NULL AFTER stop_loss'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

