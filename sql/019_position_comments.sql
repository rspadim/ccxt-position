SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_orders'
          AND COLUMN_NAME = 'comment'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN comment VARCHAR(255) NULL AFTER reason'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_deals'
          AND COLUMN_NAME = 'comment'
    ),
    'SELECT 1',
    'ALTER TABLE oms_deals ADD COLUMN comment VARCHAR(255) NULL AFTER reason'
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
          AND COLUMN_NAME = 'comment'
    ),
    'SELECT 1',
    'ALTER TABLE oms_positions ADD COLUMN comment VARCHAR(255) NULL AFTER reason'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

