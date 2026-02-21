SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_orders'
          AND COLUMN_NAME = 'comment'
    ),
    'SELECT 1',
    'ALTER TABLE position_orders ADD COLUMN comment VARCHAR(255) NULL AFTER reason'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_deals'
          AND COLUMN_NAME = 'comment'
    ),
    'SELECT 1',
    'ALTER TABLE position_deals ADD COLUMN comment VARCHAR(255) NULL AFTER reason'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_positions'
          AND COLUMN_NAME = 'comment'
    ),
    'SELECT 1',
    'ALTER TABLE position_positions ADD COLUMN comment VARCHAR(255) NULL AFTER reason'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

