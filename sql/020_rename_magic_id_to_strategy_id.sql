-- Backward-compatible rename for databases with either old table names
-- (position_*) or new table names (oms_*).

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_orders'
          AND COLUMN_NAME = 'magic_id'
    )
    AND NOT EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_orders'
          AND COLUMN_NAME = 'strategy_id'
    ),
    'ALTER TABLE position_orders CHANGE COLUMN magic_id strategy_id BIGINT NOT NULL DEFAULT 0',
    'SELECT 1'
);
PREPARE stmt1 FROM @ddl;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_deals'
          AND COLUMN_NAME = 'magic_id'
    )
    AND NOT EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'position_deals'
          AND COLUMN_NAME = 'strategy_id'
    ),
    'ALTER TABLE position_deals CHANGE COLUMN magic_id strategy_id BIGINT NOT NULL DEFAULT 0',
    'SELECT 1'
);
PREPARE stmt2 FROM @ddl;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_orders'
          AND COLUMN_NAME = 'magic_id'
    )
    AND NOT EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_orders'
          AND COLUMN_NAME = 'strategy_id'
    ),
    'ALTER TABLE oms_orders CHANGE COLUMN magic_id strategy_id BIGINT NOT NULL DEFAULT 0',
    'SELECT 1'
);
PREPARE stmt3 FROM @ddl;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;

SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_deals'
          AND COLUMN_NAME = 'magic_id'
    )
    AND NOT EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'oms_deals'
          AND COLUMN_NAME = 'strategy_id'
    ),
    'ALTER TABLE oms_deals CHANGE COLUMN magic_id strategy_id BIGINT NOT NULL DEFAULT 0',
    'SELECT 1'
);
PREPARE stmt4 FROM @ddl;
EXECUTE stmt4;
DEALLOCATE PREPARE stmt4;
