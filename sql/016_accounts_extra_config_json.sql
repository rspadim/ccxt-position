SET @ddl := IF(
    EXISTS(
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'accounts'
          AND COLUMN_NAME = 'extra_config_json'
    ),
    'SELECT 1',
    'ALTER TABLE accounts ADD COLUMN extra_config_json JSON NULL AFTER position_mode'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

