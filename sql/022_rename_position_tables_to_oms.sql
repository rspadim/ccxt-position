-- Idempotent migration for existing databases:
-- position_* -> oms_*
SET @db_name := DATABASE();

SET @rename_position_commands := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_commands'
    )
    AND NOT EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_commands'
    ),
    'RENAME TABLE position_commands TO oms_commands',
    'SELECT 1'
  )
);
PREPARE stmt1 FROM @rename_position_commands;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @rename_position_orders := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_orders'
    )
    AND NOT EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
    ),
    'RENAME TABLE position_orders TO oms_orders',
    'SELECT 1'
  )
);
PREPARE stmt2 FROM @rename_position_orders;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

SET @rename_position_deals := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_deals'
    )
    AND NOT EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_deals'
    ),
    'RENAME TABLE position_deals TO oms_deals',
    'SELECT 1'
  )
);
PREPARE stmt3 FROM @rename_position_deals;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;

SET @rename_position_positions := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_positions'
    )
    AND NOT EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_positions'
    ),
    'RENAME TABLE position_positions TO oms_positions',
    'SELECT 1'
  )
);
PREPARE stmt4 FROM @rename_position_positions;
EXECUTE stmt4;
DEALLOCATE PREPARE stmt4;

SET @rename_position_close_locks := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_close_locks'
    )
    AND NOT EXISTS(
      SELECT 1
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_close_locks'
    ),
    'RENAME TABLE position_close_locks TO oms_close_locks',
    'SELECT 1'
  )
);
PREPARE stmt5 FROM @rename_position_close_locks;
EXECUTE stmt5;
DEALLOCATE PREPARE stmt5;
