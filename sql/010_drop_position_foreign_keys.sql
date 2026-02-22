-- Drops FK constraints related to position tables while keeping columns and indexes.
-- Safe to run multiple times.

SET @db_name := DATABASE();

SET @drop_fk_pos_order_command := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND CONSTRAINT_NAME = 'fk_pos_order_command'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    ),
    'ALTER TABLE oms_orders DROP FOREIGN KEY fk_pos_order_command',
    'SELECT 1'
  )
);
PREPARE stmt1 FROM @drop_fk_pos_order_command;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @drop_fk_pos_order_account := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND CONSTRAINT_NAME = 'fk_pos_order_account'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    ),
    'ALTER TABLE oms_orders DROP FOREIGN KEY fk_pos_order_account',
    'SELECT 1'
  )
);
PREPARE stmt2 FROM @drop_fk_pos_order_account;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

SET @drop_fk_pos_deal_account := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_deals'
        AND CONSTRAINT_NAME = 'fk_pos_deal_account'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    ),
    'ALTER TABLE oms_deals DROP FOREIGN KEY fk_pos_deal_account',
    'SELECT 1'
  )
);
PREPARE stmt3 FROM @drop_fk_pos_deal_account;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;

SET @drop_fk_pos_deal_order := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_deals'
        AND CONSTRAINT_NAME = 'fk_pos_deal_order'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    ),
    'ALTER TABLE oms_deals DROP FOREIGN KEY fk_pos_deal_order',
    'SELECT 1'
  )
);
PREPARE stmt4 FROM @drop_fk_pos_deal_order;
EXECUTE stmt4;
DEALLOCATE PREPARE stmt4;

SET @drop_fk_pos_position_account := (
  SELECT IF(
    EXISTS(
      SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
      WHERE CONSTRAINT_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_positions'
        AND CONSTRAINT_NAME = 'fk_pos_position_account'
        AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    ),
    'ALTER TABLE oms_positions DROP FOREIGN KEY fk_pos_position_account',
    'SELECT 1'
  )
);
PREPARE stmt5 FROM @drop_fk_pos_position_account;
EXECUTE stmt5;
DEALLOCATE PREPARE stmt5;
