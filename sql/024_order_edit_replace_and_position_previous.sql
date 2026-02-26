-- Safe to run multiple times.
SET @db_name := DATABASE();

SET @add_orders_previous_position_id := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND COLUMN_NAME = 'previous_position_id'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN previous_position_id BIGINT NULL AFTER position_id'
  )
);
PREPARE stmt1 FROM @add_orders_previous_position_id;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

SET @add_orders_edit_replace_state := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND COLUMN_NAME = 'edit_replace_state'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN edit_replace_state VARCHAR(32) NULL AFTER reconciled'
  )
);
PREPARE stmt2 FROM @add_orders_edit_replace_state;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

SET @add_orders_edit_replace_at := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND COLUMN_NAME = 'edit_replace_at'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN edit_replace_at TIMESTAMP NULL AFTER edit_replace_state'
  )
);
PREPARE stmt3 FROM @add_orders_edit_replace_at;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;

SET @add_orders_edit_replace_orphan_order_id := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND COLUMN_NAME = 'edit_replace_orphan_order_id'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN edit_replace_orphan_order_id BIGINT NULL AFTER edit_replace_at'
  )
);
PREPARE stmt4 FROM @add_orders_edit_replace_orphan_order_id;
EXECUTE stmt4;
DEALLOCATE PREPARE stmt4;

SET @add_orders_edit_replace_origin_order_id := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND COLUMN_NAME = 'edit_replace_origin_order_id'
    ),
    'SELECT 1',
    'ALTER TABLE oms_orders ADD COLUMN edit_replace_origin_order_id BIGINT NULL AFTER edit_replace_orphan_order_id'
  )
);
PREPARE stmt5 FROM @add_orders_edit_replace_origin_order_id;
EXECUTE stmt5;
DEALLOCATE PREPARE stmt5;

SET @add_deals_previous_position_id := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_deals'
        AND COLUMN_NAME = 'previous_position_id'
    ),
    'SELECT 1',
    'ALTER TABLE oms_deals ADD COLUMN previous_position_id BIGINT NULL AFTER position_id'
  )
);
PREPARE stmt6 FROM @add_deals_previous_position_id;
EXECUTE stmt6;
DEALLOCATE PREPARE stmt6;
