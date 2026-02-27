-- Performance indexes for OMS multi-account read queries used by /oms/* endpoints.
-- Keep idempotent style compatible with existing migration approach.

SET @db_name := DATABASE();

-- oms_orders: account_id + status filters, ordered by id
SET @add_idx_orders_multi := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_orders'
        AND INDEX_NAME = 'idx_oms_orders_account_status_id'
    ),
    'SELECT 1',
    'CREATE INDEX idx_oms_orders_account_status_id ON oms_orders (account_id, status, id)'
  )
);
PREPARE stmt1 FROM @add_idx_orders_multi;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

-- oms_deals: account/date range filters, ordered by id
SET @add_idx_deals_multi := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_deals'
        AND INDEX_NAME = 'idx_oms_deals_account_executed_id'
    ),
    'SELECT 1',
    'CREATE INDEX idx_oms_deals_account_executed_id ON oms_deals (account_id, executed_at, id)'
  )
);
PREPARE stmt2 FROM @add_idx_deals_multi;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

-- oms_positions: account + state filters, ordered by id
SET @add_idx_positions_multi := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'oms_positions'
        AND INDEX_NAME = 'idx_oms_positions_account_state_id'
    ),
    'SELECT 1',
    'CREATE INDEX idx_oms_positions_account_state_id ON oms_positions (account_id, state, id)'
  )
);
PREPARE stmt3 FROM @add_idx_positions_multi;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;
