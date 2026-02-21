-- Safe to run multiple times.
SET @db_name := DATABASE();

SET @add_uq_exchange_trade := (
  SELECT IF(
    EXISTS(
      SELECT 1
      FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = @db_name
        AND TABLE_NAME = 'position_deals'
        AND INDEX_NAME = 'uq_position_deals_exchange_trade'
    ),
    'SELECT 1',
    'CREATE UNIQUE INDEX uq_position_deals_exchange_trade ON position_deals (account_id, exchange_trade_id)'
  )
);
PREPARE stmt1 FROM @add_uq_exchange_trade;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
