CREATE UNIQUE INDEX uq_position_deals_exchange_trade
  ON position_deals (account_id, exchange_trade_id);
