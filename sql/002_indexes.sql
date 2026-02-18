CREATE INDEX idx_position_orders_account_status_created
  ON position_orders (account_id, status, created_at);

CREATE INDEX idx_position_deals_account_position_executed
  ON position_deals (account_id, position_id, executed_at);

CREATE UNIQUE INDEX uq_position_deals_exchange_trade
  ON position_deals (account_id, exchange_trade_id);

CREATE INDEX idx_position_positions_account_symbol_state
  ON position_positions (account_id, symbol, state);

CREATE UNIQUE INDEX uq_ccxt_orders_raw_candidate
  ON ccxt_orders_raw (account_id, exchange_id, exchange_order_id, client_order_id);

CREATE UNIQUE INDEX uq_ccxt_trades_raw_candidate
  ON ccxt_trades_raw (account_id, exchange_id, exchange_trade_id);

CREATE INDEX idx_ccxt_orders_raw_fingerprint
  ON ccxt_orders_raw (account_id, fingerprint_hash);

CREATE INDEX idx_ccxt_trades_raw_fingerprint
  ON ccxt_trades_raw (account_id, fingerprint_hash);
