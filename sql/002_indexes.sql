CREATE INDEX idx_oms_orders_account_status_created
  ON oms_orders (account_id, status, created_at);

CREATE INDEX idx_oms_deals_account_position_executed
  ON oms_deals (account_id, position_id, executed_at);

CREATE UNIQUE INDEX uq_oms_deals_exchange_trade
  ON oms_deals (account_id, exchange_trade_id);

CREATE INDEX idx_oms_positions_account_symbol_state
  ON oms_positions (account_id, symbol, state);

CREATE UNIQUE INDEX uq_ccxt_orders_raw_candidate
  ON ccxt_orders_raw (account_id, exchange_id, exchange_order_id, client_order_id);

CREATE UNIQUE INDEX uq_ccxt_trades_raw_candidate
  ON ccxt_trades_raw (account_id, exchange_id, exchange_trade_id);

CREATE INDEX idx_ccxt_orders_raw_fingerprint
  ON ccxt_orders_raw (account_id, fingerprint_hash);

CREATE INDEX idx_ccxt_trades_raw_fingerprint
  ON ccxt_trades_raw (account_id, fingerprint_hash);
