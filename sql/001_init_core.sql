CREATE TABLE IF NOT EXISTS users (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_api_keys (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  api_key_hash VARCHAR(255) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_user_api_keys_user FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS accounts (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  exchange_id VARCHAR(64) NOT NULL,
  is_testnet BOOLEAN NOT NULL DEFAULT FALSE,
  label VARCHAR(128) NOT NULL,
  position_mode VARCHAR(16) NOT NULL DEFAULT 'hedge',
  reconcile_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  reconcile_short_interval_seconds INT NULL,
  reconcile_short_lookback_seconds INT NULL,
  reconcile_hourly_interval_seconds INT NULL,
  reconcile_hourly_lookback_seconds INT NULL,
  reconcile_long_interval_seconds INT NULL,
  reconcile_long_lookback_seconds INT NULL,
  dispatcher_worker_hint INT NULL,
  dispatcher_hint_updated_at TIMESTAMP NULL,
  raw_storage_mode VARCHAR(16) NOT NULL DEFAULT 'shared',
  pool_id INT NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS account_credentials_encrypted (
  account_id BIGINT PRIMARY KEY,
  api_key_enc TEXT NOT NULL,
  secret_enc TEXT NOT NULL,
  passphrase_enc TEXT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_account_creds_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS user_account_permissions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  can_read BOOLEAN NOT NULL DEFAULT TRUE,
  can_trade BOOLEAN NOT NULL DEFAULT FALSE,
  can_risk_manage BOOLEAN NOT NULL DEFAULT FALSE,
  UNIQUE KEY uq_user_account (user_id, account_id),
  CONSTRAINT fk_uap_user FOREIGN KEY (user_id) REFERENCES users(id),
  CONSTRAINT fk_uap_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS account_risk_state (
  account_id BIGINT PRIMARY KEY,
  allow_new_positions BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_ars_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS oms_commands (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  command_type VARCHAR(32) NOT NULL,
  request_id VARCHAR(128) NULL,
  payload_json JSON NOT NULL,
  status VARCHAR(32) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_pos_cmd_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS oms_orders (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  command_id BIGINT NULL,
  account_id BIGINT NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  side VARCHAR(8) NOT NULL,
  order_type VARCHAR(16) NOT NULL,
  status VARCHAR(32) NOT NULL,
  strategy_id BIGINT NOT NULL DEFAULT 0,
  position_id BIGINT NOT NULL DEFAULT 0,
  reason VARCHAR(32) NOT NULL DEFAULT 'api',
  comment VARCHAR(255) NULL,
  client_order_id VARCHAR(128) NULL,
  exchange_order_id VARCHAR(128) NULL,
  qty DECIMAL(36,18) NOT NULL,
  price DECIMAL(36,18) NULL,
  stop_loss DECIMAL(36,18) NULL,
  stop_gain DECIMAL(36,18) NULL,
  filled_qty DECIMAL(36,18) NOT NULL DEFAULT 0,
  avg_fill_price DECIMAL(36,18) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  closed_at TIMESTAMP NULL,
  KEY idx_oms_orders_command_id (command_id)
);

CREATE TABLE IF NOT EXISTS oms_deals (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  order_id BIGINT NULL,
  position_id BIGINT NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  side VARCHAR(8) NOT NULL,
  qty DECIMAL(36,18) NOT NULL,
  price DECIMAL(36,18) NOT NULL,
  fee DECIMAL(36,18) NULL,
  fee_currency VARCHAR(32) NULL,
  pnl DECIMAL(36,18) NULL,
  strategy_id BIGINT NOT NULL DEFAULT 0,
  reason VARCHAR(32) NOT NULL DEFAULT 'api',
  comment VARCHAR(255) NULL,
  reconciled BOOLEAN NOT NULL DEFAULT TRUE,
  exchange_trade_id VARCHAR(128) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  executed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS oms_positions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  symbol VARCHAR(64) NOT NULL,
  strategy_id BIGINT NOT NULL DEFAULT 0,
  side VARCHAR(8) NOT NULL,
  qty DECIMAL(36,18) NOT NULL,
  avg_price DECIMAL(36,18) NOT NULL,
  stop_loss DECIMAL(36,18) NULL,
  stop_gain DECIMAL(36,18) NULL,
  state VARCHAR(16) NOT NULL,
  reason VARCHAR(32) NOT NULL DEFAULT 'api',
  comment VARCHAR(255) NULL,
  opened_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  closed_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_cursor (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  entity VARCHAR(32) NOT NULL,
  cursor_value VARCHAR(255) NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_recon_cursor (account_id, entity),
  CONSTRAINT fk_recon_cursor_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS audit_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NULL,
  actor VARCHAR(128) NOT NULL,
  action VARCHAR(64) NOT NULL,
  entity VARCHAR(64) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  before_json JSON NULL,
  after_json JSON NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ccxt_orders_raw (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  exchange_id VARCHAR(64) NOT NULL,
  exchange_order_id VARCHAR(128) NULL,
  client_order_id VARCHAR(128) NULL,
  symbol VARCHAR(64) NULL,
  raw_json JSON NOT NULL,
  fingerprint_hash CHAR(64) NOT NULL,
  observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ccxt_order_raw_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS ccxt_trades_raw (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  exchange_id VARCHAR(64) NOT NULL,
  exchange_trade_id VARCHAR(128) NULL,
  exchange_order_id VARCHAR(128) NULL,
  symbol VARCHAR(64) NULL,
  raw_json JSON NOT NULL,
  fingerprint_hash CHAR(64) NOT NULL,
  observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_ccxt_trade_raw_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

