CREATE TABLE IF NOT EXISTS api_key_account_permissions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_key_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  can_read BOOLEAN NOT NULL DEFAULT TRUE,
  can_trade BOOLEAN NOT NULL DEFAULT FALSE,
  can_close_position BOOLEAN NOT NULL DEFAULT FALSE,
  can_risk_manage BOOLEAN NOT NULL DEFAULT FALSE,
  can_block_new_positions BOOLEAN NOT NULL DEFAULT FALSE,
  can_block_account BOOLEAN NOT NULL DEFAULT FALSE,
  restrict_to_strategies BOOLEAN NOT NULL DEFAULT FALSE,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_api_key_account (api_key_id, account_id),
  CONSTRAINT fk_akap_api_key FOREIGN KEY (api_key_id) REFERENCES user_api_keys(id),
  CONSTRAINT fk_akap_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS api_key_strategy_permissions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_key_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  strategy_id BIGINT NOT NULL,
  can_read BOOLEAN NOT NULL DEFAULT TRUE,
  can_trade BOOLEAN NOT NULL DEFAULT TRUE,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_api_key_account_strategy (api_key_id, account_id, strategy_id),
  CONSTRAINT fk_aksp_api_key FOREIGN KEY (api_key_id) REFERENCES user_api_keys(id),
  CONSTRAINT fk_aksp_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS account_strategy_risk_state (
  account_id BIGINT NOT NULL,
  strategy_id BIGINT NOT NULL,
  allow_new_positions BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (account_id, strategy_id),
  CONSTRAINT fk_asrs_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);

INSERT INTO api_key_account_permissions (
  api_key_id,
  account_id,
  can_read,
  can_trade,
  can_close_position,
  can_risk_manage,
  can_block_new_positions,
  can_block_account,
  restrict_to_strategies,
  status
)
SELECT
  uak.id,
  uap.account_id,
  uap.can_read,
  uap.can_trade,
  uap.can_trade,
  uap.can_risk_manage,
  uap.can_risk_manage,
  uap.can_risk_manage,
  FALSE,
  'active'
FROM user_api_keys uak
JOIN user_account_permissions uap ON uap.user_id = uak.user_id
WHERE uak.status = 'active'
ON DUPLICATE KEY UPDATE
  can_read = VALUES(can_read),
  can_trade = VALUES(can_trade),
  can_close_position = VALUES(can_close_position),
  can_risk_manage = VALUES(can_risk_manage),
  can_block_new_positions = VALUES(can_block_new_positions),
  can_block_account = VALUES(can_block_account),
  status = VALUES(status);
