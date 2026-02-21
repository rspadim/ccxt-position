ALTER TABLE users
ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'trade';

CREATE TABLE IF NOT EXISTS strategies (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_strategies_name (name)
);

CREATE TABLE IF NOT EXISTS strategy_accounts (
  strategy_id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (strategy_id, account_id),
  CONSTRAINT fk_strategy_accounts_strategy FOREIGN KEY (strategy_id) REFERENCES strategies(id),
  CONSTRAINT fk_strategy_accounts_account FOREIGN KEY (account_id) REFERENCES accounts(id)
);
