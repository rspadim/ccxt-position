CREATE TABLE IF NOT EXISTS position_close_locks (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  position_id BIGINT NOT NULL,
  request_id VARCHAR(128) NULL,
  lock_reason VARCHAR(32) NOT NULL DEFAULT 'close_position',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP NOT NULL,
  UNIQUE KEY uq_position_close_lock_active (position_id),
  CONSTRAINT fk_position_close_lock_account FOREIGN KEY (account_id) REFERENCES accounts(id),
  CONSTRAINT fk_position_close_lock_position FOREIGN KEY (position_id) REFERENCES position_positions(id)
);

CREATE INDEX idx_position_close_locks_expires_at
  ON position_close_locks (expires_at);
