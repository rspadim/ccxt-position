CREATE TABLE IF NOT EXISTS command_queue (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NOT NULL,
  pool_id INT NOT NULL DEFAULT 0,
  command_id BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'queued',
  attempts INT NOT NULL DEFAULT 0,
  available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_by VARCHAR(128) NULL,
  locked_at TIMESTAMP NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_command_queue_command (command_id),
  CONSTRAINT fk_command_queue_account FOREIGN KEY (account_id) REFERENCES accounts(id),
  CONSTRAINT fk_command_queue_command FOREIGN KEY (command_id) REFERENCES position_commands(id)
);

CREATE TABLE IF NOT EXISTS event_outbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  account_id BIGINT NULL,
  namespace VARCHAR(32) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  payload_json JSON NOT NULL,
  delivered BOOLEAN NOT NULL DEFAULT FALSE,
  delivered_at TIMESTAMP NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_command_queue_poll
  ON command_queue (pool_id, status, available_at, id);

CREATE INDEX idx_event_outbox_delivery
  ON event_outbox (delivered, created_at, id);
