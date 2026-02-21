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

CREATE INDEX idx_event_outbox_delivery
  ON event_outbox (delivered, created_at, id);
