ALTER TABLE oms_orders
  ADD COLUMN IF NOT EXISTS previous_position_id BIGINT NULL AFTER position_id,
  ADD COLUMN IF NOT EXISTS edit_replace_state VARCHAR(32) NULL AFTER reconciled,
  ADD COLUMN IF NOT EXISTS edit_replace_at TIMESTAMP NULL AFTER edit_replace_state,
  ADD COLUMN IF NOT EXISTS edit_replace_orphan_order_id BIGINT NULL AFTER edit_replace_at,
  ADD COLUMN IF NOT EXISTS edit_replace_origin_order_id BIGINT NULL AFTER edit_replace_orphan_order_id;

ALTER TABLE oms_deals
  ADD COLUMN IF NOT EXISTS previous_position_id BIGINT NULL AFTER position_id;
