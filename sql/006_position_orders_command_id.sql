ALTER TABLE position_orders
  ADD COLUMN command_id BIGINT NULL AFTER id;

ALTER TABLE position_orders
  ADD KEY idx_position_orders_command_id (command_id);
