ALTER TABLE position_orders
  ADD COLUMN command_id BIGINT NULL AFTER id;

ALTER TABLE position_orders
  ADD KEY idx_position_orders_command_id (command_id);

ALTER TABLE position_orders
  ADD CONSTRAINT fk_pos_order_command
  FOREIGN KEY (command_id) REFERENCES position_commands(id);
