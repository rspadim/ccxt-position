ALTER TABLE user_api_keys
  ADD COLUMN label VARCHAR(128) NULL AFTER user_id;
