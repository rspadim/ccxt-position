ALTER TABLE users
  MODIFY COLUMN role VARCHAR(32) NOT NULL DEFAULT 'trader';

UPDATE users
SET role = 'trader'
WHERE role = 'trade';
