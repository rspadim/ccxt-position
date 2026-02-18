-- Example policy checks (application-level enforcement expected)

-- Trade permission view
CREATE OR REPLACE VIEW v_user_account_trade_permission AS
SELECT
  uap.user_id,
  uap.account_id,
  uap.can_trade,
  ars.allow_new_positions
FROM user_account_permissions uap
JOIN account_risk_state ars ON ars.account_id = uap.account_id;
