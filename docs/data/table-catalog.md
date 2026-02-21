# Table Catalog

Core tables:

- `users`
- `user_api_keys`
- `accounts`
- `account_credentials_encrypted`
- `user_account_permissions`
- `account_risk_state`
- `position_commands`
- `position_orders`
- `position_deals`
- `position_positions`
- `reconciliation_cursor`
- `audit_log`
- `event_outbox`

Raw tables:

- Shared mode: `ccxt_orders_raw`, `ccxt_trades_raw`
- Dedicated mode: account-specific raw tables created by provisioning
