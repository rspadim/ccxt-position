# Table Catalog

Core tables:

- `users`
- `user_api_keys`
- `accounts`
- `account_credentials_encrypted`
- `user_account_permissions`
- `account_risk_state`
- `oms_commands`
- `oms_orders`
- `oms_deals`
- `oms_positions`
- `oms_close_locks`
- `reconciliation_cursor`
- `audit_log`
- `event_outbox`

Raw tables:

- Shared mode: `ccxt_orders_raw`, `ccxt_trades_raw`
- Dedicated mode: account-specific raw tables created by provisioning
