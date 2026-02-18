# Indexing Strategy

Primary goals:

- Fast account-scoped lookups
- Idempotent raw ingestion
- Fast open-state queries

Important indexes:

- `(account_id, status, created_at)` on orders
- `(account_id, position_id, executed_at)` on deals
- `(account_id, symbol, state)` on positions
- outbox delivery index `(delivered, created_at)`
