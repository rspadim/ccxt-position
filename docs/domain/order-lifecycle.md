# Order Lifecycle

States:

- `PENDING_SUBMIT`
- `SUBMITTED`
- `PARTIALLY_FILLED`
- `FILLED`
- `CANCELED`
- `REJECTED`

Correlate local/exchange by:

1. `exchange_order_id`
2. `clientOrderId` (preferred as local order id)
3. deterministic fallback fingerprint
