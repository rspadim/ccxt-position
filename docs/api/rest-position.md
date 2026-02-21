# REST: Position Domain

## `POST /position/commands`

Unified command endpoint (single or batch):

- `send_order`
- `cancel_order`
- `change_order`
- `close_by`
- `close_position`

Batch response is index-aligned with request input.

Request body is a discriminated union by `command`:

- Common fields:
  - `account_id` (int > 0)
  - `command` (`send_order|cancel_order|change_order|close_by|close_position`)
  - `request_id` (optional string)
- `send_order.payload`:
  - `symbol` (string)
  - `side` (`buy|sell`)
  - `order_type` (`market|limit`) or alias `type`
  - `qty` (> 0) or alias `amount`
  - `price` required for `limit`
  - optional: `magic_id`, `position_id`, `reason`, `reduce_only`, `client_order_id`
- `cancel_order.payload`:
  - `order_id` (int > 0)
- `change_order.payload`:
  - `order_id` (int > 0)
  - at least one of `new_price` or `new_qty` (`new_qty` > 0)
- `close_by.payload`:
  - `position_id_a` (int > 0)
  - `position_id_b` (int > 0)
  - optional: `magic_id`
- `close_position.payload`:
  - `position_id` (int > 0)
  - optional: `order_type` (`market|limit`, default `market`), `price` (required for `limit`), `qty`, `magic_id`, `reason`, `client_order_id`

Invalid payload now fails fast with HTTP `422` before queueing.

Behavior:

- `close_position` is converted internally to a reduce-only `send_order`.
- `close_position` acquires a position-level lock and rejects parallel close attempts.
- `change_order` is validated against current order state before queueing.

## Query endpoints

- `GET /position/orders/open`
- `GET /position/orders/history`
- `GET /position/deals`
- `GET /position/positions/open`
- `GET /position/positions/history`
- `POST /position/reassign`
- `POST /position/reconcile`
- `GET /position/reconcile/{account_id}/status`
- `GET /position/reconcile/status`

`/position/reassign` updates `magic_id` and `position_id` for selected deals/orders and marks deals as reconciled.

`/position/reconcile` triggers on-demand reconciliation.

- with `account_id`: runs for one account
- without `account_id`: runs for all accounts visible to the authenticated user

`/position/reconcile/{account_id}/status` returns one account reconciliation health.

`/position/reconcile/status` returns all visible accounts and supports `?status=fresh|stale|never`.
