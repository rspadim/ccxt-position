# REST: OMS Domain

## `POST /oms/commands`

Unified command endpoint (single or batch):

- `send_order`
- `cancel_order`
- `change_order`
- `close_by`
- `close_position`

Request body is a discriminated union keyed by `command`:

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
  - optional: `strategy_id`, `position_id`, `reason`, `reduce_only`, `client_order_id`
- `cancel_order.payload`:
  - `order_id` (int > 0)
- `change_order.payload`:
  - `order_id` (int > 0)
  - at least one of `new_price` or `new_qty` (`new_qty` > 0)
- `close_by.payload`:
  - `position_id_a` (int > 0)
  - `position_id_b` (int > 0)
  - optional: `strategy_id`
- `close_position.payload`:
  - `position_id` (int > 0)
  - optional: `order_type` (`market|limit`, default `market`), `price` (required for `limit`), `qty`, `strategy_id`, `reason`, `client_order_id`

Invalid payloads fail fast with HTTP `422` before queueing.

Behavior:

- `close_position` is converted to a reduce-only `send_order`.
- `close_position` acquires a position-level lock and rejects parallel close attempts.
- `change_order` is validated against current order state before queueing.

## Query endpoints

- `GET /oms/orders/open`
- `GET /oms/orders/history`
- `GET /oms/deals`
- `GET /oms/positions/open`
- `GET /oms/positions/history`
- `POST /oms/reassign`
- `POST /oms/reconcile`
- `GET /oms/reconcile/{account_id}/status`
- `GET /oms/reconcile/status`

`/oms/reassign` updates `strategy_id` and `position_id` for selected deals/orders and marks deals as reconciled.

`/oms/reconcile` triggers on-demand reconciliation:

- with `account_id`: runs for one account
- without `account_id`: runs for all accounts visible to the authenticated user

`/oms/reconcile/{account_id}/status` returns one account reconciliation health.

`/oms/reconcile/status` returns all visible accounts and supports `?status=fresh|stale|never`.

Legacy note: the database column previously named `magic_id` is now called `strategy_id`, so new commands and payloads should rely on the renamed field.
