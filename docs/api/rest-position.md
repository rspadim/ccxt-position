# REST: Position Domain

## `POST /position/commands`

Unified command endpoint (single or batch):

- `send_order`
- `cancel_order`
- `change_order`
- `close_by`
- `close_position`

Batch response is index-aligned with request input.

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
