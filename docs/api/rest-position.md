# REST: Position Domain

## `POST /position/commands`

Unified command endpoint (single or batch):

- `send_order`
- `cancel_order`
- `close_by`

Batch response is index-aligned with request input.

## Query endpoints

- `GET /position/orders/open`
- `GET /position/orders/history`
- `GET /position/deals`
- `GET /position/positions/open`
- `GET /position/positions/history`
- `POST /position/reassign`
