# WebSocket Contract

Endpoint: `WS /ws`

Connection query params:

- `api_key`
- `account_id`
- optional `after_id`

Request envelope:

```json
{"id":"req-1","type":"position_command","namespace":"position","action":"send_order","payload":{}}
```

Response/Event envelope:

```json
{"id":"req-1","ok":true,"type":"position_event","namespace":"position","action":"ack","event":"accepted","payload":{}}
```

Rules:

- Client request `id` is required
- Server echoes same `id` for command responses
- Event streams support `position_*` and `ccxt_*`
- `action=command` under `namespace=position` maps to the same command pipeline as REST
- `action=call` under `namespace=ccxt` maps to CCXT gateway execution
