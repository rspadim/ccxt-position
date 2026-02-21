# WebSocket Contract

Endpoint: `WS /ws`

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
- Authenticate with `x-api-key` header or `action=auth` + `payload.api_key`
- Subscribe with `action=subscribe`, `payload.account_ids[]`, and `payload.namespaces[]`
- Client can request initial open-orders/open-positions snapshot on subscribe (`payload.with_snapshot=true`, default true)
- `action=command` under `namespace=position` maps to the same command pipeline as REST
- `action=call` under `namespace=ccxt` maps to CCXT gateway execution
