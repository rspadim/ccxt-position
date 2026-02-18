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
