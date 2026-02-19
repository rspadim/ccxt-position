# API

First vertical slice implemented:

- `GET /healthz`
- `POST /position/commands` (single command or batch)
- `GET /position/orders/open|history`
- `GET /position/deals`
- `GET /position/positions/open|history`
- `POST /position/reassign`
- `POST /ccxt/{account_id}/{func}`
- `POST /ccxt/multiple_commands`
- `WS /ws`

## Run

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r apps/api/requirements.txt
uvicorn apps.api.main:app --reload --port 8000
```

Optional local MySQL with Docker:

```bash
docker compose -f apps/api/docker-compose.mysql.yml up -d
```

Run worker in another process:

```bash
python -m apps.api.worker_position
```

Worker behavior in v0:

- Executes `send_order`, `cancel_order`, and `change_order` through CCXT
- Persists exchange raw order payloads into `ccxt_orders_raw`
- Updates `position_orders` status and `exchange_order_id`
- Retries queue items with backoff and max attempt limit
- Reconciliation poll imports external trades and projects deals/positions

## Logging

Logs are split by domain and process to avoid concurrent writes to the same file:

- `logs/api.log`
- `logs/ccxt.log`
- `logs/position.log`

Rotation:

- daily rotation at midnight
- retention of 10 backup files

Security defaults:

- `uvicorn.access` can be disabled via config (`disable_uvicorn_access_log=true`)
- sensitive headers (`x-api-key`, `authorization`, cookies) are masked in app logs

## Engine Configuration

`v0` is optimized for MySQL with raw SQL repositories.

Create `apps/api/config.json` from `apps/api/config.example.json` and set:

- `app.db_engine`: `"mysql"` (required in v0)
- database settings under `database.*`
: `database.mysql_driver` supports `"asyncmy"` or `"aiomysql"`
- worker settings under `worker.*`
- logging settings under `logging.*`
- security settings under `security.*`

Credential encryption:

- `security.encryption_master_key` enables encrypted values in `account_credentials_encrypted`
- encrypted format: `enc:v1:<fernet-token>`
- plaintext credentials are rejected when `security.require_encrypted_credentials=true`

Future `v1` can add `postgresql` by implementing a separate raw-SQL repository module.

## Auth

Use header:

- `x-api-key: <plain_api_key>`

The API hashes this value with SHA-256 and checks `user_api_keys.api_key_hash`.

Exchange credentials are loaded from `account_credentials_encrypted` columns.
Provide encrypted credentials (`enc:v1:*`) for production operation.

## Position Commands

Supported command types:

- `send_order`
- `cancel_order`
- `change_order`
- `close_by`
- `close_position`

Request accepts object or array.

Notes:

- `close_position` is internally transformed into a reduce-only `send_order`.
- `close_position` acquires a per-position lock, so only one close flow runs at a time.
- `change_order` validates mutable state and enqueues modification command.
- Queue consumption and status progression happen in `worker_position`.
- `close_by` is executed internally in the worker and generates internal compensation deals.

## WebSocket

Connect with headers:

- `x-api-key: <plain_api_key>`
- `x-account-id: <account_id>`
- `x-after-id: <optional last event id>`

Supported actions:

- `ping`
- `subscribe`
- `namespace=position, action=command`
- `namespace=ccxt, action=call`

### Example

```json
[
  {
    "account_id": 1,
    "command": "send_order",
    "request_id": "req-1",
    "payload": {
      "symbol": "BTC/USDT",
      "side": "buy",
      "order_type": "limit",
      "qty": "0.01",
      "price": "50000",
      "magic_id": 0,
      "position_id": 0
    }
  }
]
```

## Tests

```bash
pip install -r apps/api/requirements-dev.txt
pytest -q apps/api/tests
```
