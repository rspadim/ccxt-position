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

- `logs/api-<pid>.log`
- `logs/ccxt-<pid>.log`
- `logs/position-<pid>.log`

Security defaults:

- `uvicorn.access` can be disabled via config (`disable_uvicorn_access_log=true`)
- sensitive headers (`x-api-key`, `authorization`, cookies) are masked in app logs

## Engine Configuration

`v0` is optimized for MySQL with raw SQL repositories.

Create `apps/api/config.json` from `apps/api/config.example.json` and set:

- `app.db_engine`: `"mysql"` (required in v0)
- database settings under `database.*`
- worker settings under `worker.*`
- logging settings under `logging.*`
- security settings under `security.*`

Credential encryption:

- `security.encryption_master_key` enables encrypted values in `account_credentials_encrypted`
- encrypted format: `enc:v1:<fernet-token>`
- legacy plaintext values are still accepted for compatibility

Future `v1` can add `postgresql` by implementing a separate raw-SQL repository module.

## Auth

Use header:

- `x-api-key: <plain_api_key>`

The API hashes this value with SHA-256 and checks `user_api_keys.api_key_hash`.

Exchange credentials are loaded from `account_credentials_encrypted` columns.
In v0 compatibility mode, values are treated as already usable secret strings.

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
