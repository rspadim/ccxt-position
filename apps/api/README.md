# API

First vertical slice implemented:

- `GET /healthz`
- `POST /oms/commands` (single command or batch)
- `GET /oms/orders/open|history`
- `GET /oms/deals`
- `GET /oms/positions/open|history`
- `POST /oms/reassign`
- `POST /ccxt/{account_id}/{func}`
- `POST /ccxt/commands`
- `WS /ws`

## Run

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r apps/api/requirements.txt
uvicorn apps.api.main:app --reload --port 8000
```

Optional (advanced): install `asyncmy` driver too:

```bash
pip install -r apps/api/requirements-asyncmy.txt
```

Optional local MySQL with Docker:

```bash
docker compose -f apps/api/docker-compose.mysql.yml up -d
```

## Quick Start (Beginner, Docker)

This is the easiest way to start from zero (MySQL + API + dispatcher).

1. Copy Docker config:

```bash
cp apps/api/config.docker.example.json apps/api/config.docker.json
```

2. (Optional but recommended) set `security.encryption_master_key` in `apps/api/config.docker.json`.

3. Start full stack:

```bash
docker compose -f apps/api/docker-compose.stack.yml up -d --build
```

4. Check health:

```bash
curl http://127.0.0.1:8000/healthz
```

5. Read logs if needed:

```bash
docker compose -f apps/api/docker-compose.stack.yml logs -f api
docker compose -f apps/api/docker-compose.stack.yml logs -f dispatcher
```

6. Stop stack:

```bash
docker compose -f apps/api/docker-compose.stack.yml down
```

Notes:

- MySQL schema is auto-applied from `sql/*.sql` on first startup.
- If you need a clean reset, run `down -v` to remove volumes and recreate.

Dispatcher behavior in v0:

- Executes `send_order`, `cancel_order`, and `change_order` through CCXT in per-account workers
- Persists exchange raw order payloads into `ccxt_orders_raw`
- Updates `oms_orders` status and `exchange_order_id`
- Runs reconciliation (manual and scheduled policies) and projects deals/positions

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
- `database.mysql_driver` supports `"aiomysql"` (default) or `"asyncmy"`
- worker settings under `worker.*`
- logging settings under `logging.*` (canonical section)
- security settings under `security.*`

Backward compatibility:

- Use `logging.disable_uvicorn_access_log` and `logging.app_request_log`.

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
- `change_order` validates mutable state before exchange execution.
- `close_by` is executed internally in the account worker and generates internal compensation deals.

## WebSocket

Connect with headers:

- `x-api-key: <plain_api_key>`
- `x-account-id: <account_id>`

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
      "strategy_id": 0,
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

Live integration test (against running API + dispatcher + exchange testnet):

```bash
RUN_LIVE_INTEGRATION=1 \
INTEGRATION_BASE_URL=http://127.0.0.1:8000 \
INTEGRATION_API_KEY=<internal_api_key_plain> \
INTEGRATION_ACCOUNT_ID=1 \
python -m pytest -q apps/api/tests/test_integration_position_flow.py
```

On PowerShell:

```powershell
$env:RUN_LIVE_INTEGRATION="1"
$env:INTEGRATION_BASE_URL="http://127.0.0.1:8000"
$env:INTEGRATION_API_KEY="<internal_api_key_plain>"
$env:INTEGRATION_ACCOUNT_ID="1"
python -m pytest -q apps/api/tests/test_integration_position_flow.py
```

## CLI

Run:

```bash
python -m apps.api.cli --help
```

Security commands:

```bash
python -m apps.api.cli install --with-account --exchange-id binance --label binance-testnet --testnet
python -m apps.api.cli create-user --name trader-bot
python -m apps.api.cli create-api-key --user-id 1
python -m apps.api.cli add-account --user-id 1 --exchange-id binance --label main --testnet
python -m apps.api.cli generate-master-key
python -m apps.api.cli encrypt --value "my-secret"
python -m apps.api.cli upsert-account-credentials --account-id 1 --api-key "..." --secret "..." --encrypt-input
python -m apps.api.cli set-account-testnet --account-id 1 --enabled
```

Trading/position commands:

```bash
python -m apps.api.cli send-order --api-key "$KEY" --account-id 1 --symbol BTC/USDT --side buy --order-type limit --qty 0.01 --price 50000
python -m apps.api.cli change-order --api-key "$KEY" --account-id 1 --order-id 123 --new-price 50100
python -m apps.api.cli cancel-order --api-key "$KEY" --account-id 1 --order-id 123
python -m apps.api.cli close-position --api-key "$KEY" --account-id 1 --position-id 77
python -m apps.api.cli close-by --api-key "$KEY" --account-id 1 --position-id-a 77 --position-id-b 88
python -m apps.api.cli reassign-position --api-key "$KEY" --account-id 1 --deal-ids 1 2 3 --target-strategy-id 42 --target-position-id 77
```

