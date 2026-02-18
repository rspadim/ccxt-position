# API

First vertical slice implemented:

- `GET /healthz`
- `POST /position/commands` (single command or batch)

## Run

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r apps/api/requirements.txt
uvicorn apps.api.main:app --reload --port 8000
```

## Engine Configuration

`v0` is optimized for MySQL with raw SQL repositories.

Create `apps/api/config.json` from `apps/api/config.example.json` and set:

- `db_engine`: `"mysql"` (required in v0)

Future `v1` can add `postgresql` by implementing a separate raw-SQL repository module.

## Auth

Use header:

- `x-api-key: <plain_api_key>`

The API hashes this value with SHA-256 and checks `user_api_keys.api_key_hash`.

## Position Commands

Supported command types:

- `send_order`
- `cancel_order`
- `close_by`

Request accepts object or array.

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
