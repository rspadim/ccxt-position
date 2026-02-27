# ccxt-position

`ccxt-position` is a single-host OMS gateway that combines:

- A CCXT-like API surface (`/ccxt`) for direct exchange routing
- An MT5-like position engine (`/oms`) for orders, deals, positions, and reconciliation

## Project Status

Specification-first bootstrap with first implementation slice available in `apps/api`:

- FastAPI app startup/shutdown
- API key auth (`x-api-key`, SHA-256 lookup)
- `POST /oms/commands` with batch support
- Position query/reassign endpoints and CCXT gateway endpoints
- WebSocket (`/ws`) with command/call and outbox event streaming
- MySQL persistence for commands/orders/deals/positions, raw sync, and queue
- Worker processing pipeline with CCXT execution and raw exchange sync

## Core Concepts

- `account`: an exchange credential set and runtime config
- `order`: command intent and exchange lifecycle state
- `deal`: executed trade event
- `position`: MT5-like tracked exposure
- `strategy_id`: strategy/robot identifier (`0` = automatic/default)

## User Profiles

- `admin`: admin/backoffice only (can manage users/accounts/permissions; cannot place trades)
- `trader`: discretionary trader permissions
- `portfolio_manager`: portfolio/manager trading permissions
- `robot`: automated strategy/robot permissions
- `risk`: risk controls (close/block/unblock) with mandatory action comment
- `readonly`: read-only visibility

## API Surfaces

- `POST /oms/commands`: unified MT5-like command entrypoint
- `POST /ccxt/{account_id}/{func}`: CCXT function gateway
- `POST /ccxt/commands`: batch CCXT commands
- `WS /ws`: unified websocket envelope (`position_*` and `ccxt_*`)

## Front-end Gallery

<table>
  <tr>
    <td><img src="docs/media/screenshots/01-login-language-options.png" alt="Login Language Options" width="420" /></td>
    <td><img src="docs/media/screenshots/02-oms-commands.png" alt="OMS Commands" width="420" /></td>
  </tr>
  <tr>
    <td><img src="docs/media/screenshots/03-oms-positions.png" alt="OMS Positions" width="420" /></td>
    <td><img src="docs/media/screenshots/04-oms-symbol-list.png" alt="OMS Symbol List" width="420" /></td>
  </tr>
  <tr>
    <td><img src="docs/media/screenshots/05-system-ccxt-orders.png" alt="System CCXT Orders" width="420" /></td>
    <td><img src="docs/media/screenshots/06-system-ccxt-trades.png" alt="System CCXT Trades" width="420" /></td>
  </tr>
  <tr>
    <td><img src="docs/media/screenshots/07-admin-accounts.png" alt="Admin Accounts" width="420" /></td>
    <td><img src="docs/media/screenshots/08-admin-api-keys.png" alt="Admin API Keys" width="420" /></td>
  </tr>
  <tr>
    <td><img src="docs/media/screenshots/09-admin-system-status.png" alt="Admin System Status" width="420" /></td>
    <td><img src="docs/media/screenshots/10-risk-accounts.png" alt="Risk Accounts" width="420" /></td>
  </tr>
</table>

Screenshot generation guide: `docs/ops/front-end-screenshots.md`.

## Documentation Index

- Architecture: `docs/architecture/overview.md`
- Runtime topology: `docs/architecture/runtime-topology.md`
- Data flow: `docs/architecture/data-flow.md`
- Domain mapping: `docs/domain/mt5-mapping.md`
- Position API: `docs/api/rest-position.md`
- CCXT API: `docs/api/rest-ccxt.md`
- WebSocket contract: `docs/api/websocket.md`
- Schema catalog: `docs/data/table-catalog.md`
- Security model: `docs/security/authentication.md`
- Operations: `docs/ops/deployment-single-host.md`
- Simple Front: `docs/ops/front-end.md`
- Front-end screenshots: `docs/ops/front-end-screenshots.md`
- Roadmap: `docs/roadmap/mvp-scope.md`
- Testnet bootstrap: `test/testnet/README.md`
  - Includes `run.py` (bootstrap) and `scenarios.py` (hedge/netting + multi-strategy live validation)

## Beginner Install (Docker)

This is the fastest way to run everything from zero.

1. Copy Docker config:

```bash
cp apps/api/config.docker.example.json apps/api/config.docker.json
```

2. Start stack (`mysql + api + worker`):

```bash
docker compose -f apps/api/docker-compose.stack.yml up -d --build
```

Note:

- Docker image uses `aiomysql` by default to avoid native compilation issues.
- If you explicitly need `asyncmy`, use `apps/api/requirements-asyncmy.txt` outside Docker.

3. Run installer (creates schema, internal user, internal API key, and one account):

```bash
docker compose -f apps/api/docker-compose.stack.yml exec api \
  python -m apps.api.cli install --with-account --exchange-id ccxt.binance --label binance-testnet --testnet
```

If schema was already initialized by MySQL entrypoint, use:

```bash
docker compose -f apps/api/docker-compose.stack.yml exec api \
  python -m apps.api.cli install --skip-schema --with-account --exchange-id ccxt.binance --label binance-testnet --testnet
```

Save values returned by installer:

- `api_key.plain` (internal API key for `x-api-key`)
- `account.id` (account to bind Binance credentials)

4. Store Binance Testnet credentials on created account:

```bash
docker compose -f apps/api/docker-compose.stack.yml exec api \
  python -m apps.api.cli upsert-account-credentials \
  --account-id <ACCOUNT_ID> \
  --api-key "<BINANCE_TESTNET_API_KEY>" \
  --secret "<BINANCE_TESTNET_SECRET_KEY>" \
  --encrypt-input
```

5. Validate API is running:

```bash
curl http://127.0.0.1:8000/healthz
```

6. Validate CCXT connectivity (`fetch_balance`):

```bash
curl -X POST "http://127.0.0.1:8000/ccxt/core/<ACCOUNT_ID>/fetch_balance" \
  -H "x-api-key: <INTERNAL_API_KEY_PLAIN>" \
  -H "Content-Type: application/json" \
  -d "{\"params\":{}}"
```

7. Optional test order (limit):

```bash
curl -X POST "http://127.0.0.1:8000/ccxt/core/<ACCOUNT_ID>/create_order" \
  -H "x-api-key: <INTERNAL_API_KEY_PLAIN>" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"BTC/USDT\",\"side\":\"buy\",\"order_type\":\"limit\",\"amount\":\"0.001\",\"price\":\"10000\",\"params\":{}}"
```

## OMS Examples (Recommended)

Use `/oms` for trading flows tied to OMS orders/deals/positions.

```bash
BASE_URL="http://127.0.0.1:8000"
API_KEY="<INTERNAL_API_KEY_PLAIN>"
ACCOUNT_ID="<ACCOUNT_ID>"
```

1. Send market order (`send_order`):

```bash
curl -X POST "$BASE_URL/oms/commands" \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"account_id\": $ACCOUNT_ID,
    \"command\": \"send_order\",
    \"payload\": {
      \"symbol\": \"BTC/USDT\",
      \"side\": \"buy\",
      \"order_type\": \"market\",
      \"qty\": \"0.001\",
      \"strategy_id\": 1
    }
  }"
```

2. Change open order (`change_order`, replace `<ORDER_ID>`):

```bash
curl -X POST "$BASE_URL/oms/commands" \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"account_id\": $ACCOUNT_ID,
    \"command\": \"change_order\",
    \"payload\": {
      \"order_id\": <ORDER_ID>,
      \"new_price\": \"65000.00\"
    }
  }"
```

3. Cancel order (`cancel_order`, replace `<ORDER_ID>`):

```bash
curl -X POST "$BASE_URL/oms/commands" \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"account_id\": $ACCOUNT_ID,
    \"command\": \"cancel_order\",
    \"payload\": {
      \"order_id\": <ORDER_ID>
    }
  }"
```

4. Close position (`close_position`, replace `<POSITION_ID>`):

```bash
curl -X POST "$BASE_URL/oms/commands" \
  -H "x-api-key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"account_id\": $ACCOUNT_ID,
    \"command\": \"close_position\",
    \"payload\": {
      \"position_id\": <POSITION_ID>,
      \"order_type\": \"market\",
      \"strategy_id\": 1
    }
  }"
```

5. Read open orders:

```bash
curl "$BASE_URL/oms/orders/open?account_ids=$ACCOUNT_ID&limit=200" \
  -H "x-api-key: $API_KEY"
```

6. Read open positions:

```bash
curl "$BASE_URL/oms/positions/open?account_ids=$ACCOUNT_ID&limit=200" \
  -H "x-api-key: $API_KEY"
```

## CCXT-like Driver Examples (OMS-first)

Python wrapper lives in `ccxt_driver/` and exposes CCXT-style methods mapped to `/oms` first.

```python
from ccxt_driver import OmsCcxtExchange

exchange = OmsCcxtExchange(
    api_key="YOUR_INTERNAL_API_KEY",
    account_id=1,
    strategy_id=1001,
    base_url="http://127.0.0.1:8000",
)
```

1. Send market order:

```python
order = exchange.create_order("BTC/USDT", "market", "buy", "0.001")
print(order)
```

2. Change order (price/qty):

```python
changed = exchange.edit_order(order["id"], "BTC/USDT", "limit", "buy", "0.001", "65000")
print(changed)
```

3. Cancel order:

```python
canceled = exchange.cancel_order(order["id"], "BTC/USDT")
print(canceled)
```

4. Read open orders:

```python
open_orders = exchange.fetch_open_orders("BTC/USDT")
print(len(open_orders))
```

5. Read open positions:

```python
open_positions = exchange.fetch_positions(["BTC/USDT"])
print(len(open_positions))
```

6. Market-data fallback (proxied to `/ccxt/{account_id}/{func}`):

```python
ticker = exchange.fetch_ticker("BTC/USDT")
order_book = exchange.fetch_order_book("BTC/USDT", 20)
print(ticker["symbol"], len(order_book.get("bids", [])))
```

More details and runnable examples:

- `ccxt_driver/README.md`
- `ccxt_driver/examples/basic_usage.py`
- `ccxt_driver/examples/order_flow_oms_first.py`
- `ccxt_driver/examples/marketdata_fallback.py`

