# OMS-First CCXT Driver Roadmap

## Objective
Create a CCXT-compatible driver that uses OMS as the main source of truth for orders/deals/positions, preserving internal IDs.

## Architecture Decision
- `createOrder`, `editOrder`, `cancelOrder`, `fetchOrder`, `fetchOpenOrders`, `fetchClosedOrders`, `fetchMyTrades`, `fetchPositions`:
  - use `\/oms/*` endpoints by default.
- `fetchBalance` and `fetchTicker`:
  - use `\/ccxt/*` for now.
- Fallback:
  - if OMS endpoint is missing, use `\/ccxt/{account_id}/*`.

## Exchange Configuration
- `apiKey`: internal API key (sent as `x-api-key`).
- `options.account_id`: required.
- `options.strategy_id`: optional default.
- `params.strategy_id`: per-call override.

## Required API Endpoints
To reduce roundtrips and provide a stable driver contract:

1. `GET /oms/orders/{ids}`
- `ids` accepts CSV: `123` or `123,124,125`.
- returns orders by internal ID (open or historical).

2. `GET /oms/deals/{ids}`
- `ids` accepts CSV.
- returns deals by internal ID.

3. `GET /oms/positions/{ids}`
- `ids` accepts CSV.
- returns positions by internal ID (open or historical).

Keep existing filtered endpoints as well:
- `/oms/orders/open`
- `/oms/orders/history`
- `/oms/deals`
- `/oms/positions/open`
- `/oms/positions/history`

## CCXT -> OMS Mapping (MVP)
1. `createOrder` -> `POST /oms/commands` (`send_order`)
2. `editOrder` -> `POST /oms/commands` (`change_order`)
3. `cancelOrder` -> `POST /oms/commands` (`cancel_order`)
4. `fetchOrder` -> `GET /oms/orders/{ids}`
5. `fetchOpenOrders` -> `GET /oms/orders/open`
6. `fetchClosedOrders` -> `GET /oms/orders/history`
7. `fetchMyTrades` -> `GET /oms/deals`
8. `fetchPositions` -> `GET /oms/positions/open` (history optional by params)
9. `fetchBalance` -> `POST /ccxt/core/{account_id}/fetch_balance`
10. `fetchTicker` -> `POST /ccxt/{account_id}/fetch_ticker`

## Suggested Project Structure (root-level module)
- `ccxt_driver/oms_ccxt_exchange.py`
- `ccxt_driver/http_client.py`
- `ccxt_driver/mappers/order_mapper.py`
- `ccxt_driver/mappers/trade_mapper.py`
- `ccxt_driver/mappers/position_mapper.py`
- `ccxt_driver/tests/test_*.py`
- `ccxt_driver/docs/roadmap/oms-first-ccxt-driver-plan.md`

## Phase 1 (MVP)
- Implement sync CCXT driver with the 10 methods above.
- Add `GET /oms/*/{ids}` endpoints with CSV support.
- Add unit tests for:
  - request/response contracts
  - OMS -> CCXT unified mapping
  - error handling (`invalid_api_key`, `account_not_found`, `validation_error`, etc).

## Phase 2 (Hardening)
- retry/backoff and default timeouts.
- normalize OMS errors to CCXT exceptions.
- optional short cache for `fetchTicker`.

## Phase 3 (CCXTPRO / WebSocket)
- implement ccxtpro version.
- map `watchOrders`, `watchMyTrades`, `watchPositions` to internal WS stream.
- preserve auth contract (`apiKey + account_id + strategy_id`).

## Acceptance Criteria
- A strategy using CCXT interface runs against OMS with no strategy-code changes.
- Returned order/deal/position IDs are OMS internal IDs.
- `fetchBalance`/`fetchTicker` work through `ccxt/*` without breaking OMS-first flow.
