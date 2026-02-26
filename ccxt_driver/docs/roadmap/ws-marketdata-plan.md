# WS Market Data by Account Plan

## Goal
Allow market data streaming over OMS WebSocket bound to a specific `account_id`, so `ccxtpro` driver can consume real-time-like feed using the same API key/account authorization model.

## Why
- keep auth/permission model centralized in OMS
- avoid direct exchange credentials in client apps
- provide ccxtpro-style `watch*` methods with OMS transport

## MVP Scope (Phase 1)
Implement market data streaming over `/ws` with server-side periodic fetch:

1. New WS action:
- `namespace: "ccxt"`
- `action: "subscribe_market"`
- payload:
  - `account_id` (required)
  - `symbol` (required for symbol channels)
  - `channels` (list): `ticker`, `orderbook`, `trades`, `ohlcv`
  - optional: `timeframe`, `depth`, `limit`, `poll_interval_ms`

2. Optional action:
- `namespace: "ccxt"`
- `action: "unsubscribe_market"`

3. Server loop:
- in WS connection loop, process active market subscriptions
- for each channel, call dispatcher `ccxt_call` with account routing
- emit events:
  - `namespace: "ccxt"`
  - `action: "event"`
  - `event`: `ticker_updated`, `orderbook_updated`, `trades_updated`, `ohlcv_updated`
  - `payload`: `{account_id, symbol, channel, data, ts_ms}`

4. Access control:
- validate account authorization on subscribe
- deny unauthorized account with ws error event

## Phase 2 (Real Exchange Watch)
Replace periodic `ccxt_call` fetch by exchange-level watch in dispatcher:
- `watchTicker`, `watchOrderBook`, `watchTrades`, `watchOHLCV` (when available)
- fanout per `(account_id,symbol,channel)` to many WS clients
- lower latency and fewer duplicate exchange calls

## Driver Integration
`OmsCcxtProExchange`:
- `watch_ticker`, `watch_order_book`, `watch_trades`, `watch_ohlcv`
- connect/auth/subscribe_market on `/ws`
- wait matching event and return normalized data
- fallback to REST polling if WS unavailable

## Acceptance Criteria (MVP)
- client can subscribe ticker/orderbook/trades/ohlcv via `/ws`
- events routed by `account_id` and permissions
- `OmsCcxtProExchange.watch_ticker/watch_order_book/watch_trades/watch_ohlcv` work against running OMS
