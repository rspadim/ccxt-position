# CCXT/CCXTPRO Drop-In Compatibility Plan

## Goal
Users should run existing CCXT/CCXTPRO strategy code against OMS with minimal or no code changes.

## Non-Negotiable Principle
No OMS-specific behavior should leak into user strategy code as a requirement.

## Compatibility Layers
1. Method compatibility:
- same method names and signatures where feasible.

2. Data-shape compatibility:
- return unified CCXT/CCXTPRO fields and types.
- keep original payload in `info`.

3. Capability compatibility:
- expose realistic `has` map through `describe()`.
- include OMS-first supported methods and remote account capabilities.

4. Error compatibility:
- normalize OMS/API errors into predictable exception semantics.

## Coverage Matrix (Phase 1)
### Trading / Account (OMS-first)
- `create_order` / `createOrder`
- `edit_order` / `editOrder`
- `cancel_order` / `cancelOrder`
- `fetch_order` / `fetchOrder`
- `fetch_open_orders` / `fetchOpenOrders`
- `fetch_closed_orders` / `fetchClosedOrders`
- `fetch_my_trades` / `fetchMyTrades`
- `fetch_positions` / `fetchPositions`

### Market Data (generic fallback to CCXT API endpoint)
- `fetch_ticker` / `fetchTicker`
- `fetch_order_book` / `fetchOrderBook`
- `fetch_trades` / `fetchTrades`
- `fetch_ohlcv` / `fetchOHLCV`
- `fetch_markets` / `fetchMarkets`
- `load_markets` / `loadMarkets`

## CCXTPRO Coverage (Phase 1)
- `watch_orders` / `watchOrders`
- `watch_my_trades` / `watchMyTrades`
- `watch_positions` / `watchPositions`

Current backend mode: async polling with CCXTPRO-compatible signatures.
Future backend mode: native WebSocket stream (`/ws`) with same public method contract.

## Contract Tests (Mandatory)
1. Signature-level usage works without OMS-specific params.
2. Return shapes include mandatory CCXT fields:
- orders: `id,symbol,type,side,status,amount,filled,remaining,timestamp,info`
- trades: `id,order,symbol,side,price,amount,timestamp,info`
- positions: `id,symbol,side,contracts,entryPrice,timestamp,info`
3. `has` and `describe()` include expected capabilities.
4. Generic fallback works for unimplemented methods.

## Rollout Plan
1. Stabilize mapping contracts and tests (unit + live smoke).
2. Add WS-backed internals for CCXTPRO `watch*` methods.
3. Add broader market/account parity and error normalization.
4. Publish usage examples mirroring CCXT docs style.
