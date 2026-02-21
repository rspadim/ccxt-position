# REST: CCXT Domain

## `POST /ccxt/{account_id}/{func}`

Generic function gateway to CCXT exchange methods.

- Kept intentionally flexible for exchange-specific behavior.
- This is the fallback path when unified/core endpoints are not enough.

## `POST /ccxt/core/{account_id}/create_order`

Stable typed wrapper for unified CCXT `create_order`.

## `POST /ccxt/core/{account_id}/cancel_order`

Stable typed wrapper for unified CCXT `cancel_order`.

## `POST /ccxt/core/{account_id}/fetch_order`

Stable typed wrapper for unified CCXT `fetch_order`.

## `POST /ccxt/core/{account_id}/fetch_open_orders`

Stable typed wrapper for unified CCXT `fetch_open_orders`.

## `POST /ccxt/core/{account_id}/fetch_balance`

Stable typed wrapper for unified CCXT `fetch_balance`.

## `POST /ccxt/commands`

Batch command execution for one or multiple accounts.

Notes:

- Domain is independent from position OMS tables.
- Raw exchange events are persisted before OMS projection.
- Trade-like methods require account `trade` permission.
- Core endpoints check CCXT capability flags (`exchange.has`) before execution.
- Keep `params` open (`dict[str, Any]`) to support exchange-specific options.
