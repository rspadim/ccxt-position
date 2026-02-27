# OMS Exchange Driver (CCXT-style)

This package provides an OMS-first exchange interface with CCXT-like method names.

## Install/Import

From this repository root:

```python
from ccxt_driver import OmsCcxtExchange
```

## Quick Start

```python
from ccxt_driver import OmsCcxtExchange

exchange = OmsCcxtExchange(
    api_key="YOUR_INTERNAL_API_KEY",
    account_id=1,
    strategy_id=1001,
    base_url="http://127.0.0.1:8000",
)

ticker = exchange.fetch_ticker("BTC/USDT")
print(ticker)
```

## Auto Patch (no code change in client bot)

If a third-party bot imports `ccxt` directly and you want OMS integration automatically, install an auto patch into `sitecustomize.py`:

```bash
py -3.13 -m ccxt_driver.autopatch install --scope user --mode safe
```

Check status:

```bash
py -3.13 -m ccxt_driver.autopatch status --scope user
```

Remove:

```bash
py -3.13 -m ccxt_driver.autopatch uninstall --scope user
```

Environment variables used at runtime:

- `CCXT_OMS_AUTOPATCH=1`
- `CCXT_OMS_AUTOPATCH_MODE=safe|aggressive`
- `CCXT_OMS_OVERRIDE_EXCHANGES=binance,bybit` (aggressive mode)
- `CCXT_OMS_API_KEY=...`
- `CCXT_OMS_ACCOUNT_ID=...`
- `CCXT_OMS_STRATEGY_ID=...`
- `CCXT_OMS_BASE_URL=http://127.0.0.1:8000`

`safe` mode adds `ccxt.oms_exchange(...)` factory without overriding existing exchanges.
`aggressive` mode can override selected exchange constructors listed in `CCXT_OMS_OVERRIDE_EXCHANGES`.

## OMS-first methods

These methods use `\/oms/*` by default:

- `create_order`
- `edit_order`
- `cancel_order`
- `fetch_order`
- `fetch_open_orders`
- `fetch_closed_orders`
- `fetch_my_trades`
- `fetch_positions`

## CCXT fallback methods

If a method is not explicitly implemented, it is proxied to:

- `POST /ccxt/{account_id}/{func}`

Examples:

```python
exchange.fetch_order_book("BTC/USDT", 20)
exchange.fetch_trades("BTC/USDT", None, 50)
exchange.fetch_ohlcv("BTC/USDT", "1m", None, 100)
exchange.fetch_markets()
exchange.load_markets()
```

You can also call directly:

```python
exchange.call_ccxt("fetchFundingRate", "BTC/USDT")
```

## Capabilities (`has`) and `describe()`

`exchange.describe()` merges:

- remote capabilities from account CCXT `describe.has`
- OMS-first capabilities forced to `True` for core trading/OMS operations

```python
desc = exchange.describe()
print(desc["has"].get("fetchOHLCV"))
```

## Live Test Commands

Unit tests:

```bash
py -3.13 -m pytest -q ccxt_driver/tests/test_oms_ccxt_exchange.py
```

Live integration tests (requires running API + valid context):

```bash
set RUN_LIVE_CCXT_DRIVER=1
py -3.13 -m pytest -q ccxt_driver/tests/test_oms_ccxt_exchange_live.py
```

Scenario-style live script:

```bash
py -3.13 ccxt_driver/tests/scenarios_live.py --verbose --timeout-seconds 10
```

## Examples

See:

- `ccxt_driver/examples/basic_usage.py`
- `ccxt_driver/examples/marketdata_fallback.py`
- `ccxt_driver/examples/order_flow_oms_first.py`
- `ccxt_driver/examples/ccxtpro_watch_basic.py`
- `ccxt_driver/examples/ccxtpro_watch_loop.py`

## CCXTPRO-style Usage

```python
import asyncio
from ccxt_driver import OmsCcxtProExchange

async def main():
    ex = OmsCcxtProExchange(
        api_key="YOUR_INTERNAL_API_KEY",
        account_id=1,
        strategy_id=1001,
        base_url="http://127.0.0.1:8000",
    )
    orders = await ex.watch_orders("BTC/USDT")
    trades = await ex.watch_my_trades("BTC/USDT")
    positions = await ex.watch_positions(["BTC/USDT"])
    print(len(orders), len(trades), len(positions))

asyncio.run(main())
```

Notes:
- `watch*` currently tries OMS WebSocket (`/ws`) first.
- If WS is unavailable, it falls back to async polling.

Market data watch methods are also available in `OmsCcxtProExchange`:

- `watch_ticker(symbol)`
- `watch_order_book(symbol, limit=...)`
- `watch_trades(symbol, limit=...)`
- `watch_ohlcv(symbol, timeframe="1m", limit=...)`

These methods use OMS WebSocket action `ccxt/subscribe_market` (MVP) and
fallback to REST-based polling when WS is unavailable.
