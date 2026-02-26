import asyncio
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from ccxt_driver.http_client import OmsHttpClient
from ccxt_driver.oms_ccxtpro_exchange import OmsCcxtProExchange


class _RecorderTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, method: str, url: str, headers: dict[str, str], payload: dict[str, Any] | None) -> dict[str, Any]:
        self.calls.append({"method": method, "url": url, "headers": headers, "payload": payload})
        path = urlparse(url).path

        if path == "/ccxt/1/describe":
            return {
                "ok": True,
                "result": {
                    "id": "binance",
                    "has": {"fetchOHLCV": True, "watchOHLCV": False},
                },
            }

        if path == "/oms/orders/open":
            return {
                "items": [
                    {
                        "id": 1001,
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "order_type": "limit",
                        "status": "SUBMITTED",
                        "qty": "1.0",
                        "filled_qty": "0.0",
                        "price": "100.0",
                        "created_at": "2026-02-26T10:00:00",
                    }
                ]
            }

        if path == "/oms/orders/history":
            return {
                "items": [
                    {
                        "id": 1002,
                        "symbol": "BTC/USDT",
                        "side": "sell",
                        "order_type": "market",
                        "status": "FILLED",
                        "qty": "0.5",
                        "filled_qty": "0.5",
                        "price": "101.0",
                        "created_at": "2026-02-26T10:00:01",
                    }
                ]
            }

        if path == "/oms/deals":
            return {
                "items": [
                    {
                        "id": 2001,
                        "order_id": 1002,
                        "symbol": "BTC/USDT",
                        "side": "sell",
                        "qty": "0.5",
                        "price": "101.0",
                        "fee": "0.01",
                        "fee_currency": "USDT",
                        "executed_at": "2026-02-26T10:00:02",
                    }
                ]
            }

        if path == "/oms/positions/open":
            return {
                "items": [
                    {
                        "id": 3001,
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "qty": "0.5",
                        "avg_price": "100.5",
                        "opened_at": "2026-02-26T10:00:03",
                    }
                ]
            }

        return {"ok": True}


def _make_exchange(rec: _RecorderTransport) -> OmsCcxtProExchange:
    http = OmsHttpClient(base_url="http://local", api_key="k", transport=rec)
    ex = OmsCcxtProExchange(
        api_key="k",
        account_id=1,
        strategy_id=7,
        base_url="http://local",
        poll_interval_seconds=0.01,
        watch_timeout_seconds=0.1,
    )
    ex.http = http
    return ex


def test_ccxtpro_has_includes_watch_capabilities() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)
    has_map = ex.load_has(refresh=True)

    assert has_map["watchOrders"] is True
    assert has_map["watchMyTrades"] is True
    assert has_map["watchPositions"] is True
    assert has_map["fetchOHLCV"] is True


def test_watch_orders_returns_ccxt_order_shape() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rows = asyncio.run(ex.watch_orders(symbol="BTC/USDT", limit=20))
    assert isinstance(rows, list) and len(rows) >= 1
    row = rows[0]
    for key in ["id", "symbol", "type", "side", "status", "amount", "filled", "remaining", "timestamp", "info"]:
        assert key in row
    assert isinstance(Decimal(str(row["amount"])), Decimal)


def test_watch_my_trades_returns_ccxt_trade_shape() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rows = asyncio.run(ex.watch_my_trades(symbol="BTC/USDT", limit=20))
    assert isinstance(rows, list) and len(rows) >= 1
    row = rows[0]
    for key in ["id", "order", "symbol", "side", "price", "amount", "timestamp", "info"]:
        assert key in row


def test_watch_positions_returns_ccxt_position_shape() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rows = asyncio.run(ex.watch_positions(symbols=["BTC/USDT"]))
    assert isinstance(rows, list) and len(rows) >= 1
    row = rows[0]
    for key in ["id", "symbol", "side", "contracts", "entryPrice", "timestamp", "info"]:
        assert key in row


def test_extract_ws_rows_orders_snapshot() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    msg = {
        "namespace": "position",
        "event": "snapshot_open_orders",
        "payload": {
            "account_id": 1,
            "items": [
                {
                    "id": 9001,
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "order_type": "limit",
                    "status": "SUBMITTED",
                    "qty": "1.0",
                    "filled_qty": "0.0",
                    "price": "100.0",
                    "created_at": "2026-02-26T10:00:00",
                }
            ],
        },
    }
    rows = ex._extract_ws_rows(kind="orders", msg=msg, symbol="BTC/USDT")
    assert len(rows) == 1
    assert rows[0]["id"] == "9001"


def test_extract_ws_rows_trades_event() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    msg = {
        "namespace": "position",
        "event": "deal_updated",
        "payload": {
            "id": 9101,
            "order_id": 9001,
            "symbol": "BTC/USDT",
            "side": "buy",
            "qty": "0.2",
            "price": "100.0",
            "executed_at": "2026-02-26T10:00:00",
        },
    }
    rows = ex._extract_ws_rows(kind="trades", msg=msg, symbol="BTC/USDT")
    assert len(rows) == 1
    assert rows[0]["id"] == "9101"


def test_extract_ws_rows_positions_snapshot() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    msg = {
        "namespace": "position",
        "event": "snapshot_open_positions",
        "payload": {
            "account_id": 1,
            "items": [
                {
                    "id": 9201,
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "qty": "0.5",
                    "avg_price": "100.5",
                    "opened_at": "2026-02-26T10:00:00",
                }
            ],
        },
    }
    rows = ex._extract_ws_rows(kind="positions", msg=msg, symbol="BTC/USDT")
    assert len(rows) == 1
    assert rows[0]["id"] == "9201"


def test_watch_ticker_uses_ws_payload_when_available() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    async def _fake_ws(*args, **kwargs):
        _ = args, kwargs
        return {"symbol": "BTC/USDT", "last": 123.45}

    ex._watch_market_via_ws = _fake_ws  # type: ignore[attr-defined]
    out = asyncio.run(ex.watch_ticker("BTC/USDT"))
    assert isinstance(out, dict)
    assert out["last"] == 123.45


def test_watch_marketdata_falls_back_to_ccxt_call_when_ws_unavailable() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    async def _none_ws(*args, **kwargs):
        _ = args, kwargs
        return None

    ex._watch_market_via_ws = _none_ws  # type: ignore[attr-defined]
    out_book = asyncio.run(ex.watch_order_book("BTC/USDT", limit=10))
    out_trades = asyncio.run(ex.watch_trades("BTC/USDT", limit=10))
    out_ohlcv = asyncio.run(ex.watch_ohlcv("BTC/USDT", timeframe="1m", limit=2))

    assert isinstance(out_book, dict)
    assert isinstance(out_trades, list)
    assert isinstance(out_ohlcv, list)
