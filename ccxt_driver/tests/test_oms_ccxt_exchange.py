from typing import Any
from urllib.parse import urlparse, parse_qs

from ccxt_driver.http_client import OmsHttpClient
from ccxt_driver.oms_ccxt_exchange import OmsCcxtExchange


class _RecorderTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, method: str, url: str, headers: dict[str, str], payload: dict[str, Any] | None) -> dict[str, Any]:
        self.calls.append({"method": method, "url": url, "headers": headers, "payload": payload})
        path = urlparse(url).path
        query = parse_qs(urlparse(url).query)

        if path == "/oms/commands":
            command = (payload or {}).get("command")
            if command == "send_order":
                return {"ok": True, "results": [{"ok": True, "order_id": 101, "command_id": 1}]}
            if command == "change_order":
                return {"ok": True, "results": [{"ok": True, "order_id": int((payload or {}).get("payload", {}).get("order_id", 0)), "command_id": 2}]}
            if command == "cancel_order":
                return {"ok": True, "results": [{"ok": True, "order_id": int((payload or {}).get("payload", {}).get("order_id", 0)), "command_id": 3}]}

        if path == "/oms/orders/101":
            return {
                "items": [
                    {
                        "id": 101,
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "order_type": "limit",
                        "status": "SUBMITTED",
                        "qty": "1.0",
                        "filled_qty": "0.2",
                        "price": "100.5",
                        "created_at": "2026-02-25T20:00:00",
                    }
                ]
            }

        if path == "/oms/orders/open":
            return {
                "items": [
                    {
                        "id": 11,
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "order_type": "limit",
                        "status": "SUBMITTED",
                        "qty": "1.0",
                        "filled_qty": "0",
                        "price": "101",
                        "created_at": "2026-02-25T20:00:00",
                    }
                ]
            }

        if path == "/oms/orders/history":
            return {
                "items": [
                    {
                        "id": 12,
                        "symbol": "BTC/USDT",
                        "side": "sell",
                        "order_type": "market",
                        "status": "FILLED",
                        "qty": "2.0",
                        "filled_qty": "2.0",
                        "price": "102",
                        "created_at": "2026-02-25T20:00:00",
                    }
                ],
                "page": int((query.get("page") or ["1"])[0]),
            }

        if path == "/oms/deals":
            return {
                "items": [
                    {
                        "id": 21,
                        "order_id": 12,
                        "symbol": "BTC/USDT",
                        "side": "sell",
                        "qty": "2.0",
                        "price": "102",
                        "fee": "0.1",
                        "fee_currency": "USDT",
                        "executed_at": "2026-02-25T20:00:00",
                    }
                ]
            }

        if path == "/oms/positions/open":
            return {
                "items": [
                    {
                        "id": 31,
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "qty": "0.5",
                        "avg_price": "100",
                        "opened_at": "2026-02-25T20:00:00",
                    }
                ]
            }

        if path == "/oms/positions/history":
            return {
                "items": [
                    {
                        "id": 32,
                        "symbol": "BTC/USDT",
                        "side": "sell",
                        "qty": "0.2",
                        "avg_price": "99",
                        "opened_at": "2026-02-25T20:00:00",
                    }
                ]
            }

        if path == "/ccxt/core/1/fetch_balance":
            return {"ok": True, "result": {"USDT": {"free": 100}}}

        if path == "/ccxt/1/fetch_ticker":
            return {"ok": True, "result": {"symbol": "BTC/USDT", "last": 12345}}

        if path == "/ccxt/1/fetch_order_book":
            return {
                "ok": True,
                "result": {"symbol": "BTC/USDT", "bids": [[100.0, 1.0]], "asks": [[101.0, 1.2]]},
            }

        if path == "/ccxt/1/fetch_trades":
            return {
                "ok": True,
                "result": [
                    {"id": "t1", "symbol": "BTC/USDT", "price": 100.0, "amount": 0.1},
                    {"id": "t2", "symbol": "BTC/USDT", "price": 100.1, "amount": 0.2},
                ],
            }

        if path == "/ccxt/1/fetch_ohlcv":
            return {
                "ok": True,
                "result": [
                    [1700000000000, 100.0, 101.0, 99.0, 100.5, 123.0],
                    [1700000060000, 100.5, 102.0, 100.2, 101.2, 80.0],
                ],
            }

        if path == "/ccxt/1/load_markets":
            return {
                "ok": True,
                "result": {
                    "BTC/USDT": {"symbol": "BTC/USDT", "active": True},
                    "ETH/USDT": {"symbol": "ETH/USDT", "active": True},
                },
            }

        if path == "/ccxt/1/fetch_markets":
            return {
                "ok": True,
                "result": [
                    {"symbol": "BTC/USDT", "active": True},
                    {"symbol": "ETH/USDT", "active": True},
                ],
            }

        if path == "/ccxt/1/fetch_funding_rate":
            return {
                "ok": True,
                "result": {"symbol": "BTC/USDT", "fundingRate": 0.0001},
            }

        if path == "/ccxt/1/describe":
            return {
                "ok": True,
                "result": {
                    "id": "binance",
                    "has": {
                        "fetchOHLCV": True,
                        "fetchFundingRate": True,
                        "createOrder": False,
                    },
                },
            }

        return {"ok": True}


def _make_exchange(recorder: _RecorderTransport) -> OmsCcxtExchange:
    http = OmsHttpClient(base_url="http://local", api_key="k", transport=recorder)
    return OmsCcxtExchange(api_key="k", account_id=1, strategy_id=7, base_url="http://local", http_client=http)


def test_create_order_routes_to_oms_send_order() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    out = ex.create_order("BTC/USDT", "limit", "buy", "1.25", price="100")

    assert out["id"] == "101"
    last = rec.calls[-1]
    assert last["method"] == "POST"
    assert urlparse(last["url"]).path == "/oms/commands"
    assert last["payload"]["command"] == "send_order"
    assert last["payload"]["payload"]["strategy_id"] == 7


def test_edit_and_cancel_order_route_to_oms_commands() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    edited = ex.edit_order(99, "BTC/USDT", "limit", "buy", amount="1.1", price="101")
    canceled = ex.cancel_order(99)

    assert edited["id"] == "99"
    assert canceled["id"] == "99"
    assert rec.calls[-2]["payload"]["command"] == "change_order"
    assert rec.calls[-1]["payload"]["command"] == "cancel_order"


def test_fetch_order_by_id_uses_dedicated_endpoint() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    row = ex.fetch_order(101)

    assert row is not None
    assert row["id"] == "101"
    assert row["status"] == "open"
    assert row["remaining"] == 0.8


def test_fetch_open_orders_maps_rows() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rows = ex.fetch_open_orders(limit=10)

    assert len(rows) == 1
    assert rows[0]["id"] == "11"
    assert rows[0]["status"] == "open"
    assert rows[0]["type"] == "limit"


def test_fetch_closed_orders_maps_rows() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rows = ex.fetch_closed_orders(limit=10, params={"page": 1, "page_size": 20})

    assert len(rows) == 1
    assert rows[0]["id"] == "12"
    assert rows[0]["status"] == "closed"
    assert rows[0]["side"] == "sell"


def test_fetch_my_trades_maps_rows() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rows = ex.fetch_my_trades(limit=10)

    assert len(rows) == 1
    assert rows[0]["id"] == "21"
    assert rows[0]["order"] == "12"
    assert rows[0]["fee"]["currency"] == "USDT"


def test_fetch_positions_open_and_history() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    open_rows = ex.fetch_positions()
    history_rows = ex.fetch_positions(params={"history": True})

    assert len(open_rows) == 1
    assert open_rows[0]["id"] == "31"
    assert len(history_rows) == 1
    assert history_rows[0]["id"] == "32"


def test_fetch_balance_and_ticker_use_ccxt_endpoints() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    bal = ex.fetch_balance()
    ticker = ex.fetch_ticker("BTC/USDT")

    assert bal["USDT"]["free"] == 100
    assert ticker["last"] == 12345
    assert any(urlparse(c["url"]).path == "/ccxt/core/1/fetch_balance" for c in rec.calls)
    assert any(urlparse(c["url"]).path == "/ccxt/1/fetch_ticker" for c in rec.calls)


def test_generic_ccxt_fallback_supports_unknown_methods() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    book = ex.fetch_order_book("BTC/USDT", 5)
    assert isinstance(book, dict)
    assert book["symbol"] == "BTC/USDT"
    assert any(urlparse(c["url"]).path == "/ccxt/1/fetch_order_book" for c in rec.calls)


def test_generic_ccxt_fallback_normalizes_camel_case() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    rate = ex.call_ccxt("fetchFundingRate", "BTC/USDT")
    assert isinstance(rate, dict)
    assert rate["symbol"] == "BTC/USDT"
    assert any(urlparse(c["url"]).path == "/ccxt/1/fetch_funding_rate" for c in rec.calls)


def test_load_has_merges_remote_describe_with_oms_defaults() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    has_map = ex.load_has(refresh=True)
    desc = ex.describe()

    assert has_map["createOrder"] is True  # OMS-first override
    assert has_map["fetchOHLCV"] is True  # remote capability preserved
    assert has_map["fetchFundingRate"] is True
    assert isinstance(desc, dict)
    assert desc["has"]["fetchOHLCV"] is True
    assert any(urlparse(c["url"]).path == "/ccxt/1/describe" for c in rec.calls)


def test_generic_ccxt_fallback_for_market_data_suite() -> None:
    rec = _RecorderTransport()
    ex = _make_exchange(rec)

    trades = ex.fetch_trades("BTC/USDT", 0, 10)
    ohlcv = ex.fetch_ohlcv("BTC/USDT", "1m", 0, 2)
    markets = ex.fetch_markets()
    loaded = ex.load_markets()

    assert isinstance(trades, list) and len(trades) == 2
    assert isinstance(ohlcv, list) and len(ohlcv) == 2
    assert isinstance(markets, list) and len(markets) >= 2
    assert isinstance(loaded, dict) and "BTC/USDT" in loaded
    assert any(urlparse(c["url"]).path == "/ccxt/1/fetch_trades" for c in rec.calls)
    assert any(urlparse(c["url"]).path == "/ccxt/1/fetch_ohlcv" for c in rec.calls)
    assert any(urlparse(c["url"]).path == "/ccxt/1/fetch_markets" for c in rec.calls)
    assert any(urlparse(c["url"]).path == "/ccxt/1/load_markets" for c in rec.calls)
