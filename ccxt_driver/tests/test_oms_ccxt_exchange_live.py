import json
import os
import time
import uuid
from decimal import Decimal
from pathlib import Path
from urllib import request as urllib_request

import pytest

from ccxt_driver import OmsCcxtExchange


def _http_json(method: str, url: str, headers: dict[str, str], payload: dict | None = None) -> dict:
    body = None
    req_headers = dict(headers)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib_request.Request(url=url, data=body, headers=req_headers, method=method)
    with urllib_request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _poll_until(fn, timeout_s: int = 40, interval_s: float = 1.5):
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        last = fn()
        if last:
            return last
        time.sleep(interval_s)
    return last


def _load_live_context() -> dict:
    base_url = os.environ.get("CCXT_DRIVER_BASE_URL", "").strip()
    api_key = os.environ.get("CCXT_DRIVER_API_KEY", "").strip()
    account_id = int(os.environ.get("CCXT_DRIVER_ACCOUNT_ID", "0") or 0)
    symbol = os.environ.get("CCXT_DRIVER_SYMBOL", "BTC/USDT").strip()

    if base_url and api_key and account_id > 0:
        return {
            "base_url": base_url.rstrip("/"),
            "api_key": api_key,
            "account_id": account_id,
            "symbol": symbol,
        }

    ctx_path = Path("test/testnet/runtime/context.json")
    if not ctx_path.exists():
        raise RuntimeError(
            "missing test/testnet/runtime/context.json and no CCXT_DRIVER_* env overrides provided"
        )
    ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
    return {
        "base_url": str(ctx.get("base_url", "http://127.0.0.1:8000")).rstrip("/"),
        "api_key": str(ctx.get("internal_api_key", "")).strip(),
        "account_id": int(ctx.get("account_id", 0) or 0),
        "symbol": symbol or str(ctx.get("symbol", "BTC/USDT")),
    }


def _create_strategy(base_url: str, headers: dict[str, str], account_id: int) -> int:
    name = f"ccxt-driver-live-{account_id}-{uuid.uuid4().hex[:8]}"
    out = _http_json(
        "POST",
        f"{base_url}/strategies",
        headers,
        {"name": name, "account_ids": [account_id]},
    )
    strategy_id = int(out.get("strategy_id", 0) or 0)
    if strategy_id <= 0:
        raise RuntimeError(f"strategy creation failed: {out}")
    return strategy_id


@pytest.mark.integration
def test_oms_ccxt_exchange_live_read_paths() -> None:
    if os.environ.get("RUN_LIVE_CCXT_DRIVER", "0") != "1":
        pytest.skip("set RUN_LIVE_CCXT_DRIVER=1 to run live ccxt_driver integration tests")

    cfg = _load_live_context()
    headers = {"x-api-key": cfg["api_key"]}
    strategy_id = _create_strategy(cfg["base_url"], headers, int(cfg["account_id"]))
    ex = OmsCcxtExchange(
        api_key=cfg["api_key"],
        account_id=int(cfg["account_id"]),
        strategy_id=strategy_id,
        base_url=cfg["base_url"],
    )

    ticker = ex.fetch_ticker(cfg["symbol"])
    balance = ex.fetch_balance()
    open_orders = ex.fetch_open_orders(limit=20)
    closed_orders = ex.fetch_closed_orders(limit=20)
    trades = ex.fetch_my_trades(limit=20)
    positions = ex.fetch_positions()

    assert isinstance(ticker, dict)
    assert isinstance(balance, dict)
    assert isinstance(open_orders, list)
    assert isinstance(closed_orders, list)
    assert isinstance(trades, list)
    assert isinstance(positions, list)


@pytest.mark.integration
def test_oms_ccxt_exchange_live_limit_change_cancel() -> None:
    if os.environ.get("RUN_LIVE_CCXT_DRIVER", "0") != "1":
        pytest.skip("set RUN_LIVE_CCXT_DRIVER=1 to run live ccxt_driver integration tests")

    cfg = _load_live_context()
    headers = {"x-api-key": cfg["api_key"]}
    strategy_id = _create_strategy(cfg["base_url"], headers, int(cfg["account_id"]))
    ex = OmsCcxtExchange(
        api_key=cfg["api_key"],
        account_id=int(cfg["account_id"]),
        strategy_id=strategy_id,
        base_url=cfg["base_url"],
    )

    ticker = ex.fetch_ticker(cfg["symbol"])
    last_price = Decimal(str(ticker.get("last") or ticker.get("close") or "0"))
    assert last_price > Decimal("0")
    price_submit = (last_price * Decimal("0.97")).quantize(Decimal("0.01"))
    price_change = (last_price * Decimal("0.96")).quantize(Decimal("0.01"))

    created = ex.create_order(
        cfg["symbol"],
        "limit",
        "buy",
        "0.001",
        price=str(price_submit),
    )
    order_id = int(created["id"])
    assert order_id > 0

    found_open = _poll_until(lambda: ex.fetch_order(order_id), timeout_s=35, interval_s=1.5)
    assert found_open is not None

    edited = ex.edit_order(
        order_id,
        cfg["symbol"],
        "limit",
        "buy",
        amount="0.001",
        price=str(price_change),
    )
    assert int(edited["id"]) == order_id

    canceled = ex.cancel_order(order_id, cfg["symbol"])
    assert int(canceled["id"]) == order_id

    def _is_terminal():
        row = ex.fetch_order(order_id)
        if not row:
            return None
        return row if row.get("status") in {"closed", "canceled", "rejected"} else None

    terminal = _poll_until(_is_terminal, timeout_s=45, interval_s=2.0)
    assert terminal is not None


@pytest.mark.integration
def test_oms_ccxt_exchange_live_generic_marketdata_fallback() -> None:
    if os.environ.get("RUN_LIVE_CCXT_DRIVER", "0") != "1":
        pytest.skip("set RUN_LIVE_CCXT_DRIVER=1 to run live ccxt_driver integration tests")

    cfg = _load_live_context()
    headers = {"x-api-key": cfg["api_key"]}
    strategy_id = _create_strategy(cfg["base_url"], headers, int(cfg["account_id"]))
    ex = OmsCcxtExchange(
        api_key=cfg["api_key"],
        account_id=int(cfg["account_id"]),
        strategy_id=strategy_id,
        base_url=cfg["base_url"],
    )

    markets = ex.fetch_markets()
    loaded = ex.load_markets()
    book = ex.fetch_order_book(cfg["symbol"], 10)
    trades = ex.fetch_trades(cfg["symbol"], 0, 20)
    ohlcv = ex.fetch_ohlcv(cfg["symbol"], "1m", None, 5)

    assert isinstance(markets, list)
    assert isinstance(loaded, dict)
    assert isinstance(book, dict)
    assert "bids" in book or "asks" in book
    assert isinstance(trades, list)
    assert isinstance(ohlcv, list)
