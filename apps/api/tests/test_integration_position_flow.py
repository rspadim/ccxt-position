import json
import os
import time
from decimal import Decimal
from urllib import request as urllib_request

import pytest


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


def _poll_until(fn, timeout_s: int = 30, interval_s: float = 1.0):
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        last = fn()
        if last:
            return last
        time.sleep(interval_s)
    return last


@pytest.mark.integration
def test_position_send_change_cancel_live() -> None:
    if os.environ.get("RUN_LIVE_INTEGRATION", "0") != "1":
        pytest.skip("set RUN_LIVE_INTEGRATION=1 to run live integration test")

    base_url = os.environ.get("INTEGRATION_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    api_key = os.environ.get("INTEGRATION_API_KEY", "").strip()
    account_id = int(os.environ.get("INTEGRATION_ACCOUNT_ID", "0"))
    symbol = os.environ.get("INTEGRATION_SYMBOL", "BTC/USDT").strip()

    if not api_key or account_id <= 0:
        pytest.skip("INTEGRATION_API_KEY and INTEGRATION_ACCOUNT_ID are required")

    headers = {"x-api-key": api_key}

    ticker = _http_json(
        "POST",
        f"{base_url}/ccxt/{account_id}/fetch_ticker",
        headers,
        {"args": [symbol], "kwargs": {}},
    )
    last_price = Decimal(str(ticker["result"]["last"]))
    price_submit = (last_price * Decimal("0.995")).quantize(Decimal("0.01"))
    price_change = (last_price * Decimal("0.994")).quantize(Decimal("0.01"))

    send = _http_json(
        "POST",
        f"{base_url}/position/commands",
        headers,
        {
            "account_id": account_id,
            "command": "send_order",
            "payload": {
                "symbol": symbol,
                "side": "buy",
                "order_type": "limit",
                "qty": "0.001",
                "price": str(price_submit),
                "strategy_id": 4242,
                "position_id": 0,
            },
        },
    )
    first = send["results"][0]
    assert first["ok"] is True
    order_id = int(first["order_id"])

    def _get_order() -> dict | None:
        out = _http_json(
            "GET",
            f"{base_url}/position/orders/open?account_id={account_id}",
            headers,
        )
        for item in out.get("items", []):
            if int(item["id"]) == order_id:
                return item
        return None

    order_open = _poll_until(_get_order, timeout_s=30)
    assert order_open is not None

    change = _http_json(
        "POST",
        f"{base_url}/position/commands",
        headers,
        {
            "account_id": account_id,
            "command": "change_order",
            "payload": {"order_id": order_id, "new_price": str(price_change)},
        },
    )
    assert change["results"][0]["ok"] is True

    cancel = _http_json(
        "POST",
        f"{base_url}/position/commands",
        headers,
        {"account_id": account_id, "command": "cancel_order", "payload": {"order_id": order_id}},
    )
    assert cancel["results"][0]["ok"] is True

    def _is_terminal() -> bool:
        hist = _http_json(
            "GET",
            f"{base_url}/position/orders/history?account_id={account_id}",
            headers,
        )
        for item in hist.get("items", []):
            if int(item["id"]) == order_id:
                return item.get("status") in {"CANCELED", "FILLED", "REJECTED"}
        return False

    assert _poll_until(_is_terminal, timeout_s=40) is True


