import argparse
import json
import os
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from urllib import request as urllib_request

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

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


def _load_context() -> dict:
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
            "missing test/testnet/runtime/context.json; run testnet bootstrap or set CCXT_DRIVER_* env vars"
        )
    ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
    return {
        "base_url": str(ctx.get("base_url", "http://127.0.0.1:8000")).rstrip("/"),
        "api_key": str(ctx.get("internal_api_key", "")).strip(),
        "account_id": int(ctx.get("account_id", 0) or 0),
        "symbol": str(ctx.get("symbol", "BTC/USDT")).strip(),
    }


def _create_strategy(base_url: str, api_key: str, account_id: int) -> int:
    headers = {"x-api-key": api_key}
    out = _http_json(
        "POST",
        f"{base_url}/strategies",
        headers,
        {
            "name": f"ccxt-driver-scenarios-{account_id}-{uuid.uuid4().hex[:8]}",
            "account_ids": [account_id],
        },
    )
    sid = int(out.get("strategy_id", 0) or 0)
    if sid <= 0:
        raise RuntimeError(f"strategy creation failed: {out}")
    return sid


def _force_reconcile(base_url: str, api_key: str, account_id: int, symbol: str) -> None:
    headers = {"x-api-key": api_key}
    _http_json(
        "POST",
        f"{base_url}/oms/reconcile",
        headers,
        {
            "account_id": int(account_id),
            "scope": "long",
            "symbols_hint": [symbol],
        },
    )


def _count_deals(base_url: str, api_key: str, account_id: int, strategy_id: int) -> int:
    headers = {"x-api-key": api_key}
    today = time.strftime("%Y-%m-%d")
    out = _http_json(
        "GET",
        (
            f"{base_url}/oms/deals?account_ids={account_id}&strategy_id={strategy_id}"
            f"&start_date={today}&end_date={today}&page=1&page_size=5000"
        ),
        headers,
    )
    return len(out.get("items", []))


def _open_positions(base_url: str, api_key: str, account_id: int, strategy_id: int) -> list[dict]:
    headers = {"x-api-key": api_key}
    out = _http_json(
        "GET",
        f"{base_url}/oms/positions/open?account_ids={account_id}&strategy_id={strategy_id}&limit=200",
        headers,
    )
    return out.get("items", [])


def _wait_order_visible(ex: OmsCcxtExchange, order_id: int, timeout_s: int) -> dict | None:
    return _poll_until(lambda: ex.fetch_order(order_id), timeout_s=timeout_s, interval_s=1.5)


def _send_market(ex: OmsCcxtExchange, symbol: str, side: str, qty: str) -> int:
    out = ex.create_order(symbol, "market", side, qty)
    return int(out["id"])


def _choose_qty(ex: OmsCcxtExchange, symbol: str, quote_to_use: Decimal) -> str:
    ticker = ex.fetch_ticker(symbol)
    last = Decimal(str(ticker.get("last") or ticker.get("close") or "0"))
    if last <= Decimal("0"):
        raise RuntimeError(f"invalid ticker price: {ticker}")
    qty = (quote_to_use / last).quantize(Decimal("0.000001"))
    if qty <= Decimal("0"):
        qty = Decimal("0.000001")
    return format(qty, "f")


def main() -> int:
    parser = argparse.ArgumentParser(description="Live scenarios for root ccxt_driver (OMS-first)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--loops-up", type=int, default=3)
    parser.add_argument("--loops-down", type=int, default=3)
    parser.add_argument("--quote-notional", type=str, default="8")
    args = parser.parse_args()

    cfg = _load_context()
    base_url = cfg["base_url"]
    api_key = cfg["api_key"]
    account_id = int(cfg["account_id"])
    symbol = cfg["symbol"]
    quote_notional = Decimal(str(args.quote_notional))

    if not api_key or account_id <= 0:
        raise RuntimeError("invalid context/api key/account id")

    strategy_id = _create_strategy(base_url, api_key, account_id)
    ex = OmsCcxtExchange(
        api_key=api_key,
        account_id=account_id,
        strategy_id=strategy_id,
        base_url=base_url,
    )
    qty = _choose_qty(ex, symbol, quote_notional)

    if args.verbose:
        print(
            f"[live] base_url={base_url} account_id={account_id} symbol={symbol} "
            f"strategy_id={strategy_id} qty={qty}"
        )

    deals_before = _count_deals(base_url, api_key, account_id, strategy_id)
    positions_before = _open_positions(base_url, api_key, account_id, strategy_id)

    buy_orders: list[int] = []
    for _ in range(max(1, int(args.loops_up))):
        oid = _send_market(ex, symbol, "buy", qty)
        buy_orders.append(oid)
        if not _wait_order_visible(ex, oid, args.timeout_seconds):
            _force_reconcile(base_url, api_key, account_id, symbol)
            if not _wait_order_visible(ex, oid, 20):
                raise RuntimeError(f"buy order not visible: {oid}")

    sell_orders: list[int] = []
    for _ in range(max(1, int(args.loops_down))):
        oid = _send_market(ex, symbol, "sell", qty)
        sell_orders.append(oid)
        if not _wait_order_visible(ex, oid, args.timeout_seconds):
            _force_reconcile(base_url, api_key, account_id, symbol)
            if not _wait_order_visible(ex, oid, 20):
                raise RuntimeError(f"sell order not visible: {oid}")

    reverse_order = _send_market(ex, symbol, "sell", qty)
    close_reverse_order = _send_market(ex, symbol, "buy", qty)
    _force_reconcile(base_url, api_key, account_id, symbol)

    deals_after = _count_deals(base_url, api_key, account_id, strategy_id)
    positions_after = _open_positions(base_url, api_key, account_id, strategy_id)
    expected_min_delta = len(buy_orders) + len(sell_orders)

    if deals_after - deals_before < expected_min_delta:
        raise RuntimeError(
            f"deals did not increase as expected: before={deals_before} after={deals_after} "
            f"expected_min_delta={expected_min_delta}"
        )

    summary = {
        "ok": True,
        "base_url": base_url,
        "account_id": account_id,
        "symbol": symbol,
        "strategy_id": strategy_id,
        "qty": qty,
        "orders": {
            "buy": buy_orders,
            "sell": sell_orders,
            "reverse": reverse_order,
            "close_reverse": close_reverse_order,
        },
        "deals": {
            "before": deals_before,
            "after": deals_after,
            "delta": deals_after - deals_before,
        },
        "positions": {
            "before_count": len(positions_before),
            "after_count": len(positions_after),
            "after_sides": sorted({str(p.get("side", "")) for p in positions_after if p.get("side")}),
        },
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
