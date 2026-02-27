from typing import Any

from fastapi.testclient import TestClient

import apps.api.main as main_module


class _OmsDispatchStub:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, *, host: str, port: int, timeout_seconds: int | None, payload: dict[str, Any]):
        _ = host, port, timeout_seconds
        self.calls.append(payload)
        if payload.get("op") != "oms_query_multi":
            return {"ok": True, "result": []}
        query = str(payload.get("query", ""))
        if query == "orders_history":
            items = [
                {
                    "id": i,
                    "command_id": None,
                    "account_id": 1,
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "order_type": "market",
                    "status": "FILLED",
                    "strategy_id": 1,
                    "position_id": 1,
                    "previous_position_id": None,
                    "reason": "api",
                    "comment": None,
                    "client_order_id": None,
                    "exchange_order_id": f"ex-{i}",
                    "qty": "0.001",
                    "price": "60000",
                    "stop_loss": None,
                    "stop_gain": None,
                    "filled_qty": "0.001",
                    "avg_fill_price": "60000",
                    "edit_replace_state": None,
                    "edit_replace_at": None,
                    "edit_replace_orphan_order_id": None,
                    "edit_replace_origin_order_id": None,
                    "created_at": "2026-02-01T00:00:00",
                    "updated_at": "2026-02-01T00:00:00",
                    "closed_at": "2026-02-01T00:00:01",
                }
                for i in range(1, 11)
            ]
            return {"ok": True, "result": items}
        if query == "deals":
            items = [
                {
                    "id": i,
                    "account_id": 1,
                    "order_id": i,
                    "position_id": 1,
                    "previous_position_id": None,
                    "symbol": "BTC/USDT",
                    "side": "buy",
                    "qty": "0.001",
                    "price": "60000",
                    "fee": "0",
                    "fee_currency": "USDT",
                    "pnl": "0",
                    "strategy_id": 1,
                    "reason": "api",
                    "comment": None,
                    "reconciled": True,
                    "exchange_trade_id": f"tr-{i}",
                    "created_at": "2026-02-01T00:00:00",
                    "executed_at": "2026-02-01T00:00:00",
                }
                for i in range(1, 9)
            ]
            return {"ok": True, "result": items}
        if query == "positions_history":
            items = [
                {
                    "id": i,
                    "account_id": 1,
                    "symbol": "BTC/USDT",
                    "strategy_id": 1,
                    "side": "buy",
                    "qty": "0.001",
                    "avg_price": "60000",
                    "stop_loss": None,
                    "stop_gain": None,
                    "state": "closed",
                    "reason": "api",
                    "comment": None,
                    "opened_at": "2026-02-01T00:00:00",
                    "updated_at": "2026-02-01T00:00:00",
                    "closed_at": "2026-02-01T00:00:01",
                }
                for i in range(1, 7)
            ]
            return {"ok": True, "result": items}
        return {"ok": True, "result": []}


def test_orders_history_pagination(monkeypatch) -> None:
    stub = _OmsDispatchStub()
    monkeypatch.setattr(main_module, "dispatch_request", stub)
    client = TestClient(main_module.app)

    res = client.get(
        "/oms/orders/history?account_ids=1&start_date=2026-02-01&end_date=2026-02-27&page=2&page_size=3",
        headers={"x-api-key": "k"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 10
    assert data["page"] == 2
    assert data["page_size"] == 3
    assert [row["id"] for row in data["items"]] == [4, 5, 6]


def test_deals_pagination(monkeypatch) -> None:
    stub = _OmsDispatchStub()
    monkeypatch.setattr(main_module, "dispatch_request", stub)
    client = TestClient(main_module.app)

    res = client.get(
        "/oms/deals?account_ids=1&start_date=2026-02-01&end_date=2026-02-27&page=3&page_size=2",
        headers={"x-api-key": "k"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 8
    assert data["page"] == 3
    assert data["page_size"] == 2
    assert [row["id"] for row in data["items"]] == [5, 6]


def test_positions_history_pagination(monkeypatch) -> None:
    stub = _OmsDispatchStub()
    monkeypatch.setattr(main_module, "dispatch_request", stub)
    client = TestClient(main_module.app)

    res = client.get(
        "/oms/positions/history?account_ids=1&start_date=2026-02-01&end_date=2026-02-27&page=2&page_size=4",
        headers={"x-api-key": "k"},
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 6
    assert data["page"] == 2
    assert data["page_size"] == 4
    assert [row["id"] for row in data["items"]] == [5, 6]
