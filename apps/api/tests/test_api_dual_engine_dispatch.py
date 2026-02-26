from typing import Any

from fastapi.testclient import TestClient

import apps.api.main as main_module


class _DispatchStub:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, *, host: str, port: int, timeout_seconds: int | None, payload: dict[str, Any]):
        _ = host, port, timeout_seconds
        self.calls.append(payload)
        if payload.get("op") != "ccxt_call":
            return {"ok": True, "result": {}}
        account_id = int(payload.get("account_id", 0) or 0)
        if account_id == 1:
            return {"ok": True, "result": {"engine": "ccxt", "ok": True}}
        if account_id == 2:
            return {"ok": True, "result": {"engine": "ccxtpro", "ok": True}}
        if account_id == 3:
            return {"ok": False, "error": {"code": "engine_unavailable"}}
        return {"ok": False, "error": {"code": "unsupported_engine"}}


def test_ccxt_call_api_supports_both_engines(monkeypatch) -> None:
    stub = _DispatchStub()
    monkeypatch.setattr(main_module, "dispatch_request", stub)
    client = TestClient(main_module.app)

    res_ccxt = client.post(
        "/ccxt/1/fetch_balance",
        headers={"x-api-key": "k"},
        json={"args": [], "kwargs": {}},
    )
    res_pro = client.post(
        "/ccxt/2/fetch_balance",
        headers={"x-api-key": "k"},
        json={"args": [], "kwargs": {}},
    )

    assert res_ccxt.status_code == 200
    assert res_pro.status_code == 200
    assert res_ccxt.json().get("result", {}).get("engine") == "ccxt"
    assert res_pro.json().get("result", {}).get("engine") == "ccxtpro"


def test_ccxt_call_api_exposes_engine_errors(monkeypatch) -> None:
    stub = _DispatchStub()
    monkeypatch.setattr(main_module, "dispatch_request", stub)
    client = TestClient(main_module.app)

    res_unavailable = client.post(
        "/ccxt/3/fetch_balance",
        headers={"x-api-key": "k"},
        json={"args": [], "kwargs": {}},
    )
    res_unsupported = client.post(
        "/ccxt/4/fetch_balance",
        headers={"x-api-key": "k"},
        json={"args": [], "kwargs": {}},
    )

    assert res_unavailable.status_code == 503
    assert (res_unavailable.json().get("detail") or {}).get("code") == "engine_unavailable"

    assert res_unsupported.status_code == 422
    assert (res_unsupported.json().get("detail") or {}).get("code") == "unsupported_engine"
