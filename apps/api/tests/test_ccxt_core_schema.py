import asyncio
from decimal import Decimal
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import TypeAdapter, ValidationError
import pytest

from apps.api.app.ccxt_adapter import CCXTAdapter
from apps.api.app.schemas import CcxtCoreCreateOrderInput
import apps.api.app.ccxt_adapter as ccxt_adapter_module


def _schema_test_app() -> FastAPI:
    app = FastAPI()

    @app.post("/ccxt/core/create")
    async def ccxt_core_create(req: CcxtCoreCreateOrderInput) -> dict[str, bool]:
        _ = req
        return {"ok": True}

    return app


def test_ccxt_core_create_order_requires_price_for_limit() -> None:
    adapter = TypeAdapter(CcxtCoreCreateOrderInput)
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "symbol": "BTC/USDT",
                "side": "buy",
                "order_type": "limit",
                "amount": "0.1",
            }
        )


def test_ccxt_core_create_order_accepts_valid_market() -> None:
    adapter = TypeAdapter(CcxtCoreCreateOrderInput)
    parsed = adapter.validate_python(
        {
            "symbol": "ETH/USDT",
            "side": "sell",
            "order_type": "market",
            "amount": "1.25",
        }
    )
    assert parsed.amount == Decimal("1.25")


def test_ccxt_core_endpoint_returns_422_for_invalid_payload() -> None:
    app = _schema_test_app()
    client = TestClient(app)
    response = client.post(
        "/ccxt/core/create",
        json={
            "symbol": "BTC/USDT",
            "side": "buy",
            "order_type": "limit",
            "amount": "0.5",
        },
    )
    assert response.status_code == 422


class _FakeExchange:
    def __init__(self, _config: dict[str, object]) -> None:
        self.has = {"fetchBalance": True}

    async def fetch_balance(self, params: dict[str, object] | None = None) -> dict[str, object]:
        _ = params
        return {"ok": True}

    async def close(self) -> None:
        return None


def test_ccxt_adapter_capability_check_blocks_unsupported() -> None:
    original = ccxt_adapter_module.ccxt_async
    ccxt_adapter_module.ccxt_async = SimpleNamespace(fakeex=_FakeExchange)
    try:
        adapter = CCXTAdapter()
        result = asyncio.run(
            adapter.execute_unified_with_capability(
                exchange_id="fakeex",
                use_testnet=False,
                api_key=None,
                secret=None,
                passphrase=None,
                method="fetch_balance",
                capabilities=["fetchBalance"],
                kwargs={"params": {}},
            )
        )
        assert result["ok"] is True

        with pytest.raises(RuntimeError):
            asyncio.run(
                adapter.execute_unified_with_capability(
                    exchange_id="fakeex",
                    use_testnet=False,
                    api_key=None,
                    secret=None,
                    passphrase=None,
                    method="fetch_open_orders",
                    capabilities=["fetchOpenOrders"],
                    kwargs={"symbol": "BTC/USDT", "since": None, "limit": 10, "params": {}},
                )
            )
    finally:
        ccxt_adapter_module.ccxt_async = original

