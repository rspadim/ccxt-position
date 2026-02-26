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


class _FakeProExchange:
    instances_created = 0
    instances_closed = 0

    def __init__(self, _config: dict[str, object]) -> None:
        _FakeProExchange.instances_created += 1
        self.has = {}

    async def fetch_ticker(self, symbol: str) -> dict[str, object]:
        return {"symbol": symbol, "last": 1}

    async def close(self) -> None:
        _FakeProExchange.instances_closed += 1
        return None


def test_ccxt_adapter_capability_check_blocks_unsupported() -> None:
    original = ccxt_adapter_module.ccxt_async
    ccxt_adapter_module.ccxt_async = SimpleNamespace(fakeex=_FakeExchange)
    try:
        adapter = CCXTAdapter()
        result = asyncio.run(
                adapter.execute_unified_with_capability(
                    exchange_id="ccxt.fakeex",
                    use_testnet=False,
                    api_key=None,
                    secret=None,
                    passphrase=None,
                    extra_config={},
                    method="fetch_balance",
                    capabilities=["fetchBalance"],
                    kwargs={"params": {}},
                )
        )
        assert result["ok"] is True

        with pytest.raises(RuntimeError):
            asyncio.run(
                adapter.execute_unified_with_capability(
                    exchange_id="ccxt.fakeex",
                    use_testnet=False,
                    api_key=None,
                    secret=None,
                    passphrase=None,
                    extra_config={},
                    method="fetch_open_orders",
                    capabilities=["fetchOpenOrders"],
                    kwargs={"symbol": "BTC/USDT", "since": None, "limit": 10, "params": {}},
                )
            )
    finally:
        ccxt_adapter_module.ccxt_async = original


def test_ccxtpro_adapter_reuses_session_per_session_key() -> None:
    original_pro = ccxt_adapter_module.ccxt_pro
    ccxt_adapter_module.ccxt_pro = SimpleNamespace(fakepro=_FakeProExchange)
    _FakeProExchange.instances_created = 0
    _FakeProExchange.instances_closed = 0
    try:
        adapter = CCXTAdapter(session_ttl_seconds=9999)
        one = asyncio.run(
            adapter.execute_method(
                exchange_id="ccxtpro.fakepro",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["BTC/USDT"],
                session_key="account:1",
            )
        )
        two = asyncio.run(
            adapter.execute_method(
                exchange_id="ccxtpro.fakepro",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["ETH/USDT"],
                session_key="account:1",
            )
        )
        assert one["symbol"] == "BTC/USDT"
        assert two["symbol"] == "ETH/USDT"
        assert _FakeProExchange.instances_created == 1
        assert _FakeProExchange.instances_closed == 0
        asyncio.run(adapter.close_all_sessions())
        assert _FakeProExchange.instances_closed == 1
    finally:
        ccxt_adapter_module.ccxt_pro = original_pro


def test_ccxtpro_adapter_isolates_sessions_across_accounts() -> None:
    original_pro = ccxt_adapter_module.ccxt_pro
    ccxt_adapter_module.ccxt_pro = SimpleNamespace(fakepro=_FakeProExchange)
    _FakeProExchange.instances_created = 0
    _FakeProExchange.instances_closed = 0
    try:
        adapter = CCXTAdapter(session_ttl_seconds=9999)
        asyncio.run(
            adapter.execute_method(
                exchange_id="ccxtpro.fakepro",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["BTC/USDT"],
                session_key="account:1",
            )
        )
        asyncio.run(
            adapter.execute_method(
                exchange_id="ccxtpro.fakepro",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["BTC/USDT"],
                session_key="account:2",
            )
        )
        assert _FakeProExchange.instances_created == 2
        asyncio.run(adapter.close_all_sessions())
        assert _FakeProExchange.instances_closed == 2
    finally:
        ccxt_adapter_module.ccxt_pro = original_pro


def test_ccxt_adapter_does_not_persist_regular_ccxt_sessions() -> None:
    original = ccxt_adapter_module.ccxt_async
    ccxt_adapter_module.ccxt_async = SimpleNamespace(fakeex=_FakeProExchange)
    _FakeProExchange.instances_created = 0
    _FakeProExchange.instances_closed = 0
    try:
        adapter = CCXTAdapter(session_ttl_seconds=9999)
        asyncio.run(
            adapter.execute_method(
                exchange_id="ccxt.fakeex",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["BTC/USDT"],
                session_key="account:1",
            )
        )
        asyncio.run(
            adapter.execute_method(
                exchange_id="ccxt.fakeex",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["BTC/USDT"],
                session_key="account:1",
            )
        )
        assert _FakeProExchange.instances_created == 2
        assert _FakeProExchange.instances_closed == 2
    finally:
        ccxt_adapter_module.ccxt_async = original


def test_ccxt_adapter_session_status_exposes_counts() -> None:
    original_pro = ccxt_adapter_module.ccxt_pro
    ccxt_adapter_module.ccxt_pro = SimpleNamespace(fakepro=_FakeProExchange)
    _FakeProExchange.instances_created = 0
    _FakeProExchange.instances_closed = 0
    try:
        adapter = CCXTAdapter(session_ttl_seconds=123)
        asyncio.run(
            adapter.execute_method(
                exchange_id="ccxtpro.fakepro",
                use_testnet=False,
                api_key="k",
                secret="s",
                passphrase=None,
                extra_config={},
                method="fetch_ticker",
                args=["BTC/USDT"],
                session_key="account:99",
            )
        )
        st = adapter.get_session_status()
        assert st["session_count_total"] == 1
        assert int(st["session_count_by_engine"]["ccxtpro"]) == 1
        assert 99 in st["session_account_ids"]
        assert int(st["session_ttl_seconds"]) == 123
        asyncio.run(adapter.close_all_sessions())
    finally:
        ccxt_adapter_module.ccxt_pro = original_pro
