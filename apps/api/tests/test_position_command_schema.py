from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import TypeAdapter, ValidationError
import pytest

from apps.api.app.schemas import CommandInput


def _schema_test_app() -> FastAPI:
    app = FastAPI()

    @app.post("/oms/commands")
    async def position_commands(commands: CommandInput | list[CommandInput]) -> dict[str, bool]:
        return {"ok": True}

    return app


def test_command_schema_send_order_accepts_aliases() -> None:
    adapter = TypeAdapter(CommandInput)
    parsed = adapter.validate_python(
        {
            "account_id": 1,
            "command": "send_order",
            "payload": {
                "symbol": "BTC/USDT",
                "side": "buy",
                "type": "limit",
                "amount": "0.5",
                "price": "50000",
            },
        }
    )
    payload = parsed.payload
    assert payload.order_type == "limit"
    assert payload.qty == Decimal("0.5")
    assert payload.price == Decimal("50000")


def test_command_schema_change_order_requires_new_price_or_qty() -> None:
    adapter = TypeAdapter(CommandInput)
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "account_id": 1,
                "command": "change_order",
                "payload": {"order_id": 10},
            }
        )


def test_command_schema_close_position_requires_position_id() -> None:
    adapter = TypeAdapter(CommandInput)
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {
                "account_id": 1,
                "command": "close_position",
                "payload": {"order_type": "market"},
            }
        )


def test_position_commands_endpoint_returns_422_for_invalid_limit_order() -> None:
    app = _schema_test_app()
    client = TestClient(app)
    response = client.post(
        "/oms/commands",
        json={
            "account_id": 1,
            "command": "send_order",
            "payload": {
                "symbol": "BTC/USDT",
                "side": "buy",
                "order_type": "limit",
                "qty": "0.1",
            },
        },
    )
    assert response.status_code == 422


def test_position_commands_endpoint_returns_422_for_invalid_change_order() -> None:
    app = _schema_test_app()
    client = TestClient(app)
    response = client.post(
        "/oms/commands",
        json={
            "account_id": 1,
            "command": "change_order",
            "payload": {"order_id": 123},
        },
    )
    assert response.status_code == 422
