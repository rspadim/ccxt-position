from typing import Any, Literal

from pydantic import BaseModel, Field


CommandType = Literal[
    "send_order",
    "cancel_order",
    "change_order",
    "close_by",
    "close_position",
]


class CommandInput(BaseModel):
    account_id: int = Field(gt=0)
    command: CommandType
    payload: dict[str, Any] = Field(default_factory=dict)
    request_id: str | None = None


class CommandResult(BaseModel):
    index: int
    ok: bool
    command_id: int | None = None
    order_id: int | None = None
    error: dict[str, Any] | None = None


class CommandsResponse(BaseModel):
    results: list[CommandResult]


class CcxtCallInput(BaseModel):
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)


class CcxtBatchItem(BaseModel):
    account_id: int = Field(gt=0)
    func: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)


class CcxtBatchResponse(BaseModel):
    results: list[dict[str, Any]]


class ReassignInput(BaseModel):
    account_id: int = Field(gt=0)
    deal_ids: list[int] = Field(default_factory=list)
    order_ids: list[int] = Field(default_factory=list)
    target_magic_id: int = 0
    target_position_id: int = 0


class PositionOrderModel(BaseModel):
    id: int
    account_id: int
    symbol: str
    side: str
    order_type: str
    status: str
    magic_id: int
    position_id: int
    reason: str
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    qty: str
    price: str | None = None
    filled_qty: str
    avg_fill_price: str | None = None
    created_at: str
    updated_at: str
    closed_at: str | None = None


class PositionDealModel(BaseModel):
    id: int
    account_id: int
    order_id: int | None = None
    position_id: int
    symbol: str
    side: str
    qty: str
    price: str
    fee: str | None = None
    fee_currency: str | None = None
    pnl: str | None = None
    magic_id: int
    reason: str
    reconciled: bool
    exchange_trade_id: str | None = None
    created_at: str
    executed_at: str


class PositionModel(BaseModel):
    id: int
    account_id: int
    symbol: str
    side: str
    qty: str
    avg_price: str
    state: str
    reason: str
    opened_at: str
    updated_at: str
    closed_at: str | None = None
