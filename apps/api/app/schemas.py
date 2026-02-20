from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


CommandType = Literal["send_order", "cancel_order", "change_order", "close_by", "close_position"]
OrderSide = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]
OrderStatus = Literal[
    "PENDING_SUBMIT",
    "SUBMITTED",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELED",
    "REJECTED",
]
PositionState = Literal["open", "closed"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class SendOrderPayload(StrictModel):
    symbol: str = Field(min_length=1)
    side: OrderSide
    order_type: OrderType = Field(
        validation_alias=AliasChoices("order_type", "type"),
        serialization_alias="order_type",
    )
    qty: Decimal = Field(
        gt=Decimal("0"),
        validation_alias=AliasChoices("qty", "amount"),
        serialization_alias="qty",
    )
    price: Decimal | None = None
    magic_id: int = 0
    position_id: int = 0
    reason: str = "api"
    reduce_only: bool = False
    client_order_id: str | None = None

    @model_validator(mode="after")
    def validate_limit_price(self) -> "SendOrderPayload":
        if self.order_type == "limit" and self.price is None:
            raise ValueError("price is required for limit orders")
        return self


class CancelOrderPayload(StrictModel):
    order_id: int = Field(gt=0)


class ChangeOrderPayload(StrictModel):
    order_id: int = Field(gt=0)
    new_price: Decimal | None = None
    new_qty: Decimal | None = Field(default=None, gt=Decimal("0"))

    @model_validator(mode="after")
    def validate_change_fields(self) -> "ChangeOrderPayload":
        if self.new_price is None and self.new_qty is None:
            raise ValueError("new_price or new_qty is required")
        return self


class CloseByPayload(StrictModel):
    position_id_a: int = Field(gt=0)
    position_id_b: int = Field(gt=0)
    magic_id: int = 0


class ClosePositionPayload(StrictModel):
    position_id: int = Field(gt=0)
    order_type: OrderType = "market"
    price: Decimal | None = None
    qty: Decimal | None = Field(default=None, gt=Decimal("0"))
    magic_id: int = 0
    reason: str = "api"
    client_order_id: str | None = None

    @model_validator(mode="after")
    def validate_limit_price(self) -> "ClosePositionPayload":
        if self.order_type == "limit" and self.price is None:
            raise ValueError("price is required for limit close")
        return self


class BaseCommand(StrictModel):
    account_id: int = Field(gt=0)
    request_id: str | None = None


class SendOrderCommand(BaseCommand):
    command: Literal["send_order"]
    payload: SendOrderPayload


class CancelOrderCommand(BaseCommand):
    command: Literal["cancel_order"]
    payload: CancelOrderPayload


class ChangeOrderCommand(BaseCommand):
    command: Literal["change_order"]
    payload: ChangeOrderPayload


class CloseByCommand(BaseCommand):
    command: Literal["close_by"]
    payload: CloseByPayload


class ClosePositionCommand(BaseCommand):
    command: Literal["close_position"]
    payload: ClosePositionPayload


CommandInput = Annotated[
    SendOrderCommand
    | CancelOrderCommand
    | ChangeOrderCommand
    | CloseByCommand
    | ClosePositionCommand,
    Field(discriminator="command"),
]


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


class CcxtCoreCreateOrderInput(BaseModel):
    symbol: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"] = Field(
        validation_alias=AliasChoices("order_type", "type"),
        serialization_alias="order_type",
    )
    amount: Decimal = Field(gt=Decimal("0"))
    price: Decimal | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_limit_price(self) -> "CcxtCoreCreateOrderInput":
        if self.order_type == "limit" and self.price is None:
            raise ValueError("price is required for limit orders")
        return self


class CcxtCoreCancelOrderInput(BaseModel):
    id: str = Field(min_length=1)
    symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class CcxtCoreFetchOrderInput(BaseModel):
    id: str = Field(min_length=1)
    symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class CcxtCoreFetchOpenOrdersInput(BaseModel):
    symbol: str | None = None
    since: int | None = Field(default=None, ge=0)
    limit: int | None = Field(default=200, ge=1, le=1000)
    params: dict[str, Any] = Field(default_factory=dict)


class CcxtCoreFetchBalanceInput(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)


class CcxtResponse(BaseModel):
    ok: bool = True
    result: Any


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
    side: OrderSide
    order_type: OrderType
    status: OrderStatus
    magic_id: int
    position_id: int
    reason: str
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    qty: Decimal
    price: Decimal | None = None
    filled_qty: Decimal
    avg_fill_price: Decimal | None = None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None


class PositionDealModel(BaseModel):
    id: int
    account_id: int
    order_id: int | None = None
    position_id: int
    symbol: str
    side: OrderSide
    qty: Decimal
    price: Decimal
    fee: Decimal | None = None
    fee_currency: str | None = None
    pnl: Decimal | None = None
    magic_id: int
    reason: str
    reconciled: bool
    exchange_trade_id: str | None = None
    created_at: datetime
    executed_at: datetime


class PositionModel(BaseModel):
    id: int
    account_id: int
    symbol: str
    side: OrderSide
    qty: Decimal
    avg_price: Decimal
    state: PositionState
    reason: str
    opened_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None


class PositionOrdersResponse(BaseModel):
    items: list[PositionOrderModel]


class PositionDealsResponse(BaseModel):
    items: list[PositionDealModel]


class PositionsResponse(BaseModel):
    items: list[PositionModel]


class ReassignResponse(BaseModel):
    ok: bool
    deals_updated: int
    orders_updated: int
