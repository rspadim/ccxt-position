from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_validator


CommandType = Literal[
    "send_order",
    "cancel_order",
    "cancel_all_orders",
    "change_order",
    "close_by",
    "merge_positions",
    "close_position",
    "position_change",
]
OrderSide = Literal["buy", "sell"]
OrderType = str
OrderStatus = Literal[
    "PENDING_SUBMIT",
    "SUBMITTED",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELED",
    "REJECTED",
]
PositionState = Literal["open", "close_requested", "closed"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class SendOrderPayload(StrictModel):
    symbol: str = Field(min_length=1)
    side: OrderSide
    order_type: str = Field(
        min_length=1,
        validation_alias=AliasChoices("order_type", "type"),
        serialization_alias="order_type",
    )
    qty: Decimal = Field(
        gt=Decimal("0"),
        validation_alias=AliasChoices("qty", "amount"),
        serialization_alias="qty",
    )
    price: Decimal | None = None
    stop_loss: Decimal | None = Field(default=None, validation_alias="oms_stop_loss", serialization_alias="oms_stop_loss")
    stop_gain: Decimal | None = Field(default=None, validation_alias="oms_stop_gain", serialization_alias="oms_stop_gain")
    strategy_id: int = Field(
        default=0,
        validation_alias=AliasChoices("strategy_id"),
        serialization_alias="strategy_id",
    )
    position_id: int = 0
    reason: str = "api"
    comment: str | None = None
    reduce_only: bool = False
    client_order_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    post_only: bool = False
    time_in_force: str | None = None
    trigger_price: Decimal | None = None
    stop_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    trailing_amount: Decimal | None = None
    trailing_percent: Decimal | None = None

    @model_validator(mode="after")
    def validate_limit_price(self) -> "SendOrderPayload":
        if str(self.order_type or "").strip().lower() == "limit" and self.price is None:
            raise ValueError("price is required for limit order")
        return self

class CancelOrderPayload(StrictModel):
    order_id: int | None = Field(default=None, gt=0)
    order_ids: list[int] = Field(default_factory=list)
    order_ids_csv: str | None = None

    @model_validator(mode="after")
    def normalize_order_ids(self) -> "CancelOrderPayload":
        ids: list[int] = [int(x) for x in self.order_ids if int(x) > 0]
        if self.order_id is not None:
            ids.append(int(self.order_id))
        csv = str(self.order_ids_csv or "").strip()
        if csv:
            for part in csv.split(","):
                part = part.strip()
                if part.isdigit():
                    n = int(part)
                    if n > 0:
                        ids.append(n)
        uniq = sorted(set(ids))
        if not uniq:
            raise ValueError("order_id or order_ids is required")
        self.order_ids = uniq
        return self


class CancelAllOrdersPayload(StrictModel):
    strategy_ids: list[int] = Field(default_factory=list)
    strategy_ids_csv: str | None = None

    @model_validator(mode="after")
    def normalize_strategy_ids(self) -> "CancelAllOrdersPayload":
        ids: list[int] = [int(x) for x in self.strategy_ids if int(x) >= 0]
        csv = str(self.strategy_ids_csv or "").strip()
        if csv:
            for part in csv.split(","):
                part = part.strip()
                if part.isdigit():
                    n = int(part)
                    if n >= 0:
                        ids.append(n)
        self.strategy_ids = sorted(set(ids))
        return self


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
    qty: Decimal | None = Field(default=None, gt=Decimal("0"))
    strategy_id: int = Field(
        default=0,
        validation_alias=AliasChoices("strategy_id"),
        serialization_alias="strategy_id",
    )


class MergePositionsPayload(StrictModel):
    source_position_id: int = Field(gt=0)
    target_position_id: int = Field(gt=0)
    stop_mode: Literal["keep", "clear", "set"] = "keep"
    stop_loss: Decimal | None = Field(default=None, validation_alias="oms_stop_loss", serialization_alias="oms_stop_loss")
    stop_gain: Decimal | None = Field(default=None, validation_alias="oms_stop_gain", serialization_alias="oms_stop_gain")

    @model_validator(mode="after")
    def validate_positions(self) -> "MergePositionsPayload":
        if int(self.source_position_id) == int(self.target_position_id):
            raise ValueError("source_position_id and target_position_id must differ")
        return self


class ClosePositionPayload(StrictModel):
    position_id: int = Field(gt=0)
    order_type: str = Field(default="market", min_length=1)
    price: Decimal | None = None
    qty: Decimal | None = Field(default=None, gt=Decimal("0"))
    strategy_id: int = Field(
        default=0,
        validation_alias=AliasChoices("strategy_id"),
        serialization_alias="strategy_id",
    )
    reason: str = "api"
    comment: str | None = None
    client_order_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    post_only: bool = False
    time_in_force: str | None = None
    trigger_price: Decimal | None = None
    stop_price: Decimal | None = None
    take_profit_price: Decimal | None = None
    trailing_amount: Decimal | None = None
    trailing_percent: Decimal | None = None

class BaseCommand(StrictModel):
    account_id: int | None = Field(default=None, gt=0)
    request_id: str | None = None


class SendOrderCommand(BaseCommand):
    command: Literal["send_order"]
    payload: SendOrderPayload


class CancelOrderCommand(BaseCommand):
    command: Literal["cancel_order"]
    payload: CancelOrderPayload


class CancelAllOrdersCommand(BaseCommand):
    command: Literal["cancel_all_orders"]
    payload: CancelAllOrdersPayload


class ChangeOrderCommand(BaseCommand):
    command: Literal["change_order"]
    payload: ChangeOrderPayload


class CloseByCommand(BaseCommand):
    command: Literal["close_by"]
    payload: CloseByPayload


class MergePositionsCommand(BaseCommand):
    command: Literal["merge_positions"]
    payload: MergePositionsPayload


class ClosePositionCommand(BaseCommand):
    command: Literal["close_position"]
    payload: ClosePositionPayload


class PositionChangePayload(StrictModel):
    position_id: int = Field(gt=0)
    stop_loss: Decimal | None = Field(default=None, validation_alias="oms_stop_loss", serialization_alias="oms_stop_loss")
    stop_gain: Decimal | None = Field(default=None, validation_alias="oms_stop_gain", serialization_alias="oms_stop_gain")
    comment: str | None = None

    @model_validator(mode="after")
    def validate_change_fields(self) -> "PositionChangePayload":
        changed = {"stop_loss", "stop_gain", "comment"} & set(self.model_fields_set)
        if not changed:
            raise ValueError("at least one of stop_loss, stop_gain or comment must be provided")
        return self


class PositionChangeCommand(BaseCommand):
    command: Literal["position_change"]
    payload: PositionChangePayload


CommandInput = Annotated[
    SendOrderCommand
    | CancelOrderCommand
    | CancelAllOrdersCommand
    | ChangeOrderCommand
    | CloseByCommand
    | MergePositionsCommand
    | ClosePositionCommand
    | PositionChangeCommand,
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
    order_type: str = Field(
        min_length=1,
        validation_alias=AliasChoices("order_type", "type"),
        serialization_alias="order_type",
    )
    amount: Decimal = Field(gt=Decimal("0"))
    price: Decimal | None = None
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_limit_price(self) -> "CcxtCoreCreateOrderInput":
        if str(self.order_type or "").strip().lower() == "limit" and self.price is None:
            raise ValueError("price is required for limit order")
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


class CcxtRawOrderModel(BaseModel):
    id: int
    account_id: int
    exchange_id: str
    exchange_order_id: str | None = None
    client_order_id: str | None = None
    symbol: str | None = None
    raw_json: dict[str, Any]
    observed_at: datetime


class CcxtRawTradeModel(BaseModel):
    id: int
    account_id: int
    exchange_id: str
    exchange_trade_id: str | None = None
    exchange_order_id: str | None = None
    symbol: str | None = None
    raw_json: dict[str, Any]
    observed_at: datetime


class CcxtRawOrdersResponse(BaseModel):
    items: list[CcxtRawOrderModel]
    total: int | None = None
    page: int | None = None
    page_size: int | None = None


class CcxtRawTradesResponse(BaseModel):
    items: list[CcxtRawTradeModel]
    total: int | None = None
    page: int | None = None
    page_size: int | None = None


class ReassignInput(BaseModel):
    account_id: int | None = Field(default=None, gt=0)
    account_ids: list[int] | str | None = None
    deal_ids: list[int] = Field(default_factory=list)
    order_ids: list[int] = Field(default_factory=list)
    start_date: str | None = None
    end_date: str | None = None
    reconciled: bool | None = None
    order_statuses: list[str] = Field(default_factory=list)
    kinds: list[Literal["order", "deal"]] = Field(default_factory=list)
    preview: bool = False
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=100, ge=1, le=500)
    target_strategy_id: int = Field(
        default=0,
        validation_alias=AliasChoices("target_strategy_id"),
        serialization_alias="target_strategy_id",
    )
    target_position_id: int | None = 0


class ReassignPreviewItem(BaseModel):
    kind: Literal["deal", "order"]
    id: int
    account_id: int
    symbol: str | None = None
    side: str | None = None
    status: str | None = None
    reconciled: bool | None = None
    strategy_id: int | None = None
    position_id: int | None = None
    previous_position_id: int | None = None
    edit_replace_state: str | None = None
    edit_replace_at: datetime | None = None
    edit_replace_orphan_order_id: int | None = None
    edit_replace_origin_order_id: int | None = None
    executed_at: datetime | None = None
    created_at: datetime | None = None


class PositionOrderModel(BaseModel):
    id: int
    command_id: int | None = None
    account_id: int
    symbol: str
    side: OrderSide
    order_type: str
    status: OrderStatus
    strategy_id: int = Field(validation_alias=AliasChoices("strategy_id"))
    position_id: int
    previous_position_id: int | None = None
    reason: str
    comment: str | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    qty: Decimal
    price: Decimal | None = None
    stop_loss: Decimal | None = None
    stop_gain: Decimal | None = None
    filled_qty: Decimal
    avg_fill_price: Decimal | None = None
    edit_replace_state: str | None = None
    edit_replace_at: datetime | None = None
    edit_replace_orphan_order_id: int | None = None
    edit_replace_origin_order_id: int | None = None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None


class PositionDealModel(BaseModel):
    id: int
    account_id: int
    order_id: int | None = None
    position_id: int
    previous_position_id: int | None = None
    symbol: str
    side: OrderSide
    qty: Decimal
    price: Decimal
    fee: Decimal | None = None
    fee_currency: str | None = None
    pnl: Decimal | None = None
    strategy_id: int = Field(validation_alias=AliasChoices("strategy_id"))
    reason: str
    comment: str | None = None
    reconciled: bool
    exchange_trade_id: str | None = None
    created_at: datetime
    executed_at: datetime


class PositionModel(BaseModel):
    id: int
    account_id: int
    symbol: str
    strategy_id: int = Field(validation_alias=AliasChoices("strategy_id"))
    side: OrderSide
    qty: Decimal
    avg_price: Decimal
    stop_loss: Decimal | None = None
    stop_gain: Decimal | None = None
    state: PositionState
    reason: str
    comment: str | None = None
    opened_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None


class PositionOrdersResponse(BaseModel):
    items: list[PositionOrderModel]
    total: int | None = None
    page: int | None = None
    page_size: int | None = None


class PositionDealsResponse(BaseModel):
    items: list[PositionDealModel]
    total: int | None = None
    page: int | None = None
    page_size: int | None = None


class PositionsResponse(BaseModel):
    items: list[PositionModel]
    total: int | None = None
    page: int | None = None
    page_size: int | None = None


class ReassignResponse(BaseModel):
    ok: bool
    deals_updated: int
    orders_updated: int
    deals_total: int = 0
    orders_total: int = 0
    preview: bool = False
    page: int = 1
    page_size: int = 100
    items: list[ReassignPreviewItem] = Field(default_factory=list)


class ReconcileNowInput(BaseModel):
    account_id: int | None = Field(default=None, gt=0)
    account_ids: list[int] | str | None = None
    symbols_hint: list[str] | None = None
    scope: Literal["short", "hourly", "long", "period"] = "short"
    start_date: str | None = None
    end_date: str | None = None


class ReconcileNowResponse(BaseModel):
    ok: bool
    account_ids: list[int]
    triggered_count: int


ReconcileHealth = Literal["fresh", "stale", "never"]


class ReconcileStatusItem(BaseModel):
    account_id: int
    status: ReconcileHealth
    cursor_value: str | None = None
    updated_at: datetime | None = None
    age_seconds: int | None = None


class ReconcileStatusResponse(BaseModel):
    items: list[ReconcileStatusItem]


class AccountSummaryItem(BaseModel):
    account_id: int
    label: str
    exchange_id: str
    position_mode: str
    is_testnet: bool
    status: str
    allow_new_positions: bool | None = None
    can_read: bool
    can_trade: bool
    can_risk_manage: bool


class AccountsResponse(BaseModel):
    items: list[AccountSummaryItem]


class RiskStrategyStateItem(BaseModel):
    account_id: int
    strategy_id: int
    name: str
    status: str
    allow_new_positions: bool


class RiskStrategyStateResponse(BaseModel):
    items: list[RiskStrategyStateItem]


class RiskSetAllowNewPositionsInput(BaseModel):
    allow_new_positions: bool
    comment: str = Field(min_length=1)


class RiskSetStrategyAllowNewPositionsInput(BaseModel):
    strategy_id: int
    allow_new_positions: bool
    comment: str = Field(min_length=1)


class RiskSetAccountStatusInput(BaseModel):
    status: Literal["active", "blocked"]
    comment: str = Field(min_length=1)


class RiskActionResponse(BaseModel):
    ok: bool
    account_id: int
    strategy_id: int | None = None
    status: str | None = None
    allow_new_positions: bool | None = None
    rows: int = 0


class AdminCreateAccountInput(BaseModel):
    exchange_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    position_mode: Literal["hedge", "netting", "strategy_netting"] = "hedge"
    is_testnet: bool = True
    extra_config_json: dict[str, Any] = Field(default_factory=dict)


class AdminCreateAccountResponse(BaseModel):
    ok: bool
    account_id: int


class AdminAccountItem(BaseModel):
    account_id: int
    label: str
    exchange_id: str
    position_mode: str
    extra_config_json: dict[str, Any] = Field(default_factory=dict)
    is_testnet: bool
    reconcile_enabled: bool
    reconcile_short_interval_seconds: int | None = None
    reconcile_short_lookback_seconds: int | None = None
    reconcile_hourly_interval_seconds: int | None = None
    reconcile_hourly_lookback_seconds: int | None = None
    reconcile_long_interval_seconds: int | None = None
    reconcile_long_lookback_seconds: int | None = None
    dispatcher_worker_hint: int | None = None
    dispatcher_hint_updated_at: str | None = None
    raw_storage_mode: str
    status: str
    created_at: str
    api_key_enc: str | None = None
    secret_enc: str | None = None
    passphrase_enc: str | None = None
    credentials_updated_at: str | None = None


class AdminAccountsResponse(BaseModel):
    items: list[AdminAccountItem]


class AdminUpdateAccountCredentialsInput(BaseModel):
    api_key: str | None = None
    secret: str | None = None
    passphrase: str | None = None


class AdminUpdateAccountInput(BaseModel):
    exchange_id: str | None = None
    label: str | None = None
    position_mode: Literal["hedge", "netting", "strategy_netting"] | None = None
    is_testnet: bool | None = None
    status: Literal["active", "blocked"] | None = None
    extra_config_json: dict[str, Any] | None = None
    credentials: AdminUpdateAccountCredentialsInput | None = None


class AdminUpdateAccountResponse(BaseModel):
    ok: bool
    account_id: int
    rows: int


class AdminUserItem(BaseModel):
    user_id: int
    user_name: str
    role: str
    status: str
    created_at: str


class AdminUsersResponse(BaseModel):
    items: list[AdminUserItem]


class AdminApiKeyAccountPermissionInput(BaseModel):
    account_id: int
    can_read: bool = True
    can_trade: bool = False
    can_close_position: bool = False
    can_risk_manage: bool = False
    can_block_new_positions: bool = False
    can_block_account: bool = False
    restrict_to_strategies: bool = False
    strategy_ids: list[int] = Field(default_factory=list)


class AdminCreateUserApiKeyInput(BaseModel):
    user_name: str = Field(min_length=1)
    role: Literal["admin", "trader", "portfolio_manager", "robot", "risk", "readonly"] = "trader"
    api_key: str | None = None
    password: str | None = None
    permissions: list[AdminApiKeyAccountPermissionInput] = Field(default_factory=list)
    label: str | None = None


class AdminCreateUserApiKeyResponse(BaseModel):
    ok: bool
    user_id: int
    api_key_id: int
    api_key_plain: str
    label: str | None = None


class AdminCreateApiKeyInput(BaseModel):
    user_id: int = Field(ge=1)
    api_key: str | None = None
    label: str | None = None


class AdminCreateApiKeyResponse(BaseModel):
    ok: bool
    user_id: int
    api_key_id: int
    api_key_plain: str
    label: str | None = None


class AdminUserApiKeyItem(BaseModel):
    user_id: int
    user_name: str
    role: str
    user_status: str
    api_key_id: int
    api_key_status: str
    label: str | None = None
    created_at: str


class AdminUsersApiKeysResponse(BaseModel):
    items: list[AdminUserApiKeyItem]


class AdminUpdateApiKeyInput(BaseModel):
    status: Literal["active", "disabled"]


class AdminUpdateApiKeyResponse(BaseModel):
    ok: bool
    api_key_id: int
    rows: int


class AdminApiKeyPermissionItem(BaseModel):
    api_key_id: int
    account_id: int
    can_read: bool
    can_trade: bool
    can_close_position: bool
    can_risk_manage: bool
    can_block_new_positions: bool
    can_block_account: bool
    restrict_to_strategies: bool
    strategy_ids: list[int] = Field(default_factory=list)
    status: str


class AdminApiKeyPermissionsResponse(BaseModel):
    items: list[AdminApiKeyPermissionItem]


class AdminUpsertApiKeyPermissionInput(BaseModel):
    account_id: int
    can_read: bool = True
    can_trade: bool = False
    can_close_position: bool = False
    can_risk_manage: bool = False
    can_block_new_positions: bool = False
    can_block_account: bool = False
    restrict_to_strategies: bool = False
    strategy_ids: list[int] = Field(default_factory=list)


class AdminCreateStrategyInput(BaseModel):
    name: str = Field(min_length=1)
    account_ids: list[int] = Field(default_factory=list)
    client_strategy_id: int | None = Field(default=None, ge=1)


class AdminCreateStrategyResponse(BaseModel):
    ok: bool
    strategy_id: int


class AdminStrategyItem(BaseModel):
    strategy_id: int
    client_strategy_id: int | None = None
    name: str
    status: Literal["active", "disabled"]
    account_ids: list[int] = Field(default_factory=list)


class AdminStrategiesResponse(BaseModel):
    items: list[AdminStrategyItem]


class AdminUpdateStrategyInput(BaseModel):
    name: str | None = None
    status: Literal["active", "disabled"] | None = None
    client_strategy_id: int | None = Field(default=None, ge=1)
    account_ids: list[int] | None = None


class AdminUpdateStrategyResponse(BaseModel):
    ok: bool
    strategy_id: int
    rows: int


AdminOmsView = Literal[
    "open_orders",
    "history_orders",
    "open_positions",
    "history_positions",
    "deals",
]


class AdminOmsQueryResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    page: int
    page_size: int


class AdminOmsOrderRow(BaseModel):
    id: int | None = None
    command_id: int | None = None
    account_id: int | None = None
    symbol: str | None = None
    side: str | None = None
    order_type: str | None = None
    status: str | None = None
    strategy_id: int | None = None
    position_id: int | None = None
    previous_position_id: int | None = None
    reason: str | None = None
    comment: str | None = None
    client_order_id: str | None = None
    exchange_order_id: str | None = None
    qty: Decimal | None = None
    price: Decimal | None = None
    stop_loss: Decimal | None = None
    stop_gain: Decimal | None = None
    filled_qty: Decimal | None = None
    avg_fill_price: Decimal | None = None
    edit_replace_state: str | None = None
    edit_replace_at: str | None = None
    edit_replace_orphan_order_id: int | None = None
    edit_replace_origin_order_id: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    closed_at: str | None = None


class AdminOmsPositionRow(BaseModel):
    id: int | None = None
    account_id: int | None = None
    symbol: str | None = None
    strategy_id: int | None = None
    side: str | None = None
    qty: Decimal | None = None
    avg_price: Decimal | None = None
    stop_loss: Decimal | None = None
    stop_gain: Decimal | None = None
    state: str | None = None
    reason: str | None = None
    comment: str | None = None
    opened_at: str | None = None
    updated_at: str | None = None
    closed_at: str | None = None


class AdminOmsDealRow(BaseModel):
    id: int | None = None
    account_id: int | None = None
    order_id: int | None = None
    position_id: int | None = None
    previous_position_id: int | None = None
    symbol: str | None = None
    side: str | None = None
    qty: Decimal | None = None
    price: Decimal | None = None
    fee: Decimal | None = None
    fee_currency: str | None = None
    pnl: Decimal | None = None
    strategy_id: int | None = None
    reason: str | None = None
    comment: str | None = None
    reconciled: bool | None = None
    exchange_trade_id: str | None = None
    created_at: str | None = None
    executed_at: str | None = None


class AdminOmsOrderMutation(BaseModel):
    op: Literal["insert", "update", "delete"]
    row: AdminOmsOrderRow


class AdminOmsPositionMutation(BaseModel):
    op: Literal["insert", "update", "delete"]
    row: AdminOmsPositionRow


class AdminOmsDealMutation(BaseModel):
    op: Literal["insert", "update", "delete"]
    row: AdminOmsDealRow


class AdminOmsMutateResult(BaseModel):
    index: int
    ok: bool
    op: str
    id: int | None = None
    error: str | None = None


class AdminOmsMutateResponse(BaseModel):
    ok: bool
    entity: Literal["orders", "positions", "deals"]
    results: list[AdminOmsMutateResult]


class StrategyItem(BaseModel):
    strategy_id: int
    client_strategy_id: int | None = None
    name: str
    status: Literal["active", "disabled"]
    account_ids: list[int] = Field(default_factory=list)


class StrategiesResponse(BaseModel):
    items: list[StrategyItem]


class CreateStrategyInput(BaseModel):
    name: str = Field(min_length=1)
    account_ids: list[int] = Field(default_factory=list)
    client_strategy_id: int | None = Field(default=None, ge=1)


class CreateStrategyResponse(BaseModel):
    ok: bool
    strategy_id: int


class AuthLoginPasswordInput(BaseModel):
    user_name: str = Field(min_length=1)
    password: str = Field(min_length=1)
    api_key_id: int | None = None


class AuthLoginPasswordResponse(BaseModel):
    ok: bool
    token: str
    token_type: str
    expires_at: str
    user_id: int
    role: str
    api_key_id: int


class UserProfileResponse(BaseModel):
    user_id: int
    user_name: str
    role: str
    status: str
    api_key_id: int


class UserUpdateProfileInput(BaseModel):
    user_name: str = Field(min_length=1)


class UserUpdateProfileResponse(BaseModel):
    ok: bool
    user_id: int
    user_name: str


class UserUpdatePasswordInput(BaseModel):
    current_password: str = Field(min_length=1)
    new_password: str = Field(min_length=1)


class UserUpdatePasswordResponse(BaseModel):
    ok: bool
    user_id: int


class UserApiKeyItem(BaseModel):
    api_key_id: int
    user_id: int
    user_name: str
    role: str
    status: str
    created_at: str
    label: str | None = None


class UserApiKeysResponse(BaseModel):
    items: list[UserApiKeyItem]


class UserCreateApiKeyInput(BaseModel):
    user_id: int | None = Field(default=None, ge=1)
    api_key: str | None = None
    label: str | None = None


class UserCreateApiKeyResponse(BaseModel):
    ok: bool
    user_id: int
    api_key_id: int
    api_key_plain: str
    label: str | None = None


class UserUpdateApiKeyInput(BaseModel):
    status: Literal["active", "disabled"]
    user_id: int | None = Field(default=None, ge=1)


class UserUpdateApiKeyResponse(BaseModel):
    ok: bool
    api_key_id: int
    rows: int


class CcxtExchangesResponse(BaseModel):
    items: list[str]

