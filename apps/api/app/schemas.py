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
