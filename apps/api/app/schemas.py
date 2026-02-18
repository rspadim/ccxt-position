from typing import Any, Literal

from pydantic import BaseModel, Field


CommandType = Literal["send_order", "cancel_order", "close_by"]


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

