from typing import Any

from .auth import AuthContext
from .repository_mysql import MySQLCommandRepository
from .schemas import CommandInput, CommandResult


class CommandValidationError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


async def _check_permission(
    repo: MySQLCommandRepository, conn: Any, user_id: int, account_id: int, command: str
) -> None:
    row = await repo.fetch_permissions(conn, user_id, account_id)
    if row is None:
        raise CommandValidationError("permission_denied", "user has no access to account")

    can_trade = bool(row[1])
    if command in {"send_order", "cancel_order", "close_by"} and not can_trade:
        raise CommandValidationError("permission_denied", "trade permission required")


async def _check_risk_open_permission(
    repo: MySQLCommandRepository, conn: Any, account_id: int, reduce_only: bool
) -> None:
    if reduce_only:
        return

    if not await repo.fetch_allow_new_positions(conn, account_id):
        raise CommandValidationError(
            "risk_blocked", "new positions are blocked for this account"
        )


async def _validate_position_binding(
    repo: MySQLCommandRepository, conn: Any, account_id: int, position_id: int, symbol: str
) -> None:
    if position_id == 0:
        return

    if not await repo.position_exists_open(conn, account_id, position_id, symbol):
        raise CommandValidationError(
            "invalid_position_id", "position_id does not exist or is incompatible"
        )


async def _insert_command(
    repo: MySQLCommandRepository,
    conn: Any,
    account_id: int,
    command_type: str,
    request_id: str | None,
    payload: dict[str, Any],
) -> int:
    return await repo.insert_position_command(conn, account_id, command_type, request_id, payload)


async def _enqueue_command(
    repo: MySQLCommandRepository, conn: Any, account_id: int, pool_id: int, command_id: int
) -> None:
    await repo.enqueue_command(conn, account_id, pool_id, command_id)


async def _insert_pending_order(
    repo: MySQLCommandRepository,
    conn: Any,
    account_id: int,
    payload: dict[str, Any],
    magic_id: int,
    position_id: int,
    reason: str,
) -> int:
    symbol = str(payload.get("symbol", "")).strip()
    side = str(payload.get("side", "")).lower()
    order_type = str(payload.get("order_type", payload.get("type", ""))).lower()
    qty = payload.get("qty", payload.get("amount"))
    price = payload.get("price")
    client_order_id = payload.get("client_order_id")

    if not symbol:
        raise CommandValidationError("validation_error", "payload.symbol is required")
    if side not in {"buy", "sell"}:
        raise CommandValidationError("validation_error", "payload.side must be buy or sell")
    if order_type not in {"market", "limit"}:
        raise CommandValidationError("validation_error", "payload.order_type must be market or limit")
    if qty is None:
        raise CommandValidationError("validation_error", "payload.qty is required")

    if order_type == "limit" and price is None:
        raise CommandValidationError("validation_error", "payload.price is required for limit orders")

    await _validate_position_binding(repo, conn, account_id, position_id, symbol)
    return await repo.insert_position_order_pending_submit(
        conn=conn,
        account_id=account_id,
        symbol=symbol,
        side=side,
        order_type=order_type,
        magic_id=magic_id,
        position_id=position_id,
        reason=reason,
        client_order_id=client_order_id,
        qty=qty,
        price=price,
    )


async def process_single_command(
    db: Any,
    repo: MySQLCommandRepository,
    auth: AuthContext,
    item: CommandInput,
    index: int,
) -> CommandResult:
    try:
        async with db.connection() as conn:
            try:
                account_id, pool_id = await repo.fetch_account(conn, item.account_id)
            except ValueError:
                raise CommandValidationError(
                    "account_not_found", "account not found or inactive"
                ) from None

            await _check_permission(repo, conn, auth.user_id, account_id, item.command)

            payload = dict(item.payload)
            reason = str(payload.get("reason", "api"))
            magic_id = int(payload.get("magic_id", 0) or 0)
            position_id = int(payload.get("position_id", 0) or 0)

            command_id = await _insert_command(
                repo, conn, account_id, item.command, item.request_id, payload
            )
            order_id: int | None = None

            if item.command == "send_order":
                reduce_only = bool(payload.get("reduce_only", False))
                await _check_risk_open_permission(
                    repo, conn, account_id, reduce_only=reduce_only
                )
                order_id = await _insert_pending_order(
                    repo=repo,
                    conn=conn,
                    account_id=account_id,
                    payload=payload,
                    magic_id=magic_id,
                    position_id=position_id,
                    reason=reason,
                )

            await _enqueue_command(repo, conn, account_id, pool_id, command_id)
            await conn.commit()

        return CommandResult(index=index, ok=True, command_id=command_id, order_id=order_id)

    except CommandValidationError as exc:
        return CommandResult(
            index=index,
            ok=False,
            error={"code": exc.code, "message": exc.message},
        )
    except Exception as exc:  # pragma: no cover
        return CommandResult(
            index=index,
            ok=False,
            error={"code": "internal_error", "message": str(exc)},
        )
