import asyncio
import contextlib
from typing import Any

from .app.ccxt_adapter import CCXTAdapter
from .app.config import load_settings
from .app.db_mysql import DatabaseMySQL
from .app.repository_mysql import MySQLCommandRepository


class PermanentCommandError(Exception):
    pass


def _release_close_position_requested(payload: dict[str, Any]) -> int | None:
    if str(payload.get("origin_command", "")) == "close_position":
        position_id = int(payload.get("position_id", 0) or 0)
        return position_id if position_id > 0 else None
    return None


async def _process_claimed_queue_item(
    db: DatabaseMySQL,
    repo: MySQLCommandRepository,
    ccxt_adapter: CCXTAdapter,
    queue_id: int,
    command_id: int,
    account_id: int,
) -> None:
    async with db.connection() as conn:
        try:
            cmd_account_id, command_type, payload = await repo.fetch_command_for_worker(
                conn, command_id
            )
            if cmd_account_id != account_id:
                raise RuntimeError("queue/account mismatch")

            exchange_id, api_key, secret, passphrase = await repo.fetch_account_exchange_credentials(
                conn, account_id
            )
            position_lock_id = _release_close_position_requested(payload)

            if command_type == "send_order":
                order = await repo.fetch_order_for_command_send(conn, command_id)
                if order is None:
                    raise PermanentCommandError("missing local order for send_order")

                params = {}
                if payload.get("reduce_only") is True:
                    params["reduceOnly"] = True
                client_order_id = order.get("client_order_id") or str(order["id"])
                params["clientOrderId"] = client_order_id

                created = await ccxt_adapter.create_order(
                    exchange_id=exchange_id,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    symbol=order["symbol"],
                    side=order["side"],
                    order_type=order["order_type"],
                    amount=order["qty"],
                    price=order["price"],
                    params=params,
                )
                exchange_order_id = str(created.get("id")) if created.get("id") is not None else None
                await repo.mark_order_submitted_exchange(conn, order["id"], exchange_order_id)
                await repo.insert_ccxt_order_raw(
                    conn=conn,
                    account_id=account_id,
                    exchange_id=exchange_id,
                    exchange_order_id=exchange_order_id,
                    client_order_id=str(created.get("clientOrderId")) if created.get("clientOrderId") else client_order_id,
                    symbol=str(created.get("symbol")) if created.get("symbol") else order["symbol"],
                    raw_json=created,
                )
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="order_submitted",
                    payload={
                        "command_id": command_id,
                        "order_id": order["id"],
                        "exchange_order_id": exchange_order_id,
                    },
                )

            elif command_type == "cancel_order":
                order_id = int(payload.get("order_id", 0) or 0)
                if order_id <= 0:
                    raise PermanentCommandError("payload.order_id is required for cancel_order")
                order = await repo.fetch_order_by_id(conn, account_id, order_id)
                if order is None:
                    raise PermanentCommandError("order not found")
                if not order.get("exchange_order_id"):
                    raise PermanentCommandError("order has no exchange_order_id to cancel")
                canceled = await ccxt_adapter.cancel_order(
                    exchange_id=exchange_id,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    exchange_order_id=str(order["exchange_order_id"]),
                    symbol=str(order["symbol"]),
                    params={},
                )
                await repo.mark_order_canceled(conn, order_id)
                await repo.insert_ccxt_order_raw(
                    conn=conn,
                    account_id=account_id,
                    exchange_id=exchange_id,
                    exchange_order_id=str(canceled.get("id")) if canceled.get("id") else str(order["exchange_order_id"]),
                    client_order_id=str(canceled.get("clientOrderId")) if canceled.get("clientOrderId") else str(order.get("client_order_id") or ""),
                    symbol=str(canceled.get("symbol")) if canceled.get("symbol") else str(order["symbol"]),
                    raw_json=canceled,
                )
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="order_canceled",
                    payload={"command_id": command_id, "order_id": order_id},
                )

            elif command_type == "change_order":
                order_id = int(payload.get("order_id", 0) or 0)
                if order_id <= 0:
                    raise PermanentCommandError("payload.order_id is required for change_order")
                order = await repo.fetch_order_by_id(conn, account_id, order_id)
                if order is None:
                    raise PermanentCommandError("order not found")
                if not order.get("exchange_order_id"):
                    raise PermanentCommandError("order has no exchange_order_id to change")
                new_price = payload.get("new_price", order["price"])
                new_qty = payload.get("new_qty", order["qty"])
                edited = await ccxt_adapter.edit_or_replace_order(
                    exchange_id=exchange_id,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    exchange_order_id=str(order["exchange_order_id"]),
                    symbol=str(order["symbol"]),
                    side=str(order["side"]),
                    order_type=str(order["order_type"]),
                    amount=new_qty,
                    price=new_price,
                    params={},
                )
                new_exchange_order_id = str(edited.get("id")) if edited.get("id") else str(order["exchange_order_id"])
                await repo.mark_order_submitted_exchange(conn, order_id, new_exchange_order_id)
                await repo.insert_ccxt_order_raw(
                    conn=conn,
                    account_id=account_id,
                    exchange_id=exchange_id,
                    exchange_order_id=new_exchange_order_id,
                    client_order_id=str(edited.get("clientOrderId")) if edited.get("clientOrderId") else str(order.get("client_order_id") or ""),
                    symbol=str(edited.get("symbol")) if edited.get("symbol") else str(order["symbol"]),
                    raw_json=edited,
                )
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="order_changed",
                    payload={"command_id": command_id, "order_id": order_id},
                )
            elif command_type == "close_by":
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="close_by_accepted",
                    payload={"command_id": command_id},
                )
            else:
                raise PermanentCommandError(f"unsupported command_type: {command_type}")

            await repo.mark_command_completed(conn, command_id)
            await repo.mark_queue_done(conn, queue_id)
            if position_lock_id is not None:
                await repo.release_close_position_lock(conn, position_lock_id)
            await conn.commit()
        except PermanentCommandError:
            await repo.mark_command_failed(conn, command_id)
            order_id = await repo.fetch_order_id_by_command_id(conn, command_id)
            if order_id is not None:
                await repo.mark_order_rejected(conn, order_id)
            position_lock_id = _release_close_position_requested(payload if "payload" in locals() else {})
            if position_lock_id is not None:
                await repo.release_close_position_lock(conn, position_lock_id)
            await repo.mark_queue_dead(conn, queue_id)
            await conn.commit()
            return
        except Exception:
            await repo.mark_command_failed(conn, command_id)
            position_lock_id = _release_close_position_requested(payload if "payload" in locals() else {})
            if position_lock_id is not None:
                await repo.release_close_position_lock(conn, position_lock_id)
            await repo.mark_queue_failed(conn, queue_id, delay_seconds=15)
            await conn.commit()
            raise


async def run_worker() -> None:
    settings = load_settings()
    if settings.db_engine != "mysql":
        raise RuntimeError("worker-position v0 supports only mysql db_engine")

    db = DatabaseMySQL(settings)
    repo = MySQLCommandRepository()
    ccxt_adapter = CCXTAdapter()
    await db.connect()

    try:
        while True:
            claimed: tuple[int, int, int, int] | None = None
            async with db.connection() as conn:
                claimed = await repo.claim_next_queue_item(
                    conn=conn,
                    pool_id=settings.worker_pool_id,
                    worker_id=settings.worker_id,
                )
                await conn.commit()

            if claimed is None:
                await asyncio.sleep(max(settings.worker_poll_interval_ms, 50) / 1000.0)
                continue

            queue_id, command_id, account_id, attempts = claimed
            if attempts > settings.worker_max_attempts:
                async with db.connection() as conn:
                    await repo.mark_command_failed(conn, command_id)
                    await repo.mark_queue_dead(conn, queue_id)
                    await conn.commit()
                continue
            try:
                await _process_claimed_queue_item(
                    db, repo, ccxt_adapter, queue_id, command_id, account_id
                )
            except Exception:
                # failure already persisted; continue polling.
                pass
    finally:
        with contextlib.suppress(Exception):
            await db.disconnect()


if __name__ == "__main__":
    asyncio.run(run_worker())
