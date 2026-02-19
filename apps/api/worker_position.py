import asyncio
import contextlib
from decimal import Decimal
from typing import Any

from .app.ccxt_adapter import CCXTAdapter
from .app.config import load_settings
from .app.credentials_codec import CredentialsCodec
from .app.db_mysql import DatabaseMySQL
from .app.logging_utils import setup_application_logging
from .app.repository_mysql import MySQLCommandRepository


class PermanentCommandError(Exception):
    pass


def _release_close_position_requested(payload: dict[str, Any]) -> int | None:
    if str(payload.get("origin_command", "")) == "close_position":
        position_id = int(payload.get("position_id", 0) or 0)
        return position_id if position_id > 0 else None
    return None


def _dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _safe_trade(trade: dict[str, Any]) -> dict[str, Any] | None:
    symbol = trade.get("symbol")
    side = str(trade.get("side", "")).lower()
    amount = trade.get("amount")
    price = trade.get("price")
    if not symbol or side not in {"buy", "sell"}:
        return None
    if amount is None or price is None:
        return None
    return {
        "id": str(trade.get("id")) if trade.get("id") is not None else None,
        "order": str(trade.get("order")) if trade.get("order") is not None else None,
        "symbol": str(symbol),
        "side": side,
        "amount": _dec(amount),
        "price": _dec(price),
        "fee_cost": _dec((trade.get("fee") or {}).get("cost")),
        "fee_currency": (trade.get("fee") or {}).get("currency"),
        "timestamp": trade.get("timestamp"),
        "raw": trade,
    }


async def _project_trade_to_position(
    repo: MySQLCommandRepository,
    conn: Any,
    account_id: int,
    exchange_trade: dict[str, Any],
    reason: str,
    reconciled: bool,
) -> None:
    if await repo.deal_exists_by_exchange_trade_id(conn, account_id, exchange_trade.get("id")):
        return

    linked_order = await repo.fetch_open_order_by_exchange_order_id(
        conn, account_id, exchange_trade.get("order")
    )
    magic_id = int(linked_order["magic_id"]) if linked_order else 0
    position_id = int(linked_order["position_id"]) if linked_order else 0
    order_id = int(linked_order["id"]) if linked_order else None

    qty = exchange_trade["amount"]
    price = exchange_trade["price"]
    symbol = exchange_trade["symbol"]
    side = exchange_trade["side"]
    mode = await repo.fetch_account_position_mode(conn, account_id)

    if mode == "hedge":
        if position_id > 0:
            explicit = await repo.fetch_open_position(conn, account_id, position_id)
            if explicit is not None and explicit[1] == symbol and explicit[2] == side:
                old_qty = _dec(explicit[3])
                old_avg = _dec(explicit[4])
                new_qty = old_qty + qty
                if new_qty <= 0:
                    await repo.close_position(conn, position_id)
                else:
                    new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
                    await repo.update_position_open_qty_price(conn, position_id, new_qty, new_avg)
            else:
                existing = await repo.fetch_open_position_for_symbol(conn, account_id, symbol, side)
                if existing is None:
                    position_id = await repo.create_position_open(
                        conn=conn,
                        account_id=account_id,
                        symbol=symbol,
                        side=side,
                        qty=qty,
                        avg_price=price,
                        reason=reason,
                    )
                else:
                    position_id = int(existing["id"])
                    old_qty = _dec(existing["qty"])
                    old_avg = _dec(existing["avg_price"])
                    new_qty = old_qty + qty
                    if new_qty <= 0:
                        await repo.close_position(conn, position_id)
                    else:
                        new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
                        await repo.update_position_open_qty_price(conn, position_id, new_qty, new_avg)
        else:
            existing = await repo.fetch_open_position_for_symbol(conn, account_id, symbol, side)
            if existing is None:
                position_id = await repo.create_position_open(
                    conn=conn,
                    account_id=account_id,
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    avg_price=price,
                    reason=reason,
                )
            else:
                position_id = int(existing["id"])
                old_qty = _dec(existing["qty"])
                old_avg = _dec(existing["avg_price"])
                new_qty = old_qty + qty
                if new_qty <= 0:
                    await repo.close_position(conn, position_id)
                else:
                    new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
                    await repo.update_position_open_qty_price(conn, position_id, new_qty, new_avg)
    else:
        # netting: single live position per symbol. Opposite trades reduce/close/reverse.
        existing = await repo.fetch_open_net_position_by_symbol(conn, account_id, symbol)
        if existing is None:
            position_id = await repo.create_position_open(
                conn=conn,
                account_id=account_id,
                symbol=symbol,
                side=side,
                qty=qty,
                avg_price=price,
                reason=reason,
            )
        else:
            existing_id = int(existing["id"])
            existing_side = str(existing["side"]).lower()
            old_qty = _dec(existing["qty"])
            old_avg = _dec(existing["avg_price"])
            if existing_side == side:
                new_qty = old_qty + qty
                new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
                await repo.update_position_open_qty_price(conn, existing_id, new_qty, new_avg)
                position_id = existing_id
            else:
                if old_qty > qty:
                    remain = old_qty - qty
                    await repo.update_position_open_qty_price(conn, existing_id, remain, old_avg)
                    position_id = existing_id
                elif old_qty == qty:
                    await repo.close_position(conn, existing_id)
                    position_id = existing_id
                else:
                    reverse_qty = qty - old_qty
                    await repo.close_position(conn, existing_id)
                    position_id = await repo.create_position_open(
                        conn=conn,
                        account_id=account_id,
                        symbol=symbol,
                        side=side,
                        qty=reverse_qty,
                        avg_price=price,
                        reason=reason,
                    )

    await repo.insert_position_deal(
        conn=conn,
        account_id=account_id,
        order_id=order_id,
        position_id=position_id,
        symbol=symbol,
        side=side,
        qty=qty,
        price=price,
        fee=exchange_trade["fee_cost"],
        fee_currency=exchange_trade["fee_currency"],
        pnl=Decimal("0"),
        magic_id=magic_id,
        reason=reason,
        reconciled=reconciled,
        exchange_trade_id=exchange_trade["id"],
    )

    await repo.insert_event(
        conn=conn,
        account_id=account_id,
        namespace="position",
        event_type="deal_created",
        payload={
            "exchange_trade_id": exchange_trade["id"],
            "position_id": position_id,
            "symbol": symbol,
            "side": side,
        },
    )


async def _process_claimed_queue_item(
    db: DatabaseMySQL,
    repo: MySQLCommandRepository,
    ccxt_adapter: CCXTAdapter,
    queue_id: int,
    command_id: int,
    account_id: int,
    credentials_codec: CredentialsCodec,
) -> None:
    async with db.connection() as conn:
        try:
            cmd_account_id, command_type, payload = await repo.fetch_command_for_worker(
                conn, command_id
            )
            if cmd_account_id != account_id:
                raise RuntimeError("queue/account mismatch")

            exchange_id, is_testnet, api_key_enc, secret_enc, passphrase_enc = await repo.fetch_account_exchange_credentials(
                conn, account_id
            )
            api_key = credentials_codec.decrypt_maybe(api_key_enc)
            secret = credentials_codec.decrypt_maybe(secret_enc)
            passphrase = credentials_codec.decrypt_maybe(passphrase_enc)
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
                    use_testnet=is_testnet,
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
                    use_testnet=is_testnet,
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
                    use_testnet=is_testnet,
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
                pos_a = int(payload.get("position_id_a", payload.get("position_id", 0)) or 0)
                pos_b = int(payload.get("position_id_b", 0) or 0)
                if pos_a <= 0 or pos_b <= 0:
                    raise PermanentCommandError("close_by requires position_id_a/position_id_b")
                row_a = await repo.fetch_open_position(conn, account_id, pos_a)
                row_b = await repo.fetch_open_position(conn, account_id, pos_b)
                if row_a is None or row_b is None:
                    raise PermanentCommandError("close_by positions must exist and be open")

                pid_a, symbol_a, side_a, qty_a, avg_a = row_a
                pid_b, symbol_b, side_b, qty_b, avg_b = row_b
                if symbol_a != symbol_b:
                    raise PermanentCommandError("close_by positions must have same symbol")
                if side_a == side_b:
                    raise PermanentCommandError("close_by positions must be opposite sides")

                q_a = _dec(qty_a)
                q_b = _dec(qty_b)
                close_qty = min(q_a, q_b)
                if close_qty <= 0:
                    raise PermanentCommandError("close_by quantity is zero")

                magic_id = int(payload.get("magic_id", 0) or 0)
                reason = "close_by_internal"

                await repo.insert_position_deal(
                    conn=conn,
                    account_id=account_id,
                    order_id=None,
                    position_id=pid_a,
                    symbol=symbol_a,
                    side=side_a,
                    qty=close_qty,
                    price=_dec(avg_a),
                    fee=Decimal("0"),
                    fee_currency=None,
                    pnl=Decimal("0"),
                    magic_id=magic_id,
                    reason=reason,
                    reconciled=True,
                    exchange_trade_id=None,
                )
                await repo.insert_position_deal(
                    conn=conn,
                    account_id=account_id,
                    order_id=None,
                    position_id=pid_b,
                    symbol=symbol_b,
                    side=side_b,
                    qty=close_qty,
                    price=_dec(avg_b),
                    fee=Decimal("0"),
                    fee_currency=None,
                    pnl=Decimal("0"),
                    magic_id=magic_id,
                    reason=reason,
                    reconciled=True,
                    exchange_trade_id=None,
                )

                left_a = q_a - close_qty
                left_b = q_b - close_qty
                if left_a <= 0:
                    await repo.close_position(conn, pid_a)
                else:
                    await repo.update_position_open_qty_price(conn, pid_a, left_a, _dec(avg_a))
                if left_b <= 0:
                    await repo.close_position(conn, pid_b)
                else:
                    await repo.update_position_open_qty_price(conn, pid_b, left_b, _dec(avg_b))

                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="close_by_executed",
                    payload={
                        "command_id": command_id,
                        "position_id_a": pid_a,
                        "position_id_b": pid_b,
                        "qty": str(close_qty),
                    },
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


async def _run_reconciliation_once(
    db: DatabaseMySQL,
    repo: MySQLCommandRepository,
    ccxt_adapter: CCXTAdapter,
    pool_id: int,
    credentials_codec: CredentialsCodec,
) -> None:
    async with db.connection() as conn:
        accounts = await repo.list_active_accounts_by_pool(conn, pool_id)
        await conn.commit()

    for account in accounts:
        account_id = int(account["id"])
        async with db.connection() as conn:
            try:
                exchange_id, is_testnet, api_key_enc, secret_enc, passphrase_enc = await repo.fetch_account_exchange_credentials(
                    conn, account_id
                )
                api_key = credentials_codec.decrypt_maybe(api_key_enc)
                secret = credentials_codec.decrypt_maybe(secret_enc)
                passphrase = credentials_codec.decrypt_maybe(passphrase_enc)
                cursor_raw = await repo.fetch_reconciliation_cursor(
                    conn, account_id, "my_trades_since"
                )
                since = int(cursor_raw) if cursor_raw and cursor_raw.isdigit() else None
                trades = await ccxt_adapter.fetch_my_trades(
                    exchange_id=exchange_id,
                    use_testnet=is_testnet,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    symbol=None,
                    since=since,
                    limit=200,
                    params={},
                )

                max_ts = since or 0
                for trade in trades:
                    norm = _safe_trade(trade)
                    if norm is None:
                        continue
                    await repo.insert_ccxt_trade_raw(
                        conn=conn,
                        account_id=account_id,
                        exchange_id=exchange_id,
                        exchange_trade_id=norm["id"],
                        exchange_order_id=norm["order"],
                        symbol=norm["symbol"],
                        raw_json=norm["raw"],
                    )
                    await _project_trade_to_position(
                        repo=repo,
                        conn=conn,
                        account_id=account_id,
                        exchange_trade=norm,
                        reason="external",
                        reconciled=False,
                    )
                    if isinstance(norm["timestamp"], int) and norm["timestamp"] > max_ts:
                        max_ts = norm["timestamp"]

                if max_ts > 0:
                    await repo.update_reconciliation_cursor(
                        conn=conn,
                        account_id=account_id,
                        entity="my_trades_since",
                        cursor_value=str(max_ts + 1),
                    )
                    await repo.insert_event(
                        conn=conn,
                        account_id=account_id,
                        namespace="position",
                        event_type="reconciliation_tick",
                        payload={"trades_count": len(trades), "cursor": max_ts + 1},
                    )
                await conn.commit()
            except Exception:
                await conn.rollback()
                continue


async def run_worker() -> None:
    settings = load_settings()
    if settings.db_engine != "mysql":
        raise RuntimeError("worker-position v0 supports only mysql db_engine")

    db = DatabaseMySQL(settings)
    repo = MySQLCommandRepository()
    credentials_codec = CredentialsCodec(
        settings.encryption_master_key,
        require_encrypted=settings.require_encrypted_credentials,
    )
    loggers = setup_application_logging(
        settings.disable_uvicorn_access_log, log_dir=settings.log_dir
    )
    position_logger = loggers.get("position")
    ccxt_adapter = CCXTAdapter(logger=loggers.get("ccxt"))
    await db.connect()
    if position_logger is not None:
        position_logger.info(
            "worker_started %s",
            {
                "worker_id": settings.worker_id,
                "pool_id": settings.worker_pool_id,
            },
        )
    last_recon_ts = 0.0

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
                now = asyncio.get_running_loop().time()
                if (
                    now - last_recon_ts
                    >= max(settings.worker_reconciliation_interval_seconds, 5)
                ):
                    await _run_reconciliation_once(
                        db=db,
                        repo=repo,
                        ccxt_adapter=ccxt_adapter,
                        pool_id=settings.worker_pool_id,
                        credentials_codec=credentials_codec,
                    )
                    last_recon_ts = now
                continue

            queue_id, command_id, account_id, attempts = claimed
            if attempts > settings.worker_max_attempts:
                async with db.connection() as conn:
                    await repo.mark_command_failed(conn, command_id)
                    await repo.mark_queue_dead(conn, queue_id)
                    await conn.commit()
                if position_logger is not None:
                    position_logger.warning(
                        "queue_dead %s",
                        {"queue_id": queue_id, "command_id": command_id, "attempts": attempts},
                    )
                continue
            try:
                await _process_claimed_queue_item(
                    db, repo, ccxt_adapter, queue_id, command_id, account_id, credentials_codec
                )
            except Exception:
                # failure already persisted; continue polling.
                if position_logger is not None:
                    position_logger.exception(
                        "worker_process_error %s",
                        {"queue_id": queue_id, "command_id": command_id, "account_id": account_id},
                    )
                pass

            now = asyncio.get_running_loop().time()
            if (
                now - last_recon_ts
                >= max(settings.worker_reconciliation_interval_seconds, 5)
            ):
                await _run_reconciliation_once(
                    db=db,
                    repo=repo,
                    ccxt_adapter=ccxt_adapter,
                    pool_id=settings.worker_pool_id,
                    credentials_codec=credentials_codec,
                )
                last_recon_ts = now
    finally:
        with contextlib.suppress(Exception):
            await db.disconnect()
        if position_logger is not None:
            position_logger.info("worker_stopped %s", {"worker_id": settings.worker_id})


if __name__ == "__main__":
    asyncio.run(run_worker())
