from decimal import Decimal
from typing import Any

from .ccxt_adapter import CCXTAdapter
from .credentials_codec import CredentialsCodec
from .db_mysql import DatabaseMySQL
from .repository_mysql import MySQLCommandRepository


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


def _parse_int_list(value: Any) -> list[int]:
    if isinstance(value, list):
        return [int(x) for x in value if str(x).strip().isdigit() and int(x) > 0]
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    return [int(x.strip()) for x in text.split(",") if x.strip().isdigit() and int(x.strip()) > 0]


async def _merge_open_positions_keep_target(
    repo: MySQLCommandRepository,
    conn: Any,
    *,
    account_id: int,
    source_position_id: int,
    target_position_id: int,
) -> dict[str, Any] | None:
    if source_position_id <= 0 or target_position_id <= 0 or source_position_id == target_position_id:
        return None
    source = await repo.fetch_open_position(conn, account_id, source_position_id)
    target = await repo.fetch_open_position(conn, account_id, target_position_id)
    if source is None or target is None:
        return None
    src_pid, src_symbol, _src_strategy_id, src_side, src_qty, src_avg = source
    dst_pid, dst_symbol, _dst_strategy_id, dst_side, dst_qty, dst_avg = target
    if src_symbol != dst_symbol or src_side != dst_side:
        return None
    q_src = _dec(src_qty)
    q_dst = _dec(dst_qty)
    if q_src <= 0 or q_dst <= 0:
        return None
    new_qty = q_src + q_dst
    new_avg = ((q_src * _dec(src_avg)) + (q_dst * _dec(dst_avg))) / new_qty
    await repo.update_position_open_qty_price(conn, dst_pid, new_qty, new_avg)
    await repo.reassign_open_orders_position(
        conn,
        account_id=account_id,
        from_position_id=src_pid,
        to_position_id=dst_pid,
    )
    await repo.reassign_deals_position(
        conn,
        account_id=account_id,
        from_position_id=src_pid,
        to_position_id=dst_pid,
    )
    await repo.close_position_merged(conn, src_pid)
    return {
        "source_position_id": int(src_pid),
        "target_position_id": int(dst_pid),
        "source_qty": str(q_src),
        "target_qty_before": str(q_dst),
        "target_qty_after": str(new_qty),
        "target_avg_price_after": str(new_avg),
        "symbol": str(src_symbol),
        "side": str(src_side),
    }


async def execute_command_by_id(
    db: DatabaseMySQL,
    repo: MySQLCommandRepository,
    ccxt_adapter: CCXTAdapter,
    credentials_codec: CredentialsCodec,
    command_id: int,
    account_id: int,
) -> None:
    async with db.connection() as conn:
        payload: dict[str, Any] = {}
        try:
            cmd_account_id, command_type, payload = await repo.fetch_command_for_worker(
                conn, command_id
            )
            if cmd_account_id != account_id:
                raise RuntimeError("command/account mismatch")

            exchange_id, is_testnet, api_key_enc, secret_enc, passphrase_enc, extra_config = await repo.fetch_account_exchange_credentials(
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

                params: dict[str, Any] = {}
                raw_params = payload.get("params")
                if isinstance(raw_params, dict):
                    params.update(raw_params)
                if bool(payload.get("post_only", False)):
                    params["postOnly"] = True
                tif = str(payload.get("time_in_force") or "").strip()
                if tif:
                    params["timeInForce"] = tif.upper()
                trigger_price = payload.get("trigger_price")
                if trigger_price is not None:
                    params["triggerPrice"] = trigger_price
                stop_price = payload.get("stop_price")
                if stop_price is not None:
                    params["stopPrice"] = stop_price
                tp_price = payload.get("take_profit_price")
                if tp_price is not None:
                    params["takeProfitPrice"] = tp_price
                trailing_amount = payload.get("trailing_amount")
                if trailing_amount is not None:
                    params["trailingAmount"] = trailing_amount
                trailing_percent = payload.get("trailing_percent")
                if trailing_percent is not None:
                    params["trailingPercent"] = trailing_percent
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
                    extra_config=extra_config,
                    session_key=f"account:{int(account_id)}",
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

            elif command_type in {"cancel_order", "cancel_all_orders"}:
                order_ids: list[int] = []
                if command_type == "cancel_order":
                    order_ids = _parse_int_list(payload.get("order_ids"))
                    if int(payload.get("order_id", 0) or 0) > 0:
                        order_ids.append(int(payload.get("order_id", 0) or 0))
                    if not order_ids:
                        raise PermanentCommandError("payload.order_id/order_ids is required for cancel_order")
                    order_ids = sorted(set(order_ids))
                else:
                    strategy_ids = _parse_int_list(payload.get("strategy_ids"))
                    if not strategy_ids and isinstance(payload.get("strategy_ids_csv"), str):
                        strategy_ids = _parse_int_list(payload.get("strategy_ids_csv"))
                    rows = await repo.list_cancelable_orders(
                        conn=conn,
                        account_id=account_id,
                        strategy_ids=(strategy_ids if strategy_ids else None),
                    )
                    order_ids = [int(r["id"]) for r in rows]
                    if not order_ids:
                        raise PermanentCommandError("no open orders to cancel")

                canceled_ids: list[int] = []
                skipped_ids: list[int] = []
                for order_id in order_ids:
                    order = await repo.fetch_order_by_id(conn, account_id, order_id)
                    if order is None:
                        skipped_ids.append(order_id)
                        continue
                    if not order.get("exchange_order_id"):
                        skipped_ids.append(order_id)
                        continue
                    try:
                        canceled = await ccxt_adapter.cancel_order(
                            exchange_id=exchange_id,
                            use_testnet=is_testnet,
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            extra_config=extra_config,
                            session_key=f"account:{int(account_id)}",
                            exchange_order_id=str(order["exchange_order_id"]),
                            symbol=str(order["symbol"]),
                            params={},
                        )
                    except Exception:
                        skipped_ids.append(order_id)
                        continue
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
                    canceled_ids.append(order_id)
                    await repo.insert_event(
                        conn=conn,
                        account_id=account_id,
                        namespace="position",
                        event_type="order_canceled",
                        payload={"command_id": command_id, "order_id": order_id},
                    )
                if not canceled_ids:
                    raise PermanentCommandError("no orders canceled")
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="orders_canceled_batch",
                    payload={
                        "command_id": command_id,
                        "command_type": command_type,
                        "canceled_order_ids": canceled_ids,
                        "skipped_order_ids": skipped_ids,
                    },
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
                client_order_id = str(order.get("client_order_id") or order["id"])
                edited = await ccxt_adapter.edit_order_if_supported(
                    exchange_id=exchange_id,
                    use_testnet=is_testnet,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    extra_config=extra_config,
                    session_key=f"account:{int(account_id)}",
                    exchange_order_id=str(order["exchange_order_id"]),
                    symbol=str(order["symbol"]),
                    side=str(order["side"]),
                    order_type=str(order["order_type"]),
                    amount=new_qty,
                    price=new_price,
                    params={"clientOrderId": client_order_id},
                )
                if edited is not None:
                    new_exchange_order_id = str(edited.get("id")) if edited.get("id") else str(order["exchange_order_id"])
                    await repo.mark_order_submitted_exchange_with_values(
                        conn,
                        order_id=order_id,
                        exchange_order_id=new_exchange_order_id,
                        qty=new_qty,
                        price=new_price,
                    )
                    await repo.insert_ccxt_order_raw(
                        conn=conn,
                        account_id=account_id,
                        exchange_id=exchange_id,
                        exchange_order_id=new_exchange_order_id,
                        client_order_id=str(edited.get("clientOrderId")) if edited.get("clientOrderId") else client_order_id,
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
                else:
                    canceled = await ccxt_adapter.cancel_order(
                        exchange_id=exchange_id,
                        use_testnet=is_testnet,
                        api_key=api_key,
                        secret=secret,
                        passphrase=passphrase,
                        extra_config=extra_config,
                        session_key=f"account:{int(account_id)}",
                        exchange_order_id=str(order["exchange_order_id"]),
                        symbol=str(order["symbol"]),
                        params={},
                    )
                    await repo.mark_order_canceled_edit_pending(conn, order_id)
                    await repo.insert_ccxt_order_raw(
                        conn=conn,
                        account_id=account_id,
                        exchange_id=exchange_id,
                        exchange_order_id=str(canceled.get("id")) if canceled.get("id") else str(order["exchange_order_id"]),
                        client_order_id=str(canceled.get("clientOrderId")) if canceled.get("clientOrderId") else client_order_id,
                        symbol=str(canceled.get("symbol")) if canceled.get("symbol") else str(order["symbol"]),
                        raw_json=canceled,
                    )
                    await repo.insert_event(
                        conn=conn,
                        account_id=account_id,
                        namespace="position",
                        event_type="order_change_replace_pending",
                        payload={"command_id": command_id, "order_id": order_id},
                    )
                    try:
                        created = await ccxt_adapter.create_order(
                            exchange_id=exchange_id,
                            use_testnet=is_testnet,
                            api_key=api_key,
                            secret=secret,
                            passphrase=passphrase,
                            extra_config=extra_config,
                            session_key=f"account:{int(account_id)}",
                            symbol=str(order["symbol"]),
                            side=str(order["side"]),
                            order_type=str(order["order_type"]),
                            amount=new_qty,
                            price=new_price,
                            params={"clientOrderId": client_order_id},
                        )
                    except Exception as exc:
                        await repo.mark_order_edit_replace_failed(conn, order_id)
                        await repo.insert_event(
                            conn=conn,
                            account_id=account_id,
                            namespace="position",
                            event_type="order_change_replace_failed",
                            payload={"command_id": command_id, "order_id": order_id, "error": str(exc)},
                        )
                        raise PermanentCommandError("change_order_replace_create_failed") from exc

                    new_exchange_order_id = str(created.get("id")) if created.get("id") else None
                    new_client_order_id = str(created.get("clientOrderId")) if created.get("clientOrderId") else client_order_id
                    await repo.insert_ccxt_order_raw(
                        conn=conn,
                        account_id=account_id,
                        exchange_id=exchange_id,
                        exchange_order_id=new_exchange_order_id,
                        client_order_id=new_client_order_id,
                        symbol=str(created.get("symbol")) if created.get("symbol") else str(order["symbol"]),
                        raw_json=created,
                    )
                    orphan = await repo.find_external_orphan_order_for_replace(
                        conn=conn,
                        account_id=account_id,
                        exchange_order_id=new_exchange_order_id,
                        client_order_id=new_client_order_id,
                        symbol=str(order["symbol"]),
                        side=str(order["side"]),
                    )
                    if orphan is None:
                        await repo.mark_order_submitted_exchange_with_values(
                            conn,
                            order_id=order_id,
                            exchange_order_id=new_exchange_order_id,
                            qty=new_qty,
                            price=new_price,
                        )
                        await repo.insert_event(
                            conn=conn,
                            account_id=account_id,
                            namespace="position",
                            event_type="order_changed",
                            payload={"command_id": command_id, "order_id": order_id},
                        )
                    else:
                        orphan_id = int(orphan["id"])
                        await repo.mark_order_consolidated_to_orphan(conn, order_id, orphan_id)
                        await repo.adopt_external_orphan_order(
                            conn,
                            orphan_order_id=orphan_id,
                            origin_order_id=order_id,
                            strategy_id=int(order["strategy_id"]),
                            reason=str(order["reason"]),
                            comment=order.get("comment"),
                        )
                        if int(order["strategy_id"]) > 0:
                            await repo.reassign_deals_strategy_by_orders(
                                conn,
                                account_id=account_id,
                                order_ids=[orphan_id],
                                target_strategy_id=int(order["strategy_id"]),
                            )
                        old_position_id = int(order.get("position_id", 0) or 0)
                        orphan_position_id = int(orphan.get("position_id", 0) or 0)
                        merged_meta = await _merge_open_positions_keep_target(
                            repo,
                            conn,
                            account_id=account_id,
                            source_position_id=orphan_position_id,
                            target_position_id=old_position_id,
                        )
                        if merged_meta is not None:
                            await repo.update_order_position_link(conn, orphan_id, old_position_id)
                        await repo.insert_event(
                            conn=conn,
                            account_id=account_id,
                            namespace="position",
                            event_type="order_change_replace_consolidated",
                            payload={
                                "command_id": command_id,
                                "order_id": order_id,
                                "orphan_order_id": orphan_id,
                                "merge": merged_meta,
                            },
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

                pid_a, symbol_a, _strategy_a, side_a, qty_a, avg_a = row_a
                pid_b, symbol_b, _strategy_b, side_b, qty_b, avg_b = row_b
                if symbol_a != symbol_b:
                    raise PermanentCommandError("close_by positions must have same symbol")
                if side_a == side_b:
                    raise PermanentCommandError("close_by positions must be opposite sides")

                q_a = _dec(qty_a)
                q_b = _dec(qty_b)
                close_qty_max = min(q_a, q_b)
                req_qty_raw = payload.get("qty")
                if req_qty_raw is None:
                    close_qty = close_qty_max
                else:
                    req_qty = _dec(req_qty_raw)
                    if req_qty <= 0:
                        raise PermanentCommandError("close_by qty must be > 0")
                    if req_qty > close_qty_max:
                        raise PermanentCommandError("close_by qty exceeds available minimum")
                    close_qty = req_qty
                if close_qty <= 0:
                    raise PermanentCommandError("close_by quantity is zero")

                strategy_id = int(payload.get("strategy_id", 0) or 0)
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
                    strategy_id=strategy_id,
                    reason=reason,
                    comment=None,
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
                    strategy_id=strategy_id,
                    reason=reason,
                    comment=None,
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
            elif command_type == "merge_positions":
                source_id = int(payload.get("source_position_id", 0) or 0)
                target_id = int(payload.get("target_position_id", 0) or 0)
                if source_id <= 0 or target_id <= 0 or source_id == target_id:
                    raise PermanentCommandError("merge_positions requires different source/target ids")
                source = await repo.fetch_open_position(conn, account_id, source_id)
                target = await repo.fetch_open_position(conn, account_id, target_id)
                if source is None or target is None:
                    raise PermanentCommandError("merge_positions positions must exist and be open")

                src_pid, src_symbol, src_strategy_id, src_side, src_qty, src_avg = source
                dst_pid, dst_symbol, dst_strategy_id, dst_side, dst_qty, dst_avg = target
                if src_symbol != dst_symbol:
                    raise PermanentCommandError("merge_positions requires same symbol")
                if src_side != dst_side:
                    raise PermanentCommandError("merge_positions requires same side")

                q_src = _dec(src_qty)
                q_dst = _dec(dst_qty)
                if q_src <= 0 or q_dst <= 0:
                    raise PermanentCommandError("merge_positions requires positive qty in both positions")

                merged = await _merge_open_positions_keep_target(
                    repo,
                    conn,
                    account_id=account_id,
                    source_position_id=src_pid,
                    target_position_id=dst_pid,
                )
                if merged is None:
                    raise PermanentCommandError("merge_positions cannot merge current positions")

                stop_mode = str(payload.get("stop_mode", "keep") or "keep").strip().lower()
                if stop_mode not in {"keep", "clear", "set"}:
                    raise PermanentCommandError("merge_positions stop_mode invalid")
                if stop_mode == "clear":
                    await repo.update_position_targets_comment(
                        conn=conn,
                        account_id=account_id,
                        position_id=dst_pid,
                        set_stop_loss=True,
                        stop_loss=None,
                        set_stop_gain=True,
                        stop_gain=None,
                        set_comment=False,
                        comment=None,
                    )
                elif stop_mode == "set":
                    await repo.update_position_targets_comment(
                        conn=conn,
                        account_id=account_id,
                        position_id=dst_pid,
                        set_stop_loss=True,
                        stop_loss=payload.get("oms_stop_loss"),
                        set_stop_gain=True,
                        stop_gain=payload.get("oms_stop_gain"),
                        set_comment=False,
                        comment=None,
                    )

                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="positions_merged",
                    payload={
                        "command_id": command_id,
                        "source_position_id": src_pid,
                        "target_position_id": dst_pid,
                        "symbol": src_symbol,
                        "side": src_side,
                        "source_qty": str(merged["source_qty"]),
                        "target_qty_before": str(merged["target_qty_before"]),
                        "target_qty_after": str(merged["target_qty_after"]),
                        "target_avg_price_after": str(merged["target_avg_price_after"]),
                        "target_strategy_id": int(dst_strategy_id),
                        "source_strategy_id": int(src_strategy_id),
                        "stop_mode": stop_mode,
                    },
                )
            else:
                raise PermanentCommandError(f"unsupported command_type: {command_type}")

            await repo.mark_command_completed(conn, command_id)
            if position_lock_id is not None:
                await repo.release_close_position_lock(conn, position_lock_id)
            await conn.commit()
        except PermanentCommandError:
            await repo.mark_command_failed(conn, command_id)
            order_id = await repo.fetch_order_id_by_command_id(conn, command_id)
            if order_id is not None:
                await repo.mark_order_rejected(conn, order_id)
            close_pid = _release_close_position_requested(payload)
            if close_pid is not None:
                await repo.reopen_position_if_close_requested(conn, account_id, close_pid)
                await repo.release_close_position_lock(conn, close_pid)
            await conn.commit()
            raise
        except Exception:
            await repo.mark_command_failed(conn, command_id)
            close_pid = _release_close_position_requested(payload)
            if close_pid is not None:
                await repo.reopen_position_if_close_requested(conn, account_id, close_pid)
                await repo.release_close_position_lock(conn, close_pid)
            await conn.commit()
            raise

