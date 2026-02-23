import time
from decimal import Decimal
from typing import Any

from .app.ccxt_adapter import CCXTAdapter
from .app.credentials_codec import CredentialsCodec
from .app.repository_mysql import MySQLCommandRepository


def _dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def _extract_client_order_id(trade: dict[str, Any]) -> str | None:
    direct = trade.get("clientOrderId")
    if direct:
        return str(direct)
    info = trade.get("info")
    if isinstance(info, dict):
        for key in ("clientOrderId", "client_order_id", "clientOrderIdStr", "c"):
            value = info.get(key)
            if value:
                return str(value)
    return None


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
        "client_order_id": _extract_client_order_id(trade),
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

    qty = exchange_trade["amount"]
    price = exchange_trade["price"]
    symbol = exchange_trade["symbol"]
    side = exchange_trade["side"]
    exchange_order_id = exchange_trade.get("order")
    client_order_id = exchange_trade.get("client_order_id")

    linked_order = await repo.fetch_open_order_link(
        conn,
        account_id,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id,
    )
    if linked_order is None:
        # Build deterministic fallback key when exchange omits order id.
        if not exchange_order_id and not client_order_id and exchange_trade.get("id"):
            client_order_id = f"ext-trade:{exchange_trade['id']}"
        linked_order = await repo.get_or_create_external_unmatched_order(
            conn=conn,
            account_id=account_id,
            symbol=symbol,
            side=side,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            qty=qty,
            price=price,
        )

    strategy_id = int(linked_order["strategy_id"]) if linked_order else 0
    position_id = int(linked_order["position_id"]) if linked_order else 0
    order_id = int(linked_order["id"]) if linked_order else None
    order_stop_loss = linked_order.get("stop_loss") if linked_order else None
    order_stop_gain = linked_order.get("stop_gain") if linked_order else None
    order_comment = linked_order.get("comment") if linked_order else None
    order_reason = str(linked_order.get("reason", "")).lower() if linked_order else ""
    mode = await repo.fetch_account_position_mode(conn, account_id)
    isolated_external = bool(
        linked_order
        and strategy_id == 0
        and order_reason == "external"
    )

    if isolated_external:
        if position_id <= 0:
            position_id = await repo.create_position_open(
                conn=conn,
                account_id=account_id,
                symbol=symbol,
                strategy_id=0,
                side=side,
                qty=qty,
                avg_price=price,
                stop_loss=order_stop_loss,
                stop_gain=order_stop_gain,
                comment=order_comment,
                reason="external",
            )
            if order_id is not None:
                await repo.update_order_position_link(conn, order_id, position_id)
        else:
            explicit = await repo.fetch_open_position(conn, account_id, position_id)
            if explicit is None or explicit[1] != symbol:
                position_id = await repo.create_position_open(
                    conn=conn,
                    account_id=account_id,
                    symbol=symbol,
                    strategy_id=0,
                    side=side,
                    qty=qty,
                    avg_price=price,
                    stop_loss=order_stop_loss,
                    stop_gain=order_stop_gain,
                    comment=order_comment,
                    reason="external",
                )
                if order_id is not None:
                    await repo.update_order_position_link(conn, order_id, position_id)
            else:
                explicit_side = str(explicit[3]).lower()
                old_qty = _dec(explicit[4])
                old_avg = _dec(explicit[5])
                if explicit_side == side:
                    new_qty = old_qty + qty
                    if new_qty <= 0:
                        await repo.close_position(conn, position_id)
                    else:
                        new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
                        await repo.update_position_open_qty_price(conn, position_id, new_qty, new_avg)
                else:
                    if old_qty > qty:
                        remain = old_qty - qty
                        await repo.update_position_open_qty_price(conn, position_id, remain, old_avg)
                    elif old_qty == qty:
                        await repo.close_position(conn, position_id)
                    else:
                        reverse_qty = qty - old_qty
                        await repo.close_position(conn, position_id)
                        position_id = await repo.create_position_open(
                            conn=conn,
                            account_id=account_id,
                            symbol=symbol,
                            strategy_id=0,
                            side=side,
                            qty=reverse_qty,
                            avg_price=price,
                            stop_loss=order_stop_loss,
                            stop_gain=order_stop_gain,
                            comment=order_comment,
                            reason="external",
                        )
                        if order_id is not None:
                            await repo.update_order_position_link(conn, order_id, position_id)
    elif mode == "hedge":
        if position_id > 0:
            explicit = await repo.fetch_open_position(conn, account_id, position_id)
            if explicit is not None and explicit[1] == symbol:
                explicit_side = str(explicit[3]).lower()
                old_qty = _dec(explicit[4])
                old_avg = _dec(explicit[5])
                if explicit_side == side:
                    new_qty = old_qty + qty
                    if new_qty <= 0:
                        await repo.close_position(conn, position_id)
                    else:
                        new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
                        await repo.update_position_open_qty_price(conn, position_id, new_qty, new_avg)
                else:
                    if old_qty > qty:
                        remain = old_qty - qty
                        await repo.update_position_open_qty_price(conn, position_id, remain, old_avg)
                    elif old_qty == qty:
                        await repo.close_position(conn, position_id)
                    else:
                        reverse_qty = qty - old_qty
                        await repo.close_position(conn, position_id)
                        reverse = await repo.fetch_open_position_for_symbol_non_external(
                            conn, account_id, symbol, side
                        )
                        if reverse is None:
                            position_id = await repo.create_position_open(
                                conn=conn,
                                account_id=account_id,
                                symbol=symbol,
                                strategy_id=strategy_id,
                                side=side,
                                qty=reverse_qty,
                                avg_price=price,
                                stop_loss=order_stop_loss,
                                stop_gain=order_stop_gain,
                                comment=order_comment,
                                reason=reason,
                            )
                        else:
                            position_id = int(reverse["id"])
                            rev_old_qty = _dec(reverse["qty"])
                            rev_old_avg = _dec(reverse["avg_price"])
                            rev_new_qty = rev_old_qty + reverse_qty
                            rev_new_avg = ((rev_old_qty * rev_old_avg) + (reverse_qty * price)) / rev_new_qty
                            await repo.update_position_open_qty_price(conn, position_id, rev_new_qty, rev_new_avg)
            else:
                existing = await repo.fetch_open_position_for_symbol_non_external(
                    conn, account_id, symbol, side
                )
                if existing is None:
                    position_id = await repo.create_position_open(
                        conn=conn,
                        account_id=account_id,
                        symbol=symbol,
                        strategy_id=strategy_id,
                        side=side,
                        qty=qty,
                        avg_price=price,
                        stop_loss=order_stop_loss,
                        stop_gain=order_stop_gain,
                        comment=order_comment,
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
            existing = await repo.fetch_open_position_for_symbol_non_external(
                conn, account_id, symbol, side
            )
            if existing is None:
                position_id = await repo.create_position_open(
                    conn=conn,
                    account_id=account_id,
                    symbol=symbol,
                    strategy_id=strategy_id,
                    side=side,
                    qty=qty,
                    avg_price=price,
                    stop_loss=order_stop_loss,
                    stop_gain=order_stop_gain,
                    comment=order_comment,
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
        existing = await repo.fetch_open_net_position_by_symbol_non_external(conn, account_id, symbol)
        if existing is None:
            position_id = await repo.create_position_open(
                conn=conn,
                account_id=account_id,
                symbol=symbol,
                strategy_id=strategy_id,
                side=side,
                qty=qty,
                avg_price=price,
                stop_loss=order_stop_loss,
                stop_gain=order_stop_gain,
                comment=order_comment,
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
                        strategy_id=strategy_id,
                        side=side,
                        qty=reverse_qty,
                        avg_price=price,
                        stop_loss=order_stop_loss,
                        stop_gain=order_stop_gain,
                        comment=order_comment,
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
        strategy_id=strategy_id,
        reason=reason,
        comment=order_comment,
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
            "strategy_id": strategy_id,
        },
    )


def _normalized_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for trade in trades:
        norm = _safe_trade(trade)
        if norm is not None:
            out.append(norm)
    out.sort(key=lambda t: (int(t["timestamp"] or 0), str(t.get("id") or "")))
    return out


async def _reconcile_account_once(
    conn: Any,
    repo: MySQLCommandRepository,
    ccxt_adapter: CCXTAdapter,
    credentials_codec: CredentialsCodec,
    account_id: int,
    lookback_seconds: int,
    scope: str,
    limit: int,
) -> None:
    exchange_id, is_testnet, api_key_enc, secret_enc, passphrase_enc, extra_config = await repo.fetch_account_exchange_credentials(
        conn, account_id
    )
    api_key = credentials_codec.decrypt_maybe(api_key_enc)
    secret = credentials_codec.decrypt_maybe(secret_enc)
    passphrase = credentials_codec.decrypt_maybe(passphrase_enc)

    lookback_ms = max(1, int(lookback_seconds)) * 1000
    floor_since = max(0, int(time.time() * 1000) - lookback_ms)

    cursor_raw = await repo.fetch_reconciliation_cursor(conn, account_id, "my_trades_since")
    cursor_since = int(cursor_raw) if cursor_raw and cursor_raw.isdigit() else None
    since = floor_since if cursor_since is None else min(cursor_since, floor_since)

    try:
        trades = await ccxt_adapter.fetch_my_trades(
            exchange_id=exchange_id,
            use_testnet=is_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            symbol=None,
            since=since,
            limit=max(10, int(limit)),
            params={},
        )
    except Exception:
        symbols = await repo.list_recent_symbols_for_account(conn, account_id, limit=20)
        trades = []
        for symbol in symbols:
            try:
                chunk = await ccxt_adapter.fetch_my_trades(
                    exchange_id=exchange_id,
                    use_testnet=is_testnet,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    extra_config=extra_config,
                    symbol=symbol,
                    since=since,
                    limit=max(10, int(limit)),
                    params={},
                )
                trades.extend(chunk or [])
            except Exception:
                continue

    normalized = _normalized_trades(trades)
    max_ts = cursor_since or 0
    for norm in normalized:
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
        payload={
            "scope": scope,
            "lookback_seconds": int(lookback_seconds),
            "trades_count": len(normalized),
            "cursor": max_ts + 1 if max_ts > 0 else None,
        },
    )

