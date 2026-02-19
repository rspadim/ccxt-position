import hashlib
import json
from typing import Any


class MySQLCommandRepository:
    async def fetch_account(self, conn: Any, account_id: int) -> tuple[int, int]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, pool_id
                FROM accounts
                WHERE id = %s AND status = 'active'
                LIMIT 1
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None:
            raise ValueError("account_not_found")
        return int(row[0]), int(row[1])

    async def fetch_permissions(
        self, conn: Any, user_id: int, account_id: int
    ) -> tuple[bool, bool, bool] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT can_read, can_trade, can_risk_manage
                FROM user_account_permissions
                WHERE user_id = %s AND account_id = %s
                LIMIT 1
                """,
                (user_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return bool(row[0]), bool(row[1]), bool(row[2])

    async def fetch_account_by_id(self, conn: Any, account_id: int) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, exchange_id, pool_id, status
                FROM accounts
                WHERE id = %s
                LIMIT 1
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "exchange_id": str(row[1]),
            "pool_id": int(row[2]),
            "status": str(row[3]),
        }

    async def fetch_account_position_mode(self, conn: Any, account_id: int) -> str:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT position_mode
                FROM accounts
                WHERE id = %s
                LIMIT 1
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return "hedge"
        mode = str(row[0]).lower().strip()
        if mode not in {"hedge", "netting"}:
            return "hedge"
        return mode

    async def list_active_accounts_by_pool(self, conn: Any, pool_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, exchange_id
                FROM accounts
                WHERE status = 'active' AND pool_id = %s
                ORDER BY id
                """,
                (pool_id,),
            )
            rows = await cur.fetchall()
        return [{"id": int(r[0]), "exchange_id": str(r[1])} for r in rows]

    async def fetch_allow_new_positions(self, conn: Any, account_id: int) -> bool:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT allow_new_positions
                FROM account_risk_state
                WHERE account_id = %s
                LIMIT 1
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return True
        return bool(row[0])

    async def position_exists_open(
        self, conn: Any, account_id: int, position_id: int, symbol: str
    ) -> bool:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM position_positions
                WHERE id = %s AND account_id = %s AND symbol = %s AND state = 'open'
                LIMIT 1
                """,
                (position_id, account_id, symbol),
            )
            row = await cur.fetchone()
        return row is not None

    async def insert_position_command(
        self,
        conn: Any,
        account_id: int,
        command_type: str,
        request_id: str | None,
        payload: dict[str, Any],
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO position_commands (account_id, command_type, request_id, payload_json, status)
                VALUES (%s, %s, %s, %s, 'accepted')
                """,
                (account_id, command_type, request_id, payload),
            )
            return int(cur.lastrowid)

    async def insert_position_order_pending_submit(
        self,
        conn: Any,
        command_id: int,
        account_id: int,
        symbol: str,
        side: str,
        order_type: str,
        magic_id: int,
        position_id: int,
        reason: str,
        client_order_id: str | None,
        qty: Any,
        price: Any,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO position_orders (
                    command_id, account_id, symbol, side, order_type, status,
                    magic_id, position_id, reason, client_order_id,
                    qty, price
                ) VALUES (
                    %s, %s, %s, %s, %s, 'PENDING_SUBMIT',
                    %s, %s, %s, %s,
                    %s, %s
                )
                """,
                (
                    command_id,
                    account_id,
                    symbol,
                    side,
                    order_type,
                    magic_id,
                    position_id,
                    reason,
                    client_order_id,
                    qty,
                    price,
                ),
            )
            return int(cur.lastrowid)

    async def enqueue_command(
        self, conn: Any, account_id: int, pool_id: int, command_id: int
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO command_queue (account_id, pool_id, command_id, status)
                VALUES (%s, %s, %s, 'queued')
                """,
                (account_id, pool_id, command_id),
            )

    async def fetch_open_position(
        self, conn: Any, account_id: int, position_id: int
    ) -> tuple[int, str, str, str, str] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, symbol, side, qty, avg_price
                FROM position_positions
                WHERE id = %s AND account_id = %s AND state = 'open'
                LIMIT 1
                """,
                (position_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1]), str(row[2]).lower(), str(row[3]), str(row[4])

    async def fetch_order_for_update(
        self, conn: Any, account_id: int, order_id: int
    ) -> tuple[int, str, str] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, status, order_type
                FROM position_orders
                WHERE id = %s AND account_id = %s
                LIMIT 1
                """,
                (order_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1]), str(row[2]).lower()

    async def cleanup_expired_close_locks(self, conn: Any, position_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM position_close_locks
                WHERE position_id = %s AND expires_at <= NOW()
                """,
                (position_id,),
            )

    async def acquire_close_position_lock(
        self,
        conn: Any,
        account_id: int,
        position_id: int,
        request_id: str | None,
        lock_ttl_seconds: int = 120,
    ) -> bool:
        await self.cleanup_expired_close_locks(conn, position_id)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO position_close_locks (
                        account_id, position_id, request_id, lock_reason, expires_at
                    ) VALUES (
                        %s, %s, %s, 'close_position', DATE_ADD(NOW(), INTERVAL %s SECOND)
                    )
                    """,
                    (account_id, position_id, request_id, lock_ttl_seconds),
                )
            return True
        except Exception:
            return False

    async def claim_next_queue_item(
        self, conn: Any, pool_id: int, worker_id: str
    ) -> tuple[int, int, int, int] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, command_id, account_id, attempts
                FROM command_queue
                WHERE pool_id = %s
                  AND status = 'queued'
                  AND available_at <= NOW()
                ORDER BY id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (pool_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None

            queue_id = int(row[0])
            await cur.execute(
                """
                UPDATE command_queue
                SET status = 'processing',
                    attempts = attempts + 1,
                    locked_by = %s,
                    locked_at = NOW()
                WHERE id = %s
                """,
                (worker_id, queue_id),
            )
        return int(row[0]), int(row[1]), int(row[2]), int(row[3]) + 1

    async def fetch_command_for_worker(
        self, conn: Any, command_id: int
    ) -> tuple[int, str, dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT account_id, command_type, payload_json
                FROM position_commands
                WHERE id = %s
                LIMIT 1
                """,
                (command_id,),
            )
            row = await cur.fetchone()
        if row is None:
            raise ValueError("command_not_found")
        payload = row[2] if isinstance(row[2], dict) else {}
        return int(row[0]), str(row[1]), payload

    async def mark_command_completed(self, conn: Any, command_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_commands
                SET status = 'completed', updated_at = NOW()
                WHERE id = %s
                """,
                (command_id,),
            )

    async def mark_command_failed(self, conn: Any, command_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_commands
                SET status = 'failed', updated_at = NOW()
                WHERE id = %s
                """,
                (command_id,),
            )

    async def mark_queue_done(self, conn: Any, queue_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE command_queue
                SET status = 'done', updated_at = NOW()
                WHERE id = %s
                """,
                (queue_id,),
            )

    async def mark_queue_failed(self, conn: Any, queue_id: int, delay_seconds: int = 30) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE command_queue
                SET status = 'queued',
                    available_at = DATE_ADD(NOW(), INTERVAL %s SECOND),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (delay_seconds, queue_id),
            )

    async def mark_queue_dead(self, conn: Any, queue_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE command_queue
                SET status = 'failed',
                    updated_at = NOW()
                WHERE id = %s
                """,
                (queue_id,),
            )

    async def fetch_order_id_by_command_id(self, conn: Any, command_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM position_orders
                WHERE command_id = %s
                LIMIT 1
                """,
                (command_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    async def mark_order_submitted(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_orders
                SET status = 'SUBMITTED', updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def mark_order_submitted_exchange(
        self, conn: Any, order_id: int, exchange_order_id: str | None
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_orders
                SET status = 'SUBMITTED',
                    exchange_order_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (exchange_order_id, order_id),
            )

    async def mark_order_rejected(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_orders
                SET status = 'REJECTED', updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def mark_order_canceled(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_orders
                SET status = 'CANCELED', closed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def fetch_account_exchange_credentials(
        self, conn: Any, account_id: int
    ) -> tuple[str, str | None, str | None, str | None]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT a.exchange_id, c.api_key_enc, c.secret_enc, c.passphrase_enc
                FROM accounts a
                LEFT JOIN account_credentials_encrypted c ON c.account_id = a.id
                WHERE a.id = %s
                LIMIT 1
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None:
            raise ValueError("account_not_found")
        return str(row[0]), row[1], row[2], row[3]

    async def fetch_order_by_id(
        self, conn: Any, account_id: int, order_id: int
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, symbol, side, order_type, status, qty, price, filled_qty,
                       magic_id, position_id, reason, client_order_id, exchange_order_id
                FROM position_orders
                WHERE id = %s AND account_id = %s
                LIMIT 1
                """,
                (order_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "symbol": str(row[1]),
            "side": str(row[2]).lower(),
            "order_type": str(row[3]).lower(),
            "status": str(row[4]),
            "qty": row[5],
            "price": row[6],
            "filled_qty": row[7],
            "magic_id": int(row[8]),
            "position_id": int(row[9]),
            "reason": str(row[10]),
            "client_order_id": row[11],
            "exchange_order_id": row[12],
        }

    async def fetch_order_for_command_send(self, conn: Any, command_id: int) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, account_id, symbol, side, order_type, qty, price, client_order_id
                FROM position_orders
                WHERE command_id = %s
                LIMIT 1
                """,
                (command_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "account_id": int(row[1]),
            "symbol": str(row[2]),
            "side": str(row[3]).lower(),
            "order_type": str(row[4]).lower(),
            "qty": row[5],
            "price": row[6],
            "client_order_id": row[7],
        }

    async def release_close_position_lock(self, conn: Any, position_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM position_close_locks
                WHERE position_id = %s
                """,
                (position_id,),
            )

    async def insert_ccxt_order_raw(
        self,
        conn: Any,
        account_id: int,
        exchange_id: str,
        exchange_order_id: str | None,
        client_order_id: str | None,
        symbol: str | None,
        raw_json: dict[str, Any],
    ) -> None:
        payload = json.dumps(raw_json, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT IGNORE INTO ccxt_orders_raw (
                    account_id, exchange_id, exchange_order_id, client_order_id, symbol, raw_json, fingerprint_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (account_id, exchange_id, exchange_order_id, client_order_id, symbol, raw_json, fingerprint),
            )

    async def insert_ccxt_trade_raw(
        self,
        conn: Any,
        account_id: int,
        exchange_id: str,
        exchange_trade_id: str | None,
        exchange_order_id: str | None,
        symbol: str | None,
        raw_json: dict[str, Any],
    ) -> None:
        payload = json.dumps(raw_json, sort_keys=True, separators=(",", ":"))
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT IGNORE INTO ccxt_trades_raw (
                    account_id, exchange_id, exchange_trade_id, exchange_order_id, symbol, raw_json, fingerprint_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (account_id, exchange_id, exchange_trade_id, exchange_order_id, symbol, raw_json, fingerprint),
            )

    async def fetch_open_position_for_symbol(
        self, conn: Any, account_id: int, symbol: str, side: str
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side
                FROM position_positions
                WHERE account_id = %s AND symbol = %s AND side = %s AND state = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol, side),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {"id": int(row[0]), "qty": row[1], "avg_price": row[2], "side": str(row[3]).lower()}

    async def fetch_open_net_position_by_symbol(
        self, conn: Any, account_id: int, symbol: str
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side
                FROM position_positions
                WHERE account_id = %s AND symbol = %s AND state = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {"id": int(row[0]), "qty": row[1], "avg_price": row[2], "side": str(row[3]).lower()}

    async def create_position_open(
        self,
        conn: Any,
        account_id: int,
        symbol: str,
        side: str,
        qty: Any,
        avg_price: Any,
        reason: str = "api",
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO position_positions (account_id, symbol, side, qty, avg_price, state, reason)
                VALUES (%s, %s, %s, %s, %s, 'open', %s)
                """,
                (account_id, symbol, side, qty, avg_price, reason),
            )
            return int(cur.lastrowid)

    async def update_position_open_qty_price(
        self, conn: Any, position_id: int, qty: Any, avg_price: Any
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_positions
                SET qty = %s,
                    avg_price = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (qty, avg_price, position_id),
            )

    async def close_position(self, conn: Any, position_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE position_positions
                SET state = 'closed', qty = 0, closed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (position_id,),
            )

    async def deal_exists_by_exchange_trade_id(
        self, conn: Any, account_id: int, exchange_trade_id: str | None
    ) -> bool:
        if not exchange_trade_id:
            return False
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM position_deals
                WHERE account_id = %s AND exchange_trade_id = %s
                LIMIT 1
                """,
                (account_id, exchange_trade_id),
            )
            row = await cur.fetchone()
        return row is not None

    async def insert_position_deal(
        self,
        conn: Any,
        account_id: int,
        order_id: int | None,
        position_id: int,
        symbol: str,
        side: str,
        qty: Any,
        price: Any,
        fee: Any,
        fee_currency: str | None,
        pnl: Any,
        magic_id: int,
        reason: str,
        reconciled: bool,
        exchange_trade_id: str | None,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO position_deals (
                    account_id, order_id, position_id, symbol, side, qty, price, fee, fee_currency, pnl,
                    magic_id, reason, reconciled, exchange_trade_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                (
                    account_id,
                    order_id,
                    position_id,
                    symbol,
                    side,
                    qty,
                    price,
                    fee,
                    fee_currency,
                    pnl,
                    magic_id,
                    reason,
                    reconciled,
                    exchange_trade_id,
                ),
            )
            return int(cur.lastrowid)

    async def fetch_open_order_by_exchange_order_id(
        self, conn: Any, account_id: int, exchange_order_id: str | None
    ) -> dict[str, Any] | None:
        if not exchange_order_id:
            return None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, magic_id, position_id
                FROM position_orders
                WHERE account_id = %s AND exchange_order_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, exchange_order_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {"id": int(row[0]), "magic_id": int(row[1]), "position_id": int(row[2])}

    async def fetch_outbox_events(
        self, conn: Any, account_id: int, after_id: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, namespace, event_type, payload_json, created_at
                FROM event_outbox
                WHERE account_id = %s AND id > %s
                ORDER BY id ASC
                LIMIT %s
                """,
                (account_id, after_id, limit),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = row[3] if isinstance(row[3], dict) else {}
            out.append(
                {
                    "id": int(row[0]),
                    "namespace": str(row[1]),
                    "event_type": str(row[2]),
                    "payload": payload,
                    "created_at": str(row[4]),
                }
            )
        return out

    async def update_reconciliation_cursor(
        self, conn: Any, account_id: int, entity: str, cursor_value: str
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO reconciliation_cursor (account_id, entity, cursor_value)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE cursor_value = VALUES(cursor_value), updated_at = NOW()
                """,
                (account_id, entity, cursor_value),
            )

    async def fetch_reconciliation_cursor(
        self, conn: Any, account_id: int, entity: str
    ) -> str | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT cursor_value
                FROM reconciliation_cursor
                WHERE account_id = %s AND entity = %s
                LIMIT 1
                """,
                (account_id, entity),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return str(row[0])

    async def list_orders(self, conn: Any, account_id: int, open_only: bool) -> list[dict[str, Any]]:
        status_filter = "AND status IN ('PENDING_SUBMIT','SUBMITTED','PARTIALLY_FILLED')" if open_only else ""
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, symbol, side, order_type, status, magic_id, position_id, reason,
                       client_order_id, exchange_order_id, qty, price, filled_qty, avg_fill_price,
                       created_at, updated_at, closed_at
                FROM position_orders
                WHERE account_id = %s {status_filter}
                ORDER BY id DESC
                LIMIT 500
                """,
                (account_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "symbol": r[1],
                    "side": r[2],
                    "order_type": r[3],
                    "status": r[4],
                    "magic_id": int(r[5]),
                    "position_id": int(r[6]),
                    "reason": r[7],
                    "client_order_id": r[8],
                    "exchange_order_id": r[9],
                    "qty": str(r[10]),
                    "price": None if r[11] is None else str(r[11]),
                    "filled_qty": str(r[12]),
                    "avg_fill_price": None if r[13] is None else str(r[13]),
                    "created_at": str(r[14]),
                    "updated_at": str(r[15]),
                    "closed_at": None if r[16] is None else str(r[16]),
                }
            )
        return out

    async def list_deals(self, conn: Any, account_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, order_id, position_id, symbol, side, qty, price, fee, fee_currency,
                       pnl, magic_id, reason, reconciled, exchange_trade_id, created_at, executed_at
                FROM position_deals
                WHERE account_id = %s
                ORDER BY id DESC
                LIMIT 1000
                """,
                (account_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "order_id": None if r[1] is None else int(r[1]),
                    "position_id": int(r[2]),
                    "symbol": r[3],
                    "side": r[4],
                    "qty": str(r[5]),
                    "price": str(r[6]),
                    "fee": None if r[7] is None else str(r[7]),
                    "fee_currency": r[8],
                    "pnl": None if r[9] is None else str(r[9]),
                    "magic_id": int(r[10]),
                    "reason": r[11],
                    "reconciled": bool(r[12]),
                    "exchange_trade_id": r[13],
                    "created_at": str(r[14]),
                    "executed_at": str(r[15]),
                }
            )
        return out

    async def list_positions(self, conn: Any, account_id: int, open_only: bool) -> list[dict[str, Any]]:
        state_filter = "AND state = 'open'" if open_only else ""
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, symbol, side, qty, avg_price, state, reason, opened_at, updated_at, closed_at
                FROM position_positions
                WHERE account_id = %s {state_filter}
                ORDER BY id DESC
                LIMIT 500
                """,
                (account_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "symbol": r[1],
                    "side": r[2],
                    "qty": str(r[3]),
                    "avg_price": str(r[4]),
                    "state": r[5],
                    "reason": r[6],
                    "opened_at": str(r[7]),
                    "updated_at": str(r[8]),
                    "closed_at": None if r[9] is None else str(r[9]),
                }
            )
        return out

    async def reassign_deals(
        self,
        conn: Any,
        account_id: int,
        deal_ids: list[int],
        target_magic_id: int,
        target_position_id: int,
    ) -> int:
        if not deal_ids:
            return 0
        placeholders = ",".join(["%s"] * len(deal_ids))
        params: list[Any] = [target_magic_id, target_position_id, account_id, *deal_ids]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE position_deals
                SET magic_id = %s,
                    position_id = %s,
                    reconciled = TRUE
                WHERE account_id = %s
                  AND id IN ({placeholders})
                """,
                params,
            )
            return int(cur.rowcount or 0)

    async def reassign_orders(
        self,
        conn: Any,
        account_id: int,
        order_ids: list[int],
        target_magic_id: int,
        target_position_id: int,
    ) -> int:
        if not order_ids:
            return 0
        placeholders = ",".join(["%s"] * len(order_ids))
        params: list[Any] = [target_magic_id, target_position_id, account_id, *order_ids]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE position_orders
                SET magic_id = %s,
                    position_id = %s
                WHERE account_id = %s
                  AND id IN ({placeholders})
                """,
                params,
            )
            return int(cur.rowcount or 0)

    async def insert_event(
        self, conn: Any, account_id: int, namespace: str, event_type: str, payload: dict[str, Any]
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO event_outbox (account_id, namespace, event_type, payload_json, delivered)
                VALUES (%s, %s, %s, %s, FALSE)
                """,
                (account_id, namespace, event_type, payload),
            )
