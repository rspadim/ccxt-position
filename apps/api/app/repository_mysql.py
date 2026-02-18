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
    ) -> tuple[int, str, str, str] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, symbol, side, qty
                FROM position_positions
                WHERE id = %s AND account_id = %s AND state = 'open'
                LIMIT 1
                """,
                (position_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1]), str(row[2]).lower(), str(row[3])

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
    ) -> tuple[int, int, int] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, command_id, account_id
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
        return int(row[0]), int(row[1]), int(row[2])

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
