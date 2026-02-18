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
                    account_id, symbol, side, order_type, status,
                    magic_id, position_id, reason, client_order_id,
                    qty, price
                ) VALUES (
                    %s, %s, %s, %s, 'PENDING_SUBMIT',
                    %s, %s, %s, %s,
                    %s, %s
                )
                """,
                (
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

