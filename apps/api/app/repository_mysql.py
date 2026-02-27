import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def _json_param(value: dict[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _json_column(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return json.loads(value.decode("utf-8"))
        except Exception:
            return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    return {}


class MySQLCommandRepository:
    def __init__(self, event_sink: Any | None = None) -> None:
        self._event_sink = event_sink

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

    async def create_account(
        self,
        conn: Any,
        exchange_id: str,
        label: str,
        position_mode: str,
        is_testnet: bool,
        pool_id: int = 0,
        extra_config_json: dict[str, Any] | None = None,
    ) -> int:
        effective_pool_id = 0 if int(pool_id) < 0 else int(pool_id)
        extra_config_db = None
        if extra_config_json is not None:
            extra_config_db = _json_param(extra_config_json)
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO accounts (
                    exchange_id, is_testnet, label, position_mode, extra_config_json, pool_id, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                """,
                (exchange_id, bool(is_testnet), label, position_mode, extra_config_db, effective_pool_id),
            )
            return int(cur.lastrowid)

    async def create_user(self, conn: Any, name: str, role: str = "trader") -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO users (name, role, status)
                VALUES (%s, %s, 'active')
                """,
                (name, role),
            )
            return int(cur.lastrowid)

    async def fetch_user_by_name(self, conn: Any, name: str) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, name, role, status
                FROM users
                WHERE name = %s
                LIMIT 1
                """,
                (name,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "user_id": int(row[0]),
            "user_name": str(row[1]),
            "role": str(row[2]),
            "status": str(row[3]),
        }

    async def fetch_user_by_id(self, conn: Any, user_id: int) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, name, role, status
                FROM users
                WHERE id = %s
                LIMIT 1
                """,
                (user_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "user_id": int(row[0]),
            "user_name": str(row[1]),
            "role": str(row[2]),
            "status": str(row[3]),
        }

    async def update_user_name(self, conn: Any, user_id: int, user_name: str) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE users
                SET name = %s
                WHERE id = %s
                """,
                (user_name, user_id),
            )
            return int(cur.rowcount or 0)

    async def set_user_password_hash(self, conn: Any, user_id: int, password_hash: str) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO user_password_credentials (user_id, password_hash)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    password_hash = VALUES(password_hash),
                    updated_at = NOW()
                """,
                (user_id, password_hash),
            )
            return int(cur.rowcount or 0)

    async def fetch_user_password_hash(self, conn: Any, user_id: int) -> str | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT password_hash
                FROM user_password_credentials
                WHERE user_id = %s
                LIMIT 1
                """,
                (user_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return str(row[0])

    async def create_api_key(self, conn: Any, user_id: int, api_key_hash: str, label: str | None = None) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO user_api_keys (user_id, label, api_key_hash, status)
                VALUES (%s, %s, %s, 'active')
                """,
                (user_id, label, api_key_hash),
            )
            return int(cur.lastrowid)

    async def list_active_api_keys_for_user(self, conn: Any, user_id: int) -> list[int]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM user_api_keys
                WHERE user_id = %s
                  AND status = 'active'
                ORDER BY id ASC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
        return [int(row[0]) for row in rows]

    async def list_api_keys_for_user(self, conn: Any, user_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT k.id, u.id, u.name, u.role, k.status, k.label, k.created_at
                FROM user_api_keys k
                JOIN users u ON u.id = k.user_id
                WHERE k.user_id = %s
                ORDER BY k.id ASC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "api_key_id": int(row[0]),
                    "user_id": int(row[1]),
                    "user_name": str(row[2]),
                    "role": str(row[3]),
                    "status": str(row[4]),
                    "label": str(row[5]) if row[5] is not None else "",
                    "created_at": str(row[6]),
                }
            )
        return out

    async def fetch_api_key_owner(self, conn: Any, api_key_id: int) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT k.id, u.id, u.name, u.role, u.status, k.status, k.label
                FROM user_api_keys k
                JOIN users u ON u.id = k.user_id
                WHERE k.id = %s
                LIMIT 1
                """,
                (api_key_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "api_key_id": int(row[0]),
            "user_id": int(row[1]),
            "user_name": str(row[2]),
            "role": str(row[3]),
            "user_status": str(row[4]),
            "api_key_status": str(row[5]),
            "label": str(row[6]) if row[6] is not None else "",
        }

    async def create_auth_token(
        self,
        conn: Any,
        user_id: int,
        api_key_id: int,
        token_hash: str,
        expires_at: str | None,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO auth_tokens (user_id, api_key_id, token_hash, status, expires_at)
                VALUES (%s, %s, %s, 'active', %s)
                """,
                (user_id, api_key_id, token_hash, expires_at),
            )
            return int(cur.lastrowid)

    async def upsert_user_account_permissions(
        self,
        conn: Any,
        user_id: int,
        account_id: int,
        can_read: bool,
        can_trade: bool,
        can_risk_manage: bool,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO user_account_permissions (
                    user_id, account_id, can_read, can_trade, can_risk_manage
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    can_read = VALUES(can_read),
                    can_trade = VALUES(can_trade),
                    can_risk_manage = VALUES(can_risk_manage)
                """,
                (user_id, account_id, bool(can_read), bool(can_trade), bool(can_risk_manage)),
            )
            return int(cur.rowcount or 0)

    async def upsert_api_key_account_permissions(
        self,
        conn: Any,
        api_key_id: int,
        account_id: int,
        *,
        can_read: bool,
        can_trade: bool,
        can_close_position: bool,
        can_risk_manage: bool,
        can_block_new_positions: bool,
        can_block_account: bool,
        restrict_to_strategies: bool,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO api_key_account_permissions (
                    api_key_id, account_id, can_read, can_trade, can_close_position,
                    can_risk_manage, can_block_new_positions, can_block_account,
                    restrict_to_strategies, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'active')
                ON DUPLICATE KEY UPDATE
                    can_read = VALUES(can_read),
                    can_trade = VALUES(can_trade),
                    can_close_position = VALUES(can_close_position),
                    can_risk_manage = VALUES(can_risk_manage),
                    can_block_new_positions = VALUES(can_block_new_positions),
                    can_block_account = VALUES(can_block_account),
                    restrict_to_strategies = VALUES(restrict_to_strategies),
                    status = 'active'
                """,
                (
                    api_key_id,
                    account_id,
                    bool(can_read),
                    bool(can_trade),
                    bool(can_close_position),
                    bool(can_risk_manage),
                    bool(can_block_new_positions),
                    bool(can_block_account),
                    bool(restrict_to_strategies),
                ),
            )
            return int(cur.rowcount or 0)

    async def upsert_api_key_strategy_permissions(
        self,
        conn: Any,
        api_key_id: int,
        account_id: int,
        strategy_id: int,
        can_read: bool,
        can_trade: bool,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO api_key_strategy_permissions (
                    api_key_id, account_id, strategy_id, can_read, can_trade, status
                ) VALUES (%s, %s, %s, %s, %s, 'active')
                ON DUPLICATE KEY UPDATE
                    can_read = VALUES(can_read),
                    can_trade = VALUES(can_trade),
                    status = 'active'
                """,
                (api_key_id, account_id, strategy_id, bool(can_read), bool(can_trade)),
            )
            return int(cur.rowcount or 0)

    async def create_strategy(
        self, conn: Any, name: str, client_strategy_id: int | None = None
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO strategies (name, client_strategy_id, status)
                VALUES (%s, %s, 'active')
                """,
                (name, client_strategy_id),
            )
            return int(cur.lastrowid)

    async def link_strategy_to_account(self, conn: Any, strategy_id: int, account_id: int) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO strategy_accounts (strategy_id, account_id, status)
                VALUES (%s, %s, 'active')
                ON DUPLICATE KEY UPDATE status = 'active'
                """,
                (strategy_id, account_id),
            )
            return int(cur.rowcount or 0)

    async def sync_strategy_accounts(
        self, conn: Any, strategy_id: int, account_ids: list[int]
    ) -> int:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        changed_rows = 0
        async with conn.cursor() as cur:
            if normalized_ids:
                placeholders = ",".join(["%s"] * len(normalized_ids))
                await cur.execute(
                    f"""
                    UPDATE strategy_accounts
                    SET status = 'disabled'
                    WHERE strategy_id = %s
                      AND account_id NOT IN ({placeholders})
                      AND status <> 'disabled'
                    """,
                    (strategy_id, *normalized_ids),
                )
                changed_rows += int(cur.rowcount or 0)
            else:
                await cur.execute(
                    """
                    UPDATE strategy_accounts
                    SET status = 'disabled'
                    WHERE strategy_id = %s
                      AND status <> 'disabled'
                    """,
                    (strategy_id,),
                )
                changed_rows += int(cur.rowcount or 0)

            for aid in normalized_ids:
                await cur.execute(
                    """
                    INSERT INTO strategy_accounts (strategy_id, account_id, status)
                    VALUES (%s, %s, 'active')
                    ON DUPLICATE KEY UPDATE status = 'active'
                    """,
                    (strategy_id, aid),
                )
                changed_rows += int(cur.rowcount or 0)
        return changed_rows

    async def list_strategies(self, conn: Any) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  s.id,
                  s.client_strategy_id,
                  s.name,
                  s.status,
                  COALESCE(GROUP_CONCAT(sa.account_id ORDER BY sa.account_id SEPARATOR ','), '') AS account_ids_csv
                FROM strategies s
                LEFT JOIN strategy_accounts sa
                  ON sa.strategy_id = s.id
                 AND sa.status = 'active'
                GROUP BY s.id, s.client_strategy_id, s.name, s.status
                ORDER BY s.id ASC
                """
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            csv = str(row[4] or "")
            account_ids = [int(x) for x in csv.split(",") if x.strip().isdigit()]
            out.append(
                {
                    "strategy_id": int(row[0]),
                    "client_strategy_id": None if row[1] is None else int(row[1]),
                    "name": str(row[2]),
                    "status": str(row[3]),
                    "account_ids": account_ids,
                }
            )
        return out

    async def update_strategy(
        self,
        conn: Any,
        strategy_id: int,
        *,
        name: str | None = None,
        status: str | None = None,
        client_strategy_id: int | None = None,
        update_client_strategy_id: bool = False,
    ) -> int:
        sets: list[str] = []
        params: list[Any] = []
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        if status is not None:
            sets.append("status = %s")
            params.append(status)
        if update_client_strategy_id:
            sets.append("client_strategy_id = %s")
            params.append(client_strategy_id)
        if not sets:
            return 0
        params.append(strategy_id)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE strategies
                SET {", ".join(sets)}
                WHERE id = %s
                """,
                tuple(params),
            )
            return int(cur.rowcount or 0)

    async def strategy_exists_for_account(self, conn: Any, account_id: int, strategy_id: int) -> bool:
        if int(strategy_id) == 0:
            return True
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT sa.strategy_id
                FROM strategy_accounts sa
                JOIN strategies s ON s.id = sa.strategy_id
                WHERE sa.account_id = %s
                  AND sa.strategy_id = %s
                  AND sa.status = 'active'
                  AND s.status = 'active'
                LIMIT 1
                """,
                (account_id, strategy_id),
            )
            row = await cur.fetchone()
        return row is not None

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

    async def fetch_api_key_account_permissions(
        self, conn: Any, api_key_id: int, account_id: int
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT can_read, can_trade, can_close_position, can_risk_manage,
                       can_block_new_positions, can_block_account, restrict_to_strategies
                FROM api_key_account_permissions
                WHERE api_key_id = %s
                  AND account_id = %s
                  AND status = 'active'
                LIMIT 1
                """,
                (api_key_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "can_read": bool(row[0]),
            "can_trade": bool(row[1]),
            "can_close_position": bool(row[2]),
            "can_risk_manage": bool(row[3]),
            "can_block_new_positions": bool(row[4]),
            "can_block_account": bool(row[5]),
            "restrict_to_strategies": bool(row[6]),
        }

    async def fetch_api_key_account_permissions_multi(
        self, conn: Any, api_key_id: int, account_ids: list[int]
    ) -> dict[int, dict[str, Any]]:
        normalized_ids = sorted({int(aid) for aid in account_ids if int(aid) > 0})
        if not normalized_ids:
            return {}
        placeholders = ",".join(["%s"] * len(normalized_ids))
        params: list[Any] = [int(api_key_id)]
        params.extend(normalized_ids)
        sql = f"""
            SELECT a.id,
                   a.status,
                   p.can_read,
                   p.can_trade,
                   p.can_close_position,
                   p.can_risk_manage,
                   p.can_block_new_positions,
                   p.can_block_account,
                   p.restrict_to_strategies
            FROM accounts a
            LEFT JOIN api_key_account_permissions p
              ON p.account_id = a.id
             AND p.api_key_id = %s
             AND p.status = 'active'
            WHERE a.id IN ({placeholders})
        """
        out: dict[int, dict[str, Any]] = {}
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(params))
            rows = await cur.fetchall()
        for row in rows or []:
            aid = int(row[0])
            account_status = str(row[1] or "")
            out[aid] = {
                "account_status": account_status,
                "can_read": bool(row[2]) if row[2] is not None else False,
                "can_trade": bool(row[3]) if row[3] is not None else False,
                "can_close_position": bool(row[4]) if row[4] is not None else False,
                "can_risk_manage": bool(row[5]) if row[5] is not None else False,
                "can_block_new_positions": bool(row[6]) if row[6] is not None else False,
                "can_block_account": bool(row[7]) if row[7] is not None else False,
                "restrict_to_strategies": bool(row[8]) if row[8] is not None else False,
            }
        return out

    async def api_key_strategy_allowed(
        self, conn: Any, api_key_id: int, account_id: int, strategy_id: int, for_trade: bool
    ) -> bool:
        column = "can_trade" if for_trade else "can_read"
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT {column}
                FROM api_key_strategy_permissions
                WHERE api_key_id = %s
                  AND account_id = %s
                  AND strategy_id = %s
                  AND status = 'active'
                LIMIT 1
                """,
                (api_key_id, account_id, strategy_id),
            )
            row = await cur.fetchone()
        if row is None:
            return False
        return bool(row[0])

    async def fetch_account_by_id(self, conn: Any, account_id: int) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, exchange_id, is_testnet, pool_id, status,
                       reconcile_enabled,
                       reconcile_short_interval_seconds, reconcile_short_lookback_seconds,
                       reconcile_hourly_interval_seconds, reconcile_hourly_lookback_seconds,
                       reconcile_long_interval_seconds, reconcile_long_lookback_seconds,
                       dispatcher_worker_hint,
                       extra_config_json
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
            "is_testnet": bool(row[2]),
            "pool_id": int(row[3]),
            "status": str(row[4]),
            "reconcile_enabled": bool(row[5]),
            "reconcile_short_interval_seconds": None if row[6] is None else int(row[6]),
            "reconcile_short_lookback_seconds": None if row[7] is None else int(row[7]),
            "reconcile_hourly_interval_seconds": None if row[8] is None else int(row[8]),
            "reconcile_hourly_lookback_seconds": None if row[9] is None else int(row[9]),
            "reconcile_long_interval_seconds": None if row[10] is None else int(row[10]),
            "reconcile_long_lookback_seconds": None if row[11] is None else int(row[11]),
            "dispatcher_worker_hint": None if row[12] is None else int(row[12]),
            "extra_config_json": _json_column(row[13]),
        }

    async def fetch_account_dispatcher_worker_hint(self, conn: Any, account_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT dispatcher_worker_hint
                FROM accounts
                WHERE id = %s
                LIMIT 1
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    async def set_account_dispatcher_worker_hint(
        self, conn: Any, account_id: int, worker_hint: int
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE accounts
                SET dispatcher_worker_hint = %s,
                    dispatcher_hint_updated_at = NOW()
                WHERE id = %s
                """,
                (int(worker_hint), account_id),
            )
            return int(cur.rowcount or 0)

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
        if mode not in {"hedge", "netting", "strategy_netting"}:
            return "hedge"
        return mode

    async def list_active_accounts_by_pool(self, conn: Any, pool_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, exchange_id, is_testnet,
                       reconcile_enabled,
                       reconcile_short_interval_seconds, reconcile_short_lookback_seconds,
                       reconcile_hourly_interval_seconds, reconcile_hourly_lookback_seconds,
                       reconcile_long_interval_seconds, reconcile_long_lookback_seconds
                FROM accounts
                WHERE status = 'active' AND pool_id = %s
                ORDER BY id
                """,
                (pool_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "exchange_id": str(r[1]),
                    "is_testnet": bool(r[2]),
                    "reconcile_enabled": bool(r[3]),
                    "reconcile_short_interval_seconds": None if r[4] is None else int(r[4]),
                    "reconcile_short_lookback_seconds": None if r[5] is None else int(r[5]),
                    "reconcile_hourly_interval_seconds": None if r[6] is None else int(r[6]),
                    "reconcile_hourly_lookback_seconds": None if r[7] is None else int(r[7]),
                    "reconcile_long_interval_seconds": None if r[8] is None else int(r[8]),
                    "reconcile_long_lookback_seconds": None if r[9] is None else int(r[9]),
                }
            )
        return out

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

    async def fetch_allow_new_positions_for_strategy(
        self, conn: Any, account_id: int, strategy_id: int
    ) -> bool:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT allow_new_positions
                FROM account_strategy_risk_state
                WHERE account_id = %s AND strategy_id = %s
                LIMIT 1
                """,
                (account_id, strategy_id),
            )
            row = await cur.fetchone()
        if row is None:
            return True
        return bool(row[0])

    async def set_allow_new_positions(self, conn: Any, account_id: int, allow_new_positions: bool) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO account_risk_state (account_id, allow_new_positions)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    allow_new_positions = VALUES(allow_new_positions),
                    updated_at = NOW()
                """,
                (account_id, bool(allow_new_positions)),
            )
            return int(cur.rowcount or 0)

    async def set_allow_new_positions_for_strategy(
        self, conn: Any, account_id: int, strategy_id: int, allow_new_positions: bool
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO account_strategy_risk_state (account_id, strategy_id, allow_new_positions)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    allow_new_positions = VALUES(allow_new_positions),
                    updated_at = NOW()
                """,
                (account_id, strategy_id, bool(allow_new_positions)),
            )
            return int(cur.rowcount or 0)

    async def set_account_status(self, conn: Any, account_id: int, status: str) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE accounts
                SET status = %s
                WHERE id = %s
                """,
                (status, account_id),
            )
            return int(cur.rowcount or 0)

    async def position_exists_open(
        self, conn: Any, account_id: int, position_id: int, symbol: str
    ) -> bool:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM oms_positions
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
        payload_json = _json_param(payload)
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO oms_commands (account_id, command_type, request_id, payload_json, status)
                VALUES (%s, %s, %s, %s, 'accepted')
                """,
                (account_id, command_type, request_id, payload_json),
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
        strategy_id: int,
        position_id: int,
        reason: str,
        comment: str | None,
        client_order_id: str | None,
        qty: Any,
        price: Any,
        stop_loss: Any,
        stop_gain: Any,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO oms_orders (
                    command_id, account_id, symbol, side, order_type, status,
                    strategy_id, position_id, reason, comment, client_order_id,
                    qty, price, stop_loss, stop_gain
                ) VALUES (
                    %s, %s, %s, %s, %s, 'PENDING_SUBMIT',
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """,
                (
                    command_id,
                    account_id,
                    symbol,
                    side,
                    order_type,
                    strategy_id,
                    position_id,
                    reason,
                    comment,
                    client_order_id,
                    qty,
                    price,
                    stop_loss,
                    stop_gain,
                ),
            )
            return int(cur.lastrowid)

    async def fetch_open_position(
        self, conn: Any, account_id: int, position_id: int
    ) -> tuple[int, str, int, str, str, str] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, symbol, strategy_id, side, qty, avg_price
                FROM oms_positions
                WHERE id = %s AND account_id = %s AND state IN ('open', 'close_requested')
                LIMIT 1
                """,
                (position_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1]), int(row[2]), str(row[3]).lower(), str(row[4]), str(row[5])

    async def fetch_order_for_update(
        self, conn: Any, account_id: int, order_id: int
    ) -> tuple[int, str, str] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, status, order_type
                FROM oms_orders
                WHERE id = %s AND account_id = %s
                LIMIT 1
                """,
                (order_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0]), str(row[1]), str(row[2]).lower()

    async def fetch_order_strategy_id(self, conn: Any, account_id: int, order_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT strategy_id
                FROM oms_orders
                WHERE id = %s AND account_id = %s
                LIMIT 1
                """,
                (order_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    async def fetch_order_account_id(self, conn: Any, order_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT account_id
                FROM oms_orders
                WHERE id = %s
                LIMIT 1
                """,
                (order_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    async def fetch_position_strategy_id(self, conn: Any, account_id: int, position_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT strategy_id
                FROM oms_positions
                WHERE id = %s AND account_id = %s
                LIMIT 1
                """,
                (position_id, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    async def fetch_position_account_id(self, conn: Any, position_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT account_id
                FROM oms_positions
                WHERE id = %s
                LIMIT 1
                """,
                (position_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    async def cleanup_expired_close_locks(self, conn: Any, position_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM oms_close_locks
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
                    INSERT INTO oms_close_locks (
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

    async def fetch_command_for_worker(
        self, conn: Any, command_id: int
    ) -> tuple[int, str, dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT account_id, command_type, payload_json
                FROM oms_commands
                WHERE id = %s
                LIMIT 1
                """,
                (command_id,),
            )
            row = await cur.fetchone()
        if row is None:
            raise ValueError("command_not_found")
        payload = _json_column(row[2])
        return int(row[0]), str(row[1]), payload

    async def mark_command_completed(self, conn: Any, command_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_commands
                SET status = 'completed', updated_at = NOW()
                WHERE id = %s
                """,
                (command_id,),
            )

    async def mark_command_failed(self, conn: Any, command_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_commands
                SET status = 'failed', updated_at = NOW()
                WHERE id = %s
                """,
                (command_id,),
            )

    async def fetch_order_id_by_command_id(self, conn: Any, command_id: int) -> int | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM oms_orders
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
                UPDATE oms_orders
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
                UPDATE oms_orders
                SET status = 'SUBMITTED',
                    exchange_order_id = %s,
                    closed_at = NULL,
                    edit_replace_state = NULL,
                    edit_replace_orphan_order_id = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (exchange_order_id, order_id),
            )

    async def mark_order_submitted_exchange_with_values(
        self,
        conn: Any,
        order_id: int,
        exchange_order_id: str | None,
        qty: Any,
        price: Any,
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET status = 'SUBMITTED',
                    exchange_order_id = %s,
                    qty = %s,
                    price = %s,
                    closed_at = NULL,
                    edit_replace_state = 'consolidated_to_self',
                    edit_replace_at = NOW(),
                    edit_replace_orphan_order_id = NULL,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (exchange_order_id, qty, price, order_id),
            )

    async def mark_order_rejected(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET status = 'REJECTED', updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def mark_order_canceled(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET status = 'CANCELED', closed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def mark_order_canceled_edit_pending(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET status = 'CANCELED',
                    closed_at = NOW(),
                    edit_replace_state = 'create_pending',
                    edit_replace_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def mark_order_edit_replace_failed(self, conn: Any, order_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET edit_replace_state = 'create_failed',
                    edit_replace_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (order_id,),
            )

    async def mark_order_consolidated_to_orphan(
        self, conn: Any, order_id: int, orphan_order_id: int
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET status = 'CANCELED',
                    closed_at = COALESCE(closed_at, NOW()),
                    edit_replace_state = 'consolidated_to_orphan',
                    edit_replace_at = NOW(),
                    edit_replace_orphan_order_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (int(orphan_order_id), int(order_id)),
            )

    async def adopt_external_orphan_order(
        self,
        conn: Any,
        orphan_order_id: int,
        *,
        origin_order_id: int,
        strategy_id: int,
        reason: str,
        comment: str | None,
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET strategy_id = %s,
                    reason = %s,
                    comment = %s,
                    edit_replace_origin_order_id = %s,
                    edit_replace_state = 'consolidated_to_orphan',
                    edit_replace_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    int(strategy_id),
                    str(reason),
                    comment,
                    int(origin_order_id),
                    int(orphan_order_id),
                ),
            )

    async def update_order_position_link(self, conn: Any, order_id: int, position_id: int) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET position_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (int(position_id), int(order_id)),
            )
            return int(cur.rowcount or 0)

    async def fetch_account_exchange_credentials(
        self, conn: Any, account_id: int
    ) -> tuple[str, bool, str | None, str | None, str | None, dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    a.exchange_id,
                    a.is_testnet,
                    c.api_key_enc,
                    c.secret_enc,
                    c.passphrase_enc,
                    a.extra_config_json
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
        return str(row[0]), bool(row[1]), row[2], row[3], row[4], _json_column(row[5])

    async def set_account_testnet(self, conn: Any, account_id: int, is_testnet: bool) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE accounts
                SET is_testnet = %s
                WHERE id = %s
                """,
                (is_testnet, account_id),
            )
            return int(cur.rowcount or 0)

    async def fetch_order_by_id(
        self, conn: Any, account_id: int, order_id: int
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, symbol, side, order_type, status, qty, price, stop_loss, stop_gain, filled_qty,
                       strategy_id, position_id, reason, comment, client_order_id, exchange_order_id,
                       previous_position_id, edit_replace_state, edit_replace_at, edit_replace_orphan_order_id,
                       edit_replace_origin_order_id
                FROM oms_orders
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
            "stop_loss": row[7],
            "stop_gain": row[8],
            "filled_qty": row[9],
            "strategy_id": int(row[10]),
            "position_id": int(row[11]),
            "reason": str(row[12]),
            "comment": row[13],
            "client_order_id": row[14],
            "exchange_order_id": row[15],
            "previous_position_id": None if row[16] is None else int(row[16]),
            "edit_replace_state": None if row[17] is None else str(row[17]),
            "edit_replace_at": None if row[18] is None else str(row[18]),
            "edit_replace_orphan_order_id": None if row[19] is None else int(row[19]),
            "edit_replace_origin_order_id": None if row[20] is None else int(row[20]),
        }

    async def list_cancelable_orders(
        self, conn: Any, account_id: int, strategy_ids: list[int] | None = None
    ) -> list[dict[str, Any]]:
        params: list[Any] = [account_id]
        strategy_sql = ""
        if strategy_ids is not None and len(strategy_ids) > 0:
            placeholders = ",".join(["%s"] * len(strategy_ids))
            strategy_sql = f" AND strategy_id IN ({placeholders})"
            params.extend([int(x) for x in strategy_ids])
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, symbol, status, strategy_id, client_order_id, exchange_order_id
                FROM oms_orders
                WHERE account_id = %s
                  AND status IN ('PENDING_SUBMIT','SUBMITTED','PARTIALLY_FILLED')
                  {strategy_sql}
                ORDER BY id ASC
                """,
                tuple(params),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": int(row[0]),
                    "symbol": str(row[1]),
                    "status": str(row[2]),
                    "strategy_id": int(row[3]),
                    "client_order_id": None if row[4] is None else str(row[4]),
                    "exchange_order_id": None if row[5] is None else str(row[5]),
                }
            )
        return out

    async def fetch_order_for_command_send(self, conn: Any, command_id: int) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, account_id, symbol, side, order_type, qty, price, stop_loss, stop_gain, comment, client_order_id
                FROM oms_orders
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
            "stop_loss": row[7],
            "stop_gain": row[8],
            "comment": row[9],
            "client_order_id": row[10],
        }

    async def release_close_position_lock(self, conn: Any, position_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM oms_close_locks
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
        payload = _json_param(raw_json)
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT IGNORE INTO ccxt_orders_raw (
                    account_id, exchange_id, exchange_order_id, client_order_id, symbol, raw_json, fingerprint_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (account_id, exchange_id, exchange_order_id, client_order_id, symbol, payload, fingerprint),
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
    ) -> bool:
        payload = _json_param(raw_json)
        fingerprint = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT IGNORE INTO ccxt_trades_raw (
                    account_id, exchange_id, exchange_trade_id, exchange_order_id, symbol, raw_json, fingerprint_hash
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (account_id, exchange_id, exchange_trade_id, exchange_order_id, symbol, payload, fingerprint),
            )
            return int(cur.rowcount or 0) > 0

    async def list_ccxt_orders_raw(
        self,
        conn: Any,
        account_id: int,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, exchange_id, exchange_order_id, client_order_id, symbol, raw_json, observed_at
                FROM ccxt_orders_raw
                WHERE account_id = %s
                  AND observed_at >= %s
                  AND observed_at < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY id ASC
                """,
                (account_id, str(date_from), str(date_to)),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": account_id,
                    "exchange_id": str(r[1]),
                    "exchange_order_id": None if r[2] is None else str(r[2]),
                    "client_order_id": None if r[3] is None else str(r[3]),
                    "symbol": None if r[4] is None else str(r[4]),
                    "raw_json": _json_column(r[5]),
                    "observed_at": str(r[6]),
                }
            )
        return out

    async def list_ccxt_trades_raw(
        self,
        conn: Any,
        account_id: int,
        date_from: str,
        date_to: str,
    ) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, exchange_id, exchange_trade_id, exchange_order_id, symbol, raw_json, observed_at
                FROM ccxt_trades_raw
                WHERE account_id = %s
                  AND observed_at >= %s
                  AND observed_at < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY id ASC
                """,
                (account_id, str(date_from), str(date_to)),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": account_id,
                    "exchange_id": str(r[1]),
                    "exchange_trade_id": None if r[2] is None else str(r[2]),
                    "exchange_order_id": None if r[3] is None else str(r[3]),
                    "symbol": None if r[4] is None else str(r[4]),
                    "raw_json": _json_column(r[5]),
                    "observed_at": str(r[6]),
                }
            )
        return out

    async def count_ccxt_orders_raw_multi(
        self,
        conn: Any,
        account_ids: list[int],
        date_from: str,
        date_to: str,
    ) -> int:
        if not account_ids:
            return 0
        placeholders = ",".join(["%s"] * len(account_ids))
        params: list[Any] = [*account_ids, str(date_from), str(date_to)]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT COUNT(*)
                FROM ccxt_orders_raw
                WHERE account_id IN ({placeholders})
                  AND observed_at >= %s
                  AND observed_at < DATE_ADD(%s, INTERVAL 1 DAY)
                """,
                tuple(params),
            )
            row = await cur.fetchone()
        return int(row[0] or 0) if row is not None else 0

    async def list_ccxt_orders_raw_multi(
        self,
        conn: Any,
        account_ids: list[int],
        date_from: str,
        date_to: str,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        if not account_ids:
            return []
        placeholders = ",".join(["%s"] * len(account_ids))
        params: list[Any] = [*account_ids, str(date_from), str(date_to), int(limit), int(offset)]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, exchange_id, exchange_order_id, client_order_id, symbol, raw_json, observed_at
                FROM ccxt_orders_raw
                WHERE account_id IN ({placeholders})
                  AND observed_at >= %s
                  AND observed_at < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": int(r[1]),
                    "exchange_id": str(r[2]),
                    "exchange_order_id": None if r[3] is None else str(r[3]),
                    "client_order_id": None if r[4] is None else str(r[4]),
                    "symbol": None if r[5] is None else str(r[5]),
                    "raw_json": _json_column(r[6]),
                    "observed_at": str(r[7]),
                }
            )
        return out

    async def count_ccxt_trades_raw_multi(
        self,
        conn: Any,
        account_ids: list[int],
        date_from: str,
        date_to: str,
    ) -> int:
        if not account_ids:
            return 0
        placeholders = ",".join(["%s"] * len(account_ids))
        params: list[Any] = [*account_ids, str(date_from), str(date_to)]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT COUNT(*)
                FROM ccxt_trades_raw
                WHERE account_id IN ({placeholders})
                  AND observed_at >= %s
                  AND observed_at < DATE_ADD(%s, INTERVAL 1 DAY)
                """,
                tuple(params),
            )
            row = await cur.fetchone()
        return int(row[0] or 0) if row is not None else 0

    async def list_ccxt_trades_raw_multi(
        self,
        conn: Any,
        account_ids: list[int],
        date_from: str,
        date_to: str,
        *,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        if not account_ids:
            return []
        placeholders = ",".join(["%s"] * len(account_ids))
        params: list[Any] = [*account_ids, str(date_from), str(date_to), int(limit), int(offset)]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, exchange_id, exchange_trade_id, exchange_order_id, symbol, raw_json, observed_at
                FROM ccxt_trades_raw
                WHERE account_id IN ({placeholders})
                  AND observed_at >= %s
                  AND observed_at < DATE_ADD(%s, INTERVAL 1 DAY)
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": int(r[1]),
                    "exchange_id": str(r[2]),
                    "exchange_trade_id": None if r[3] is None else str(r[3]),
                    "exchange_order_id": None if r[4] is None else str(r[4]),
                    "symbol": None if r[5] is None else str(r[5]),
                    "raw_json": _json_column(r[6]),
                    "observed_at": str(r[7]),
                }
            )
        return out

    async def fetch_open_position_for_symbol(
        self, conn: Any, account_id: int, symbol: str, side: str
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side, strategy_id, stop_loss, stop_gain, comment
                FROM oms_positions
                WHERE account_id = %s AND symbol = %s AND side = %s AND state = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol, side),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "qty": row[1],
            "avg_price": row[2],
            "side": str(row[3]).lower(),
            "strategy_id": int(row[4]),
            "stop_loss": row[5],
            "stop_gain": row[6],
            "comment": row[7],
        }

    async def fetch_open_position_for_symbol_non_external(
        self, conn: Any, account_id: int, symbol: str, side: str
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side, strategy_id, stop_loss, stop_gain, comment
                FROM oms_positions
                WHERE account_id = %s
                  AND symbol = %s
                  AND side = %s
                  AND state = 'open'
                  AND reason <> 'external'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol, side),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "qty": row[1],
            "avg_price": row[2],
            "side": str(row[3]).lower(),
            "strategy_id": int(row[4]),
            "stop_loss": row[5],
            "stop_gain": row[6],
            "comment": row[7],
        }

    async def fetch_open_net_position_by_symbol(
        self, conn: Any, account_id: int, symbol: str
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side, strategy_id, stop_loss, stop_gain, comment
                FROM oms_positions
                WHERE account_id = %s AND symbol = %s AND state = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "qty": row[1],
            "avg_price": row[2],
            "side": str(row[3]).lower(),
            "strategy_id": int(row[4]),
            "stop_loss": row[5],
            "stop_gain": row[6],
            "comment": row[7],
        }

    async def fetch_open_net_position_by_symbol_non_external(
        self, conn: Any, account_id: int, symbol: str
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side, strategy_id, stop_loss, stop_gain, comment
                FROM oms_positions
                WHERE account_id = %s
                  AND symbol = %s
                  AND state = 'open'
                  AND reason <> 'external'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "qty": row[1],
            "avg_price": row[2],
            "side": str(row[3]).lower(),
            "strategy_id": int(row[4]),
            "stop_loss": row[5],
            "stop_gain": row[6],
            "comment": row[7],
        }

    async def fetch_open_strategy_net_position_by_symbol_strategy(
        self, conn: Any, account_id: int, symbol: str, strategy_id: int
    ) -> dict[str, Any] | None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, qty, avg_price, side, strategy_id, stop_loss, stop_gain, comment
                FROM oms_positions
                WHERE account_id = %s
                  AND symbol = %s
                  AND strategy_id = %s
                  AND state = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (account_id, symbol, strategy_id),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "qty": row[1],
            "avg_price": row[2],
            "side": str(row[3]).lower(),
            "strategy_id": int(row[4]),
            "stop_loss": row[5],
            "stop_gain": row[6],
            "comment": row[7],
        }

    async def create_position_open(
        self,
        conn: Any,
        account_id: int,
        symbol: str,
        strategy_id: int,
        side: str,
        qty: Any,
        avg_price: Any,
        stop_loss: Any = None,
        stop_gain: Any = None,
        comment: str | None = None,
        reason: str = "api",
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO oms_positions (
                    account_id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, state, reason, comment
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open', %s, %s)
                """,
                (account_id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, reason, comment),
            )
            return int(cur.lastrowid)

    async def update_position_open_qty_price(
        self, conn: Any, position_id: int, qty: Any, avg_price: Any
    ) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_positions
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
                UPDATE oms_positions
                SET state = 'closed', qty = 0, closed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (position_id,),
            )

    async def close_position_merged(self, conn: Any, position_id: int) -> None:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_positions
                SET state = 'merged', qty = 0, closed_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (position_id,),
            )

    async def mark_position_close_requested(
        self, conn: Any, account_id: int, position_id: int
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_positions
                SET state = 'close_requested',
                    updated_at = NOW()
                WHERE id = %s
                  AND account_id = %s
                  AND state = 'open'
                """,
                (position_id, account_id),
            )
            return int(cur.rowcount or 0)

    async def reopen_position_if_close_requested(
        self, conn: Any, account_id: int, position_id: int
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_positions
                SET state = 'open',
                    updated_at = NOW()
                WHERE id = %s
                  AND account_id = %s
                  AND state = 'close_requested'
                """,
                (position_id, account_id),
            )
            return int(cur.rowcount or 0)

    async def update_position_targets_comment(
        self,
        conn: Any,
        account_id: int,
        position_id: int,
        *,
        set_stop_loss: bool,
        stop_loss: Any,
        set_stop_gain: bool,
        stop_gain: Any,
        set_comment: bool,
        comment: str | None,
    ) -> int:
        sets: list[str] = []
        params: list[Any] = []
        if set_stop_loss:
            sets.append("stop_loss = %s")
            params.append(stop_loss)
        if set_stop_gain:
            sets.append("stop_gain = %s")
            params.append(stop_gain)
        if set_comment:
            sets.append("comment = %s")
            params.append(comment)
        if not sets:
            return 0
        sets.append("updated_at = NOW()")
        params.extend([account_id, position_id])
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE oms_positions
                SET {", ".join(sets)}
                WHERE account_id = %s
                  AND id = %s
                  AND state IN ('open', 'close_requested')
                """,
                tuple(params),
            )
            return int(cur.rowcount or 0)

    async def deal_exists_by_exchange_trade_id(
        self, conn: Any, account_id: int, exchange_trade_id: str | None
    ) -> bool:
        if not exchange_trade_id:
            return False
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id
                FROM oms_deals
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
        strategy_id: int,
        reason: str,
        comment: str | None,
        reconciled: bool,
        exchange_trade_id: str | None,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO oms_deals (
                    account_id, order_id, position_id, symbol, side, qty, price, fee, fee_currency, pnl,
                    strategy_id, reason, comment, reconciled, exchange_trade_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
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
                    strategy_id,
                    reason,
                    comment,
                    reconciled,
                    exchange_trade_id,
                ),
            )
            return int(cur.lastrowid)

    async def fetch_open_order_link(
        self,
        conn: Any,
        account_id: int,
        exchange_order_id: str | None,
        client_order_id: str | None,
    ) -> dict[str, Any] | None:
        if not exchange_order_id and not client_order_id:
            return None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, strategy_id, position_id, stop_loss, stop_gain, comment, reason
                FROM oms_orders
                WHERE account_id = %s
                  AND (
                    (%s IS NOT NULL AND %s <> '' AND exchange_order_id = %s)
                    OR
                    (%s IS NOT NULL AND %s <> '' AND client_order_id = %s)
                  )
                ORDER BY
                    CASE
                        WHEN %s IS NOT NULL AND %s <> '' AND exchange_order_id = %s THEN 0
                        ELSE 1
                    END,
                    id DESC
                LIMIT 1
                """,
                (
                    account_id,
                    exchange_order_id,
                    exchange_order_id,
                    exchange_order_id,
                    client_order_id,
                    client_order_id,
                    client_order_id,
                    exchange_order_id,
                    exchange_order_id,
                    exchange_order_id,
                ),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "strategy_id": int(row[1]),
            "position_id": int(row[2]),
            "stop_loss": row[3],
            "stop_gain": row[4],
            "comment": row[5],
            "reason": str(row[6]),
        }

    async def get_or_create_external_unmatched_order(
        self,
        conn: Any,
        account_id: int,
        *,
        symbol: str,
        side: str,
        exchange_order_id: str | None,
        client_order_id: str | None,
        qty: Any,
        price: Any,
    ) -> dict[str, Any]:
        link = await self.fetch_open_order_link(
            conn=conn,
            account_id=account_id,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        )
        if link is not None:
            return link

        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO oms_orders (
                    command_id, account_id, symbol, side, order_type, status,
                    strategy_id, position_id, reason, comment, client_order_id, exchange_order_id,
                    qty, price, stop_loss, stop_gain, filled_qty, avg_fill_price, reconciled
                ) VALUES (
                    NULL, %s, %s, %s, 'market', 'FILLED',
                    0, 0, 'external', NULL, %s, %s,
                    %s, %s, NULL, NULL, %s, %s, FALSE
                )
                """,
                (
                    int(account_id),
                    str(symbol),
                    str(side),
                    client_order_id,
                    exchange_order_id,
                    qty,
                    price,
                    qty,
                    price,
                ),
            )
            new_id = int(cur.lastrowid)
        return {
            "id": new_id,
            "strategy_id": 0,
            "position_id": 0,
            "stop_loss": None,
            "stop_gain": None,
            "comment": None,
            "reason": "external",
        }

    async def find_external_orphan_order_for_replace(
        self,
        conn: Any,
        *,
        account_id: int,
        exchange_order_id: str | None,
        client_order_id: str | None,
        symbol: str,
        side: str,
    ) -> dict[str, Any] | None:
        if not exchange_order_id and not client_order_id:
            return None
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, strategy_id, position_id, reason, comment, client_order_id, exchange_order_id
                FROM oms_orders
                WHERE account_id = %s
                  AND reason = 'external'
                  AND strategy_id = 0
                  AND symbol = %s
                  AND LOWER(side) = LOWER(%s)
                  AND (
                    (%s IS NOT NULL AND %s <> '' AND exchange_order_id = %s)
                    OR
                    (%s IS NOT NULL AND %s <> '' AND client_order_id = %s)
                  )
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    int(account_id),
                    str(symbol),
                    str(side),
                    exchange_order_id,
                    exchange_order_id,
                    exchange_order_id,
                    client_order_id,
                    client_order_id,
                    client_order_id,
                ),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "strategy_id": int(row[1]),
            "position_id": int(row[2]),
            "reason": str(row[3]),
            "comment": row[4],
            "client_order_id": None if row[5] is None else str(row[5]),
            "exchange_order_id": None if row[6] is None else str(row[6]),
        }

    async def fetch_outbox_events(
        self, conn: Any, account_id: int, from_event_id: int, limit: int = 100
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
                (account_id, from_event_id, limit),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = _json_column(row[3])
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

    async def fetch_outbox_tail_id(self, conn: Any, account_id: int) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT COALESCE(MAX(id), 0)
                FROM event_outbox
                WHERE account_id = %s
                """,
                (account_id,),
            )
            row = await cur.fetchone()
        if row is None:
            return 0
        return int(row[0] or 0)

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

    async def fetch_reconciliation_status_for_account(
        self, conn: Any, account_id: int, entity: str = "my_trades_since"
    ) -> dict[str, Any]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT a.id, rc.cursor_value, rc.updated_at
                FROM accounts a
                LEFT JOIN reconciliation_cursor rc
                  ON rc.account_id = a.id
                 AND rc.entity = %s
                WHERE a.id = %s
                LIMIT 1
                """,
                (entity, account_id),
            )
            row = await cur.fetchone()
        if row is None:
            raise ValueError("account_not_found")
        updated_at = row[2]
        return {
            "account_id": int(row[0]),
            "cursor_value": None if row[1] is None else str(row[1]),
            "updated_at": updated_at,
        }

    async def list_reconciliation_status_for_user(
        self, conn: Any, user_id: int, entity: str = "my_trades_since"
    ) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT a.id, rc.cursor_value, rc.updated_at
                FROM accounts a
                JOIN user_account_permissions uap
                  ON uap.account_id = a.id
                 AND uap.user_id = %s
                 AND uap.can_read = TRUE
                LEFT JOIN reconciliation_cursor rc
                  ON rc.account_id = a.id
                 AND rc.entity = %s
                WHERE a.status = 'active'
                ORDER BY a.id ASC
                """,
                (user_id, entity),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "account_id": int(row[0]),
                    "cursor_value": None if row[1] is None else str(row[1]),
                    "updated_at": row[2],
                }
            )
        return out

    async def list_accounts_for_user(self, conn: Any, user_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT a.id, a.label, a.exchange_id, a.position_mode, a.is_testnet, a.status,
                       uap.can_read, uap.can_trade, uap.can_risk_manage,
                       COALESCE(ars.allow_new_positions, TRUE) AS allow_new_positions
                FROM accounts a
                JOIN user_account_permissions uap
                  ON uap.account_id = a.id
                 AND uap.user_id = %s
                LEFT JOIN account_risk_state ars
                  ON ars.account_id = a.id
                WHERE a.status = 'active' AND uap.can_read = TRUE
                ORDER BY a.id ASC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "account_id": int(row[0]),
                    "label": str(row[1]),
                    "exchange_id": str(row[2]),
                    "position_mode": str(row[3]),
                    "is_testnet": bool(row[4]),
                    "status": str(row[5]),
                    "can_read": bool(row[6]),
                    "can_trade": bool(row[7]),
                    "can_risk_manage": bool(row[8]),
                    "allow_new_positions": bool(row[9]),
                }
            )
        return out

    async def list_accounts_for_api_key(self, conn: Any, api_key_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT a.id, a.label, a.exchange_id, a.position_mode, a.is_testnet, a.status,
                       akap.can_read, akap.can_trade, akap.can_risk_manage,
                       COALESCE(ars.allow_new_positions, TRUE) AS allow_new_positions
                FROM accounts a
                JOIN api_key_account_permissions akap
                  ON akap.account_id = a.id
                 AND akap.api_key_id = %s
                 AND akap.status = 'active'
                LEFT JOIN account_risk_state ars
                  ON ars.account_id = a.id
                WHERE a.status = 'active' AND akap.can_read = TRUE
                ORDER BY a.id ASC
                """,
                (api_key_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "account_id": int(row[0]),
                    "label": str(row[1]),
                    "exchange_id": str(row[2]),
                    "position_mode": str(row[3]),
                    "is_testnet": bool(row[4]),
                    "status": str(row[5]),
                    "can_read": bool(row[6]),
                    "can_trade": bool(row[7]),
                    "can_risk_manage": bool(row[8]),
                    "allow_new_positions": bool(row[9]),
                }
            )
        return out

    async def list_strategy_risk_state_for_api_key(
        self, conn: Any, api_key_id: int, account_id: int
    ) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  sa.account_id,
                  s.id AS strategy_id,
                  s.name,
                  s.status,
                  COALESCE(asrs.allow_new_positions, TRUE) AS allow_new_positions
                FROM strategies s
                JOIN strategy_accounts sa
                  ON sa.strategy_id = s.id
                 AND sa.status = 'active'
                JOIN api_key_account_permissions akap
                  ON akap.account_id = sa.account_id
                 AND akap.api_key_id = %s
                 AND akap.status = 'active'
                 AND akap.can_read = TRUE
                LEFT JOIN api_key_strategy_permissions asp
                  ON asp.api_key_id = akap.api_key_id
                 AND asp.account_id = sa.account_id
                 AND asp.strategy_id = s.id
                 AND asp.status = 'active'
                 AND asp.can_read = TRUE
                LEFT JOIN account_strategy_risk_state asrs
                  ON asrs.account_id = sa.account_id
                 AND asrs.strategy_id = s.id
                WHERE sa.account_id = %s
                  AND (akap.restrict_to_strategies = FALSE OR asp.strategy_id IS NOT NULL)
                ORDER BY s.id ASC
                """,
                (api_key_id, account_id),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "account_id": int(row[0]),
                    "strategy_id": int(row[1]),
                    "name": str(row[2]),
                    "status": str(row[3]),
                    "allow_new_positions": bool(row[4]),
                }
            )
        return out

    async def list_accounts_admin(self, conn: Any) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    a.id,
                    a.label,
                    a.exchange_id,
                    a.position_mode,
                    a.extra_config_json,
                    a.is_testnet,
                    a.reconcile_enabled,
                    a.reconcile_short_interval_seconds,
                    a.reconcile_short_lookback_seconds,
                    a.reconcile_hourly_interval_seconds,
                    a.reconcile_hourly_lookback_seconds,
                    a.reconcile_long_interval_seconds,
                    a.reconcile_long_lookback_seconds,
                    a.dispatcher_worker_hint,
                    a.dispatcher_hint_updated_at,
                    a.raw_storage_mode,
                    a.pool_id,
                    a.status,
                    a.created_at,
                    c.api_key_enc,
                    c.secret_enc,
                    c.passphrase_enc,
                    c.updated_at
                FROM accounts a
                LEFT JOIN account_credentials_encrypted c
                    ON c.account_id = a.id
                ORDER BY a.id ASC
                """
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "account_id": int(row[0]),
                    "label": str(row[1]),
                    "exchange_id": str(row[2]),
                    "position_mode": str(row[3]),
                    "extra_config_json": _json_column(row[4]),
                    "is_testnet": bool(row[5]),
                    "reconcile_enabled": bool(row[6]),
                    "reconcile_short_interval_seconds": None if row[7] is None else int(row[7]),
                    "reconcile_short_lookback_seconds": None if row[8] is None else int(row[8]),
                    "reconcile_hourly_interval_seconds": None if row[9] is None else int(row[9]),
                    "reconcile_hourly_lookback_seconds": None if row[10] is None else int(row[10]),
                    "reconcile_long_interval_seconds": None if row[11] is None else int(row[11]),
                    "reconcile_long_lookback_seconds": None if row[12] is None else int(row[12]),
                    "dispatcher_worker_hint": None if row[13] is None else int(row[13]),
                    "dispatcher_hint_updated_at": None if row[14] is None else str(row[14]),
                    "raw_storage_mode": str(row[15]),
                    "pool_id": int(row[16]),
                    "status": str(row[17]),
                    "created_at": str(row[18]),
                    "api_key_enc": None if row[19] is None else str(row[19]),
                    "secret_enc": None if row[20] is None else str(row[20]),
                    "passphrase_enc": None if row[21] is None else str(row[21]),
                    "credentials_updated_at": None if row[22] is None else str(row[22]),
                }
            )
        return out

    async def update_account_admin(
        self,
        conn: Any,
        account_id: int,
        *,
        exchange_id: str | None = None,
        label: str | None = None,
        position_mode: str | None = None,
        is_testnet: bool | None = None,
        pool_id: int | None = None,
        status: str | None = None,
        extra_config_json: dict[str, Any] | None = None,
    ) -> int:
        sets: list[str] = []
        params: list[Any] = []
        if exchange_id is not None:
            sets.append("exchange_id = %s")
            params.append(exchange_id)
        if label is not None:
            sets.append("label = %s")
            params.append(label)
        if position_mode is not None:
            sets.append("position_mode = %s")
            params.append(position_mode)
        if is_testnet is not None:
            sets.append("is_testnet = %s")
            params.append(bool(is_testnet))
        if pool_id is not None:
            sets.append("pool_id = %s")
            params.append(0 if int(pool_id) < 0 else int(pool_id))
        if status is not None:
            sets.append("status = %s")
            params.append(status)
        if extra_config_json is not None:
            sets.append("extra_config_json = %s")
            params.append(_json_param(extra_config_json))
        if not sets:
            return 0
        params.append(account_id)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE accounts
                SET {", ".join(sets)}
                WHERE id = %s
                """,
                tuple(params),
            )
            return int(cur.rowcount or 0)

    async def delete_api_key_strategy_permissions(
        self, conn: Any, api_key_id: int, account_id: int
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM api_key_strategy_permissions
                WHERE api_key_id = %s
                  AND account_id = %s
                """,
                (api_key_id, account_id),
            )
            return int(cur.rowcount or 0)

    async def upsert_account_credentials(
        self,
        conn: Any,
        account_id: int,
        api_key_enc: str | None,
        secret_enc: str | None,
        passphrase_enc: str | None,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO account_credentials_encrypted (
                    account_id, api_key_enc, secret_enc, passphrase_enc
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    api_key_enc = VALUES(api_key_enc),
                    secret_enc = VALUES(secret_enc),
                    passphrase_enc = VALUES(passphrase_enc),
                    updated_at = NOW()
                """,
                (account_id, api_key_enc, secret_enc, passphrase_enc),
            )
            return int(cur.rowcount or 0)

    async def list_users_admin(self, conn: Any) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, name, role, status, created_at
                FROM users
                ORDER BY id ASC
                """
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "user_id": int(row[0]),
                    "user_name": str(row[1]),
                    "role": str(row[2]),
                    "status": str(row[3]),
                    "created_at": str(row[4]),
                }
            )
        return out

    async def list_users_api_keys_admin(self, conn: Any) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT u.id, u.name, u.role, u.status, k.id, k.status, k.label, k.created_at
                FROM users u
                JOIN user_api_keys k ON k.user_id = u.id
                ORDER BY u.id ASC, k.id ASC
                """
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "user_id": int(row[0]),
                    "user_name": str(row[1]),
                    "role": str(row[2]),
                    "user_status": str(row[3]),
                    "api_key_id": int(row[4]),
                    "api_key_status": str(row[5]),
                    "label": str(row[6]) if row[6] is not None else "",
                    "created_at": str(row[7]),
                }
            )
        return out

    async def list_api_key_permissions_admin(
        self, conn: Any, api_key_id: int
    ) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    akap.api_key_id,
                    akap.account_id,
                    akap.can_read,
                    akap.can_trade,
                    akap.can_close_position,
                    akap.can_risk_manage,
                    akap.can_block_new_positions,
                    akap.can_block_account,
                    akap.restrict_to_strategies,
                    akap.status,
                    k.label,
                    COALESCE(
                        GROUP_CONCAT(
                            DISTINCT CASE
                                WHEN asp.status = 'active' AND asp.can_read = TRUE THEN asp.strategy_id
                                ELSE NULL
                            END
                            ORDER BY asp.strategy_id SEPARATOR ','
                        ),
                        ''
                    ) AS strategy_ids_csv
                FROM api_key_account_permissions akap
                LEFT JOIN api_key_strategy_permissions asp
                  ON asp.api_key_id = akap.api_key_id
                 AND asp.account_id = akap.account_id
                LEFT JOIN user_api_keys k
                  ON k.id = akap.api_key_id
                WHERE akap.api_key_id = %s
                GROUP BY
                    akap.api_key_id,
                    akap.account_id,
                    akap.can_read,
                    akap.can_trade,
                    akap.can_close_position,
                    akap.can_risk_manage,
                    akap.can_block_new_positions,
                    akap.can_block_account,
                    akap.restrict_to_strategies,
                    akap.status
                ORDER BY akap.account_id ASC
                """,
                (api_key_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            label_value = str(row[10] or "")
            csv = str(row[11] or "")
            strategy_ids = [int(x) for x in csv.split(",") if x.strip().isdigit()]
            out.append(
                {
                    "api_key_id": int(row[0]),
                    "account_id": int(row[1]),
                    "can_read": bool(row[2]),
                    "can_trade": bool(row[3]),
                    "can_close_position": bool(row[4]),
                    "can_risk_manage": bool(row[5]),
                    "can_block_new_positions": bool(row[6]),
                    "can_block_account": bool(row[7]),
                    "restrict_to_strategies": bool(row[8]),
                    "status": str(row[9]),
                    "label": label_value,
                    "strategy_ids": strategy_ids,
                }
            )
        return out

    async def set_api_key_status(self, conn: Any, api_key_id: int, status: str) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE user_api_keys
                SET status = %s
                WHERE id = %s
                """,
                (status, api_key_id),
            )
            return int(cur.rowcount or 0)

    async def list_strategies_for_api_key(self, conn: Any, api_key_id: int) -> list[dict[str, Any]]:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                  s.id,
                  s.client_strategy_id,
                  s.name,
                  s.status,
                  COALESCE(GROUP_CONCAT(DISTINCT sa.account_id ORDER BY sa.account_id SEPARATOR ','), '') AS account_ids_csv
                FROM strategies s
                JOIN strategy_accounts sa
                  ON sa.strategy_id = s.id
                 AND sa.status = 'active'
                JOIN api_key_account_permissions akap
                  ON akap.account_id = sa.account_id
                 AND akap.api_key_id = %s
                 AND akap.status = 'active'
                 AND akap.can_read = TRUE
                LEFT JOIN api_key_strategy_permissions asp
                  ON asp.api_key_id = akap.api_key_id
                 AND asp.account_id = sa.account_id
                 AND asp.strategy_id = s.id
                 AND asp.status = 'active'
                 AND asp.can_read = TRUE
                WHERE (akap.restrict_to_strategies = FALSE OR asp.strategy_id IS NOT NULL)
                GROUP BY s.id, s.client_strategy_id, s.name, s.status
                ORDER BY
                  CASE WHEN s.status = 'active' THEN 0 ELSE 1 END ASC,
                  s.name ASC,
                  s.id ASC
                """,
                (api_key_id,),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            csv = str(row[4] or "")
            account_ids = [int(x) for x in csv.split(",") if x.strip().isdigit()]
            out.append(
                {
                    "strategy_id": int(row[0]),
                    "client_strategy_id": None if row[1] is None else int(row[1]),
                    "name": str(row[2]),
                    "status": str(row[3]),
                    "account_ids": account_ids,
                }
            )
        return out

    async def list_orders(
        self,
        conn: Any,
        account_id: int,
        open_only: bool,
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        open_limit: int = 500,
    ) -> list[dict[str, Any]]:
        status_filter = "AND status IN ('PENDING_SUBMIT','SUBMITTED','PARTIALLY_FILLED')" if open_only else ""
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        limit_clause = "LIMIT %s" if open_only else ""
        date_filter = ""
        params_list: list[Any] = [account_id]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if (not open_only) and date_from and date_to:
            date_filter = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        if open_only:
            params_list.append(max(1, min(5000, int(open_limit or 500))))
        params: tuple[Any, ...] = tuple(params_list)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, symbol, side, order_type, status, strategy_id, position_id, reason,
                       comment, client_order_id, exchange_order_id, qty, price, stop_loss, stop_gain, filled_qty, avg_fill_price,
                       created_at, updated_at, closed_at, command_id,
                       previous_position_id, edit_replace_state, edit_replace_at,
                       edit_replace_orphan_order_id, edit_replace_origin_order_id
                FROM oms_orders
                WHERE account_id = %s {status_filter} {strategy_filter} {date_filter}
                ORDER BY id ASC
                {limit_clause}
                """,
                params,
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": account_id,
                    "symbol": r[1],
                    "side": r[2],
                    "order_type": r[3],
                    "status": r[4],
                    "strategy_id": int(r[5]),
                    "position_id": int(r[6]),
                    "reason": r[7],
                    "comment": r[8],
                    "client_order_id": r[9],
                    "exchange_order_id": r[10],
                    "qty": str(r[11]),
                    "price": None if r[12] is None else str(r[12]),
                    "stop_loss": None if r[13] is None else str(r[13]),
                    "stop_gain": None if r[14] is None else str(r[14]),
                    "filled_qty": str(r[15]),
                    "avg_fill_price": None if r[16] is None else str(r[16]),
                    "created_at": str(r[17]),
                    "updated_at": str(r[18]),
                    "closed_at": None if r[19] is None else str(r[19]),
                    "command_id": None if r[20] is None else int(r[20]),
                    "previous_position_id": None if r[21] is None else int(r[21]),
                    "edit_replace_state": None if r[22] is None else str(r[22]),
                    "edit_replace_at": None if r[23] is None else str(r[23]),
                    "edit_replace_orphan_order_id": None if r[24] is None else int(r[24]),
                    "edit_replace_origin_order_id": None if r[25] is None else int(r[25]),
                }
            )
        return out

    async def list_orders_multi(
        self,
        conn: Any,
        account_ids: list[int],
        open_only: bool,
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        open_limit: int = 500,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not normalized_ids:
            return []
        placeholders = ",".join(["%s"] * len(normalized_ids))
        status_filter = "AND status IN ('PENDING_SUBMIT','SUBMITTED','PARTIALLY_FILLED')" if open_only else ""
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        limit_clause = "LIMIT %s" if open_only else ""
        date_filter = ""
        params_list: list[Any] = [*normalized_ids]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if (not open_only) and date_from and date_to:
            date_filter = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        if open_only:
            per_account_limit = max(1, min(5000, int(open_limit or 500)))
            params_list.append(per_account_limit)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, symbol, side, order_type, status, strategy_id, position_id, reason,
                       comment, client_order_id, exchange_order_id, qty, price, stop_loss, stop_gain, filled_qty, avg_fill_price,
                       created_at, updated_at, closed_at, command_id,
                       previous_position_id, edit_replace_state, edit_replace_at,
                       edit_replace_orphan_order_id, edit_replace_origin_order_id
                FROM oms_orders USE INDEX (idx_oms_orders_account_status_id)
                WHERE account_id IN ({placeholders}) {status_filter} {strategy_filter} {date_filter}
                ORDER BY id ASC
                {limit_clause}
                """,
                tuple(params_list),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "account_id": r[1],
                    "symbol": r[2],
                    "side": r[3],
                    "order_type": r[4],
                    "status": r[5],
                    "strategy_id": r[6],
                    "position_id": r[7],
                    "reason": r[8],
                    "comment": r[9],
                    "client_order_id": r[10],
                    "exchange_order_id": r[11],
                    "qty": r[12],
                    "price": r[13],
                    "stop_loss": r[14],
                    "stop_gain": r[15],
                    "filled_qty": r[16],
                    "avg_fill_price": r[17],
                    "created_at": r[18],
                    "updated_at": r[19],
                    "closed_at": r[20],
                    "command_id": r[21],
                    "previous_position_id": r[22],
                    "edit_replace_state": None if r[23] is None else str(r[23]),
                    "edit_replace_at": r[24],
                    "edit_replace_orphan_order_id": r[25],
                    "edit_replace_origin_order_id": r[26],
                }
            )
        return out

    async def list_orders_multi_paged(
        self,
        conn: Any,
        account_ids: list[int],
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not normalized_ids:
            return [], 0
        placeholders = ",".join(["%s"] * len(normalized_ids))
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        date_filter = ""
        params_list: list[Any] = [*normalized_ids]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if date_from and date_to:
            date_filter = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        safe_page = max(1, int(page or 1))
        safe_page_size = max(1, min(500, int(page_size or 100)))
        offset = (safe_page - 1) * safe_page_size
        count_sql = f"""
            SELECT COUNT(1)
            FROM oms_orders USE INDEX (idx_oms_orders_account_status_id)
            WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
        """
        data_sql = f"""
            SELECT id, account_id, symbol, side, order_type, status, strategy_id, position_id, reason,
                   comment, client_order_id, exchange_order_id, qty, price, stop_loss, stop_gain, filled_qty, avg_fill_price,
                   created_at, updated_at, closed_at, command_id,
                   previous_position_id, edit_replace_state, edit_replace_at,
                   edit_replace_orphan_order_id, edit_replace_origin_order_id
            FROM oms_orders USE INDEX (idx_oms_orders_account_status_id)
            WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
            ORDER BY id ASC
            LIMIT %s OFFSET %s
        """
        total = 0
        async with conn.cursor() as cur:
            await cur.execute(count_sql, tuple(params_list))
            row = await cur.fetchone()
            total = int((row[0] if row else 0) or 0)
            await cur.execute(data_sql, tuple([*params_list, safe_page_size, offset]))
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "account_id": r[1],
                    "symbol": r[2],
                    "side": r[3],
                    "order_type": r[4],
                    "status": r[5],
                    "strategy_id": r[6],
                    "position_id": r[7],
                    "reason": r[8],
                    "comment": r[9],
                    "client_order_id": r[10],
                    "exchange_order_id": r[11],
                    "qty": r[12],
                    "price": r[13],
                    "stop_loss": r[14],
                    "stop_gain": r[15],
                    "filled_qty": r[16],
                    "avg_fill_price": r[17],
                    "created_at": r[18],
                    "updated_at": r[19],
                    "closed_at": r[20],
                    "command_id": r[21],
                    "previous_position_id": r[22],
                    "edit_replace_state": None if r[23] is None else str(r[23]),
                    "edit_replace_at": r[24],
                    "edit_replace_orphan_order_id": r[25],
                    "edit_replace_origin_order_id": r[26],
                }
            )
        return out, total


    async def list_deals(
        self,
        conn: Any,
        account_id: int,
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        date_filter = ""
        params_list: list[Any] = [account_id]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if date_from and date_to:
            date_filter = "AND executed_at >= %s AND executed_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        params: tuple[Any, ...] = tuple(params_list)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, order_id, position_id, symbol, side, qty, price, fee, fee_currency,
                       pnl, strategy_id, reason, comment, reconciled, exchange_trade_id, created_at, executed_at,
                       previous_position_id
                FROM oms_deals
                WHERE account_id = %s {strategy_filter} {date_filter}
                ORDER BY id ASC
                """,
                params,
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": account_id,
                    "order_id": None if r[1] is None else int(r[1]),
                    "position_id": int(r[2]),
                    "symbol": r[3],
                    "side": r[4],
                    "qty": str(r[5]),
                    "price": str(r[6]),
                    "fee": None if r[7] is None else str(r[7]),
                    "fee_currency": r[8],
                    "pnl": None if r[9] is None else str(r[9]),
                    "strategy_id": int(r[10]),
                    "reason": r[11],
                    "comment": r[12],
                    "reconciled": bool(r[13]),
                    "exchange_trade_id": r[14],
                    "created_at": str(r[15]),
                    "executed_at": str(r[16]),
                    "previous_position_id": None if r[17] is None else int(r[17]),
                }
            )
        return out

    async def list_deals_multi(
        self,
        conn: Any,
        account_ids: list[int],
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not normalized_ids:
            return []
        placeholders = ",".join(["%s"] * len(normalized_ids))
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        date_filter = ""
        params_list: list[Any] = [*normalized_ids]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if date_from and date_to:
            date_filter = "AND executed_at >= %s AND executed_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, order_id, position_id, symbol, side, qty, price, fee, fee_currency,
                       pnl, strategy_id, reason, comment, reconciled, exchange_trade_id, created_at, executed_at,
                       previous_position_id
                FROM oms_deals USE INDEX (idx_oms_deals_account_executed_id)
                WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
                ORDER BY id ASC
                """,
                tuple(params_list),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "account_id": r[1],
                    "order_id": r[2],
                    "position_id": r[3],
                    "symbol": r[4],
                    "side": r[5],
                    "qty": r[6],
                    "price": r[7],
                    "fee": r[8],
                    "fee_currency": r[9],
                    "pnl": r[10],
                    "strategy_id": r[11],
                    "reason": r[12],
                    "comment": r[13],
                    "reconciled": bool(r[14]),
                    "exchange_trade_id": r[15],
                    "created_at": r[16],
                    "executed_at": r[17],
                    "previous_position_id": r[18],
                }
            )
        return out

    async def list_deals_multi_paged(
        self,
        conn: Any,
        account_ids: list[int],
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not normalized_ids:
            return [], 0
        placeholders = ",".join(["%s"] * len(normalized_ids))
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        date_filter = ""
        params_list: list[Any] = [*normalized_ids]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if date_from and date_to:
            date_filter = "AND executed_at >= %s AND executed_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        safe_page = max(1, int(page or 1))
        safe_page_size = max(1, min(500, int(page_size or 100)))
        offset = (safe_page - 1) * safe_page_size
        count_sql = f"""
            SELECT COUNT(1)
            FROM oms_deals USE INDEX (idx_oms_deals_account_executed_id)
            WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
        """
        data_sql = f"""
            SELECT id, account_id, order_id, position_id, symbol, side, qty, price, fee, fee_currency,
                   pnl, strategy_id, reason, comment, reconciled, exchange_trade_id, created_at, executed_at,
                   previous_position_id
            FROM oms_deals USE INDEX (idx_oms_deals_account_executed_id)
            WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
            ORDER BY id ASC
            LIMIT %s OFFSET %s
        """
        total = 0
        async with conn.cursor() as cur:
            await cur.execute(count_sql, tuple(params_list))
            row = await cur.fetchone()
            total = int((row[0] if row else 0) or 0)
            await cur.execute(data_sql, tuple([*params_list, safe_page_size, offset]))
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "account_id": r[1],
                    "order_id": r[2],
                    "position_id": r[3],
                    "symbol": r[4],
                    "side": r[5],
                    "qty": r[6],
                    "price": r[7],
                    "fee": r[8],
                    "fee_currency": r[9],
                    "pnl": r[10],
                    "strategy_id": r[11],
                    "reason": r[12],
                    "comment": r[13],
                    "reconciled": bool(r[14]),
                    "exchange_trade_id": r[15],
                    "created_at": r[16],
                    "executed_at": r[17],
                    "previous_position_id": r[18],
                }
            )
        return out, total


    async def list_positions(
        self,
        conn: Any,
        account_id: int,
        open_only: bool,
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        open_limit: int = 500,
    ) -> list[dict[str, Any]]:
        state_filter = "AND state IN ('open', 'close_requested')" if open_only else ""
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        limit_clause = "LIMIT %s" if open_only else ""
        date_filter = ""
        params_list: list[Any] = [account_id]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if (not open_only) and date_from and date_to:
            date_filter = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        if open_only:
            params_list.append(max(1, min(5000, int(open_limit or 500))))
        params: tuple[Any, ...] = tuple(params_list)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, state, reason, comment, opened_at, updated_at, closed_at
                FROM oms_positions
                WHERE account_id = %s {state_filter} {strategy_filter} {date_filter}
                ORDER BY id ASC
                {limit_clause}
                """,
                params,
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": account_id,
                    "symbol": r[1],
                    "strategy_id": int(r[2]),
                    "side": r[3],
                    "qty": str(r[4]),
                    "avg_price": str(r[5]),
                    "stop_loss": None if r[6] is None else str(r[6]),
                    "stop_gain": None if r[7] is None else str(r[7]),
                    "state": r[8],
                    "reason": r[9],
                    "comment": r[10],
                    "opened_at": str(r[11]),
                    "updated_at": str(r[12]),
                    "closed_at": None if r[13] is None else str(r[13]),
                }
            )
        return out

    async def list_positions_multi(
        self,
        conn: Any,
        account_ids: list[int],
        open_only: bool,
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        open_limit: int = 500,
    ) -> list[dict[str, Any]]:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not normalized_ids:
            return []
        placeholders = ",".join(["%s"] * len(normalized_ids))
        state_filter = "AND state IN ('open', 'close_requested')" if open_only else ""
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        limit_clause = "LIMIT %s" if open_only else ""
        date_filter = ""
        params_list: list[Any] = [*normalized_ids]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if (not open_only) and date_from and date_to:
            date_filter = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        if open_only:
            per_account_limit = max(1, min(5000, int(open_limit or 500)))
            params_list.append(per_account_limit)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, state, reason, comment, opened_at, updated_at, closed_at
                FROM oms_positions USE INDEX (idx_oms_positions_account_state_id)
                WHERE account_id IN ({placeholders}) {state_filter} {strategy_filter} {date_filter}
                ORDER BY id ASC
                {limit_clause}
                """,
                tuple(params_list),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "account_id": r[1],
                    "symbol": r[2],
                    "strategy_id": r[3],
                    "side": r[4],
                    "qty": r[5],
                    "avg_price": r[6],
                    "stop_loss": r[7],
                    "stop_gain": r[8],
                    "state": r[9],
                    "reason": r[10],
                    "comment": r[11],
                    "opened_at": r[12],
                    "updated_at": r[13],
                    "closed_at": r[14],
                }
            )
        return out

    async def list_positions_multi_paged(
        self,
        conn: Any,
        account_ids: list[int],
        strategy_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        normalized_ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not normalized_ids:
            return [], 0
        placeholders = ",".join(["%s"] * len(normalized_ids))
        strategy_filter = "AND strategy_id = %s" if strategy_id is not None else ""
        date_filter = ""
        params_list: list[Any] = [*normalized_ids]
        if strategy_id is not None:
            params_list.append(int(strategy_id))
        if date_from and date_to:
            date_filter = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params_list.append(str(date_from))
            params_list.append(str(date_to))
        safe_page = max(1, int(page or 1))
        safe_page_size = max(1, min(500, int(page_size or 100)))
        offset = (safe_page - 1) * safe_page_size
        count_sql = f"""
            SELECT COUNT(1)
            FROM oms_positions USE INDEX (idx_oms_positions_account_state_id)
            WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
        """
        data_sql = f"""
            SELECT id, account_id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, state, reason, comment, opened_at, updated_at, closed_at
            FROM oms_positions USE INDEX (idx_oms_positions_account_state_id)
            WHERE account_id IN ({placeholders}) {strategy_filter} {date_filter}
            ORDER BY id ASC
            LIMIT %s OFFSET %s
        """
        total = 0
        async with conn.cursor() as cur:
            await cur.execute(count_sql, tuple(params_list))
            row = await cur.fetchone()
            total = int((row[0] if row else 0) or 0)
            await cur.execute(data_sql, tuple([*params_list, safe_page_size, offset]))
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "account_id": r[1],
                    "symbol": r[2],
                    "strategy_id": r[3],
                    "side": r[4],
                    "qty": r[5],
                    "avg_price": r[6],
                    "stop_loss": r[7],
                    "stop_gain": r[8],
                    "state": r[9],
                    "reason": r[10],
                    "comment": r[11],
                    "opened_at": r[12],
                    "updated_at": r[13],
                    "closed_at": r[14],
                }
            )
        return out, total


    async def list_recent_symbols_for_account(
        self, conn: Any, account_id: int, limit: int = 20
    ) -> list[str]:
        lim = max(1, int(limit))
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT symbol FROM (
                    SELECT symbol, MAX(created_at) AS ts
                    FROM oms_orders
                    WHERE account_id = %s
                    GROUP BY symbol
                    UNION ALL
                    SELECT symbol, MAX(executed_at) AS ts
                    FROM oms_deals
                    WHERE account_id = %s
                    GROUP BY symbol
                ) s
                GROUP BY symbol
                ORDER BY MAX(ts) DESC
                LIMIT %s
                """,
                (account_id, account_id, lim),
            )
            rows = await cur.fetchall()
        return [str(r[0]) for r in rows if r and r[0]]

    async def reassign_deals(
        self,
        conn: Any,
        account_id: int,
        deal_ids: list[int],
        target_strategy_id: int,
        target_position_id: int,
    ) -> int:
        if not deal_ids:
            return 0
        placeholders = ",".join(["%s"] * len(deal_ids))
        params: list[Any] = [target_strategy_id, target_position_id, account_id, *deal_ids]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE oms_deals
                SET strategy_id = %s,
                    position_id = %s,
                    reconciled = TRUE
                WHERE account_id = %s
                  AND id IN ({placeholders})
                """,
                params,
            )
            return int(cur.rowcount or 0)

    async def list_reassign_deal_candidates(
        self,
        conn: Any,
        *,
        account_ids: list[int],
        deal_ids: list[int] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        reconciled: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        if not account_ids:
            return [], 0
        account_placeholders = ",".join(["%s"] * len(account_ids))
        where = [f"o.account_id IN ({account_placeholders})"]
        where.append("reconciled IN (TRUE, FALSE)")
        params: list[Any] = [*account_ids]
        if deal_ids:
            deal_placeholders = ",".join(["%s"] * len(deal_ids))
            where.append(f"id IN ({deal_placeholders})")
            params.extend([int(x) for x in deal_ids])
        if date_from:
            where.append("DATE(executed_at) >= %s")
            params.append(date_from)
        if date_to:
            where.append("DATE(executed_at) <= %s")
            params.append(date_to)
        if reconciled is not None:
            where.append("reconciled = %s")
            params.append(bool(reconciled))
        where_sql = " AND ".join(where)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT COUNT(1)
                FROM oms_deals
                WHERE {where_sql}
                """,
                tuple(params),
            )
            total_row = await cur.fetchone()
            total = int(total_row[0] or 0) if total_row else 0
            await cur.execute(
                f"""
                SELECT id, account_id, symbol, side, reconciled, strategy_id, position_id, executed_at, created_at, previous_position_id
                FROM oms_deals
                WHERE {where_sql}
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                (*params, int(limit), int(offset)),
            )
            rows = await cur.fetchall()
        items: list[dict[str, Any]] = []
        for r in rows:
            items.append(
                {
                    "kind": "deal",
                    "id": int(r[0]),
                    "account_id": int(r[1]),
                    "symbol": None if r[2] is None else str(r[2]),
                    "side": None if r[3] is None else str(r[3]),
                    "reconciled": bool(r[4]),
                    "strategy_id": None if r[5] is None else int(r[5]),
                    "position_id": None if r[6] is None else int(r[6]),
                    "executed_at": None if r[7] is None else str(r[7]),
                    "created_at": None if r[8] is None else str(r[8]),
                    "previous_position_id": None if r[9] is None else int(r[9]),
                }
            )
        return items, total

    async def reassign_orders(
        self,
        conn: Any,
        account_id: int,
        order_ids: list[int],
        target_strategy_id: int,
        target_position_id: int,
    ) -> int:
        if not order_ids:
            return 0
        placeholders = ",".join(["%s"] * len(order_ids))
        params: list[Any] = [target_strategy_id, target_position_id, account_id, *order_ids]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE oms_orders
                SET strategy_id = %s,
                    position_id = %s,
                    reconciled = TRUE
                WHERE account_id = %s
                  AND id IN ({placeholders})
                """,
                params,
            )
            return int(cur.rowcount or 0)

    async def reassign_open_orders_position(
        self,
        conn: Any,
        *,
        account_id: int,
        from_position_id: int,
        to_position_id: int,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_orders
                SET previous_position_id = position_id,
                    position_id = %s,
                    updated_at = NOW()
                WHERE account_id = %s
                  AND position_id = %s
                  AND status IN ('PENDING_SUBMIT','SUBMITTED','PARTIALLY_FILLED')
                """,
                (int(to_position_id), int(account_id), int(from_position_id)),
            )
            return int(cur.rowcount or 0)

    async def reassign_deals_position(
        self,
        conn: Any,
        *,
        account_id: int,
        from_position_id: int,
        to_position_id: int,
    ) -> int:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE oms_deals
                SET previous_position_id = position_id,
                    position_id = %s
                WHERE account_id = %s
                  AND position_id = %s
                """,
                (int(to_position_id), int(account_id), int(from_position_id)),
            )
            return int(cur.rowcount or 0)

    async def reassign_deals_strategy_by_orders(
        self,
        conn: Any,
        *,
        account_id: int,
        order_ids: list[int],
        target_strategy_id: int,
    ) -> int:
        if not order_ids:
            return 0
        placeholders = ",".join(["%s"] * len(order_ids))
        params: list[Any] = [int(target_strategy_id), int(account_id), *[int(x) for x in order_ids]]
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE oms_deals
                SET strategy_id = %s,
                    reconciled = TRUE
                WHERE account_id = %s
                  AND order_id IN ({placeholders})
                """,
                tuple(params),
            )
            return int(cur.rowcount or 0)

    async def reassign_positions_strategy_by_orders(
        self,
        conn: Any,
        *,
        account_id: int,
        order_ids: list[int],
        target_strategy_id: int,
    ) -> int:
        if not order_ids:
            return 0
        placeholders = ",".join(["%s"] * len(order_ids))
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT DISTINCT position_id
                FROM oms_deals
                WHERE account_id = %s
                  AND order_id IN ({placeholders})
                  AND position_id > 0
                """,
                (int(account_id), *[int(x) for x in order_ids]),
            )
            rows = await cur.fetchall()
        position_ids = sorted(set([int(r[0]) for r in rows if r and int(r[0]) > 0]))
        if not position_ids:
            return 0
        pos_placeholders = ",".join(["%s"] * len(position_ids))
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                UPDATE oms_positions
                SET strategy_id = %s,
                    updated_at = NOW()
                WHERE account_id = %s
                  AND id IN ({pos_placeholders})
                """,
                (int(target_strategy_id), int(account_id), *position_ids),
            )
            return int(cur.rowcount or 0)

    async def list_reassign_order_candidates(
        self,
        conn: Any,
        *,
        account_ids: list[int],
        order_ids: list[int] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        statuses: list[str] | None = None,
        reconciled: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        if not account_ids:
            return [], 0
        account_placeholders = ",".join(["%s"] * len(account_ids))
        where = [f"account_id IN ({account_placeholders})"]
        params: list[Any] = [*account_ids]
        if order_ids:
            order_placeholders = ",".join(["%s"] * len(order_ids))
            where.append(f"o.id IN ({order_placeholders})")
            params.extend([int(x) for x in order_ids])
        if date_from:
            where.append("DATE(o.created_at) >= %s")
            params.append(date_from)
        if date_to:
            where.append("DATE(o.created_at) <= %s")
            params.append(date_to)
        if statuses:
            st = [str(x).strip().upper() for x in statuses if str(x).strip()]
            if st:
                st_placeholders = ",".join(["%s"] * len(st))
                where.append(f"UPPER(o.status) IN ({st_placeholders})")
                params.extend(st)
        if reconciled is not None:
            where.append("o.reconciled = %s")
            params.append(bool(reconciled))
        where_sql = " AND ".join(where)
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT COUNT(1)
                FROM oms_orders o
                WHERE {where_sql}
                """,
                tuple(params),
            )
            total_row = await cur.fetchone()
            total = int(total_row[0] or 0) if total_row else 0
            await cur.execute(
                f"""
                SELECT
                    o.id,
                    o.account_id,
                    a.exchange_id,
                    o.symbol,
                    o.side,
                    o.status,
                    o.strategy_id,
                    o.position_id,
                    o.qty,
                    o.price,
                    o.filled_qty,
                    o.avg_fill_price,
                    o.exchange_order_id,
                    o.edit_replace_state,
                    o.edit_replace_at,
                    o.edit_replace_orphan_order_id,
                    o.edit_replace_origin_order_id,
                    o.created_at,
                    o.updated_at,
                    o.reconciled
                FROM oms_orders o
                LEFT JOIN accounts a ON a.id = o.account_id
                WHERE {where_sql}
                ORDER BY o.id ASC
                LIMIT %s OFFSET %s
                """,
                (*params, int(limit), int(offset)),
            )
            rows = await cur.fetchall()
        items: list[dict[str, Any]] = []
        for r in rows:
            items.append(
                {
                    "kind": "order",
                    "id": int(r[0]),
                    "account_id": int(r[1]),
                    "exchange_id": None if r[2] is None else str(r[2]),
                    "symbol": None if r[3] is None else str(r[3]),
                    "side": None if r[4] is None else str(r[4]),
                    "status": None if r[5] is None else str(r[5]),
                    "strategy_id": None if r[6] is None else int(r[6]),
                    "position_id": None if r[7] is None else int(r[7]),
                    "qty": None if r[8] is None else str(r[8]),
                    "price": None if r[9] is None else str(r[9]),
                    "filled_qty": None if r[10] is None else str(r[10]),
                    "avg_fill_price": None if r[11] is None else str(r[11]),
                    "exchange_order_id": None if r[12] is None else str(r[12]),
                    "edit_replace_state": None if r[13] is None else str(r[13]),
                    "edit_replace_at": None if r[14] is None else str(r[14]),
                    "edit_replace_orphan_order_id": None if r[15] is None else int(r[15]),
                    "edit_replace_origin_order_id": None if r[16] is None else int(r[16]),
                    "created_at": None if r[17] is None else str(r[17]),
                    "executed_at": None,
                    "reconciled": bool(r[19]),
                }
            )
        return items, total

    async def fetch_reassign_before_state(
        self,
        conn: Any,
        *,
        account_id: int,
        deal_ids: list[int],
        order_ids: list[int],
    ) -> dict[str, list[dict[str, Any]]]:
        out: dict[str, list[dict[str, Any]]] = {"deals": [], "orders": []}
        if deal_ids:
            placeholders = ",".join(["%s"] * len(deal_ids))
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                SELECT id, strategy_id, position_id, reconciled
                    FROM oms_deals
                    WHERE account_id = %s
                      AND id IN ({placeholders})
                    ORDER BY id ASC
                    """,
                    (account_id, *deal_ids),
                )
                rows = await cur.fetchall()
            out["deals"] = [
                {
                    "id": int(r[0]),
                    "strategy_id": None if r[1] is None else int(r[1]),
                    "position_id": None if r[2] is None else int(r[2]),
                    "reconciled": bool(r[3]),
                }
                for r in rows
            ]
        if order_ids:
            placeholders = ",".join(["%s"] * len(order_ids))
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                SELECT id, strategy_id, position_id, status
                    FROM oms_orders
                    WHERE account_id = %s
                      AND id IN ({placeholders})
                    ORDER BY id ASC
                    """,
                    (account_id, *order_ids),
                )
                rows = await cur.fetchall()
            out["orders"] = [
                {
                    "id": int(r[0]),
                    "strategy_id": None if r[1] is None else int(r[1]),
                    "position_id": None if r[2] is None else int(r[2]),
                    "status": None if r[3] is None else str(r[3]),
                }
                for r in rows
            ]
        return out

    async def admin_fetch_oms_order_by_id(
        self,
        conn: Any,
        order_id: int,
        account_id: int | None = None,
    ) -> dict[str, Any] | None:
        account_sql = "AND account_id = %s" if account_id is not None else ""
        params: list[Any] = [int(order_id)]
        if account_id is not None:
            params.append(int(account_id))
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, command_id, account_id, symbol, side, order_type, status, strategy_id, position_id,
                       reason, comment, client_order_id, exchange_order_id, qty, price, stop_loss, stop_gain,
                       filled_qty, avg_fill_price, reconciled, created_at, updated_at, closed_at,
                       previous_position_id, edit_replace_state, edit_replace_at,
                       edit_replace_orphan_order_id, edit_replace_origin_order_id
                FROM oms_orders
                WHERE id = %s {account_sql}
                LIMIT 1
                """,
                tuple(params),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "command_id": None if row[1] is None else int(row[1]),
            "account_id": int(row[2]),
            "symbol": str(row[3]),
            "side": str(row[4]),
            "order_type": str(row[5]),
            "status": str(row[6]),
            "strategy_id": int(row[7]),
            "position_id": int(row[8]),
            "reason": str(row[9]),
            "comment": row[10],
            "client_order_id": row[11],
            "exchange_order_id": row[12],
            "qty": str(row[13]),
            "price": None if row[14] is None else str(row[14]),
            "stop_loss": None if row[15] is None else str(row[15]),
            "stop_gain": None if row[16] is None else str(row[16]),
            "filled_qty": str(row[17]),
            "avg_fill_price": None if row[18] is None else str(row[18]),
            "reconciled": bool(row[19]),
            "created_at": str(row[20]),
            "updated_at": str(row[21]),
            "closed_at": None if row[22] is None else str(row[22]),
            "previous_position_id": None if row[23] is None else int(row[23]),
            "edit_replace_state": None if row[24] is None else str(row[24]),
            "edit_replace_at": None if row[25] is None else str(row[25]),
            "edit_replace_orphan_order_id": None if row[26] is None else int(row[26]),
            "edit_replace_origin_order_id": None if row[27] is None else int(row[27]),
        }

    async def admin_fetch_oms_position_by_id(
        self,
        conn: Any,
        position_id: int,
        account_id: int | None = None,
    ) -> dict[str, Any] | None:
        account_sql = "AND account_id = %s" if account_id is not None else ""
        params: list[Any] = [int(position_id)]
        if account_id is not None:
            params.append(int(account_id))
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, state,
                       reason, comment, opened_at, updated_at, closed_at
                FROM oms_positions
                WHERE id = %s {account_sql}
                LIMIT 1
                """,
                tuple(params),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "account_id": int(row[1]),
            "symbol": str(row[2]),
            "strategy_id": int(row[3]),
            "side": str(row[4]),
            "qty": str(row[5]),
            "avg_price": str(row[6]),
            "stop_loss": None if row[7] is None else str(row[7]),
            "stop_gain": None if row[8] is None else str(row[8]),
            "state": str(row[9]),
            "reason": str(row[10]),
            "comment": row[11],
            "opened_at": str(row[12]),
            "updated_at": str(row[13]),
            "closed_at": None if row[14] is None else str(row[14]),
        }

    async def admin_fetch_oms_deal_by_id(
        self,
        conn: Any,
        deal_id: int,
        account_id: int | None = None,
    ) -> dict[str, Any] | None:
        account_sql = "AND account_id = %s" if account_id is not None else ""
        params: list[Any] = [int(deal_id)]
        if account_id is not None:
            params.append(int(account_id))
        async with conn.cursor() as cur:
            await cur.execute(
                f"""
                SELECT id, account_id, order_id, position_id, symbol, side, qty, price, fee, fee_currency, pnl,
                       strategy_id, reason, comment, reconciled, exchange_trade_id, created_at, executed_at, previous_position_id
                FROM oms_deals
                WHERE id = %s {account_sql}
                LIMIT 1
                """,
                tuple(params),
            )
            row = await cur.fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "account_id": int(row[1]),
            "order_id": None if row[2] is None else int(row[2]),
            "position_id": int(row[3]),
            "symbol": str(row[4]),
            "side": str(row[5]),
            "qty": str(row[6]),
            "price": str(row[7]),
            "fee": None if row[8] is None else str(row[8]),
            "fee_currency": row[9],
            "pnl": None if row[10] is None else str(row[10]),
            "strategy_id": int(row[11]),
            "reason": str(row[12]),
            "comment": row[13],
            "reconciled": bool(row[14]),
            "exchange_trade_id": row[15],
            "created_at": str(row[16]),
            "executed_at": str(row[17]),
            "previous_position_id": None if row[18] is None else int(row[18]),
        }

    async def admin_list_oms_orders_multi(
        self,
        conn: Any,
        *,
        account_ids: list[int],
        open_only: bool,
        date_from: str | None,
        date_to: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        account_sql = ""
        params: list[Any] = []
        if account_ids:
            placeholders = ",".join(["%s"] * len(account_ids))
            account_sql = f"AND account_id IN ({placeholders})"
            params.extend([int(x) for x in account_ids])
        status_sql = "AND status IN ('PENDING_SUBMIT','SUBMITTED','PARTIALLY_FILLED')" if open_only else ""
        date_sql = ""
        if (not open_only) and date_from and date_to:
            date_sql = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params.extend([str(date_from), str(date_to)])
        count_params = list(params)
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM oms_orders WHERE 1=1 {account_sql} {status_sql} {date_sql}",
                tuple(count_params),
            )
            row = await cur.fetchone()
            total = int(row[0] or 0) if row is not None else 0
            params.extend([int(limit), int(offset)])
            await cur.execute(
                f"""
                SELECT id, command_id, account_id, symbol, side, order_type, status, strategy_id, position_id,
                       reason, comment, client_order_id, exchange_order_id, qty, price, stop_loss, stop_gain,
                       filled_qty, avg_fill_price, reconciled, created_at, updated_at, closed_at,
                       previous_position_id, edit_replace_state, edit_replace_at,
                       edit_replace_orphan_order_id, edit_replace_origin_order_id
                FROM oms_orders
                WHERE 1=1 {account_sql} {status_sql} {date_sql}
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "command_id": None if r[1] is None else int(r[1]),
                    "account_id": int(r[2]),
                    "symbol": r[3],
                    "side": r[4],
                    "order_type": r[5],
                    "status": r[6],
                    "strategy_id": int(r[7]),
                    "position_id": int(r[8]),
                    "reason": r[9],
                    "comment": r[10],
                    "client_order_id": r[11],
                    "exchange_order_id": r[12],
                    "qty": str(r[13]),
                    "price": None if r[14] is None else str(r[14]),
                    "stop_loss": None if r[15] is None else str(r[15]),
                    "stop_gain": None if r[16] is None else str(r[16]),
                    "filled_qty": str(r[17]),
                    "avg_fill_price": None if r[18] is None else str(r[18]),
                    "reconciled": bool(r[19]),
                    "created_at": str(r[20]),
                    "updated_at": str(r[21]),
                    "closed_at": None if r[22] is None else str(r[22]),
                    "previous_position_id": None if r[23] is None else int(r[23]),
                    "edit_replace_state": None if r[24] is None else str(r[24]),
                    "edit_replace_at": None if r[25] is None else str(r[25]),
                    "edit_replace_orphan_order_id": None if r[26] is None else int(r[26]),
                    "edit_replace_origin_order_id": None if r[27] is None else int(r[27]),
                }
            )
        return out, total

    async def admin_list_oms_positions_multi(
        self,
        conn: Any,
        *,
        account_ids: list[int],
        open_only: bool,
        date_from: str | None,
        date_to: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        account_sql = ""
        params: list[Any] = []
        if account_ids:
            placeholders = ",".join(["%s"] * len(account_ids))
            account_sql = f"AND account_id IN ({placeholders})"
            params.extend([int(x) for x in account_ids])
        state_sql = "AND state IN ('open','close_requested')" if open_only else ""
        date_sql = ""
        if (not open_only) and date_from and date_to:
            date_sql = "AND updated_at >= %s AND updated_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params.extend([str(date_from), str(date_to)])
        count_params = list(params)
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM oms_positions WHERE 1=1 {account_sql} {state_sql} {date_sql}",
                tuple(count_params),
            )
            row = await cur.fetchone()
            total = int(row[0] or 0) if row is not None else 0
            params.extend([int(limit), int(offset)])
            await cur.execute(
                f"""
                SELECT id, account_id, symbol, strategy_id, side, qty, avg_price, stop_loss, stop_gain, state,
                       reason, comment, opened_at, updated_at, closed_at
                FROM oms_positions
                WHERE 1=1 {account_sql} {state_sql} {date_sql}
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": int(r[1]),
                    "symbol": r[2],
                    "strategy_id": int(r[3]),
                    "side": r[4],
                    "qty": str(r[5]),
                    "avg_price": str(r[6]),
                    "stop_loss": None if r[7] is None else str(r[7]),
                    "stop_gain": None if r[8] is None else str(r[8]),
                    "state": r[9],
                    "reason": r[10],
                    "comment": r[11],
                    "opened_at": str(r[12]),
                    "updated_at": str(r[13]),
                    "closed_at": None if r[14] is None else str(r[14]),
                }
            )
        return out, total

    async def admin_list_oms_deals_multi(
        self,
        conn: Any,
        *,
        account_ids: list[int],
        date_from: str | None,
        date_to: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        account_sql = ""
        params: list[Any] = []
        if account_ids:
            placeholders = ",".join(["%s"] * len(account_ids))
            account_sql = f"AND account_id IN ({placeholders})"
            params.extend([int(x) for x in account_ids])
        date_sql = ""
        if date_from and date_to:
            date_sql = "AND executed_at >= %s AND executed_at < DATE_ADD(%s, INTERVAL 1 DAY)"
            params.extend([str(date_from), str(date_to)])
        count_params = list(params)
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT COUNT(*) FROM oms_deals WHERE 1=1 {account_sql} {date_sql}",
                tuple(count_params),
            )
            row = await cur.fetchone()
            total = int(row[0] or 0) if row is not None else 0
            params.extend([int(limit), int(offset)])
            await cur.execute(
                f"""
                SELECT id, account_id, order_id, position_id, symbol, side, qty, price, fee, fee_currency, pnl,
                       strategy_id, reason, comment, reconciled, exchange_trade_id, created_at, executed_at, previous_position_id
                FROM oms_deals
                WHERE 1=1 {account_sql} {date_sql}
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            rows = await cur.fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "account_id": int(r[1]),
                    "order_id": None if r[2] is None else int(r[2]),
                    "position_id": int(r[3]),
                    "symbol": r[4],
                    "side": r[5],
                    "qty": str(r[6]),
                    "price": str(r[7]),
                    "fee": None if r[8] is None else str(r[8]),
                    "fee_currency": r[9],
                    "pnl": None if r[10] is None else str(r[10]),
                    "strategy_id": int(r[11]),
                    "reason": r[12],
                    "comment": r[13],
                    "reconciled": bool(r[14]),
                    "exchange_trade_id": r[15],
                    "created_at": str(r[16]),
                    "executed_at": str(r[17]),
                    "previous_position_id": None if r[18] is None else int(r[18]),
                }
            )
        return out, total

    async def admin_insert_oms_order(self, conn: Any, row: dict[str, Any]) -> int:
        allowed = [
            "id", "command_id", "account_id", "symbol", "side", "order_type", "status", "strategy_id", "position_id",
            "reason", "comment", "client_order_id", "exchange_order_id", "qty", "price", "stop_loss", "stop_gain",
            "filled_qty", "avg_fill_price", "reconciled", "created_at", "updated_at", "closed_at",
            "previous_position_id", "edit_replace_state", "edit_replace_at",
            "edit_replace_orphan_order_id", "edit_replace_origin_order_id",
        ]
        cols = [k for k in allowed if k in row]
        if not cols:
            raise ValueError("empty_row")
        values = [row.get(k) for k in cols]
        placeholders = ",".join(["%s"] * len(cols))
        async with conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO oms_orders ({', '.join(cols)}) VALUES ({placeholders})",
                tuple(values),
            )
            return int(cur.lastrowid)

    async def admin_update_oms_order(self, conn: Any, order_id: int, row: dict[str, Any]) -> int:
        allowed = [
            "command_id", "account_id", "symbol", "side", "order_type", "status", "strategy_id", "position_id",
            "reason", "comment", "client_order_id", "exchange_order_id", "qty", "price", "stop_loss", "stop_gain",
            "filled_qty", "avg_fill_price", "reconciled", "created_at", "updated_at", "closed_at",
            "previous_position_id", "edit_replace_state", "edit_replace_at",
            "edit_replace_orphan_order_id", "edit_replace_origin_order_id",
        ]
        sets: list[str] = []
        params: list[Any] = []
        for k in allowed:
            if k in row:
                sets.append(f"{k} = %s")
                params.append(row.get(k))
        if not sets:
            return 0
        params.append(int(order_id))
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE oms_orders SET {', '.join(sets)} WHERE id = %s",
                tuple(params),
            )
            return int(cur.rowcount or 0)

    async def admin_delete_oms_order(self, conn: Any, order_id: int, account_id: int | None = None) -> int:
        account_sql = "AND account_id = %s" if account_id is not None else ""
        params: list[Any] = [int(order_id)]
        if account_id is not None:
            params.append(int(account_id))
        async with conn.cursor() as cur:
            await cur.execute(f"DELETE FROM oms_orders WHERE id = %s {account_sql}", tuple(params))
            return int(cur.rowcount or 0)

    async def admin_insert_oms_position(self, conn: Any, row: dict[str, Any]) -> int:
        allowed = [
            "id", "account_id", "symbol", "strategy_id", "side", "qty", "avg_price", "stop_loss", "stop_gain",
            "state", "reason", "comment", "opened_at", "updated_at", "closed_at",
        ]
        cols = [k for k in allowed if k in row]
        if not cols:
            raise ValueError("empty_row")
        values = [row.get(k) for k in cols]
        placeholders = ",".join(["%s"] * len(cols))
        async with conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO oms_positions ({', '.join(cols)}) VALUES ({placeholders})",
                tuple(values),
            )
            return int(cur.lastrowid)

    async def admin_update_oms_position(self, conn: Any, position_id: int, row: dict[str, Any]) -> int:
        allowed = [
            "account_id", "symbol", "strategy_id", "side", "qty", "avg_price", "stop_loss", "stop_gain",
            "state", "reason", "comment", "opened_at", "updated_at", "closed_at",
        ]
        sets: list[str] = []
        params: list[Any] = []
        for k in allowed:
            if k in row:
                sets.append(f"{k} = %s")
                params.append(row.get(k))
        if not sets:
            return 0
        params.append(int(position_id))
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE oms_positions SET {', '.join(sets)} WHERE id = %s",
                tuple(params),
            )
            return int(cur.rowcount or 0)

    async def admin_delete_oms_position(self, conn: Any, position_id: int, account_id: int | None = None) -> int:
        account_sql = "AND account_id = %s" if account_id is not None else ""
        params: list[Any] = [int(position_id)]
        if account_id is not None:
            params.append(int(account_id))
        async with conn.cursor() as cur:
            await cur.execute(f"DELETE FROM oms_positions WHERE id = %s {account_sql}", tuple(params))
            return int(cur.rowcount or 0)

    async def admin_insert_oms_deal(self, conn: Any, row: dict[str, Any]) -> int:
        allowed = [
            "id", "account_id", "order_id", "position_id", "symbol", "side", "qty", "price", "fee", "fee_currency",
            "pnl", "strategy_id", "reason", "comment", "reconciled", "exchange_trade_id", "created_at", "executed_at",
            "previous_position_id",
        ]
        cols = [k for k in allowed if k in row]
        if not cols:
            raise ValueError("empty_row")
        values = [row.get(k) for k in cols]
        placeholders = ",".join(["%s"] * len(cols))
        async with conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO oms_deals ({', '.join(cols)}) VALUES ({placeholders})",
                tuple(values),
            )
            return int(cur.lastrowid)

    async def admin_update_oms_deal(self, conn: Any, deal_id: int, row: dict[str, Any]) -> int:
        allowed = [
            "account_id", "order_id", "position_id", "symbol", "side", "qty", "price", "fee", "fee_currency",
            "pnl", "strategy_id", "reason", "comment", "reconciled", "exchange_trade_id", "created_at", "executed_at",
            "previous_position_id",
        ]
        sets: list[str] = []
        params: list[Any] = []
        for k in allowed:
            if k in row:
                sets.append(f"{k} = %s")
                params.append(row.get(k))
        if not sets:
            return 0
        params.append(int(deal_id))
        async with conn.cursor() as cur:
            await cur.execute(f"UPDATE oms_deals SET {', '.join(sets)} WHERE id = %s", tuple(params))
            return int(cur.rowcount or 0)

    async def admin_delete_oms_deal(self, conn: Any, deal_id: int, account_id: int | None = None) -> int:
        account_sql = "AND account_id = %s" if account_id is not None else ""
        params: list[Any] = [int(deal_id)]
        if account_id is not None:
            params.append(int(account_id))
        async with conn.cursor() as cur:
            await cur.execute(f"DELETE FROM oms_deals WHERE id = %s {account_sql}", tuple(params))
            return int(cur.rowcount or 0)

    async def insert_event(
        self, conn: Any, account_id: int, namespace: str, event_type: str, payload: dict[str, Any]
    ) -> None:
        _ = conn
        if self._event_sink is None:
            return
        event = {
            "id": 0,
            "namespace": str(namespace),
            "event_type": str(event_type),
            "payload": payload,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        maybe_coro = self._event_sink(int(account_id), event)
        if hasattr(maybe_coro, "__await__"):
            await maybe_coro

