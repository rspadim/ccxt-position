import asyncio
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path
from collections import deque
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any

import ccxt.async_support as ccxt_async
from pydantic import TypeAdapter

from .app.auth import AuthContext, validate_api_key
from .app.ccxt_adapter import CCXTAdapter
from .app.config import load_settings
from .app.credentials_codec import CredentialsCodec
from .app.db_mysql import DatabaseMySQL
from .app.logging_utils import build_file_logger, setup_application_logging
from .app.repository_mysql import MySQLCommandRepository
from .app.schemas import CommandInput
from .app.service import process_single_command_direct
from .worker_position import _reconcile_account_once

try:
    import orjson  # type: ignore
except Exception:
    orjson = None

try:
    import ccxt.pro as ccxt_pro  # type: ignore
except Exception:
    ccxt_pro = None


COMMAND_INPUT_ADAPTER = TypeAdapter(CommandInput)


def fastjson_encode(value: Any) -> bytes:
    if orjson is not None:
        try:
            return orjson.dumps(value, default=str)
        except Exception:
            pass
    return json.dumps(value, separators=(",", ":"), default=str).encode("utf-8")


def fastjson_decode(value: bytes) -> Any:
    if orjson is not None:
        try:
            return orjson.loads(value)
        except Exception:
            pass
    return json.loads(value.decode("utf-8"))


@dataclass
class _Job:
    account_id: int
    payload: dict[str, Any]
    future: asyncio.Future
    enqueued_at: float = 0.0


class Dispatcher:
    def __init__(self) -> None:
        self.settings = load_settings()
        self.db = DatabaseMySQL(self.settings)
        self.repo = MySQLCommandRepository(event_sink=self._publish_event)
        self.loggers = setup_application_logging(
            self.settings.disable_uvicorn_access_log, log_dir=self.settings.log_dir
        )
        self._hint_dispatcher_loggers: dict[tuple[str, int], Any] = {}
        self._hint_ccxt_loggers: dict[tuple[str, int], Any] = {}
        self.ccxt = CCXTAdapter(logger=self.loggers.get("ccxt"))
        self.codec = CredentialsCodec(
            self.settings.encryption_master_key,
            require_encrypted=self.settings.require_encrypted_credentials,
        )
        self.pool_size_by_engine: dict[str, int] = {
            "ccxt": int(self.settings.dispatcher_pool_size_ccxt),
            "ccxtpro": int(self.settings.dispatcher_pool_size_ccxtpro),
        }
        self.worker_queues: dict[str, dict[int, asyncio.Queue[_Job]]] = {
            engine: {} for engine in self.pool_size_by_engine
        }
        self.worker_tasks: dict[str, dict[int, asyncio.Task]] = {
            engine: {} for engine in self.pool_size_by_engine
        }
        self.account_worker: dict[tuple[str, int], int] = {}
        self.account_locks: dict[int, asyncio.Lock] = {}
        self.worker_active_accounts: dict[str, dict[int, set[int]]] = {
            engine: {wid: set() for wid in range(size)}
            for engine, size in self.pool_size_by_engine.items()
        }
        self.worker_inflight: dict[str, dict[int, int]] = {
            engine: {wid: 0 for wid in range(size)}
            for engine, size in self.pool_size_by_engine.items()
        }
        self.read_pool_size: int = max(
            4,
            min(16, int(self.pool_size_by_engine.get("ccxt", 0)) + int(self.pool_size_by_engine.get("ccxtpro", 0))),
        )
        self.read_worker_queues: dict[int, asyncio.Queue[_Job]] = {}
        self.read_worker_tasks: dict[int, asyncio.Task] = {}
        self.read_worker_inflight: dict[int, int] = {wid: 0 for wid in range(self.read_pool_size)}
        self.control_queue: asyncio.Queue[_Job] = asyncio.Queue()
        self.control_task: asyncio.Task | None = None
        self.started_at = int(time.time())
        self.total_requests = 0
        self.total_errors = 0
        self.op_counts: dict[str, int] = {}
        self._server: asyncio.AbstractServer | None = None
        self._ws_events_by_account: dict[int, deque[dict[str, Any]]] = {}
        self._ws_event_seq = 0
        self._ws_event_buffer_limit = 5000
        self._queue_wait_samples_ms: deque[float] = deque(maxlen=5000)
        self._execute_samples_ms: deque[float] = deque(maxlen=5000)
        self._op_timing_total_ms: dict[str, float] = {}
        self._op_timing_count: dict[str, int] = {}
        self._op_timing_max_ms: dict[str, float] = {}
        self._op_timing_samples_ms: dict[str, deque[float]] = {}
        self.oms_query_cache_ttl_seconds: int = max(
            0, int(os.getenv("DISPATCHER_OMS_QUERY_CACHE_TTL_SECONDS", "5") or 5)
        )
        self._oms_query_cache: dict[str, tuple[float, Any]] = {}
        self._oms_query_cache_keys_by_account: dict[int, set[str]] = {}
        self._oms_query_cache_hits = 0
        self._oms_query_cache_misses = 0
        self._oms_query_cache_invalidations = 0
        self.auth_cache_ttl_seconds: int = max(
            0, int(os.getenv("DISPATCHER_AUTH_CACHE_TTL_SECONDS", "60") or 60)
        )
        self._auth_cache_by_key: dict[str, tuple[float, AuthContext]] = {}
        self._auth_cache_keys_by_api_key_id: dict[int, set[str]] = {}
        self._auth_cache_hits = 0
        self._auth_cache_misses = 0
        self._auth_cache_invalidations = 0
        self.permission_cache_ttl_seconds: int = max(
            0, int(os.getenv("DISPATCHER_PERMISSION_CACHE_TTL_SECONDS", "300") or 300)
        )
        self._perm_account_cache: dict[tuple[int, int], tuple[float, dict[str, Any]]] = {}
        self._perm_strategy_cache: dict[tuple[int, int, int, bool], tuple[float, bool]] = {}
        self._perm_cache_account_hits = 0
        self._perm_cache_account_misses = 0
        self._perm_cache_strategy_hits = 0
        self._perm_cache_strategy_misses = 0
        self._perm_cache_invalidations = 0

    def _dispatcher_logger_for_hint(self, engine: str, hint_id: int) -> Any:
        hint = int(hint_id)
        key = (str(engine), hint)
        logger = self._hint_dispatcher_loggers.get(key)
        if logger is not None:
            return logger
        base = Path(self.settings.log_dir)
        logger = build_file_logger(
            f"ccxt_position.dispatcher.{engine}.hint.{hint}",
            base / f"dispatcher-{engine}-hint-{hint}.log",
        )
        self._hint_dispatcher_loggers[key] = logger
        return logger

    def _ccxt_logger_for_hint(self, engine: str, hint_id: int) -> Any:
        hint = int(hint_id)
        key = (str(engine), hint)
        logger = self._hint_ccxt_loggers.get(key)
        if logger is not None:
            return logger
        base = Path(self.settings.log_dir)
        logger = build_file_logger(
            f"ccxt_position.ccxt.{engine}.hint.{hint}",
            base / f"ccxt-{engine}-hint-{hint}.log",
        )
        self._hint_ccxt_loggers[key] = logger
        return logger

    async def _auth_from_payload(self, msg: dict[str, Any]) -> AuthContext:
        api_key = str(msg.get("x_api_key", "") or "").strip()
        if not api_key:
            raise RuntimeError("missing_api_key")
        auth = self._auth_cache_get(api_key)
        if auth is None:
            auth = await validate_api_key(self.db, api_key)
            if auth is not None:
                self._auth_cache_set(api_key, auth)
        if auth is None:
            raise RuntimeError("invalid_api_key")
        return auth

    def _auth_cache_get(self, raw_api_key: str) -> AuthContext | None:
        if self.auth_cache_ttl_seconds <= 0:
            return None
        row = self._auth_cache_by_key.get(str(raw_api_key))
        if row is None:
            self._auth_cache_misses += 1
            return None
        expires_at, auth = row
        if expires_at < time.monotonic():
            self._auth_cache_by_key.pop(str(raw_api_key), None)
            self._auth_cache_misses += 1
            return None
        self._auth_cache_hits += 1
        return auth

    def _auth_cache_set(self, raw_api_key: str, auth: AuthContext) -> None:
        if self.auth_cache_ttl_seconds <= 0:
            return
        key = str(raw_api_key)
        self._auth_cache_by_key[key] = (time.monotonic() + float(self.auth_cache_ttl_seconds), auth)
        bucket = self._auth_cache_keys_by_api_key_id.setdefault(int(auth.api_key_id), set())
        bucket.add(key)

    def _auth_cache_invalidate(self, *, api_key_id: int | None = None) -> None:
        if self.auth_cache_ttl_seconds <= 0:
            return
        self._auth_cache_invalidations += 1
        if api_key_id is None:
            self._auth_cache_by_key.clear()
            self._auth_cache_keys_by_api_key_id.clear()
            return
        keys = self._auth_cache_keys_by_api_key_id.pop(int(api_key_id), set())
        for key in keys:
            self._auth_cache_by_key.pop(key, None)

    @staticmethod
    def _require_admin(auth: AuthContext) -> None:
        if not auth.is_admin:
            raise RuntimeError("admin_required")

    @staticmethod
    def _normalize_role(role: str) -> str:
        normalized = str(role or "").strip().lower()
        if normalized in {"admin", "trader", "portfolio_manager", "robot", "risk", "readonly"}:
            return normalized
        return "trader"

    @classmethod
    def _default_reason_for_role(cls, role: str) -> str:
        normalized = cls._normalize_role(role)
        if normalized in {"trader", "portfolio_manager", "robot", "risk"}:
            return normalized
        if normalized in {"readonly", "admin"}:
            return "readonly"
        return "trader"

    @staticmethod
    def _json_dumps(value: Any) -> str:
        return fastjson_encode(value).decode("utf-8")

    @staticmethod
    def _json_dumps_bytes(value: Any) -> bytes:
        return fastjson_encode(value)

    @staticmethod
    def _json_loads_bytes(value: bytes) -> Any:
        return fastjson_decode(value)

    @staticmethod
    def _percentile(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        if q <= 0:
            return float(min(values))
        if q >= 1:
            return float(max(values))
        ordered = sorted(values)
        idx = (len(ordered) - 1) * q
        lo = int(idx)
        hi = min(lo + 1, len(ordered) - 1)
        frac = idx - lo
        return float(ordered[lo] * (1 - frac) + ordered[hi] * frac)

    @staticmethod
    def _op_timing_key(payload: dict[str, Any]) -> str:
        op = str(payload.get("op", "") or "").strip()
        if op == "oms_query_multi":
            query = str(payload.get("query", "") or "").strip()
            if query:
                return f"{op}:{query}"
        return op or "unknown"

    def _record_op_timing(self, payload: dict[str, Any], execute_ms: float) -> None:
        key = self._op_timing_key(payload)
        self._op_timing_count[key] = int(self._op_timing_count.get(key, 0)) + 1
        self._op_timing_total_ms[key] = float(self._op_timing_total_ms.get(key, 0.0)) + float(execute_ms)
        self._op_timing_max_ms[key] = max(float(self._op_timing_max_ms.get(key, 0.0)), float(execute_ms))
        samples = self._op_timing_samples_ms.get(key)
        if samples is None:
            samples = deque(maxlen=2000)
            self._op_timing_samples_ms[key] = samples
        samples.append(float(execute_ms))

    def _maybe_invalidate_oms_query_cache(self, payload: dict[str, Any], out: Any) -> None:
        if self.oms_query_cache_ttl_seconds <= 0:
            return
        if not isinstance(out, dict) or not bool(out.get("ok")):
            return
        op = str(payload.get("op", "") or "").strip().lower()
        if op in {
            "status",
            "authorize_account",
            "oms_query",
            "oms_query_multi",
            "ccxt_raw_query",
            "ccxt_raw_query_multi",
            "reconcile_status_account",
            "reconcile_status_list",
            "accounts_list",
            "ws_pull_events",
            "ws_tail_id",
            "auth_check",
            "meta_ccxt_exchanges",
            "user_profile_get",
            "user_api_keys_list",
            "user_api_key_permissions_list",
            "admin_list_accounts",
            "admin_list_users",
            "admin_list_users_api_keys",
            "admin_list_api_key_permissions",
            "admin_list_strategies",
            "strategy_list",
            "admin_oms_query",
        }:
            return
        if op == "ccxt_call":
            func = str(payload.get("func", "") or "").strip()
            if not self._ccxt_requires_trade(func):
                return
        account_ids: list[int] = []
        account_id = int(payload.get("account_id", 0) or 0)
        if account_id > 0:
            account_ids.append(account_id)
        if op in {"oms_commands_batch", "ccxt_batch"}:
            raw_items = payload.get("items")
            if isinstance(raw_items, list):
                for raw in raw_items:
                    if not isinstance(raw, dict):
                        continue
                    aid = int(raw.get("account_id", 0) or 0)
                    if aid > 0:
                        account_ids.append(aid)
        if account_ids:
            self._oms_query_cache_invalidate_accounts(account_ids)
        else:
            self._oms_query_cache_invalidate_all()

    def _oms_query_cache_get(self, key: str) -> Any | None:
        if self.oms_query_cache_ttl_seconds <= 0:
            return None
        row = self._oms_query_cache.get(key)
        if row is None:
            self._oms_query_cache_misses += 1
            return None
        expires_at, value = row
        if expires_at < time.monotonic():
            self._oms_query_cache.pop(key, None)
            self._oms_query_cache_misses += 1
            return None
        self._oms_query_cache_hits += 1
        return value

    def _oms_query_cache_set(self, key: str, value: Any, account_ids: list[int]) -> None:
        if self.oms_query_cache_ttl_seconds <= 0:
            return
        self._oms_query_cache[key] = (time.monotonic() + float(self.oms_query_cache_ttl_seconds), value)
        for aid in sorted({int(x) for x in account_ids if int(x) > 0}):
            self._oms_query_cache_keys_by_account.setdefault(aid, set()).add(key)

    def _oms_query_cache_invalidate_accounts(self, account_ids: list[int]) -> None:
        if self.oms_query_cache_ttl_seconds <= 0:
            return
        ids = sorted({int(x) for x in account_ids if int(x) > 0})
        if not ids:
            return
        self._oms_query_cache_invalidations += 1
        for aid in ids:
            keys = self._oms_query_cache_keys_by_account.pop(aid, set())
            for key in keys:
                self._oms_query_cache.pop(key, None)

    def _oms_query_cache_invalidate_all(self) -> None:
        if self.oms_query_cache_ttl_seconds <= 0:
            return
        self._oms_query_cache_invalidations += 1
        self._oms_query_cache.clear()
        self._oms_query_cache_keys_by_account.clear()

    @staticmethod
    def _make_oms_query_cache_key(
        *,
        api_key_id: int,
        query: str,
        account_ids: list[int],
        strategy_id: int | None,
        date_from: str | None,
        date_to: str | None,
        open_limit: int | None,
        page: int | None,
        page_size: int | None,
    ) -> str:
        payload = {
            "api_key_id": int(api_key_id),
            "query": str(query),
            "account_ids": [int(x) for x in account_ids if int(x) > 0],
            "strategy_id": None if strategy_id is None else int(strategy_id),
            "date_from": date_from,
            "date_to": date_to,
            "open_limit": None if open_limit is None else int(open_limit),
            "page": None if page is None else int(page),
            "page_size": None if page_size is None else int(page_size),
        }
        return fastjson_encode(payload).decode("utf-8")

    @staticmethod
    def _exchange_engine_id(value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            raise RuntimeError("unsupported_engine")
        low = raw.lower()
        # Backward compatibility: legacy records stored bare exchange id (e.g. "binance").
        # Default those to the ccxt engine.
        if "." not in raw:
            return f"ccxt.{raw}"
        if low.startswith("ccxt.") or low.startswith("ccxtpro."):
            parts = raw.split(".", 1)
            if len(parts) != 2 or not parts[1].strip():
                raise RuntimeError("unsupported_engine")
            return raw
        raise RuntimeError("unsupported_engine")

    @staticmethod
    def _engine_of_exchange_id(value: Any) -> str:
        normalized = Dispatcher._exchange_engine_id(value).lower()
        if normalized.startswith("ccxtpro."):
            return "ccxtpro"
        if normalized.startswith("ccxt."):
            return "ccxt"
        raise RuntimeError("unsupported_engine")

    @classmethod
    def _decorate_exchange_ids(cls, payload: Any) -> Any:
        return payload

    @staticmethod
    def _hash_password(password: str, salt_hex: str) -> str:
        digest = hashlib.sha256((salt_hex + password).encode("utf-8")).hexdigest()
        return f"sha256${salt_hex}${digest}"

    @classmethod
    def _new_password_hash(cls, password: str) -> str:
        salt_hex = secrets.token_hex(16)
        return cls._hash_password(password, salt_hex)

    @classmethod
    def _verify_password(cls, password: str, stored_hash: str | None) -> bool:
        if not stored_hash:
            return False
        parts = str(stored_hash).split("$")
        if len(parts) != 3 or parts[0] != "sha256":
            return False
        expected = cls._hash_password(password, parts[1])
        return hmac.compare_digest(expected, stored_hash)

    async def _engine_for_account(self, account_id: int) -> str:
        async with self.db.connection() as conn:
            account = await self.repo.fetch_account_by_id(conn, account_id)
            await conn.commit()
        if account is None:
            raise RuntimeError("account_not_found")
        return self._engine_of_exchange_id(account.get("exchange_id"))

    async def _require_account_permission(
        self,
        auth: AuthContext,
        account_id: int,
        *,
        require_trade: bool = False,
        require_close_position: bool = False,
        require_risk_manage: bool = False,
        require_block_new_positions: bool = False,
        require_block_account: bool = False,
        for_ws: bool = False,
    ) -> dict[str, Any]:
        account, perms = await self._get_account_and_permissions_cached(auth.api_key_id, account_id)
        if account is None or str(account.get("status", "")).strip().lower() != "active":
            raise RuntimeError("account_not_found")
        if perms is None or not bool(perms.get("can_read")):
            raise RuntimeError("permission_denied")
        if require_trade and not bool(perms.get("can_trade")):
            raise RuntimeError("permission_denied")
        if require_close_position and not bool(perms.get("can_close_position")):
            raise RuntimeError("permission_denied")
        if require_risk_manage and not bool(perms.get("can_risk_manage")):
            raise RuntimeError("permission_denied")
        if require_block_new_positions and not bool(perms.get("can_block_new_positions")):
            raise RuntimeError("permission_denied")
        if require_block_account and not bool(perms.get("can_block_account")):
            raise RuntimeError("permission_denied")
        if for_ws and bool(perms.get("restrict_to_strategies")):
            raise RuntimeError("strategy_ws_not_supported")
        return account

    async def _require_strategy_permission(
        self,
        auth: AuthContext,
        account_id: int,
        strategy_id: int,
        *,
        for_trade: bool,
    ) -> None:
        _account, perms = await self._get_account_and_permissions_cached(auth.api_key_id, account_id)
        if perms is None:
            raise RuntimeError("permission_denied")
        if not bool(perms.get("restrict_to_strategies")):
            return
        allowed = self._perm_cache_get_strategy(auth.api_key_id, account_id, int(strategy_id), bool(for_trade))
        if allowed is None:
            async with self.db.connection() as conn:
                allowed = await self.repo.api_key_strategy_allowed(
                    conn, auth.api_key_id, account_id, int(strategy_id), for_trade=for_trade
                )
                await conn.commit()
            self._perm_cache_set_strategy(auth.api_key_id, account_id, int(strategy_id), bool(for_trade), bool(allowed))
        if not allowed:
            raise RuntimeError("strategy_permission_denied")

    async def _get_account_and_permissions_cached(
        self, api_key_id: int, account_id: int
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        cached = self._perm_cache_get_account(api_key_id, account_id)
        if cached is not None:
            return (
                cached.get("account") if isinstance(cached.get("account"), dict) else None,
                cached.get("perms") if isinstance(cached.get("perms"), dict) else None,
            )
        async with self.db.connection() as conn:
            account = await self.repo.fetch_account_by_id(conn, account_id)
            perms = await self.repo.fetch_api_key_account_permissions(conn, api_key_id, account_id)
            await conn.commit()
        self._perm_cache_set_account(
            api_key_id,
            account_id,
            {
                "account": account if isinstance(account, dict) else {},
                "perms": perms if isinstance(perms, dict) else None,
            },
        )
        return account, perms

    def _perm_cache_get_account(self, api_key_id: int, account_id: int) -> dict[str, Any] | None:
        if self.permission_cache_ttl_seconds <= 0:
            return None
        key = (int(api_key_id), int(account_id))
        row = self._perm_account_cache.get(key)
        if row is None:
            self._perm_cache_account_misses += 1
            return None
        expires_at, value = row
        if expires_at < time.monotonic():
            self._perm_account_cache.pop(key, None)
            self._perm_cache_account_misses += 1
            return None
        self._perm_cache_account_hits += 1
        return value

    def _perm_cache_set_account(self, api_key_id: int, account_id: int, value: dict[str, Any]) -> None:
        if self.permission_cache_ttl_seconds <= 0:
            return
        key = (int(api_key_id), int(account_id))
        self._perm_account_cache[key] = (time.monotonic() + float(self.permission_cache_ttl_seconds), value)

    def _perm_cache_get_strategy(
        self, api_key_id: int, account_id: int, strategy_id: int, for_trade: bool
    ) -> bool | None:
        if self.permission_cache_ttl_seconds <= 0:
            return None
        key = (int(api_key_id), int(account_id), int(strategy_id), bool(for_trade))
        row = self._perm_strategy_cache.get(key)
        if row is None:
            self._perm_cache_strategy_misses += 1
            return None
        expires_at, value = row
        if expires_at < time.monotonic():
            self._perm_strategy_cache.pop(key, None)
            self._perm_cache_strategy_misses += 1
            return None
        self._perm_cache_strategy_hits += 1
        return bool(value)

    def _perm_cache_set_strategy(
        self, api_key_id: int, account_id: int, strategy_id: int, for_trade: bool, value: bool
    ) -> None:
        if self.permission_cache_ttl_seconds <= 0:
            return
        key = (int(api_key_id), int(account_id), int(strategy_id), bool(for_trade))
        self._perm_strategy_cache[key] = (
            time.monotonic() + float(self.permission_cache_ttl_seconds),
            bool(value),
        )

    def _perm_cache_invalidate(self, *, api_key_id: int | None = None, account_id: int | None = None) -> None:
        if self.permission_cache_ttl_seconds <= 0:
            return
        self._perm_cache_invalidations += 1
        if api_key_id is None and account_id is None:
            self._perm_account_cache.clear()
            self._perm_strategy_cache.clear()
            return
        if api_key_id is not None:
            for key in [k for k in self._perm_account_cache.keys() if int(k[0]) == int(api_key_id)]:
                self._perm_account_cache.pop(key, None)
            for key in [k for k in self._perm_strategy_cache.keys() if int(k[0]) == int(api_key_id)]:
                self._perm_strategy_cache.pop(key, None)
        if account_id is not None:
            for key in [k for k in self._perm_account_cache.keys() if int(k[1]) == int(account_id)]:
                self._perm_account_cache.pop(key, None)
            for key in [k for k in self._perm_strategy_cache.keys() if int(k[1]) == int(account_id)]:
                self._perm_strategy_cache.pop(key, None)

    async def _can_reassign_account(self, auth: AuthContext, account_id: int) -> bool:
        if auth.is_admin:
            return True
        async with self.db.connection() as conn:
            perms = await self.repo.fetch_api_key_account_permissions(conn, auth.api_key_id, account_id)
            await conn.commit()
        if perms is None:
            return False
        return bool(perms.get("can_trade")) or bool(perms.get("can_risk_manage"))

    async def _require_oms_command_permission(self, auth: AuthContext, item: CommandInput) -> int:
        account_id = int(item.account_id or 0)
        payload = item.payload.model_dump(by_alias=True, exclude_none=True, mode="json")
        command = str(item.command)
        role = self._normalize_role(auth.role)
        if role == "admin":
            raise RuntimeError("admin_read_only")
        def _parse_int_ids(raw: Any) -> list[int]:
            if isinstance(raw, list):
                return [int(x) for x in raw if str(x).strip().isdigit() and int(x) > 0]
            if raw is None:
                return []
            text = str(raw).strip()
            if not text:
                return []
            return [int(x.strip()) for x in text.split(",") if x.strip().isdigit() and int(x.strip()) > 0]
        if command == "send_order":
            if account_id <= 0:
                raise RuntimeError("missing_account_id")
            await self._require_account_permission(auth, account_id, require_trade=True)
            strategy_id = int(payload.get("strategy_id", 0) or 0)
            await self._require_strategy_permission(auth, account_id, strategy_id, for_trade=True)
            return account_id
        if command in {"cancel_order", "change_order"}:
            order_ids = _parse_int_ids(payload.get("order_ids"))
            order_id_single = int(payload.get("order_id", 0) or 0)
            if order_id_single > 0:
                order_ids.append(order_id_single)
            order_ids = sorted(set(order_ids))
            if not order_ids:
                raise RuntimeError("validation_error")
            if command == "change_order" and len(order_ids) != 1:
                raise RuntimeError("validation_error")
            inferred_accounts: set[int] = set()
            async with self.db.connection() as conn:
                for order_id in order_ids:
                    inferred = await self.repo.fetch_order_account_id(conn, order_id)
                    if inferred is None:
                        await conn.commit()
                        raise RuntimeError("order_not_found")
                    inferred_accounts.add(int(inferred))
                await conn.commit()
            if account_id > 0:
                if any(a != account_id for a in inferred_accounts):
                    raise RuntimeError("order_not_found")
            else:
                if len(inferred_accounts) != 1:
                    raise RuntimeError("validation_error")
                account_id = next(iter(inferred_accounts))
            await self._require_account_permission(auth, account_id, require_trade=True)
            strategy_ids: list[int] = []
            async with self.db.connection() as conn:
                for order_id in order_ids:
                    sid = await self.repo.fetch_order_strategy_id(conn, account_id, order_id)
                    if sid is not None:
                        strategy_ids.append(int(sid))
                await conn.commit()
            for sid in sorted(set(strategy_ids)):
                await self._require_strategy_permission(auth, account_id, sid, for_trade=True)
            return account_id
        if command == "cancel_all_orders":
            if account_id <= 0:
                raise RuntimeError("missing_account_id")
            await self._require_account_permission(auth, account_id, require_trade=True)
            strategy_ids = _parse_int_ids(payload.get("strategy_ids"))
            strategy_ids_csv = _parse_int_ids(payload.get("strategy_ids_csv"))
            all_ids = sorted(set(strategy_ids + strategy_ids_csv))
            for sid in all_ids:
                await self._require_strategy_permission(auth, account_id, sid, for_trade=True)
            return account_id
        if command == "close_position":
            if account_id <= 0:
                raise RuntimeError("missing_account_id")
            await self._require_account_permission(auth, account_id, require_close_position=True)
            position_id = int(payload.get("position_id", 0) or 0)
            if position_id > 0:
                async with self.db.connection() as conn:
                    strategy_id = await self.repo.fetch_position_strategy_id(conn, account_id, position_id)
                    await conn.commit()
                if strategy_id is not None:
                    await self._require_strategy_permission(auth, account_id, strategy_id, for_trade=True)
            return account_id
        if command == "close_by":
            if account_id <= 0:
                raise RuntimeError("missing_account_id")
            await self._require_account_permission(auth, account_id, require_close_position=True)
            pid_a = int(payload.get("position_id_a", 0) or 0)
            pid_b = int(payload.get("position_id_b", 0) or 0)
            async with self.db.connection() as conn:
                sid_a = await self.repo.fetch_position_strategy_id(conn, account_id, pid_a) if pid_a > 0 else None
                sid_b = await self.repo.fetch_position_strategy_id(conn, account_id, pid_b) if pid_b > 0 else None
                await conn.commit()
            if sid_a is not None:
                await self._require_strategy_permission(auth, account_id, sid_a, for_trade=True)
            if sid_b is not None:
                await self._require_strategy_permission(auth, account_id, sid_b, for_trade=True)
            return account_id
        if command == "merge_positions":
            if account_id <= 0:
                raise RuntimeError("missing_account_id")
            await self._require_account_permission(auth, account_id, require_close_position=True)
            src_id = int(payload.get("source_position_id", 0) or 0)
            dst_id = int(payload.get("target_position_id", 0) or 0)
            if src_id <= 0 or dst_id <= 0 or src_id == dst_id:
                raise RuntimeError("validation_error")
            async with self.db.connection() as conn:
                sid_src = await self.repo.fetch_position_strategy_id(conn, account_id, src_id)
                sid_dst = await self.repo.fetch_position_strategy_id(conn, account_id, dst_id)
                await conn.commit()
            if sid_src is None or sid_dst is None:
                raise RuntimeError("position_not_found")
            await self._require_strategy_permission(auth, account_id, int(sid_src), for_trade=True)
            await self._require_strategy_permission(auth, account_id, int(sid_dst), for_trade=True)
            return account_id
        if command == "position_change":
            position_id = int(payload.get("position_id", 0) or 0)
            if position_id <= 0:
                raise RuntimeError("validation_error")
            async with self.db.connection() as conn:
                inferred_account_id = await self.repo.fetch_position_account_id(conn, position_id)
                await conn.commit()
            if inferred_account_id is None:
                raise RuntimeError("position_not_found")
            if account_id > 0 and int(inferred_account_id) != account_id:
                raise RuntimeError("position_not_found")
            account_id = int(inferred_account_id)
            await self._require_account_permission(auth, account_id, require_trade=True)
            async with self.db.connection() as conn:
                strategy_id = await self.repo.fetch_position_strategy_id(conn, account_id, position_id)
                await conn.commit()
            if strategy_id is not None:
                await self._require_strategy_permission(auth, account_id, strategy_id, for_trade=True)
            return account_id
        raise RuntimeError("unsupported_command")

    @staticmethod
    def _ccxt_requires_trade(func: str) -> bool:
        fn = func.lower()
        trade_prefixes = (
            "create_",
            "cancel_",
            "edit_",
            "private_post",
            "private_put",
            "private_delete",
        )
        return fn.startswith(trade_prefixes)

    def _op_requires_account_lock(self, payload: dict[str, Any]) -> bool:
        op = str(payload.get("op", "")).strip().lower()
        if op in {
            "status",
            "authorize_account",
            "oms_query",
            "oms_query_multi",
            "ccxt_raw_query",
            "reconcile_status_account",
            "ws_pull_events",
            "ws_tail_id",
        }:
            return False
        if op == "ccxt_call":
            func = str(payload.get("func", "")).strip()
            return self._ccxt_requires_trade(func)
        # Safe default: lock unknown ops.
        return True

    @staticmethod
    def _op_is_read_dispatch(payload: dict[str, Any]) -> bool:
        op = str(payload.get("op", "")).strip().lower()
        return op in {
            "authorize_account",
            "oms_query",
            "oms_query_multi",
            "ccxt_raw_query",
            "reconcile_status_account",
            "ws_pull_events",
            "ws_tail_id",
        }

    @staticmethod
    def _reconcile_status_of(updated_at: Any, stale_after_seconds: int) -> tuple[str, int | None]:
        if updated_at is None:
            return "never", None
        if isinstance(updated_at, str):
            try:
                updated_dt = datetime.fromisoformat(updated_at)
            except ValueError:
                return "stale", None
        else:
            updated_dt = updated_at
        if updated_dt.tzinfo is None:
            updated_dt = updated_dt.replace(tzinfo=timezone.utc)
        age = int((datetime.now(timezone.utc) - updated_dt).total_seconds())
        return ("fresh" if age <= stale_after_seconds else "stale"), age

    async def start(self) -> None:
        await self.db.connect()
        for engine, pool_size in self.pool_size_by_engine.items():
            for wid in range(pool_size):
                q: asyncio.Queue[_Job] = asyncio.Queue()
                self.worker_queues[engine][wid] = q
                self.worker_tasks[engine][wid] = asyncio.create_task(
                    self._worker_loop(engine, wid, q)
                )
        for wid in range(self.read_pool_size):
            q: asyncio.Queue[_Job] = asyncio.Queue()
            self.read_worker_queues[wid] = q
            self.read_worker_tasks[wid] = asyncio.create_task(self._worker_loop_read(wid, q))
        self.control_task = asyncio.create_task(self._worker_loop_control())
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.settings.dispatcher_host,
            port=self.settings.dispatcher_port,
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self.control_task is not None:
            self.control_task.cancel()
        for workers in self.worker_tasks.values():
            for task in workers.values():
                task.cancel()
        for task in self.read_worker_tasks.values():
            task.cancel()
        await self.ccxt.close_all_sessions()
        await self.db.disconnect()

    async def _resolve_worker_for_account(self, account_id: int) -> tuple[str, int]:
        engine = await self._engine_for_account(account_id)
        engine_pool_size = self.pool_size_by_engine.get(engine, 0)
        if engine_pool_size <= 0:
            raise RuntimeError("unsupported_engine")
        cache_key = (engine, int(account_id))
        cached = self.account_worker.get(cache_key)
        if cached is not None and 0 <= cached < engine_pool_size:
            self._dispatcher_logger_for_hint(engine, cached).info(
                "resolve account_id=%s source=cache worker_id=%s",
                int(account_id),
                int(cached),
            )
            return engine, cached

        hinted: int | None = None
        async with self.db.connection() as conn:
            hinted = await self.repo.fetch_account_dispatcher_worker_hint(conn, account_id)
            await conn.commit()
        if hinted is not None and 0 <= int(hinted) < engine_pool_size:
            wid = int(hinted)
            self.account_worker[cache_key] = wid
            self.worker_active_accounts[engine][wid].add(account_id)
            self._dispatcher_logger_for_hint(engine, wid).info(
                "resolve account_id=%s source=hint worker_id=%s",
                int(account_id),
                int(wid),
            )
            return engine, wid

        # Least-loaded by (inflight + active_accounts).
        wid = min(
            range(engine_pool_size),
            key=lambda w: (
                self.worker_inflight[engine][w],
                len(self.worker_active_accounts[engine][w]),
                w,
            ),
        )
        self.account_worker[cache_key] = wid
        self.worker_active_accounts[engine][wid].add(account_id)
        async with self.db.connection() as conn:
            await self.repo.set_account_dispatcher_worker_hint(conn, account_id, wid)
            await conn.commit()
        self._dispatcher_logger_for_hint(engine, wid).info(
            "resolve account_id=%s source=least_loaded worker_id=%s inflight=%s active_accounts=%s",
            int(account_id),
            int(wid),
            int(self.worker_inflight[engine][wid]),
            int(len(self.worker_active_accounts[engine][wid])),
        )
        return engine, wid

    def _select_least_loaded_worker(self, engine: str) -> int:
        engine_pool_size = int(self.pool_size_by_engine.get(engine, 0) or 0)
        if engine_pool_size <= 0:
            raise RuntimeError("unsupported_engine")
        return min(
            range(engine_pool_size),
            key=lambda w: (
                self.worker_inflight[engine][w],
                self.worker_queues[engine][w].qsize() if w in self.worker_queues[engine] else 0,
                len(self.worker_active_accounts[engine][w]),
                w,
            ),
        )

    async def _worker_loop(self, engine: str, worker_id: int, queue: asyncio.Queue[_Job]) -> None:
        while True:
            job = await queue.get()
            started_at = time.perf_counter()
            queue_wait_ms = max(0.0, (started_at - float(job.enqueued_at or started_at)) * 1000.0)
            try:
                self.worker_inflight[engine][worker_id] += 1
                needs_lock = self._op_requires_account_lock(job.payload)
                if needs_lock:
                    lock = self.account_locks.setdefault(int(job.account_id), asyncio.Lock())
                    async with lock:
                        out = await self._execute(job.payload)
                else:
                    out = await self._execute(job.payload)
                self._maybe_invalidate_oms_query_cache(job.payload, out)
                if not job.future.done():
                    job.future.set_result(out)
            except Exception as exc:
                self.total_errors += 1
                if not job.future.done():
                    job.future.set_result(
                        {"ok": False, "error": {"code": "dispatcher_error", "message": str(exc)}}
                    )
            finally:
                execute_ms = (time.perf_counter() - started_at) * 1000.0
                self._queue_wait_samples_ms.append(float(queue_wait_ms))
                self._execute_samples_ms.append(float(execute_ms))
                self._record_op_timing(job.payload, execute_ms)
                self._dispatcher_logger_for_hint(engine, worker_id).info(
                    "worker_done engine=%s worker_id=%s account_id=%s op=%s queue_wait_ms=%s execute_ms=%s inflight=%s queue_depth=%s",
                    str(engine),
                    int(worker_id),
                    int(job.account_id),
                    str(job.payload.get("op", "")),
                    round(queue_wait_ms, 2),
                    round(execute_ms, 2),
                    int(self.worker_inflight[engine][worker_id]),
                    int(queue.qsize()),
                )
                self.worker_inflight[engine][worker_id] = max(
                    0, self.worker_inflight[engine][worker_id] - 1
                )
                queue.task_done()

    async def _worker_loop_control(self) -> None:
        while True:
            job = await self.control_queue.get()
            started_at = time.perf_counter()
            try:
                out = await self._execute(job.payload)
                self._maybe_invalidate_oms_query_cache(job.payload, out)
                if not job.future.done():
                    job.future.set_result(out)
            except Exception as exc:
                self.total_errors += 1
                if not job.future.done():
                    job.future.set_result(
                        {"ok": False, "error": {"code": "dispatcher_error", "message": str(exc)}}
                    )
            finally:
                execute_ms = (time.perf_counter() - started_at) * 1000.0
                self._execute_samples_ms.append(float(execute_ms))
                self._record_op_timing(job.payload, execute_ms)
                self.control_queue.task_done()

    async def _worker_loop_read(self, worker_id: int, queue: asyncio.Queue[_Job]) -> None:
        while True:
            job = await queue.get()
            started_at = time.perf_counter()
            queue_wait_ms = max(0.0, (started_at - float(job.enqueued_at or started_at)) * 1000.0)
            try:
                self.read_worker_inflight[worker_id] += 1
                out = await self._execute(job.payload)
                self._maybe_invalidate_oms_query_cache(job.payload, out)
                if not job.future.done():
                    job.future.set_result(out)
            except Exception as exc:
                self.total_errors += 1
                if not job.future.done():
                    job.future.set_result(
                        {"ok": False, "error": {"code": "dispatcher_error", "message": str(exc)}}
                    )
            finally:
                execute_ms = (time.perf_counter() - started_at) * 1000.0
                self._queue_wait_samples_ms.append(float(queue_wait_ms))
                self._execute_samples_ms.append(float(execute_ms))
                self._record_op_timing(job.payload, execute_ms)
                self.loggers.get("dispatcher").info(
                    "read_worker_done worker_id=%s account_id=%s op=%s queue_wait_ms=%s execute_ms=%s inflight=%s queue_depth=%s",
                    int(worker_id),
                    int(job.account_id),
                    str(job.payload.get("op", "")),
                    round(queue_wait_ms, 2),
                    round(execute_ms, 2),
                    int(self.read_worker_inflight[worker_id]),
                    int(queue.qsize()),
                )
                self.read_worker_inflight[worker_id] = max(0, self.read_worker_inflight[worker_id] - 1)
                queue.task_done()

    async def _dispatch_to_account(self, account_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        try:
            needs_lock = self._op_requires_account_lock(payload)
            if needs_lock:
                engine, wid = await self._resolve_worker_for_account(account_id)
            else:
                engine = await self._engine_for_account(account_id)
                wid = self._select_least_loaded_worker(engine)
        except RuntimeError as exc:
            code = str(exc)
            if code not in {"unsupported_engine", "account_not_found"}:
                code = "dispatcher_error"
            return {"ok": False, "error": {"code": code, "message": str(exc)}}
        logger = self._dispatcher_logger_for_hint(engine, wid)
        enqueued_at = time.perf_counter()
        logger.info(
            "enqueue engine=%s worker_id=%s account_id=%s op=%s inflight=%s queue_depth_before=%s",
            str(engine),
            int(wid),
            int(account_id),
            str(payload.get("op", "")),
            int(self.worker_inflight[engine][wid]),
            int(self.worker_queues[engine][wid].qsize()),
        )
        await self.worker_queues[engine][wid].put(
            _Job(account_id=account_id, payload=payload, future=fut, enqueued_at=time.perf_counter())
        )
        out = await fut
        logger.info(
            "result engine=%s worker_id=%s account_id=%s op=%s elapsed_ms=%s ok=%s",
            str(engine),
            int(wid),
            int(account_id),
            str(payload.get("op", "")),
            round((time.perf_counter() - enqueued_at) * 1000, 2),
            bool(isinstance(out, dict) and out.get("ok")),
        )
        if isinstance(out, dict):
            return out
        return {"ok": False, "error": {"code": "dispatcher_error", "message": "invalid_worker_response"}}

    async def _dispatch_to_control(self, payload: dict[str, Any]) -> dict[str, Any]:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self.control_queue.put(_Job(account_id=0, payload=payload, future=fut))
        out = await fut
        if isinstance(out, dict):
            return out
        return {"ok": False, "error": {"code": "dispatcher_error", "message": "invalid_worker_response"}}

    async def _dispatch_to_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        account_id = int(payload.get("account_id", 0) or 0)
        wid = min(
            range(self.read_pool_size),
            key=lambda w: (
                self.read_worker_inflight[w],
                self.read_worker_queues[w].qsize() if w in self.read_worker_queues else 0,
                w,
            ),
        )
        enqueued_at = time.perf_counter()
        self.loggers.get("dispatcher").info(
            "read_enqueue worker_id=%s account_id=%s op=%s inflight=%s queue_depth_before=%s",
            int(wid),
            int(account_id),
            str(payload.get("op", "")),
            int(self.read_worker_inflight[wid]),
            int(self.read_worker_queues[wid].qsize()),
        )
        await self.read_worker_queues[wid].put(
            _Job(account_id=account_id, payload=payload, future=fut, enqueued_at=time.perf_counter())
        )
        out = await fut
        self.loggers.get("dispatcher").info(
            "read_result worker_id=%s account_id=%s op=%s elapsed_ms=%s ok=%s",
            int(wid),
            int(account_id),
            str(payload.get("op", "")),
            round((time.perf_counter() - enqueued_at) * 1000, 2),
            bool(isinstance(out, dict) and out.get("ok")),
        )
        if isinstance(out, dict):
            return out
        return {"ok": False, "error": {"code": "dispatcher_error", "message": "invalid_worker_response"}}

    async def _publish_event(self, account_id: int, event: dict[str, Any]) -> None:
        self._ws_event_seq += 1
        seq = int(self._ws_event_seq)
        entry = {
            "id": seq,
            "namespace": str(event.get("namespace", "")),
            "event_type": str(event.get("event_type", "")),
            "payload": event.get("payload") if isinstance(event.get("payload"), dict) else {},
            "created_at": str(event.get("created_at") or datetime.now(timezone.utc).isoformat()),
        }
        bucket = self._ws_events_by_account.setdefault(account_id, deque(maxlen=self._ws_event_buffer_limit))
        bucket.append(entry)

    def _pull_ws_events(self, account_id: int, from_event_id: int, limit: int) -> list[dict[str, Any]]:
        bucket = self._ws_events_by_account.get(account_id)
        if not bucket:
            return []
        out: list[dict[str, Any]] = []
        for ev in bucket:
            if int(ev["id"]) > int(from_event_id):
                out.append(ev)
                if len(out) >= limit:
                    break
        return out

    async def _execute_oms_commands_batch(self, msg: dict[str, Any]) -> dict[str, Any]:
        items = msg.get("items")
        if not isinstance(items, list):
            return {"ok": False, "error": {"code": "validation_error", "message": "items must be a list"}}
        parallel = bool(msg.get("parallel", False))
        x_api_key = str(msg.get("x_api_key", "") or "")
        requests: list[dict[str, Any]] = []
        for index, raw in enumerate(items):
            if not isinstance(raw, dict):
                requests.append(
                    {
                        "index": index,
                        "ok": False,
                        "command_id": None,
                        "order_id": None,
                        "error": {"code": "validation_error", "message": "item must be an object"},
                    }
                )
                continue
            item = dict(raw)
            try:
                item["account_id"] = int(item.get("account_id", 0) or 0)
            except Exception:
                item["account_id"] = 0
            requests.append(
                {
                    "index": index,
                    "account_id": int(item["account_id"]),
                    "payload": {
                        "op": "oms_command",
                        "x_api_key": x_api_key,
                        "account_id": int(item["account_id"]),
                        "index": index,
                        "item": item,
                    },
                }
            )

        async def _run_one(req: dict[str, Any]) -> dict[str, Any]:
            if "payload" not in req:
                return req
            account_id = int(req["account_id"])
            if account_id > 0:
                out = await self._dispatch_to_account(account_id, req["payload"])
            else:
                out = await self._dispatch_to_control(req["payload"])
            if out.get("ok") and isinstance(out.get("result"), dict):
                return out["result"]
            return {
                "index": int(req["index"]),
                "ok": False,
                "command_id": None,
                "order_id": None,
                "error": out.get("error") or {"code": "dispatcher_error"},
            }

        if parallel:
            results = await asyncio.gather(*[_run_one(req) for req in requests])
        else:
            results = []
            for req in requests:
                results.append(await _run_one(req))
        return {"ok": True, "result": {"results": results}}

    async def _execute_ccxt_batch(self, msg: dict[str, Any]) -> dict[str, Any]:
        items = msg.get("items")
        if not isinstance(items, list):
            return {"ok": False, "error": {"code": "validation_error", "message": "items must be a list"}}
        parallel = bool(msg.get("parallel", False))
        x_api_key = str(msg.get("x_api_key", "") or "")
        requests: list[dict[str, Any]] = []
        for index, raw in enumerate(items):
            if not isinstance(raw, dict):
                requests.append(
                    {
                        "index": index,
                        "ok": False,
                        "error": {"code": "validation_error", "message": "item must be an object"},
                    }
                )
                continue
            try:
                account_id = int(raw.get("account_id", 0) or 0)
            except Exception:
                account_id = 0
            func = str(raw.get("func", "")).strip()
            args = raw.get("args") if isinstance(raw.get("args"), list) else []
            kwargs = raw.get("kwargs") if isinstance(raw.get("kwargs"), dict) else {}
            requests.append(
                {
                    "index": index,
                    "account_id": account_id,
                    "payload": {
                        "op": "ccxt_call",
                        "x_api_key": x_api_key,
                        "account_id": account_id,
                        "func": func,
                        "args": args,
                        "kwargs": kwargs,
                    },
                }
            )

        async def _run_one(req: dict[str, Any]) -> dict[str, Any]:
            if "payload" not in req:
                return req
            account_id = int(req["account_id"])
            if account_id <= 0:
                return {"index": int(req["index"]), "ok": False, "error": {"code": "missing_account_id"}}
            out = await self._dispatch_to_account(account_id, req["payload"])
            if out.get("ok"):
                return {"index": int(req["index"]), "ok": True, "result": out.get("result")}
            return {
                "index": int(req["index"]),
                "ok": False,
                "error": out.get("error") or {"code": "ccxt_error"},
            }

        if parallel:
            results = await asyncio.gather(*[_run_one(req) for req in requests])
        else:
            results = []
            for req in requests:
                results.append(await _run_one(req))
        return {"ok": True, "result": {"results": results}}

    async def _execute(self, msg: dict[str, Any]) -> dict[str, Any]:
        op = str(msg.get("op", "")).strip()
        self.op_counts[op] = self.op_counts.get(op, 0) + 1
        if op == "oms_command":
            auth = await self._auth_from_payload(msg)
            index = int(msg.get("index", 0) or 0)
            raw_item = msg.get("item") if isinstance(msg.get("item"), dict) else {}
            command = str(raw_item.get("command", "")).strip()
            payload_raw = raw_item.get("payload")
            if command in {"send_order", "close_position"} and isinstance(payload_raw, dict):
                reason_raw = payload_raw.get("reason")
                if reason_raw is None or not str(reason_raw).strip():
                    payload_raw["reason"] = self._default_reason_for_role(auth.role)
            item = COMMAND_INPUT_ADAPTER.validate_python(raw_item)
            resolved_account_id = await self._require_oms_command_permission(auth, item)
            item = item.model_copy(update={"account_id": resolved_account_id})
            result = await process_single_command_direct(
                db=self.db,
                repo=self.repo,
                ccxt_adapter=self.ccxt,
                credentials_codec=self.codec,
                auth=auth,
                item=item,
                index=index,
            )
            if bool(getattr(result, "ok", False)):
                self._oms_query_cache_invalidate_accounts([int(resolved_account_id)])
            return {"ok": True, "result": result.model_dump()}

        if op == "authorize_account":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            require_trade = bool(msg.get("require_trade", False))
            for_ws = bool(msg.get("for_ws", False))
            account = await self._require_account_permission(
                auth, account_id, require_trade=require_trade, for_ws=for_ws
            )
            try:
                exchange_id = self._exchange_engine_id(account.get("exchange_id"))
            except RuntimeError:
                return {"ok": False, "error": {"code": "unsupported_engine"}}
            return {
                "ok": True,
                "result": {
                    "account_id": account_id,
                    "exchange_id": exchange_id,
                },
            }

        if op == "meta_ccxt_exchanges":
            await self._auth_from_payload(msg)
            ccxt_items = sorted([f"ccxt.{str(x)}" for x in list(getattr(ccxt_async, "exchanges", []))])
            pro_items: list[str] = []
            if ccxt_pro is not None:
                pro_items = sorted([f"ccxtpro.{str(x)}" for x in list(getattr(ccxt_pro, "exchanges", []))])
            return {"ok": True, "result": sorted(set([*ccxt_items, *pro_items]))}

        if op == "ccxt_call":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            func = str(msg.get("func", "")).strip()
            args = msg.get("args") if isinstance(msg.get("args"), list) else []
            kwargs = msg.get("kwargs") if isinstance(msg.get("kwargs"), dict) else {}
            if self._normalize_role(auth.role) == "admin" and self._ccxt_requires_trade(func):
                return {"ok": False, "error": {"code": "permission_denied", "message": "admin_read_only"}}
            account = await self._require_account_permission(
                auth, account_id, require_trade=self._ccxt_requires_trade(func)
            )
            async with self.db.connection() as conn:
                _, is_testnet, api_key_enc, secret_enc, passphrase_enc, extra_config = await self.repo.fetch_account_exchange_credentials(
                    conn, account_id
                )
                await conn.commit()
            api_key = self.codec.decrypt_maybe(api_key_enc)
            secret = self.codec.decrypt_maybe(secret_enc)
            passphrase = self.codec.decrypt_maybe(passphrase_enc)
            try:
                exchange_id = self._exchange_engine_id(account.get("exchange_id"))
                engine = self._engine_of_exchange_id(exchange_id)
            except RuntimeError:
                return {"ok": False, "error": {"code": "unsupported_engine"}}
            if engine == "ccxtpro" and ccxt_pro is None:
                return {"ok": False, "error": {"code": "engine_unavailable", "message": "ccxtpro"}}
            cache_key = (engine, int(account_id))
            hint_id = self.account_worker.get(cache_key)
            if hint_id is None or not (0 <= int(hint_id) < self.pool_size_by_engine[engine]):
                _, hint_id = await self._resolve_worker_for_account(account_id)
            result = await self.ccxt.execute_method(
                exchange_id=exchange_id,
                use_testnet=is_testnet,
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
                extra_config=extra_config,
                method=func,
                args=args,
                kwargs=kwargs,
                logger=self._ccxt_logger_for_hint(engine, int(hint_id)),
                session_key=f"account:{int(account_id)}",
            )
            return {"ok": True, "result": result}

        if op == "oms_query":
            auth = await self._auth_from_payload(msg)
            query = str(msg.get("query", "")).strip()
            account_id = int(msg.get("account_id", 0) or 0)
            use_cache = bool(msg.get("cache", True))
            strategy_id_raw = msg.get("strategy_id")
            date_from_raw = msg.get("date_from")
            date_to_raw = msg.get("date_to")
            open_limit_raw = msg.get("open_limit")
            date_from = None if date_from_raw in {None, ""} else str(date_from_raw).strip()
            date_to = None if date_to_raw in {None, ""} else str(date_to_raw).strip()
            try:
                open_limit = int(open_limit_raw or 500)
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "open_limit must be integer"}}
            try:
                strategy_id = None if strategy_id_raw in {None, ""} else int(strategy_id_raw)
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "strategy_id must be integer"}}
            if (date_from and not date_to) or (date_to and not date_from):
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "date_from and date_to must be provided together"},
                }
            await self._require_account_permission(auth, account_id, require_trade=False)
            cache_key = self._make_oms_query_cache_key(
                api_key_id=auth.api_key_id,
                query=query,
                account_ids=[account_id],
                strategy_id=strategy_id,
                date_from=date_from,
                date_to=date_to,
                open_limit=open_limit,
                page=None,
                page_size=None,
            )
            if use_cache:
                cached = self._oms_query_cache_get(cache_key)
                if cached is not None:
                    return {"ok": True, "result": cached}
            async with self.db.connection() as conn:
                perms = await self.repo.fetch_api_key_account_permissions(conn, auth.api_key_id, account_id)
                if perms is None or not bool(perms.get("can_read")):
                    await conn.commit()
                    return {"ok": False, "error": {"code": "permission_denied"}}
                if bool(perms.get("restrict_to_strategies")):
                    if strategy_id is None:
                        await conn.commit()
                        return {
                            "ok": False,
                            "error": {"code": "strategy_required", "message": "strategy_id is required for this key"},
                        }
                    allowed = await self.repo.api_key_strategy_allowed(
                        conn, auth.api_key_id, account_id, strategy_id, for_trade=False
                    )
                    if not allowed:
                        await conn.commit()
                        return {"ok": False, "error": {"code": "strategy_permission_denied"}}
                if query == "orders_open":
                    rows = await self.repo.list_orders(
                        conn, account_id, open_only=True, strategy_id=strategy_id, open_limit=open_limit
                    )
                elif query == "orders_history":
                    rows = await self.repo.list_orders(
                        conn, account_id, open_only=False, strategy_id=strategy_id, date_from=date_from, date_to=date_to
                    )
                elif query == "deals":
                    rows = await self.repo.list_deals(
                        conn, account_id, strategy_id=strategy_id, date_from=date_from, date_to=date_to
                    )
                elif query == "positions_open":
                    rows = await self.repo.list_positions(
                        conn, account_id, open_only=True, strategy_id=strategy_id, open_limit=open_limit
                    )
                elif query == "positions_history":
                    rows = await self.repo.list_positions(
                        conn, account_id, open_only=False, strategy_id=strategy_id, date_from=date_from, date_to=date_to
                    )
                else:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "unsupported_query"}}
                await conn.commit()
            if use_cache:
                self._oms_query_cache_set(cache_key, rows, [account_id])
            return {"ok": True, "result": rows}

        if op == "oms_query_multi":
            auth = await self._auth_from_payload(msg)
            query = str(msg.get("query", "")).strip()
            use_cache = bool(msg.get("cache", True))
            raw_account_ids = msg.get("account_ids")
            strategy_id_raw = msg.get("strategy_id")
            date_from_raw = msg.get("date_from")
            date_to_raw = msg.get("date_to")
            open_limit_raw = msg.get("open_limit")
            page_raw = msg.get("page")
            page_size_raw = msg.get("page_size")
            date_from = None if date_from_raw in {None, ""} else str(date_from_raw).strip()
            date_to = None if date_to_raw in {None, ""} else str(date_to_raw).strip()
            if (date_from and not date_to) or (date_to and not date_from):
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "date_from and date_to must be provided together"},
                }
            try:
                strategy_id = None if strategy_id_raw in {None, ""} else int(strategy_id_raw)
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "strategy_id must be integer"}}
            try:
                open_limit = int(open_limit_raw or 500)
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "open_limit must be integer"}}
            try:
                page = max(1, int(page_raw or 1))
                page_size = max(1, min(500, int(page_size_raw or 100)))
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "page/page_size must be integer"}}
            account_ids: list[int] = []
            seen_ids: set[int] = set()
            if isinstance(raw_account_ids, list):
                for raw in raw_account_ids:
                    try:
                        aid = int(raw or 0)
                    except Exception:
                        aid = 0
                    if aid <= 0 or aid in seen_ids:
                        continue
                    seen_ids.add(aid)
                    account_ids.append(aid)
            elif isinstance(raw_account_ids, str):
                for part in raw_account_ids.split(","):
                    raw = part.strip()
                    if not raw.isdigit():
                        continue
                    aid = int(raw)
                    if aid <= 0 or aid in seen_ids:
                        continue
                    seen_ids.add(aid)
                    account_ids.append(aid)
            if not account_ids:
                return {"ok": False, "error": {"code": "validation_error", "message": "account_ids is required"}}
            cache_key = self._make_oms_query_cache_key(
                api_key_id=auth.api_key_id,
                query=query,
                account_ids=account_ids,
                strategy_id=strategy_id,
                date_from=date_from,
                date_to=date_to,
                open_limit=open_limit,
                page=page,
                page_size=page_size,
            )
            if use_cache:
                cached = self._oms_query_cache_get(cache_key)
                if cached is not None:
                    return {"ok": True, "result": cached}
            async with self.db.connection() as conn:
                perms_by_account = await self.repo.fetch_api_key_account_permissions_multi(
                    conn, auth.api_key_id, account_ids
                )
                for aid in account_ids:
                    perms = perms_by_account.get(int(aid))
                    if perms is None or str(perms.get("account_status", "")).strip().lower() != "active":
                        await conn.commit()
                        return {"ok": False, "error": {"code": "account_not_found", "message": f"account_id={aid}"}}
                    if not bool(perms.get("can_read")):
                        await conn.commit()
                        return {"ok": False, "error": {"code": "permission_denied", "message": f"account_id={aid}"}}
                    if bool(perms.get("restrict_to_strategies")):
                        if strategy_id is None:
                            await conn.commit()
                            return {
                                "ok": False,
                                "error": {
                                    "code": "strategy_required",
                                    "message": f"strategy_id is required for account_id={aid}",
                                },
                            }
                        allowed = await self.repo.api_key_strategy_allowed(
                            conn, auth.api_key_id, aid, strategy_id, for_trade=False
                        )
                        if not allowed:
                            await conn.commit()
                            return {"ok": False, "error": {"code": "strategy_permission_denied"}}
                if query == "orders_open":
                    rows = await self.repo.list_orders_multi(
                        conn, account_ids=account_ids, open_only=True, strategy_id=strategy_id, open_limit=open_limit
                    )
                    result: Any = rows
                elif query == "orders_history":
                    rows, total = await self.repo.list_orders_multi_paged(
                        conn,
                        account_ids=account_ids,
                        strategy_id=strategy_id,
                        date_from=date_from,
                        date_to=date_to,
                        page=page,
                        page_size=page_size,
                    )
                    result = {"items": rows, "total": int(total), "page": int(page), "page_size": int(page_size)}
                elif query == "deals":
                    rows, total = await self.repo.list_deals_multi_paged(
                        conn,
                        account_ids=account_ids,
                        strategy_id=strategy_id,
                        date_from=date_from,
                        date_to=date_to,
                        page=page,
                        page_size=page_size,
                    )
                    result = {"items": rows, "total": int(total), "page": int(page), "page_size": int(page_size)}
                elif query == "positions_open":
                    rows = await self.repo.list_positions_multi(
                        conn,
                        account_ids=account_ids,
                        open_only=True,
                        strategy_id=strategy_id,
                        open_limit=open_limit,
                    )
                    result = rows
                elif query == "positions_history":
                    rows, total = await self.repo.list_positions_multi_paged(
                        conn,
                        account_ids=account_ids,
                        strategy_id=strategy_id,
                        date_from=date_from,
                        date_to=date_to,
                        page=page,
                        page_size=page_size,
                    )
                    result = {"items": rows, "total": int(total), "page": int(page), "page_size": int(page_size)}
                else:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "unsupported_query"}}
                await conn.commit()
            if use_cache:
                self._oms_query_cache_set(cache_key, result, account_ids)
            return {"ok": True, "result": result}

        if op == "ccxt_raw_query":
            auth = await self._auth_from_payload(msg)
            query = str(msg.get("query", "")).strip()
            account_id = int(msg.get("account_id", 0) or 0)
            date_from_raw = msg.get("date_from")
            date_to_raw = msg.get("date_to")
            date_from = None if date_from_raw in {None, ""} else str(date_from_raw).strip()
            date_to = None if date_to_raw in {None, ""} else str(date_to_raw).strip()
            if not date_from or not date_to:
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "date_from and date_to are required"},
                }
            await self._require_account_permission(auth, account_id, require_trade=False)
            async with self.db.connection() as conn:
                perms = await self.repo.fetch_api_key_account_permissions(conn, auth.api_key_id, account_id)
                if perms is None or not bool(perms.get("can_read")):
                    await conn.commit()
                    return {"ok": False, "error": {"code": "permission_denied"}}
                if query == "orders_raw":
                    rows = await self.repo.list_ccxt_orders_raw(conn, account_id, date_from=date_from, date_to=date_to)
                elif query == "trades_raw":
                    rows = await self.repo.list_ccxt_trades_raw(conn, account_id, date_from=date_from, date_to=date_to)
                else:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "unsupported_query"}}
                await conn.commit()
            return {"ok": True, "result": rows}

        if op == "ccxt_raw_query_multi":
            auth = await self._auth_from_payload(msg)
            query = str(msg.get("query", "")).strip()
            raw_account_ids = msg.get("account_ids")
            date_from_raw = msg.get("date_from")
            date_to_raw = msg.get("date_to")
            page_raw = msg.get("page")
            page_size_raw = msg.get("page_size")

            account_ids: list[int] = []
            seen_ids: set[int] = set()
            if isinstance(raw_account_ids, list):
                for raw in raw_account_ids:
                    try:
                        aid = int(raw or 0)
                    except Exception:
                        aid = 0
                    if aid <= 0 or aid in seen_ids:
                        continue
                    seen_ids.add(aid)
                    account_ids.append(aid)
            elif isinstance(raw_account_ids, str):
                for part in raw_account_ids.split(","):
                    text = str(part).strip()
                    if not text.isdigit():
                        continue
                    aid = int(text)
                    if aid <= 0 or aid in seen_ids:
                        continue
                    seen_ids.add(aid)
                    account_ids.append(aid)
            if not account_ids:
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "account_ids is required"},
                }

            date_from = None if date_from_raw in {None, ""} else str(date_from_raw).strip()
            date_to = None if date_to_raw in {None, ""} else str(date_to_raw).strip()
            if not date_from or not date_to:
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "date_from and date_to are required"},
                }
            try:
                page = max(1, int(page_raw or 1))
                page_size = max(1, min(500, int(page_size_raw or 100)))
            except Exception:
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "page and page_size must be integers"},
                }
            offset = (page - 1) * page_size

            for aid in account_ids:
                await self._require_account_permission(auth, aid, require_trade=False)

            async with self.db.connection() as conn:
                if query == "orders_raw":
                    total = await self.repo.count_ccxt_orders_raw_multi(
                        conn,
                        account_ids=account_ids,
                        date_from=date_from,
                        date_to=date_to,
                    )
                    rows = await self.repo.list_ccxt_orders_raw_multi(
                        conn,
                        account_ids=account_ids,
                        date_from=date_from,
                        date_to=date_to,
                        limit=page_size,
                        offset=offset,
                    )
                elif query == "trades_raw":
                    total = await self.repo.count_ccxt_trades_raw_multi(
                        conn,
                        account_ids=account_ids,
                        date_from=date_from,
                        date_to=date_to,
                    )
                    rows = await self.repo.list_ccxt_trades_raw_multi(
                        conn,
                        account_ids=account_ids,
                        date_from=date_from,
                        date_to=date_to,
                        limit=page_size,
                        offset=offset,
                    )
                else:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "unsupported_query"}}
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "items": self._decorate_exchange_ids(rows),
                    "total": int(total),
                    "page": int(page),
                    "page_size": int(page_size),
                },
            }

        if op == "oms_reassign":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            account_ids_raw = msg.get("account_ids")
            account_ids: list[int] = []
            if isinstance(account_ids_raw, list):
                account_ids = [int(x) for x in account_ids_raw if int(x or 0) > 0]
            elif isinstance(account_ids_raw, str):
                account_ids = [int(x.strip()) for x in account_ids_raw.split(",") if x.strip().isdigit() and int(x.strip()) > 0]
            if account_id > 0:
                account_ids = [account_id]
            account_ids = sorted(set([int(x) for x in account_ids if int(x) > 0]))
            if not account_ids:
                async with self.db.connection() as conn:
                    if auth.is_admin:
                        rows = await self.repo.list_accounts_admin(conn)
                        candidate_ids = [int(r.get("account_id", 0)) for r in rows if int(r.get("account_id", 0)) > 0]
                    else:
                        rows = await self.repo.list_accounts_for_api_key(conn, auth.api_key_id)
                        candidate_ids = [
                            int(r.get("account_id", 0))
                            for r in rows
                            if int(r.get("account_id", 0)) > 0
                            and (bool(r.get("can_trade")) or bool(r.get("can_risk_manage")))
                        ]
                    await conn.commit()
                account_ids = sorted(set(candidate_ids))
            if not account_ids:
                return {"ok": False, "error": {"code": "validation_error", "message": "no eligible accounts for reassign"}}

            deal_ids = [int(x) for x in (msg.get("deal_ids") if isinstance(msg.get("deal_ids"), list) else []) if int(x or 0) > 0]
            order_ids = [int(x) for x in (msg.get("order_ids") if isinstance(msg.get("order_ids"), list) else []) if int(x or 0) > 0]
            target_strategy_id = int(msg.get("target_strategy_id", 0) or 0)
            target_position_id_raw = msg.get("target_position_id")
            target_position_id = None if target_position_id_raw is None else int(target_position_id_raw or 0)
            date_from = None if msg.get("date_from") is None else str(msg.get("date_from"))
            date_to = None if msg.get("date_to") is None else str(msg.get("date_to"))
            reconciled = msg.get("reconciled")
            order_statuses = msg.get("order_statuses") if isinstance(msg.get("order_statuses"), list) else []
            preview = bool(msg.get("preview", False))
            page = max(1, int(msg.get("page", 1) or 1))
            page_size = max(1, min(500, int(msg.get("page_size", 100) or 100)))

            for aid in account_ids:
                if not await self._can_reassign_account(auth, aid):
                    return {"ok": False, "error": {"code": "permission_denied", "message": f"reassign_not_allowed account_id={aid}"}}

            async with self.db.connection() as conn:
                offset = (page - 1) * page_size
                # Post-trading reassignment is intentionally order-only.
                if deal_ids:
                    await conn.commit()
                    return {
                        "ok": False,
                        "error": {
                            "code": "validation_error",
                            "message": "order_only_reassign: deal_ids are not supported",
                        },
                    }

                order_preview_items, orders_total = await self.repo.list_reassign_order_candidates(
                    conn,
                    account_ids=account_ids,
                    order_ids=order_ids,
                    date_from=date_from,
                    date_to=date_to,
                    statuses=order_statuses,
                    reconciled=reconciled,
                    limit=page_size,
                    offset=offset,
                )
                deals_total = 0
                if preview:
                    await conn.commit()
                    merged_items = sorted(
                        [*order_preview_items],
                        key=lambda x: (int(x.get("account_id", 0)), str(x.get("kind", "")), int(x.get("id", 0))),
                    )
                    return {
                        "ok": True,
                        "result": {
                            "preview": True,
                            "deals_updated": 0,
                            "orders_updated": 0,
                            "deals_total": int(deals_total),
                            "orders_total": int(orders_total),
                            "page": page,
                            "page_size": page_size,
                            "items": self._decorate_exchange_ids(merged_items),
                        },
                    }

                if target_strategy_id <= 0:
                    await conn.commit()
                    return {
                        "ok": False,
                        "error": {
                            "code": "validation_error",
                            "message": "target_strategy_id must be > 0",
                        },
                    }
                if target_position_id is not None and int(target_position_id) < 0:
                    await conn.commit()
                    return {
                        "ok": False,
                        "error": {
                            "code": "validation_error",
                            "message": "target_position_id must be >= 0",
                        },
                    }

                scoped_order_ids_by_account: dict[int, list[int]] = {}
                scoped_orders_by_account: dict[int, list[dict[str, Any]]] = {}
                if order_ids:
                    for oid in sorted(set(order_ids)):
                        row = await self.repo.admin_fetch_oms_order_by_id(conn, oid)
                        if row is None:
                            await conn.commit()
                            return {
                                "ok": False,
                                "error": {"code": "validation_error", "message": f"order_not_found id={oid}"},
                            }
                        aid = int(row.get("account_id", 0) or 0)
                        if aid not in account_ids:
                            await conn.commit()
                            return {
                                "ok": False,
                                "error": {
                                    "code": "validation_error",
                                    "message": f"order_not_allowed_for_selected_accounts id={oid}",
                                },
                            }
                        scoped_order_ids_by_account.setdefault(aid, []).append(int(oid))
                        scoped_orders_by_account.setdefault(aid, []).append(row)
                else:
                    for row in order_preview_items:
                        oid = int(row.get("id", 0) or 0)
                        aid = int(row.get("account_id", 0) or 0)
                        if oid <= 0 or aid <= 0:
                            continue
                        scoped_order_ids_by_account.setdefault(aid, []).append(oid)
                    for aid, ids in scoped_order_ids_by_account.items():
                        loaded: list[dict[str, Any]] = []
                        for oid in sorted(set(ids)):
                            row = await self.repo.admin_fetch_oms_order_by_id(conn, oid, aid)
                            if row is not None:
                                loaded.append(row)
                        scoped_orders_by_account[aid] = loaded
                    for aid in list(scoped_order_ids_by_account.keys()):
                        scoped_order_ids_by_account[aid] = sorted(
                            set([int(x) for x in scoped_order_ids_by_account[aid] if int(x) > 0])
                        )

                final_order_ids = sorted(
                    set(
                        [
                            oid
                            for ids in scoped_order_ids_by_account.values()
                            for oid in ids
                            if int(oid) > 0
                        ]
                    )
                )
                if not final_order_ids:
                    await conn.commit()
                    return {
                        "ok": True,
                        "result": {
                            "preview": False,
                            "deals_updated": 0,
                            "orders_updated": 0,
                            "deals_total": int(deals_total),
                            "orders_total": int(orders_total),
                            "page": page,
                            "page_size": page_size,
                            "items": [],
                        },
                    }

                # Safety checks before mutating any row.
                if target_position_id and len(scoped_order_ids_by_account.keys()) > 1:
                    await conn.commit()
                    return {
                        "ok": False,
                        "error": {
                            "code": "validation_error",
                            "message": "target_position_id requires exactly one account scope",
                        },
                    }
                target_position_row: dict[str, Any] | None = None
                if target_position_id and int(target_position_id) > 0:
                    target_position_row = await self.repo.admin_fetch_oms_position_by_id(conn, int(target_position_id))
                    if target_position_row is None:
                        await conn.commit()
                        return {
                            "ok": False,
                            "error": {
                                "code": "validation_error",
                                "message": f"target_position_not_found id={int(target_position_id)}",
                            },
                        }

                orders_count = 0
                deals_count = 0
                positions_count = 0
                before_by_account: dict[int, dict[str, list[dict[str, Any]]]] = {}
                affected_accounts: set[int] = set()
                for aid in sorted(scoped_order_ids_by_account.keys()):
                    scoped_order_ids = sorted(set(scoped_order_ids_by_account.get(aid, [])))
                    scoped_orders = scoped_orders_by_account.get(aid, [])
                    if not scoped_order_ids:
                        continue

                    strategy_ok = await self.repo.strategy_exists_for_account(conn, aid, target_strategy_id)
                    if not strategy_ok:
                        await conn.commit()
                        return {
                            "ok": False,
                            "error": {
                                "code": "validation_error",
                                "message": f"strategy_not_allowed account_id={aid} strategy_id={target_strategy_id}",
                            },
                        }

                    for order_row in scoped_orders:
                        current_strategy_id = int(order_row.get("strategy_id", 0) or 0)
                        current_position_id = int(order_row.get("position_id", 0) or 0)
                        if current_strategy_id > 0 or current_position_id > 0:
                            await conn.commit()
                            return {
                                "ok": False,
                                "error": {
                                    "code": "validation_error",
                                    "message": (
                                        f"order_already_assigned id={int(order_row.get('id', 0))} "
                                        f"strategy_id={current_strategy_id} position_id={current_position_id}"
                                    ),
                                },
                            }

                        if target_position_row is not None:
                            pos_account_id = int(target_position_row.get("account_id", 0) or 0)
                            if pos_account_id != aid:
                                await conn.commit()
                                return {
                                    "ok": False,
                                    "error": {
                                        "code": "validation_error",
                                        "message": "target_position_account_mismatch",
                                    },
                                }
                            order_symbol = str(order_row.get("symbol", "") or "")
                            pos_symbol = str(target_position_row.get("symbol", "") or "")
                            if order_symbol and pos_symbol and (order_symbol.upper() != pos_symbol.upper()):
                                await conn.commit()
                                return {
                                    "ok": False,
                                    "error": {
                                        "code": "validation_error",
                                        "message": (
                                            f"target_position_symbol_mismatch order_id={int(order_row.get('id', 0))} "
                                            f"order_symbol={order_symbol} position_symbol={pos_symbol}"
                                        ),
                                    },
                                }
                            order_side = str(order_row.get("side", "") or "").lower()
                            pos_side = str(target_position_row.get("side", "") or "").lower()
                            if order_side and pos_side and (order_side != pos_side):
                                await conn.commit()
                                return {
                                    "ok": False,
                                    "error": {
                                        "code": "validation_error",
                                        "message": (
                                            f"target_position_side_mismatch order_id={int(order_row.get('id', 0))} "
                                            f"order_side={order_side} position_side={pos_side}"
                                        ),
                                    },
                                }

                    before = await self.repo.fetch_reassign_before_state(
                        conn,
                        account_id=aid,
                        deal_ids=[],
                        order_ids=scoped_order_ids,
                    )
                    before_by_account[aid] = before
                    orders_count += await self.repo.reassign_orders(
                        conn=conn,
                        account_id=aid,
                        order_ids=scoped_order_ids,
                        target_strategy_id=target_strategy_id,
                        target_position_id=int(target_position_id or 0),
                    )
                    deals_count += await self.repo.reassign_deals_strategy_by_orders(
                        conn=conn,
                        account_id=aid,
                        order_ids=scoped_order_ids,
                        target_strategy_id=target_strategy_id,
                    )
                    positions_count += await self.repo.reassign_positions_strategy_by_orders(
                        conn=conn,
                        account_id=aid,
                        order_ids=scoped_order_ids,
                        target_strategy_id=target_strategy_id,
                    )
                    affected_accounts.add(aid)

                for aid in sorted(affected_accounts):
                    before = before_by_account.get(aid, {"deals": [], "orders": []})
                    after = {
                        "target_strategy_id": target_strategy_id,
                        "target_position_id": target_position_id,
                    }
                    await self.repo.insert_event(
                        conn=conn,
                        account_id=aid,
                        namespace="position",
                        event_type="reassigned",
                        payload={
                            "deals_updated": deals_count,
                            "orders_updated": orders_count,
                            "positions_updated": positions_count,
                            "target_strategy_id": target_strategy_id,
                            "target_position_id": target_position_id,
                            "before": before,
                            "after": after,
                        },
                    )
                await self.repo.insert_event(
                    conn=conn,
                    account_id=account_ids[0],
                    namespace="position",
                    event_type="reassign_audit",
                    payload={
                        "deals_updated": deals_count,
                        "orders_updated": orders_count,
                        "positions_updated": positions_count,
                        "target_strategy_id": target_strategy_id,
                        "target_position_id": target_position_id,
                    },
                )
                await conn.commit()
            self._oms_query_cache_invalidate_accounts(account_ids)
            return {
                "ok": True,
                "result": {
                    "preview": False,
                    "deals_updated": deals_count,
                    "orders_updated": orders_count,
                    "deals_total": int(deals_total),
                    "orders_total": int(orders_total),
                    "page": page,
                    "page_size": page_size,
                    "items": [],
                },
            }

        if op == "reconcile_now":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            lookback_seconds = int(msg.get("lookback_seconds", 600) or 600)
            scope = str(msg.get("scope", "manual")).strip() or "manual"
            raw_symbols_hint = msg.get("symbols_hint")
            symbols_hint: list[str] = []
            if isinstance(raw_symbols_hint, list):
                seen_symbols: set[str] = set()
                for raw in raw_symbols_hint:
                    symbol = str(raw or "").strip().upper()
                    if not symbol or "/" not in symbol or symbol in seen_symbols:
                        continue
                    seen_symbols.add(symbol)
                    symbols_hint.append(symbol)
                    if len(symbols_hint) >= 20:
                        break
            await self._require_account_permission(auth, account_id, require_trade=False)
            started_ms = int(time.time() * 1000)
            async with self.db.connection() as conn:
                stats = await _reconcile_account_once(
                    conn=conn,
                    repo=self.repo,
                    ccxt_adapter=self.ccxt,
                    credentials_codec=self.codec,
                    account_id=account_id,
                    lookback_seconds=max(60, lookback_seconds),
                    scope=scope,
                    symbols_hint=symbols_hint,
                    limit=max(10, int(self.settings.worker_reconcile_batch_limit)),
                )
                await conn.commit()
            elapsed_ms = max(0, int(time.time() * 1000) - started_ms)
            stats["elapsed_ms"] = elapsed_ms
            dispatcher_logger = self.loggers.get("dispatcher")
            if dispatcher_logger is not None:
                dispatcher_logger.info("reconcile_now_summary %s", json.dumps(stats, separators=(",", ":")))
            self._oms_query_cache_invalidate_accounts([account_id])
            return {
                "ok": True,
                "result": {
                    **stats,
                },
            }

        if op == "reconcile_status_account":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            stale_after_seconds = int(msg.get("stale_after_seconds", 120) or 120)
            await self._require_account_permission(auth, account_id, require_trade=False)
            async with self.db.connection() as conn:
                row = await self.repo.fetch_reconciliation_status_for_account(conn, account_id)
                await conn.commit()
            status, age = self._reconcile_status_of(row["updated_at"], stale_after_seconds)
            return {
                "ok": True,
                "result": {
                    "account_id": account_id,
                    "status": status,
                    "cursor_value": row["cursor_value"],
                    "updated_at": row["updated_at"],
                    "age_seconds": age,
                },
            }

        if op == "reconcile_status_list":
            auth = await self._auth_from_payload(msg)
            filter_status = msg.get("status")
            stale_after_seconds = int(msg.get("stale_after_seconds", 120) or 120)
            async with self.db.connection() as conn:
                accounts = await self.repo.list_accounts_for_api_key(conn, auth.api_key_id)
                allowed_ids = {int(item["account_id"]) for item in accounts}
                rows = await self.repo.list_reconciliation_status_for_user(conn, auth.user_id)
                await conn.commit()
            items: list[dict[str, Any]] = []
            for row in rows:
                if int(row["account_id"]) not in allowed_ids:
                    continue
                computed, age = self._reconcile_status_of(row["updated_at"], stale_after_seconds)
                if filter_status is not None and computed != str(filter_status):
                    continue
                items.append(
                    {
                    "account_id": int(row["account_id"]),
                    "status": computed,
                    "cursor_value": row["cursor_value"],
                    "updated_at": row["updated_at"],
                    "age_seconds": age,
                    }
                )
            return {"ok": True, "result": items}

        if op == "accounts_list":
            auth = await self._auth_from_payload(msg)
            async with self.db.connection() as conn:
                rows = await self.repo.list_accounts_for_api_key(conn, auth.api_key_id)
                await conn.commit()
            return {"ok": True, "result": self._decorate_exchange_ids(rows)}

        if op == "risk_list_strategy_allow_new_positions":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            await self._require_account_permission(auth, account_id)
            async with self.db.connection() as conn:
                rows = await self.repo.list_strategy_risk_state_for_api_key(
                    conn, auth.api_key_id, account_id
                )
                await conn.commit()
            return {"ok": True, "result": rows}

        if op == "risk_set_allow_new_positions":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            allow = bool(msg.get("allow_new_positions", True))
            comment = str(msg.get("comment", "") or "").strip()
            if not comment:
                return {"ok": False, "error": {"code": "validation_error", "message": "comment is required"}}
            await self._require_account_permission(
                auth,
                account_id,
                require_risk_manage=True,
                require_block_new_positions=True,
            )
            async with self.db.connection() as conn:
                changed = await self.repo.set_allow_new_positions(conn, account_id, allow)
                await self.repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="risk",
                    event_type="account_allow_new_positions_changed",
                    payload={
                        "account_id": account_id,
                        "allow_new_positions": allow,
                        "comment": comment,
                        "actor_user_id": auth.user_id,
                        "actor_api_key_id": auth.api_key_id,
                    },
                )
                await conn.commit()
            return {"ok": True, "result": {"account_id": account_id, "allow_new_positions": allow, "rows": changed}}

        if op == "risk_set_strategy_allow_new_positions":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            strategy_id = int(msg.get("strategy_id", 0) or 0)
            allow = bool(msg.get("allow_new_positions", True))
            comment = str(msg.get("comment", "") or "").strip()
            if not comment:
                return {"ok": False, "error": {"code": "validation_error", "message": "comment is required"}}
            await self._require_account_permission(
                auth,
                account_id,
                require_risk_manage=True,
                require_block_new_positions=True,
            )
            async with self.db.connection() as conn:
                changed = await self.repo.set_allow_new_positions_for_strategy(
                    conn, account_id, strategy_id, allow
                )
                await self.repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="risk",
                    event_type="strategy_allow_new_positions_changed",
                    payload={
                        "account_id": account_id,
                        "strategy_id": strategy_id,
                        "allow_new_positions": allow,
                        "comment": comment,
                        "actor_user_id": auth.user_id,
                        "actor_api_key_id": auth.api_key_id,
                    },
                )
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "account_id": account_id,
                    "strategy_id": strategy_id,
                    "allow_new_positions": allow,
                    "rows": changed,
                },
            }

        if op == "risk_set_account_status":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            status = str(msg.get("status", "active")).strip().lower()
            comment = str(msg.get("comment", "") or "").strip()
            if status not in {"active", "blocked"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|blocked"}}
            if not comment:
                return {"ok": False, "error": {"code": "validation_error", "message": "comment is required"}}
            await self._require_account_permission(
                auth,
                account_id,
                require_risk_manage=True,
                require_block_account=True,
            )
            async with self.db.connection() as conn:
                changed = await self.repo.set_account_status(conn, account_id, status)
                await self.repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="risk",
                    event_type="account_status_changed",
                    payload={
                        "account_id": account_id,
                        "status": status,
                        "comment": comment,
                        "actor_user_id": auth.user_id,
                        "actor_api_key_id": auth.api_key_id,
                    },
                )
                await conn.commit()
            self._oms_query_cache_invalidate_accounts([account_id])
            return {"ok": True, "result": {"account_id": account_id, "status": status, "rows": changed}}

        if op == "admin_create_account":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            try:
                exchange_id = self._exchange_engine_id(str(msg.get("exchange_id", "")).strip())
            except RuntimeError:
                return {"ok": False, "error": {"code": "unsupported_engine"}}
            label = str(msg.get("label", "")).strip()
            position_mode = str(msg.get("position_mode", "hedge")).strip()
            is_testnet = bool(msg.get("is_testnet", True))
            extra_config_json = (
                msg.get("extra_config_json") if isinstance(msg.get("extra_config_json"), dict) else {}
            )
            if not exchange_id or not label:
                return {"ok": False, "error": {"code": "validation_error", "message": "exchange_id/label are required"}}
            async with self.db.connection() as conn:
                account_id = await self.repo.create_account(
                    conn,
                    exchange_id=exchange_id,
                    label=label,
                    position_mode=position_mode,
                    is_testnet=is_testnet,
                    extra_config_json=extra_config_json,
                )
                await conn.commit()
            return {"ok": True, "result": {"account_id": account_id}}

        if op == "admin_list_accounts":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            async with self.db.connection() as conn:
                items = await self.repo.list_accounts_admin(conn)
                await conn.commit()
            return {"ok": True, "result": self._decorate_exchange_ids(items)}

        if op == "admin_update_account":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            account_id = int(msg.get("account_id", 0) or 0)
            if account_id <= 0:
                return {"ok": False, "error": {"code": "validation_error", "message": "account_id is required"}}
            exchange_id_raw = msg.get("exchange_id")
            label_raw = msg.get("label")
            position_mode_raw = msg.get("position_mode")
            is_testnet_raw = msg.get("is_testnet")
            status_raw = msg.get("status")
            extra_config_raw = msg.get("extra_config_json")
            credentials_raw = msg.get("credentials") if isinstance(msg.get("credentials"), dict) else None
            exchange_id = None
            if exchange_id_raw is not None:
                try:
                    exchange_id = self._exchange_engine_id(str(exchange_id_raw).strip())
                except RuntimeError:
                    return {"ok": False, "error": {"code": "unsupported_engine"}}
            label = None if label_raw is None else str(label_raw).strip()
            position_mode = None if position_mode_raw is None else str(position_mode_raw).strip()
            if position_mode is not None and position_mode not in {"hedge", "netting", "strategy_netting"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "invalid position_mode"}}
            is_testnet = None if is_testnet_raw is None else bool(is_testnet_raw)
            status = None if status_raw is None else str(status_raw).strip().lower()
            if status is not None and status not in {"active", "blocked"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|blocked"}}
            extra_config_json = (
                None if extra_config_raw is None else (extra_config_raw if isinstance(extra_config_raw, dict) else {})
            )
            async with self.db.connection() as conn:
                rows = await self.repo.update_account_admin(
                    conn,
                    account_id,
                    exchange_id=exchange_id,
                    label=label,
                    position_mode=position_mode,
                    is_testnet=is_testnet,
                    status=status,
                    extra_config_json=extra_config_json,
                )
                if credentials_raw is not None:
                    _, _, current_api_key_enc, current_secret_enc, current_passphrase_enc, _ = await self.repo.fetch_account_exchange_credentials(
                        conn, account_id
                    )
                    api_key_raw = credentials_raw.get("api_key")
                    secret_raw = credentials_raw.get("secret")
                    passphrase_raw = credentials_raw.get("passphrase")
                    api_key_enc = current_api_key_enc if api_key_raw is None else self.codec.encrypt(str(api_key_raw))
                    secret_enc = current_secret_enc if secret_raw is None else self.codec.encrypt(str(secret_raw))
                    passphrase_enc = (
                        current_passphrase_enc if passphrase_raw is None else self.codec.encrypt(str(passphrase_raw))
                    )
                    if not api_key_enc or not secret_enc:
                        await conn.commit()
                        return {
                            "ok": False,
                            "error": {
                                "code": "validation_error",
                                "message": "api_key and secret are required for credentials",
                            },
                        }
                    rows += await self.repo.upsert_account_credentials(
                        conn=conn,
                        account_id=account_id,
                        api_key_enc=api_key_enc,
                        secret_enc=secret_enc,
                        passphrase_enc=passphrase_enc,
                    )
                await conn.commit()
            self._perm_cache_invalidate(account_id=account_id)
            self._oms_query_cache_invalidate_accounts([account_id])
            return {"ok": True, "result": {"account_id": account_id, "rows": rows}}

        if op == "admin_create_user_api_key":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            user_name = str(msg.get("user_name", "")).strip()
            role = str(msg.get("role", "trader")).strip().lower()
            if role not in {"admin", "trader", "portfolio_manager", "robot", "risk", "readonly"}:
                return {
                    "ok": False,
                    "error": {
                        "code": "validation_error",
                        "message": "role must be admin|trader|portfolio_manager|robot|risk|readonly",
                    },
                }
            if not user_name:
                return {"ok": False, "error": {"code": "validation_error", "message": "user_name is required"}}
            password_raw = msg.get("password")
            password = None if password_raw is None else str(password_raw)
            role = self._normalize_role(role)
            api_key_plain = str(msg.get("api_key") or secrets.token_urlsafe(32))
            api_key_hash = hashlib.sha256(api_key_plain.encode("utf-8")).hexdigest()
            permissions = msg.get("permissions") if isinstance(msg.get("permissions"), list) else []
            label = msg.get("label")
            label = str(label).strip() if label is not None else None
            async with self.db.connection() as conn:
                user_id = await self.repo.create_user(conn, user_name, role=role)
                if password:
                    await self.repo.set_user_password_hash(conn, user_id, self._new_password_hash(password))
                api_key_id = await self.repo.create_api_key(conn, user_id, api_key_hash, label=label)
                for raw in permissions:
                    if not isinstance(raw, dict):
                        continue
                    account_id = int(raw.get("account_id", 0) or 0)
                    if account_id <= 0:
                        continue
                    can_read = bool(raw.get("can_read", True))
                    can_trade = bool(raw.get("can_trade", False))
                    can_close_position = bool(raw.get("can_close_position", False))
                    can_risk_manage = bool(raw.get("can_risk_manage", False))
                    can_block_new_positions = bool(raw.get("can_block_new_positions", False))
                    can_block_account = bool(raw.get("can_block_account", False))
                    restrict_to_strategies = bool(raw.get("restrict_to_strategies", False))
                    await self.repo.upsert_user_account_permissions(
                        conn, user_id, account_id, can_read, can_trade, can_risk_manage
                    )
                    await self.repo.upsert_api_key_account_permissions(
                        conn,
                        api_key_id,
                        account_id,
                        can_read=can_read,
                        can_trade=can_trade,
                        can_close_position=can_close_position,
                        can_risk_manage=can_risk_manage,
                        can_block_new_positions=can_block_new_positions,
                        can_block_account=can_block_account,
                        restrict_to_strategies=restrict_to_strategies,
                    )
                    strategy_ids = raw.get("strategy_ids") if isinstance(raw.get("strategy_ids"), list) else []
                    for sid in strategy_ids:
                        sid_int = int(sid or 0)
                        if sid_int <= 0:
                            continue
                        if not await self.repo.strategy_exists_for_account(conn, account_id, sid_int):
                            continue
                        await self.repo.upsert_api_key_strategy_permissions(
                            conn, api_key_id, account_id, sid_int, can_read=True, can_trade=can_trade
                        )
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "user_id": user_id,
                    "api_key_id": api_key_id,
                    "api_key_plain": api_key_plain,
                },
            }

        if op == "admin_list_users_api_keys":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            async with self.db.connection() as conn:
                items = await self.repo.list_users_api_keys_admin(conn)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "admin_create_api_key":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            user_id = int(msg.get("user_id", 0) or 0)
            if user_id <= 0:
                return {"ok": False, "error": {"code": "validation_error", "message": "user_id is required"}}
            api_key_plain = str(msg.get("api_key") or secrets.token_urlsafe(32))
            api_key_hash = hashlib.sha256(api_key_plain.encode("utf-8")).hexdigest()
            label = msg.get("label")
            label = str(label).strip() if label is not None else None
            async with self.db.connection() as conn:
                user = await self.repo.fetch_user_by_id(conn, user_id)
                if user is None:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "not_found", "message": "user not found"}}
                api_key_id = await self.repo.create_api_key(conn, user_id, api_key_hash, label=label)
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "user_id": user_id,
                    "api_key_id": api_key_id,
                    "api_key_plain": api_key_plain,
                },
            }

        if op == "admin_list_api_key_permissions":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            api_key_id = int(msg.get("api_key_id", 0) or 0)
            if api_key_id <= 0:
                return {"ok": False, "error": {"code": "validation_error", "message": "api_key_id is required"}}
            async with self.db.connection() as conn:
                items = await self.repo.list_api_key_permissions_admin(conn, api_key_id)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "admin_upsert_api_key_permission":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            api_key_id = int(msg.get("api_key_id", 0) or 0)
            account_id = int(msg.get("account_id", 0) or 0)
            if api_key_id <= 0 or account_id <= 0:
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "api_key_id/account_id are required"},
                }
            can_read = bool(msg.get("can_read", True))
            can_trade = bool(msg.get("can_trade", False))
            can_close_position = bool(msg.get("can_close_position", False))
            can_risk_manage = bool(msg.get("can_risk_manage", False))
            can_block_new_positions = bool(msg.get("can_block_new_positions", False))
            can_block_account = bool(msg.get("can_block_account", False))
            restrict_to_strategies = bool(msg.get("restrict_to_strategies", False))
            strategy_ids_raw = msg.get("strategy_ids") if isinstance(msg.get("strategy_ids"), list) else []
            async with self.db.connection() as conn:
                rows = await self.repo.upsert_api_key_account_permissions(
                    conn=conn,
                    api_key_id=api_key_id,
                    account_id=account_id,
                    can_read=can_read,
                    can_trade=can_trade,
                    can_close_position=can_close_position,
                    can_risk_manage=can_risk_manage,
                    can_block_new_positions=can_block_new_positions,
                    can_block_account=can_block_account,
                    restrict_to_strategies=restrict_to_strategies,
                )
                await self.repo.delete_api_key_strategy_permissions(conn, api_key_id, account_id)
                for sid_raw in strategy_ids_raw:
                    sid = int(sid_raw or 0)
                    if sid <= 0:
                        continue
                    if not await self.repo.strategy_exists_for_account(conn, account_id, sid):
                        continue
                    rows += await self.repo.upsert_api_key_strategy_permissions(
                        conn,
                        api_key_id,
                        account_id,
                        sid,
                        can_read=True,
                        can_trade=can_trade,
                    )
                await conn.commit()
            self._perm_cache_invalidate(api_key_id=api_key_id, account_id=account_id)
            self._oms_query_cache_invalidate_all()
            return {"ok": True, "result": {"api_key_id": api_key_id, "account_id": account_id, "rows": rows}}

        if op == "admin_update_api_key":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            api_key_id = int(msg.get("api_key_id", 0) or 0)
            status = str(msg.get("status", "")).strip().lower()
            if api_key_id <= 0:
                return {"ok": False, "error": {"code": "validation_error", "message": "api_key_id is required"}}
            if status not in {"active", "disabled"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|disabled"}}
            async with self.db.connection() as conn:
                rows = await self.repo.set_api_key_status(conn, api_key_id, status)
                await conn.commit()
            self._auth_cache_invalidate(api_key_id=api_key_id)
            self._perm_cache_invalidate(api_key_id=api_key_id)
            self._oms_query_cache_invalidate_all()
            return {"ok": True, "result": {"api_key_id": api_key_id, "rows": rows}}

        if op == "auth_login_password":
            user_name = str(msg.get("user_name", "")).strip()
            password = str(msg.get("password", ""))
            api_key_id_raw = msg.get("api_key_id")
            if not user_name or not password:
                return {
                    "ok": False,
                    "error": {"code": "validation_error", "message": "user_name and password are required"},
                }
            async with self.db.connection() as conn:
                user = await self.repo.fetch_user_by_name(conn, user_name)
                if user is None or str(user.get("status")) != "active":
                    await conn.commit()
                    return {"ok": False, "error": {"code": "invalid_credentials"}}
                stored_hash = await self.repo.fetch_user_password_hash(conn, int(user["user_id"]))
                if not self._verify_password(password, stored_hash):
                    await conn.commit()
                    return {"ok": False, "error": {"code": "invalid_credentials"}}
                active_api_keys = await self.repo.list_active_api_keys_for_user(conn, int(user["user_id"]))
                if not active_api_keys:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "no_active_api_key"}}
                selected_api_key_id: int
                if api_key_id_raw is None:
                    selected_api_key_id = int(active_api_keys[0])
                else:
                    selected_api_key_id = int(api_key_id_raw)
                    if selected_api_key_id not in set(active_api_keys):
                        await conn.commit()
                        return {"ok": False, "error": {"code": "api_key_not_allowed"}}
                token_plain = f"tok_{secrets.token_urlsafe(32)}"
                token_hash = hashlib.sha256(token_plain.encode("utf-8")).hexdigest()
                expires_at = datetime.now(timezone.utc) + timedelta(hours=12)
                await self.repo.create_auth_token(
                    conn=conn,
                    user_id=int(user["user_id"]),
                    api_key_id=selected_api_key_id,
                    token_hash=token_hash,
                    expires_at=expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                )
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "token": token_plain,
                    "token_type": "bearer",
                    "expires_at": expires_at.isoformat(),
                    "user_id": int(user["user_id"]),
                    "role": str(user["role"]),
                    "api_key_id": selected_api_key_id,
                },
            }

        if op == "user_profile_get":
            auth = await self._auth_from_payload(msg)
            async with self.db.connection() as conn:
                user = await self.repo.fetch_user_by_id(conn, auth.user_id)
                await conn.commit()
            if user is None:
                return {"ok": False, "error": {"code": "not_found", "message": "user not found"}}
            return {
                "ok": True,
                "result": {
                    "user_id": int(user.get("user_id", auth.user_id)),
                    "user_name": str(user.get("user_name", "")),
                    "role": str(user.get("role", "")),
                    "status": str(user.get("status", "")),
                    "api_key_id": int(auth.api_key_id),
                },
            }

        if op == "user_profile_update":
            auth = await self._auth_from_payload(msg)
            user_name = str(msg.get("user_name", "")).strip()
            if not user_name:
                return {"ok": False, "error": {"code": "validation_error", "message": "user_name is required"}}
            async with self.db.connection() as conn:
                rows = await self.repo.update_user_name(conn, auth.user_id, user_name)
                user = await self.repo.fetch_user_by_id(conn, auth.user_id)
                await conn.commit()
            if user is None:
                return {"ok": False, "error": {"code": "not_found", "message": "user not found"}}
            return {
                "ok": True,
                "result": {
                    "user_id": int(auth.user_id),
                    "user_name": str(user.get("user_name", user_name)),
                },
            }

        if op == "user_password_update":
            auth = await self._auth_from_payload(msg)
            current_password = str(msg.get("current_password", ""))
            new_password = str(msg.get("new_password", ""))
            if not current_password or not new_password:
                return {
                    "ok": False,
                    "error": {
                        "code": "validation_error",
                        "message": "current_password and new_password are required",
                    },
                }
            async with self.db.connection() as conn:
                stored_hash = await self.repo.fetch_user_password_hash(conn, auth.user_id)
                if not self._verify_password(current_password, stored_hash):
                    await conn.commit()
                    return {"ok": False, "error": {"code": "invalid_credentials"}}
                rows = await self.repo.set_user_password_hash(
                    conn,
                    auth.user_id,
                    self._new_password_hash(new_password),
                )
                await conn.commit()
            return {"ok": True, "result": {"user_id": int(auth.user_id), "rows": int(rows)}}

        if op == "user_api_keys_list":
            auth = await self._auth_from_payload(msg)
            async with self.db.connection() as conn:
                items = await self.repo.list_api_keys_for_user(conn, auth.user_id)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "user_api_key_permissions_list":
            auth = await self._auth_from_payload(msg)
            requested_api_key_id = int(msg.get("api_key_id", 0) or 0)
            async with self.db.connection() as conn:
                user_keys = await self.repo.list_api_keys_for_user(conn, auth.user_id)
                owned_key_ids = sorted(
                    set(
                        [
                            int(item.get("api_key_id", 0) or 0)
                            for item in (user_keys or [])
                            if int(item.get("api_key_id", 0) or 0) > 0
                        ]
                    )
                )
                if requested_api_key_id > 0:
                    if requested_api_key_id not in owned_key_ids:
                        await conn.commit()
                        return {"ok": False, "error": {"code": "permission_denied"}}
                    target_key_ids = [requested_api_key_id]
                else:
                    target_key_ids = owned_key_ids
                items: list[dict[str, Any]] = []
                for api_key_id in target_key_ids:
                    rows = await self.repo.list_api_key_permissions_admin(conn, api_key_id)
                    if rows:
                        items.extend(rows)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "user_api_key_create":
            auth = await self._auth_from_payload(msg)
            api_key_plain = str(msg.get("api_key") or secrets.token_urlsafe(32))
            api_key_hash = hashlib.sha256(api_key_plain.encode("utf-8")).hexdigest()
            label = msg.get("label")
            label = str(label).strip() if label is not None else None
            async with self.db.connection() as conn:
                api_key_id = await self.repo.create_api_key(conn, auth.user_id, api_key_hash, label=label)
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "user_id": int(auth.user_id),
                    "api_key_id": int(api_key_id),
                    "api_key_plain": api_key_plain,
                },
            }

        if op == "user_api_key_update":
            auth = await self._auth_from_payload(msg)
            api_key_id = int(msg.get("api_key_id", 0) or 0)
            status = str(msg.get("status", "")).strip().lower()
            if api_key_id <= 0:
                return {"ok": False, "error": {"code": "validation_error", "message": "api_key_id is required"}}
            if status not in {"active", "disabled"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|disabled"}}
            async with self.db.connection() as conn:
                owner = await self.repo.fetch_api_key_owner(conn, api_key_id)
                if owner is None:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "not_found", "message": "api key not found"}}
                if int(owner.get("user_id", 0) or 0) != int(auth.user_id):
                    await conn.commit()
                    return {"ok": False, "error": {"code": "permission_denied"}}
                rows = await self.repo.set_api_key_status(conn, api_key_id, status)
                await conn.commit()
            self._auth_cache_invalidate(api_key_id=api_key_id)
            self._perm_cache_invalidate(api_key_id=api_key_id)
            self._oms_query_cache_invalidate_all()
            return {"ok": True, "result": {"api_key_id": api_key_id, "rows": int(rows)}}

        if op == "admin_list_users":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            async with self.db.connection() as conn:
                items = await self.repo.list_users_admin(conn)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "admin_create_strategy":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            name = str(msg.get("name", "")).strip()
            account_ids = msg.get("account_ids") if isinstance(msg.get("account_ids"), list) else []
            raw_client_strategy_id = msg.get("client_strategy_id")
            client_strategy_id: int | None = None
            if raw_client_strategy_id is not None and str(raw_client_strategy_id).strip() != "":
                parsed_client_strategy_id = int(raw_client_strategy_id)
                if parsed_client_strategy_id <= 0:
                    return {
                        "ok": False,
                        "error": {"code": "validation_error", "message": "client_strategy_id must be >= 1"},
                    }
                client_strategy_id = parsed_client_strategy_id
            if not name:
                return {"ok": False, "error": {"code": "validation_error", "message": "name is required"}}
            async with self.db.connection() as conn:
                strategy_id = await self.repo.create_strategy(
                    conn, name=name, client_strategy_id=client_strategy_id
                )
                for raw in account_ids:
                    aid = int(raw or 0)
                    if aid > 0:
                        await self.repo.link_strategy_to_account(conn, strategy_id, aid)
                await conn.commit()
            return {"ok": True, "result": {"strategy_id": strategy_id}}

        if op == "admin_list_strategies":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            async with self.db.connection() as conn:
                items = await self.repo.list_strategies(conn)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "strategy_list":
            auth = await self._auth_from_payload(msg)
            async with self.db.connection() as conn:
                items = await self.repo.list_strategies_for_api_key(conn, auth.api_key_id)
                await conn.commit()
            return {"ok": True, "result": items}

        if op == "strategy_create":
            auth = await self._auth_from_payload(msg)
            name = str(msg.get("name", "")).strip()
            account_ids = msg.get("account_ids") if isinstance(msg.get("account_ids"), list) else []
            raw_client_strategy_id = msg.get("client_strategy_id")
            client_strategy_id: int | None = None
            if raw_client_strategy_id is not None and str(raw_client_strategy_id).strip() != "":
                parsed_client_strategy_id = int(raw_client_strategy_id)
                if parsed_client_strategy_id <= 0:
                    return {
                        "ok": False,
                        "error": {"code": "validation_error", "message": "client_strategy_id must be >= 1"},
                    }
                client_strategy_id = parsed_client_strategy_id
            if not name:
                return {"ok": False, "error": {"code": "validation_error", "message": "name is required"}}
            normalized_account_ids: list[int] = []
            for raw in account_ids:
                aid = int(raw or 0)
                if aid <= 0:
                    continue
                await self._require_account_permission(auth, aid, require_trade=True)
                normalized_account_ids.append(aid)
            if not normalized_account_ids:
                return {"ok": False, "error": {"code": "validation_error", "message": "account_ids is required"}}
            async with self.db.connection() as conn:
                strategy_id = await self.repo.create_strategy(
                    conn, name=name, client_strategy_id=client_strategy_id
                )
                for aid in normalized_account_ids:
                    await self.repo.link_strategy_to_account(conn, strategy_id, aid)
                await conn.commit()
            return {"ok": True, "result": {"strategy_id": strategy_id}}

        if op == "admin_update_strategy":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            strategy_id = int(msg.get("strategy_id", 0) or 0)
            if strategy_id <= 0:
                return {"ok": False, "error": {"code": "validation_error", "message": "strategy_id is required"}}
            name_raw = msg.get("name")
            status_raw = msg.get("status")
            account_ids_raw = msg.get("account_ids")
            has_client_strategy_id = "client_strategy_id" in msg
            raw_client_strategy_id = msg.get("client_strategy_id")
            name = None if name_raw is None else str(name_raw).strip()
            status = None if status_raw is None else str(status_raw).strip().lower()
            has_account_ids = account_ids_raw is not None
            client_strategy_id: int | None = None
            if has_client_strategy_id and raw_client_strategy_id is not None and str(raw_client_strategy_id).strip() != "":
                client_strategy_id = int(raw_client_strategy_id)
                if int(client_strategy_id) <= 0:
                    return {
                        "ok": False,
                        "error": {"code": "validation_error", "message": "client_strategy_id must be >= 1"},
                    }
            if status is not None and status not in {"active", "disabled"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|disabled"}}
            normalized_account_ids: list[int] = []
            if has_account_ids:
                if not isinstance(account_ids_raw, list):
                    return {"ok": False, "error": {"code": "validation_error", "message": "account_ids must be a list"}}
                for raw in account_ids_raw:
                    aid = int(raw or 0)
                    if aid <= 0:
                        continue
                    normalized_account_ids.append(aid)
                normalized_account_ids = sorted(set(normalized_account_ids))
            async with self.db.connection() as conn:
                rows = await self.repo.update_strategy(
                    conn,
                    strategy_id,
                    name=name,
                    status=status,
                    client_strategy_id=client_strategy_id,
                    update_client_strategy_id=has_client_strategy_id,
                )
                if has_account_ids:
                    for aid in normalized_account_ids:
                        account = await self.repo.fetch_account_by_id(conn, aid)
                        if account is None or str(account.get("status")) != "active":
                            await conn.commit()
                            return {
                                "ok": False,
                                "error": {
                                    "code": "validation_error",
                                    "message": f"account_id={aid} not found or inactive",
                                },
                            }
                    rows += await self.repo.sync_strategy_accounts(conn, strategy_id, normalized_account_ids)
                await conn.commit()
            return {"ok": True, "result": {"strategy_id": strategy_id, "rows": rows}}

        if op == "admin_oms_query":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            view = str(msg.get("view", "")).strip()
            raw_account_ids = msg.get("account_ids")
            date_from_raw = msg.get("date_from")
            date_to_raw = msg.get("date_to")
            page_raw = msg.get("page")
            page_size_raw = msg.get("page_size")
            account_ids: list[int] = []
            seen: set[int] = set()
            if isinstance(raw_account_ids, list):
                for raw in raw_account_ids:
                    try:
                        aid = int(raw or 0)
                    except Exception:
                        aid = 0
                    if aid <= 0 or aid in seen:
                        continue
                    seen.add(aid)
                    account_ids.append(aid)
            elif isinstance(raw_account_ids, str):
                for part in raw_account_ids.split(","):
                    text = str(part).strip()
                    if not text.isdigit():
                        continue
                    aid = int(text)
                    if aid <= 0 or aid in seen:
                        continue
                    seen.add(aid)
                    account_ids.append(aid)
            try:
                page = max(1, int(page_raw or 1))
                page_size = max(1, min(500, int(page_size_raw or 100)))
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "page/page_size invalid"}}
            offset = (page - 1) * page_size
            date_from = None if date_from_raw in {None, ""} else str(date_from_raw).strip()
            date_to = None if date_to_raw in {None, ""} else str(date_to_raw).strip()
            async with self.db.connection() as conn:
                if view == "open_orders":
                    items, total = await self.repo.admin_list_oms_orders_multi(
                        conn,
                        account_ids=account_ids,
                        open_only=True,
                        date_from=None,
                        date_to=None,
                        limit=page_size,
                        offset=offset,
                    )
                elif view == "history_orders":
                    items, total = await self.repo.admin_list_oms_orders_multi(
                        conn,
                        account_ids=account_ids,
                        open_only=False,
                        date_from=date_from,
                        date_to=date_to,
                        limit=page_size,
                        offset=offset,
                    )
                elif view == "open_positions":
                    items, total = await self.repo.admin_list_oms_positions_multi(
                        conn,
                        account_ids=account_ids,
                        open_only=True,
                        date_from=None,
                        date_to=None,
                        limit=page_size,
                        offset=offset,
                    )
                elif view == "history_positions":
                    items, total = await self.repo.admin_list_oms_positions_multi(
                        conn,
                        account_ids=account_ids,
                        open_only=False,
                        date_from=date_from,
                        date_to=date_to,
                        limit=page_size,
                        offset=offset,
                    )
                elif view == "deals":
                    items, total = await self.repo.admin_list_oms_deals_multi(
                        conn,
                        account_ids=account_ids,
                        date_from=date_from,
                        date_to=date_to,
                        limit=page_size,
                        offset=offset,
                    )
                else:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "validation_error", "message": "unsupported view"}}
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "items": items,
                    "total": int(total),
                    "page": int(page),
                    "page_size": int(page_size),
                },
            }

        if op == "admin_oms_mutate":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            entity = str(msg.get("entity", "")).strip()
            operations = msg.get("operations") if isinstance(msg.get("operations"), list) else []
            if entity not in {"orders", "positions", "deals"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "entity invalid"}}
            results: list[dict[str, Any]] = []
            affected_accounts: set[int] = set()
            pending_events: list[tuple[int, str, dict[str, Any]]] = []
            async with self.db.connection() as conn:
                for index, raw in enumerate(operations):
                    if not isinstance(raw, dict):
                        results.append({"index": index, "ok": False, "op": "", "error": "invalid operation"})
                        continue
                    op_kind = str(raw.get("op", "")).strip().lower()
                    row = raw.get("row") if isinstance(raw.get("row"), dict) else {}
                    row = dict(row)
                    try:
                        if entity == "orders":
                            if op_kind == "insert":
                                row_id = await self.repo.admin_insert_oms_order(conn, row)
                                after = await self.repo.admin_fetch_oms_order_by_id(conn, row_id)
                                if after is not None:
                                    affected_accounts.add(int(after["account_id"]))
                                    pending_events.append((int(after["account_id"]), "order_updated", dict(after)))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            elif op_kind == "update":
                                row_id = int(row.get("id", 0) or 0)
                                if row_id <= 0:
                                    raise ValueError("id is required for update")
                                await self.repo.admin_update_oms_order(conn, row_id, row)
                                after = await self.repo.admin_fetch_oms_order_by_id(conn, row_id)
                                if after is not None:
                                    affected_accounts.add(int(after["account_id"]))
                                    pending_events.append((int(after["account_id"]), "order_updated", dict(after)))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            elif op_kind == "delete":
                                row_id = int(row.get("id", 0) or 0)
                                account_id = int(row.get("account_id", 0) or 0) or None
                                if row_id <= 0:
                                    raise ValueError("id is required for delete")
                                before = await self.repo.admin_fetch_oms_order_by_id(conn, row_id, account_id)
                                await self.repo.admin_delete_oms_order(conn, row_id, account_id)
                                payload = dict(before or {"id": row_id, "account_id": int(account_id or 0)})
                                payload["__deleted"] = True
                                payload["order_id"] = int(row_id)
                                payload["status"] = payload.get("status") or "CANCELED"
                                aid = int(payload.get("account_id", 0) or 0)
                                if aid > 0:
                                    affected_accounts.add(aid)
                                    pending_events.append((aid, "order_deleted", payload))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            else:
                                raise ValueError("op invalid")
                        elif entity == "positions":
                            if op_kind == "insert":
                                row_id = await self.repo.admin_insert_oms_position(conn, row)
                                after = await self.repo.admin_fetch_oms_position_by_id(conn, row_id)
                                if after is not None:
                                    affected_accounts.add(int(after["account_id"]))
                                    pending_events.append((int(after["account_id"]), "position_updated", dict(after)))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            elif op_kind == "update":
                                row_id = int(row.get("id", 0) or 0)
                                if row_id <= 0:
                                    raise ValueError("id is required for update")
                                await self.repo.admin_update_oms_position(conn, row_id, row)
                                after = await self.repo.admin_fetch_oms_position_by_id(conn, row_id)
                                if after is not None:
                                    affected_accounts.add(int(after["account_id"]))
                                    pending_events.append((int(after["account_id"]), "position_updated", dict(after)))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            elif op_kind == "delete":
                                row_id = int(row.get("id", 0) or 0)
                                account_id = int(row.get("account_id", 0) or 0) or None
                                if row_id <= 0:
                                    raise ValueError("id is required for delete")
                                before = await self.repo.admin_fetch_oms_position_by_id(conn, row_id, account_id)
                                await self.repo.admin_delete_oms_position(conn, row_id, account_id)
                                payload = dict(before or {"id": row_id, "account_id": int(account_id or 0)})
                                payload["__deleted"] = True
                                payload["position_id"] = int(row_id)
                                payload["state"] = payload.get("state") or "closed"
                                payload["qty"] = "0"
                                aid = int(payload.get("account_id", 0) or 0)
                                if aid > 0:
                                    affected_accounts.add(aid)
                                    pending_events.append((aid, "position_deleted", payload))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            else:
                                raise ValueError("op invalid")
                        else:  # deals
                            if op_kind == "insert":
                                row_id = await self.repo.admin_insert_oms_deal(conn, row)
                                after = await self.repo.admin_fetch_oms_deal_by_id(conn, row_id)
                                if after is not None:
                                    affected_accounts.add(int(after["account_id"]))
                                    pending_events.append((int(after["account_id"]), "deal_updated", dict(after)))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            elif op_kind == "update":
                                row_id = int(row.get("id", 0) or 0)
                                if row_id <= 0:
                                    raise ValueError("id is required for update")
                                await self.repo.admin_update_oms_deal(conn, row_id, row)
                                after = await self.repo.admin_fetch_oms_deal_by_id(conn, row_id)
                                if after is not None:
                                    affected_accounts.add(int(after["account_id"]))
                                    pending_events.append((int(after["account_id"]), "deal_updated", dict(after)))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            elif op_kind == "delete":
                                row_id = int(row.get("id", 0) or 0)
                                account_id = int(row.get("account_id", 0) or 0) or None
                                if row_id <= 0:
                                    raise ValueError("id is required for delete")
                                before = await self.repo.admin_fetch_oms_deal_by_id(conn, row_id, account_id)
                                await self.repo.admin_delete_oms_deal(conn, row_id, account_id)
                                payload = dict(before or {"id": row_id, "account_id": int(account_id or 0)})
                                payload["__deleted"] = True
                                aid = int(payload.get("account_id", 0) or 0)
                                if aid > 0:
                                    affected_accounts.add(aid)
                                    pending_events.append((aid, "deal_deleted", payload))
                                results.append({"index": index, "ok": True, "op": op_kind, "id": int(row_id)})
                            else:
                                raise ValueError("op invalid")
                    except Exception as exc:
                        results.append({"index": index, "ok": False, "op": op_kind, "error": str(exc)})
                await conn.commit()
                # Emit atomic-like UI state events after commit.
                for account_id, ev_type, payload in pending_events:
                    await self.repo.insert_event(
                        conn=conn,
                        account_id=account_id,
                        namespace="position",
                        event_type=ev_type,
                        payload=payload,
                    )
                for aid in sorted(affected_accounts):
                    open_orders = await self.repo.list_orders(conn, aid, open_only=True, open_limit=5000)
                    open_positions = await self.repo.list_positions(conn, aid, open_only=True, open_limit=5000)
                    await self.repo.insert_event(
                        conn=conn,
                        account_id=aid,
                        namespace="position",
                        event_type="snapshot_open_orders",
                        payload={"items": open_orders},
                    )
                    await self.repo.insert_event(
                        conn=conn,
                        account_id=aid,
                        namespace="position",
                        event_type="snapshot_open_positions",
                        payload={"items": open_positions},
                    )
            if affected_accounts:
                self._oms_query_cache_invalidate_accounts([int(a) for a in affected_accounts])
            return {"ok": True, "result": {"entity": entity, "results": results}}

        if op == "ws_pull_events":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            from_event_id = int(msg.get("from_event_id", 0) or 0)
            limit = max(1, min(500, int(msg.get("limit", 100) or 100)))
            await self._require_account_permission(auth, account_id, require_trade=False)
            events = self._pull_ws_events(account_id, from_event_id, limit=limit)
            return {"ok": True, "result": events}

        if op == "ws_tail_id":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            await self._require_account_permission(auth, account_id, require_trade=False)
            tail_id = int(self._ws_event_seq)
            return {"ok": True, "result": {"tail_id": tail_id}}

        if op == "status":
            ccxt_sessions = self.ccxt.get_session_status()
            queue_wait = list(self._queue_wait_samples_ms)
            execute = list(self._execute_samples_ms)
            op_timing: dict[str, Any] = {}
            for key in sorted(self._op_timing_count.keys()):
                count = int(self._op_timing_count.get(key, 0))
                total_ms = float(self._op_timing_total_ms.get(key, 0.0))
                max_ms = float(self._op_timing_max_ms.get(key, 0.0))
                samples = list(self._op_timing_samples_ms.get(key, []))
                avg_ms = (total_ms / count) if count > 0 else 0.0
                op_timing[key] = {
                    "count": count,
                    "total_ms": round(total_ms, 2),
                    "avg_ms": round(avg_ms, 2),
                    "max_ms": round(max_ms, 2),
                    "p95_ms": round(self._percentile(samples, 0.95), 2),
                    "p99_ms": round(self._percentile(samples, 0.99), 2),
                }
            return {
                "ok": True,
                "result": {
                    "started_at": self.started_at,
                    "pool_size_by_engine": self.pool_size_by_engine,
                    "total_requests": self.total_requests,
                    "total_errors": self.total_errors,
                    "op_counts": self.op_counts,
                    "accounts_mapped": len(self.account_worker),
                    "worker_inflight": {
                        engine: {str(k): v for k, v in workers.items()}
                        for engine, workers in self.worker_inflight.items()
                    },
                    "worker_active_accounts": {
                        engine: {str(k): len(v) for k, v in workers.items()}
                        for engine, workers in self.worker_active_accounts.items()
                    },
                    "worker_queue_depth": {
                        engine: {str(k): queues[k].qsize() for k in queues}
                        for engine, queues in self.worker_queues.items()
                    },
                    "read_pool_size": int(self.read_pool_size),
                    "read_worker_inflight": {str(k): int(v) for k, v in self.read_worker_inflight.items()},
                    "read_worker_queue_depth": {str(k): self.read_worker_queues[k].qsize() for k in self.read_worker_queues},
                    "control_queue_depth": self.control_queue.qsize(),
                    "ccxt_session_count_total": ccxt_sessions.get("session_count_total", 0),
                    "ccxt_session_count_by_engine": ccxt_sessions.get("session_count_by_engine", {}),
                    "ccxt_session_account_ids": ccxt_sessions.get("session_account_ids", []),
                    "ccxt_session_ttl_seconds": ccxt_sessions.get("session_ttl_seconds", 0),
                    "ccxt_session_oldest_age_seconds": ccxt_sessions.get("session_oldest_age_seconds", 0),
                    "auth_cache_ttl_seconds": int(self.auth_cache_ttl_seconds),
                    "auth_cache_size": len(self._auth_cache_by_key),
                    "auth_cache_hits": int(self._auth_cache_hits),
                    "auth_cache_misses": int(self._auth_cache_misses),
                    "auth_cache_invalidations": int(self._auth_cache_invalidations),
                    "permission_cache_ttl_seconds": int(self.permission_cache_ttl_seconds),
                    "permission_cache_account_size": len(self._perm_account_cache),
                    "permission_cache_strategy_size": len(self._perm_strategy_cache),
                    "permission_cache_account_hits": int(self._perm_cache_account_hits),
                    "permission_cache_account_misses": int(self._perm_cache_account_misses),
                    "permission_cache_strategy_hits": int(self._perm_cache_strategy_hits),
                    "permission_cache_strategy_misses": int(self._perm_cache_strategy_misses),
                    "permission_cache_invalidations": int(self._perm_cache_invalidations),
                    "oms_query_cache_ttl_seconds": int(self.oms_query_cache_ttl_seconds),
                    "oms_query_cache_size": len(self._oms_query_cache),
                    "oms_query_cache_hits": int(self._oms_query_cache_hits),
                    "oms_query_cache_misses": int(self._oms_query_cache_misses),
                    "oms_query_cache_invalidations": int(self._oms_query_cache_invalidations),
                    "timing_samples_count": len(queue_wait),
                    "queue_wait_ms": {
                        "p50": round(self._percentile(queue_wait, 0.50), 2),
                        "p95": round(self._percentile(queue_wait, 0.95), 2),
                        "p99": round(self._percentile(queue_wait, 0.99), 2),
                    },
                    "execute_ms": {
                        "p50": round(self._percentile(execute, 0.50), 2),
                        "p95": round(self._percentile(execute, 0.95), 2),
                        "p99": round(self._percentile(execute, 0.99), 2),
                    },
                    "op_timing_ms": op_timing,
                },
            }

        if op == "oms_query_cache_clear":
            auth = await self._auth_from_payload(msg)
            if bool(auth.is_admin):
                cleared_before = len(self._oms_query_cache)
                self._oms_query_cache_invalidate_all()
                return {
                    "ok": True,
                    "result": {
                        "scope": "all",
                        "cleared_before": int(cleared_before),
                    },
                }
            async with self.db.connection() as conn:
                rows = await self.repo.list_accounts_for_api_key(conn, auth.api_key_id)
                await conn.commit()
            account_ids = sorted(
                {
                    int(r.get("account_id", 0) or 0)
                    for r in (rows or [])
                    if int(r.get("account_id", 0) or 0) > 0
                }
            )
            cleared_before = len(self._oms_query_cache)
            self._oms_query_cache_invalidate_accounts(account_ids)
            return {
                "ok": True,
                "result": {
                    "scope": "visible_accounts",
                    "account_count": int(len(account_ids)),
                    "cleared_before": int(cleared_before),
                },
            }

        if op == "auth_check":
            auth = await self._auth_from_payload(msg)
            return {"ok": True, "result": {"user_id": auth.user_id, "api_key_id": auth.api_key_id, "role": auth.role}}

        return {"ok": False, "error": {"code": "unsupported_op"}}

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                try:
                    msg = self._json_loads_bytes(line)
                except Exception:
                    writer.write(b"{\"ok\":false,\"error\":{\"code\":\"invalid_json\"}}\n")
                    await writer.drain()
                    continue
                self.total_requests += 1
                op = str(msg.get("op", "")).strip()
                if op == "status":
                    out = await self._execute(msg)
                    writer.write(self._json_dumps_bytes(out) + b"\n")
                    await writer.drain()
                    continue
                if op == "oms_commands_batch":
                    out = await self._execute_oms_commands_batch(msg)
                    writer.write(self._json_dumps_bytes(out) + b"\n")
                    await writer.drain()
                    continue
                if op == "ccxt_batch":
                    out = await self._execute_ccxt_batch(msg)
                    writer.write(self._json_dumps_bytes(out) + b"\n")
                    await writer.drain()
                    continue
                account_id = int(msg.get("account_id", 0) or 0)
                if op in {
                    "authorize_account",
                    "ccxt_call",
                    "reconcile_now",
                    "oms_query",
                    "ws_pull_events",
                    "ws_tail_id",
                    "risk_set_allow_new_positions",
                    "risk_set_strategy_allow_new_positions",
                    "risk_set_account_status",
                }:
                    if account_id <= 0:
                        writer.write(b"{\"ok\":false,\"error\":{\"code\":\"missing_account_id\"}}\n")
                        await writer.drain()
                        continue
                if account_id > 0:
                    if self._op_is_read_dispatch(msg):
                        out = await self._dispatch_to_read(msg)
                    else:
                        out = await self._dispatch_to_account(account_id, msg)
                else:
                    out = await self._dispatch_to_control(msg)
                writer.write(self._json_dumps_bytes(out) + b"\n")
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()


async def run_dispatcher() -> None:
    d = Dispatcher()
    await d.start()
    try:
        await asyncio.Future()
    finally:
        await d.stop()


if __name__ == "__main__":
    asyncio.run(run_dispatcher())

