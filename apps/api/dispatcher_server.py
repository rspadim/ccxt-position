import asyncio
import hashlib
import hmac
import json
import secrets
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any

from pydantic import TypeAdapter

from .app.auth import AuthContext, validate_api_key
from .app.ccxt_adapter import CCXTAdapter
from .app.config import load_settings
from .app.credentials_codec import CredentialsCodec
from .app.db_mysql import DatabaseMySQL
from .app.logging_utils import setup_application_logging
from .app.repository_mysql import MySQLCommandRepository
from .app.schemas import CommandInput
from .app.service import process_single_command_direct
from .worker_position import _reconcile_account_once


COMMAND_INPUT_ADAPTER = TypeAdapter(CommandInput)


@dataclass
class _Job:
    account_id: int
    payload: dict[str, Any]
    future: asyncio.Future


class Dispatcher:
    def __init__(self) -> None:
        self.settings = load_settings()
        self.db = DatabaseMySQL(self.settings)
        self.repo = MySQLCommandRepository(event_sink=self._publish_event)
        self.loggers = setup_application_logging(
            self.settings.disable_uvicorn_access_log, log_dir=self.settings.log_dir
        )
        self.ccxt = CCXTAdapter(logger=self.loggers.get("ccxt"))
        self.codec = CredentialsCodec(
            self.settings.encryption_master_key,
            require_encrypted=self.settings.require_encrypted_credentials,
        )
        self.pool_size = max(1, int(self.settings.dispatcher_pool_size))
        self.worker_queues: dict[int, asyncio.Queue[_Job]] = {}
        self.worker_tasks: dict[int, asyncio.Task] = {}
        self.account_worker: dict[int, int] = {}
        self.account_locks: dict[int, asyncio.Lock] = {}
        self.worker_active_accounts: dict[int, set[int]] = {
            wid: set() for wid in range(self.pool_size)
        }
        self.worker_inflight: dict[int, int] = {wid: 0 for wid in range(self.pool_size)}
        self.started_at = int(time.time())
        self.total_requests = 0
        self.total_errors = 0
        self.op_counts: dict[str, int] = {}
        self._server: asyncio.AbstractServer | None = None
        self._ws_events_by_account: dict[int, deque[dict[str, Any]]] = {}
        self._ws_event_seq = 0
        self._ws_event_buffer_limit = 5000

    async def _auth_from_payload(self, msg: dict[str, Any]) -> AuthContext:
        api_key = str(msg.get("x_api_key", "") or "").strip()
        if not api_key:
            raise RuntimeError("missing_api_key")
        auth = await validate_api_key(self.db, api_key)
        if auth is None:
            raise RuntimeError("invalid_api_key")
        return auth

    @staticmethod
    def _require_admin(auth: AuthContext) -> None:
        if not auth.is_admin:
            raise RuntimeError("admin_required")

    @staticmethod
    def _json_dumps(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), default=str)

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
        async with self.db.connection() as conn:
            account = await self.repo.fetch_account_by_id(conn, account_id)
            if account is None or account["status"] != "active":
                await conn.commit()
                raise RuntimeError("account_not_found")
            perms = await self.repo.fetch_api_key_account_permissions(conn, auth.api_key_id, account_id)
            await conn.commit()
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
        async with self.db.connection() as conn:
            perms = await self.repo.fetch_api_key_account_permissions(conn, auth.api_key_id, account_id)
            if perms is None:
                await conn.commit()
                raise RuntimeError("permission_denied")
            if not bool(perms.get("restrict_to_strategies")):
                await conn.commit()
                return
            allowed = await self.repo.api_key_strategy_allowed(
                conn, auth.api_key_id, account_id, int(strategy_id), for_trade=for_trade
            )
            await conn.commit()
        if not allowed:
            raise RuntimeError("strategy_permission_denied")

    async def _require_position_command_permission(self, auth: AuthContext, item: CommandInput) -> int:
        account_id = int(item.account_id or 0)
        payload = item.payload.model_dump(by_alias=True, exclude_none=True, mode="json")
        command = str(item.command)
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
        for wid in range(self.pool_size):
            q: asyncio.Queue[_Job] = asyncio.Queue()
            self.worker_queues[wid] = q
            self.worker_tasks[wid] = asyncio.create_task(self._worker_loop(wid, q))
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.settings.dispatcher_host,
            port=self.settings.dispatcher_port,
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for task in self.worker_tasks.values():
            task.cancel()
        await self.db.disconnect()

    async def _resolve_worker_for_account(self, account_id: int) -> int:
        cached = self.account_worker.get(account_id)
        if cached is not None and 0 <= cached < self.pool_size:
            return cached

        hinted: int | None = None
        async with self.db.connection() as conn:
            hinted = await self.repo.fetch_account_dispatcher_worker_hint(conn, account_id)
            await conn.commit()
        if hinted is not None and 0 <= int(hinted) < self.pool_size:
            wid = int(hinted)
            self.account_worker[account_id] = wid
            self.worker_active_accounts[wid].add(account_id)
            return wid

        # Least-loaded by (inflight + active_accounts).
        wid = min(
            range(self.pool_size),
            key=lambda w: (self.worker_inflight[w], len(self.worker_active_accounts[w]), w),
        )
        self.account_worker[account_id] = wid
        self.worker_active_accounts[wid].add(account_id)
        async with self.db.connection() as conn:
            await self.repo.set_account_dispatcher_worker_hint(conn, account_id, wid)
            await conn.commit()
        return wid

    async def _worker_loop(self, worker_id: int, queue: asyncio.Queue[_Job]) -> None:
        while True:
            job = await queue.get()
            try:
                self.worker_inflight[worker_id] += 1
                lock = self.account_locks.setdefault(job.account_id, asyncio.Lock())
                async with lock:
                    out = await self._execute(job.payload)
                if not job.future.done():
                    job.future.set_result(out)
            except Exception as exc:
                self.total_errors += 1
                if not job.future.done():
                    job.future.set_result(
                        {"ok": False, "error": {"code": "dispatcher_error", "message": str(exc)}}
                    )
            finally:
                self.worker_inflight[worker_id] = max(0, self.worker_inflight[worker_id] - 1)
                queue.task_done()

    async def _dispatch_to_account(self, account_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        wid = await self._resolve_worker_for_account(account_id)
        await self.worker_queues[wid].put(_Job(account_id=account_id, payload=payload, future=fut))
        out = await fut
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

    async def _execute_position_commands_batch(self, msg: dict[str, Any]) -> dict[str, Any]:
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
                        "op": "position_command",
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
                fut: asyncio.Future = asyncio.get_running_loop().create_future()
                await self.worker_queues[0].put(_Job(account_id=0, payload=req["payload"], future=fut))
                out = await fut
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
        if op == "position_command":
            auth = await self._auth_from_payload(msg)
            index = int(msg.get("index", 0) or 0)
            raw_item = msg.get("item") if isinstance(msg.get("item"), dict) else {}
            item = COMMAND_INPUT_ADAPTER.validate_python(raw_item)
            resolved_account_id = await self._require_position_command_permission(auth, item)
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
            return {"ok": True, "result": result.model_dump()}

        if op == "authorize_account":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            require_trade = bool(msg.get("require_trade", False))
            for_ws = bool(msg.get("for_ws", False))
            account = await self._require_account_permission(
                auth, account_id, require_trade=require_trade, for_ws=for_ws
            )
            return {"ok": True, "result": {"account_id": account_id, "exchange_id": account["exchange_id"]}}

        if op == "ccxt_call":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            func = str(msg.get("func", "")).strip()
            args = msg.get("args") if isinstance(msg.get("args"), list) else []
            kwargs = msg.get("kwargs") if isinstance(msg.get("kwargs"), dict) else {}
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
            result = await self.ccxt.execute_method(
                exchange_id=account["exchange_id"],
                use_testnet=is_testnet,
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
                extra_config=extra_config,
                method=func,
                args=args,
                kwargs=kwargs,
            )
            return {"ok": True, "result": result}

        if op == "position_query":
            auth = await self._auth_from_payload(msg)
            query = str(msg.get("query", "")).strip()
            account_id = int(msg.get("account_id", 0) or 0)
            strategy_id_raw = msg.get("strategy_id")
            try:
                strategy_id = None if strategy_id_raw in {None, ""} else int(strategy_id_raw)
            except Exception:
                return {"ok": False, "error": {"code": "validation_error", "message": "strategy_id must be integer"}}
            await self._require_account_permission(auth, account_id, require_trade=False)
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
                    rows = await self.repo.list_orders(conn, account_id, open_only=True, strategy_id=strategy_id)
                elif query == "orders_history":
                    rows = await self.repo.list_orders(conn, account_id, open_only=False, strategy_id=strategy_id)
                elif query == "deals":
                    rows = await self.repo.list_deals(conn, account_id, strategy_id=strategy_id)
                elif query == "positions_open":
                    rows = await self.repo.list_positions(conn, account_id, open_only=True, strategy_id=strategy_id)
                elif query == "positions_history":
                    rows = await self.repo.list_positions(conn, account_id, open_only=False, strategy_id=strategy_id)
                else:
                    await conn.commit()
                    return {"ok": False, "error": {"code": "unsupported_query"}}
                await conn.commit()
            return {"ok": True, "result": rows}

        if op == "position_reassign":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            deal_ids = msg.get("deal_ids") if isinstance(msg.get("deal_ids"), list) else []
            order_ids = msg.get("order_ids") if isinstance(msg.get("order_ids"), list) else []
            target_strategy_id = int(msg.get("target_strategy_id", 0) or 0)
            target_position_id = int(msg.get("target_position_id", 0) or 0)
            await self._require_account_permission(auth, account_id, require_trade=True)
            async with self.db.connection() as conn:
                deals_count = await self.repo.reassign_deals(
                    conn=conn,
                    account_id=account_id,
                    deal_ids=[int(x) for x in deal_ids],
                    target_strategy_id=target_strategy_id,
                    target_position_id=target_position_id,
                )
                orders_count = await self.repo.reassign_orders(
                    conn=conn,
                    account_id=account_id,
                    order_ids=[int(x) for x in order_ids],
                    target_strategy_id=target_strategy_id,
                    target_position_id=target_position_id,
                )
                await self.repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="reassigned",
                    payload={
                        "deals_updated": deals_count,
                        "orders_updated": orders_count,
                        "target_strategy_id": target_strategy_id,
                        "target_position_id": target_position_id,
                    },
                )
                await conn.commit()
            return {"ok": True, "result": {"deals_updated": deals_count, "orders_updated": orders_count}}

        if op == "reconcile_now":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            lookback_seconds = int(msg.get("lookback_seconds", 600) or 600)
            scope = str(msg.get("scope", "manual")).strip() or "manual"
            await self._require_account_permission(auth, account_id, require_trade=False)
            async with self.db.connection() as conn:
                await _reconcile_account_once(
                    conn=conn,
                    repo=self.repo,
                    ccxt_adapter=self.ccxt,
                    credentials_codec=self.codec,
                    account_id=account_id,
                    lookback_seconds=max(60, lookback_seconds),
                    scope=scope,
                    limit=max(10, int(self.settings.worker_reconcile_batch_limit)),
                )
                await conn.commit()
            return {
                "ok": True,
                "result": {
                    "account_id": account_id,
                    "scope": scope,
                    "lookback_seconds": max(60, lookback_seconds),
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
            return {"ok": True, "result": rows}

        if op == "risk_set_allow_new_positions":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            allow = bool(msg.get("allow_new_positions", True))
            await self._require_account_permission(
                auth,
                account_id,
                require_risk_manage=True,
                require_block_new_positions=True,
            )
            async with self.db.connection() as conn:
                changed = await self.repo.set_allow_new_positions(conn, account_id, allow)
                await conn.commit()
            return {"ok": True, "result": {"account_id": account_id, "allow_new_positions": allow, "rows": changed}}

        if op == "risk_set_strategy_allow_new_positions":
            auth = await self._auth_from_payload(msg)
            account_id = int(msg.get("account_id", 0) or 0)
            strategy_id = int(msg.get("strategy_id", 0) or 0)
            allow = bool(msg.get("allow_new_positions", True))
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
            if status not in {"active", "blocked"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|blocked"}}
            await self._require_account_permission(
                auth,
                account_id,
                require_risk_manage=True,
                require_block_account=True,
            )
            async with self.db.connection() as conn:
                changed = await self.repo.set_account_status(conn, account_id, status)
                await conn.commit()
            return {"ok": True, "result": {"account_id": account_id, "status": status, "rows": changed}}

        if op == "admin_create_account":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            exchange_id = str(msg.get("exchange_id", "")).strip()
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
            return {"ok": True, "result": items}

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
            exchange_id = None if exchange_id_raw is None else str(exchange_id_raw).strip()
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
            return {"ok": True, "result": {"account_id": account_id, "rows": rows}}

        if op == "admin_create_user_api_key":
            auth = await self._auth_from_payload(msg)
            self._require_admin(auth)
            user_name = str(msg.get("user_name", "")).strip()
            role = str(msg.get("role", "trade")).strip().lower()
            if role not in {"admin", "trade"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "role must be admin|trade"}}
            if not user_name:
                return {"ok": False, "error": {"code": "validation_error", "message": "user_name is required"}}
            password_raw = msg.get("password")
            password = None if password_raw is None else str(password_raw)
            api_key_plain = str(msg.get("api_key") or secrets.token_urlsafe(32))
            api_key_hash = hashlib.sha256(api_key_plain.encode("utf-8")).hexdigest()
            permissions = msg.get("permissions") if isinstance(msg.get("permissions"), list) else []
            async with self.db.connection() as conn:
                user_id = await self.repo.create_user(conn, user_name, role=role)
                if password:
                    await self.repo.set_user_password_hash(conn, user_id, self._new_password_hash(password))
                api_key_id = await self.repo.create_api_key(conn, user_id, api_key_hash)
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
            if not name:
                return {"ok": False, "error": {"code": "validation_error", "message": "name is required"}}
            async with self.db.connection() as conn:
                strategy_id = await self.repo.create_strategy(conn, name=name)
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
                strategy_id = await self.repo.create_strategy(conn, name=name)
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
            name = None if name_raw is None else str(name_raw).strip()
            status = None if status_raw is None else str(status_raw).strip().lower()
            if status is not None and status not in {"active", "disabled"}:
                return {"ok": False, "error": {"code": "validation_error", "message": "status must be active|disabled"}}
            async with self.db.connection() as conn:
                rows = await self.repo.update_strategy(conn, strategy_id, name=name, status=status)
                await conn.commit()
            return {"ok": True, "result": {"strategy_id": strategy_id, "rows": rows}}

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
            return {
                "ok": True,
                "result": {
                    "started_at": self.started_at,
                    "pool_size": self.pool_size,
                    "total_requests": self.total_requests,
                    "total_errors": self.total_errors,
                    "op_counts": self.op_counts,
                    "accounts_mapped": len(self.account_worker),
                    "worker_inflight": self.worker_inflight,
                    "worker_active_accounts": {
                        str(k): len(v) for k, v in self.worker_active_accounts.items()
                    },
                    "worker_queue_depth": {
                        str(k): self.worker_queues[k].qsize() for k in self.worker_queues
                    },
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
            line = await reader.readline()
            if not line:
                return
            try:
                msg = json.loads(line.decode("utf-8"))
            except Exception:
                writer.write(b"{\"ok\":false,\"error\":{\"code\":\"invalid_json\"}}\n")
                await writer.drain()
                return
            self.total_requests += 1
            op = str(msg.get("op", "")).strip()
            if op == "status":
                out = await self._execute(msg)
                writer.write((self._json_dumps(out) + "\n").encode("utf-8"))
                await writer.drain()
                return
            if op == "position_commands_batch":
                out = await self._execute_position_commands_batch(msg)
                writer.write((self._json_dumps(out) + "\n").encode("utf-8"))
                await writer.drain()
                return
            if op == "ccxt_batch":
                out = await self._execute_ccxt_batch(msg)
                writer.write((self._json_dumps(out) + "\n").encode("utf-8"))
                await writer.drain()
                return
            account_id = int(msg.get("account_id", 0) or 0)
            if op in {
                "authorize_account",
                "ccxt_call",
                "reconcile_now",
                "position_query",
                "position_reassign",
                "ws_pull_events",
                "ws_tail_id",
                "risk_set_allow_new_positions",
                "risk_set_strategy_allow_new_positions",
                "risk_set_account_status",
            }:
                if account_id <= 0:
                    writer.write(b"{\"ok\":false,\"error\":{\"code\":\"missing_account_id\"}}\n")
                    await writer.drain()
                    return
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            if account_id > 0:
                wid = await self._resolve_worker_for_account(account_id)
                queue = self.worker_queues[wid]
                await queue.put(_Job(account_id=account_id, payload=msg, future=fut))
            else:
                # Account-less ops run on worker 0.
                await self.worker_queues[0].put(_Job(account_id=0, payload=msg, future=fut))
            out = await fut
            writer.write((self._json_dumps(out) + "\n").encode("utf-8"))
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

