import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import ccxt.async_support as ccxt_async

try:
    import ccxt.pro as ccxt_pro  # type: ignore
except Exception:
    ccxt_pro = None


def _as_plain_secret(value: str | None) -> str | None:
    if value is None:
        return None
    # v0 compatibility: treat stored value as already usable secret text.
    return str(value)


class CCXTAdapter:
    @dataclass
    class _Session:
        exchange: Any
        fingerprint: str
        last_used_at: float

    def __init__(self, logger: Any | None = None, session_ttl_seconds: float = 600.0) -> None:
        self.logger = logger
        self.session_ttl_seconds = float(session_ttl_seconds)
        self._sessions: dict[str, CCXTAdapter._Session] = {}
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    @staticmethod
    def _session_key(exchange_id: str, session_key: str) -> str:
        return f"{str(exchange_id).strip().lower()}::{str(session_key).strip()}"

    @staticmethod
    def _fingerprint(
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
    ) -> str:
        payload = {
            "use_testnet": bool(use_testnet),
            "api_key": _as_plain_secret(api_key),
            "secret": _as_plain_secret(secret),
            "passphrase": _as_plain_secret(passphrase),
            "extra_config": (extra_config or {}),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _should_use_persistent_session(exchange_id: str, session_key: str | None) -> bool:
        if not session_key:
            return False
        return str(exchange_id or "").strip().lower().startswith("ccxtpro.")

    async def _close_session_by_key(self, key: str) -> None:
        session = self._sessions.pop(key, None)
        if session is None:
            return
        try:
            await session.exchange.close()
        except Exception:
            return

    async def close_all_sessions(self) -> None:
        keys = list(self._sessions.keys())
        for key in keys:
            await self._close_session_by_key(key)

    def get_session_status(self) -> dict[str, Any]:
        now = time.monotonic()
        by_engine: dict[str, int] = {}
        account_ids: set[int] = set()
        oldest_age = 0.0
        for key, session in self._sessions.items():
            parts = str(key).split("::", 1)
            exchange_id = parts[0] if parts else ""
            session_key = parts[1] if len(parts) > 1 else ""
            engine = "ccxtpro" if str(exchange_id).startswith("ccxtpro.") else (
                "ccxt" if str(exchange_id).startswith("ccxt.") else "unknown"
            )
            by_engine[engine] = int(by_engine.get(engine, 0)) + 1
            if str(session_key).startswith("account:"):
                raw = str(session_key).split(":", 1)[1].strip()
                if raw.isdigit():
                    account_ids.add(int(raw))
            age = max(0.0, now - float(session.last_used_at))
            if age > oldest_age:
                oldest_age = age
        return {
            "session_count_total": int(len(self._sessions)),
            "session_count_by_engine": by_engine,
            "session_account_ids": sorted(account_ids),
            "session_ttl_seconds": float(self.session_ttl_seconds),
            "session_oldest_age_seconds": int(oldest_age),
        }

    async def _get_session_lock(self, key: str) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._session_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._session_locks[key] = lock
            return lock

    async def _cleanup_expired_sessions(self) -> None:
        if self.session_ttl_seconds <= 0:
            return
        now = time.monotonic()
        expired = [
            key
            for key, session in list(self._sessions.items())
            if (now - float(session.last_used_at)) > self.session_ttl_seconds
        ]
        for key in expired:
            await self._close_session_by_key(key)

    async def _with_exchange(
        self,
        *,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        session_key: str | None,
        runner: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        await self._cleanup_expired_sessions()
        persistent = self._should_use_persistent_session(exchange_id, session_key)
        if not persistent:
            exchange = self._build_exchange(
                exchange_id=exchange_id,
                use_testnet=use_testnet,
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
                extra_config=extra_config,
            )
            try:
                return await runner(exchange)
            finally:
                await exchange.close()

        key = self._session_key(exchange_id, str(session_key))
        expected_fp = self._fingerprint(use_testnet, api_key, secret, passphrase, extra_config)
        lock = await self._get_session_lock(key)
        async with lock:
            session = self._sessions.get(key)
            if session is not None and session.fingerprint != expected_fp:
                await self._close_session_by_key(key)
                session = None
            if session is None:
                exchange = self._build_exchange(
                    exchange_id=exchange_id,
                    use_testnet=use_testnet,
                    api_key=api_key,
                    secret=secret,
                    passphrase=passphrase,
                    extra_config=extra_config,
                )
                session = CCXTAdapter._Session(
                    exchange=exchange,
                    fingerprint=expected_fp,
                    last_used_at=time.monotonic(),
                )
                self._sessions[key] = session
            try:
                out = await runner(session.exchange)
                session.last_used_at = time.monotonic()
                self._sessions[key] = session
                return out
            except Exception:
                await self._close_session_by_key(key)
                raise

    @staticmethod
    def _resolve_exchange_class_id(exchange_id: str) -> tuple[str, str]:
        raw = str(exchange_id or "").strip()
        if not raw:
            raise RuntimeError("unsupported_engine")
        lowered = raw.lower()
        if lowered.startswith("ccxtpro."):
            cls_id = raw.split(".", 1)[1].strip()
            if not cls_id:
                raise RuntimeError("unsupported_engine")
            return "ccxtpro", cls_id
        if lowered.startswith("ccxt."):
            cls_id = raw.split(".", 1)[1].strip()
            if not cls_id:
                raise RuntimeError("unsupported_engine")
            return "ccxt", cls_id
        raise RuntimeError("unsupported_engine")

    def _build_exchange(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None = None,
    ) -> Any:
        engine, exchange_class_id = self._resolve_exchange_class_id(exchange_id)
        module: Any
        if engine == "ccxt":
            module = ccxt_async
        elif engine == "ccxtpro":
            if ccxt_pro is None:
                raise RuntimeError("engine_unavailable")
            module = ccxt_pro
        else:
            raise RuntimeError("unsupported_engine")
        exchange_cls = getattr(module, exchange_class_id, None)
        if exchange_cls is None:
            raise RuntimeError(f"unsupported exchange_id: {exchange_id}")
        config: dict[str, Any] = {}
        if isinstance(extra_config, dict):
            config.update(extra_config)
        config["apiKey"] = _as_plain_secret(api_key)
        config["secret"] = _as_plain_secret(secret)
        config["password"] = _as_plain_secret(passphrase)
        if "enableRateLimit" not in config:
            config["enableRateLimit"] = True
        exchange = exchange_cls(
            config
        )
        if use_testnet:
            setter = getattr(exchange, "set_sandbox_mode", None)
            if callable(setter):
                setter(True)
        return exchange

    async def execute_method(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        method: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        logger: Any | None = None,
        session_key: str | None = None,
    ) -> Any:
        async def _run(exchange: Any) -> Any:
            fn = getattr(exchange, method, None)
            if fn is None:
                raise RuntimeError(f"unsupported ccxt method: {method}")
            active_logger = logger or self.logger
            if active_logger is not None:
                active_logger.info(
                    "ccxt_call %s",
                    {"exchange_id": exchange_id, "method": method},
                )
            return await fn(*(args or []), **(kwargs or {}))
        return await self._with_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            session_key=session_key,
            runner=_run,
        )

    @staticmethod
    def _supports_capability(has_map: Any, capability: str) -> bool:
        if not isinstance(has_map, dict):
            return False
        value = has_map.get(capability)
        return value is True or value == "emulated"

    async def execute_unified_with_capability(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        method: str,
        capabilities: list[str],
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        session_key: str | None = None,
    ) -> Any:
        async def _run(exchange: Any) -> Any:
            if capabilities and not any(
                self._supports_capability(exchange.has, capability) for capability in capabilities
            ):
                raise RuntimeError(
                    f"exchange {exchange_id} does not support required capability for {method}: {capabilities}"
                )
            fn = getattr(exchange, method, None)
            if fn is None:
                raise RuntimeError(f"unsupported ccxt method: {method}")
            return await fn(*(args or []), **(kwargs or {}))
        return await self._with_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            session_key=session_key,
            runner=_run,
        )

    async def create_order(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
        session_key: str | None = None,
    ) -> dict[str, Any]:
        async def _run(exchange: Any) -> dict[str, Any]:
            return await exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params,
            )
        return await self._with_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            session_key=session_key,
            runner=_run,
        )

    async def cancel_order(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        exchange_order_id: str,
        symbol: str,
        params: dict[str, Any] | None = None,
        session_key: str | None = None,
    ) -> dict[str, Any]:
        async def _run(exchange: Any) -> dict[str, Any]:
            return await exchange.cancel_order(
                id=exchange_order_id,
                symbol=symbol,
                params=params or {},
            )
        return await self._with_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            session_key=session_key,
            runner=_run,
        )

    async def edit_or_replace_order(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        exchange_order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
        session_key: str | None = None,
    ) -> dict[str, Any]:
        async def _run(exchange: Any) -> dict[str, Any]:
            await exchange.load_markets()
            can_edit = bool(exchange.has.get("editOrder")) if isinstance(exchange.has, dict) else False
            if can_edit:
                return await exchange.edit_order(
                    id=exchange_order_id,
                    symbol=symbol,
                    type=order_type,
                    side=side,
                    amount=amount,
                    price=price,
                    params=params,
                )

            await exchange.cancel_order(id=exchange_order_id, symbol=symbol, params={})
            return await exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params,
            )
        return await self._with_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            session_key=session_key,
            runner=_run,
        )

    async def edit_order_if_supported(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        exchange_order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
        session_key: str | None = None,
    ) -> dict[str, Any] | None:
        async def _run(exchange: Any) -> dict[str, Any] | None:
            await exchange.load_markets()
            can_edit = bool(exchange.has.get("editOrder")) if isinstance(exchange.has, dict) else False
            if not can_edit:
                return None
            return await exchange.edit_order(
                id=exchange_order_id,
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params,
            )
        return await self._with_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            session_key=session_key,
            runner=_run,
        )

    async def fetch_my_trades(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = 200,
        params: dict[str, Any] | None = None,
        session_key: str | None = None,
    ) -> list[dict[str, Any]]:
        out = await self.execute_method(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            method="fetch_my_trades",
            args=[symbol, since, limit, params or {}],
            session_key=session_key,
        )
        if isinstance(out, list):
            return out
        return []
