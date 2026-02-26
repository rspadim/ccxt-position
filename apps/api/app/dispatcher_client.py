import asyncio
import json
import os
from typing import Any

DISPATCH_STREAM_LIMIT_BYTES = 8 * 1024 * 1024
DISPATCH_POOL_MAX_SIZE = max(1, int(os.getenv("DISPATCHER_CLIENT_POOL_SIZE", "16")))


class _DispatcherConnection:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = int(port)
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def _connect(self, timeout_seconds: int | None) -> None:
        if timeout_seconds is None:
            self.reader, self.writer = await asyncio.open_connection(
                host=self.host,
                port=self.port,
                limit=DISPATCH_STREAM_LIMIT_BYTES,
            )
            return
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(
                host=self.host,
                port=self.port,
                limit=DISPATCH_STREAM_LIMIT_BYTES,
            ),
            timeout=max(1, int(timeout_seconds)),
        )

    async def _ensure_connected(self, timeout_seconds: int | None) -> None:
        if self.reader is not None and self.writer is not None and not self.writer.is_closing():
            return
        await self._connect(timeout_seconds)

    async def close(self) -> None:
        writer = self.writer
        self.reader = None
        self.writer = None
        if writer is None:
            return
        writer.close()
        await writer.wait_closed()

    async def request(self, payload: dict[str, Any], timeout_seconds: int | None) -> dict[str, Any]:
        await self._ensure_connected(timeout_seconds)
        if self.writer is None or self.reader is None:
            raise RuntimeError("dispatcher_connection_not_ready")
        self.writer.write((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
        await self.writer.drain()
        if timeout_seconds is None:
            raw = await self.reader.readline()
        else:
            raw = await asyncio.wait_for(self.reader.readline(), timeout=max(1, int(timeout_seconds)))
        if not raw:
            raise RuntimeError("dispatcher_empty_response")
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise RuntimeError(f"dispatcher_invalid_json:{exc}") from exc


class _DispatcherConnectionPool:
    def __init__(self, host: str, port: int, max_size: int) -> None:
        self.host = host
        self.port = int(port)
        self.max_size = max(1, int(max_size))
        self._slots = asyncio.Semaphore(self.max_size)
        self._idle: asyncio.LifoQueue[_DispatcherConnection] = asyncio.LifoQueue()

    async def _acquire(self, timeout_seconds: int | None) -> _DispatcherConnection:
        if timeout_seconds is None:
            await self._slots.acquire()
        else:
            await asyncio.wait_for(self._slots.acquire(), timeout=max(1, int(timeout_seconds)))
        try:
            return self._idle.get_nowait()
        except asyncio.QueueEmpty:
            return _DispatcherConnection(self.host, self.port)

    async def _release(self, conn: _DispatcherConnection, reusable: bool) -> None:
        try:
            if reusable:
                self._idle.put_nowait(conn)
            else:
                await conn.close()
        finally:
            self._slots.release()

    async def request(self, payload: dict[str, Any], timeout_seconds: int | None) -> dict[str, Any]:
        conn = await self._acquire(timeout_seconds)
        reusable = False
        try:
            out = await conn.request(payload, timeout_seconds)
            reusable = True
            return out
        finally:
            await self._release(conn, reusable=reusable)


_POOLS: dict[tuple[str, int], _DispatcherConnectionPool] = {}
_POOLS_LOCK = asyncio.Lock()


async def _get_pool(host: str, port: int) -> _DispatcherConnectionPool:
    key = (str(host), int(port))
    pool = _POOLS.get(key)
    if pool is not None:
        return pool
    async with _POOLS_LOCK:
        pool = _POOLS.get(key)
        if pool is None:
            pool = _DispatcherConnectionPool(str(host), int(port), DISPATCH_POOL_MAX_SIZE)
            _POOLS[key] = pool
        return pool


async def dispatch_request(
    host: str,
    port: int,
    payload: dict[str, Any],
    timeout_seconds: int | None = 30,
) -> dict[str, Any]:
    try:
        pool = await _get_pool(host, port)
        return await pool.request(payload, timeout_seconds)
    except TimeoutError:
        return {"ok": False, "error": {"code": "dispatcher_timeout"}}
    except RuntimeError as exc:
        msg = str(exc)
        if msg == "dispatcher_empty_response":
            return {"ok": False, "error": {"code": "dispatcher_empty_response"}}
        if msg.startswith("dispatcher_invalid_json:"):
            return {"ok": False, "error": {"code": "dispatcher_invalid_json", "message": msg.split(":", 1)[1]}}
        return {"ok": False, "error": {"code": "dispatcher_unavailable", "message": msg}}
    except Exception as exc:
        return {"ok": False, "error": {"code": "dispatcher_unavailable", "message": str(exc)}}
