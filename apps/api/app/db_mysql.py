import asyncio
import contextlib
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from .config import Settings


class DatabaseMySQL:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pool: Any = None

    async def connect(self) -> None:
        attempts = 20
        delay_seconds = 1.0
        driver = self.settings.mysql_driver.lower().strip()
        if driver == "asyncmy":
            import asyncmy  # type: ignore

            last_exc: Exception | None = None
            for idx in range(attempts):
                try:
                    self.pool = await asyncmy.create_pool(
                        host=self.settings.mysql_host,
                        port=self.settings.mysql_port,
                        user=self.settings.mysql_user,
                        password=self.settings.mysql_password,
                        db=self.settings.mysql_database,
                        minsize=self.settings.mysql_min_pool_size,
                        maxsize=self.settings.mysql_max_pool_size,
                        autocommit=False,
                    )
                    return
                except Exception as exc:
                    last_exc = exc
                    if idx == attempts - 1:
                        break
                    await asyncio.sleep(delay_seconds)
            raise RuntimeError(f"failed to connect mysql after {attempts} attempts") from last_exc

        if driver == "aiomysql":
            import aiomysql  # type: ignore

            last_exc: Exception | None = None
            for idx in range(attempts):
                try:
                    self.pool = await aiomysql.create_pool(
                        host=self.settings.mysql_host,
                        port=self.settings.mysql_port,
                        user=self.settings.mysql_user,
                        password=self.settings.mysql_password,
                        db=self.settings.mysql_database,
                        minsize=self.settings.mysql_min_pool_size,
                        maxsize=self.settings.mysql_max_pool_size,
                        autocommit=False,
                    )
                    return
                except Exception as exc:
                    last_exc = exc
                    if idx == attempts - 1:
                        break
                    await asyncio.sleep(delay_seconds)
            raise RuntimeError(f"failed to connect mysql after {attempts} attempts") from last_exc

        raise RuntimeError(f"unsupported mysql_driver: {self.settings.mysql_driver!r}")

    async def reconnect(self) -> None:
        await self.disconnect()
        await self.connect()

    async def disconnect(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        if self.pool is None:
            await self.connect()
        acquired = False
        try:
            async with self.pool.acquire() as conn:
                acquired = True
                try:
                    yield conn
                except Exception:
                    await conn.rollback()
                    raise
        except Exception as exc:
            if acquired:
                # SQL/runtime errors from caller should propagate as-is.
                raise
            with contextlib.suppress(Exception):
                await self.reconnect()
            raise RuntimeError("database unavailable after reconnect attempt") from exc
