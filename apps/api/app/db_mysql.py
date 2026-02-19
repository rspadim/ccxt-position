from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from .config import Settings


class DatabaseMySQL:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pool: Any = None

    async def connect(self) -> None:
        driver = self.settings.mysql_driver.lower().strip()
        if driver == "asyncmy":
            import asyncmy  # type: ignore

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

        if driver == "aiomysql":
            import aiomysql  # type: ignore

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

        raise RuntimeError(f"unsupported mysql_driver: {self.settings.mysql_driver!r}")

    async def disconnect(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        if self.pool is None:
            raise RuntimeError("database not initialized")
        async with self.pool.acquire() as conn:
            try:
                yield conn
            except Exception:
                await conn.rollback()
                raise
