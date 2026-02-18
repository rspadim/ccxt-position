import asyncio
import contextlib
from typing import Any

from .app.config import load_settings
from .app.db_mysql import DatabaseMySQL
from .app.repository_mysql import MySQLCommandRepository


async def _process_claimed_queue_item(
    db: DatabaseMySQL,
    repo: MySQLCommandRepository,
    queue_id: int,
    command_id: int,
    account_id: int,
) -> None:
    async with db.connection() as conn:
        try:
            cmd_account_id, command_type, payload = await repo.fetch_command_for_worker(
                conn, command_id
            )
            if cmd_account_id != account_id:
                raise RuntimeError("queue/account mismatch")

            if command_type == "send_order":
                order_id = await repo.fetch_order_id_by_command_id(conn, command_id)
                if order_id is not None:
                    # v0: placeholder exchange stage. Real CCXT send will replace this.
                    await repo.mark_order_submitted(conn, order_id)
                    await repo.insert_event(
                        conn=conn,
                        account_id=account_id,
                        namespace="position",
                        event_type="order_submitted",
                        payload={"command_id": command_id, "order_id": order_id},
                    )
            elif command_type in {"cancel_order", "change_order", "close_by"}:
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type=f"{command_type}_accepted",
                    payload={"command_id": command_id},
                )
            else:
                await repo.insert_event(
                    conn=conn,
                    account_id=account_id,
                    namespace="position",
                    event_type="command_accepted",
                    payload={"command_id": command_id, "command_type": command_type},
                )

            await repo.mark_command_completed(conn, command_id)
            await repo.mark_queue_done(conn, queue_id)
            await conn.commit()
        except Exception:
            await repo.mark_command_failed(conn, command_id)
            await repo.mark_queue_failed(conn, queue_id)
            await conn.commit()
            raise


async def run_worker() -> None:
    settings = load_settings()
    if settings.db_engine != "mysql":
        raise RuntimeError("worker-position v0 supports only mysql db_engine")

    db = DatabaseMySQL(settings)
    repo = MySQLCommandRepository()
    await db.connect()

    try:
        while True:
            claimed: tuple[int, int, int] | None = None
            async with db.connection() as conn:
                claimed = await repo.claim_next_queue_item(
                    conn=conn,
                    pool_id=settings.worker_pool_id,
                    worker_id=settings.worker_id,
                )
                await conn.commit()

            if claimed is None:
                await asyncio.sleep(max(settings.worker_poll_interval_ms, 50) / 1000.0)
                continue

            queue_id, command_id, account_id = claimed
            try:
                await _process_claimed_queue_item(db, repo, queue_id, command_id, account_id)
            except Exception:
                # failure already persisted; continue polling.
                pass
    finally:
        with contextlib.suppress(Exception):
            await db.disconnect()


if __name__ == "__main__":
    asyncio.run(run_worker())

