import asyncio
from decimal import Decimal
from typing import Any

from apps.api.app.command_executor import execute_command_by_id


class _FakeConn:
    def __init__(self) -> None:
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


class _ConnCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeDB:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def connection(self) -> _ConnCtx:
        return _ConnCtx(self.conn)


class FakeCodec:
    def decrypt_maybe(self, value: Any) -> Any:
        return value


class FakeCCXTAdapter:
    def __init__(self) -> None:
        self.cancel_calls = 0
        self.create_calls = 0
        self.edit_calls = 0

    async def edit_order_if_supported(self, **_kwargs: Any) -> dict[str, Any] | None:
        self.edit_calls += 1
        return None

    async def cancel_order(self, **_kwargs: Any) -> dict[str, Any]:
        self.cancel_calls += 1
        return {
            "id": "old-exch-id",
            "clientOrderId": "cid-1",
            "symbol": "BTC/USDT",
        }

    async def create_order(self, **_kwargs: Any) -> dict[str, Any]:
        self.create_calls += 1
        return {
            "id": "new-exch-id",
            "clientOrderId": "cid-1",
            "symbol": "BTC/USDT",
        }


class FakeRepo:
    def __init__(self, command_type: str, payload: dict[str, Any]) -> None:
        self.command_type = command_type
        self.payload = payload
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.calls: dict[str, int] = {}
        self.last_merge_update: tuple[int, Decimal, Decimal] | None = None

    def _hit(self, name: str) -> None:
        self.calls[name] = int(self.calls.get(name, 0)) + 1

    async def fetch_command_for_worker(self, _conn: Any, _command_id: int) -> tuple[int, str, dict[str, Any]]:
        return 1, self.command_type, dict(self.payload)

    async def fetch_account_exchange_credentials(self, _conn: Any, _account_id: int):
        return "ccxt.binance", True, "k", "s", None, {}

    async def fetch_order_for_command_send(self, _conn: Any, _command_id: int) -> dict[str, Any] | None:
        return None

    async def fetch_order_by_id(self, _conn: Any, _account_id: int, order_id: int) -> dict[str, Any] | None:
        if order_id != 101:
            return None
        return {
            "id": 101,
            "symbol": "BTC/USDT",
            "side": "buy",
            "order_type": "limit",
            "status": "SUBMITTED",
            "qty": Decimal("1"),
            "price": Decimal("100"),
            "stop_loss": None,
            "stop_gain": None,
            "filled_qty": Decimal("0"),
            "strategy_id": 7,
            "position_id": 10,
            "reason": "api",
            "comment": "orig",
            "client_order_id": "cid-1",
            "exchange_order_id": "old-exch-id",
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
        _ = conn
        if account_id == 1 and exchange_order_id == "new-exch-id" and client_order_id == "cid-1" and symbol == "BTC/USDT" and side == "buy":
            return {
                "id": 202,
                "strategy_id": 0,
                "position_id": 20,
                "reason": "external",
                "comment": None,
                "client_order_id": "cid-1",
                "exchange_order_id": "new-exch-id",
            }
        return None

    async def fetch_open_position(self, _conn: Any, _account_id: int, position_id: int):
        if position_id == 20:
            return (20, "BTC/USDT", 0, "buy", "0.5", "110")
        if position_id == 10:
            return (10, "BTC/USDT", 7, "buy", "1.0", "100")
        if position_id == 30:
            return (30, "BTC/USDT", 11, "buy", "0.3", "90")
        if position_id == 40:
            return (40, "BTC/USDT", 11, "buy", "0.7", "95")
        return None

    async def mark_order_submitted_exchange(self, _conn: Any, _order_id: int, _exchange_order_id: str | None) -> None:
        self._hit("mark_order_submitted_exchange")

    async def mark_order_submitted_exchange_with_values(
        self, _conn: Any, _order_id: int, _exchange_order_id: str | None, _qty: Any, _price: Any
    ) -> None:
        self._hit("mark_order_submitted_exchange_with_values")

    async def mark_order_canceled(self, _conn: Any, _order_id: int) -> None:
        self._hit("mark_order_canceled")

    async def mark_order_canceled_edit_pending(self, _conn: Any, _order_id: int) -> None:
        self._hit("mark_order_canceled_edit_pending")

    async def mark_order_edit_replace_failed(self, _conn: Any, _order_id: int) -> None:
        self._hit("mark_order_edit_replace_failed")

    async def mark_order_consolidated_to_orphan(self, _conn: Any, _order_id: int, _orphan_order_id: int) -> None:
        self._hit("mark_order_consolidated_to_orphan")

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
        _ = (conn, orphan_order_id, origin_order_id, strategy_id, reason, comment)
        self._hit("adopt_external_orphan_order")

    async def update_order_position_link(self, _conn: Any, _order_id: int, _position_id: int) -> int:
        self._hit("update_order_position_link")
        return 1

    async def reassign_deals_strategy_by_orders(
        self, _conn: Any, *, account_id: int, order_ids: list[int], target_strategy_id: int
    ) -> int:
        _ = (account_id, order_ids, target_strategy_id)
        self._hit("reassign_deals_strategy_by_orders")
        return 1

    async def update_position_open_qty_price(self, _conn: Any, position_id: int, qty: Any, avg_price: Any) -> None:
        self._hit("update_position_open_qty_price")
        self.last_merge_update = (int(position_id), Decimal(str(qty)), Decimal(str(avg_price)))

    async def reassign_open_orders_position(
        self, _conn: Any, *, account_id: int, from_position_id: int, to_position_id: int
    ) -> int:
        _ = (account_id, from_position_id, to_position_id)
        self._hit("reassign_open_orders_position")
        return 2

    async def reassign_deals_position(
        self, _conn: Any, *, account_id: int, from_position_id: int, to_position_id: int
    ) -> int:
        _ = (account_id, from_position_id, to_position_id)
        self._hit("reassign_deals_position")
        return 3

    async def close_position_merged(self, _conn: Any, _position_id: int) -> None:
        self._hit("close_position_merged")

    async def update_position_targets_comment(self, _conn: Any, **_kwargs: Any) -> int:
        self._hit("update_position_targets_comment")
        return 1

    async def insert_ccxt_order_raw(self, conn: Any, **_kwargs: Any) -> None:
        _ = conn
        self._hit("insert_ccxt_order_raw")

    async def insert_event(self, conn: Any, *, namespace: str, event_type: str, payload: dict[str, Any], account_id: int) -> None:
        _ = conn
        _ = (namespace, account_id)
        self.events.append((event_type, payload))

    async def mark_command_completed(self, _conn: Any, _command_id: int) -> None:
        self._hit("mark_command_completed")

    async def mark_command_failed(self, _conn: Any, _command_id: int) -> None:
        self._hit("mark_command_failed")

    async def fetch_order_id_by_command_id(self, _conn: Any, _command_id: int) -> int | None:
        return 101

    async def mark_order_rejected(self, _conn: Any, _order_id: int) -> None:
        self._hit("mark_order_rejected")

    async def release_close_position_lock(self, _conn: Any, _position_id: int) -> None:
        self._hit("release_close_position_lock")

    async def reopen_position_if_close_requested(self, _conn: Any, _account_id: int, _position_id: int) -> int:
        self._hit("reopen_position_if_close_requested")
        return 1


def test_change_order_replace_consolidates_orphan_and_merges_positions() -> None:
    db = FakeDB()
    repo = FakeRepo(
        command_type="change_order",
        payload={"order_id": 101, "new_price": "101.5", "new_qty": "1.2"},
    )
    adapter = FakeCCXTAdapter()
    codec = FakeCodec()

    asyncio.run(
        execute_command_by_id(
            db=db,  # type: ignore[arg-type]
            repo=repo,  # type: ignore[arg-type]
            ccxt_adapter=adapter,  # type: ignore[arg-type]
            credentials_codec=codec,  # type: ignore[arg-type]
            command_id=1,
            account_id=1,
        )
    )

    assert adapter.edit_calls == 1
    assert adapter.cancel_calls == 1
    assert adapter.create_calls == 1
    assert repo.calls.get("mark_order_canceled_edit_pending", 0) == 1
    assert repo.calls.get("mark_order_consolidated_to_orphan", 0) == 1
    assert repo.calls.get("adopt_external_orphan_order", 0) == 1
    assert repo.calls.get("reassign_open_orders_position", 0) == 1
    assert repo.calls.get("reassign_deals_position", 0) == 1
    assert repo.calls.get("close_position_merged", 0) == 1
    assert repo.calls.get("update_order_position_link", 0) == 1
    assert repo.events[-1][0] == "order_change_replace_consolidated"


def test_merge_positions_reparents_open_orders_and_deals() -> None:
    db = FakeDB()
    repo = FakeRepo(
        command_type="merge_positions",
        payload={
            "source_position_id": 30,
            "target_position_id": 40,
            "stop_mode": "keep",
        },
    )
    adapter = FakeCCXTAdapter()
    codec = FakeCodec()

    asyncio.run(
        execute_command_by_id(
            db=db,  # type: ignore[arg-type]
            repo=repo,  # type: ignore[arg-type]
            ccxt_adapter=adapter,  # type: ignore[arg-type]
            credentials_codec=codec,  # type: ignore[arg-type]
            command_id=2,
            account_id=1,
        )
    )

    assert repo.calls.get("update_position_open_qty_price", 0) == 1
    assert repo.calls.get("reassign_open_orders_position", 0) == 1
    assert repo.calls.get("reassign_deals_position", 0) == 1
    assert repo.calls.get("close_position_merged", 0) == 1
    assert repo.last_merge_update is not None
    merged_position_id, merged_qty, merged_avg = repo.last_merge_update
    assert merged_position_id == 40
    assert merged_qty == Decimal("1.0")
    assert merged_avg == Decimal("93.5")
