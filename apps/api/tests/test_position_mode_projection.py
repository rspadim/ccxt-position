import asyncio
from decimal import Decimal
from typing import Any

from apps.api.worker_position import _project_trade_to_position


class FakeRepo:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.next_position_id = 1
        self.next_order_id = 1
        self.positions: dict[int, dict[str, Any]] = {}
        self.orders: dict[int, dict[str, Any]] = {}
        self.deals: list[dict[str, Any]] = []

    async def deal_exists_by_exchange_trade_id(self, _conn: Any, _account_id: int, _trade_id: str | None) -> bool:
        return False

    async def fetch_open_order_link(
        self,
        _conn: Any,
        _account_id: int,
        exchange_order_id: str | None,
        client_order_id: str | None,
    ) -> dict[str, Any] | None:
        for o in self.orders.values():
            if exchange_order_id and o.get("exchange_order_id") == exchange_order_id:
                return dict(o)
            if client_order_id and o.get("client_order_id") == client_order_id:
                return dict(o)
        return None

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
        row = await self.fetch_open_order_link(
            conn,
            account_id,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        )
        if row is not None:
            return row
        oid = self.next_order_id
        self.next_order_id += 1
        row = {
            "id": oid,
            "strategy_id": 0,
            "position_id": 0,
            "stop_loss": None,
            "stop_gain": None,
            "comment": None,
            "reason": "external",
            "exchange_order_id": exchange_order_id,
            "client_order_id": client_order_id,
            "account_id": account_id,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
        }
        self.orders[oid] = dict(row)
        return dict(row)

    async def update_order_position_link(self, _conn: Any, order_id: int, position_id: int) -> int:
        if order_id in self.orders:
            self.orders[order_id]["position_id"] = int(position_id)
            return 1
        return 0

    async def fetch_account_position_mode(self, _conn: Any, _account_id: int) -> str:
        return self.mode

    async def fetch_open_position(self, _conn: Any, _account_id: int, position_id: int):
        p = self.positions.get(position_id)
        if p is None or p["state"] != "open":
            return None
        return (p["id"], p["symbol"], 0, p["side"], str(p["qty"]), str(p["avg_price"]))

    async def fetch_open_position_for_symbol(
        self, _conn: Any, account_id: int, symbol: str, side: str
    ) -> dict[str, Any] | None:
        for p in self.positions.values():
            if (
                p["account_id"] == account_id
                and p["symbol"] == symbol
                and p["side"] == side
                and p["state"] == "open"
            ):
                return {"id": p["id"], "qty": p["qty"], "avg_price": p["avg_price"], "side": p["side"]}
        return None

    async def fetch_open_position_for_symbol_non_external(
        self, _conn: Any, account_id: int, symbol: str, side: str
    ) -> dict[str, Any] | None:
        return await self.fetch_open_position_for_symbol(_conn, account_id, symbol, side)

    async def fetch_open_net_position_by_symbol(
        self, _conn: Any, account_id: int, symbol: str
    ) -> dict[str, Any] | None:
        open_positions = [
            p
            for p in self.positions.values()
            if p["account_id"] == account_id and p["symbol"] == symbol and p["state"] == "open"
        ]
        if not open_positions:
            return None
        p = sorted(open_positions, key=lambda x: x["id"], reverse=True)[0]
        return {"id": p["id"], "qty": p["qty"], "avg_price": p["avg_price"], "side": p["side"]}

    async def fetch_open_net_position_by_symbol_non_external(
        self, _conn: Any, account_id: int, symbol: str
    ) -> dict[str, Any] | None:
        return await self.fetch_open_net_position_by_symbol(_conn, account_id, symbol)

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
        _ = conn
        _ = strategy_id
        pid = self.next_position_id
        self.next_position_id += 1
        self.positions[pid] = {
            "id": pid,
            "account_id": account_id,
            "symbol": symbol,
            "side": side,
            "qty": Decimal(str(qty)),
            "avg_price": Decimal(str(avg_price)),
            "stop_loss": None if stop_loss is None else Decimal(str(stop_loss)),
            "stop_gain": None if stop_gain is None else Decimal(str(stop_gain)),
            "comment": comment,
            "state": "open",
            "reason": reason,
        }
        return pid

    async def update_position_open_qty_price(self, _conn: Any, position_id: int, qty: Any, avg_price: Any) -> None:
        self.positions[position_id]["qty"] = Decimal(str(qty))
        self.positions[position_id]["avg_price"] = Decimal(str(avg_price))

    async def close_position(self, _conn: Any, position_id: int) -> None:
        self.positions[position_id]["state"] = "closed"
        self.positions[position_id]["qty"] = Decimal("0")

    async def insert_position_deal(self, conn: Any, **kwargs: Any) -> int:
        _ = conn
        self.deals.append(kwargs)
        return len(self.deals)

    async def insert_event(self, conn: Any, **_kwargs: Any) -> None:
        _ = conn
        return None


def _trade(trade_id: str, side: str, amount: str, price: str, symbol: str = "BTC/USDT") -> dict[str, Any]:
    return {
        "id": trade_id,
        "order": None,
        "symbol": symbol,
        "side": side,
        "amount": Decimal(amount),
        "price": Decimal(price),
        "fee_cost": Decimal("0"),
        "fee_currency": None,
        "timestamp": None,
        "raw": {},
    }


def test_hedge_mode_keeps_buy_and_sell_positions_separate() -> None:
    repo = FakeRepo(mode="hedge")
    asyncio.run(
        _project_trade_to_position(
            repo=repo,
            conn=None,
            account_id=1,
            exchange_trade=_trade("t1", "buy", "1", "100"),
            reason="external",
            reconciled=False,
        )
    )
    asyncio.run(
        _project_trade_to_position(
            repo=repo,
            conn=None,
            account_id=1,
            exchange_trade=_trade("t2", "sell", "0.5", "110"),
            reason="external",
            reconciled=False,
        )
    )
    open_positions = [p for p in repo.positions.values() if p["state"] == "open"]
    assert len(open_positions) == 2
    assert {p["side"] for p in open_positions} == {"buy", "sell"}


def test_netting_mode_external_unmatched_keeps_positions_isolated_per_order() -> None:
    repo = FakeRepo(mode="netting")
    asyncio.run(
        _project_trade_to_position(
            repo=repo,
            conn=None,
            account_id=1,
            exchange_trade=_trade("t1", "buy", "1", "100"),
            reason="external",
            reconciled=False,
        )
    )
    asyncio.run(
        _project_trade_to_position(
            repo=repo,
            conn=None,
            account_id=1,
            exchange_trade=_trade("t2", "sell", "2", "105"),
            reason="external",
            reconciled=False,
        )
    )
    open_positions = [p for p in repo.positions.values() if p["state"] == "open"]
    assert len(open_positions) == 2
    assert {p["side"] for p in open_positions} == {"buy", "sell"}
