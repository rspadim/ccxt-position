import asyncio
from decimal import Decimal
from typing import Any

from apps.api.worker_position import _project_trade_to_position


class FakeRepo:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.next_position_id = 1
        self.positions: dict[int, dict[str, Any]] = {}
        self.deals: list[dict[str, Any]] = []

    async def deal_exists_by_exchange_trade_id(self, _conn: Any, _account_id: int, _trade_id: str | None) -> bool:
        return False

    async def fetch_open_order_by_exchange_order_id(self, _conn: Any, _account_id: int, _exchange_order_id: str | None) -> dict[str, Any] | None:
        return None

    async def fetch_account_position_mode(self, _conn: Any, _account_id: int) -> str:
        return self.mode

    async def fetch_open_position(self, _conn: Any, _account_id: int, position_id: int):
        p = self.positions.get(position_id)
        if p is None or p["state"] != "open":
            return None
        return (p["id"], p["symbol"], p["side"], str(p["qty"]), str(p["avg_price"]))

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

    async def create_position_open(
        self,
        conn: Any,
        account_id: int,
        symbol: str,
        side: str,
        qty: Any,
        avg_price: Any,
        reason: str = "api",
    ) -> int:
        _ = conn
        pid = self.next_position_id
        self.next_position_id += 1
        self.positions[pid] = {
            "id": pid,
            "account_id": account_id,
            "symbol": symbol,
            "side": side,
            "qty": Decimal(str(qty)),
            "avg_price": Decimal(str(avg_price)),
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


def test_netting_reversal_closes_old_and_opens_new_position_id() -> None:
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
    first_id = next(iter(repo.positions.keys()))
    assert repo.positions[first_id]["state"] == "open"
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
    assert repo.positions[first_id]["state"] == "closed"
    open_positions = [p for p in repo.positions.values() if p["state"] == "open"]
    assert len(open_positions) == 1
    assert open_positions[0]["id"] != first_id
    assert open_positions[0]["side"] == "sell"
    assert open_positions[0]["qty"] == Decimal("1")
