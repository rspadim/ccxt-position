import asyncio
from types import MethodType

from apps.api.dispatcher_server import Dispatcher, _Job


class _FakeConn:
    async def commit(self) -> None:
        return None


class _FakeConnCtx:
    async def __aenter__(self) -> _FakeConn:
        return _FakeConn()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeDb:
    def connection(self) -> _FakeConnCtx:
        return _FakeConnCtx()


class _FakeRepo:
    def __init__(self) -> None:
        self.hints: dict[int, int] = {}

    async def fetch_account_by_id(self, _conn, account_id: int):
        if account_id == 1:
            return {"id": 1, "status": "active", "exchange_id": "ccxt.binance"}
        if account_id == 2:
            return {"id": 2, "status": "active", "exchange_id": "ccxtpro.binance"}
        if account_id == 3:
            return {"id": 3, "status": "active", "exchange_id": "binance.spot"}
        return None

    async def fetch_account_dispatcher_worker_hint(self, _conn, account_id: int):
        return self.hints.get(int(account_id))

    async def set_account_dispatcher_worker_hint(self, _conn, account_id: int, worker_hint: int) -> None:
        self.hints[int(account_id)] = int(worker_hint)


def test_exchange_engine_id_legacy_bare_id_defaults_to_ccxt() -> None:
    assert Dispatcher._exchange_engine_id("binance") == "ccxt.binance"


def test_exchange_engine_id_rejects_non_prefixed_dot_id() -> None:
    try:
        Dispatcher._exchange_engine_id("binance.spot")
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert str(exc) == "unsupported_engine"


def test_engine_of_exchange_id_accepts_ccxt_and_ccxtpro() -> None:
    assert Dispatcher._engine_of_exchange_id("ccxt.binance") == "ccxt"
    assert Dispatcher._engine_of_exchange_id("ccxtpro.binance") == "ccxtpro"


def test_resolve_worker_is_scoped_per_engine() -> None:
    d = Dispatcher()
    d.db = _FakeDb()  # type: ignore[assignment]
    d.repo = _FakeRepo()  # type: ignore[assignment]

    async def _run() -> None:
        engine_1, wid_1 = await d._resolve_worker_for_account(1)
        engine_2, wid_2 = await d._resolve_worker_for_account(2)
        assert engine_1 == "ccxt"
        assert engine_2 == "ccxtpro"
        assert 0 <= wid_1 < d.pool_size_by_engine["ccxt"]
        assert 0 <= wid_2 < d.pool_size_by_engine["ccxtpro"]
        assert ("ccxt", 1) in d.account_worker
        assert ("ccxtpro", 2) in d.account_worker

    asyncio.run(_run())


def test_dispatch_to_account_returns_unsupported_engine_for_invalid_prefix() -> None:
    d = Dispatcher()
    d.db = _FakeDb()  # type: ignore[assignment]
    d.repo = _FakeRepo()  # type: ignore[assignment]

    async def _run() -> None:
        out = await d._dispatch_to_account(3, {"op": "status"})
        assert out["ok"] is False
        assert (out.get("error") or {}).get("code") == "unsupported_engine"

    asyncio.run(_run())


def test_worker_locks_are_account_scoped() -> None:
    d = Dispatcher()

    async def _execute_stub(self, msg):
        _ = msg
        await asyncio.sleep(0.01)
        return {"ok": True}

    d._execute = MethodType(_execute_stub, d)

    async def _run() -> None:
        q_ccxt: asyncio.Queue[_Job] = asyncio.Queue()
        q_pro: asyncio.Queue[_Job] = asyncio.Queue()
        t_ccxt = asyncio.create_task(d._worker_loop("ccxt", 0, q_ccxt))
        t_pro = asyncio.create_task(d._worker_loop("ccxtpro", 0, q_pro))

        f1 = asyncio.get_running_loop().create_future()
        f2 = asyncio.get_running_loop().create_future()
        await q_ccxt.put(_Job(account_id=7, payload={"op": "x"}, future=f1))
        await q_pro.put(_Job(account_id=7, payload={"op": "y"}, future=f2))
        await asyncio.wait_for(f1, timeout=1)
        await asyncio.wait_for(f2, timeout=1)

        assert 7 in d.account_locks
        assert len(d.account_locks) == 1

        t_ccxt.cancel()
        t_pro.cancel()
        await asyncio.gather(t_ccxt, t_pro, return_exceptions=True)

    asyncio.run(_run())
