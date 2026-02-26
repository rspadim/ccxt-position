import asyncio
import json
import time
from typing import Any

from .oms_ccxt_exchange import OmsCcxtExchange

try:
    import websockets  # type: ignore
except Exception:
    websockets = None


class OmsCcxtProExchange(OmsCcxtExchange):
    """
    CCXTPRO-like async interface for OMS-first exchange.

    Current implementation uses async polling over REST endpoints.
    It keeps the same method signatures expected by ccxtpro-style usage,
    so we can switch internals to real WebSocket streaming later without
    breaking caller code.
    """

    def __init__(
        self,
        *,
        api_key: str,
        account_id: int,
        strategy_id: int = 0,
        base_url: str = "http://127.0.0.1:8000",
        timeout_seconds: int = 30,
        poll_interval_seconds: float = 1.0,
        watch_timeout_seconds: float = 30.0,
    ) -> None:
        super().__init__(
            api_key=api_key,
            account_id=account_id,
            strategy_id=strategy_id,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.watch_timeout_seconds = float(watch_timeout_seconds)
        self._seen_order_ids: set[str] = set()
        self._seen_trade_ids: set[str] = set()
        self._last_positions_fingerprint: str | None = None
        self.has.update(
            {
                "watchOrders": True,
                "watchMyTrades": True,
                "watchPositions": True,
                "ws": True,
            }
        )

    def load_has(self, refresh: bool = False) -> dict[str, Any]:
        out = super().load_has(refresh=refresh)
        out.update(
            {
                "watchOrders": True,
                "watchMyTrades": True,
                "watchPositions": True,
                "ws": True,
            }
        )
        self.has = dict(out)
        return dict(self.has)

    def _ws_url(self) -> str:
        if self.base_url.startswith("https://"):
            return "wss://" + self.base_url[len("https://") :] + "/ws"
        if self.base_url.startswith("http://"):
            return "ws://" + self.base_url[len("http://") :] + "/ws"
        return self.base_url.rstrip("/") + "/ws"

    async def _watch_via_ws(
        self,
        kind: str,
        *,
        symbol: str | None = None,
        timeout_s: float,
    ) -> list[dict[str, Any]] | None:
        if websockets is None:
            return None
        ws_url = self._ws_url()
        deadline = time.monotonic() + max(0.5, float(timeout_s))
        conn_timeout = min(10.0, max(2.0, float(timeout_s) / 2.0))
        try:
            async with websockets.connect(
                ws_url,
                additional_headers={"x-api-key": self.api_key},
                open_timeout=conn_timeout,
            ) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "id": "auth-1",
                            "action": "auth",
                            "payload": {"api_key": self.api_key},
                        }
                    )
                )
                await ws.send(
                    json.dumps(
                        {
                            "id": "sub-1",
                            "action": "subscribe",
                            "payload": {
                                "namespaces": ["position"],
                                "account_ids": [int(self.account_id)],
                                "with_snapshot": True,
                            },
                        }
                    )
                )
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return []
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    msg = json.loads(raw)
                    rows = self._extract_ws_rows(kind=kind, msg=msg, symbol=symbol)
                    if rows:
                        return rows
        except Exception:
            return None

    async def call_ccxt_async(self, func: str, *args: Any, **kwargs: Any) -> Any:
        # Run sync REST fallback in thread so caller keeps async ccxtpro-style usage.
        return await asyncio.to_thread(self.call_ccxt, func, *args, **kwargs)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        async def _async_proxy(*args: Any, **kwargs: Any) -> Any:
            return await self.call_ccxt_async(name, *args, **kwargs)

        return _async_proxy

    async def watch_orders(
        self,
        symbol: str | None = None,
        since: Any | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = since
        timeout_s = float((params or {}).get("watch_timeout_seconds", self.watch_timeout_seconds))
        interval_s = float((params or {}).get("poll_interval_seconds", self.poll_interval_seconds))
        force_polling = bool((params or {}).get("force_polling", False))
        if not force_polling:
            ws_rows = await self._watch_via_ws("orders", symbol=symbol, timeout_s=timeout_s)
            if ws_rows is not None:
                return ws_rows
        start = time.monotonic()
        while True:
            open_rows = self.fetch_open_orders(symbol=symbol, since=None, limit=limit, params=params)
            closed_rows = self.fetch_closed_orders(symbol=symbol, since=None, limit=limit, params=params)
            all_rows = open_rows + closed_rows
            fresh: list[dict[str, Any]] = []
            for row in all_rows:
                oid = str(row.get("id", "")).strip()
                if not oid:
                    continue
                if oid in self._seen_order_ids:
                    continue
                self._seen_order_ids.add(oid)
                fresh.append(row)
            if fresh:
                return fresh
            if time.monotonic() - start >= timeout_s:
                return []
            await asyncio.sleep(max(0.05, interval_s))

    async def watch_my_trades(
        self,
        symbol: str | None = None,
        since: Any | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = since
        timeout_s = float((params or {}).get("watch_timeout_seconds", self.watch_timeout_seconds))
        interval_s = float((params or {}).get("poll_interval_seconds", self.poll_interval_seconds))
        force_polling = bool((params or {}).get("force_polling", False))
        if not force_polling:
            ws_rows = await self._watch_via_ws("trades", symbol=symbol, timeout_s=timeout_s)
            if ws_rows is not None:
                return ws_rows
        start = time.monotonic()
        while True:
            rows = self.fetch_my_trades(symbol=symbol, since=None, limit=limit, params=params)
            fresh: list[dict[str, Any]] = []
            for row in rows:
                tid = str(row.get("id", "")).strip()
                if not tid:
                    continue
                if tid in self._seen_trade_ids:
                    continue
                self._seen_trade_ids.add(tid)
                fresh.append(row)
            if fresh:
                return fresh
            if time.monotonic() - start >= timeout_s:
                return []
            await asyncio.sleep(max(0.05, interval_s))

    async def watch_positions(
        self,
        symbols: list[str] | None = None,
        since: Any | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = since, limit, symbols
        timeout_s = float((params or {}).get("watch_timeout_seconds", self.watch_timeout_seconds))
        interval_s = float((params or {}).get("poll_interval_seconds", self.poll_interval_seconds))
        force_polling = bool((params or {}).get("force_polling", False))
        if not force_polling:
            first_symbol = (symbols or [None])[0]
            ws_rows = await self._watch_via_ws("positions", symbol=first_symbol, timeout_s=timeout_s)
            if ws_rows is not None:
                return ws_rows
        start = time.monotonic()
        while True:
            rows = self.fetch_positions(symbols=symbols, params=params)
            normalized = sorted(
                [
                    (
                        str(r.get("id", "")),
                        str(r.get("symbol", "")),
                        str(r.get("side", "")),
                        str(r.get("contracts", "")),
                    )
                    for r in rows
                ]
            )
            fingerprint = "|".join([",".join(item) for item in normalized])
            if self._last_positions_fingerprint is None:
                self._last_positions_fingerprint = fingerprint
                return rows
            if fingerprint != self._last_positions_fingerprint:
                self._last_positions_fingerprint = fingerprint
                return rows
            if time.monotonic() - start >= timeout_s:
                return []
            await asyncio.sleep(max(0.05, interval_s))

    async def close(self) -> None:
        # Polling implementation has no long-lived sockets yet.
        return None

    def _extract_ws_rows(
        self,
        *,
        kind: str,
        msg: dict[str, Any],
        symbol: str | None = None,
    ) -> list[dict[str, Any]]:
        namespace = str(msg.get("namespace", "") or "")
        event = str(msg.get("event", "") or "")
        payload = msg.get("payload") if isinstance(msg.get("payload"), dict) else {}
        if namespace != "position":
            return []

        rows: list[dict[str, Any]] = []
        if kind == "orders":
            if event == "snapshot_open_orders":
                items = payload.get("items") if isinstance(payload.get("items"), list) else []
                rows = [self._map_order(item) for item in items if isinstance(item, dict)]
            elif event in {"order_updated", "order_deleted"} and payload:
                rows = [self._map_order(payload)]
        elif kind == "trades":
            if event in {"deal_updated", "deal_deleted"} and payload:
                rows = [self._map_trade(payload)]
        elif kind == "positions":
            if event == "snapshot_open_positions":
                items = payload.get("items") if isinstance(payload.get("items"), list) else []
                rows = [self._map_position(item) for item in items if isinstance(item, dict)]
            elif event in {"position_updated", "position_deleted"} and payload:
                rows = [self._map_position(payload)]

        if symbol:
            wanted = str(symbol).upper()
            rows = [r for r in rows if str(r.get("symbol", "")).upper() == wanted]
        return rows
