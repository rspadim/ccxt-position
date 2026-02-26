import datetime as dt
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from .http_client import OmsHttpClient


class OmsCcxtExchange:
    def __init__(
        self,
        *,
        api_key: str,
        account_id: int,
        strategy_id: int = 0,
        base_url: str = "http://127.0.0.1:8000",
        timeout_seconds: int = 30,
        http_client: OmsHttpClient | None = None,
    ) -> None:
        self.api_key = str(api_key or "").strip()
        self.account_id = int(account_id)
        self.strategy_id = int(strategy_id)
        self.base_url = base_url.rstrip("/")
        self.http = http_client or OmsHttpClient(
            base_url=self.base_url,
            api_key=self.api_key,
            timeout_seconds=timeout_seconds,
        )
        self.has: dict[str, Any] = self._default_has()
        self._has_loaded = False

    def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: Any,
        price: Any | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        p = dict(params or {})
        payload: dict[str, Any] = {
            "symbol": symbol,
            "side": str(side).lower(),
            "order_type": str(order_type).lower(),
            "qty": self._as_str(amount),
            "strategy_id": int(p.pop("strategy_id", self.strategy_id)),
            "position_id": int(p.pop("position_id", 0) or 0),
        }
        if price is not None:
            payload["price"] = self._as_str(price)
        if "client_order_id" in p:
            payload["client_order_id"] = str(p.pop("client_order_id"))
        if "post_only" in p:
            payload["post_only"] = bool(p.pop("post_only"))
        if p:
            payload["extra"] = p
        out = self._post_oms_command("send_order", payload)
        first = self._first_command_result(out)
        return {"id": str(first.get("order_id")), "info": out}

    def edit_order(
        self,
        order_id: Any,
        symbol: str,
        order_type: str,
        side: str,
        amount: Any | None = None,
        price: Any | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = symbol, order_type, side, params
        payload: dict[str, Any] = {"order_id": int(order_id)}
        if amount is not None:
            payload["new_qty"] = self._as_str(amount)
        if price is not None:
            payload["new_price"] = self._as_str(price)
        out = self._post_oms_command("change_order", payload)
        first = self._first_command_result(out)
        return {"id": str(first.get("order_id") or order_id), "info": out}

    def cancel_order(
        self,
        order_id: Any,
        symbol: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        _ = symbol, params
        out = self._post_oms_command("cancel_order", {"order_id": int(order_id)})
        first = self._first_command_result(out)
        return {"id": str(first.get("order_id") or order_id), "info": out}

    def fetch_order(
        self,
        order_id: Any,
        symbol: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        _ = symbol
        p = dict(params or {})
        oid = int(order_id)
        by_id = self._safe_request("GET", f"/oms/orders/{oid}")
        if isinstance(by_id, dict) and isinstance(by_id.get("items"), list):
            for row in by_id["items"]:
                if int(row.get("id", 0) or 0) == oid:
                    return self._map_order(row)

        for row in self.fetch_open_orders(symbol=None, since=None, limit=None, params=p):
            if int(row.get("id", 0) or 0) == oid:
                return row
        for row in self.fetch_closed_orders(symbol=None, since=None, limit=None, params=p):
            if int(row.get("id", 0) or 0) == oid:
                return row
        return None

    def fetch_open_orders(
        self,
        symbol: str | None = None,
        since: Any | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = symbol, since
        p = dict(params or {})
        q: dict[str, Any] = {"account_ids": str(self.account_id)}
        if limit is not None:
            q["limit"] = int(limit)
        strategy_id = p.pop("strategy_id", self.strategy_id)
        if int(strategy_id or 0) > 0:
            q["strategy_id"] = int(strategy_id)
        out = self.http.request("GET", "/oms/orders/open", query=q)
        return [self._map_order(row) for row in (out.get("items") or [])]

    def fetch_closed_orders(
        self,
        symbol: str | None = None,
        since: Any | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = symbol, since
        p = dict(params or {})
        start_date, end_date = self._window_dates(p)
        q: dict[str, Any] = {
            "account_ids": str(self.account_id),
            "start_date": start_date,
            "end_date": end_date,
            "page": int(p.pop("page", 1) or 1),
            "page_size": int(p.pop("page_size", limit or 100) or 100),
        }
        strategy_id = p.pop("strategy_id", self.strategy_id)
        if int(strategy_id or 0) > 0:
            q["strategy_id"] = int(strategy_id)
        out = self.http.request("GET", "/oms/orders/history", query=q)
        return [self._map_order(row) for row in (out.get("items") or [])]

    def fetch_my_trades(
        self,
        symbol: str | None = None,
        since: Any | None = None,
        limit: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = symbol, since
        p = dict(params or {})
        start_date, end_date = self._window_dates(p)
        q: dict[str, Any] = {
            "account_ids": str(self.account_id),
            "start_date": start_date,
            "end_date": end_date,
            "page": int(p.pop("page", 1) or 1),
            "page_size": int(p.pop("page_size", limit or 100) or 100),
        }
        strategy_id = p.pop("strategy_id", self.strategy_id)
        if int(strategy_id or 0) > 0:
            q["strategy_id"] = int(strategy_id)
        out = self.http.request("GET", "/oms/deals", query=q)
        return [self._map_trade(row) for row in (out.get("items") or [])]

    def fetch_balance(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        p = dict(params or {})
        out = self.http.request(
            "POST",
            f"/ccxt/core/{self.account_id}/fetch_balance",
            payload={"params": p},
        )
        return out.get("result") if isinstance(out, dict) and "result" in out else out

    def fetch_ticker(self, symbol: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        kwargs = dict(params or {})
        out = self.http.request(
            "POST",
            f"/ccxt/{self.account_id}/fetch_ticker",
            payload={"args": [symbol], "kwargs": kwargs},
        )
        return out.get("result") if isinstance(out, dict) and "result" in out else out

    def fetch_positions(
        self,
        symbols: list[str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        _ = symbols
        p = dict(params or {})
        history = bool(p.pop("history", False))
        q: dict[str, Any] = {"account_ids": str(self.account_id)}
        strategy_id = p.pop("strategy_id", self.strategy_id)
        if int(strategy_id or 0) > 0:
            q["strategy_id"] = int(strategy_id)
        if history:
            start_date, end_date = self._window_dates(p)
            q["start_date"] = start_date
            q["end_date"] = end_date
            q["page"] = int(p.pop("page", 1) or 1)
            q["page_size"] = int(p.pop("page_size", 100) or 100)
            out = self.http.request("GET", "/oms/positions/history", query=q)
        else:
            q["limit"] = int(p.pop("limit", 500) or 500)
            out = self.http.request("GET", "/oms/positions/open", query=q)
        return [self._map_position(row) for row in (out.get("items") or [])]

    def call_ccxt(self, func: str, *args: Any, **kwargs: Any) -> Any:
        ccxt_func = self._normalize_ccxt_func_name(func)
        out = self.http.request(
            "POST",
            f"/ccxt/{self.account_id}/{ccxt_func}",
            payload={"args": list(args), "kwargs": dict(kwargs)},
        )
        if isinstance(out, dict) and "result" in out:
            return out.get("result")
        return out

    def load_has(self, refresh: bool = False) -> dict[str, Any]:
        if self._has_loaded and not refresh:
            return dict(self.has)
        base = self._default_has()
        desc = self.call_ccxt("describe")
        remote_has = None
        if isinstance(desc, dict):
            remote_has = desc.get("has")
        if isinstance(remote_has, dict):
            merged = dict(remote_has)
            merged.update(base)
            self.has = merged
        else:
            self.has = base
        self._has_loaded = True
        return dict(self.has)

    def describe(self) -> dict[str, Any]:
        return {
            "id": "oms",
            "name": "OMS First Exchange",
            "countries": [],
            "has": self.load_has(),
        }

    def __getattr__(self, name: str):
        # Generic fallback for methods not explicitly implemented in OMS-first driver.
        # Example: fetch_order_book -> /ccxt/{account_id}/fetch_order_book
        if name.startswith("_"):
            raise AttributeError(name)

        def _proxy(*args: Any, **kwargs: Any) -> Any:
            return self.call_ccxt(name, *args, **kwargs)

        return _proxy

    def _post_oms_command(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self.http.request(
            "POST",
            "/oms/commands",
            payload={
                "account_id": self.account_id,
                "command": command,
                "payload": payload,
            },
        )

    @staticmethod
    def _first_command_result(out: dict[str, Any]) -> dict[str, Any]:
        rows = out.get("results") if isinstance(out, dict) else None
        if not isinstance(rows, list) or not rows:
            raise RuntimeError(f"invalid_command_response: {out}")
        first = rows[0] if isinstance(rows[0], dict) else {}
        if not first.get("ok"):
            err = first.get("error") if isinstance(first.get("error"), dict) else {"message": "command_failed"}
            msg = err.get("message") or err.get("code") or "command_failed"
            raise RuntimeError(str(msg))
        return first

    def _safe_request(self, method: str, path: str) -> dict[str, Any] | None:
        try:
            return self.http.request(method, path)
        except Exception:
            return None

    @staticmethod
    def _window_dates(params: dict[str, Any]) -> tuple[str, str]:
        start = str(params.pop("start_date", "") or "").strip()
        end = str(params.pop("end_date", "") or "").strip()
        if start and end:
            return start, end
        today = dt.datetime.now(dt.UTC).date()
        return (today - dt.timedelta(days=1)).isoformat(), today.isoformat()

    @staticmethod
    def _as_str(value: Any) -> str:
        try:
            return format(Decimal(str(value)), "f")
        except (InvalidOperation, ValueError, TypeError):
            return str(value)

    @staticmethod
    def _normalize_ccxt_func_name(name: str) -> str:
        raw = str(name or "").strip()
        if not raw:
            return raw
        # Accept camelCase method names and normalize to snake_case endpoint style.
        if "_" in raw:
            return raw.lower()
        step1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", raw)
        step2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", step1)
        return step2.lower()

    @staticmethod
    def _default_has() -> dict[str, Any]:
        return {
            "createOrder": True,
            "editOrder": True,
            "cancelOrder": True,
            "fetchOrder": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchMyTrades": True,
            "fetchBalance": True,
            "fetchTicker": True,
            "fetchPositions": True,
        }

    @staticmethod
    def _parse_ts(value: Any) -> int | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            dt_obj = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=dt.UTC)
            return int(dt_obj.timestamp() * 1000)
        except ValueError:
            return None

    def _map_order(self, row: dict[str, Any]) -> dict[str, Any]:
        amount = Decimal(str(row.get("qty", "0") or "0"))
        filled = Decimal(str(row.get("filled_qty", "0") or "0"))
        remaining = amount - filled
        if remaining < Decimal("0"):
            remaining = Decimal("0")
        order_type = str(row.get("order_type", "") or "").lower() or None
        side = str(row.get("side", "") or "").lower() or None
        status_oms = str(row.get("status", "") or "").upper()
        status = self._map_order_status(status_oms)
        return {
            "id": str(row.get("id")),
            "clientOrderId": row.get("client_order_id"),
            "symbol": row.get("symbol"),
            "type": order_type,
            "side": side,
            "price": self._to_float(row.get("price")),
            "amount": float(amount),
            "filled": float(filled),
            "remaining": float(remaining),
            "status": status,
            "timestamp": self._parse_ts(row.get("created_at") or row.get("updated_at")),
            "info": row,
        }

    @staticmethod
    def _map_order_status(value: str) -> str:
        if value in {"FILLED"}:
            return "closed"
        if value in {"CANCELED"}:
            return "canceled"
        if value in {"REJECTED"}:
            return "rejected"
        return "open"

    def _map_trade(self, row: dict[str, Any]) -> dict[str, Any]:
        qty = Decimal(str(row.get("qty", "0") or "0"))
        return {
            "id": str(row.get("id")),
            "order": str(row.get("order_id")) if row.get("order_id") is not None else None,
            "symbol": row.get("symbol"),
            "side": str(row.get("side", "") or "").lower() or None,
            "price": self._to_float(row.get("price")),
            "amount": float(qty),
            "cost": self._to_float(row.get("price")) * float(qty) if self._to_float(row.get("price")) is not None else None,
            "fee": self._map_fee(row),
            "timestamp": self._parse_ts(row.get("executed_at") or row.get("created_at")),
            "info": row,
        }

    @staticmethod
    def _map_fee(row: dict[str, Any]) -> dict[str, Any] | None:
        fee = row.get("fee")
        if fee is None:
            return None
        return {
            "cost": float(Decimal(str(fee or "0"))),
            "currency": row.get("fee_currency"),
        }

    def _map_position(self, row: dict[str, Any]) -> dict[str, Any]:
        qty = Decimal(str(row.get("qty", "0") or "0"))
        return {
            "id": str(row.get("id")),
            "symbol": row.get("symbol"),
            "side": str(row.get("side", "") or "").lower() or None,
            "contracts": float(qty),
            "entryPrice": self._to_float(row.get("avg_price")),
            "timestamp": self._parse_ts(row.get("opened_at") or row.get("created_at")),
            "info": row,
        }

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(Decimal(str(value)))
        except (InvalidOperation, ValueError, TypeError):
            return None
