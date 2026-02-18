from typing import Any

import ccxt.async_support as ccxt_async


def _as_plain_secret(value: str | None) -> str | None:
    if value is None:
        return None
    # v0 compatibility: treat stored value as already usable secret text.
    return str(value)


class CCXTAdapter:
    async def create_order(
        self,
        exchange_id: str,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        exchange_cls = getattr(ccxt_async, exchange_id, None)
        if exchange_cls is None:
            raise RuntimeError(f"unsupported exchange_id: {exchange_id}")

        exchange = exchange_cls(
            {
                "apiKey": _as_plain_secret(api_key),
                "secret": _as_plain_secret(secret),
                "password": _as_plain_secret(passphrase),
                "enableRateLimit": True,
            }
        )
        try:
            return await exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params,
            )
        finally:
            await exchange.close()

    async def cancel_order(
        self,
        exchange_id: str,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        exchange_order_id: str,
        symbol: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        exchange_cls = getattr(ccxt_async, exchange_id, None)
        if exchange_cls is None:
            raise RuntimeError(f"unsupported exchange_id: {exchange_id}")
        exchange = exchange_cls(
            {
                "apiKey": _as_plain_secret(api_key),
                "secret": _as_plain_secret(secret),
                "password": _as_plain_secret(passphrase),
                "enableRateLimit": True,
            }
        )
        try:
            return await exchange.cancel_order(
                id=exchange_order_id,
                symbol=symbol,
                params=params or {},
            )
        finally:
            await exchange.close()

    async def edit_or_replace_order(
        self,
        exchange_id: str,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        exchange_order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        exchange_cls = getattr(ccxt_async, exchange_id, None)
        if exchange_cls is None:
            raise RuntimeError(f"unsupported exchange_id: {exchange_id}")
        exchange = exchange_cls(
            {
                "apiKey": _as_plain_secret(api_key),
                "secret": _as_plain_secret(secret),
                "password": _as_plain_secret(passphrase),
                "enableRateLimit": True,
            }
        )
        try:
            await exchange.load_markets()
            can_edit = bool(exchange.has.get("editOrder")) if isinstance(exchange.has, dict) else False
            if can_edit:
                return await exchange.edit_order(
                    id=exchange_order_id,
                    symbol=symbol,
                    type=order_type,
                    side=side,
                    amount=amount,
                    price=price,
                    params=params,
                )

            await exchange.cancel_order(id=exchange_order_id, symbol=symbol, params={})
            return await exchange.create_order(
                symbol=symbol,
                type=order_type,
                side=side,
                amount=amount,
                price=price,
                params=params,
            )
        finally:
            await exchange.close()

