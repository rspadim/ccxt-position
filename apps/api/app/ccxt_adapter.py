from typing import Any

import ccxt.async_support as ccxt_async


def _as_plain_secret(value: str | None) -> str | None:
    if value is None:
        return None
    # v0 compatibility: treat stored value as already usable secret text.
    return str(value)


class CCXTAdapter:
    def __init__(self, logger: Any | None = None) -> None:
        self.logger = logger

    def _build_exchange(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None = None,
    ) -> Any:
        exchange_cls = getattr(ccxt_async, exchange_id, None)
        if exchange_cls is None:
            raise RuntimeError(f"unsupported exchange_id: {exchange_id}")
        config: dict[str, Any] = {}
        if isinstance(extra_config, dict):
            config.update(extra_config)
        config["apiKey"] = _as_plain_secret(api_key)
        config["secret"] = _as_plain_secret(secret)
        config["password"] = _as_plain_secret(passphrase)
        if "enableRateLimit" not in config:
            config["enableRateLimit"] = True
        exchange = exchange_cls(
            config
        )
        if use_testnet:
            setter = getattr(exchange, "set_sandbox_mode", None)
            if callable(setter):
                setter(True)
        return exchange

    async def execute_method(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        method: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        logger: Any | None = None,
    ) -> Any:
        exchange = self._build_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
        )
        try:
            fn = getattr(exchange, method, None)
            if fn is None:
                raise RuntimeError(f"unsupported ccxt method: {method}")
            active_logger = logger or self.logger
            if active_logger is not None:
                active_logger.info(
                    "ccxt_call %s",
                    {"exchange_id": exchange_id, "method": method},
                )
            return await fn(*(args or []), **(kwargs or {}))
        finally:
            await exchange.close()

    @staticmethod
    def _supports_capability(has_map: Any, capability: str) -> bool:
        if not isinstance(has_map, dict):
            return False
        value = has_map.get(capability)
        return value is True or value == "emulated"

    async def execute_unified_with_capability(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        method: str,
        capabilities: list[str],
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        exchange = self._build_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
        )
        try:
            if capabilities and not any(
                self._supports_capability(exchange.has, capability) for capability in capabilities
            ):
                raise RuntimeError(
                    f"exchange {exchange_id} does not support required capability for {method}: {capabilities}"
                )
            fn = getattr(exchange, method, None)
            if fn is None:
                raise RuntimeError(f"unsupported ccxt method: {method}")
            return await fn(*(args or []), **(kwargs or {}))
        finally:
            await exchange.close()

    async def create_order(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        exchange = self._build_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
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
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        exchange_order_id: str,
        symbol: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        exchange = self._build_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
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
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        exchange_order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        amount: Any,
        price: Any,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        exchange = self._build_exchange(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
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

    async def fetch_my_trades(
        self,
        exchange_id: str,
        use_testnet: bool,
        api_key: str | None,
        secret: str | None,
        passphrase: str | None,
        extra_config: dict[str, Any] | None,
        symbol: str | None = None,
        since: int | None = None,
        limit: int | None = 200,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        out = await self.execute_method(
            exchange_id=exchange_id,
            use_testnet=use_testnet,
            api_key=api_key,
            secret=secret,
            passphrase=passphrase,
            extra_config=extra_config,
            method="fetch_my_trades",
            args=[symbol, since, limit, params or {}],
        )
        if isinstance(out, list):
            return out
        return []
