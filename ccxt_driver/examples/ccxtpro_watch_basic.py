import asyncio

from ccxt_driver import OmsCcxtProExchange


async def main() -> None:
    ex = OmsCcxtProExchange(
        api_key="YOUR_INTERNAL_API_KEY",
        account_id=1,
        strategy_id=1001,
        base_url="http://127.0.0.1:8000",
        watch_timeout_seconds=20,
        poll_interval_seconds=1.0,
    )

    symbol = "BTC/USDT"
    orders = await ex.watch_orders(symbol=symbol)
    trades = await ex.watch_my_trades(symbol=symbol)
    positions = await ex.watch_positions(symbols=[symbol])

    print("orders:", len(orders))
    print("trades:", len(trades))
    print("positions:", len(positions))


if __name__ == "__main__":
    asyncio.run(main())
