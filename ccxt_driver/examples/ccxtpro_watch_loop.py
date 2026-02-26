import asyncio
from typing import Any

from ccxt_driver import OmsCcxtProExchange


def _brief(rows: list[dict[str, Any]], key: str = "id") -> str:
    if not rows:
        return "[]"
    preview = [str(r.get(key, "")) for r in rows[:5]]
    return "[" + ", ".join(preview) + (", ..." if len(rows) > 5 else "") + "]"


async def main() -> None:
    ex = OmsCcxtProExchange(
        api_key="YOUR_INTERNAL_API_KEY",
        account_id=1,
        strategy_id=1001,
        base_url="http://127.0.0.1:8000",
        watch_timeout_seconds=15,
        poll_interval_seconds=0.8,
    )

    symbol = "BTC/USDT"
    print("starting watch loop (Ctrl+C to stop)")
    while True:
        try:
            orders = await ex.watch_orders(symbol=symbol)
            if orders:
                print(f"orders event count={len(orders)} ids={_brief(orders)}")

            trades = await ex.watch_my_trades(symbol=symbol)
            if trades:
                print(f"trades event count={len(trades)} ids={_brief(trades)}")

            positions = await ex.watch_positions(symbols=[symbol])
            if positions:
                print(f"positions event count={len(positions)} ids={_brief(positions)}")

            await asyncio.sleep(0.2)
        except KeyboardInterrupt:
            break
        except Exception as exc:
            print(f"watch loop error: {exc}; retrying in 2s")
            await asyncio.sleep(2.0)

    await ex.close()


if __name__ == "__main__":
    asyncio.run(main())
