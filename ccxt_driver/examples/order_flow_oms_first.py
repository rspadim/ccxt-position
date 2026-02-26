from decimal import Decimal

from ccxt_driver import OmsCcxtExchange


def main() -> None:
    exchange = OmsCcxtExchange(
        api_key="YOUR_INTERNAL_API_KEY",
        account_id=1,
        strategy_id=1001,
        base_url="http://127.0.0.1:8000",
    )

    symbol = "BTC/USDT"
    ticker = exchange.fetch_ticker(symbol)
    last = Decimal(str(ticker.get("last") or ticker.get("close") or "0"))
    if last <= Decimal("0"):
        raise RuntimeError(f"invalid ticker: {ticker}")

    price = (last * Decimal("0.97")).quantize(Decimal("0.01"))

    created = exchange.create_order(
        symbol=symbol,
        order_type="limit",
        side="buy",
        amount="0.001",
        price=str(price),
    )
    order_id = int(created["id"])
    print("created:", order_id)

    edited = exchange.edit_order(
        order_id=order_id,
        symbol=symbol,
        order_type="limit",
        side="buy",
        amount="0.001",
        price=str((last * Decimal("0.96")).quantize(Decimal("0.01"))),
    )
    print("edited:", edited["id"])

    canceled = exchange.cancel_order(order_id, symbol=symbol)
    print("canceled:", canceled["id"])

    fetched = exchange.fetch_order(order_id)
    print("fetch_order:", fetched["status"] if fetched else None)


if __name__ == "__main__":
    main()
