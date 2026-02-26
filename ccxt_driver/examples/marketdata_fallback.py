from ccxt_driver import OmsCcxtExchange


def main() -> None:
    exchange = OmsCcxtExchange(
        api_key="YOUR_INTERNAL_API_KEY",
        account_id=1,
        strategy_id=1001,
        base_url="http://127.0.0.1:8000",
    )

    # These methods are dynamically proxied to /ccxt/{account_id}/{func}
    markets = exchange.fetch_markets()
    order_book = exchange.fetch_order_book("BTC/USDT", 20)
    trades = exchange.fetch_trades("BTC/USDT", None, 50)
    ohlcv = exchange.fetch_ohlcv("BTC/USDT", "1m", None, 10)

    print("markets:", len(markets) if isinstance(markets, list) else type(markets).__name__)
    print("order book keys:", list(order_book.keys()) if isinstance(order_book, dict) else order_book)
    print("trades:", len(trades) if isinstance(trades, list) else type(trades).__name__)
    print("ohlcv candles:", len(ohlcv) if isinstance(ohlcv, list) else type(ohlcv).__name__)


if __name__ == "__main__":
    main()
