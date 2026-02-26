from ccxt_driver import OmsCcxtExchange


def main() -> None:
    exchange = OmsCcxtExchange(
        api_key="YOUR_INTERNAL_API_KEY",
        account_id=1,
        strategy_id=1001,
        base_url="http://127.0.0.1:8000",
    )

    ticker = exchange.fetch_ticker("BTC/USDT")
    balance = exchange.fetch_balance()
    positions = exchange.fetch_positions()

    print("ticker:", ticker)
    print("balance keys:", list(balance.keys())[:5] if isinstance(balance, dict) else balance)
    print("open positions:", len(positions))


if __name__ == "__main__":
    main()
