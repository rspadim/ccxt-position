import json
import argparse
import datetime as dt
import os
import subprocess
import sys
import time
import uuid
from decimal import Decimal
from pathlib import Path
from urllib import request as urllib_request
from urllib import error as urllib_error


class Logger:
    def __init__(self, runtime_dir: Path, verbose: bool) -> None:
        self.verbose = verbose
        self.log_path = runtime_dir / "scenarios.log"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")

    def _line(self, level: str, message: str) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] [{level}] {message}"

    def info(self, message: str) -> None:
        line = self._line("INFO", message)
        if self.verbose:
            print(line)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def warn(self, message: str) -> None:
        line = self._line("WARN", message)
        print(line, file=sys.stderr)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def run_cmd(args: list[str]) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(args)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc.stdout.strip()


def run_json_cmd(args: list[str]) -> dict:
    return json.loads(run_cmd(args))


def http_json(method: str, url: str, headers: dict[str, str], payload: dict | None = None) -> dict:
    body = None
    req_headers = dict(headers)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib_request.Request(url=url, data=body, headers=req_headers, method=method)
    try:
        with urllib_request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"HTTP {exc.code} {exc.reason} on {method} {url}\n"
            f"payload={json.dumps(payload or {}, ensure_ascii=True)}\n"
            f"response={raw}"
        ) from exc


def wait_until(fn, timeout_s: int = 120, sleep_s: float = 2.0):
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        last = fn()
        if last:
            return last
        time.sleep(sleep_s)
    return last


def build_client_order_id(prefix: str = "tn") -> str:
    millis = int(time.time() * 1000)
    token = uuid.uuid4().hex[:8]
    return f"{prefix}{millis}{token}"[:36]


def build_strategy_name(symbol: str, role: str, account_id: int) -> str:
    safe_symbol = symbol.replace("/", "-").lower()
    token = uuid.uuid4().hex[:6]
    return f"tn-{safe_symbol}-{role}-a{account_id}-{token}"[:128]


def history_window_qs() -> str:
    today = dt.datetime.now(dt.UTC).date()
    start = today - dt.timedelta(days=1)
    return f"start_date={start.isoformat()}&end_date={today.isoformat()}"


def create_strategy(
    base_url: str,
    headers: dict[str, str],
    account_id: int,
    name: str,
    client_strategy_id: int | None,
    logger: Logger,
) -> int:
    payload: dict[str, object] = {"name": name, "account_ids": [account_id]}
    if client_strategy_id is not None:
        payload["client_strategy_id"] = int(client_strategy_id)
    logger.info(
        f"creating strategy account_id={account_id} name={name} client_strategy_id={client_strategy_id}"
    )
    out = http_json("POST", f"{base_url}/strategies", headers, payload)
    strategy_id = int(out.get("strategy_id", 0) or 0)
    if strategy_id <= 0:
        raise RuntimeError(f"strategy creation failed account_id={account_id}: {json.dumps(out)}")
    logger.info(f"strategy created account_id={account_id} strategy_id={strategy_id}")
    return strategy_id


def create_account_for_mode(
    compose: list[str],
    user_id: int,
    mode: str,
    label: str,
    api_key: str,
    secret_key: str,
    logger: Logger,
    exchange_id: str = "ccxt.binance",
) -> int:
    logger.info(f"creating account mode={mode} exchange_id={exchange_id} label={label}")
    created = run_json_cmd(
        compose
        + [
            "exec",
            "-T",
            "api",
            "python",
            "-m",
            "apps.api.cli",
            "add-account",
            "--user-id",
            str(user_id),
            "--exchange-id",
            str(exchange_id),
            "--label",
            label,
            "--position-mode",
            mode,
            "--testnet",
        ]
    )
    account_id = int(created["account_id"])
    logger.info(f"account created id={account_id}; storing credentials")
    run_cmd(
        compose
        + [
            "exec",
            "-T",
            "api",
            "python",
            "-m",
            "apps.api.cli",
            "upsert-account-credentials",
            "--account-id",
            str(account_id),
            "--api-key",
            api_key,
            "--secret",
            secret_key,
            "--encrypt-input",
        ]
    )
    logger.info(f"credentials stored for account_id={account_id}")
    return account_id


def send_market(
    base_url: str,
    headers: dict[str, str],
    account_id: int,
    symbol: str,
    side: str,
    qty: str,
    strategy_id: int,
    logger: Logger,
) -> int:
    logger.info(
        f"sending market order account_id={account_id} symbol={symbol} side={side} qty={qty} strategy_id={strategy_id}"
    )
    out = http_json(
        "POST",
        f"{base_url}/oms/commands",
        headers,
        {
            "account_id": account_id,
            "command": "send_order",
            "payload": {
                "symbol": symbol,
                "side": side,
                "order_type": "market",
                "qty": qty,
                "strategy_id": strategy_id,
                "position_id": 0,
                "client_order_id": build_client_order_id(f"tn{account_id}"),
            },
        },
    )
    first = out["results"][0]
    if not first.get("ok"):
        raise RuntimeError(f"send_market failed: {json.dumps(first)}")
    logger.info(
        f"order accepted account_id={account_id} order_id={first['order_id']} command_id={first['command_id']}"
    )
    return int(first["order_id"])


def wait_order_terminal(base_url: str, headers: dict[str, str], account_id: int, order_id: int) -> dict | None:
    def _check():
        qs = history_window_qs()
        rows = http_json("GET", f"{base_url}/oms/orders/history?account_ids={account_id}&{qs}", headers).get(
            "items", []
        )
        for row in rows:
            if int(row["id"]) == order_id:
                if row["status"] in {"FILLED", "CANCELED", "REJECTED"}:
                    return row
        return None

    return wait_until(_check, timeout_s=180, sleep_s=2.0)


def wait_deal_for_order(
    base_url: str,
    headers: dict[str, str],
    account_id: int,
    order_id: int,
    strategy_id: int | None = None,
    timeout_s: int = 180,
    sleep_s: float = 2.0,
) -> dict | None:
    def _check():
        qs = history_window_qs()
        strategy_qs = f"&strategy_id={int(strategy_id)}" if strategy_id is not None else ""
        rows = http_json(
            "GET",
            f"{base_url}/oms/deals?account_ids={account_id}&{qs}&page=1&page_size=5000{strategy_qs}",
            headers,
        ).get("items", [])
        for row in rows:
            if row.get("order_id") is not None and int(row["order_id"]) == order_id:
                return row
            if strategy_id is not None and int(row.get("strategy_id", 0) or 0) == int(strategy_id):
                return row
        return None

    return wait_until(_check, timeout_s=timeout_s, sleep_s=sleep_s)


def force_reconcile_now(
    base_url: str,
    headers: dict[str, str],
    account_id: int,
    logger: Logger,
    symbols_hint: list[str] | None = None,
) -> dict:
    logger.info(f"forcing reconciliation for account_id={account_id}")
    payload: dict[str, object] = {"account_id": account_id, "scope": "long"}
    if symbols_hint:
        payload["symbols_hint"] = symbols_hint
    out = http_json(
        "POST",
        f"{base_url}/oms/reconcile",
        headers,
        payload,
    )
    triggered = int(out.get("triggered_count", 0) or 0)
    logger.info(f"reconcile response account_id={account_id} triggered_count={triggered}")
    if triggered <= 0:
        raise RuntimeError(f"reconcile_not_triggered account_id={account_id}: {json.dumps(out)}")
    return out


def fetch_reconcile_status(
    base_url: str,
    headers: dict[str, str],
    account_id: int,
    stale_after_seconds: int = 120,
) -> dict:
    out = http_json(
        "GET",
        f"{base_url}/oms/reconcile/{account_id}/status?stale_after_seconds={int(stale_after_seconds)}",
        headers,
    )
    items = out.get("items", [])
    if isinstance(items, list) and items:
        return items[0]
    return {}


def choose_buy_qty(
    base_url: str, headers: dict[str, str], account_id: int, symbol: str, logger: Logger | None = None
) -> str:
    ticker = http_json(
        "POST",
        f"{base_url}/ccxt/{account_id}/fetch_ticker",
        headers,
        {"args": [symbol], "kwargs": {}},
    )
    last = Decimal(str(ticker["result"]["last"]))
    quote = symbol.split("/")[-1]
    min_quote = Decimal("12")

    fallback_quote_to_use = Decimal(os.getenv("TESTNET_SCENARIO_FALLBACK_QUOTE", "15"))
    quote_to_use = max(min_quote, fallback_quote_to_use)
    free = Decimal("0")
    try:
        balance = http_json(
            "POST",
            f"{base_url}/ccxt/core/{account_id}/fetch_balance",
            headers,
            {"params": {}},
        )
        free = Decimal(str(balance["result"][quote]["free"]))
        if free <= min_quote:
            raise RuntimeError(f"insufficient {quote} free balance for scenarios: {free}")
        quote_to_use = min(free * Decimal("0.15"), Decimal("30"))
        if quote_to_use < min_quote:
            quote_to_use = min_quote
    except (urllib_error.HTTPError, urllib_error.URLError, KeyError, ValueError, TypeError) as exc:
        if logger:
            logger.warn(
                f"fetch_balance unavailable for account_id={account_id}; using fallback quote={quote_to_use} {quote}: {exc}"
            )

    qty = (quote_to_use / last).quantize(Decimal("0.000001"))
    if qty <= 0:
        raise RuntimeError(f"calculated qty invalid: {qty}")
    return format(qty, "f")


def wait_min_deals(base_url: str, headers: dict[str, str], account_id: int, min_count: int) -> list[dict] | None:
    def _check():
        qs = history_window_qs()
        rows = http_json(
            "GET",
            f"{base_url}/oms/deals?account_ids={account_id}&{qs}&page=1&page_size=5000",
            headers,
        ).get("items", [])
        return rows if len(rows) >= min_count else None

    return wait_until(_check, timeout_s=240, sleep_s=3.0)


def collect_exchange_trade_ids(deals: list[dict]) -> set[str]:
    out: set[str] = set()
    for d in deals:
        trade_id = d.get("exchange_trade_id")
        if trade_id:
            out.add(str(trade_id))
    return out


def find_open_positions_for_strategy(
    base_url: str,
    headers: dict[str, str],
    account_id: int,
    strategy_id: int,
) -> list[dict]:
    rows = http_json(
        "GET",
        f"{base_url}/oms/positions/open?account_ids={account_id}",
        headers,
    ).get("items", [])
    out: list[dict] = []
    for row in rows:
        if int(row.get("strategy_id", 0) or 0) == int(strategy_id):
            out.append(row)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Run live testnet scenarios")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    runtime_dir = root / "test/testnet/runtime"
    logger = Logger(runtime_dir=runtime_dir, verbose=args.verbose)

    context_path = runtime_dir / "context.json"
    if not context_path.exists():
        raise RuntimeError("missing context.json. run: py -3.13 test/testnet/run.py")
    context = json.loads(context_path.read_text(encoding="utf-8"))

    base_url = context["base_url"]
    user_id = int(context["user_id"])
    internal_api_key = str(context["internal_api_key"])
    symbol = str(context.get("symbol", "BTC/USDT"))
    headers = {"x-api-key": internal_api_key}

    env_path = root / "test/testnet/.env.testnet"
    if not env_path.exists():
        raise RuntimeError("missing .env.testnet")
    env_map: dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_map[key.strip()] = value.strip()
    api_key = env_map.get("BINANCE_TESTNET_API_KEY", "")
    secret_key = env_map.get("BINANCE_TESTNET_SECRET_KEY", "")
    if not api_key or not secret_key:
        raise RuntimeError("BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_SECRET_KEY are required in .env.testnet")

    compose = ["docker", "compose", "-f", "apps/api/docker-compose.stack.yml"]
    logger.info("starting scenarios execution")
    logger.info(f"base_url={base_url} symbol={symbol} user_id={user_id}")
    # Scenarios require trading permissions; admins are intentionally read-only in dispatcher.
    run_cmd(
        compose
        + [
            "exec",
            "-T",
            "mysql",
            "mysql",
            "-uroot",
            "-proot",
            "ccxt_position",
            "-e",
            f"UPDATE users SET role='trader' WHERE id={user_id};",
        ]
    )
    qty = choose_buy_qty(base_url, headers, int(context["account_id"]), symbol, logger=logger)
    logger.info(f"calculated qty={qty} from live balance/ticker")

    hedge_account = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="hedge",
        label=f"{symbol.replace('/', '-')}-hedge",
        api_key=api_key,
        secret_key=secret_key,
        logger=logger,
    )
    netting_account = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="netting",
        label=f"{symbol.replace('/', '-')}-netting",
        api_key=api_key,
        secret_key=secret_key,
        logger=logger,
        exchange_id="ccxtpro.binance",
    )
    hedge_buy_strategy = create_strategy(
        base_url, headers, hedge_account, build_strategy_name(symbol, "hedge-buy", hedge_account), 101, logger
    )
    hedge_sell_strategy = create_strategy(
        base_url, headers, hedge_account, build_strategy_name(symbol, "hedge-sell", hedge_account), 202, logger
    )
    net_buy_strategy = create_strategy(
        base_url, headers, netting_account, build_strategy_name(symbol, "net-buy", netting_account), 301, logger
    )
    net_reduce_strategy = create_strategy(
        base_url, headers, netting_account, build_strategy_name(symbol, "net-reduce", netting_account), 302, logger
    )
    net_reverse_strategy = create_strategy(
        base_url, headers, netting_account, build_strategy_name(symbol, "net-reverse", netting_account), 303, logger
    )
    strategy_netting_account = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="strategy_netting",
        label=f"{symbol.replace('/', '-')}-strategy-netting",
        api_key=api_key,
        secret_key=secret_key,
        logger=logger,
    )
    sn_strategy_a = create_strategy(
        base_url,
        headers,
        strategy_netting_account,
        build_strategy_name(symbol, "sn-a", strategy_netting_account),
        501,
        logger,
    )
    sn_strategy_b = create_strategy(
        base_url,
        headers,
        strategy_netting_account,
        build_strategy_name(symbol, "sn-b", strategy_netting_account),
        502,
        logger,
    )

    # Hedge scenario: buy with strategy 101, sell with strategy 202 -> both sides can coexist.
    logger.info("running hedge scenario")
    hedge_order_1 = send_market(
        base_url, headers, hedge_account, symbol, "buy", qty, hedge_buy_strategy, logger
    )
    hedge_order_2 = send_market(
        base_url, headers, hedge_account, symbol, "sell", qty, hedge_sell_strategy, logger
    )
    h1_deal = wait_deal_for_order(
        base_url, headers, hedge_account, hedge_order_1, strategy_id=hedge_buy_strategy, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not h1_deal:
        force_reconcile_now(base_url, headers, hedge_account, logger, symbols_hint=[symbol])
        h1_deal = wait_deal_for_order(
            base_url, headers, hedge_account, hedge_order_1, strategy_id=hedge_buy_strategy, timeout_s=30, sleep_s=2.0
        )
    h2_deal = wait_deal_for_order(
        base_url, headers, hedge_account, hedge_order_2, strategy_id=hedge_sell_strategy, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not h2_deal:
        force_reconcile_now(base_url, headers, hedge_account, logger, symbols_hint=[symbol])
        h2_deal = wait_deal_for_order(
            base_url, headers, hedge_account, hedge_order_2, strategy_id=hedge_sell_strategy, timeout_s=30, sleep_s=2.0
        )
    logger.info(f"hedge deals found: order1={bool(h1_deal)} order2={bool(h2_deal)}")
    if not h1_deal or not h2_deal:
        raise RuntimeError(f"hedge scenario requires deals for both orders, got: {h1_deal} / {h2_deal}")
    hedge_deals = wait_min_deals(base_url, headers, hedge_account, 2) or []
    hedge_positions = http_json(
        "GET", f"{base_url}/oms/positions/open?account_ids={hedge_account}", headers
    ).get("items", [])

    # Netting scenario: buy, partial sell reduce, then sell larger to reverse side.
    qty_dec = Decimal(qty)
    qty_half = format((qty_dec / Decimal("2")).quantize(Decimal("0.000001")), "f")
    qty_reverse = format((qty_dec * Decimal("1.5")).quantize(Decimal("0.000001")), "f")
    logger.info("running netting scenario")
    net_order_1 = send_market(
        base_url, headers, netting_account, symbol, "buy", qty, net_buy_strategy, logger
    )
    net_order_2 = send_market(
        base_url, headers, netting_account, symbol, "sell", qty_half, net_reduce_strategy, logger
    )
    net_order_3 = send_market(
        base_url, headers, netting_account, symbol, "sell", qty_reverse, net_reverse_strategy, logger
    )
    n1 = wait_deal_for_order(
        base_url, headers, netting_account, net_order_1, strategy_id=net_buy_strategy, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not n1:
        force_reconcile_now(base_url, headers, netting_account, logger, symbols_hint=[symbol])
        n1 = wait_deal_for_order(
            base_url, headers, netting_account, net_order_1, strategy_id=net_buy_strategy, timeout_s=30, sleep_s=2.0
        )
    n2 = wait_deal_for_order(
        base_url, headers, netting_account, net_order_2, strategy_id=net_reduce_strategy, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not n2:
        force_reconcile_now(base_url, headers, netting_account, logger, symbols_hint=[symbol])
        n2 = wait_deal_for_order(
            base_url, headers, netting_account, net_order_2, strategy_id=net_reduce_strategy, timeout_s=30, sleep_s=2.0
        )
    n3 = wait_deal_for_order(
        base_url, headers, netting_account, net_order_3, strategy_id=net_reverse_strategy, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not n3:
        force_reconcile_now(base_url, headers, netting_account, logger, symbols_hint=[symbol])
        n3 = wait_deal_for_order(
            base_url, headers, netting_account, net_order_3, strategy_id=net_reverse_strategy, timeout_s=30, sleep_s=2.0
        )
    logger.info(f"netting deals found: o1={bool(n1)} o2={bool(n2)} o3={bool(n3)}")
    if not n1:
        raise RuntimeError(f"netting scenario requires at least first buy deal, got: {n1}")
    if not n2 or not n3:
        logger.warn(
            "netting sell/reverse deals missing (common on spot/no-short environments); continuing with partial netting validation"
        )
    expected_net_deals = max(1, int(bool(n1)) + int(bool(n2)) + int(bool(n3)))
    net_deals = wait_min_deals(base_url, headers, netting_account, expected_net_deals) or []
    net_open = http_json(
        "GET", f"{base_url}/oms/positions/open?account_ids={netting_account}", headers
    ).get("items", [])
    net_hist = http_json(
        "GET", f"{base_url}/oms/positions/history?account_ids={netting_account}&{history_window_qs()}", headers
    ).get("items", [])

    # Strategy netting staircase:
    # per strategy_id + symbol, increase position in small steps, reduce to zero, then try reverse/close.
    logger.info("running strategy_netting staircase scenario")
    qty_step = format((qty_dec / Decimal("3")).quantize(Decimal("0.000001")), "f")
    if Decimal(qty_step) <= Decimal("0"):
        qty_step = qty
    logger.info(
        f"strategy_netting staircase qty_step={qty_step} account_id={strategy_netting_account} "
        f"strategy_a={sn_strategy_a} strategy_b={sn_strategy_b}"
    )

    # Baseline "mix": same symbol, distinct strategies should map to independent positions in strategy_netting mode.
    sn_mix_order_a = send_market(
        base_url, headers, strategy_netting_account, symbol, "buy", qty_step, sn_strategy_a, logger
    )
    sn_mix_order_b = send_market(
        base_url, headers, strategy_netting_account, symbol, "buy", qty_step, sn_strategy_b, logger
    )
    sn_mix_deal_a = wait_deal_for_order(
        base_url,
        headers,
        strategy_netting_account,
        sn_mix_order_a,
        strategy_id=sn_strategy_a,
        timeout_s=args.timeout_seconds,
        sleep_s=2.0,
    )
    sn_mix_deal_b = wait_deal_for_order(
        base_url,
        headers,
        strategy_netting_account,
        sn_mix_order_b,
        strategy_id=sn_strategy_b,
        timeout_s=args.timeout_seconds,
        sleep_s=2.0,
    )
    if not sn_mix_deal_a or not sn_mix_deal_b:
        force_reconcile_now(base_url, headers, strategy_netting_account, logger, symbols_hint=[symbol])
        if not sn_mix_deal_a:
            sn_mix_deal_a = wait_deal_for_order(
                base_url,
                headers,
                strategy_netting_account,
                sn_mix_order_a,
                strategy_id=sn_strategy_a,
                timeout_s=30,
                sleep_s=2.0,
            )
        if not sn_mix_deal_b:
            sn_mix_deal_b = wait_deal_for_order(
                base_url,
                headers,
                strategy_netting_account,
                sn_mix_order_b,
                strategy_id=sn_strategy_b,
                timeout_s=30,
                sleep_s=2.0,
            )
    if not sn_mix_deal_a or not sn_mix_deal_b:
        raise RuntimeError(
            "strategy_netting baseline requires deals for both strategies, "
            f"got: a={sn_mix_deal_a} b={sn_mix_deal_b}"
        )

    # Staircase up for strategy A
    sn_up_orders: list[int] = []
    sn_up_deals_ok = 0
    for _ in range(3):
        oid = send_market(
            base_url, headers, strategy_netting_account, symbol, "buy", qty_step, sn_strategy_a, logger
        )
        sn_up_orders.append(oid)
        deal = wait_deal_for_order(
            base_url,
            headers,
            strategy_netting_account,
            oid,
            strategy_id=sn_strategy_a,
            timeout_s=args.timeout_seconds,
            sleep_s=2.0,
        )
        if not deal:
            force_reconcile_now(base_url, headers, strategy_netting_account, logger, symbols_hint=[symbol])
            deal = wait_deal_for_order(
                base_url,
                headers,
                strategy_netting_account,
                oid,
                strategy_id=sn_strategy_a,
                timeout_s=30,
                sleep_s=2.0,
            )
        if deal:
            sn_up_deals_ok += 1
    if sn_up_deals_ok < 3:
        raise RuntimeError(f"strategy_netting staircase up failed: deals_ok={sn_up_deals_ok}/3")

    # Staircase down for strategy A (attempt to flatten to zero)
    sn_down_orders: list[int] = []
    sn_down_deals_ok = 0
    for _ in range(3):
        oid = send_market(
            base_url, headers, strategy_netting_account, symbol, "sell", qty_step, sn_strategy_a, logger
        )
        sn_down_orders.append(oid)
        deal = wait_deal_for_order(
            base_url,
            headers,
            strategy_netting_account,
            oid,
            strategy_id=sn_strategy_a,
            timeout_s=args.timeout_seconds,
            sleep_s=2.0,
        )
        if not deal:
            force_reconcile_now(base_url, headers, strategy_netting_account, logger, symbols_hint=[symbol])
            deal = wait_deal_for_order(
                base_url,
                headers,
                strategy_netting_account,
                oid,
                strategy_id=sn_strategy_a,
                timeout_s=30,
                sleep_s=2.0,
            )
        if deal:
            sn_down_deals_ok += 1
    logger.info(f"strategy_netting staircase down deals_ok={sn_down_deals_ok}/3")

    # Reverse and close attempt for strategy A. On spot/no-short this may not fill; keep as best-effort validation.
    sn_reverse_order = send_market(
        base_url, headers, strategy_netting_account, symbol, "sell", qty_step, sn_strategy_a, logger
    )
    sn_reverse_deal = wait_deal_for_order(
        base_url,
        headers,
        strategy_netting_account,
        sn_reverse_order,
        strategy_id=sn_strategy_a,
        timeout_s=args.timeout_seconds,
        sleep_s=2.0,
    )
    if not sn_reverse_deal:
        force_reconcile_now(base_url, headers, strategy_netting_account, logger, symbols_hint=[symbol])
        sn_reverse_deal = wait_deal_for_order(
            base_url,
            headers,
            strategy_netting_account,
            sn_reverse_order,
            strategy_id=sn_strategy_a,
            timeout_s=30,
            sleep_s=2.0,
        )
    sn_close_order = send_market(
        base_url, headers, strategy_netting_account, symbol, "buy", qty_step, sn_strategy_a, logger
    )
    sn_close_deal = wait_deal_for_order(
        base_url,
        headers,
        strategy_netting_account,
        sn_close_order,
        strategy_id=sn_strategy_a,
        timeout_s=args.timeout_seconds,
        sleep_s=2.0,
    )
    if not sn_close_deal:
        force_reconcile_now(base_url, headers, strategy_netting_account, logger, symbols_hint=[symbol])
        sn_close_deal = wait_deal_for_order(
            base_url,
            headers,
            strategy_netting_account,
            sn_close_order,
            strategy_id=sn_strategy_a,
            timeout_s=30,
            sleep_s=2.0,
        )
    if not sn_reverse_deal or not sn_close_deal:
        logger.warn(
            "strategy_netting reverse/close deal missing (common on spot/no-short); "
            "continuing with staircase validation"
        )

    sn_deals_all = wait_min_deals(base_url, headers, strategy_netting_account, 2) or []
    sn_open_a = find_open_positions_for_strategy(
        base_url, headers, strategy_netting_account, sn_strategy_a
    )
    sn_open_b = find_open_positions_for_strategy(
        base_url, headers, strategy_netting_account, sn_strategy_b
    )

    # Mirror scenario: two account_ids sharing exact same exchange credentials.
    logger.info("running mirror reconciliation scenario")
    mirror_a = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="hedge",
        label=f"{symbol.replace('/', '-')}-mirror-a",
        api_key=api_key,
        secret_key=secret_key,
        logger=logger,
    )
    mirror_b = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="hedge",
        label=f"{symbol.replace('/', '-')}-mirror-b",
        api_key=api_key,
        secret_key=secret_key,
        logger=logger,
    )
    mirror_strategy = create_strategy(
        base_url, headers, mirror_a, build_strategy_name(symbol, "mirror-a", mirror_a), 404, logger
    )
    mirror_order = send_market(base_url, headers, mirror_a, symbol, "buy", qty, mirror_strategy, logger)
    m = wait_deal_for_order(
        base_url, headers, mirror_a, mirror_order, strategy_id=mirror_strategy, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not m:
        force_reconcile_now(base_url, headers, mirror_a, logger, symbols_hint=[symbol])
        m = wait_deal_for_order(
            base_url, headers, mirror_a, mirror_order, strategy_id=mirror_strategy, timeout_s=30, sleep_s=2.0
        )
    logger.info(f"mirror source order deal found: {bool(m)}")
    if not m:
        raise RuntimeError(f"mirror source order must generate a deal, got: {m}")
    # Mirror account needs an explicit reconcile kick to ingest trades from the same exchange credentials.
    force_reconcile_now(base_url, headers, mirror_b, logger, symbols_hint=[symbol])

    # Reconciliation runs periodically; wait until mirrored trade appears in both accounts.
    mirror_poll_count = 0

    def _mirror_check():
        nonlocal mirror_poll_count
        mirror_poll_count += 1
        if mirror_poll_count % 5 == 0:
            force_reconcile_now(base_url, headers, mirror_b, logger, symbols_hint=[symbol])
        qs = history_window_qs()
        deals_a = http_json("GET", f"{base_url}/oms/deals?account_ids={mirror_a}&{qs}", headers).get("items", [])
        deals_b = http_json("GET", f"{base_url}/oms/deals?account_ids={mirror_b}&{qs}", headers).get("items", [])
        st_a = fetch_reconcile_status(base_url, headers, mirror_a, stale_after_seconds=120)
        st_b = fetch_reconcile_status(base_url, headers, mirror_b, stale_after_seconds=120)
        ids_a = collect_exchange_trade_ids(deals_a)
        ids_b = collect_exchange_trade_ids(deals_b)
        common = sorted(ids_a.intersection(ids_b))
        logger.info(
            "mirror poll: "
            f"account_a_deals={len(deals_a)} account_b_deals={len(deals_b)} common_trade_ids={len(common)} "
            f"| reconcile_a={st_a.get('status')} cursor_a={st_a.get('cursor_value')} "
            f"| reconcile_b={st_b.get('status')} cursor_b={st_b.get('cursor_value')}"
        )
        if common:
            return {"common_trade_ids": common, "deals_a": deals_a, "deals_b": deals_b}
        return None

    mirror = wait_until(_mirror_check, timeout_s=240, sleep_s=3.0)
    if not mirror:
        logger.warn("mirror reconciliation failed; collecting diagnostics")
        diag = {
            "orders_a": http_json(
                "GET", f"{base_url}/oms/orders/history?account_ids={mirror_a}&{history_window_qs()}", headers
            ).get("items", []),
            "orders_b": http_json(
                "GET", f"{base_url}/oms/orders/history?account_ids={mirror_b}&{history_window_qs()}", headers
            ).get("items", []),
            "deals_a": http_json(
                "GET", f"{base_url}/oms/deals?account_ids={mirror_a}&{history_window_qs()}", headers
            ).get("items", []),
            "deals_b": http_json(
                "GET", f"{base_url}/oms/deals?account_ids={mirror_b}&{history_window_qs()}", headers
            ).get("items", []),
            "reconcile_status_a": fetch_reconcile_status(base_url, headers, mirror_a, stale_after_seconds=120),
            "reconcile_status_b": fetch_reconcile_status(base_url, headers, mirror_b, stale_after_seconds=120),
        }
        diag_path = runtime_dir / "scenarios-diagnostics.json"
        diag_path.write_text(json.dumps(diag, indent=2), encoding="utf-8")
        logger.warn(f"diagnostics saved: {diag_path}")
        raise RuntimeError("mirror reconciliation failed: no shared exchange_trade_id between mirror accounts")

    summary = {
        "hedge": {
            "account_id": hedge_account,
            "qty": qty,
            "orders": [hedge_order_1, hedge_order_2],
            "deal_count": len(hedge_deals),
            "deal_strategy_ids": sorted({int(d["strategy_id"]) for d in hedge_deals}) if hedge_deals else [],
            "open_positions_sides": sorted({str(p["side"]) for p in hedge_positions}),
        },
        "netting": {
            "account_id": netting_account,
            "qty": {"base": qty, "half": qty_half, "reverse": qty_reverse},
            "orders": [net_order_1, net_order_2, net_order_3],
            "deal_found_flags": {"o1": bool(n1), "o2": bool(n2), "o3": bool(n3)},
            "deal_count": len(net_deals),
            "deal_strategy_ids": sorted({int(d["strategy_id"]) for d in net_deals}) if net_deals else [],
            "open_positions_sides": sorted({str(p["side"]) for p in net_open}),
            "history_positions": len(net_hist),
        },
        "mirror_reconciliation": {
            "account_a": mirror_a,
            "account_b": mirror_b,
            "order_from_a": mirror_order,
            "shared_trade_ids": mirror["common_trade_ids"],
            "deal_count_a": len(mirror["deals_a"]),
            "deal_count_b": len(mirror["deals_b"]),
        },
        "strategy_netting_staircase": {
            "account_id": strategy_netting_account,
            "symbol": symbol,
            "qty_step": qty_step,
            "strategy_ids": {"a": sn_strategy_a, "b": sn_strategy_b},
            "baseline": {
                "mix_orders": [sn_mix_order_a, sn_mix_order_b],
                "mix_deals_ok": {"a": bool(sn_mix_deal_a), "b": bool(sn_mix_deal_b)},
            },
            "staircase": {
                "up_orders": sn_up_orders,
                "down_orders": sn_down_orders,
                "up_deals_ok_count": sn_up_deals_ok,
                "down_deals_ok_count": sn_down_deals_ok,
            },
            "reverse_close": {
                "reverse_order": sn_reverse_order,
                "close_order": sn_close_order,
                "reverse_deal_ok": bool(sn_reverse_deal),
                "close_deal_ok": bool(sn_close_deal),
            },
            "open_positions_per_strategy": {
                "a_count": len(sn_open_a),
                "b_count": len(sn_open_b),
                "a_sides": sorted({str(p.get("side", "")) for p in sn_open_a if p.get("side")}),
                "b_sides": sorted({str(p.get("side", "")) for p in sn_open_b if p.get("side")}),
            },
            "deal_count_total": len(sn_deals_all),
            "deal_strategy_ids": sorted({int(d["strategy_id"]) for d in sn_deals_all}) if sn_deals_all else [],
        },
    }

    out_path = runtime_dir / "scenarios.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info(f"scenario summary saved: {out_path}")
    print(json.dumps(summary, indent=2))
    print(f"saved: {out_path}")
    print(f"log: {logger.log_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)


