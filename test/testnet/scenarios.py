import json
import argparse
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from urllib import request as urllib_request


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
    with urllib_request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def wait_until(fn, timeout_s: int = 120, sleep_s: float = 2.0):
    start = time.time()
    last = None
    while time.time() - start < timeout_s:
        last = fn()
        if last:
            return last
        time.sleep(sleep_s)
    return last


def create_account_for_mode(
    compose: list[str],
    user_id: int,
    mode: str,
    label: str,
    api_key: str,
    secret_key: str,
    logger: Logger,
) -> int:
    logger.info(f"creating account mode={mode} label={label}")
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
            "binance",
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
    magic_id: int,
    logger: Logger,
) -> int:
    logger.info(
        f"sending market order account_id={account_id} symbol={symbol} side={side} qty={qty} magic_id={magic_id}"
    )
    out = http_json(
        "POST",
        f"{base_url}/position/commands",
        headers,
        {
            "account_id": account_id,
            "command": "send_order",
            "payload": {
                "symbol": symbol,
                "side": side,
                "order_type": "market",
                "qty": qty,
                "magic_id": magic_id,
                "position_id": 0,
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
        rows = http_json("GET", f"{base_url}/position/orders/history?account_id={account_id}", headers).get("items", [])
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
    timeout_s: int = 180,
    sleep_s: float = 2.0,
) -> dict | None:
    def _check():
        rows = http_json("GET", f"{base_url}/position/deals?account_id={account_id}", headers).get("items", [])
        for row in rows:
            if row.get("order_id") is not None and int(row["order_id"]) == order_id:
                return row
        return None

    return wait_until(_check, timeout_s=timeout_s, sleep_s=sleep_s)


def force_reconcile_now(base_url: str, headers: dict[str, str], account_id: int, logger: Logger) -> None:
    logger.info(f"forcing reconciliation for account_id={account_id}")
    http_json(
        "POST",
        f"{base_url}/position/reconcile",
        headers,
        {"account_id": account_id},
    )


def choose_buy_qty(base_url: str, headers: dict[str, str], account_id: int, symbol: str) -> str:
    ticker = http_json(
        "POST",
        f"{base_url}/ccxt/{account_id}/fetch_ticker",
        headers,
        {"args": [symbol], "kwargs": {}},
    )
    last = Decimal(str(ticker["result"]["last"]))
    balance = http_json(
        "POST",
        f"{base_url}/ccxt/core/{account_id}/fetch_balance",
        headers,
        {"params": {}},
    )
    quote = symbol.split("/")[-1]
    free = Decimal("0")
    try:
        free = Decimal(str(balance["result"][quote]["free"]))
    except Exception:
        free = Decimal("0")

    if free <= Decimal("12"):
        raise RuntimeError(f"insufficient {quote} free balance for scenarios: {free}")

    quote_to_use = min(free * Decimal("0.15"), Decimal("30"))
    if quote_to_use < Decimal("12"):
        quote_to_use = Decimal("12")

    qty = (quote_to_use / last).quantize(Decimal("0.000001"))
    if qty <= 0:
        raise RuntimeError(f"calculated qty invalid: {qty}")
    return format(qty, "f")


def wait_min_deals(base_url: str, headers: dict[str, str], account_id: int, min_count: int) -> list[dict] | None:
    def _check():
        rows = http_json("GET", f"{base_url}/position/deals?account_id={account_id}", headers).get("items", [])
        return rows if len(rows) >= min_count else None

    return wait_until(_check, timeout_s=240, sleep_s=3.0)


def collect_exchange_trade_ids(deals: list[dict]) -> set[str]:
    out: set[str] = set()
    for d in deals:
        trade_id = d.get("exchange_trade_id")
        if trade_id:
            out.add(str(trade_id))
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
    qty = choose_buy_qty(base_url, headers, int(context["account_id"]), symbol)
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
    )

    # Hedge scenario: buy with magic 101, sell with magic 202 -> both sides can coexist.
    logger.info("running hedge scenario")
    hedge_order_1 = send_market(base_url, headers, hedge_account, symbol, "buy", qty, 101, logger)
    hedge_order_2 = send_market(base_url, headers, hedge_account, symbol, "sell", qty, 202, logger)
    h1_deal = wait_deal_for_order(
        base_url, headers, hedge_account, hedge_order_1, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not h1_deal:
        force_reconcile_now(base_url, headers, hedge_account, logger)
        h1_deal = wait_deal_for_order(base_url, headers, hedge_account, hedge_order_1, timeout_s=30, sleep_s=2.0)
    h2_deal = wait_deal_for_order(
        base_url, headers, hedge_account, hedge_order_2, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not h2_deal:
        force_reconcile_now(base_url, headers, hedge_account, logger)
        h2_deal = wait_deal_for_order(base_url, headers, hedge_account, hedge_order_2, timeout_s=30, sleep_s=2.0)
    logger.info(f"hedge deals found: order1={bool(h1_deal)} order2={bool(h2_deal)}")
    if not h1_deal or not h2_deal:
        raise RuntimeError(f"hedge scenario requires deals for both orders, got: {h1_deal} / {h2_deal}")
    hedge_deals = wait_min_deals(base_url, headers, hedge_account, 2) or []
    hedge_positions = http_json(
        "GET", f"{base_url}/position/positions/open?account_id={hedge_account}", headers
    ).get("items", [])

    # Netting scenario: buy, partial sell reduce, then sell larger to reverse side.
    qty_dec = Decimal(qty)
    qty_half = format((qty_dec / Decimal("2")).quantize(Decimal("0.000001")), "f")
    qty_reverse = format((qty_dec * Decimal("1.5")).quantize(Decimal("0.000001")), "f")
    logger.info("running netting scenario")
    net_order_1 = send_market(base_url, headers, netting_account, symbol, "buy", qty, 301, logger)
    net_order_2 = send_market(base_url, headers, netting_account, symbol, "sell", qty_half, 302, logger)
    net_order_3 = send_market(base_url, headers, netting_account, symbol, "sell", qty_reverse, 303, logger)
    n1 = wait_deal_for_order(
        base_url, headers, netting_account, net_order_1, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not n1:
        force_reconcile_now(base_url, headers, netting_account, logger)
        n1 = wait_deal_for_order(base_url, headers, netting_account, net_order_1, timeout_s=30, sleep_s=2.0)
    n2 = wait_deal_for_order(
        base_url, headers, netting_account, net_order_2, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not n2:
        force_reconcile_now(base_url, headers, netting_account, logger)
        n2 = wait_deal_for_order(base_url, headers, netting_account, net_order_2, timeout_s=30, sleep_s=2.0)
    n3 = wait_deal_for_order(
        base_url, headers, netting_account, net_order_3, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not n3:
        force_reconcile_now(base_url, headers, netting_account, logger)
        n3 = wait_deal_for_order(base_url, headers, netting_account, net_order_3, timeout_s=30, sleep_s=2.0)
    logger.info(f"netting deals found: o1={bool(n1)} o2={bool(n2)} o3={bool(n3)}")
    if not n1 or not n2 or not n3:
        raise RuntimeError(f"netting scenario requires deals for all orders, got: {n1} / {n2} / {n3}")
    net_deals = wait_min_deals(base_url, headers, netting_account, 3) or []
    net_open = http_json(
        "GET", f"{base_url}/position/positions/open?account_id={netting_account}", headers
    ).get("items", [])
    net_hist = http_json(
        "GET", f"{base_url}/position/positions/history?account_id={netting_account}", headers
    ).get("items", [])

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
    mirror_order = send_market(base_url, headers, mirror_a, symbol, "buy", qty, 404, logger)
    m = wait_deal_for_order(
        base_url, headers, mirror_a, mirror_order, timeout_s=args.timeout_seconds, sleep_s=2.0
    )
    if not m:
        force_reconcile_now(base_url, headers, mirror_a, logger)
        m = wait_deal_for_order(base_url, headers, mirror_a, mirror_order, timeout_s=30, sleep_s=2.0)
    logger.info(f"mirror source order deal found: {bool(m)}")
    if not m:
        raise RuntimeError(f"mirror source order must generate a deal, got: {m}")

    # Reconciliation runs periodically; wait until mirrored trade appears in both accounts.
    def _mirror_check():
        deals_a = http_json("GET", f"{base_url}/position/deals?account_id={mirror_a}", headers).get("items", [])
        deals_b = http_json("GET", f"{base_url}/position/deals?account_id={mirror_b}", headers).get("items", [])
        ids_a = collect_exchange_trade_ids(deals_a)
        ids_b = collect_exchange_trade_ids(deals_b)
        common = sorted(ids_a.intersection(ids_b))
        logger.info(
            f"mirror poll: account_a_deals={len(deals_a)} account_b_deals={len(deals_b)} common_trade_ids={len(common)}"
        )
        if common:
            return {"common_trade_ids": common, "deals_a": deals_a, "deals_b": deals_b}
        return None

    mirror = wait_until(_mirror_check, timeout_s=240, sleep_s=3.0)
    if not mirror:
        logger.warn("mirror reconciliation failed; collecting diagnostics")
        diag = {
            "orders_a": http_json("GET", f"{base_url}/position/orders/history?account_id={mirror_a}", headers).get("items", []),
            "orders_b": http_json("GET", f"{base_url}/position/orders/history?account_id={mirror_b}", headers).get("items", []),
            "deals_a": http_json("GET", f"{base_url}/position/deals?account_id={mirror_a}", headers).get("items", []),
            "deals_b": http_json("GET", f"{base_url}/position/deals?account_id={mirror_b}", headers).get("items", []),
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
            "deal_magic_ids": sorted({int(d["magic_id"]) for d in hedge_deals}) if hedge_deals else [],
            "open_positions_sides": sorted({str(p["side"]) for p in hedge_positions}),
        },
        "netting": {
            "account_id": netting_account,
            "qty": {"base": qty, "half": qty_half, "reverse": qty_reverse},
            "orders": [net_order_1, net_order_2, net_order_3],
            "deal_count": len(net_deals),
            "deal_magic_ids": sorted({int(d["magic_id"]) for d in net_deals}) if net_deals else [],
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
