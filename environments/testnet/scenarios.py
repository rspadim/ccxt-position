import json
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from urllib import request as urllib_request


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
) -> int:
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
    return account_id


def send_market(base_url: str, headers: dict[str, str], account_id: int, symbol: str, side: str, qty: str, magic_id: int) -> int:
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


def wait_min_deals(base_url: str, headers: dict[str, str], account_id: int, min_count: int) -> list[dict] | None:
    def _check():
        rows = http_json("GET", f"{base_url}/position/deals?account_id={account_id}", headers).get("items", [])
        return rows if len(rows) >= min_count else None

    return wait_until(_check, timeout_s=240, sleep_s=3.0)


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    context_path = root / "environments/testnet/runtime/context.json"
    if not context_path.exists():
        raise RuntimeError("missing context.json. run: py -3.13 environments/testnet/run.py")
    context = json.loads(context_path.read_text(encoding="utf-8"))

    base_url = context["base_url"]
    user_id = int(context["user_id"])
    internal_api_key = str(context["internal_api_key"])
    symbol = str(context.get("symbol", "BTC/USDT"))
    headers = {"x-api-key": internal_api_key}

    env_path = root / "environments/testnet/.env.testnet"
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

    hedge_account = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="hedge",
        label=f"{symbol.replace('/', '-')}-hedge",
        api_key=api_key,
        secret_key=secret_key,
    )
    netting_account = create_account_for_mode(
        compose=compose,
        user_id=user_id,
        mode="netting",
        label=f"{symbol.replace('/', '-')}-netting",
        api_key=api_key,
        secret_key=secret_key,
    )

    # Hedge scenario: buy with magic 101, sell with magic 202 -> both sides can coexist.
    hedge_order_1 = send_market(base_url, headers, hedge_account, symbol, "buy", "0.001", 101)
    hedge_order_2 = send_market(base_url, headers, hedge_account, symbol, "sell", "0.001", 202)
    wait_order_terminal(base_url, headers, hedge_account, hedge_order_1)
    wait_order_terminal(base_url, headers, hedge_account, hedge_order_2)
    hedge_deals = wait_min_deals(base_url, headers, hedge_account, 2) or []
    hedge_positions = http_json(
        "GET", f"{base_url}/position/positions/open?account_id={hedge_account}", headers
    ).get("items", [])

    # Netting scenario: buy, partial sell reduce, then sell larger to reverse side.
    net_order_1 = send_market(base_url, headers, netting_account, symbol, "buy", "0.002", 301)
    net_order_2 = send_market(base_url, headers, netting_account, symbol, "sell", "0.001", 302)
    net_order_3 = send_market(base_url, headers, netting_account, symbol, "sell", "0.003", 303)
    wait_order_terminal(base_url, headers, netting_account, net_order_1)
    wait_order_terminal(base_url, headers, netting_account, net_order_2)
    wait_order_terminal(base_url, headers, netting_account, net_order_3)
    net_deals = wait_min_deals(base_url, headers, netting_account, 3) or []
    net_open = http_json(
        "GET", f"{base_url}/position/positions/open?account_id={netting_account}", headers
    ).get("items", [])
    net_hist = http_json(
        "GET", f"{base_url}/position/positions/history?account_id={netting_account}", headers
    ).get("items", [])

    summary = {
        "hedge": {
            "account_id": hedge_account,
            "orders": [hedge_order_1, hedge_order_2],
            "deal_count": len(hedge_deals),
            "deal_magic_ids": sorted({int(d["magic_id"]) for d in hedge_deals}) if hedge_deals else [],
            "open_positions_sides": sorted({str(p["side"]) for p in hedge_positions}),
        },
        "netting": {
            "account_id": netting_account,
            "orders": [net_order_1, net_order_2, net_order_3],
            "deal_count": len(net_deals),
            "deal_magic_ids": sorted({int(d["magic_id"]) for d in net_deals}) if net_deals else [],
            "open_positions_sides": sorted({str(p["side"]) for p in net_open}),
            "history_positions": len(net_hist),
        },
    }

    out_path = root / "environments/testnet/runtime/scenarios.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"saved: {out_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
