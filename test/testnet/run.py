import json
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
from urllib import request as urllib_request


def load_env_file(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"missing env file: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip()


def run_cmd(args: list[str]) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(args)}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc.stdout.strip()


def run_json(args: list[str]) -> dict:
    raw = run_cmd(args)
    return json.loads(raw)


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


def wait_http_ok(url: str, timeout_s: int = 120, sleep_s: float = 2.0) -> None:
    start = time.time()
    last_error = ""
    while time.time() - start < timeout_s:
        try:
            req = urllib_request.Request(url=url, method="GET")
            with urllib_request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return
        except Exception as exc:
            last_error = str(exc)
        time.sleep(sleep_s)
    raise RuntimeError(f"timeout waiting for {url}: {last_error}")


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    env_file = Path(__file__).resolve().parent / ".env.testnet"
    runtime_dir = Path(__file__).resolve().parent / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    load_env_file(env_file)

    required = [
        "BINANCE_TESTNET_API_KEY",
        "BINANCE_TESTNET_SECRET_KEY",
        "TESTNET_MASTER_KEY",
        "INTERNAL_API_KEY",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise RuntimeError(f"missing required env keys: {', '.join(missing)}")

    user_name = os.environ.get("TESTNET_USER_NAME", "testnet-admin")
    account_label = os.environ.get("TESTNET_ACCOUNT_LABEL", "binance-testnet")
    symbol = os.environ.get("TESTNET_SYMBOL", "BTC/USDT")
    internal_api_key = os.environ["INTERNAL_API_KEY"]
    reset_stack = os.environ.get("TESTNET_RESET_STACK", "1").strip().lower() in {"1", "true", "yes"}

    cfg_example = root / "apps/api/config.docker.example.json"
    cfg_file = root / "apps/api/config.docker.json"
    cfg = json.loads(cfg_example.read_text(encoding="utf-8"))
    cfg["security"]["encryption_master_key"] = os.environ["TESTNET_MASTER_KEY"]
    cfg["security"]["require_encrypted_credentials"] = False
    cfg_file.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    compose = ["docker", "compose", "-f", "apps/api/docker-compose.stack.yml"]
    if reset_stack:
        run_cmd(compose + ["down", "-v"])
    run_cmd(compose + ["up", "-d", "--build"])
    wait_http_ok("http://127.0.0.1:8000/healthz", timeout_s=180, sleep_s=2.0)

    user = run_json(compose + ["exec", "-T", "api", "python", "-m", "apps.api.cli", "create-user", "--name", user_name])
    user_id = int(user["user_id"])

    run_json(
        compose
        + [
            "exec",
            "-T",
            "api",
            "python",
            "-m",
            "apps.api.cli",
            "create-api-key",
            "--user-id",
            str(user_id),
            "--api-key",
            internal_api_key,
        ]
    )

    account = run_json(
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
            account_label,
            "--testnet",
        ]
    )
    account_id = int(account["account_id"])

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
            os.environ["BINANCE_TESTNET_API_KEY"],
            "--secret",
            os.environ["BINANCE_TESTNET_SECRET_KEY"],
            "--encrypt-input",
        ]
    )

    headers = {"x-api-key": internal_api_key}
    ticker = http_json(
        "POST",
        f"http://127.0.0.1:8000/ccxt/{account_id}/fetch_ticker",
        headers,
        {"args": [symbol], "kwargs": {}},
    )
    last = Decimal(str(ticker["result"]["last"]))
    price = (last * Decimal("0.995")).quantize(Decimal("0.01"))

    cmd = http_json(
        "POST",
        "http://127.0.0.1:8000/position/commands",
        headers,
        {
            "account_id": account_id,
            "command": "send_order",
            "payload": {
                "symbol": symbol,
                "side": "buy",
                "order_type": "limit",
                "qty": "0.001",
                "price": str(price),
                "magic_id": 999,
                "position_id": 0,
            },
        },
    )

    context = {
        "base_url": "http://127.0.0.1:8000",
        "user_id": user_id,
        "account_id": account_id,
        "internal_api_key": internal_api_key,
        "symbol": symbol,
        "smoke_command_result": cmd,
    }
    context_path = runtime_dir / "context.json"
    context_path.write_text(json.dumps(context, indent=2), encoding="utf-8")

    print("Testnet environment ready.")
    print(json.dumps(context, indent=2))
    print(f"context saved: {context_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
