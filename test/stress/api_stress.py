import argparse
import json
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


@dataclass(frozen=True)
class RequestCase:
    name: str
    method: str
    path: str
    requires_auth: bool
    body: dict[str, Any] | None = None
    weight: int = 1


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if q <= 0:
        return float(min(values))
    if q >= 1:
        return float(max(values))
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return float(ordered[lo] * (1 - frac) + ordered[hi] * frac)


def http_call(
    *,
    base_url: str,
    case: RequestCase,
    api_key: str | None,
    timeout_s: float,
) -> tuple[bool, int, float, str]:
    url = f"{base_url.rstrip('/')}{case.path}"
    headers = {"Content-Type": "application/json"}
    if case.requires_auth and api_key:
        headers["x-api-key"] = api_key
    payload = None
    if case.body is not None:
        payload = json.dumps(case.body).encode("utf-8")
    req = urllib_request.Request(url=url, data=payload, headers=headers, method=case.method.upper())
    started = time.perf_counter()
    try:
        with urllib_request.urlopen(req, timeout=timeout_s) as resp:
            _ = resp.read()
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            status_code = int(getattr(resp, "status", 200) or 200)
            ok = 200 <= status_code < 300
            return ok, status_code, elapsed_ms, ""
    except urllib_error.HTTPError as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        status_code = int(getattr(exc, "code", 0) or 0)
        raw = exc.read().decode("utf-8", errors="replace")
        err = f"http_{status_code}:{raw[:180]}"
        return False, status_code, elapsed_ms, err
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return False, 0, elapsed_ms, str(exc)


def discover_account_ids(base_url: str, api_key: str, timeout_s: float, limit: int) -> list[int]:
    query = urllib_parse.urlencode({"page": 1, "page_size": max(1, min(500, int(limit)))})
    case = RequestCase(
        name="discover_admin_accounts",
        method="GET",
        path=f"/admin/accounts?{query}",
        requires_auth=True,
    )
    ok, status, _lat, _err = http_call(base_url=base_url, case=case, api_key=api_key, timeout_s=timeout_s)
    if not ok or status < 200 or status >= 300:
        return []
    url = f"{base_url.rstrip('/')}{case.path}"
    req = urllib_request.Request(url=url, headers={"x-api-key": api_key}, method="GET")
    with urllib_request.urlopen(req, timeout=timeout_s) as resp:
        data = json.loads(resp.read().decode("utf-8") or "{}")
    rows = data.get("items", [])
    out: list[int] = []
    for row in rows if isinstance(rows, list) else []:
        try:
            aid = int(row.get("id", 0) or 0)
        except Exception:
            aid = 0
        if aid > 0:
            out.append(aid)
    return sorted(set(out))


def _collect_account_ids_from_obj(value: Any, out: set[int]) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            if str(k) == "account_id":
                try:
                    aid = int(v or 0)
                except Exception:
                    aid = 0
                if aid > 0:
                    out.add(aid)
            _collect_account_ids_from_obj(v, out)
        return
    if isinstance(value, list):
        for item in value:
            _collect_account_ids_from_obj(item, out)


def discover_account_ids_from_user_permissions(base_url: str, api_key: str, timeout_s: float) -> list[int]:
    url = f"{base_url.rstrip('/')}/user/api-keys/permissions"
    req = urllib_request.Request(url=url, headers={"x-api-key": api_key}, method="GET")
    try:
        with urllib_request.urlopen(req, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
    except Exception:
        return []
    ids: set[int] = set()
    _collect_account_ids_from_obj(data, ids)
    return sorted(ids)


def discover_account_ids_from_runtime_files(root: Path) -> list[int]:
    ids: set[int] = set()
    candidates = [
        root / "testnet" / "runtime" / "context.json",
        root / "testnet" / "runtime" / "scenarios.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        _collect_account_ids_from_obj(data, ids)
        # Also accept common shorthand keys in top-level blocks.
        if isinstance(data, dict):
            for key in ("account_id", "account_a", "account_b"):
                try:
                    aid = int(data.get(key, 0) or 0)
                except Exception:
                    aid = 0
                if aid > 0:
                    ids.add(aid)
    return sorted(ids)


def build_cases(account_ids: list[int], symbol: str) -> list[RequestCase]:
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    aid_csv = ",".join(str(x) for x in account_ids)
    first_account = int(account_ids[0]) if account_ids else 0
    return [
        RequestCase("healthz", "GET", "/healthz", False, None, weight=2),
        RequestCase("dispatcher_status", "GET", "/dispatcher/status", True, None, weight=2),
        RequestCase("user_profile", "GET", "/user/profile", True, None, weight=2),
        RequestCase(
            "oms_orders_open",
            "GET",
            f"/oms/orders/open?account_ids={aid_csv}&page=1&page_size=100",
            True,
            None,
            weight=4,
        ),
        RequestCase(
            "oms_positions_open",
            "GET",
            f"/oms/positions/open?account_ids={aid_csv}&page=1&page_size=100",
            True,
            None,
            weight=4,
        ),
        RequestCase(
            "oms_deals",
            "GET",
            (
                f"/oms/deals?account_ids={aid_csv}&start_date={yesterday.isoformat()}"
                f"&end_date={today.isoformat()}&page=1&page_size=100"
            ),
            True,
            None,
            weight=3,
        ),
        RequestCase(
            "ccxt_fetch_ticker",
            "POST",
            f"/ccxt/{first_account}/fetch_ticker",
            True,
            {"args": [symbol], "kwargs": {}},
            weight=3,
        ),
    ]


def build_db_only_cases(account_ids: list[int]) -> list[RequestCase]:
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    aid_csv = ",".join(str(x) for x in account_ids)
    return [
        RequestCase(
            "oms_orders_open",
            "GET",
            f"/oms/orders/open?account_ids={aid_csv}&page=1&page_size=100",
            True,
            None,
            weight=5,
        ),
        RequestCase(
            "oms_positions_open",
            "GET",
            f"/oms/positions/open?account_ids={aid_csv}&page=1&page_size=100",
            True,
            None,
            weight=5,
        ),
        RequestCase(
            "oms_deals",
            "GET",
            (
                f"/oms/deals?account_ids={aid_csv}&start_date={yesterday.isoformat()}"
                f"&end_date={today.isoformat()}&page=1&page_size=100"
            ),
            True,
            None,
            weight=5,
        ),
    ]


def choose_case(cases: list[RequestCase]) -> RequestCase:
    weighted = [max(1, int(c.weight)) for c in cases]
    return random.choices(cases, weights=weighted, k=1)[0]


def write_report_md(path: Path, report: dict[str, Any]) -> None:
    cfg = report.get("config", {})
    summary = report.get("summary", {})
    per_endpoint = report.get("per_endpoint", {})
    errors = report.get("top_errors", [])
    lines: list[str] = []
    lines.append("# API Stress Report")
    lines.append("")
    lines.append(f"- Generated at (UTC): `{report.get('generated_at')}`")
    lines.append(f"- Base URL: `{cfg.get('base_url')}`")
    lines.append(f"- Duration: `{cfg.get('duration_seconds')}s`")
    lines.append(f"- Concurrency: `{cfg.get('concurrency')}`")
    lines.append(f"- Timeout per request: `{cfg.get('request_timeout_seconds')}s`")
    lines.append(f"- Account IDs: `{','.join(str(x) for x in cfg.get('account_ids', []))}`")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    lines.append(f"| Total requests | {summary.get('total_requests', 0)} |")
    lines.append(f"| Success requests | {summary.get('success_requests', 0)} |")
    lines.append(f"| Error requests | {summary.get('error_requests', 0)} |")
    lines.append(f"| Error rate | {summary.get('error_rate_pct', 0):.2f}% |")
    lines.append(f"| Throughput | {summary.get('throughput_rps', 0):.2f} req/s |")
    lines.append(f"| Latency p50 | {summary.get('latency_p50_ms', 0):.2f} ms |")
    lines.append(f"| Latency p95 | {summary.get('latency_p95_ms', 0):.2f} ms |")
    lines.append(f"| Latency p99 | {summary.get('latency_p99_ms', 0):.2f} ms |")
    lines.append("")
    lines.append("## Endpoint Breakdown")
    lines.append("")
    lines.append("| Endpoint | Requests | Errors | Error % | p50 ms | p95 ms | p99 ms |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for name, row in sorted(per_endpoint.items(), key=lambda kv: kv[0]):
        lines.append(
            f"| `{name}` | {row.get('requests', 0)} | {row.get('errors', 0)} | "
            f"{row.get('error_rate_pct', 0):.2f}% | {row.get('p50_ms', 0):.2f} | "
            f"{row.get('p95_ms', 0):.2f} | {row.get('p99_ms', 0):.2f} |"
        )
    lines.append("")
    lines.append("## Top Errors")
    lines.append("")
    if not errors:
        lines.append("- No errors captured.")
    else:
        for item in errors:
            lines.append(f"- `{item.get('error')}` x `{item.get('count')}`")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This is API-level stress (HTTP), not exchange fill-capacity benchmarking.")
    lines.append("- For trading endpoints, exchange-side throttling and testnet behavior affect results.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stress/load test against ccxt-position API.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--account-ids", default="", help="CSV account IDs. Empty = auto discover via /admin/accounts.")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--duration-seconds", type=int, default=30)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--request-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-requests", type=int, default=0, help="0 = unlimited during duration.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--db-only", action="store_true", help="Run only OMS DB-backed query endpoints.")
    args = parser.parse_args()

    random.seed(int(args.seed))
    started_at = time.perf_counter()
    generated_at = now_iso()
    runtime_dir = Path(__file__).resolve().parent / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    api_key = str(args.api_key or "").strip()
    if not api_key:
        env_file = Path(__file__).resolve().parents[1] / "testnet" / ".env.testnet"
        if env_file.exists():
            for raw in env_file.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k.strip() == "INTERNAL_API_KEY":
                    api_key = v.strip()
                    break
    if not api_key:
        raise RuntimeError("missing api key: provide --api-key or set INTERNAL_API_KEY in test/testnet/.env.testnet")

    stress_root = Path(__file__).resolve().parent
    if str(args.account_ids or "").strip():
        account_ids = [int(x.strip()) for x in str(args.account_ids).split(",") if x.strip().isdigit() and int(x.strip()) > 0]
        account_ids = sorted(set(account_ids))
    else:
        account_ids = discover_account_ids(
            base_url=str(args.base_url),
            api_key=api_key,
            timeout_s=float(args.request_timeout_seconds),
            limit=200,
        )
        if not account_ids:
            account_ids = discover_account_ids_from_user_permissions(
                base_url=str(args.base_url),
                api_key=api_key,
                timeout_s=float(args.request_timeout_seconds),
            )
        if not account_ids:
            account_ids = discover_account_ids_from_runtime_files(stress_root.parent)
    if not account_ids:
        raise RuntimeError("no account_ids available for stress test")

    cases = build_db_only_cases(account_ids=account_ids) if bool(args.db_only) else build_cases(
        account_ids=account_ids,
        symbol=str(args.symbol),
    )
    max_requests = max(0, int(args.max_requests))
    deadline = time.perf_counter() + max(1, int(args.duration_seconds))

    lock = threading.Lock()
    latencies_all: list[float] = []
    stats: dict[str, dict[str, Any]] = {c.name: {"requests": 0, "errors": 0, "latencies": []} for c in cases}
    error_counts: dict[str, int] = {}
    total_requests = 0

    def worker_loop() -> None:
        nonlocal total_requests
        while True:
            now = time.perf_counter()
            if now >= deadline:
                return
            with lock:
                if max_requests > 0 and total_requests >= max_requests:
                    return
                total_requests += 1
            case = choose_case(cases)
            ok, status_code, latency_ms, err = http_call(
                base_url=str(args.base_url),
                case=case,
                api_key=api_key,
                timeout_s=float(args.request_timeout_seconds),
            )
            with lock:
                row = stats[case.name]
                row["requests"] += 1
                row["latencies"].append(latency_ms)
                latencies_all.append(latency_ms)
                if not ok:
                    row["errors"] += 1
                    key = err or f"http_{status_code}"
                    error_counts[key] = int(error_counts.get(key, 0)) + 1

    threads = [threading.Thread(target=worker_loop, daemon=True) for _ in range(max(1, int(args.concurrency)))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    elapsed_s = max(0.001, time.perf_counter() - started_at)
    success_requests = sum(int(r["requests"]) - int(r["errors"]) for r in stats.values())
    error_requests = sum(int(r["errors"]) for r in stats.values())
    summary = {
        "total_requests": int(sum(int(r["requests"]) for r in stats.values())),
        "success_requests": int(success_requests),
        "error_requests": int(error_requests),
        "error_rate_pct": (float(error_requests) / max(1, int(sum(int(r["requests"]) for r in stats.values())))) * 100.0,
        "throughput_rps": float(sum(int(r["requests"]) for r in stats.values())) / elapsed_s,
        "latency_p50_ms": percentile(latencies_all, 0.50),
        "latency_p95_ms": percentile(latencies_all, 0.95),
        "latency_p99_ms": percentile(latencies_all, 0.99),
        "elapsed_seconds": elapsed_s,
    }

    per_endpoint: dict[str, dict[str, Any]] = {}
    for name, row in stats.items():
        reqs = int(row["requests"])
        errs = int(row["errors"])
        lats = list(row["latencies"])
        per_endpoint[name] = {
            "requests": reqs,
            "errors": errs,
            "error_rate_pct": (float(errs) / max(1, reqs)) * 100.0,
            "p50_ms": percentile(lats, 0.50),
            "p95_ms": percentile(lats, 0.95),
            "p99_ms": percentile(lats, 0.99),
        }

    top_errors = sorted(
        ({"error": k, "count": v} for k, v in error_counts.items()),
        key=lambda x: int(x["count"]),
        reverse=True,
    )[:15]

    report = {
        "generated_at": generated_at,
        "config": {
            "base_url": str(args.base_url),
            "duration_seconds": int(args.duration_seconds),
            "concurrency": int(args.concurrency),
            "request_timeout_seconds": float(args.request_timeout_seconds),
            "max_requests": int(max_requests),
            "symbol": str(args.symbol),
            "account_ids": account_ids,
            "cases": [{"name": c.name, "weight": c.weight, "path": c.path, "method": c.method} for c in cases],
        },
        "summary": summary,
        "per_endpoint": per_endpoint,
        "top_errors": top_errors,
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    json_path = runtime_dir / f"stress-report-{stamp}.json"
    md_path = runtime_dir / f"stress-report-{stamp}.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    write_report_md(md_path, report)

    print(json.dumps({"summary": summary, "json_report": str(json_path), "md_report": str(md_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
