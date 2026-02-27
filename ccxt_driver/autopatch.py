import argparse
import os
import site
import sys
from pathlib import Path
from typing import Iterable

from .oms_ccxt_exchange import OmsCcxtExchange

BLOCK_BEGIN = "# >>> ccxt_driver autopatch >>>"
BLOCK_END = "# <<< ccxt_driver autopatch <<<"


def _truthy(value: str | None) -> bool:
    v = str(value or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _default_base_url() -> str:
    return str(os.getenv("CCXT_OMS_BASE_URL", "http://127.0.0.1:8000")).rstrip("/")


def _default_api_key() -> str:
    return str(os.getenv("CCXT_OMS_API_KEY", "")).strip()


def _default_account_id() -> int:
    raw = str(os.getenv("CCXT_OMS_ACCOUNT_ID", "0")).strip()
    try:
        return int(raw)
    except Exception:
        return 0


def _default_strategy_id() -> int:
    raw = str(os.getenv("CCXT_OMS_STRATEGY_ID", "0")).strip()
    try:
        return int(raw)
    except Exception:
        return 0


def _parse_exchange_overrides(raw: str | Iterable[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        parts = raw.split(",")
    else:
        parts = [str(x) for x in raw]
    out: list[str] = []
    for part in parts:
        token = str(part or "").strip().lower()
        if not token:
            continue
        if token not in out:
            out.append(token)
    return out


def _coerce_constructor_kwargs(args: tuple, kwargs: dict) -> dict:
    merged: dict = {}
    if args and isinstance(args[0], dict):
        merged.update(args[0])
    merged.update(kwargs or {})
    return merged


def _build_exchange_from_kwargs(source: dict) -> OmsCcxtExchange:
    api_key = str(source.get("api_key") or source.get("apiKey") or _default_api_key()).strip()
    if not api_key:
        raise RuntimeError("ccxt_driver autopatch: missing api_key (set CCXT_OMS_API_KEY or pass api_key)")

    account_raw = source.get("account_id", source.get("accountId", _default_account_id()))
    try:
        account_id = int(account_raw)
    except Exception:
        account_id = 0
    if account_id <= 0:
        raise RuntimeError("ccxt_driver autopatch: missing account_id (set CCXT_OMS_ACCOUNT_ID or pass account_id)")

    strategy_raw = source.get("strategy_id", source.get("strategyId", _default_strategy_id()))
    try:
        strategy_id = int(strategy_raw)
    except Exception:
        strategy_id = 0

    base_url = str(source.get("base_url") or source.get("baseUrl") or _default_base_url()).rstrip("/")
    timeout_raw = source.get("timeout_seconds", source.get("timeout", 30))
    try:
        timeout_seconds = int(timeout_raw)
    except Exception:
        timeout_seconds = 30
    timeout_seconds = max(1, timeout_seconds)

    return OmsCcxtExchange(
        api_key=api_key,
        account_id=account_id,
        strategy_id=strategy_id,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )


def patch_ccxt(*, mode: str = "safe", overrides: str | Iterable[str] | None = None) -> bool:
    import ccxt  # type: ignore

    mode_norm = str(mode or "safe").strip().lower()
    if mode_norm not in {"safe", "aggressive"}:
        mode_norm = "safe"

    if getattr(ccxt, "_ccxt_oms_autopatched", False):
        return False

    def oms_exchange_factory(*args, **kwargs):
        merged = _coerce_constructor_kwargs(args, kwargs)
        return _build_exchange_from_kwargs(merged)

    setattr(ccxt, "oms_exchange", oms_exchange_factory)
    setattr(ccxt.Exchange, "oms_exchange", staticmethod(oms_exchange_factory))

    if mode_norm == "aggressive":
        targets = _parse_exchange_overrides(overrides or os.getenv("CCXT_OMS_OVERRIDE_EXCHANGES", ""))
        for ex_id in targets:
            if not hasattr(ccxt, ex_id):
                continue
            setattr(ccxt, ex_id, oms_exchange_factory)

    setattr(ccxt, "_ccxt_oms_autopatched", True)
    setattr(ccxt, "_ccxt_oms_autopatch_mode", mode_norm)
    return True


def apply_from_env() -> bool:
    if not _truthy(os.getenv("CCXT_OMS_AUTOPATCH", "0")):
        return False
    mode = str(os.getenv("CCXT_OMS_AUTOPATCH_MODE", "safe")).strip().lower() or "safe"
    overrides = os.getenv("CCXT_OMS_OVERRIDE_EXCHANGES", "")
    return patch_ccxt(mode=mode, overrides=overrides)


def _sitecustomize_path(scope: str) -> Path:
    scope_norm = str(scope or "user").strip().lower()
    if scope_norm == "user":
        return Path(site.getusersitepackages()) / "sitecustomize.py"
    if scope_norm in {"env", "venv"}:
        candidates = site.getsitepackages()
        if not candidates:
            raise RuntimeError("could not resolve environment site-packages")
        return Path(candidates[0]) / "sitecustomize.py"
    raise RuntimeError("scope must be 'user' or 'env'")


def _block_text(mode: str, overrides: str | None) -> str:
    mode_norm = str(mode or "safe").strip().lower()
    if mode_norm not in {"safe", "aggressive"}:
        mode_norm = "safe"
    overrides_text = ",".join(_parse_exchange_overrides(overrides or ""))
    return (
        f"{BLOCK_BEGIN}\n"
        "import os as _ccxt_oms_os\n"
        "_ccxt_oms_os.environ.setdefault('CCXT_OMS_AUTOPATCH', '1')\n"
        f"_ccxt_oms_os.environ.setdefault('CCXT_OMS_AUTOPATCH_MODE', '{mode_norm}')\n"
        f"_ccxt_oms_os.environ.setdefault('CCXT_OMS_OVERRIDE_EXCHANGES', '{overrides_text}')\n"
        "try:\n"
        "    import ccxt_driver.autopatch as _ccxt_oms_ap\n"
        "    _ccxt_oms_ap.apply_from_env()\n"
        "except Exception:\n"
        "    pass\n"
        f"{BLOCK_END}\n"
    )


def _remove_block(text: str) -> tuple[str, bool]:
    start = text.find(BLOCK_BEGIN)
    end = text.find(BLOCK_END)
    if start < 0 or end < 0 or end < start:
        return text, False
    end = end + len(BLOCK_END)
    while end < len(text) and text[end] in "\r\n":
        end += 1
    new_text = text[:start] + text[end:]
    return new_text, True


def install(*, scope: str = "user", mode: str = "safe", overrides: str | None = None) -> Path:
    path = _sitecustomize_path(scope)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    cleaned, _ = _remove_block(existing)
    block = _block_text(mode=mode, overrides=overrides)
    sep = "" if (not cleaned or cleaned.endswith("\n")) else "\n"
    final_text = f"{cleaned}{sep}{block}"
    path.write_text(final_text, encoding="utf-8")
    return path


def uninstall(*, scope: str = "user") -> tuple[Path, bool]:
    path = _sitecustomize_path(scope)
    if not path.exists():
        return path, False
    existing = path.read_text(encoding="utf-8")
    cleaned, removed = _remove_block(existing)
    if removed:
        path.write_text(cleaned, encoding="utf-8")
    return path, removed


def status(*, scope: str = "user") -> dict:
    path = _sitecustomize_path(scope)
    exists = path.exists()
    installed = False
    mode = None
    overrides = None
    if exists:
        text = path.read_text(encoding="utf-8")
        installed = BLOCK_BEGIN in text and BLOCK_END in text
        if installed:
            if "CCXT_OMS_AUTOPATCH_MODE" in text:
                if "'aggressive'" in text:
                    mode = "aggressive"
                elif "'safe'" in text:
                    mode = "safe"
            marker = "CCXT_OMS_OVERRIDE_EXCHANGES', '"
            idx = text.find(marker)
            if idx >= 0:
                tail = text[idx + len(marker) :]
                end = tail.find("'")
                if end >= 0:
                    overrides = tail[:end]
    return {
        "scope": scope,
        "sitecustomize": str(path),
        "exists": exists,
        "installed": installed,
        "mode": mode,
        "overrides": overrides,
    }


def _print_status(payload: dict) -> None:
    print(f"scope={payload['scope']}")
    print(f"sitecustomize={payload['sitecustomize']}")
    print(f"exists={payload['exists']}")
    print(f"installed={payload['installed']}")
    if payload.get("mode"):
        print(f"mode={payload['mode']}")
    if payload.get("overrides") is not None:
        print(f"overrides={payload['overrides']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install/uninstall ccxt auto patch via sitecustomize.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_install = sub.add_parser("install", help="Install auto patch block in sitecustomize.py")
    p_install.add_argument("--scope", choices=["user", "env"], default="user")
    p_install.add_argument("--mode", choices=["safe", "aggressive"], default="safe")
    p_install.add_argument("--override", default="", help="comma-separated exchange ids for aggressive mode")

    p_uninstall = sub.add_parser("uninstall", help="Remove auto patch block from sitecustomize.py")
    p_uninstall.add_argument("--scope", choices=["user", "env"], default="user")

    p_status = sub.add_parser("status", help="Show auto patch installation status")
    p_status.add_argument("--scope", choices=["user", "env"], default="user")

    args = parser.parse_args(argv)

    if args.cmd == "install":
        path = install(scope=args.scope, mode=args.mode, overrides=args.override)
        print(f"installed: {path}")
        _print_status(status(scope=args.scope))
        return 0
    if args.cmd == "uninstall":
        path, removed = uninstall(scope=args.scope)
        print(f"uninstall: {path} removed={removed}")
        _print_status(status(scope=args.scope))
        return 0
    if args.cmd == "status":
        _print_status(status(scope=args.scope))
        return 0

    print("unknown command", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

