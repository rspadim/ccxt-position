import logging
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any


SENSITIVE_HEADERS = {"x-api-key", "authorization", "cookie", "set-cookie"}


def mask_header_value(key: str, value: str | None) -> str:
    if value is None:
        return ""
    if key.lower() in SENSITIVE_HEADERS:
        return "***"
    return value


def _build_logger(name: str, file_path: Path) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers = []
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = TimedRotatingFileHandler(
        filename=file_path,
        when="midnight",
        interval=1,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def setup_application_logging(
    disable_uvicorn_access_log: bool, log_dir: str = "logs"
) -> dict[str, logging.Logger]:
    base = Path(log_dir)
    base.mkdir(parents=True, exist_ok=True)

    loggers = {
        "api": _build_logger("ccxt_position.api", base / "api.log"),
        "ccxt": _build_logger("ccxt_position.ccxt", base / "ccxt.log"),
        "position": _build_logger("ccxt_position.position", base / "position.log"),
    }

    if disable_uvicorn_access_log:
        logging.getLogger("uvicorn.access").disabled = True

    return loggers


def build_file_logger(name: str, file_path: str | Path) -> logging.Logger:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return _build_logger(name, path)


def http_log_payload(
    method: str, path: str, status_code: int, elapsed_s: float, account_id: str | None
) -> dict[str, Any]:
    return {
        "method": method,
        "path": path,
        "status_code": status_code,
        "elapsed_ms": round(elapsed_s * 1000, 2),
        "account_id": account_id,
    }


def now() -> float:
    return time.perf_counter()
