import json
import os
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "ccxt-position"
    app_env: str = "dev"
    db_engine: str = "mysql"
    mysql_driver: str = "asyncmy"
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "ccxt_position"
    mysql_min_pool_size: int = 1
    mysql_max_pool_size: int = 10
    worker_id: str = "worker-position-0"
    worker_pool_id: int = 0
    worker_poll_interval_ms: int = 1000
    worker_max_attempts: int = 5
    worker_reconciliation_interval_seconds: int = 30
    disable_uvicorn_access_log: bool = True
    app_request_log: bool = True
    encryption_master_key: str = ""
    require_encrypted_credentials: bool = True
    log_dir: str = "logs"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")


def _flatten_sectioned_config(data: dict[str, Any]) -> dict[str, Any]:
    # Backward-compatible: flat keys keep working. Sectioned keys override/define fields.
    out = dict(data)

    app = data.get("app", {})
    # `api.*` is legacy. Keep parsing for backward compatibility.
    api = data.get("api", {})
    database = data.get("database", {})
    worker = data.get("worker", {})
    logging_cfg = data.get("logging", {})
    security = data.get("security", {})

    if isinstance(app, dict):
        if "name" in app:
            out["app_name"] = app["name"]
        if "env" in app:
            out["app_env"] = app["env"]
        if "db_engine" in app:
            out["db_engine"] = app["db_engine"]

    if isinstance(database, dict):
        if "engine" in database:
            out["db_engine"] = database["engine"]
        if "mysql_driver" in database:
            out["mysql_driver"] = database["mysql_driver"]
        if "mysql_host" in database:
            out["mysql_host"] = database["mysql_host"]
        if "mysql_port" in database:
            out["mysql_port"] = database["mysql_port"]
        if "mysql_user" in database:
            out["mysql_user"] = database["mysql_user"]
        if "mysql_password" in database:
            out["mysql_password"] = database["mysql_password"]
        if "mysql_database" in database:
            out["mysql_database"] = database["mysql_database"]
        if "mysql_min_pool_size" in database:
            out["mysql_min_pool_size"] = database["mysql_min_pool_size"]
        if "mysql_max_pool_size" in database:
            out["mysql_max_pool_size"] = database["mysql_max_pool_size"]

    if isinstance(api, dict):
        if "disable_uvicorn_access_log" in api:
            out["disable_uvicorn_access_log"] = api["disable_uvicorn_access_log"]
        if "app_request_log" in api:
            out["app_request_log"] = api["app_request_log"]

    if isinstance(worker, dict):
        if "worker_id" in worker:
            out["worker_id"] = worker["worker_id"]
        if "pool_id" in worker:
            out["worker_pool_id"] = worker["pool_id"]
        if "poll_interval_ms" in worker:
            out["worker_poll_interval_ms"] = worker["poll_interval_ms"]
        if "max_attempts" in worker:
            out["worker_max_attempts"] = worker["max_attempts"]
        if "reconciliation_interval_seconds" in worker:
            out["worker_reconciliation_interval_seconds"] = worker[
                "reconciliation_interval_seconds"
            ]

    if isinstance(logging_cfg, dict):
        if "log_dir" in logging_cfg:
            out["log_dir"] = logging_cfg["log_dir"]
        if "disable_uvicorn_access_log" in logging_cfg:
            out["disable_uvicorn_access_log"] = logging_cfg["disable_uvicorn_access_log"]
        if "app_request_log" in logging_cfg:
            out["app_request_log"] = logging_cfg["app_request_log"]

    if isinstance(security, dict):
        if "encryption_master_key" in security:
            out["encryption_master_key"] = security["encryption_master_key"]
        if "require_encrypted_credentials" in security:
            out["require_encrypted_credentials"] = security["require_encrypted_credentials"]

    return out


def load_settings() -> Settings:
    config_path = Path(
        os.environ.get("CONFIG_JSON_PATH", "apps/api/config.json")
    )
    if config_path.exists():
        data = json.loads(config_path.read_text(encoding="utf-8"))
        return Settings(**_flatten_sectioned_config(data))
    return Settings()
