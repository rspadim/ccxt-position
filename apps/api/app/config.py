import json
import os
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "ccxt-position"
    app_env: str = "dev"
    db_engine: str = "mysql"
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "ccxt_position"
    mysql_min_pool_size: int = 1
    mysql_max_pool_size: int = 10

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")


def load_settings() -> Settings:
    config_path = Path(
        os.environ.get("CONFIG_JSON_PATH", "apps/api/config.json")
    )
    if config_path.exists():
        data = json.loads(config_path.read_text(encoding="utf-8"))
        return Settings(**data)
    return Settings()
