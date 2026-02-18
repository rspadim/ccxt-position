from apps.api.app.config import _flatten_sectioned_config


def test_flatten_sectioned_config_maps_sections() -> None:
    raw = {
        "app": {"name": "ccxt-position", "env": "prod", "db_engine": "mysql"},
        "database": {
            "mysql_host": "db",
            "mysql_port": 3307,
            "mysql_user": "u",
            "mysql_password": "p",
            "mysql_database": "d",
            "mysql_min_pool_size": 2,
            "mysql_max_pool_size": 20,
        },
        "worker": {
            "worker_id": "w1",
            "pool_id": 3,
            "poll_interval_ms": 250,
            "max_attempts": 9,
            "reconciliation_interval_seconds": 45,
        },
        "logging": {"log_dir": "logs-dev", "disable_uvicorn_access_log": True},
        "security": {"encryption_master_key": "abc"},
    }
    out = _flatten_sectioned_config(raw)
    assert out["app_name"] == "ccxt-position"
    assert out["app_env"] == "prod"
    assert out["mysql_host"] == "db"
    assert out["worker_pool_id"] == 3
    assert out["worker_poll_interval_ms"] == 250
    assert out["log_dir"] == "logs-dev"
    assert out["encryption_master_key"] == "abc"

