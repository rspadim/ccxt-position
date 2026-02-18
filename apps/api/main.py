from typing import Annotated

from fastapi import Depends, FastAPI

from .app.auth import AuthContext, get_auth_context
from .app.config import load_settings
from .app.db_mysql import DatabaseMySQL
from .app.repository_mysql import MySQLCommandRepository
from .app.schemas import CommandInput, CommandsResponse
from .app.service import process_single_command

settings = load_settings()
app = FastAPI(title="ccxt-position", version="0.1.0")
app.state.db = None
app.state.repo = None


@app.on_event("startup")
async def on_startup() -> None:
    if settings.db_engine != "mysql":
        raise RuntimeError(
            f"db_engine={settings.db_engine!r} is not supported in v0; use mysql"
        )
    app.state.db = DatabaseMySQL(settings)
    app.state.repo = MySQLCommandRepository()
    await app.state.db.connect()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if app.state.db is not None:
        await app.state.db.disconnect()


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "env": settings.app_env,
        "db_engine": settings.db_engine,
    }


@app.post("/position/commands", response_model=CommandsResponse)
async def post_position_commands(
    commands: CommandInput | list[CommandInput],
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> CommandsResponse:
    items = commands if isinstance(commands, list) else [commands]
    results = []
    for index, item in enumerate(items):
        results.append(
            await process_single_command(app.state.db, app.state.repo, auth, item, index)
        )
    return CommandsResponse(results=results)
