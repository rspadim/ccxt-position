import asyncio
import contextlib
import json
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect

from .app.auth import AuthContext, get_auth_context, validate_api_key
from .app.ccxt_adapter import CCXTAdapter
from .app.config import load_settings
from .app.db_mysql import DatabaseMySQL
from .app.repository_mysql import MySQLCommandRepository
from .app.schemas import (
    CcxtBatchItem,
    CcxtBatchResponse,
    CcxtCallInput,
    CommandInput,
    CommandsResponse,
    ReassignInput,
)
from .app.service import process_single_command

settings = load_settings()
app = FastAPI(title="ccxt-position", version="0.1.0")
app.state.db = None
app.state.repo = None
app.state.ccxt = None


@app.on_event("startup")
async def on_startup() -> None:
    if settings.db_engine != "mysql":
        raise RuntimeError(
            f"db_engine={settings.db_engine!r} is not supported in v0; use mysql"
        )
    app.state.db = DatabaseMySQL(settings)
    app.state.repo = MySQLCommandRepository()
    app.state.ccxt = CCXTAdapter()
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


async def _require_account_permission(user_id: int, account_id: int, require_trade: bool = False) -> dict[str, Any]:
    async with app.state.db.connection() as conn:
        account = await app.state.repo.fetch_account_by_id(conn, account_id)
        if account is None or account["status"] != "active":
            raise HTTPException(status_code=404, detail={"code": "account_not_found"})
        perms = await app.state.repo.fetch_permissions(conn, user_id, account_id)
        await conn.commit()
    if perms is None or not bool(perms[0]):
        raise HTTPException(status_code=403, detail={"code": "permission_denied"})
    if require_trade and not bool(perms[1]):
        raise HTTPException(status_code=403, detail={"code": "permission_denied"})
    return account


def _ccxt_requires_trade(func: str) -> bool:
    fn = func.lower()
    trade_prefixes = (
        "create_",
        "cancel_",
        "edit_",
        "private_post",
        "private_put",
        "private_delete",
    )
    return fn.startswith(trade_prefixes)


@app.post("/ccxt/{account_id}/{func}")
async def post_ccxt_call(
    account_id: int,
    func: str,
    request: CcxtCallInput,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    account = await _require_account_permission(
        auth.user_id, account_id, require_trade=_ccxt_requires_trade(func)
    )
    async with app.state.db.connection() as conn:
        _, api_key, secret, passphrase = await app.state.repo.fetch_account_exchange_credentials(
            conn, account_id
        )
        await conn.commit()
    result = await app.state.ccxt.execute_method(
        exchange_id=account["exchange_id"],
        api_key=api_key,
        secret=secret,
        passphrase=passphrase,
        method=func,
        args=request.args,
        kwargs=request.kwargs,
    )
    return {"ok": True, "result": result}


@app.post("/ccxt/multiple_commands", response_model=CcxtBatchResponse)
async def post_ccxt_batch(
    items: list[CcxtBatchItem],
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> CcxtBatchResponse:
    results: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        try:
            account = await _require_account_permission(
                auth.user_id,
                item.account_id,
                require_trade=_ccxt_requires_trade(item.func),
            )
            async with app.state.db.connection() as conn:
                _, api_key, secret, passphrase = await app.state.repo.fetch_account_exchange_credentials(
                    conn, item.account_id
                )
                await conn.commit()
            result = await app.state.ccxt.execute_method(
                exchange_id=account["exchange_id"],
                api_key=api_key,
                secret=secret,
                passphrase=passphrase,
                method=item.func,
                args=item.args,
                kwargs=item.kwargs,
            )
            results.append({"index": index, "ok": True, "result": result})
        except Exception as exc:  # pragma: no cover
            results.append({"index": index, "ok": False, "error": {"message": str(exc)}})
    return CcxtBatchResponse(results=results)


@app.get("/position/orders/open")
async def get_position_orders_open(
    account_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    await _require_account_permission(auth.user_id, account_id, require_trade=False)
    async with app.state.db.connection() as conn:
        rows = await app.state.repo.list_orders(conn, account_id, open_only=True)
        await conn.commit()
    return {"items": rows}


@app.get("/position/orders/history")
async def get_position_orders_history(
    account_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    await _require_account_permission(auth.user_id, account_id, require_trade=False)
    async with app.state.db.connection() as conn:
        rows = await app.state.repo.list_orders(conn, account_id, open_only=False)
        await conn.commit()
    return {"items": rows}


@app.get("/position/deals")
async def get_position_deals(
    account_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    await _require_account_permission(auth.user_id, account_id, require_trade=False)
    async with app.state.db.connection() as conn:
        rows = await app.state.repo.list_deals(conn, account_id)
        await conn.commit()
    return {"items": rows}


@app.get("/position/positions/open")
async def get_position_positions_open(
    account_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    await _require_account_permission(auth.user_id, account_id, require_trade=False)
    async with app.state.db.connection() as conn:
        rows = await app.state.repo.list_positions(conn, account_id, open_only=True)
        await conn.commit()
    return {"items": rows}


@app.get("/position/positions/history")
async def get_position_positions_history(
    account_id: int,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    await _require_account_permission(auth.user_id, account_id, require_trade=False)
    async with app.state.db.connection() as conn:
        rows = await app.state.repo.list_positions(conn, account_id, open_only=False)
        await conn.commit()
    return {"items": rows}


@app.post("/position/reassign")
async def post_position_reassign(
    req: ReassignInput,
    auth: Annotated[AuthContext, Depends(get_auth_context)],
) -> dict[str, Any]:
    await _require_account_permission(auth.user_id, req.account_id, require_trade=True)
    async with app.state.db.connection() as conn:
        deals_count = await app.state.repo.reassign_deals(
            conn=conn,
            account_id=req.account_id,
            deal_ids=req.deal_ids,
            target_magic_id=req.target_magic_id,
            target_position_id=req.target_position_id,
        )
        orders_count = await app.state.repo.reassign_orders(
            conn=conn,
            account_id=req.account_id,
            order_ids=req.order_ids,
            target_magic_id=req.target_magic_id,
            target_position_id=req.target_position_id,
        )
        await app.state.repo.insert_event(
            conn=conn,
            account_id=req.account_id,
            namespace="position",
            event_type="reassigned",
            payload={
                "deals_updated": deals_count,
                "orders_updated": orders_count,
                "target_magic_id": req.target_magic_id,
                "target_position_id": req.target_position_id,
            },
        )
        await conn.commit()
    return {"ok": True, "deals_updated": deals_count, "orders_updated": orders_count}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    api_key = websocket.query_params.get("api_key")
    account_id_raw = websocket.query_params.get("account_id")
    if not api_key or not account_id_raw:
        await websocket.close(code=1008)
        return

    try:
        account_id = int(account_id_raw)
    except ValueError:
        await websocket.close(code=1008)
        return

    auth = await validate_api_key(app.state.db, api_key)
    if auth is None:
        await websocket.close(code=1008)
        return

    try:
        account = await _require_account_permission(auth.user_id, account_id, require_trade=False)
    except HTTPException:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    subscriptions = {"position", "ccxt"}
    last_event_id = int(websocket.query_params.get("after_id", "0") or 0)
    await websocket.send_json(
        {"id": "server-hello", "ok": True, "type": "ws_event", "event": "connected", "payload": {"account_id": account_id}}
    )

    while True:
        try:
            try:
                text = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                msg = json.loads(text)
                req_id = msg.get("id")
                namespace = str(msg.get("namespace", "")).strip()
                action = str(msg.get("action", "")).strip()
                payload = msg.get("payload") if isinstance(msg.get("payload"), dict) else {}

                if action == "ping":
                    await websocket.send_json(
                        {"id": req_id, "ok": True, "type": "ws_response", "namespace": namespace, "action": action, "event": "pong", "payload": {}}
                    )
                    continue

                if action == "subscribe":
                    namespaces = payload.get("namespaces", [])
                    if isinstance(namespaces, list):
                        subscriptions = {str(n) for n in namespaces if str(n) in {"position", "ccxt"}}
                        if not subscriptions:
                            subscriptions = {"position"}
                    await websocket.send_json(
                        {"id": req_id, "ok": True, "type": "ws_response", "namespace": "system", "action": action, "event": "subscribed", "payload": {"namespaces": sorted(subscriptions)}}
                    )
                    continue

                if namespace == "position" and action == "command":
                    command_payload = dict(payload)
                    command_payload["account_id"] = account_id
                    item = CommandInput.model_validate(command_payload)
                    result = await process_single_command(
                        app.state.db, app.state.repo, auth, item, 0
                    )
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": result.ok,
                            "type": "ws_response",
                            "namespace": "position",
                            "action": action,
                            "event": "command_result",
                            "payload": result.model_dump(),
                        }
                    )
                    continue

                if namespace == "ccxt" and action == "call":
                    method = str(payload.get("func", "")).strip()
                    args = payload.get("args") if isinstance(payload.get("args"), list) else []
                    kwargs = payload.get("kwargs") if isinstance(payload.get("kwargs"), dict) else {}
                    async with app.state.db.connection() as conn:
                        _, api_key_val, secret, passphrase = await app.state.repo.fetch_account_exchange_credentials(
                            conn, account_id
                        )
                        await conn.commit()
                    result = await app.state.ccxt.execute_method(
                        exchange_id=account["exchange_id"],
                        api_key=api_key_val,
                        secret=secret,
                        passphrase=passphrase,
                        method=method,
                        args=args,
                        kwargs=kwargs,
                    )
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": True,
                            "type": "ws_response",
                            "namespace": "ccxt",
                            "action": action,
                            "event": "ccxt_result",
                            "payload": {"result": result},
                        }
                    )
                    continue

                await websocket.send_json(
                    {
                        "id": req_id,
                        "ok": False,
                        "type": "ws_response",
                        "namespace": namespace,
                        "action": action,
                        "event": "error",
                        "payload": {"code": "unsupported_action"},
                    }
                )
            except asyncio.TimeoutError:
                pass

            async with app.state.db.connection() as conn:
                events = await app.state.repo.fetch_outbox_events(conn, account_id, last_event_id, limit=100)
                await conn.commit()
            for ev in events:
                if ev["namespace"] not in subscriptions:
                    last_event_id = max(last_event_id, int(ev["id"]))
                    continue
                last_event_id = max(last_event_id, int(ev["id"]))
                await websocket.send_json(
                    {
                        "id": None,
                        "ok": True,
                        "type": "ws_event",
                        "namespace": ev["namespace"],
                        "action": "event",
                        "event": ev["event_type"],
                        "payload": ev["payload"],
                    }
                )
        except WebSocketDisconnect:
            return
        except Exception:
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {"id": None, "ok": False, "type": "ws_response", "event": "error", "payload": {"code": "internal_error"}}
                )
