import asyncio
import contextlib
import json
from typing import Any
import ccxt.async_support as ccxt_async

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from pydantic import TypeAdapter

from .app.config import load_settings
from .app.dispatcher_client import dispatch_request
from .app.logging_utils import (
    http_log_payload,
    mask_header_value,
    now,
    setup_application_logging,
)
from .app.schemas import (
    AdminCreateAccountInput,
    AdminAccountsResponse,
    AdminCreateAccountResponse,
    AdminUpdateAccountInput,
    AdminUpdateAccountResponse,
    AdminUsersResponse,
    AdminCreateStrategyInput,
    AdminCreateStrategyResponse,
    AdminCreateUserApiKeyInput,
    AdminUsersApiKeysResponse,
    AdminCreateUserApiKeyResponse,
    AdminUpdateApiKeyInput,
    AdminUpdateApiKeyResponse,
    AdminApiKeyPermissionsResponse,
    AdminUpsertApiKeyPermissionInput,
    AdminStrategiesResponse,
    AdminUpdateStrategyInput,
    AdminUpdateStrategyResponse,
    CreateStrategyInput,
    CreateStrategyResponse,
    StrategiesResponse,
    AccountsResponse,
    CcxtCoreCancelOrderInput,
    CcxtCoreCreateOrderInput,
    CcxtCoreFetchBalanceInput,
    CcxtCoreFetchOpenOrdersInput,
    CcxtCoreFetchOrderInput,
    CcxtResponse,
    CcxtBatchItem,
    CcxtBatchResponse,
    CcxtCallInput,
    AuthLoginPasswordInput,
    AuthLoginPasswordResponse,
    CcxtExchangesResponse,
    CommandInput,
    CommandsResponse,
    PositionDealsResponse,
    PositionOrdersResponse,
    PositionsResponse,
    ReconcileNowInput,
    ReconcileNowResponse,
    ReconcileStatusResponse,
    ReassignResponse,
    ReassignInput,
    RiskActionResponse,
    RiskSetAccountStatusInput,
    RiskSetAllowNewPositionsInput,
    RiskSetStrategyAllowNewPositionsInput,
)

settings = load_settings()
app = FastAPI(title="ccxt-position", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.state.loggers = {}
COMMAND_INPUT_ADAPTER = TypeAdapter(CommandInput)


def custom_openapi() -> dict[str, Any]:
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
    )
    components = schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes["ApiKeyAuth"] = {
        "type": "apiKey",
        "in": "header",
        "name": "x-api-key",
    }
    for path, methods in schema.get("paths", {}).items():
        if path == "/healthz":
            continue
        for _method, operation in methods.items():
            if isinstance(operation, dict):
                operation.setdefault("security", [{"ApiKeyAuth": []}])
    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = custom_openapi


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next: Any) -> Any:
    start = now()
    response = await call_next(request)
    api_logger = app.state.loggers.get("api")
    if settings.app_request_log and api_logger is not None:
        payload = http_log_payload(
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            elapsed_s=now() - start,
            account_id=request.headers.get("x-account-id"),
        )
        api_logger.info("http_request %s", json.dumps(payload, separators=(",", ":")))
    return response


@app.on_event("startup")
async def on_startup() -> None:
    app.state.loggers = setup_application_logging(
        settings.disable_uvicorn_access_log, log_dir=settings.log_dir
    )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    return


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {
        "status": "ok",
        "app": settings.app_name,
        "env": settings.app_env,
        "db_engine": settings.db_engine,
    }


@app.get("/meta/ccxt/exchanges", response_model=CcxtExchangesResponse)
async def get_ccxt_exchanges(
    x_api_key: str = Header(default=""),
) -> CcxtExchangesResponse:
    auth = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={"op": "auth_check", "x_api_key": x_api_key},
    )
    if not auth.get("ok"):
        raise HTTPException(status_code=401, detail=auth.get("error") or {"code": "invalid_api_key"})
    return CcxtExchangesResponse(items=sorted(list(getattr(ccxt_async, "exchanges", []))))


@app.get("/dispatcher/status")
async def get_dispatcher_status(
    x_api_key: str = Header(default=""),
) -> dict[str, Any]:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={"op": "status", "x_api_key": x_api_key},
    )
    if out.get("ok"):
        return {"ok": True, "result": out.get("result", {})}
    raise HTTPException(status_code=503, detail=out.get("error") or {"code": "dispatcher_unavailable"})


@app.post("/position/commands", response_model=CommandsResponse)
async def post_position_commands(
    commands: CommandInput | list[CommandInput],
    parallel: bool = False,
    request_timeout_seconds: int | None = None,
    x_api_key: str = Header(default=""),
) -> CommandsResponse:
    items = commands if isinstance(commands, list) else [commands]
    dispatched = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=_resolve_fastapi_timeout(
            request_timeout_seconds,
            default_timeout_seconds=None,
        ),
        payload={
            "op": "position_commands_batch",
            "x_api_key": x_api_key,
            "parallel": bool(parallel),
            "items": [item.model_dump(by_alias=True, mode="json") for item in items],
        },
    )
    if not dispatched.get("ok"):
        raise HTTPException(status_code=400, detail=dispatched.get("error") or {"code": "dispatcher_error"})
    result = dispatched.get("result", {})
    return CommandsResponse(results=result.get("results", []))


def _scope_lookback_seconds(scope: str, account: dict[str, Any] | None = None) -> int:
    account = account or {}
    if scope == "hourly":
        return max(
            60,
            int(
                account.get("reconcile_hourly_lookback_seconds")
                or (settings.worker_reconcile_hourly_lookback_minutes * 60)
            ),
        )
    if scope == "long":
        return max(
            60,
            int(
                account.get("reconcile_long_lookback_seconds")
                or (settings.worker_reconcile_long_lookback_days * 86400)
            ),
        )
    return max(
        60,
        int(
            account.get("reconcile_short_lookback_seconds")
            or (settings.worker_reconcile_short_lookback_minutes * 60)
        ),
    )


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


def _resolve_fastapi_timeout(
    request_timeout_seconds: int | None,
    *,
    default_timeout_seconds: int | None,
) -> int | None:
    if request_timeout_seconds is None:
        return default_timeout_seconds
    return max(1, int(request_timeout_seconds))


@app.post("/ccxt/{account_id}/{func}")
async def post_ccxt_call(
    account_id: int,
    func: str,
    request: CcxtCallInput,
    request_timeout_seconds: int | None = None,
    x_api_key: str = Header(default=""),
) -> dict[str, Any]:
    dispatched = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=_resolve_fastapi_timeout(
            request_timeout_seconds,
            default_timeout_seconds=(
                None if _ccxt_requires_trade(func) else settings.dispatcher_request_timeout_seconds
            ),
        ),
        payload={
            "op": "ccxt_call",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "func": func,
            "args": request.args,
            "kwargs": request.kwargs,
        },
    )
    if dispatched.get("ok"):
        return {"ok": True, "result": dispatched.get("result")}
    raise HTTPException(status_code=400, detail=dispatched.get("error") or {"code": "ccxt_error"})


@app.post("/ccxt/core/{account_id}/create_order", response_model=CcxtResponse)
async def post_ccxt_core_create_order(
    account_id: int,
    request: CcxtCoreCreateOrderInput,
    x_api_key: str = Header(default=""),
) -> CcxtResponse:
    out = await post_ccxt_call(
        account_id=account_id,
        func="create_order",
        request=CcxtCallInput(
            args=[],
            kwargs={
                "symbol": request.symbol,
                "type": request.order_type,
                "side": request.side,
                "amount": request.amount,
                "price": request.price,
                "params": request.params,
            },
        ),
        x_api_key=x_api_key,
    )
    return CcxtResponse(result=out["result"])


@app.post("/ccxt/core/{account_id}/cancel_order", response_model=CcxtResponse)
async def post_ccxt_core_cancel_order(
    account_id: int,
    request: CcxtCoreCancelOrderInput,
    x_api_key: str = Header(default=""),
) -> CcxtResponse:
    out = await post_ccxt_call(
        account_id=account_id,
        func="cancel_order",
        request=CcxtCallInput(args=[], kwargs={"id": request.id, "symbol": request.symbol, "params": request.params}),
        x_api_key=x_api_key,
    )
    return CcxtResponse(result=out["result"])


@app.post("/ccxt/core/{account_id}/fetch_order", response_model=CcxtResponse)
async def post_ccxt_core_fetch_order(
    account_id: int,
    request: CcxtCoreFetchOrderInput,
    x_api_key: str = Header(default=""),
) -> CcxtResponse:
    out = await post_ccxt_call(
        account_id=account_id,
        func="fetch_order",
        request=CcxtCallInput(args=[], kwargs={"id": request.id, "symbol": request.symbol, "params": request.params}),
        x_api_key=x_api_key,
    )
    return CcxtResponse(result=out["result"])


@app.post("/ccxt/core/{account_id}/fetch_open_orders", response_model=CcxtResponse)
async def post_ccxt_core_fetch_open_orders(
    account_id: int,
    request: CcxtCoreFetchOpenOrdersInput,
    x_api_key: str = Header(default=""),
) -> CcxtResponse:
    out = await post_ccxt_call(
        account_id=account_id,
        func="fetch_open_orders",
        request=CcxtCallInput(
            args=[],
            kwargs={
                "symbol": request.symbol,
                "since": request.since,
                "limit": request.limit,
                "params": request.params,
            },
        ),
        x_api_key=x_api_key,
    )
    return CcxtResponse(result=out["result"])


@app.post("/ccxt/core/{account_id}/fetch_balance", response_model=CcxtResponse)
async def post_ccxt_core_fetch_balance(
    account_id: int,
    request: CcxtCoreFetchBalanceInput,
    x_api_key: str = Header(default=""),
) -> CcxtResponse:
    out = await post_ccxt_call(
        account_id=account_id,
        func="fetch_balance",
        request=CcxtCallInput(args=[], kwargs={"params": request.params}),
        x_api_key=x_api_key,
    )
    return CcxtResponse(result=out["result"])


@app.post("/ccxt/commands", response_model=CcxtBatchResponse)
async def post_ccxt_batch(
    items: CcxtBatchItem | list[CcxtBatchItem],
    parallel: bool = False,
    request_timeout_seconds: int | None = None,
    x_api_key: str = Header(default=""),
) -> CcxtBatchResponse:
    entries = items if isinstance(items, list) else [items]
    has_trade_ops = any(_ccxt_requires_trade(item.func) for item in entries)
    dispatched = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=_resolve_fastapi_timeout(
            request_timeout_seconds,
            default_timeout_seconds=(
                None if has_trade_ops else max(settings.dispatcher_request_timeout_seconds, 60)
            ),
        ),
        payload={
            "op": "ccxt_batch",
            "x_api_key": x_api_key,
            "parallel": bool(parallel),
            "items": [item.model_dump(by_alias=True, mode="json") for item in entries],
        },
    )
    if not dispatched.get("ok"):
        raise HTTPException(status_code=400, detail=dispatched.get("error") or {"code": "dispatcher_error"})
    result = dispatched.get("result", {})
    return CcxtBatchResponse(results=result.get("results", []))


@app.get("/position/orders/open", response_model=PositionOrdersResponse)
async def get_position_orders_open(
    account_id: int,
    strategy_id: int | None = None,
    x_api_key: str = Header(default=""),
) -> PositionOrdersResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "position_query",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "query": "orders_open",
            "strategy_id": strategy_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return PositionOrdersResponse(items=out.get("result", []))


@app.get("/position/orders/history", response_model=PositionOrdersResponse)
async def get_position_orders_history(
    account_id: int,
    strategy_id: int | None = None,
    x_api_key: str = Header(default=""),
) -> PositionOrdersResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "position_query",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "query": "orders_history",
            "strategy_id": strategy_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return PositionOrdersResponse(items=out.get("result", []))


@app.get("/position/deals", response_model=PositionDealsResponse)
async def get_position_deals(
    account_id: int,
    strategy_id: int | None = None,
    x_api_key: str = Header(default=""),
) -> PositionDealsResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "position_query",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "query": "deals",
            "strategy_id": strategy_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return PositionDealsResponse(items=out.get("result", []))


@app.get("/position/positions/open", response_model=PositionsResponse)
async def get_position_positions_open(
    account_id: int,
    strategy_id: int | None = None,
    x_api_key: str = Header(default=""),
) -> PositionsResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "position_query",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "query": "positions_open",
            "strategy_id": strategy_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return PositionsResponse(items=out.get("result", []))


@app.get("/position/positions/history", response_model=PositionsResponse)
async def get_position_positions_history(
    account_id: int,
    strategy_id: int | None = None,
    x_api_key: str = Header(default=""),
) -> PositionsResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "position_query",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "query": "positions_history",
            "strategy_id": strategy_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return PositionsResponse(items=out.get("result", []))


@app.post("/position/reassign", response_model=ReassignResponse)
async def post_position_reassign(
    req: ReassignInput,
    x_api_key: str = Header(default=""),
) -> ReassignResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "position_reassign",
            "x_api_key": x_api_key,
            "account_id": req.account_id,
            "deal_ids": req.deal_ids,
            "order_ids": req.order_ids,
            "target_strategy_id": req.target_strategy_id,
            "target_position_id": req.target_position_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return ReassignResponse(ok=True, deals_updated=int(res.get("deals_updated", 0)), orders_updated=int(res.get("orders_updated", 0)))


@app.post("/position/reconcile", response_model=ReconcileNowResponse)
async def post_position_reconcile_now(
    x_api_key: str = Header(default=""),
    req: ReconcileNowInput | None = None,
) -> ReconcileNowResponse:
    request = req or ReconcileNowInput()
    targets: list[int] = []
    seen: set[int] = set()

    if request.account_id is not None:
        targets.append(int(request.account_id))
        seen.add(int(request.account_id))

    if request.account_ids is not None:
        raw_ids: list[int] = []
        if isinstance(request.account_ids, list):
            raw_ids = [int(a) for a in request.account_ids if int(a) > 0]
        elif isinstance(request.account_ids, str):
            raw_ids = [int(x.strip()) for x in request.account_ids.split(",") if x.strip().isdigit() and int(x.strip()) > 0]
        for aid in raw_ids:
            if aid in seen:
                continue
            targets.append(aid)
            seen.add(aid)

    if not targets:
        return ReconcileNowResponse(ok=True, account_ids=[], triggered_count=0)

    if not targets:
        return ReconcileNowResponse(ok=True, account_ids=[], triggered_count=0)
    triggered = 0
    for account_id in targets:
        dispatched = await dispatch_request(
            host=settings.dispatcher_host,
            port=settings.dispatcher_port,
            timeout_seconds=max(settings.dispatcher_request_timeout_seconds, 60),
            payload={
                "op": "reconcile_now",
                "x_api_key": x_api_key,
                "account_id": int(account_id),
                "scope": request.scope,
                "lookback_seconds": _scope_lookback_seconds(request.scope, account=None),
            },
        )
        if dispatched.get("ok"):
            triggered += 1

    return ReconcileNowResponse(
        ok=True,
        account_ids=targets,
        triggered_count=triggered,
    )


@app.get("/position/reconcile/{account_id}/status", response_model=ReconcileStatusResponse)
async def get_position_reconcile_account_status(
    account_id: int,
    stale_after_seconds: int = 120,
    x_api_key: str = Header(default=""),
) -> ReconcileStatusResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "reconcile_status_account",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "stale_after_seconds": stale_after_seconds,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    row = out.get("result", {})
    return ReconcileStatusResponse(
        items=[
            {
                "account_id": account_id,
                "status": row.get("status"),
                "cursor_value": row["cursor_value"],
                "updated_at": row["updated_at"],
                "age_seconds": row.get("age_seconds"),
            }
        ]
    )


@app.get("/position/reconcile/status", response_model=ReconcileStatusResponse)
async def get_position_reconcile_status(
    status: str | None = None,
    stale_after_seconds: int = 120,
    x_api_key: str = Header(default=""),
) -> ReconcileStatusResponse:
    allowed = {None, "fresh", "stale", "never"}
    if status not in allowed:
        raise HTTPException(status_code=422, detail={"code": "validation_error", "message": "status must be fresh|stale|never"})

    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "reconcile_status_list",
            "x_api_key": x_api_key,
            "status": status,
            "stale_after_seconds": stale_after_seconds,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return ReconcileStatusResponse(items=out.get("result", []))


@app.get("/position/accounts", response_model=AccountsResponse)
async def get_position_accounts(
    x_api_key: str = Header(default=""),
) -> AccountsResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "accounts_list",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AccountsResponse(items=out.get("result", []))


@app.post("/position/risk/{account_id}/allow_new_positions", response_model=RiskActionResponse)
async def post_risk_allow_new_positions(
    account_id: int,
    req: RiskSetAllowNewPositionsInput,
    x_api_key: str = Header(default=""),
) -> RiskActionResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "risk_set_allow_new_positions",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "allow_new_positions": bool(req.allow_new_positions),
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return RiskActionResponse(
        ok=True,
        account_id=int(res.get("account_id", account_id)),
        allow_new_positions=bool(res.get("allow_new_positions", req.allow_new_positions)),
        rows=int(res.get("rows", 0)),
    )


@app.post("/position/risk/{account_id}/strategies/allow_new_positions", response_model=RiskActionResponse)
async def post_risk_strategy_allow_new_positions(
    account_id: int,
    req: RiskSetStrategyAllowNewPositionsInput,
    x_api_key: str = Header(default=""),
) -> RiskActionResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "risk_set_strategy_allow_new_positions",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "strategy_id": int(req.strategy_id),
            "allow_new_positions": bool(req.allow_new_positions),
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return RiskActionResponse(
        ok=True,
        account_id=int(res.get("account_id", account_id)),
        strategy_id=int(res.get("strategy_id", req.strategy_id)),
        allow_new_positions=bool(res.get("allow_new_positions", req.allow_new_positions)),
        rows=int(res.get("rows", 0)),
    )


@app.post("/position/risk/{account_id}/status", response_model=RiskActionResponse)
async def post_risk_account_status(
    account_id: int,
    req: RiskSetAccountStatusInput,
    x_api_key: str = Header(default=""),
) -> RiskActionResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "risk_set_account_status",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "status": req.status,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return RiskActionResponse(
        ok=True,
        account_id=int(res.get("account_id", account_id)),
        status=str(res.get("status", req.status)),
        rows=int(res.get("rows", 0)),
    )


@app.post("/admin/accounts", response_model=AdminCreateAccountResponse)
async def post_admin_create_account(
    req: AdminCreateAccountInput,
    x_api_key: str = Header(default=""),
) -> AdminCreateAccountResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_create_account",
            "x_api_key": x_api_key,
            "exchange_id": req.exchange_id,
            "label": req.label,
            "position_mode": req.position_mode,
            "is_testnet": req.is_testnet,
            "extra_config_json": req.extra_config_json,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminCreateAccountResponse(ok=True, account_id=int((out.get("result") or {}).get("account_id", 0)))


@app.get("/admin/accounts", response_model=AdminAccountsResponse)
async def get_admin_accounts(
    x_api_key: str = Header(default=""),
) -> AdminAccountsResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_list_accounts",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminAccountsResponse(items=out.get("result", []))


@app.patch("/admin/accounts/{account_id}", response_model=AdminUpdateAccountResponse)
async def patch_admin_account(
    account_id: int,
    req: AdminUpdateAccountInput,
    x_api_key: str = Header(default=""),
) -> AdminUpdateAccountResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_update_account",
            "x_api_key": x_api_key,
            "account_id": account_id,
            "exchange_id": req.exchange_id,
            "label": req.label,
            "position_mode": req.position_mode,
            "is_testnet": req.is_testnet,
            "status": req.status,
            "extra_config_json": req.extra_config_json,
            "credentials": None if req.credentials is None else req.credentials.model_dump(mode="json"),
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return AdminUpdateAccountResponse(
        ok=True,
        account_id=int(res.get("account_id", account_id)),
        rows=int(res.get("rows", 0)),
    )


@app.post("/admin/users-with-api-key", response_model=AdminCreateUserApiKeyResponse)
async def post_admin_create_user_api_key(
    req: AdminCreateUserApiKeyInput,
    x_api_key: str = Header(default=""),
) -> AdminCreateUserApiKeyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_create_user_api_key",
            "x_api_key": x_api_key,
            "user_name": req.user_name,
            "role": req.role,
            "api_key": req.api_key,
            "password": req.password,
            "permissions": [item.model_dump(mode="json") for item in req.permissions],
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return AdminCreateUserApiKeyResponse(
        ok=True,
        user_id=int(res.get("user_id", 0)),
        api_key_id=int(res.get("api_key_id", 0)),
        api_key_plain=str(res.get("api_key_plain", "")),
    )


@app.get("/admin/users-api-keys", response_model=AdminUsersApiKeysResponse)
async def get_admin_users_api_keys(
    x_api_key: str = Header(default=""),
) -> AdminUsersApiKeysResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_list_users_api_keys",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminUsersApiKeysResponse(items=out.get("result", []))


@app.patch("/admin/api-keys/{api_key_id}", response_model=AdminUpdateApiKeyResponse)
async def patch_admin_api_key(
    api_key_id: int,
    req: AdminUpdateApiKeyInput,
    x_api_key: str = Header(default=""),
) -> AdminUpdateApiKeyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_update_api_key",
            "x_api_key": x_api_key,
            "api_key_id": api_key_id,
            "status": req.status,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return AdminUpdateApiKeyResponse(
        ok=True,
        api_key_id=int(res.get("api_key_id", api_key_id)),
        rows=int(res.get("rows", 0)),
    )


@app.get("/admin/api-keys/{api_key_id}/permissions", response_model=AdminApiKeyPermissionsResponse)
async def get_admin_api_key_permissions(
    api_key_id: int,
    x_api_key: str = Header(default=""),
) -> AdminApiKeyPermissionsResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_list_api_key_permissions",
            "x_api_key": x_api_key,
            "api_key_id": api_key_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminApiKeyPermissionsResponse(items=out.get("result", []))


@app.put("/admin/api-keys/{api_key_id}/permissions", response_model=AdminUpdateApiKeyResponse)
async def put_admin_api_key_permission(
    api_key_id: int,
    req: AdminUpsertApiKeyPermissionInput,
    x_api_key: str = Header(default=""),
) -> AdminUpdateApiKeyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_upsert_api_key_permission",
            "x_api_key": x_api_key,
            "api_key_id": api_key_id,
            "account_id": req.account_id,
            "can_read": req.can_read,
            "can_trade": req.can_trade,
            "can_close_position": req.can_close_position,
            "can_risk_manage": req.can_risk_manage,
            "can_block_new_positions": req.can_block_new_positions,
            "can_block_account": req.can_block_account,
            "restrict_to_strategies": req.restrict_to_strategies,
            "strategy_ids": req.strategy_ids,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return AdminUpdateApiKeyResponse(ok=True, api_key_id=int(res.get("api_key_id", api_key_id)), rows=int(res.get("rows", 0)))


@app.get("/admin/users", response_model=AdminUsersResponse)
async def get_admin_users(
    x_api_key: str = Header(default=""),
) -> AdminUsersResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_list_users",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminUsersResponse(items=out.get("result", []))


@app.post("/auth/login-password", response_model=AuthLoginPasswordResponse)
async def post_auth_login_password(
    req: AuthLoginPasswordInput,
) -> AuthLoginPasswordResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "auth_login_password",
            "user_name": req.user_name,
            "password": req.password,
            "api_key_id": req.api_key_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "auth_error"})
    res = out.get("result", {})
    return AuthLoginPasswordResponse(
        ok=True,
        token=str(res.get("token", "")),
        token_type=str(res.get("token_type", "bearer")),
        expires_at=str(res.get("expires_at", "")),
        user_id=int(res.get("user_id", 0)),
        role=str(res.get("role", "trade")),
        api_key_id=int(res.get("api_key_id", 0)),
    )


@app.post("/admin/strategies", response_model=AdminCreateStrategyResponse)
async def post_admin_create_strategy(
    req: AdminCreateStrategyInput,
    x_api_key: str = Header(default=""),
) -> AdminCreateStrategyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_create_strategy",
            "x_api_key": x_api_key,
            "name": req.name,
            "account_ids": req.account_ids,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminCreateStrategyResponse(ok=True, strategy_id=int((out.get("result") or {}).get("strategy_id", 0)))


@app.get("/admin/strategies", response_model=AdminStrategiesResponse)
async def get_admin_list_strategies(
    x_api_key: str = Header(default=""),
) -> AdminStrategiesResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_list_strategies",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return AdminStrategiesResponse(items=out.get("result", []))


@app.patch("/admin/strategies/{strategy_id}", response_model=AdminUpdateStrategyResponse)
async def patch_admin_update_strategy(
    strategy_id: int,
    req: AdminUpdateStrategyInput,
    x_api_key: str = Header(default=""),
) -> AdminUpdateStrategyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_update_strategy",
            "x_api_key": x_api_key,
            "strategy_id": strategy_id,
            "name": req.name,
            "status": req.status,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return AdminUpdateStrategyResponse(
        ok=True,
        strategy_id=int(res.get("strategy_id", strategy_id)),
        rows=int(res.get("rows", 0)),
    )


@app.get("/strategies", response_model=StrategiesResponse)
async def get_strategies(
    x_api_key: str = Header(default=""),
) -> StrategiesResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "strategy_list",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return StrategiesResponse(items=out.get("result", []))


@app.post("/strategies", response_model=CreateStrategyResponse)
async def post_strategies(
    req: CreateStrategyInput,
    x_api_key: str = Header(default=""),
) -> CreateStrategyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "strategy_create",
            "x_api_key": x_api_key,
            "name": req.name,
            "account_ids": req.account_ids,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return CreateStrategyResponse(ok=True, strategy_id=int((out.get("result") or {}).get("strategy_id", 0)))


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    api_logger = app.state.loggers.get("api")
    api_key = str(websocket.headers.get("x-api-key") or "").strip()
    subscriptions = {"position", "ccxt"}
    subscribed_accounts: set[int] = set()
    last_event_id_by_account: dict[int, int] = {}
    if api_key:
        auth_check = await dispatch_request(
            host=settings.dispatcher_host,
            port=settings.dispatcher_port,
            timeout_seconds=settings.dispatcher_request_timeout_seconds,
            payload={"op": "auth_check", "x_api_key": api_key},
        )
        if not auth_check.get("ok"):
            await websocket.close(code=1008)
            return
    await websocket.send_json(
        {"id": "server-hello", "ok": True, "type": "ws_event", "event": "connected", "payload": {}}
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

                if action == "auth":
                    candidate = str(payload.get("api_key", "")).strip()
                    if not candidate:
                        await websocket.send_json(
                            {
                                "id": req_id,
                                "ok": False,
                                "type": "ws_response",
                                "namespace": "system",
                                "action": action,
                                "event": "error",
                                "payload": {"code": "missing_api_key"},
                            }
                        )
                        continue
                    auth_check = await dispatch_request(
                        host=settings.dispatcher_host,
                        port=settings.dispatcher_port,
                        timeout_seconds=settings.dispatcher_request_timeout_seconds,
                        payload={"op": "auth_check", "x_api_key": candidate},
                    )
                    if not auth_check.get("ok"):
                        await websocket.send_json(
                            {
                                "id": req_id,
                                "ok": False,
                                "type": "ws_response",
                                "namespace": "system",
                                "action": action,
                                "event": "error",
                                "payload": auth_check.get("error") or {"code": "invalid_api_key"},
                            }
                        )
                        continue
                    api_key = candidate
                    if api_logger is not None:
                        api_logger.info(
                            "ws_auth %s",
                            json.dumps(
                                {
                                    "x_api_key": mask_header_value("x-api-key", api_key),
                                },
                                separators=(",", ":"),
                            ),
                        )
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": True,
                            "type": "ws_response",
                            "namespace": "system",
                            "action": action,
                            "event": "authenticated",
                            "payload": auth_check.get("result") or {},
                        }
                    )
                    continue

                if not api_key:
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": False,
                            "type": "ws_response",
                            "namespace": namespace or "system",
                            "action": action,
                            "event": "error",
                            "payload": {"code": "not_authenticated"},
                        }
                    )
                    continue

                if action == "subscribe":
                    namespaces = payload.get("namespaces", [])
                    if isinstance(namespaces, list):
                        subscriptions = {str(n) for n in namespaces if str(n) in {"position", "ccxt"}}
                        if not subscriptions:
                            subscriptions = {"position"}
                    account_ids_raw = payload.get("account_ids") if isinstance(payload.get("account_ids"), list) else []
                    account_ids = sorted(
                        {int(x) for x in account_ids_raw if str(x).isdigit() and int(x) > 0}
                    )
                    if not account_ids:
                        await websocket.send_json(
                            {
                                "id": req_id,
                                "ok": False,
                                "type": "ws_response",
                                "namespace": "system",
                                "action": action,
                                "event": "error",
                                "payload": {"code": "missing_account_ids"},
                            }
                        )
                        continue
                    authorized: list[int] = []
                    for account_id in account_ids:
                        chk = await dispatch_request(
                            host=settings.dispatcher_host,
                            port=settings.dispatcher_port,
                            timeout_seconds=settings.dispatcher_request_timeout_seconds,
                            payload={
                                "op": "authorize_account",
                                "x_api_key": api_key,
                                "account_id": account_id,
                                "require_trade": False,
                                "for_ws": True,
                            },
                        )
                        if chk.get("ok"):
                            authorized.append(account_id)
                    if not authorized:
                        await websocket.send_json(
                            {
                                "id": req_id,
                                "ok": False,
                                "type": "ws_response",
                                "namespace": "system",
                                "action": action,
                                "event": "error",
                                "payload": {"code": "permission_denied"},
                            }
                        )
                        continue
                    subscribed_accounts = set(authorized)
                    for account_id in subscribed_accounts:
                        tail = await dispatch_request(
                            host=settings.dispatcher_host,
                            port=settings.dispatcher_port,
                            timeout_seconds=settings.dispatcher_request_timeout_seconds,
                            payload={
                                "op": "ws_tail_id",
                                "x_api_key": api_key,
                                "account_id": account_id,
                            },
                        )
                        last_event_id_by_account[account_id] = (
                            int(((tail.get("result") or {}).get("tail_id")) or 0) if tail.get("ok") else 0
                        )
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": True,
                            "type": "ws_response",
                            "namespace": "system",
                            "action": action,
                            "event": "subscribed",
                            "payload": {
                                "namespaces": sorted(subscriptions),
                                "account_ids": sorted(subscribed_accounts),
                            },
                        }
                    )
                    with_snapshot = bool(payload.get("with_snapshot", True))
                    if with_snapshot and "position" in subscriptions:
                        for account_id in sorted(subscribed_accounts):
                            open_orders = await dispatch_request(
                                host=settings.dispatcher_host,
                                port=settings.dispatcher_port,
                                timeout_seconds=settings.dispatcher_request_timeout_seconds,
                                payload={
                                    "op": "position_query",
                                    "x_api_key": api_key,
                                    "account_id": account_id,
                                    "query": "orders_open",
                                },
                            )
                            if open_orders.get("ok"):
                                for row in open_orders.get("result", []) or []:
                                    await websocket.send_json(
                                        {
                                            "id": None,
                                            "ok": True,
                                            "type": "ws_event",
                                            "namespace": "position",
                                            "action": "snapshot",
                                            "event": "snapshot_open_order",
                                            "payload": row,
                                        }
                                    )
                            open_positions = await dispatch_request(
                                host=settings.dispatcher_host,
                                port=settings.dispatcher_port,
                                timeout_seconds=settings.dispatcher_request_timeout_seconds,
                                payload={
                                    "op": "position_query",
                                    "x_api_key": api_key,
                                    "account_id": account_id,
                                    "query": "positions_open",
                                },
                            )
                            if open_positions.get("ok"):
                                for row in open_positions.get("result", []) or []:
                                    await websocket.send_json(
                                        {
                                            "id": None,
                                            "ok": True,
                                            "type": "ws_event",
                                            "namespace": "position",
                                            "action": "snapshot",
                                            "event": "snapshot_open_position",
                                            "payload": row,
                                        }
                                    )
                    continue

                if namespace == "position" and action == "command":
                    command_payload = dict(payload)
                    account_id = int(command_payload.get("account_id", 0) or 0)
                    if account_id <= 0:
                        await websocket.send_json(
                            {
                                "id": req_id,
                                "ok": False,
                                "type": "ws_response",
                                "namespace": "position",
                                "action": action,
                                "event": "error",
                                "payload": {"code": "missing_account_id"},
                            }
                        )
                        continue
                    item = COMMAND_INPUT_ADAPTER.validate_python(command_payload)
                    dispatched = await dispatch_request(
                        host=settings.dispatcher_host,
                        port=settings.dispatcher_port,
                        timeout_seconds=settings.dispatcher_request_timeout_seconds,
                        payload={
                            "op": "position_command",
                            "x_api_key": api_key,
                            "account_id": account_id,
                            "index": 0,
                            "item": item.model_dump(by_alias=True, mode="json"),
                        },
                    )
                    result = dispatched.get("result", {}) if dispatched.get("ok") else {
                        "index": 0,
                        "ok": False,
                        "command_id": None,
                        "order_id": None,
                        "error": dispatched.get("error") or {"code": "dispatcher_error"},
                    }
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": bool(result.get("ok", False)),
                            "type": "ws_response",
                            "namespace": "position",
                            "action": action,
                            "event": "command_result",
                            "payload": result,
                        }
                    )
                    continue

                if namespace == "ccxt" and action == "call":
                    account_id = int(payload.get("account_id", 0) or 0)
                    if account_id <= 0:
                        await websocket.send_json(
                            {
                                "id": req_id,
                                "ok": False,
                                "type": "ws_response",
                                "namespace": "ccxt",
                                "action": action,
                                "event": "error",
                                "payload": {"code": "missing_account_id"},
                            }
                        )
                        continue
                    method = str(payload.get("func", "")).strip()
                    args = payload.get("args") if isinstance(payload.get("args"), list) else []
                    kwargs = payload.get("kwargs") if isinstance(payload.get("kwargs"), dict) else {}
                    dispatched = await dispatch_request(
                        host=settings.dispatcher_host,
                        port=settings.dispatcher_port,
                        timeout_seconds=settings.dispatcher_request_timeout_seconds,
                        payload={
                            "op": "ccxt_call",
                            "x_api_key": api_key,
                            "account_id": account_id,
                            "func": method,
                            "args": args,
                            "kwargs": kwargs,
                        },
                    )
                    await websocket.send_json(
                        {
                            "id": req_id,
                            "ok": bool(dispatched.get("ok")),
                            "type": "ws_response",
                            "namespace": "ccxt",
                            "action": action,
                            "event": "ccxt_result" if dispatched.get("ok") else "error",
                            "payload": (
                                {"result": dispatched.get("result")}
                                if dispatched.get("ok")
                                else {"code": "ccxt_error", "detail": dispatched.get("error")}
                            ),
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

            if api_key and subscribed_accounts:
                for account_id in sorted(subscribed_accounts):
                    from_id = int(last_event_id_by_account.get(account_id, 0))
                    pulled = await dispatch_request(
                        host=settings.dispatcher_host,
                        port=settings.dispatcher_port,
                        timeout_seconds=settings.dispatcher_request_timeout_seconds,
                        payload={
                            "op": "ws_pull_events",
                            "x_api_key": api_key,
                            "account_id": account_id,
                            "from_event_id": from_id,
                            "limit": 100,
                        },
                    )
                    events = pulled.get("result", []) if pulled.get("ok") else []
                    for ev in events:
                        if ev["namespace"] not in subscriptions:
                            last_event_id_by_account[account_id] = max(
                                int(last_event_id_by_account.get(account_id, 0)),
                                int(ev["id"]),
                            )
                            continue
                        last_event_id_by_account[account_id] = max(
                            int(last_event_id_by_account.get(account_id, 0)),
                            int(ev["id"]),
                        )
                        event_payload = ev["payload"] if isinstance(ev.get("payload"), dict) else {}
                        if "account_id" not in event_payload:
                            event_payload["account_id"] = account_id
                        await websocket.send_json(
                            {
                                "id": None,
                                "ok": True,
                                "type": "ws_event",
                                "namespace": ev["namespace"],
                                "action": "event",
                                "event": ev["event_type"],
                                "payload": event_payload,
                            }
                        )
        except WebSocketDisconnect:
            if api_logger is not None:
                api_logger.info(
                    "ws_disconnect %s",
                    json.dumps({"account_ids": sorted(subscribed_accounts)}, separators=(",", ":")),
                )
            return
        except Exception:
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {"id": None, "ok": False, "type": "ws_response", "event": "error", "payload": {"code": "internal_error"}}
                )
            if api_logger is not None:
                api_logger.exception(
                    "ws_error %s",
                    json.dumps({"account_ids": sorted(subscribed_accounts)}, separators=(",", ":")),
                )
