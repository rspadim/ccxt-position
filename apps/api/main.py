import asyncio
import contextlib
import json
from datetime import datetime
from typing import Any
import ccxt.async_support as ccxt_async

from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
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
    AdminCreateApiKeyInput,
    AdminCreateApiKeyResponse,
    AdminUpdateApiKeyInput,
    AdminUpdateApiKeyResponse,
    AdminApiKeyPermissionsResponse,
    AdminUpsertApiKeyPermissionInput,
    AdminStrategiesResponse,
    AdminUpdateStrategyInput,
    AdminUpdateStrategyResponse,
    AdminOmsView,
    AdminOmsQueryResponse,
    AdminOmsMutateResponse,
    AdminOmsOrderMutation,
    AdminOmsPositionMutation,
    AdminOmsDealMutation,
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
    CcxtRawOrdersResponse,
    CcxtRawTradesResponse,
    CcxtCallInput,
    AuthLoginPasswordInput,
    AuthLoginPasswordResponse,
    UserProfileResponse,
    UserUpdateProfileInput,
    UserUpdateProfileResponse,
    UserUpdatePasswordInput,
    UserUpdatePasswordResponse,
    UserApiKeysResponse,
    UserCreateApiKeyInput,
    UserCreateApiKeyResponse,
    UserUpdateApiKeyInput,
    UserUpdateApiKeyResponse,
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


def _map_command_error_code_to_http_status(code: str, message: str = "") -> int:
    normalized = str(code or "").strip().lower()
    normalized_message = str(message or "").strip().lower()
    if normalized_message in {
        "permission_denied",
        "admin_read_only",
        "strategy_permission_denied",
    }:
        return 403
    if normalized_message in {"validation_error", "invalid_strategy_id"}:
        return 422
    if normalized_message in {"account_not_found", "position_not_found", "order_not_found"}:
        return 404
    if normalized_message in {"risk_blocked"}:
        return 409
    if normalized in {"permission_denied", "admin_read_only", "strategy_permission_denied"}:
        return 403
    if normalized in {"validation_error", "invalid_strategy_id"}:
        return 422
    if normalized in {"account_not_found", "position_not_found", "order_not_found"}:
        return 404
    if normalized in {"risk_blocked"}:
        return 409
    if normalized in {"dispatcher_error"}:
        return 502
    return 400


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


@app.post("/oms/commands", response_model=CommandsResponse)
async def post_oms_commands(
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
            "op": "oms_commands_batch",
            "x_api_key": x_api_key,
            "parallel": bool(parallel),
            "items": [item.model_dump(by_alias=True, mode="json") for item in items],
        },
    )
    if not dispatched.get("ok"):
        raise HTTPException(status_code=400, detail=dispatched.get("error") or {"code": "dispatcher_error"})
    result = dispatched.get("result", {})
    results = result.get("results", [])
    failed_items = [item for item in results if not bool(item.get("ok"))]
    if failed_items:
        if len(failed_items) == len(results):
            first_error = failed_items[0].get("error") or {}
            error_code = str(first_error.get("code") or "dispatcher_error")
            error_message = str(first_error.get("message") or "all commands failed")
            raise HTTPException(
                status_code=_map_command_error_code_to_http_status(error_code, error_message),
                detail={
                    "code": error_code,
                    "message": error_message,
                    "results": results,
                },
            )
        # Partial success: keep batch payload, but expose HTTP 207 explicitly.
        return JSONResponse(
            status_code=207,
            content=CommandsResponse(results=results).model_dump(mode="json"),
        )
    return CommandsResponse(results=results)


def _normalize_account_targets(
    account_ids: str | list[int] | None,
) -> list[int]:
    targets: list[int] = []
    seen: set[int] = set()
    if isinstance(account_ids, str):
        for part in account_ids.split(","):
            raw = part.strip()
            if not raw.isdigit():
                continue
            aid = int(raw)
            if aid <= 0 or aid in seen:
                continue
            targets.append(aid)
            seen.add(aid)
    elif isinstance(account_ids, list):
        for raw in account_ids:
            aid = int(raw or 0)
            if aid <= 0 or aid in seen:
                continue
            targets.append(aid)
            seen.add(aid)
    return targets


async def _oms_query_multi_account(
    *,
    x_api_key: str,
    query: str,
    account_ids: str | list[int] | None,
    strategy_id: int | None,
    date_from: str | None = None,
    date_to: str | None = None,
    open_limit: int | None = None,
) -> list[dict[str, Any]]:
    targets = _normalize_account_targets(account_ids=account_ids)
    if not targets:
        raise HTTPException(status_code=422, detail={"code": "validation_error", "message": "account_ids is required"})

    requests = [
        dispatch_request(
            host=settings.dispatcher_host,
            port=settings.dispatcher_port,
            timeout_seconds=settings.dispatcher_request_timeout_seconds,
            payload={
                "op": "oms_query",
                "x_api_key": x_api_key,
                "account_id": aid,
                "query": query,
                "strategy_id": strategy_id,
                "date_from": date_from,
                "date_to": date_to,
                "open_limit": open_limit,
            },
        )
        for aid in targets
    ]
    outputs = await asyncio.gather(*requests)
    merged: list[dict[str, Any]] = []
    for aid, out in zip(targets, outputs):
        if not out.get("ok"):
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "dispatcher_error",
                    "account_id": aid,
                    "detail": out.get("error") or {"code": "dispatcher_error"},
                },
            )
        rows = out.get("result", [])
        if isinstance(rows, list):
            merged.extend(rows)
    merged.sort(key=lambda row: int(row.get("id", 0) or 0))
    return merged


async def _ccxt_raw_query_multi_account(
    *,
    x_api_key: str,
    query: str,
    account_ids: str | list[int] | None,
    date_from: str,
    date_to: str,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], int, int, int]:
    targets = _normalize_account_targets(account_ids=account_ids)
    if not targets:
        raise HTTPException(status_code=422, detail={"code": "validation_error", "message": "account_ids is required"})
    dispatched = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "ccxt_raw_query_multi",
            "x_api_key": x_api_key,
            "query": query,
            "account_ids": targets,
            "date_from": date_from,
            "date_to": date_to,
            "page": int(page or 1),
            "page_size": int(page_size or 100),
        },
    )
    if not dispatched.get("ok"):
        raise HTTPException(status_code=400, detail=dispatched.get("error") or {"code": "dispatcher_error"})
    result = dispatched.get("result", {}) if isinstance(dispatched.get("result"), dict) else {}
    items = result.get("items", [])
    total = int(result.get("total", 0) or 0)
    out_page = int(result.get("page", int(page or 1)) or int(page or 1))
    out_page_size = int(result.get("page_size", int(page_size or 100)) or int(page_size or 100))
    return (items if isinstance(items, list) else []), total, out_page, out_page_size


def _parse_date_yyyy_mm_dd(value: str, field_name: str) -> str:
    try:
        parsed = datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "validation_error",
                "message": f"{field_name} must be YYYY-MM-DD",
            },
        ) from None
    return parsed.isoformat()


def _paginate_items(items: list[dict[str, Any]], page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
    normalized_page = max(1, int(page or 1))
    normalized_page_size = max(1, min(500, int(page_size or 100)))
    total = len(items)
    offset = (normalized_page - 1) * normalized_page_size
    return items[offset: offset + normalized_page_size], total


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


@app.get("/ccxt/orders/raw", response_model=CcxtRawOrdersResponse)
async def get_ccxt_orders_raw(
    account_ids: str,
    start_date: str | None = None,
    end_date: str | None = None,
    page: int = 1,
    page_size: int = 100,
    x_api_key: str = Header(default=""),
) -> CcxtRawOrdersResponse:
    if not start_date or not end_date:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date are required"},
        )
    date_from = _parse_date_yyyy_mm_dd(start_date, "start_date")
    date_to = _parse_date_yyyy_mm_dd(end_date, "end_date")
    if date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    items, total, out_page, out_page_size = await _ccxt_raw_query_multi_account(
        x_api_key=x_api_key,
        query="orders_raw",
        account_ids=account_ids,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
    return CcxtRawOrdersResponse(items=items, total=total, page=out_page, page_size=out_page_size)


@app.get("/ccxt/trades/raw", response_model=CcxtRawTradesResponse)
async def get_ccxt_trades_raw(
    account_ids: str,
    start_date: str | None = None,
    end_date: str | None = None,
    page: int = 1,
    page_size: int = 100,
    x_api_key: str = Header(default=""),
) -> CcxtRawTradesResponse:
    if not start_date or not end_date:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date are required"},
        )
    date_from = _parse_date_yyyy_mm_dd(start_date, "start_date")
    date_to = _parse_date_yyyy_mm_dd(end_date, "end_date")
    if date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    items, total, out_page, out_page_size = await _ccxt_raw_query_multi_account(
        x_api_key=x_api_key,
        query="trades_raw",
        account_ids=account_ids,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )
    return CcxtRawTradesResponse(items=items, total=total, page=out_page, page_size=out_page_size)


@app.get("/oms/orders/open", response_model=PositionOrdersResponse)
async def get_position_orders_open(
    account_ids: str,
    strategy_id: int | None = None,
    limit: int = 500,
    x_api_key: str = Header(default=""),
) -> PositionOrdersResponse:
    rows = await _oms_query_multi_account(
        x_api_key=x_api_key,
        query="orders_open",
        account_ids=account_ids,
        strategy_id=strategy_id,
        open_limit=max(1, min(5000, int(limit or 500))),
    )
    return PositionOrdersResponse(items=rows)


@app.get("/oms/orders/history", response_model=PositionOrdersResponse)
async def get_position_orders_history(
    account_ids: str,
    strategy_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    page: int = 1,
    page_size: int = 100,
    x_api_key: str = Header(default=""),
) -> PositionOrdersResponse:
    if not start_date or not end_date:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date are required"},
        )
    date_from = _parse_date_yyyy_mm_dd(start_date, "start_date")
    date_to = _parse_date_yyyy_mm_dd(end_date, "end_date")
    if date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    rows = await _oms_query_multi_account(
        x_api_key=x_api_key,
        query="orders_history",
        account_ids=account_ids,
        strategy_id=strategy_id,
        date_from=date_from,
        date_to=date_to,
    )
    paged, total = _paginate_items(rows, page=page, page_size=page_size)
    return PositionOrdersResponse(items=paged, total=total, page=page, page_size=page_size)


@app.get("/oms/deals", response_model=PositionDealsResponse)
async def get_position_deals(
    account_ids: str,
    strategy_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    page: int = 1,
    page_size: int = 100,
    x_api_key: str = Header(default=""),
) -> PositionDealsResponse:
    if not start_date or not end_date:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date are required"},
        )
    date_from = _parse_date_yyyy_mm_dd(start_date, "start_date")
    date_to = _parse_date_yyyy_mm_dd(end_date, "end_date")
    if date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    rows = await _oms_query_multi_account(
        x_api_key=x_api_key,
        query="deals",
        account_ids=account_ids,
        strategy_id=strategy_id,
        date_from=date_from,
        date_to=date_to,
    )
    paged, total = _paginate_items(rows, page=page, page_size=page_size)
    return PositionDealsResponse(items=paged, total=total, page=page, page_size=page_size)


@app.get("/oms/positions/open", response_model=PositionsResponse)
async def get_position_positions_open(
    account_ids: str,
    strategy_id: int | None = None,
    limit: int = 500,
    x_api_key: str = Header(default=""),
) -> PositionsResponse:
    rows = await _oms_query_multi_account(
        x_api_key=x_api_key,
        query="positions_open",
        account_ids=account_ids,
        strategy_id=strategy_id,
        open_limit=max(1, min(5000, int(limit or 500))),
    )
    return PositionsResponse(items=rows)


@app.get("/oms/positions/history", response_model=PositionsResponse)
async def get_position_positions_history(
    account_ids: str,
    strategy_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    page: int = 1,
    page_size: int = 100,
    x_api_key: str = Header(default=""),
) -> PositionsResponse:
    if not start_date or not end_date:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date are required"},
        )
    date_from = _parse_date_yyyy_mm_dd(start_date, "start_date")
    date_to = _parse_date_yyyy_mm_dd(end_date, "end_date")
    if date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    rows = await _oms_query_multi_account(
        x_api_key=x_api_key,
        query="positions_history",
        account_ids=account_ids,
        strategy_id=strategy_id,
        date_from=date_from,
        date_to=date_to,
    )
    paged, total = _paginate_items(rows, page=page, page_size=page_size)
    return PositionsResponse(items=paged, total=total, page=page, page_size=page_size)


@app.post("/oms/reassign", response_model=ReassignResponse)
async def post_oms_reassign(
    req: ReassignInput,
    x_api_key: str = Header(default=""),
) -> ReassignResponse:
    date_from: str | None = None
    date_to: str | None = None
    if req.start_date:
        date_from = _parse_date_yyyy_mm_dd(req.start_date, "start_date")
    if req.end_date:
        date_to = _parse_date_yyyy_mm_dd(req.end_date, "end_date")
    if (date_from and not date_to) or (date_to and not date_from):
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date must be provided together"},
        )
    if date_from and date_to and date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "oms_reassign",
            "x_api_key": x_api_key,
            "account_id": req.account_id,
            "account_ids": req.account_ids,
            "deal_ids": req.deal_ids,
            "order_ids": req.order_ids,
            "date_from": date_from,
            "date_to": date_to,
            "reconciled": req.reconciled,
            "order_statuses": req.order_statuses,
            "kinds": req.kinds,
            "preview": req.preview,
            "page": req.page,
            "page_size": req.page_size,
            "target_strategy_id": req.target_strategy_id,
            "target_position_id": req.target_position_id,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return ReassignResponse(
        ok=True,
        deals_updated=int(res.get("deals_updated", 0)),
        orders_updated=int(res.get("orders_updated", 0)),
        deals_total=int(res.get("deals_total", 0)),
        orders_total=int(res.get("orders_total", 0)),
        preview=bool(res.get("preview", False)),
        page=int(res.get("page", req.page or 1)),
        page_size=int(res.get("page_size", req.page_size or 100)),
        items=res.get("items", []),
    )


@app.post("/oms/reconcile", response_model=ReconcileNowResponse)
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

    scope = str(request.scope or "short").strip().lower()
    lookback_override: int | None = None
    if scope == "period":
        start_date = _parse_date_yyyy_mm_dd(str(request.start_date or ""), "start_date")
        end_date = _parse_date_yyyy_mm_dd(str(request.end_date or ""), "end_date")
        if start_date > end_date:
            raise HTTPException(
                status_code=422,
                detail={"code": "validation_error", "message": "start_date must be <= end_date"},
            )
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        delta_seconds = int((datetime.utcnow() - start_dt).total_seconds())
        lookback_override = max(60, delta_seconds)

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
                "scope": scope,
                "lookback_seconds": (
                    int(lookback_override)
                    if lookback_override is not None
                    else _scope_lookback_seconds(scope, account=None)
                ),
            },
        )
        if dispatched.get("ok"):
            triggered += 1

    return ReconcileNowResponse(
        ok=True,
        account_ids=targets,
        triggered_count=triggered,
    )


@app.get("/oms/reconcile/{account_id}/status", response_model=ReconcileStatusResponse)
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


@app.get("/oms/reconcile/status", response_model=ReconcileStatusResponse)
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


@app.get("/oms/accounts", response_model=AccountsResponse)
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


@app.post("/oms/risk/{account_id}/allow_new_positions", response_model=RiskActionResponse)
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
            "comment": req.comment,
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


@app.post("/oms/risk/{account_id}/strategies/allow_new_positions", response_model=RiskActionResponse)
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
            "comment": req.comment,
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


@app.post("/oms/risk/{account_id}/status", response_model=RiskActionResponse)
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
            "comment": req.comment,
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


@app.post("/admin/api-keys", response_model=AdminCreateApiKeyResponse)
async def post_admin_create_api_key(
    req: AdminCreateApiKeyInput,
    x_api_key: str = Header(default=""),
) -> AdminCreateApiKeyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_create_api_key",
            "x_api_key": x_api_key,
            "user_id": req.user_id,
            "api_key": req.api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return AdminCreateApiKeyResponse(
        ok=True,
        user_id=int(res.get("user_id", req.user_id)),
        api_key_id=int(res.get("api_key_id", 0)),
        api_key_plain=str(res.get("api_key_plain", "")),
    )


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
        role=str(res.get("role", "trader")),
        api_key_id=int(res.get("api_key_id", 0)),
    )


@app.get("/user/profile", response_model=UserProfileResponse)
async def get_user_profile(
    x_api_key: str = Header(default=""),
) -> UserProfileResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "user_profile_get",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return UserProfileResponse(
        user_id=int(res.get("user_id", 0)),
        user_name=str(res.get("user_name", "")),
        role=str(res.get("role", "")),
        status=str(res.get("status", "")),
        api_key_id=int(res.get("api_key_id", 0)),
    )


@app.patch("/user/profile", response_model=UserUpdateProfileResponse)
async def patch_user_profile(
    req: UserUpdateProfileInput,
    x_api_key: str = Header(default=""),
) -> UserUpdateProfileResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "user_profile_update",
            "x_api_key": x_api_key,
            "user_name": req.user_name,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return UserUpdateProfileResponse(
        ok=True,
        user_id=int(res.get("user_id", 0)),
        user_name=str(res.get("user_name", req.user_name)),
    )


@app.post("/user/password", response_model=UserUpdatePasswordResponse)
async def post_user_password(
    req: UserUpdatePasswordInput,
    x_api_key: str = Header(default=""),
) -> UserUpdatePasswordResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "user_password_update",
            "x_api_key": x_api_key,
            "current_password": req.current_password,
            "new_password": req.new_password,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return UserUpdatePasswordResponse(
        ok=True,
        user_id=int(res.get("user_id", 0)),
    )


@app.get("/user/api-keys", response_model=UserApiKeysResponse)
async def get_user_api_keys(
    x_api_key: str = Header(default=""),
) -> UserApiKeysResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "user_api_keys_list",
            "x_api_key": x_api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    return UserApiKeysResponse(items=out.get("result", []))


@app.post("/user/api-keys", response_model=UserCreateApiKeyResponse)
async def post_user_api_key(
    req: UserCreateApiKeyInput,
    x_api_key: str = Header(default=""),
) -> UserCreateApiKeyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "user_api_key_create",
            "x_api_key": x_api_key,
            "api_key": req.api_key,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return UserCreateApiKeyResponse(
        ok=True,
        user_id=int(res.get("user_id", 0)),
        api_key_id=int(res.get("api_key_id", 0)),
        api_key_plain=str(res.get("api_key_plain", "")),
    )


@app.patch("/user/api-keys/{api_key_id}", response_model=UserUpdateApiKeyResponse)
async def patch_user_api_key(
    api_key_id: int,
    req: UserUpdateApiKeyInput,
    x_api_key: str = Header(default=""),
) -> UserUpdateApiKeyResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "user_api_key_update",
            "x_api_key": x_api_key,
            "api_key_id": api_key_id,
            "status": req.status,
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {})
    return UserUpdateApiKeyResponse(
        ok=True,
        api_key_id=int(res.get("api_key_id", api_key_id)),
        rows=int(res.get("rows", 0)),
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
            "client_strategy_id": req.client_strategy_id,
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
            "client_strategy_id": req.client_strategy_id,
            "account_ids": req.account_ids,
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


@app.get("/admin/oms/{view}", response_model=AdminOmsQueryResponse)
async def get_admin_oms_view(
    view: AdminOmsView,
    account_ids: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    page: int = 1,
    page_size: int = 100,
    x_api_key: str = Header(default=""),
) -> AdminOmsQueryResponse:
    date_from: str | None = None
    date_to: str | None = None
    if start_date:
        date_from = _parse_date_yyyy_mm_dd(start_date, "start_date")
    if end_date:
        date_to = _parse_date_yyyy_mm_dd(end_date, "end_date")
    if (date_from and not date_to) or (date_to and not date_from):
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date and end_date must be provided together"},
        )
    if date_from and date_to and date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail={"code": "validation_error", "message": "start_date must be <= end_date"},
        )
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_oms_query",
            "x_api_key": x_api_key,
            "view": view,
            "account_ids": account_ids,
            "date_from": date_from,
            "date_to": date_to,
            "page": int(page or 1),
            "page_size": int(page_size or 100),
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {}) if isinstance(out.get("result"), dict) else {}
    return AdminOmsQueryResponse(
        items=res.get("items", []),
        total=int(res.get("total", 0) or 0),
        page=int(res.get("page", page) or page),
        page_size=int(res.get("page_size", page_size) or page_size),
    )


@app.post("/admin/oms/orders/mutate", response_model=AdminOmsMutateResponse)
async def post_admin_oms_orders_mutate(
    operations: list[AdminOmsOrderMutation],
    x_api_key: str = Header(default=""),
) -> AdminOmsMutateResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_oms_mutate",
            "x_api_key": x_api_key,
            "entity": "orders",
            "operations": [item.model_dump(mode="json") for item in operations],
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {}) if isinstance(out.get("result"), dict) else {}
    return AdminOmsMutateResponse(ok=True, entity="orders", results=res.get("results", []))


@app.post("/admin/oms/positions/mutate", response_model=AdminOmsMutateResponse)
async def post_admin_oms_positions_mutate(
    operations: list[AdminOmsPositionMutation],
    x_api_key: str = Header(default=""),
) -> AdminOmsMutateResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_oms_mutate",
            "x_api_key": x_api_key,
            "entity": "positions",
            "operations": [item.model_dump(mode="json") for item in operations],
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {}) if isinstance(out.get("result"), dict) else {}
    return AdminOmsMutateResponse(ok=True, entity="positions", results=res.get("results", []))


@app.post("/admin/oms/deals/mutate", response_model=AdminOmsMutateResponse)
async def post_admin_oms_deals_mutate(
    operations: list[AdminOmsDealMutation],
    x_api_key: str = Header(default=""),
) -> AdminOmsMutateResponse:
    out = await dispatch_request(
        host=settings.dispatcher_host,
        port=settings.dispatcher_port,
        timeout_seconds=settings.dispatcher_request_timeout_seconds,
        payload={
            "op": "admin_oms_mutate",
            "x_api_key": x_api_key,
            "entity": "deals",
            "operations": [item.model_dump(mode="json") for item in operations],
        },
    )
    if not out.get("ok"):
        raise HTTPException(status_code=400, detail=out.get("error") or {"code": "dispatcher_error"})
    res = out.get("result", {}) if isinstance(out.get("result"), dict) else {}
    return AdminOmsMutateResponse(ok=True, entity="deals", results=res.get("results", []))


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
            "client_strategy_id": req.client_strategy_id,
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
                        snapshot_meta: list[dict[str, int]] = []
                        for account_id in sorted(subscribed_accounts):
                            open_orders = await dispatch_request(
                                host=settings.dispatcher_host,
                                port=settings.dispatcher_port,
                                timeout_seconds=settings.dispatcher_request_timeout_seconds,
                                payload={
                                    "op": "oms_query",
                                    "x_api_key": api_key,
                                    "account_id": account_id,
                                    "query": "orders_open",
                                },
                            )
                            orders_items = open_orders.get("result", []) if open_orders.get("ok") else []
                            if not isinstance(orders_items, list):
                                orders_items = []
                            snapshot_meta.append(
                                {
                                    "account_id": int(account_id),
                                    "open_orders": len(orders_items),
                                    "open_positions": 0,
                                }
                            )
                            if open_orders.get("ok"):
                                await websocket.send_json(
                                    {
                                        "id": None,
                                        "ok": True,
                                        "type": "ws_event",
                                        "namespace": "position",
                                        "action": "snapshot",
                                        "event": "snapshot_open_orders",
                                        "payload": {
                                            "account_id": account_id,
                                            "items": orders_items,
                                        },
                                    }
                                )
                            open_positions = await dispatch_request(
                                host=settings.dispatcher_host,
                                port=settings.dispatcher_port,
                                timeout_seconds=settings.dispatcher_request_timeout_seconds,
                                payload={
                                    "op": "oms_query",
                                    "x_api_key": api_key,
                                    "account_id": account_id,
                                    "query": "positions_open",
                                },
                            )
                            positions_items = open_positions.get("result", []) if open_positions.get("ok") else []
                            if not isinstance(positions_items, list):
                                positions_items = []
                            snapshot_meta[-1]["open_positions"] = len(positions_items)
                            if open_positions.get("ok"):
                                await websocket.send_json(
                                    {
                                        "id": None,
                                        "ok": True,
                                        "type": "ws_event",
                                        "namespace": "position",
                                        "action": "snapshot",
                                        "event": "snapshot_open_positions",
                                        "payload": {
                                            "account_id": account_id,
                                            "items": positions_items,
                                        },
                                    }
                                )
                        await websocket.send_json(
                            {
                                "id": None,
                                "ok": True,
                                "type": "ws_event",
                                "namespace": "position",
                                "action": "snapshot",
                                "event": "snapshot_done",
                                "payload": {
                                    "account_ids": sorted(subscribed_accounts),
                                    "accounts": snapshot_meta,
                                },
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
                            "op": "oms_command",
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
