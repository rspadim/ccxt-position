import argparse
import asyncio
import datetime as dt
import hashlib
import json
import secrets
from pathlib import Path
from typing import Any
from urllib import request as urllib_request

from cryptography.fernet import Fernet
from pymysql.err import OperationalError

from .app.config import load_settings
from .app.credentials_codec import CredentialsCodec
from .app.db_mysql import DatabaseMySQL


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=True))


def _http_json(
    method: str,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    body = None
    req_headers = dict(headers)
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib_request.Request(url=url, data=body, headers=req_headers, method=method)
    with urllib_request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        if not raw:
            return {}
        return json.loads(raw)


def _default_headers(api_key: str) -> dict[str, str]:
    return {"x-api-key": api_key}


def _date_window(days_back: int = 1) -> tuple[str, str]:
    end_date = dt.datetime.now(dt.UTC).date()
    start_date = end_date - dt.timedelta(days=max(0, int(days_back)))
    return start_date.isoformat(), end_date.isoformat()


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    for raw_line in sql_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        current.append(raw_line)
        if line.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
    tail = "\n".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


async def _apply_schema(sql_dir: Path) -> list[str]:
    files = sorted([p for p in sql_dir.glob("*.sql") if p.is_file()])
    if not files:
        raise SystemExit(f"no sql files found in {sql_dir}")

    settings = load_settings()
    db = DatabaseMySQL(settings)
    await db.connect()
    executed_files: list[str] = []
    try:
        async with db.connection() as conn:
            async with conn.cursor() as cur:
                for sql_file in files:
                    content = sql_file.read_text(encoding="utf-8")
                    for stmt in _split_sql_statements(content):
                        try:
                            await cur.execute(stmt)
                        except OperationalError as exc:
                            code = int(exc.args[0]) if exc.args else 0
                            # Idempotent bootstrap: ignore "already exists"-style DDL errors.
                            if code not in {1050, 1060, 1061, 1091}:
                                raise
                    executed_files.append(sql_file.name)
            await conn.commit()
    finally:
        await db.disconnect()

    return executed_files


async def _create_user(name: str) -> int:
    settings = load_settings()
    db = DatabaseMySQL(settings)
    await db.connect()
    try:
        async with db.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO users (name, status)
                    VALUES (%s, 'active')
                    """,
                    (name,),
                )
                user_id = int(cur.lastrowid)
            await conn.commit()
    finally:
        await db.disconnect()
    return user_id


async def _create_api_key_for_user(user_id: int, api_key_plain: str | None, label: str | None = None) -> tuple[int, str]:
    key_plain = api_key_plain or secrets.token_urlsafe(32)
    key_hash = _sha256_hex(key_plain)

    settings = load_settings()
    db = DatabaseMySQL(settings)
    await db.connect()
    try:
        async with db.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO user_api_keys (user_id, label, api_key_hash, status)
                    VALUES (%s, %s, %s, 'active')
                    """,
                    (user_id, label, key_hash),
                )
                key_id = int(cur.lastrowid)
                await cur.execute(
                    """
                    INSERT INTO api_key_account_permissions (
                        api_key_id, account_id, can_read, can_trade, can_close_position,
                        can_risk_manage, can_block_new_positions, can_block_account, restrict_to_strategies, status
                    )
                    SELECT
                        %s,
                        uap.account_id,
                        uap.can_read,
                        uap.can_trade,
                        uap.can_trade,
                        uap.can_risk_manage,
                        uap.can_risk_manage,
                        uap.can_risk_manage,
                        FALSE,
                        'active'
                    FROM user_account_permissions uap
                    WHERE uap.user_id = %s
                    ON DUPLICATE KEY UPDATE
                        can_read = VALUES(can_read),
                        can_trade = VALUES(can_trade),
                        can_close_position = VALUES(can_close_position),
                        can_risk_manage = VALUES(can_risk_manage),
                        can_block_new_positions = VALUES(can_block_new_positions),
                        can_block_account = VALUES(can_block_account),
                        status = VALUES(status)
                    """,
                    (key_id, user_id),
                )
            await conn.commit()
    finally:
        await db.disconnect()

    return key_id, key_plain


async def _create_account_and_permission(
    user_id: int,
    exchange_id: str,
    label: str,
    position_mode: str,
    pool_id: int,
    is_testnet: bool,
    can_read: bool,
    can_trade: bool,
    can_risk_manage: bool,
) -> int:
    settings = load_settings()
    db = DatabaseMySQL(settings)
    await db.connect()
    try:
        async with db.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO accounts (
                        exchange_id, is_testnet, label, position_mode, pool_id, status
                    ) VALUES (%s, %s, %s, %s, %s, 'active')
                    """,
                    (exchange_id, is_testnet, label, position_mode, pool_id),
                )
                account_id = int(cur.lastrowid)
                await cur.execute(
                    """
                    INSERT INTO user_account_permissions (
                        user_id, account_id, can_read, can_trade, can_risk_manage
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        can_read = VALUES(can_read),
                        can_trade = VALUES(can_trade),
                        can_risk_manage = VALUES(can_risk_manage)
                    """,
                    (user_id, account_id, can_read, can_trade, can_risk_manage),
                )
                await cur.execute(
                    """
                    INSERT INTO api_key_account_permissions (
                        api_key_id, account_id, can_read, can_trade, can_close_position,
                        can_risk_manage, can_block_new_positions, can_block_account, restrict_to_strategies, status
                    )
                    SELECT
                        uak.id,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        FALSE,
                        'active'
                    FROM user_api_keys uak
                    WHERE uak.user_id = %s
                      AND uak.status = 'active'
                    ON DUPLICATE KEY UPDATE
                        can_read = VALUES(can_read),
                        can_trade = VALUES(can_trade),
                        can_close_position = VALUES(can_close_position),
                        can_risk_manage = VALUES(can_risk_manage),
                        can_block_new_positions = VALUES(can_block_new_positions),
                        can_block_account = VALUES(can_block_account),
                        status = VALUES(status)
                    """,
                    (
                        account_id,
                        bool(can_read),
                        bool(can_trade),
                        bool(can_trade),
                        bool(can_risk_manage),
                        bool(can_risk_manage),
                        bool(can_risk_manage),
                        user_id,
                    ),
                )
            await conn.commit()
    finally:
        await db.disconnect()
    return account_id


async def _upsert_account_credentials(
    account_id: int,
    api_key: str | None,
    secret: str | None,
    passphrase: str | None,
    encrypt_input: bool,
) -> None:
    settings = load_settings()
    codec = CredentialsCodec(
        settings.encryption_master_key,
        require_encrypted=settings.require_encrypted_credentials,
    )

    def encode(value: str | None) -> str | None:
        if value is None:
            return None
        if value.startswith("enc:v1:"):
            return value
        if encrypt_input:
            return codec.encrypt(value)
        return value

    api_key_db = encode(api_key)
    secret_db = encode(secret)
    passphrase_db = encode(passphrase)

    db = DatabaseMySQL(settings)
    await db.connect()
    try:
        async with db.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO account_credentials_encrypted (
                        account_id, api_key_enc, secret_enc, passphrase_enc
                    ) VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        api_key_enc = VALUES(api_key_enc),
                        secret_enc = VALUES(secret_enc),
                        passphrase_enc = VALUES(passphrase_enc),
                        updated_at = NOW()
                    """,
                    (account_id, api_key_db, secret_db, passphrase_db),
                )
            await conn.commit()
    finally:
        await db.disconnect()

    _print_json(
        {
            "ok": True,
            "account_id": account_id,
            "stored": {
                "api_key_enc": bool(api_key_db),
                "secret_enc": bool(secret_db),
                "passphrase_enc": passphrase_db is not None,
            },
        }
    )


async def _set_account_testnet(account_id: int, is_testnet: bool) -> None:
    settings = load_settings()
    db = DatabaseMySQL(settings)
    await db.connect()
    try:
        async with db.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE accounts
                    SET is_testnet = %s
                    WHERE id = %s
                    """,
                    (is_testnet, account_id),
                )
                changed = int(cur.rowcount or 0)
            await conn.commit()
    finally:
        await db.disconnect()
    _print_json({"ok": True, "account_id": account_id, "is_testnet": is_testnet, "rows": changed})


def cmd_generate_master_key(_args: argparse.Namespace) -> None:
    key = Fernet.generate_key().decode("utf-8")
    _print_json({"encryption_master_key": key})


def cmd_encrypt(args: argparse.Namespace) -> None:
    settings = load_settings()
    codec = CredentialsCodec(
        settings.encryption_master_key,
        require_encrypted=settings.require_encrypted_credentials,
    )
    encrypted = codec.encrypt(args.value)
    _print_json({"encrypted": encrypted})


def cmd_upsert_account_credentials(args: argparse.Namespace) -> None:
    asyncio.run(
        _upsert_account_credentials(
            account_id=args.account_id,
            api_key=args.api_key,
            secret=args.secret,
            passphrase=args.passphrase,
            encrypt_input=args.encrypt_input,
        )
    )


def cmd_set_account_testnet(args: argparse.Namespace) -> None:
    enabled: bool
    if args.enabled and args.disabled:
        raise SystemExit("use only one flag: --enabled or --disabled")
    if not args.enabled and not args.disabled:
        raise SystemExit("one flag is required: --enabled or --disabled")
    enabled = bool(args.enabled and not args.disabled)
    asyncio.run(_set_account_testnet(args.account_id, enabled))


def cmd_install(args: argparse.Namespace) -> None:
    async def _run() -> dict[str, Any]:
        out: dict[str, Any] = {"ok": True}

        if not args.skip_schema:
            executed = await _apply_schema(Path(args.sql_dir))
            out["schema_files"] = executed

        user_id = await _create_user(args.admin_name)
        key_id, plain_key = await _create_api_key_for_user(user_id, args.api_key, args.api_key_label)
        out["user"] = {"id": user_id, "name": args.admin_name}
        out["api_key"] = {"id": key_id, "plain": plain_key, "label": args.api_key_label or ""}

        if args.with_account:
            account_id = await _create_account_and_permission(
                user_id=user_id,
                exchange_id=args.exchange_id,
                label=args.label,
                position_mode=args.position_mode,
                pool_id=args.pool_id,
                is_testnet=args.testnet,
                can_read=True,
                can_trade=True,
                can_risk_manage=True,
            )
            out["account"] = {
                "id": account_id,
                "exchange_id": args.exchange_id,
                "label": args.label,
                "position_mode": args.position_mode,
                "is_testnet": args.testnet,
            }

        return out

    _print_json(asyncio.run(_run()))


def cmd_create_user(args: argparse.Namespace) -> None:
    user_id = asyncio.run(_create_user(args.name))
    _print_json({"ok": True, "user_id": user_id, "name": args.name})


def cmd_create_api_key(args: argparse.Namespace) -> None:
    key_id, plain_key = asyncio.run(_create_api_key_for_user(args.user_id, args.api_key, args.label))
    _print_json({"ok": True, "user_id": args.user_id, "api_key_id": key_id, "api_key": plain_key, "label": args.label or ""})


def cmd_add_account(args: argparse.Namespace) -> None:
    account_id = asyncio.run(
        _create_account_and_permission(
            user_id=args.user_id,
            exchange_id=args.exchange_id,
            label=args.label,
            position_mode=args.position_mode,
            pool_id=args.pool_id,
            is_testnet=args.testnet,
            can_read=not args.no_read,
            can_trade=not args.read_only,
            can_risk_manage=args.can_risk_manage,
        )
    )
    _print_json(
        {
            "ok": True,
            "account_id": account_id,
            "user_id": args.user_id,
            "exchange_id": args.exchange_id,
            "label": args.label,
        }
    )


def _post_position_command(
    base_url: str,
    api_key: str,
    account_id: int,
    command: str,
    payload: dict[str, Any],
) -> None:
    url = f"{base_url.rstrip('/')}/oms/commands"
    req = {"account_id": account_id, "command": command, "payload": payload}
    out = _http_json("POST", url, _default_headers(api_key), req)
    _print_json(out)


def cmd_send_order(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = {
        "symbol": args.symbol,
        "side": args.side,
        "order_type": args.order_type,
        "qty": str(args.qty),
        "strategy_id": args.strategy_id,
        "position_id": args.position_id,
    }
    if args.price is not None:
        payload["price"] = str(args.price)
    _post_position_command(args.base_url, args.api_key, args.account_id, "send_order", payload)


def cmd_change_order(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = {"order_id": args.order_id}
    if args.new_price is not None:
        payload["new_price"] = str(args.new_price)
    if args.new_qty is not None:
        payload["new_qty"] = str(args.new_qty)
    _post_position_command(args.base_url, args.api_key, args.account_id, "change_order", payload)


def cmd_cancel_order(args: argparse.Namespace) -> None:
    payload = {"order_id": args.order_id}
    _post_position_command(args.base_url, args.api_key, args.account_id, "cancel_order", payload)


def cmd_close_position(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = {
        "position_id": args.position_id,
        "order_type": args.order_type,
        "strategy_id": args.strategy_id,
    }
    if args.price is not None:
        payload["price"] = str(args.price)
    _post_position_command(
        args.base_url, args.api_key, args.account_id, "close_position", payload
    )


def cmd_close_by(args: argparse.Namespace) -> None:
    payload = {
        "position_id_a": args.position_id_a,
        "position_id_b": args.position_id_b,
        "strategy_id": args.strategy_id,
    }
    _post_position_command(args.base_url, args.api_key, args.account_id, "close_by", payload)


def cmd_reassign_position(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/oms/reassign"
    req = {
        "account_id": args.account_id,
        "deal_ids": args.deal_ids,
        "order_ids": args.order_ids,
        "target_strategy_id": args.target_strategy_id,
        "target_position_id": args.target_position_id,
    }
    out = _http_json("POST", url, _default_headers(args.api_key), req)
    _print_json(out)


def cmd_healthz(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/healthz"
    out = _http_json("GET", url, {})
    _print_json(out)


def cmd_dispatcher_status(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/dispatcher/status"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_list_accounts(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/oms/accounts"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_list_strategies(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/strategies"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_create_strategy(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = {"name": args.name, "account_ids": args.account_ids}
    if args.client_strategy_id is not None:
        payload["client_strategy_id"] = int(args.client_strategy_id)
    url = f"{args.base_url.rstrip('/')}/strategies"
    out = _http_json("POST", url, _default_headers(args.api_key), payload)
    _print_json(out)


def cmd_reconcile(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = {"scope": args.scope}
    if args.account_id is not None:
        payload["account_id"] = int(args.account_id)
    if args.account_ids:
        payload["account_ids"] = args.account_ids
    if args.start_date:
        payload["start_date"] = args.start_date
    if args.end_date:
        payload["end_date"] = args.end_date
    if args.symbols_hint:
        payload["symbols_hint"] = args.symbols_hint
    url = f"{args.base_url.rstrip('/')}/oms/reconcile"
    out = _http_json("POST", url, _default_headers(args.api_key), payload)
    _print_json(out)


def cmd_reconcile_status(args: argparse.Namespace) -> None:
    base = args.base_url.rstrip("/")
    if args.account_id is not None:
        url = (
            f"{base}/oms/reconcile/{int(args.account_id)}/status"
            f"?stale_after_seconds={int(args.stale_after_seconds)}"
        )
        out = _http_json("GET", url, _default_headers(args.api_key))
        _print_json(out)
        return

    qs = f"stale_after_seconds={int(args.stale_after_seconds)}"
    if args.status:
        qs = f"{qs}&status={args.status}"
    url = f"{base}/oms/reconcile/status?{qs}"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def _resolve_date_window(args: argparse.Namespace) -> tuple[str, str]:
    if args.start_date and args.end_date:
        return str(args.start_date), str(args.end_date)
    return _date_window(days_back=args.days_back)


def cmd_orders_open(args: argparse.Namespace) -> None:
    url = (
        f"{args.base_url.rstrip('/')}/oms/orders/open?"
        f"account_ids={args.account_ids}&limit={int(args.limit)}"
    )
    if args.strategy_id is not None:
        url += f"&strategy_id={int(args.strategy_id)}"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_orders_history(args: argparse.Namespace) -> None:
    start_date, end_date = _resolve_date_window(args)
    url = (
        f"{args.base_url.rstrip('/')}/oms/orders/history?"
        f"account_ids={args.account_ids}&start_date={start_date}&end_date={end_date}"
        f"&page={int(args.page)}&page_size={int(args.page_size)}"
    )
    if args.strategy_id is not None:
        url += f"&strategy_id={int(args.strategy_id)}"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_deals(args: argparse.Namespace) -> None:
    start_date, end_date = _resolve_date_window(args)
    url = (
        f"{args.base_url.rstrip('/')}/oms/deals?"
        f"account_ids={args.account_ids}&start_date={start_date}&end_date={end_date}"
        f"&page={int(args.page)}&page_size={int(args.page_size)}"
    )
    if args.strategy_id is not None:
        url += f"&strategy_id={int(args.strategy_id)}"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_positions_open(args: argparse.Namespace) -> None:
    url = (
        f"{args.base_url.rstrip('/')}/oms/positions/open?"
        f"account_ids={args.account_ids}&limit={int(args.limit)}"
    )
    if args.strategy_id is not None:
        url += f"&strategy_id={int(args.strategy_id)}"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def cmd_positions_history(args: argparse.Namespace) -> None:
    start_date, end_date = _resolve_date_window(args)
    url = (
        f"{args.base_url.rstrip('/')}/oms/positions/history?"
        f"account_ids={args.account_ids}&start_date={start_date}&end_date={end_date}"
        f"&page={int(args.page)}&page_size={int(args.page_size)}"
    )
    if args.strategy_id is not None:
        url += f"&strategy_id={int(args.strategy_id)}"
    out = _http_json("GET", url, _default_headers(args.api_key))
    _print_json(out)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ccxt-position-cli",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_install = sub.add_parser(
        "install",
        help="Beginner setup: apply schema, create admin user, create internal API key",
    )
    p_install.add_argument("--sql-dir", default="sql")
    p_install.add_argument("--skip-schema", action="store_true")
    p_install.add_argument("--admin-name", default="admin")
    p_install.add_argument("--api-key", help="Optional fixed internal API key; generated if omitted")
    p_install.add_argument("--api-key-label", help="Optional label for internal API key")
    p_install.add_argument("--with-account", action="store_true")
    p_install.add_argument("--exchange-id", default="binance")
    p_install.add_argument("--label", default="binance-main")
    p_install.add_argument("--position-mode", choices=["hedge", "netting", "strategy_netting"], default="hedge")
    p_install.add_argument("--pool-id", type=int, default=0)
    p_install.add_argument("--testnet", action="store_true")
    p_install.set_defaults(func=cmd_install)

    p_user = sub.add_parser("create-user", help="Create internal API user")
    p_user.add_argument("--name", required=True)
    p_user.set_defaults(func=cmd_create_user)

    p_key = sub.add_parser("create-api-key", help="Create internal API key for existing user")
    p_key.add_argument("--user-id", type=int, required=True)
    p_key.add_argument("--api-key", help="Optional fixed key; generated if omitted")
    p_key.add_argument("--label", help="Optional label for the API key")
    p_key.set_defaults(func=cmd_create_api_key)

    p_account = sub.add_parser("add-account", help="Create account and grant permission to user")
    p_account.add_argument("--user-id", type=int, required=True)
    p_account.add_argument("--exchange-id", required=True)
    p_account.add_argument("--label", required=True)
    p_account.add_argument("--position-mode", choices=["hedge", "netting", "strategy_netting"], default="hedge")
    p_account.add_argument("--pool-id", type=int, default=0)
    p_account.add_argument("--testnet", action="store_true")
    p_account.add_argument("--no-read", action="store_true")
    p_account.add_argument("--read-only", action="store_true")
    p_account.add_argument("--can-risk-manage", action="store_true")
    p_account.set_defaults(func=cmd_add_account)

    p_gen = sub.add_parser("generate-master-key", help="Generate Fernet master key")
    p_gen.set_defaults(func=cmd_generate_master_key)

    p_enc = sub.add_parser("encrypt", help="Encrypt one credential value")
    p_enc.add_argument("--value", required=True)
    p_enc.set_defaults(func=cmd_encrypt)

    p_upsert = sub.add_parser(
        "upsert-account-credentials",
        help="Insert/update account_credentials_encrypted in MySQL",
    )
    p_upsert.add_argument("--account-id", type=int, required=True)
    p_upsert.add_argument("--api-key")
    p_upsert.add_argument("--secret")
    p_upsert.add_argument("--passphrase")
    p_upsert.add_argument(
        "--encrypt-input",
        action="store_true",
        help="Encrypt plaintext args before storing",
    )
    p_upsert.set_defaults(func=cmd_upsert_account_credentials)

    p_testnet = sub.add_parser(
        "set-account-testnet",
        help="Enable/disable account testnet mode in accounts table",
    )
    p_testnet.add_argument("--account-id", type=int, required=True)
    p_testnet.add_argument("--enabled", action="store_true")
    p_testnet.add_argument("--disabled", action="store_true")
    p_testnet.set_defaults(func=cmd_set_account_testnet)

    # Trading / position commands through REST API
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--base-url", default="http://127.0.0.1:8000")
    common.add_argument("--api-key", required=True)
    common.add_argument("--account-id", type=int, required=True)

    p_send = sub.add_parser("send-order", parents=[common], help="Send new order")
    p_send.add_argument("--symbol", required=True)
    p_send.add_argument("--side", choices=["buy", "sell"], required=True)
    p_send.add_argument("--order-type", choices=["market", "limit"], required=True)
    p_send.add_argument("--qty", required=True)
    p_send.add_argument("--price")
    p_send.add_argument("--strategy-id", type=int, default=0)
    p_send.add_argument("--position-id", type=int, default=0)
    p_send.set_defaults(func=cmd_send_order)

    p_change = sub.add_parser("change-order", parents=[common], help="Change order qty/price")
    p_change.add_argument("--order-id", type=int, required=True)
    p_change.add_argument("--new-price")
    p_change.add_argument("--new-qty")
    p_change.set_defaults(func=cmd_change_order)

    p_cancel = sub.add_parser("cancel-order", parents=[common], help="Cancel open order")
    p_cancel.add_argument("--order-id", type=int, required=True)
    p_cancel.set_defaults(func=cmd_cancel_order)

    p_close = sub.add_parser("close-position", parents=[common], help="Close one position")
    p_close.add_argument("--position-id", type=int, required=True)
    p_close.add_argument("--order-type", choices=["market", "limit"], default="market")
    p_close.add_argument("--price")
    p_close.add_argument("--strategy-id", type=int, default=0)
    p_close.set_defaults(func=cmd_close_position)

    p_close_by = sub.add_parser("close-by", parents=[common], help="Close two opposite positions internally")
    p_close_by.add_argument("--position-id-a", type=int, required=True)
    p_close_by.add_argument("--position-id-b", type=int, required=True)
    p_close_by.add_argument("--strategy-id", type=int, default=0)
    p_close_by.set_defaults(func=cmd_close_by)

    p_reassign = sub.add_parser("reassign-position", parents=[common], help="Reassign deals/orders to strategy/position")
    p_reassign.add_argument("--deal-ids", nargs="*", type=int, default=[])
    p_reassign.add_argument("--order-ids", nargs="*", type=int, default=[])
    p_reassign.add_argument("--target-strategy-id", type=int, default=0)
    p_reassign.add_argument("--target-position-id", type=int, default=0)
    p_reassign.set_defaults(func=cmd_reassign_position)

    # Operational and introspection helpers
    common_api = argparse.ArgumentParser(add_help=False)
    common_api.add_argument("--base-url", default="http://127.0.0.1:8000")
    common_api.add_argument("--api-key", required=True)

    p_healthz = sub.add_parser("healthz", help="Check API health endpoint")
    p_healthz.add_argument("--base-url", default="http://127.0.0.1:8000")
    p_healthz.set_defaults(func=cmd_healthz)

    p_dispatcher_status = sub.add_parser(
        "dispatcher-status",
        parents=[common_api],
        help="Fetch dispatcher pool/worker status",
    )
    p_dispatcher_status.set_defaults(func=cmd_dispatcher_status)

    p_accounts_list = sub.add_parser(
        "list-accounts",
        parents=[common_api],
        help="List OMS accounts visible by this API key",
    )
    p_accounts_list.set_defaults(func=cmd_list_accounts)

    p_strategies_list = sub.add_parser(
        "list-strategies",
        parents=[common_api],
        help="List strategies visible by this API key",
    )
    p_strategies_list.set_defaults(func=cmd_list_strategies)

    p_strategies_create = sub.add_parser(
        "create-strategy",
        parents=[common_api],
        help="Create strategy",
    )
    p_strategies_create.add_argument("--name", required=True)
    p_strategies_create.add_argument("--account-ids", type=int, nargs="+", required=True)
    p_strategies_create.add_argument("--client-strategy-id", type=int)
    p_strategies_create.set_defaults(func=cmd_create_strategy)

    p_reconcile = sub.add_parser(
        "reconcile",
        parents=[common_api],
        help="Trigger OMS reconcile for one or many accounts",
    )
    p_reconcile.add_argument("--account-id", type=int)
    p_reconcile.add_argument("--account-ids", type=int, nargs="*")
    p_reconcile.add_argument("--scope", choices=["short", "long", "period"], default="short")
    p_reconcile.add_argument("--start-date")
    p_reconcile.add_argument("--end-date")
    p_reconcile.add_argument("--symbols-hint", nargs="*")
    p_reconcile.set_defaults(func=cmd_reconcile)

    p_reconcile_status = sub.add_parser(
        "reconcile-status",
        parents=[common_api],
        help="Read reconcile status (global list or specific account)",
    )
    p_reconcile_status.add_argument("--account-id", type=int)
    p_reconcile_status.add_argument("--status", choices=["fresh", "stale", "never"])
    p_reconcile_status.add_argument("--stale-after-seconds", type=int, default=120)
    p_reconcile_status.set_defaults(func=cmd_reconcile_status)

    common_oms_query = argparse.ArgumentParser(add_help=False)
    common_oms_query.add_argument("--base-url", default="http://127.0.0.1:8000")
    common_oms_query.add_argument("--api-key", required=True)
    common_oms_query.add_argument("--account-ids", required=True)
    common_oms_query.add_argument("--strategy-id", type=int)

    p_orders_open = sub.add_parser(
        "orders-open",
        parents=[common_oms_query],
        help="Query open orders",
    )
    p_orders_open.add_argument("--limit", type=int, default=500)
    p_orders_open.set_defaults(func=cmd_orders_open)

    p_orders_history = sub.add_parser(
        "orders-history",
        parents=[common_oms_query],
        help="Query historical orders",
    )
    p_orders_history.add_argument("--start-date")
    p_orders_history.add_argument("--end-date")
    p_orders_history.add_argument("--days-back", type=int, default=1)
    p_orders_history.add_argument("--page", type=int, default=1)
    p_orders_history.add_argument("--page-size", type=int, default=100)
    p_orders_history.set_defaults(func=cmd_orders_history)

    p_deals = sub.add_parser(
        "deals",
        parents=[common_oms_query],
        help="Query historical deals",
    )
    p_deals.add_argument("--start-date")
    p_deals.add_argument("--end-date")
    p_deals.add_argument("--days-back", type=int, default=1)
    p_deals.add_argument("--page", type=int, default=1)
    p_deals.add_argument("--page-size", type=int, default=100)
    p_deals.set_defaults(func=cmd_deals)

    p_positions_open = sub.add_parser(
        "positions-open",
        parents=[common_oms_query],
        help="Query open positions",
    )
    p_positions_open.add_argument("--limit", type=int, default=500)
    p_positions_open.set_defaults(func=cmd_positions_open)

    p_positions_history = sub.add_parser(
        "positions-history",
        parents=[common_oms_query],
        help="Query historical positions",
    )
    p_positions_history.add_argument("--start-date")
    p_positions_history.add_argument("--end-date")
    p_positions_history.add_argument("--days-back", type=int, default=1)
    p_positions_history.add_argument("--page", type=int, default=1)
    p_positions_history.add_argument("--page-size", type=int, default=100)
    p_positions_history.set_defaults(func=cmd_positions_history)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

