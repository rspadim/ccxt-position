import argparse
import asyncio
import json
from pathlib import Path
from typing import Any
from urllib import request as urllib_request

from cryptography.fernet import Fernet

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


def _post_position_command(
    base_url: str,
    api_key: str,
    account_id: int,
    command: str,
    payload: dict[str, Any],
) -> None:
    url = f"{base_url.rstrip('/')}/position/commands"
    req = {"account_id": account_id, "command": command, "payload": payload}
    out = _http_json("POST", url, _default_headers(api_key), req)
    _print_json(out)


def cmd_send_order(args: argparse.Namespace) -> None:
    payload: dict[str, Any] = {
        "symbol": args.symbol,
        "side": args.side,
        "order_type": args.order_type,
        "qty": str(args.qty),
        "magic_id": args.magic_id,
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
        "magic_id": args.magic_id,
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
        "magic_id": args.magic_id,
    }
    _post_position_command(args.base_url, args.api_key, args.account_id, "close_by", payload)


def cmd_reassign_position(args: argparse.Namespace) -> None:
    url = f"{args.base_url.rstrip('/')}/position/reassign"
    req = {
        "account_id": args.account_id,
        "deal_ids": args.deal_ids,
        "order_ids": args.order_ids,
        "target_magic_id": args.target_magic_id,
        "target_position_id": args.target_position_id,
    }
    out = _http_json("POST", url, _default_headers(args.api_key), req)
    _print_json(out)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ccxt-position-cli")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_key = sub.add_parser("generate-master-key", help="Generate Fernet master key")
    p_key.set_defaults(func=cmd_generate_master_key)

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
    p_send.add_argument("--magic-id", type=int, default=0)
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
    p_close.add_argument("--magic-id", type=int, default=0)
    p_close.set_defaults(func=cmd_close_position)

    p_close_by = sub.add_parser("close-by", parents=[common], help="Close two opposite positions internally")
    p_close_by.add_argument("--position-id-a", type=int, required=True)
    p_close_by.add_argument("--position-id-b", type=int, required=True)
    p_close_by.add_argument("--magic-id", type=int, default=0)
    p_close_by.set_defaults(func=cmd_close_by)

    p_reassign = sub.add_parser("reassign-position", parents=[common], help="Reassign deals/orders to magic/position")
    p_reassign.add_argument("--deal-ids", nargs="*", type=int, default=[])
    p_reassign.add_argument("--order-ids", nargs="*", type=int, default=[])
    p_reassign.add_argument("--target-magic-id", type=int, default=0)
    p_reassign.add_argument("--target-position-id", type=int, default=0)
    p_reassign.set_defaults(func=cmd_reassign_position)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

