import hashlib
from dataclasses import dataclass
from typing import Any

from fastapi import Header, HTTPException, Request, status


@dataclass(slots=True)
class AuthContext:
    user_id: int
    api_key_id: int
    role: str

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


async def get_auth_context(
    request: Request,
    x_api_key: str | None = Header(default=None),
) -> AuthContext:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "missing_api_key", "message": "x-api-key is required"},
        )

    ctx = await validate_api_key(request.app.state.db, x_api_key)
    if ctx is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_api_key", "message": "invalid api key"},
        )
    return ctx


async def validate_api_key(db: Any, raw_api_key: str) -> AuthContext | None:
    key_hash = hashlib.sha256(raw_api_key.encode("utf-8")).hexdigest()
    async with db.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT k.id, k.user_id, u.role
                FROM user_api_keys k
                JOIN users u ON u.id = k.user_id
                WHERE k.api_key_hash = %s AND k.status = 'active' AND u.status = 'active'
                LIMIT 1
                """,
                (key_hash,),
            )
            row = await cur.fetchone()
            if row is None:
                await cur.execute(
                    """
                    SELECT t.api_key_id, t.user_id, u.role
                    FROM auth_tokens t
                    JOIN users u ON u.id = t.user_id
                    JOIN user_api_keys k ON k.id = t.api_key_id
                    WHERE t.token_hash = %s
                      AND t.status = 'active'
                      AND (t.expires_at IS NULL OR t.expires_at > NOW())
                      AND u.status = 'active'
                      AND k.status = 'active'
                    LIMIT 1
                    """,
                    (key_hash,),
                )
                row = await cur.fetchone()
        await conn.commit()
    if row is None:
        return None
    return AuthContext(api_key_id=int(row[0]), user_id=int(row[1]), role=str(row[2] or "trader"))
