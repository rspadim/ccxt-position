import hashlib
from dataclasses import dataclass

from fastapi import Header, HTTPException, Request, status


@dataclass(slots=True)
class AuthContext:
    user_id: int
    api_key_id: int


async def get_auth_context(
    request: Request,
    x_api_key: str | None = Header(default=None),
) -> AuthContext:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "missing_api_key", "message": "x-api-key is required"},
        )

    db = request.app.state.db
    key_hash = hashlib.sha256(x_api_key.encode("utf-8")).hexdigest()

    async with db.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, user_id
                FROM user_api_keys
                WHERE api_key_hash = %s AND status = 'active'
                LIMIT 1
                """,
                (key_hash,),
            )
            row = await cur.fetchone()
        await conn.commit()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_api_key", "message": "invalid api key"},
        )

    return AuthContext(api_key_id=int(row[0]), user_id=int(row[1]))

