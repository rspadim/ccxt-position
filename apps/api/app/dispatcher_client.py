import asyncio
import json
from typing import Any

DISPATCH_STREAM_LIMIT_BYTES = 8 * 1024 * 1024


async def dispatch_request(
    host: str,
    port: int,
    payload: dict[str, Any],
    timeout_seconds: int | None = 30,
) -> dict[str, Any]:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter | None = None
    try:
        if timeout_seconds is None:
            reader, writer = await asyncio.open_connection(
                host=host,
                port=port,
                limit=DISPATCH_STREAM_LIMIT_BYTES,
            )
        else:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=host,
                    port=port,
                    limit=DISPATCH_STREAM_LIMIT_BYTES,
                ),
                timeout=max(1, int(timeout_seconds)),
            )
        writer.write((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
        await writer.drain()
        if timeout_seconds is None:
            raw = await reader.readline()
        else:
            raw = await asyncio.wait_for(reader.readline(), timeout=max(1, int(timeout_seconds)))
        if not raw:
            return {"ok": False, "error": {"code": "dispatcher_empty_response"}}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            return {"ok": False, "error": {"code": "dispatcher_invalid_json", "message": str(exc)}}
    except TimeoutError:
        return {"ok": False, "error": {"code": "dispatcher_timeout"}}
    except Exception as exc:
        return {"ok": False, "error": {"code": "dispatcher_unavailable", "message": str(exc)}}
    finally:
        if writer is not None:
            writer.close()
            await writer.wait_closed()
