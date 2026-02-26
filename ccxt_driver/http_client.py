import json
from typing import Any, Callable
from urllib import parse as urllib_parse
from urllib import request as urllib_request


TransportFn = Callable[[str, str, dict[str, str], dict[str, Any] | None], dict[str, Any]]


class OmsHttpClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout_seconds: int = 30,
        transport: TransportFn | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.timeout_seconds = int(timeout_seconds)
        self._transport = transport or self._default_transport

    def request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        clean_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{clean_path}"
        if query:
            encoded = urllib_parse.urlencode(query, doseq=True)
            if encoded:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{encoded}"
        headers = {"x-api-key": self.api_key}
        return self._transport(method.upper(), url, headers, payload)

    def _default_transport(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        body = None
        req_headers = dict(headers)
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        req = urllib_request.Request(url=url, data=body, headers=req_headers, method=method)
        with urllib_request.urlopen(req, timeout=self.timeout_seconds) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)
