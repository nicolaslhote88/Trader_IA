"""Transport World Monitor configurable (MCP, API ou self-hosted)."""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

import httpx


class WorldMonitorError(RuntimeError):
    def __init__(self, code: str, detail: str, *, status_code: int | None = None):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.status_code = status_code


def canonical_hash(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def redact(value: str, secret: str) -> str:
    text = str(value)
    if secret:
        text = text.replace(secret, "[REDACTED]")
    for marker in ("wm_live_", "wm_oat_", "wm_ort_"):
        start = text.find(marker)
        while start >= 0:
            end = start + len(marker)
            while end < len(text) and (text[end].isalnum() or text[end] in "_-"):
                end += 1
            text = text[:start] + "[REDACTED]" + text[end:]
            start = text.find(marker, start + len("[REDACTED]"))
    return text[:2000]


def _decode_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        messages = []
        for line in response.text.splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
                if raw and raw != "[DONE]":
                    messages.append(json.loads(raw))
        return messages[-1] if messages else {}
    return response.json()


class WorldMonitorClient:
    def __init__(self) -> None:
        self.mode = os.environ.get("WORLD_MONITOR_MODE", "mcp").strip().lower()
        default_url = "https://worldmonitor.app/mcp" if self.mode == "mcp" else "https://api.worldmonitor.app"
        self.base_url = os.environ.get("WORLD_MONITOR_BASE_URL", default_url).rstrip("/")
        self.api_key = os.environ.get("WORLD_MONITOR_API_KEY", "")
        self.timeout = float(os.environ.get("WORLD_MONITOR_TIMEOUT_SECONDS", "30"))
        self.max_retries = max(0, int(os.environ.get("WORLD_MONITOR_MAX_RETRIES", "2")))

    @property
    def enabled(self) -> bool:
        return os.environ.get("WORLD_MONITOR_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json, text/event-stream"}
        if self.api_key:
            headers["X-WorldMonitor-Key"] = self.api_key
        return headers

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            for attempt in range(self.max_retries + 1):
                try:
                    response = await client.request(method, url, headers=self._headers(), **kwargs)
                    if response.status_code == 429:
                        raise WorldMonitorError("QUOTA_OR_RATE_LIMIT", "World Monitor returned HTTP 429", status_code=429)
                    if response.status_code in {401, 403}:
                        raise WorldMonitorError("AUTH_OR_TIER", f"World Monitor returned HTTP {response.status_code}", status_code=response.status_code)
                    response.raise_for_status()
                    return response
                except WorldMonitorError:
                    raise
                except Exception as exc:
                    last_error = exc
                    if attempt >= self.max_retries:
                        break
            detail = redact(str(last_error), self.api_key)
            raise WorldMonitorError("TRANSPORT_ERROR", detail)

    async def _mcp(self, method: str, params: dict | None = None) -> Any:
        body = {"jsonrpc": "2.0", "id": canonical_hash([method, params])[:12], "method": method}
        if params is not None:
            body["params"] = params
        response = await self._request("POST", self.base_url, json=body)
        payload = _decode_response(response)
        if isinstance(payload, dict) and payload.get("error"):
            raise WorldMonitorError("MCP_ERROR", redact(json.dumps(payload["error"]), self.api_key))
        return payload.get("result") if isinstance(payload, dict) else payload

    async def list_tools(self) -> list[dict]:
        if self.mode == "mcp":
            result = await self._mcp("tools/list")
            return list((result or {}).get("tools") or [])
        response = await self._request("GET", f"{self.base_url}/openapi.json")
        spec = _decode_response(response)
        tools = []
        for path, methods in (spec.get("paths") or {}).items():
            for method, definition in methods.items():
                if method.lower() not in {"get", "post"}:
                    continue
                tools.append({"name": definition.get("operationId") or f"{method}_{path}", "description": definition.get("summary"), "inputSchema": definition, "_rest_path": path, "_rest_method": method.upper()})
        return tools

    async def describe_tool(self, tool: dict) -> dict:
        if self.mode == "mcp":
            result = await self._mcp("tools/call", {"name": "describe_tool", "arguments": {"tool_name": tool["name"]}})
            return result if isinstance(result, dict) else {"result": result}
        return tool

    async def call_tool(self, tool: dict, arguments: dict | None = None) -> Any:
        if self.mode == "mcp":
            return await self._mcp("tools/call", {"name": tool["name"], "arguments": arguments or {}})
        path = tool.get("_rest_path")
        if not path:
            raise WorldMonitorError("REST_PATH_MISSING", tool.get("name", "unknown"))
        method = tool.get("_rest_method", "GET")
        url = f"{self.base_url}{path}"
        params = (arguments or {}) if method == "GET" else None
        body = (arguments or {}) if method != "GET" else None
        response = await self._request(method, url, params=params, json=body)
        return _decode_response(response)
