"""Secured SSE runner for Google Ads MCP Server on Railway.

Two layers of protection:
1. API Key: every request must include X-Api-Key header
2. IP Allowlist (optional): only accepts connections from known IPs

Uses pure ASGI middleware (not BaseHTTPMiddleware) to avoid
SSE connection conflicts with Starlette's response lifecycle.
"""

import os
import json
import logging

from ads_mcp.coordinator import mcp

# Custom analytics tools — import triggers @mcp.tool() registration
from tools import clients, campaigns, adgroups, keywords  # noqa: F401
from tools import search_terms, ngrams, anomalies  # noqa: F401
from tools import change_history, conversion_setup, run_gaql  # noqa: F401
from tools import keyword_ideas  # noqa: F401

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Log registered tools at import time
try:
    tool_names = sorted(mcp._tool_manager._tools.keys())
    logger.info(f"Registered {len(tool_names)} tools: {tool_names}")
except Exception:
    pass


class SecurityASGI:
    """Pure ASGI middleware for API key + IP allowlist.

    Unlike BaseHTTPMiddleware, this does NOT wrap responses in
    StreamingHttpResponse, so SSE connections work correctly.
    """

    def __init__(self, app):
        self.app = app
        self.expectedKey = os.environ.get("MCP_API_KEY", "")
        rawIps = os.environ.get("MCP_ALLOWED_IPS", "")
        self.allowedIps = [ip.strip() for ip in rawIps.split(",") if ip.strip()]

    async def __call__(self, scope, receive, send):
        # Only check HTTP requests (not lifespan, websocket, etc.)
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        headers = dict(scope.get("headers", []))

        # --- Layer 1: API Key ---
        if self.expectedKey:
            providedKey = headers.get(b"x-api-key", b"").decode()
            if providedKey != self.expectedKey:
                clientHost = scope.get("client", ("?", 0))[0]
                logger.warning(
                    f"Rejected request: invalid API key from {clientHost}"
                )
                return await self._send_json(send, 401, {"error": "Unauthorized"})

        # --- Layer 2: IP Allowlist (optional) ---
        if self.allowedIps:
            clientHost = scope.get("client", ("", 0))[0]
            forwardedFor = headers.get(b"x-forwarded-for", b"").decode()
            forwardedIp = forwardedFor.split(",")[0].strip() if forwardedFor else ""

            if clientHost not in self.allowedIps and forwardedIp not in self.allowedIps:
                logger.warning(
                    f"Rejected request: IP {clientHost} "
                    f"(forwarded: {forwardedIp}) not in allowlist"
                )
                return await self._send_json(send, 403, {"error": "Forbidden"})

        # Auth passed — forward to MCP app untouched
        return await self.app(scope, receive, send)

    @staticmethod
    async def _send_json(send, status: int, body: dict):
        """Send a simple JSON error response."""
        payload = json.dumps(body).encode()
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                [b"content-type", b"application/json"],
                [b"content-length", str(len(payload)).encode()],
            ],
        })
        await send({
            "type": "http.response.body",
            "body": payload,
        })


if __name__ == "__main__":
    import uvicorn
    import anyio

    port = int(os.environ.get("PORT", 8080))

    # Configure FastMCP
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = port
    mcp.settings.transport_security = None

    # Get the SSE Starlette app from FastMCP
    sseApp = mcp.sse_app()

    # Wrap with pure ASGI security (SSE-safe)
    securedApp = SecurityASGI(sseApp)

    print(f"MCP Server starting on 0.0.0.0:{port} (SSE mode, secured)")

    config = uvicorn.Config(
        securedApp,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    anyio.run(server.serve)
