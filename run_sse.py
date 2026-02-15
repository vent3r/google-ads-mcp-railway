"""Secured SSE runner for Google Ads MCP Server on Railway.

Two layers of protection:
1. API Key: every request must include X-Api-Key header
2. IP Allowlist (optional): only accepts connections from known IPs
"""

import os
import logging
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from ads_mcp.coordinator import mcp
from ads_mcp.tools import search, core  # noqa: F401

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SecurityMiddleware(BaseHTTPMiddleware):
    """Validates API key and optionally restricts by IP."""

    async def dispatch(self, request: Request, call_next):
        # --- Layer 1: API Key ---
        expectedKey = os.environ.get("MCP_API_KEY")
        if expectedKey:
            providedKey = request.headers.get("x-api-key", "")
            if providedKey != expectedKey:
                logger.warning(
                    f"Rejected request: invalid API key from {request.client.host}"
                )
                return JSONResponse(
                    {"error": "Unauthorized"}, status_code=401
                )

        # --- Layer 2: IP Allowlist (optional) ---
        allowedIps = os.environ.get("MCP_ALLOWED_IPS", "")
        if allowedIps:
            ipList = [ip.strip() for ip in allowedIps.split(",") if ip.strip()]
            clientIp = request.client.host
            # Also check X-Forwarded-For (Railway proxy)
            forwardedFor = request.headers.get("x-forwarded-for", "")
            forwardedIp = forwardedFor.split(",")[0].strip() if forwardedFor else ""

            if clientIp not in ipList and forwardedIp not in ipList:
                logger.warning(
                    f"Rejected request: IP {clientIp} (forwarded: {forwardedIp}) not in allowlist"
                )
                return JSONResponse(
                    {"error": "Forbidden"}, status_code=403
                )

        return await call_next(request)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))

    # Configure FastMCP
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = port
    mcp.settings.transport_security = None

    # Get the SSE Starlette app from FastMCP
    sseApp = mcp.sse_app()

    # Wrap it with security middleware
    sseApp.add_middleware(SecurityMiddleware)

    print(f"MCP Server starting on 0.0.0.0:{port} (SSE mode, secured)")

    config = uvicorn.Config(
        sseApp,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)

    import anyio
    anyio.run(server.serve)
