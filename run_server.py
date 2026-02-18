"""Multi-client SSE runner for Google Ads MCP Server on Railway.

Supports two auth methods:
1. X-Api-Key header  (Cursor desktop)
2. ?token= query param (Claude.ai / Claude Mobile)

Uses pure ASGI middleware instead of Starlette BaseHTTPMiddleware
to avoid breaking SSE streaming connections.
"""

import json
import logging
import os
from urllib.parse import parse_qs

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MCP coordinator (singleton FastMCP instance from google-ads-mcp)
# ---------------------------------------------------------------------------
from ads_mcp.coordinator import mcp  # noqa: E402

# ---------------------------------------------------------------------------
# Tool registration — import triggers @mcp.tool() decorator
# ---------------------------------------------------------------------------

# 11 original read tools
from tools import clients, campaigns, adgroups, keywords  # noqa: E402, F401
from tools import search_terms, ngrams, anomalies  # noqa: E402, F401
from tools import change_history, conversion_setup, run_gaql  # noqa: E402, F401
from tools import keyword_ideas  # noqa: E402, F401

# 13 new read tools
try:
    from tools import suggest_negatives, keyword_opportunities  # noqa: F401
    from tools import device_breakdown, ad_analysis, geo_breakdown  # noqa: F401
    from tools import hour_day_analysis, auction_insights  # noqa: F401
    from tools import landing_page_analysis, budget_pacing  # noqa: F401
    from tools import campaign_overview, optimization_suggestions  # noqa: F401
    from tools import duplicate_keywords, qs_breakdown  # noqa: F401
    logger.info("Loaded: 13 new read tools")
except Exception as e:
    logger.error("FAILED to load new read tools: %s", e)

# 15 write tools
try:
    from tools import update_budget, set_campaign_status  # noqa: F401
    from tools import set_adgroup_status, add_negatives  # noqa: F401
    from tools import remove_negatives, set_keyword_status  # noqa: F401
    from tools import update_keyword_bid, create_campaign  # noqa: F401
    from tools import create_adgroup, add_keywords  # noqa: F401
    from tools import create_rsa, set_ad_status  # noqa: F401
    from tools import set_bid_adjustments, create_sitelinks  # noqa: F401
    from tools import set_audience_targeting  # noqa: F401
    logger.info("Loaded: 15 write tools")
except Exception as e:
    logger.error("FAILED to load write tools: %s", e)

# Diagnostic: list all registered tools
tool_names = [t.name for t in mcp._tool_manager.list_tools()]
logger.info("Registered %d tools: %s", len(tool_names), tool_names)


# ---------------------------------------------------------------------------
# Pure ASGI auth middleware (SSE-safe — no BaseHTTPMiddleware)
# ---------------------------------------------------------------------------
class AuthMiddleware:
    """Pure ASGI auth middleware that wraps the app directly.

    Starlette's BaseHTTPMiddleware wraps the response body, which is
    incompatible with SSE (EventSourceResponse).  This class operates at
    the raw ASGI level and never interferes with streaming.

    Auth is enforced on ``/sse`` only.  ``/messages/`` is protected by
    session UUIDs generated server-side (128-bit, unguessable, ephemeral).
    """

    def __init__(self, app):
        self.app = app
        self.api_key = os.environ.get("MCP_API_KEY", "")
        self.auth_token = os.environ.get("MCP_AUTH_TOKEN", "")
        self.require_auth = (
            os.environ.get("MCP_REQUIRE_AUTH", "true").lower() != "false"
        )
        self.allowed_ips = self._parse_allowed_ips()

        logger.info(
            "AuthMiddleware: require_auth=%s  api_key=%s  auth_token=%s  ip_allowlist=%s",
            self.require_auth,
            "set" if self.api_key else "unset",
            "set" if self.auth_token else "unset",
            len(self.allowed_ips) if self.allowed_ips is not None else "disabled",
        )

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _parse_allowed_ips():
        raw = os.environ.get("MCP_ALLOWED_IPS", "")
        if not raw.strip():
            return None  # disabled
        return {ip.strip() for ip in raw.split(",") if ip.strip()}

    @staticmethod
    def _get_client_ip(scope):
        for name, value in scope.get("headers", []):
            if name == b"x-forwarded-for":
                return value.decode("latin-1").split(",")[0].strip()
        client = scope.get("client")
        return client[0] if client else "unknown"

    def _check_ip(self, scope) -> bool:
        """True if client IP is in the allowlist (or allowlist is disabled)."""
        if self.allowed_ips is None:
            return True
        client = scope.get("client")
        direct_ip = client[0] if client else ""
        forwarded_ip = ""
        for name, value in scope.get("headers", []):
            if name == b"x-forwarded-for":
                forwarded_ip = value.decode("latin-1").split(",")[0].strip()
                break
        return direct_ip in self.allowed_ips or forwarded_ip in self.allowed_ips

    def _check_auth(self, scope):
        """Return auth method label if valid, else None."""
        # 1. Header: X-Api-Key
        if self.api_key:
            for name, value in scope.get("headers", []):
                if name == b"x-api-key":
                    if value.decode("latin-1") == self.api_key:
                        return "header:x-api-key"
                    break  # header present but wrong value

        # 2. Query param: ?token=
        if self.auth_token:
            qs = scope.get("query_string", b"").decode("latin-1")
            params = parse_qs(qs)
            token_values = params.get("token", [])
            if token_values and token_values[0] == self.auth_token:
                return "query:token"

        return None

    @staticmethod
    async def _send_json(send, status_code, data):
        """Send a complete JSON response via raw ASGI protocol."""
        body = json.dumps(data).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("latin-1")),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": body,
                "more_body": False,
            }
        )

    # -- ASGI entry point ----------------------------------------------------

    async def __call__(self, scope, receive, send):
        # Non-HTTP scopes (lifespan, websocket): pass through
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "").rstrip("/")
        client_ip = self._get_client_ip(scope)

        # /health — no auth (Railway health checks)
        if path == "/health":
            await self._send_json(send, 200, {"status": "ok"})
            return

        # IP allowlist (all paths except /health)
        if not self._check_ip(scope):
            logger.warning("IP_REJECTED ip=%s path=%s", client_ip, path)
            await self._send_json(send, 403, {"error": "Forbidden"})
            return

        # /messages/* — protected by session UUID, no token auth needed
        if path.startswith("/messages"):
            await self.app(scope, receive, send)
            return

        # All other paths (including /sse) — require auth
        auth_method = self._check_auth(scope)
        if auth_method is None and self.require_auth:
            logger.warning(
                "AUTH_REJECTED ip=%s path=%s", client_ip, path
            )
            await self._send_json(send, 401, {"error": "Unauthorized"})
            return

        logger.info(
            "AUTH_OK ip=%s path=%s method=%s",
            client_ip,
            path,
            auth_method or "auth_disabled",
        )
        await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# Server startup
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import anyio
    import uvicorn

    port = int(os.environ.get("PORT", 8080))

    # Configure FastMCP
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = port
    mcp.settings.transport_security = None

    # SSE Starlette app from FastMCP
    sseApp = mcp.sse_app()

    # Wrap with pure ASGI auth (NOT sseApp.add_middleware — that breaks SSE)
    authedApp = AuthMiddleware(sseApp)

    logger.info("MCP Server starting on 0.0.0.0:%d (SSE, ASGI auth)", port)

    config = uvicorn.Config(
        authedApp,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    anyio.run(server.serve)
