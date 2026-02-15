"""SSE runner for Google Ads MCP Server on Railway.
Imports the official MCP server and runs it in SSE mode
with host/port configured for Railway deployment.
"""
import os
from ads_mcp.coordinator import mcp
# Register tools (same imports as the official server.py)
from ads_mcp.tools import search, core  # noqa: F401
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    # Override defaults for Railway (0.0.0.0 required, port from env)
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = port
    mcp.settings.transport_security = None
    print(f"MCP Server starting on 0.0.0.0:{port} (SSE mode)")
    mcp.run(transport="sse")
