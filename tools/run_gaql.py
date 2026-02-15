"""Tool 10: run_gaql — Execute raw GAQL queries.

Inspired by cohnen/mcp-google-ads. Covers the 5% of use cases that
the specialized tools don't handle. The agent can write custom GAQL
for edge cases like landing_page_view, asset performance, etc.
"""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    ResultFormatter,
    run_query,
)

logger = logging.getLogger(__name__)

# Safety: block mutation-capable queries
BLOCKED_PATTERNS = [
    "MUTATE", "CREATE", "UPDATE", "REMOVE",
    "mutate", "create", "update", "remove",
]


@mcp.tool()
def run_gaql(
    client: str,
    query: str,
    limit: int = 50,
) -> str:
    """Execute a raw GAQL query on a client account. Read-only.

    Use this for queries not covered by other tools: landing pages,
    asset performance, geographic reports, device segments, audience data,
    ad disapprovals, extension performance, etc.

    Args:
        client: Account name or customer ID.
        query: Complete GAQL query string. Must be a SELECT query.
        limit: Max rows to display (default 50). Does NOT add LIMIT to query;
            use LIMIT in the query itself if needed.
    """
    # Safety check
    for pattern in BLOCKED_PATTERNS:
        if pattern in query:
            return f"Error: mutation queries are not allowed. Only SELECT queries."

    if not query.strip().upper().startswith("SELECT"):
        return "Error: query must start with SELECT."

    customer_id = ClientResolver.resolve(client)

    rows = run_query(customer_id, query)

    if not rows:
        return "Query returned 0 rows."

    # Auto-detect columns from first row
    first = rows[0]
    columns = [(k, k.split(".")[-1]) for k in first.keys()]

    total = len(rows)
    display = rows[:limit]

    # Format all values as strings
    formatted = []
    for row in display:
        formatted.append({k: str(v) for k, v in row.items()})

    header = f"**GAQL Result** — {total:,} rows returned\n\n"

    return header + ResultFormatter.markdown_table(formatted, columns, max_rows=limit)
