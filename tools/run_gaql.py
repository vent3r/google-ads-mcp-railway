"""Tool 10: run_gaql — Execute raw GAQL queries (read-only).

Covers the 5% of use cases that specialized tools don't handle.
"""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    run_query,
)
from tools.options import (
    build_header,
    format_output,
)

logger = logging.getLogger(__name__)

# Safety: block mutation-capable queries
_BLOCKED = {"MUTATE", "CREATE", "UPDATE", "REMOVE"}


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
    query_upper = query.upper().strip()
    for word in _BLOCKED:
        if word in query_upper:
            return f"Error: mutation queries are not allowed. Only SELECT queries."

    if not query_upper.startswith("SELECT"):
        return "Error: query must start with SELECT."

    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

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

    header = build_header(
        title="GAQL Result",
        client_name=client_name,
        extra=f"{total:,} rows",
    )

    footer = ""
    if total > limit:
        footer = f"*Showing {limit} of {total:,} rows.*"

    return format_output(formatted, columns, header=header, footer=footer)
