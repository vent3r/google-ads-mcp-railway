"""Tool 1: list_clients — List all accounts under the MCC."""

import os

from ads_mcp.coordinator import mcp
from tools.helpers import run_query, ResultFormatter


@mcp.tool()
def list_clients() -> str:
    """List all Google Ads client accounts under the MCC (Manager Account).

    Returns a table with account name, ID, and status for every child account.
    No parameters needed.
    """
    mcc_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
    if not mcc_id:
        return "Error: GOOGLE_ADS_LOGIN_CUSTOMER_ID not configured."

    query = (
        "SELECT "
        "customer_client.client_customer, "
        "customer_client.descriptive_name, "
        "customer_client.status, "
        "customer_client.level "
        "FROM customer_client "
        "WHERE customer_client.level = 1"
    )

    rows = run_query(mcc_id, query)

    if not rows:
        return "No client accounts found under this MCC."

    formatted = []
    for row in rows:
        formatted.append({
            "name": row.get("customer_client.descriptive_name", ""),
            "id": str(row.get("customer_client.client_customer", "")).replace("customers/", "").replace("-", ""),
            "status": row.get("customer_client.status", ""),
        })

    # Sort by name
    formatted.sort(key=lambda r: r["name"].lower())

    columns = [
        ("name", "Account Name"),
        ("id", "Customer ID"),
        ("status", "Status"),
    ]

    return ResultFormatter.markdown_table(formatted, columns, max_rows=100)
