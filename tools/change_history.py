"""Tool 8: change_history — What changed in the account recently.

Uses the change_status resource to show recent modifications:
campaigns created/removed/paused, budget changes, bid strategy changes, etc.
Critical for root cause analysis: "why did CPA spike?" → check what changed.
"""

import logging
from datetime import date, timedelta

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    DateHelper,
    ResultFormatter,
    run_query,
)

logger = logging.getLogger(__name__)


@mcp.tool()
def change_history(
    client: str,
    days: int = 7,
    resource_type: str = "ALL",
    limit: int = 50,
) -> str:
    """Show recent changes in the account (campaigns, ad groups, ads, keywords).

    Essential for understanding WHY metrics changed. Use this when investigating
    performance shifts.

    Args:
        client: Account name or customer ID.
        days: How many days back to look (default 7, max 30).
        resource_type: Filter by type — CAMPAIGN, AD_GROUP, AD, AD_GROUP_CRITERION,
            CAMPAIGN_BUDGET, or ALL (default ALL).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)

    days = min(days, 30)
    end = date.today()
    start = end - timedelta(days=days)
    date_from = DateHelper.format_date(start)
    date_to = DateHelper.format_date(end)

    resource_clause = ""
    if resource_type.upper() != "ALL":
        resource_clause = (
            f" AND change_status.resource_type = '{resource_type.upper()}'"
        )

    query = (
        "SELECT "
        "change_status.last_change_date_time, "
        "change_status.resource_type, "
        "change_status.resource_status, "
        "change_status.resource_change_operation, "
        "campaign.name, "
        "ad_group.name "
        "FROM change_status "
        f"WHERE change_status.last_change_date_time >= '{date_from}' "
        f"AND change_status.last_change_date_time <= '{date_to}'"
        f"{resource_clause} "
        "ORDER BY change_status.last_change_date_time DESC "
        f"LIMIT {limit}"
    )

    rows = run_query(customer_id, query)

    if not rows:
        return f"No changes found in the last {days} days."

    output = []
    for row in rows:
        dt = str(row.get("change_status.last_change_date_time", ""))
        # Truncate to readable format (YYYY-MM-DD HH:MM)
        display_dt = dt[:16] if len(dt) >= 16 else dt

        res_type = str(row.get("change_status.resource_type", ""))
        res_status = str(row.get("change_status.resource_status", ""))
        operation = str(row.get("change_status.resource_change_operation", ""))
        campaign = row.get("campaign.name", "")
        adgroup = row.get("ad_group.name", "")

        # Clean enum values
        res_type = res_type.replace("_", " ").title()
        res_status = res_status.replace("_", " ").title()
        operation = operation.replace("_", " ").title()

        entity = campaign
        if adgroup:
            entity = f"{campaign} > {adgroup}"

        output.append({
            "when": display_dt,
            "type": res_type,
            "operation": operation,
            "status": res_status,
            "entity": entity,
        })

    columns = [
        ("when", "When"), ("type", "Resource"), ("operation", "Change"),
        ("status", "New Status"), ("entity", "Entity"),
    ]

    header = (
        f"**Change History** — last {days} days\n"
        f"{len(output)} changes found.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
