"""R9: Budget pacing analysis — are campaigns on track for the month?"""

import calendar
import logging
from collections import defaultdict
from datetime import date

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    run_query,
)
from tools.options import format_output, build_header
from tools.validation import micros_to_euros

logger = logging.getLogger(__name__)


@mcp.tool()
def budget_pacing(
    client: str,
    campaign: str = "",
) -> str:
    """Analyze budget pacing for active campaigns.

    Shows daily budget, month-to-date spend, projected monthly spend,
    and pacing status (ON_TRACK, OVER, UNDER).

    USE THIS TOOL WHEN:
    - User asks about budget utilization or pacing
    - "come stiamo spendendo", "budget pacing", "stiamo rispettando il budget"
    - Monthly budget planning

    DO NOT USE WHEN:
    - Historical performance → use campaign_analysis
    - Budget changes → use update_budget

    OUTPUT: Table with campaigns and their pacing status.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (optional — all enabled campaigns if empty).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    # Get campaign budgets
    q_budget = (
        "SELECT campaign.id, campaign.name, campaign.status, "
        "campaign_budget.amount_micros "
        f"FROM campaign WHERE campaign.status = 'ENABLED'{campaign_clause}"
    )
    budget_rows = run_query(customer_id, q_budget)

    if not budget_rows:
        return "No enabled campaigns found."

    # Get MTD spend
    today = date.today()
    first_of_month = today.replace(day=1)
    yesterday = today
    days_passed = today.day
    days_in_month = calendar.monthrange(today.year, today.month)[1]

    q_spend = (
        "SELECT campaign.id, metrics.cost_micros "
        "FROM campaign "
        f"WHERE segments.date BETWEEN '{DateHelper.format_date(first_of_month)}' "
        f"AND '{DateHelper.format_date(yesterday)}'"
        f"{campaign_clause}"
    )
    spend_rows = run_query(customer_id, q_spend)

    # Aggregate MTD spend by campaign
    mtd_spend = defaultdict(float)
    for row in spend_rows:
        cid = str(row.get("campaign.id", ""))
        mtd_spend[cid] += float(row.get("metrics.cost_micros", 0) or 0)

    # Build results
    results = []
    # Deduplicate campaigns by ID
    seen_ids = set()
    for row in budget_rows:
        cid = str(row.get("campaign.id", ""))
        if cid in seen_ids:
            continue
        seen_ids.add(cid)

        daily_budget_micros = int(row.get("campaign_budget.amount_micros", 0) or 0)
        daily_budget = micros_to_euros(daily_budget_micros)
        monthly_budget = daily_budget * days_in_month
        spend_micros = mtd_spend.get(cid, 0)
        spend_mtd = spend_micros / 1_000_000

        if days_passed > 0:
            projected = (spend_mtd / days_passed) * days_in_month
        else:
            projected = 0

        if monthly_budget > 0:
            ratio = projected / monthly_budget
            if ratio < 0.85:
                pacing = "UNDER"
            elif ratio > 1.15:
                pacing = "OVER"
            else:
                pacing = "ON_TRACK"
        else:
            pacing = "N/A"

        results.append({
            "campaign": row.get("campaign.name", ""),
            "daily_budget": f"\u20ac{daily_budget:,.2f}",
            "spend_mtd": f"\u20ac{spend_mtd:,.2f}",
            "projected": f"\u20ac{projected:,.2f}",
            "monthly_budget": f"\u20ac{monthly_budget:,.2f}",
            "pacing": pacing,
            "_sort": spend_mtd,
        })

    results.sort(key=lambda r: r["_sort"], reverse=True)
    for r in results:
        r.pop("_sort")

    columns = [
        ("campaign", "Campaign"),
        ("daily_budget", "Daily Budget"),
        ("spend_mtd", "Spend MTD"),
        ("projected", "Projected"),
        ("monthly_budget", "Monthly Budget"),
        ("pacing", "Pacing"),
    ]

    header = build_header(
        title="Budget Pacing",
        client_name=client_name,
        extra=f"Day {days_passed}/{days_in_month} \u00b7 {len(results)} campaigns",
    )

    return format_output(results, columns, header=header, output_mode="full")
