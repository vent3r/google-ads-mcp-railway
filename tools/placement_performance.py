"""T11: Display/Video placement performance (detail and group level)."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    compute_derived_metrics,
    run_query,
)
from tools.options import build_header, format_output, process_rows

logger = logging.getLogger(__name__)


@mcp.tool()
def placement_performance(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    level: str = "detail",
    sort_by: str = "impressions",
    limit: int = 50,
) -> str:
    """Show which websites, apps, and YouTube channels ads appeared on (Display/Video).

    USE THIS TOOL WHEN:
    - User asks about placements, where Display/Video ads appear
    - "placement Display", "su quali siti appaiono", "placement performance"
    - Investigating brand safety for Display/Video campaigns

    DO NOT USE WHEN:
    - PMax placements -> use pmax_placements
    - Campaign-level overview -> use campaign_analysis

    OUTPUT: Markdown table with placements, URLs, types, and metrics.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        level: "detail" for individual placements, "group" for grouped (default detail).
        sort_by: impressions, clicks, spend, conversions (default impressions).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    date_cond = DateHelper.date_condition(date_from, date_to)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    if level.lower() == "group":
        view = "group_placement_view"
        prefix = "group_placement_view"
    else:
        view = "detail_placement_view"
        prefix = "detail_placement_view"

    q = (
        f"SELECT "
        f"campaign.name, "
        f"{prefix}.display_name, "
        f"{prefix}.target_url, "
        f"{prefix}.placement_type, "
        f"metrics.impressions, metrics.clicks, metrics.cost_micros, "
        f"metrics.conversions "
        f"FROM {view} "
        f"WHERE {date_cond}{campaign_clause}"
    )
    rows = run_query(customer_id, q)

    by_placement = defaultdict(lambda: {
        "campaign.name": "", "display_name": "", "target_url": "",
        "placement_type": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        display_name = row.get(f"{prefix}.display_name", "")
        camp_name = row.get("campaign.name", "")
        key = (camp_name, display_name)
        a = by_placement[key]
        a["campaign.name"] = camp_name
        a["display_name"] = display_name
        url = row.get(f"{prefix}.target_url", "")
        if url:
            a["target_url"] = str(url)[:60] + "..." if len(str(url)) > 60 else str(url)
        ptype = row.get(f"{prefix}.placement_type", "")
        if ptype:
            a["placement_type"] = str(ptype).replace("_", " ").title()
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)

    results = []
    for a in by_placement.values():
        compute_derived_metrics(a)
        results.append(a)

    rows_out, total, truncated, filter_desc, summary = process_rows(
        results, sort_by=sort_by, limit=limit,
    )

    columns = [
        ("campaign.name", "Campaign"),
        ("display_name", "Placement"),
        ("target_url", "URL"),
        ("placement_type", "Type"),
        ("metrics.impressions", "Impr"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend \u20ac"),
        ("metrics.conversions", "Conv"),
    ]

    header = build_header(
        title=f"Placement Performance ({level.title()})",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} placements",
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
