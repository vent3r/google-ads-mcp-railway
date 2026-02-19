"""T4: Performance Max placement visibility — where PMax ads appear."""

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
def pmax_placements(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    sort_by: str = "impressions",
    limit: int = 50,
) -> str:
    """Show where Performance Max ads appear (websites, apps, YouTube channels).

    USE THIS TOOL WHEN:
    - User asks where PMax ads are shown, PMax placements
    - "dove appaiono gli annunci PMax", "placement PMax"
    - Investigating brand safety or placement quality in PMax

    DO NOT USE WHEN:
    - Display/Video placements -> use placement_performance
    - PMax asset group metrics -> use pmax_asset_groups

    OUTPUT: Markdown table with placements, URLs, types, and metrics.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: PMax campaign name or ID (optional).
        sort_by: impressions, clicks, spend, conversions (default impressions).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    conditions = [DateHelper.date_condition(date_from, date_to)]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    q = (
        "SELECT "
        "campaign.name, "
        "performance_max_placement_view.display_name, "
        "performance_max_placement_view.target_url, "
        "performance_max_placement_view.placement_type, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM performance_max_placement_view WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by campaign + display_name (rows may split by date)
    by_placement = defaultdict(lambda: {
        "campaign.name": "", "display_name": "", "target_url": "", "placement_type": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        display_name = row.get("performance_max_placement_view.display_name", "")
        camp_name = row.get("campaign.name", "")
        key = (camp_name, display_name)
        a = by_placement[key]
        a["campaign.name"] = camp_name
        a["display_name"] = display_name
        url = row.get("performance_max_placement_view.target_url", "")
        if url:
            a["target_url"] = str(url)[:60] + "..." if len(str(url)) > 60 else str(url)
        ptype = row.get("performance_max_placement_view.placement_type", "")
        if ptype:
            a["placement_type"] = str(ptype).replace("_", " ").title()
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

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
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="PMax Placement Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} placements",
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
