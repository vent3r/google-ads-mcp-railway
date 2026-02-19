"""T1: Performance Max asset group performance analysis."""

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
def pmax_asset_groups(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze Performance Max asset group performance with status diagnostics.

    USE THIS TOOL WHEN:
    - User asks about PMax asset groups, PMax performance breakdown
    - "performance max asset groups", "PMax asset group performance"
    - Investigating underperforming PMax campaigns at asset group level

    DO NOT USE WHEN:
    - Individual asset performance (headlines, images) -> use pmax_assets
    - PMax search categories -> use pmax_search_categories
    - Standard campaign overview -> use campaign_analysis

    OUTPUT: Markdown table with asset groups, status, primary status, and core metrics.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: PMax campaign name or ID (optional, all PMax campaigns if empty).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    conditions = [
        DateHelper.date_condition(date_from, date_to),
        "campaign.advertising_channel_type = 'PERFORMANCE_MAX'",
    ]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    q = (
        "SELECT "
        "campaign.name, campaign.id, "
        "asset_group.name, asset_group.id, asset_group.status, "
        "asset_group.primary_status, asset_group.primary_status_reasons, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM asset_group WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by asset_group.id (rows split by date segment)
    by_ag = defaultdict(lambda: {
        "campaign.name": "", "asset_group.name": "",
        "asset_group.status": "", "primary_status": "", "primary_reasons": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        ag_id = str(row.get("asset_group.id", ""))
        a = by_ag[ag_id]
        a["campaign.name"] = row.get("campaign.name", "")
        a["asset_group.name"] = row.get("asset_group.name", "")
        a["asset_group.status"] = str(row.get("asset_group.status", ""))
        ps = row.get("asset_group.primary_status", "")
        if ps:
            a["primary_status"] = str(ps).replace("_", " ").title()
        reasons = row.get("asset_group.primary_status_reasons", "")
        if reasons:
            if isinstance(reasons, list):
                a["primary_reasons"] = ", ".join(
                    str(r).replace("_", " ").title() for r in reasons
                )
            else:
                a["primary_reasons"] = str(reasons).replace("_", " ").title()
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for a in by_ag.values():
        compute_derived_metrics(a)
        results.append(a)

    rows_out, total, truncated, filter_desc, summary = process_rows(
        results, sort_by=sort_by, limit=limit,
    )

    columns = [
        ("campaign.name", "Campaign"),
        ("asset_group.name", "Asset Group"),
        ("asset_group.status", "Status"),
        ("primary_status", "Primary Status"),
        ("_spend", "Spend \u20ac"),
        ("metrics.clicks", "Clicks"),
        ("metrics.impressions", "Impr"),
        ("_ctr", "CTR%"),
        ("_cpc", "CPC \u20ac"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA \u20ac"),
        ("metrics.conversions_value", "Value \u20ac"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="PMax Asset Group Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} asset groups",
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
