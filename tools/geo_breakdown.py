"""R5: Geographic performance breakdown."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    compute_derived_metrics,
    run_query,
)
from tools.options import format_output, build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def geo_breakdown(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze performance by geographic location.

    USE THIS TOOL WHEN:
    - User asks about geographic performance, countries, regions
    - "performance per paese", "geo breakdown", "dove spendiamo"
    - Identifying top/bottom performing locations

    DO NOT USE WHEN:
    - Device breakdown → use device_breakdown
    - Time analysis → use hour_day_analysis

    OUTPUT: Table with locations sorted by spend.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        "SELECT geographic_view.country_criterion_id, "
        "geographic_view.location_type, "
        "geo_target_constant.name, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM geographic_view "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}{campaign_clause}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by location name
    by_location = defaultdict(lambda: {
        "location": "",
        "location_type": "",
        "metrics.impressions": 0,
        "metrics.clicks": 0,
        "metrics.cost_micros": 0,
        "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        loc_name = row.get("geo_target_constant.name", "")
        if not loc_name:
            loc_name = str(row.get("geographic_view.country_criterion_id", "Unknown"))
        loc_type = row.get("geographic_view.location_type", "")

        a = by_location[loc_name]
        a["location"] = loc_name
        a["location_type"] = str(loc_type).replace("_", " ").title() if loc_type else ""
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    total_spend = 0
    for a in by_location.values():
        compute_derived_metrics(a)
        total_spend += a["_spend"]
        results.append(a)

    # Add % of total spend
    for r in results:
        r["pct_spend"] = f"{r['_spend'] / total_spend * 100:.1f}%" if total_spend > 0 else "0.0%"

    sort_key = {"spend": "_spend", "clicks": "metrics.clicks", "conversions": "metrics.conversions",
                "cpa": "_cpa", "roas": "_roas"}.get(sort_by, "_spend")
    results.sort(key=lambda r: r.get(sort_key, 0), reverse=(sort_by != "cpa"))
    total = len(results)
    if limit and limit < total:
        results = results[:limit]

    columns = [
        ("location", "Location"),
        ("metrics.impressions", "Impressions"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend €"),
        ("pct_spend", "% Spend"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA €"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="Geographic Breakdown",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} locations · Total spend: €{total_spend:,.2f}",
    )

    return format_output(results, columns, header=header, output_mode="full")
