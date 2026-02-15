"""Tool 3: adgroup_analysis — Ad group performance analysis."""

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

logger = logging.getLogger(__name__)


@mcp.tool()
def adgroup_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    min_clicks: int = 0,
    sort_by: str = "spend",
    limit: int = 30,
) -> str:
    """Analyze ad group performance, optionally filtered by campaign.

    Aggregates per-day rows into one row per ad group, computes derived
    metrics, filters and sorts.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional).
        min_clicks: Minimum clicks to include (default 0).
        sort_by: Sort by spend, clicks, conversions, or cpa (default spend).
        limit: Max rows to return (default 30).
    """
    customer_id = ClientResolver.resolve(client)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    query = (
        "SELECT "
        "campaign.name, ad_group.name, ad_group.id, ad_group.status, "
        "ad_group.type, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        "FROM ad_group "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{campaign_clause}"
    )

    rows = run_query(customer_id, query)
    total_api = len(rows)

    # Aggregate by ad_group.id
    agg = defaultdict(lambda: {
        "campaign.name": "", "ad_group.name": "", "ad_group.id": "",
        "ad_group.status": "", "ad_group.type": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        ag_id = row.get("ad_group.id", "")
        a = agg[ag_id]
        a["campaign.name"] = row.get("campaign.name", "")
        a["ad_group.name"] = row.get("ad_group.name", "")
        a["ad_group.id"] = ag_id
        a["ad_group.status"] = row.get("ad_group.status", "")
        a["ad_group.type"] = row.get("ad_group.type", "")
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    processed = []
    for row in agg.values():
        compute_derived_metrics(row)
        if int(row.get("metrics.clicks", 0)) >= min_clicks:
            processed.append(row)

    logger.info("adgroup_analysis: %d API rows -> %d ad groups", total_api, len(processed))

    sort_keys = {"spend": "_spend", "clicks": "metrics.clicks",
                 "conversions": "metrics.conversions", "cpa": "_cpa"}
    sk = sort_keys.get(sort_by.lower(), "_spend")
    processed.sort(key=lambda r: float(r.get(sk, 0) or 0), reverse=True)

    total = len(processed)
    processed = processed[:limit]

    output = []
    for row in processed:
        output.append({
            "campaign": row.get("campaign.name", ""),
            "adgroup": row.get("ad_group.name", ""),
            "status": row.get("ad_group.status", ""),
            "clicks": ResultFormatter.fmt_int(row.get("metrics.clicks", 0)),
            "spend": ResultFormatter.fmt_currency(row["_spend"]),
            "conv": f"{float(row.get('metrics.conversions', 0)):,.1f}",
            "cpa": ResultFormatter.fmt_currency(row["_cpa"]),
            "roas": f"{row['_roas']:.2f}",
        })

    columns = [
        ("campaign", "Campaign"), ("adgroup", "Ad Group"), ("status", "Status"),
        ("clicks", "Clicks"), ("spend", "Spend"), ("conv", "Conv"),
        ("cpa", "CPA"), ("roas", "ROAS"),
    ]

    header = (
        f"**Ad Group Analysis** — {date_from} to {date_to}\n"
        f"{total:,} ad groups found ({total_api:,} API rows). "
        f"Sorted by {sort_by}.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
