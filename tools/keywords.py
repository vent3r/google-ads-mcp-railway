"""Tool 4: keyword_analysis — Keyword performance with advanced filters."""

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
def keyword_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    min_clicks: int = 0,
    max_cpa: float = 0,
    min_conversions: float = 0,
    match_type: str = "ALL",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze keyword performance with advanced filtering.

    Fetches all keyword rows across the date range, aggregates metrics by
    unique keyword (collapsing per-day rows), then filters and returns results.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional).
        min_clicks: Minimum clicks to include (default 0).
        max_cpa: Maximum CPA to include — 0 means no limit (default 0).
        min_conversions: Minimum conversions to include (default 0).
        match_type: Keyword match type — EXACT, PHRASE, BROAD, or ALL (default ALL).
        sort_by: Sort metric — spend, clicks, conversions, cpa, or ctr (default spend).
        limit: Maximum rows to return (default 50).
    """
    customer_id = ClientResolver.resolve(client)

    conditions = [DateHelper.date_condition(date_from, date_to)]

    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    if match_type.upper() != "ALL":
        conditions.append(f"ad_group_criterion.keyword.match_type = '{match_type.upper()}'")

    where = " AND ".join(conditions)

    query = (
        "SELECT "
        "campaign.name, ad_group.name, "
        "ad_group_criterion.keyword.text, "
        "ad_group_criterion.keyword.match_type, "
        "ad_group_criterion.status, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, "
        "metrics.ctr, metrics.average_cpc "
        f"FROM keyword_view WHERE {where}"
    )

    rows = run_query(customer_id, query)
    total_api_rows = len(rows)

    # Aggregate by keyword+campaign+adgroup+match_type to collapse per-day rows
    agg_map = defaultdict(lambda: {
        "campaign.name": "",
        "ad_group.name": "",
        "ad_group_criterion.keyword.text": "",
        "ad_group_criterion.keyword.match_type": "",
        "ad_group_criterion.status": "",
        "metrics.impressions": 0,
        "metrics.clicks": 0,
        "metrics.cost_micros": 0,
        "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        kw_text = row.get("ad_group_criterion.keyword.text", "")
        campaign_name = row.get("campaign.name", "")
        adgroup_name = row.get("ad_group.name", "")
        kw_match = row.get("ad_group_criterion.keyword.match_type", "")
        key = (kw_text, campaign_name, adgroup_name, kw_match)

        a = agg_map[key]
        a["campaign.name"] = campaign_name
        a["ad_group.name"] = adgroup_name
        a["ad_group_criterion.keyword.text"] = kw_text
        a["ad_group_criterion.keyword.match_type"] = kw_match
        a["ad_group_criterion.status"] = row.get("ad_group_criterion.status", "")
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    rows = list(agg_map.values())
    logger.info(
        "keyword_analysis: %d API rows -> %d unique keywords",
        total_api_rows, len(rows),
    )

    # Compute derived metrics and filter
    processed = []
    for row in rows:
        compute_derived_metrics(row)
        clicks = int(row.get("metrics.clicks", 0) or 0)
        conversions = float(row.get("metrics.conversions", 0) or 0)
        cpa = row["_cpa"]

        if clicks < min_clicks:
            continue
        if conversions < min_conversions:
            continue
        if max_cpa > 0 and cpa > max_cpa and conversions > 0:
            continue

        processed.append(row)

    # Sort
    sort_keys = {
        "spend": "_spend",
        "clicks": "metrics.clicks",
        "conversions": "metrics.conversions",
        "cpa": "_cpa",
        "ctr": "metrics.ctr",
    }
    sort_key = sort_keys.get(sort_by.lower(), "_spend")
    processed.sort(key=lambda r: float(r.get(sort_key, 0) or 0), reverse=True)

    total = len(processed)
    processed = processed[:limit]

    # Format
    output = []
    for row in processed:
        impressions = int(row.get("metrics.impressions", 0) or 0)
        clicks = int(row.get("metrics.clicks", 0) or 0)
        ctr = (clicks / impressions * 100) if impressions > 0 else 0.0
        output.append({
            "campaign": row.get("campaign.name", ""),
            "adgroup": row.get("ad_group.name", ""),
            "keyword": row.get("ad_group_criterion.keyword.text", ""),
            "match": row.get("ad_group_criterion.keyword.match_type", ""),
            "clicks": f"{clicks:,}",
            "spend": ResultFormatter.format_currency(row["_spend"]),
            "conversions": f"{float(row.get('metrics.conversions', 0) or 0):,.1f}",
            "cpa": ResultFormatter.format_currency(row["_cpa"]),
            "ctr": ResultFormatter.format_percent(ctr),
        })

    columns = [
        ("campaign", "Campaign"),
        ("adgroup", "Ad Group"),
        ("keyword", "Keyword"),
        ("match", "Match"),
        ("clicks", "Clicks"),
        ("spend", "Spend"),
        ("conversions", "Conv"),
        ("cpa", "CPA"),
        ("ctr", "CTR"),
    ]

    filters_desc = []
    if min_clicks > 0:
        filters_desc.append(f"clicks >= {min_clicks}")
    if max_cpa > 0:
        filters_desc.append(f"CPA <= {max_cpa}")
    if min_conversions > 0:
        filters_desc.append(f"conv >= {min_conversions}")
    if match_type.upper() != "ALL":
        filters_desc.append(f"match = {match_type}")
    filters_str = ", ".join(filters_desc) if filters_desc else "none"

    header = (
        f"**Keyword Analysis** — {date_from} to {date_to}\n"
        f"Found {total:,} keywords ({total_api_rows:,} API rows aggregated; "
        f"filters: {filters_str}). Sorted by {sort_by}.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
