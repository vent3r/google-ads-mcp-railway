"""Tool 5: search_term_analysis — Search term analysis with server-side processing."""

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
def search_term_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    min_clicks: int = 1,
    max_cpa: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze search terms with server-side processing for large volumes.

    Fetches all search terms across the date range, aggregates metrics by
    unique search term (collapsing per-day/per-adgroup rows), then filters
    and returns the top results.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional).
        min_clicks: Minimum clicks to include (default 1).
        max_cpa: Maximum CPA to include — 0 means no limit (default 0).
        zero_conversions: If true, show ONLY search terms with 0 conversions (default false).
        sort_by: Sort metric — spend, clicks, impressions, cpa, or ctr (default spend).
        limit: Maximum rows to return (default 50).
    """
    customer_id = ClientResolver.resolve(client)

    conditions = [
        DateHelper.date_condition(date_from, date_to),
        "metrics.impressions > 0",
    ]

    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    where = " AND ".join(conditions)

    query = (
        "SELECT "
        "campaign.name, ad_group.name, "
        "search_term_view.search_term, search_term_view.status, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, metrics.ctr "
        f"FROM search_term_view WHERE {where}"
    )

    rows = run_query(customer_id, query)
    total_api_rows = len(rows)

    # Aggregate by search term to collapse per-day/per-campaign/per-adgroup rows
    agg_map = defaultdict(lambda: {
        "search_term_view.search_term": "",
        "search_term_view.status": "",
        "campaigns": set(),
        "adgroups": set(),
        "metrics.impressions": 0,
        "metrics.clicks": 0,
        "metrics.cost_micros": 0,
        "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        term = row.get("search_term_view.search_term", "")
        if not term:
            continue
        a = agg_map[term]
        a["search_term_view.search_term"] = term
        a["search_term_view.status"] = row.get("search_term_view.status", "")
        a["campaigns"].add(row.get("campaign.name", ""))
        a["adgroups"].add(row.get("ad_group.name", ""))
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    # Convert sets to readable strings and compute derived metrics
    rows = []
    for a in agg_map.values():
        camp_set = a.pop("campaigns")
        ag_set = a.pop("adgroups")
        a["campaign.name"] = (
            f"{len(camp_set)} campaigns" if len(camp_set) > 1
            else next(iter(camp_set), "")
        )
        a["ad_group.name"] = (
            f"{len(ag_set)} ad groups" if len(ag_set) > 1
            else next(iter(ag_set), "")
        )
        compute_derived_metrics(a)
        rows.append(a)

    total_raw = len(rows)
    logger.info(
        "search_term_analysis: %d API rows -> %d unique search terms",
        total_api_rows, total_raw,
    )

    # Apply filters
    filtered = []
    for row in rows:
        clicks = int(row.get("metrics.clicks", 0) or 0)
        conversions = float(row.get("metrics.conversions", 0) or 0)
        cpa = row["_cpa"]

        if clicks < min_clicks:
            continue
        if zero_conversions and conversions > 0:
            continue
        if not zero_conversions and max_cpa > 0 and cpa > max_cpa and conversions > 0:
            continue

        filtered.append(row)

    total_filtered = len(filtered)

    # Sort
    sort_keys = {
        "spend": "_spend",
        "clicks": "metrics.clicks",
        "impressions": "metrics.impressions",
        "cpa": "_cpa",
        "ctr": "metrics.ctr",
    }
    sort_key = sort_keys.get(sort_by.lower(), "_spend")
    filtered.sort(key=lambda r: float(r.get(sort_key, 0) or 0), reverse=True)

    display = filtered[:limit]

    # Format
    output = []
    for row in display:
        output.append({
            "search_term": row.get("search_term_view.search_term", ""),
            "campaign": row.get("campaign.name", ""),
            "adgroup": row.get("ad_group.name", ""),
            "status": row.get("search_term_view.status", ""),
            "clicks": f"{int(row.get('metrics.clicks', 0) or 0):,}",
            "spend": ResultFormatter.format_currency(row["_spend"]),
            "conversions": f"{float(row.get('metrics.conversions', 0) or 0):,.1f}",
            "cpa": ResultFormatter.format_currency(row["_cpa"]),
        })

    columns = [
        ("search_term", "Search Term"),
        ("campaign", "Campaign"),
        ("adgroup", "Ad Group"),
        ("clicks", "Clicks"),
        ("spend", "Spend"),
        ("conversions", "Conv"),
        ("cpa", "CPA"),
    ]

    filters_desc = [f"clicks >= {min_clicks}"]
    if zero_conversions:
        filters_desc.append("zero conversions only")
    if max_cpa > 0:
        filters_desc.append(f"CPA <= {max_cpa}")

    header = (
        f"**Search Term Analysis** — {date_from} to {date_to}\n"
        f"Found {total_raw:,} unique search terms ({total_api_rows:,} API rows aggregated). "
        f"Showing top {min(limit, total_filtered)} by {sort_by} "
        f"(filtered: {', '.join(filters_desc)}). "
        f"{total_filtered:,} match filters.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
