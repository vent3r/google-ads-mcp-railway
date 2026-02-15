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
    detail: bool = False,
) -> str:
    """Analyze search terms with server-side processing for large volumes.

    By default, aggregates metrics by unique search term across all campaigns
    and ad groups. Set detail=true to see per-campaign/ad-group breakdown.

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
        detail: If true, show one row per search term per campaign/ad group (default false).
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
        "metrics.conversions, metrics.conversions_value "
        f"FROM search_term_view WHERE {where}"
    )

    rows = run_query(customer_id, query)
    total_api_rows = len(rows)

    if detail:
        # --- DETAIL MODE: aggregate per search_term x campaign x adgroup (collapse days only) ---
        agg_map = defaultdict(lambda: {
            "search_term_view.search_term": "",
            "campaign.name": "",
            "ad_group.name": "",
            "metrics.impressions": 0,
            "metrics.clicks": 0,
            "metrics.cost_micros": 0.0,
            "metrics.conversions": 0.0,
            "metrics.conversions_value": 0.0,
        })

        for row in rows:
            term = row.get("search_term_view.search_term", "")
            camp = row.get("campaign.name", "")
            ag = row.get("ad_group.name", "")
            if not term:
                continue
            key = (term, camp, ag)
            a = agg_map[key]
            a["search_term_view.search_term"] = term
            a["campaign.name"] = camp
            a["ad_group.name"] = ag
            a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
            a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

        rows = list(agg_map.values())
        for r in rows:
            compute_derived_metrics(r)

        mode_label = "detail"
    else:
        # --- DEFAULT MODE: aggregate per search_term (collapse days + campaigns + adgroups) ---
        agg_map = defaultdict(lambda: {
            "search_term_view.search_term": "",
            "_campaigns": set(),
            "_adgroups": set(),
            "metrics.impressions": 0,
            "metrics.clicks": 0,
            "metrics.cost_micros": 0.0,
            "metrics.conversions": 0.0,
            "metrics.conversions_value": 0.0,
        })

        for row in rows:
            term = row.get("search_term_view.search_term", "")
            if not term:
                continue
            a = agg_map[term]
            a["search_term_view.search_term"] = term
            a["_campaigns"].add(row.get("campaign.name", ""))
            a["_adgroups"].add(row.get("ad_group.name", ""))
            a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
            a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

        rows = []
        for a in agg_map.values():
            camp_set = a.pop("_campaigns")
            ag_set = a.pop("_adgroups")
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

        mode_label = "aggregated"

    total_unique = len(rows)
    logger.info(
        "search_term_analysis [%s]: %d API rows -> %d rows",
        mode_label, total_api_rows, total_unique,
    )

    # --- Apply filters ---
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
        f"**Search Term Analysis ({mode_label})** — {date_from} to {date_to}\n"
        f"{total_unique:,} rows ({total_api_rows:,} API rows). "
        f"Showing top {min(limit, total_filtered)} by {sort_by} "
        f"(filtered: {', '.join(filters_desc)}). "
        f"{total_filtered:,} match filters.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
