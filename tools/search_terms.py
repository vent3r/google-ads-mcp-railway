"""Tool 5: search_term_analysis — Search term analysis with aggregation."""

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
    """Analyze search terms with server-side aggregation.

    Default: one row per unique search term (campaigns/adgroups shown as count).
    Detail mode: one row per search term × campaign × ad group.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        min_clicks: Min clicks to include (default 1).
        max_cpa: Max CPA — 0 = no limit (default 0).
        zero_conversions: If true, only show terms with 0 conversions (default false).
        sort_by: spend, clicks, impressions, cpa, or ctr (default spend).
        limit: Max rows (default 50).
        detail: If true, show per campaign/adgroup breakdown (default false).
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
    total_api = len(rows)

    # Aggregate
    if detail:
        # Group by term + campaign + adgroup
        group_key = lambda row: (
            row.get("search_term_view.search_term", ""),
            row.get("campaign.name", ""),
            row.get("ad_group.name", ""),
        )
    else:
        # Group by term only
        group_key = lambda row: (row.get("search_term_view.search_term", ""),)

    agg = defaultdict(lambda: {
        "term": "", "status": "",
        "campaigns": set(), "adgroups": set(),
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        term = row.get("search_term_view.search_term", "")
        if not term:
            continue
        key = group_key(row)
        a = agg[key]
        a["term"] = term
        a["status"] = row.get("search_term_view.status", "")
        a["campaigns"].add(row.get("campaign.name", ""))
        a["adgroups"].add(row.get("ad_group.name", ""))
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    # Finalize sets, compute metrics, filter
    processed = []
    for a in agg.values():
        camp_set = a.pop("campaigns")
        ag_set = a.pop("adgroups")
        a["campaign"] = (
            f"{len(camp_set)} campaigns" if len(camp_set) > 1
            else next(iter(camp_set), "")
        )
        a["adgroup"] = (
            f"{len(ag_set)} ad groups" if len(ag_set) > 1
            else next(iter(ag_set), "")
        )
        compute_derived_metrics(a)

        clicks = int(a.get("metrics.clicks", 0))
        conv = float(a.get("metrics.conversions", 0))
        cpa = a["_cpa"]

        if clicks < min_clicks:
            continue
        if zero_conversions and conv > 0:
            continue
        if not zero_conversions and max_cpa > 0 and cpa > max_cpa and conv > 0:
            continue
        processed.append(a)

    total_unique = len(agg)
    total_filtered = len(processed)
    logger.info(
        "search_term_analysis: %d API rows -> %d unique -> %d filtered",
        total_api, total_unique, total_filtered,
    )

    # Sort
    sort_keys = {
        "spend": "_spend", "clicks": "metrics.clicks",
        "impressions": "metrics.impressions", "cpa": "_cpa", "ctr": "_ctr",
    }
    sk = sort_keys.get(sort_by.lower(), "_spend")
    processed.sort(key=lambda r: float(r.get(sk, 0) or 0), reverse=True)
    display = processed[:limit]

    # Format
    output = []
    for row in display:
        output.append({
            "term": row["term"],
            "campaign": row["campaign"],
            "adgroup": row["adgroup"],
            "clicks": ResultFormatter.fmt_int(row["metrics.clicks"]),
            "spend": ResultFormatter.fmt_currency(row["_spend"]),
            "conv": f"{float(row['metrics.conversions']):,.1f}",
            "cpa": ResultFormatter.fmt_currency(row["_cpa"]),
            "roas": f"{row['_roas']:.2f}",
        })

    columns = [
        ("term", "Search Term"), ("campaign", "Campaign"),
        ("adgroup", "Ad Group"), ("clicks", "Clicks"),
        ("spend", "Spend"), ("conv", "Conv"),
        ("cpa", "CPA"), ("roas", "ROAS"),
    ]

    header = (
        f"**Search Term Analysis** — {date_from} to {date_to}\n"
        f"{total_unique:,} unique terms ({total_api:,} API rows). "
        f"{total_filtered:,} match filters. Sorted by {sort_by}.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
