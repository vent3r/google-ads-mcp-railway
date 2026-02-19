"""PMax search terms with full metrics (cost, conversions, ROAS).

Uses campaign_search_term_view which provides individual search terms
at campaign level for Performance Max campaigns — single GAQL query,
no per-campaign iteration needed.
"""

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
def pmax_search_categories(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Show PMax search terms with full metrics including cost.

    Uses campaign_search_term_view which provides individual search terms
    with cost data for Performance Max campaigns.

    USE THIS TOOL WHEN:
    - User asks about PMax search terms, search queries, what triggers PMax ads
    - "search terms PMax", "termini di ricerca PMax", "cosa cercano gli utenti in PMax"
    - "campaign_search_term_view", "pmax search terms", "search terms performance max"
    - User asks for search terms and the campaign is a Performance Max campaign
    - Any question about search behavior, query matching, or user intent in PMax

    DO NOT USE WHEN:
    - Search/Shopping campaign search terms -> use search_term_analysis
    - N-gram analysis on Search campaigns -> use ngram_analysis

    OUTPUT: Markdown table with individual search terms, cost, clicks, conversions per PMax campaign.

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
    date_cond = DateHelper.date_condition(date_from, date_to)

    conditions = [
        "campaign.advertising_channel_type = 'PERFORMANCE_MAX'",
        date_cond,
    ]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    query = (
        "SELECT campaign.name, campaign_search_term_view.search_term, "
        "metrics.clicks, metrics.impressions, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM campaign_search_term_view "
        f"WHERE {' AND '.join(conditions)}"
    )

    rows = run_query(customer_id, query)

    # Aggregate by (campaign, search_term) — rows are split by date segment
    agg = defaultdict(lambda: {
        "campaign.name": "", "search_term": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        term = row.get("campaign_search_term_view.search_term", "")
        camp_name = row.get("campaign.name", "")
        key = (camp_name, term)
        a = agg[key]
        a["campaign.name"] = camp_name
        a["search_term"] = term
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for a in agg.values():
        compute_derived_metrics(a)
        results.append(a)

    total_terms = len(results)

    rows_out, total, truncated, filter_desc, summary = process_rows(
        results, sort_by=sort_by, limit=limit,
    )

    columns = [
        ("campaign.name", "Campaign"),
        ("search_term", "Search Term"),
        ("_spend", "Cost \u20ac"),
        ("metrics.clicks", "Clicks"),
        ("metrics.impressions", "Impr"),
        ("_ctr", "CTR%"),
        ("_cpc", "Avg CPC \u20ac"),
        ("metrics.conversions", "Conv"),
        ("metrics.conversions_value", "Value \u20ac"),
        ("_cpa", "CPA \u20ac"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="PMax Search Terms",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total_terms:,} search terms",
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
