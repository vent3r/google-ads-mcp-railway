"""T3: Performance Max search category insights.

campaign_search_term_insight requires filtering by a single campaign ID,
so this tool loops over all PMax campaigns.
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
    """Show search term insights for Performance Max campaigns (campaign_search_term_insight).

    This is the ONLY way to see what users search in PMax campaigns. Google does NOT
    expose PMax search terms through the normal search_term_view — they use a separate
    resource called campaign_search_term_insight that returns aggregate CATEGORIES.

    IMPORTANT: search_term_analysis DOES NOT WORK for Performance Max campaigns.
    You MUST use this tool instead for any PMax search query analysis.

    USE THIS TOOL WHEN:
    - User asks about PMax search terms, search queries, what triggers PMax ads
    - "search terms PMax", "termini di ricerca PMax", "cosa cercano gli utenti in PMax"
    - "campaign_search_term_insight", "search category insights performance max"
    - User asks for search terms and the campaign is a Performance Max campaign
    - Any question about search behavior, query matching, or user intent in PMax

    DO NOT USE WHEN:
    - Search/Shopping campaign search terms -> use search_term_analysis
    - N-gram analysis on Search campaigns -> use ngram_analysis

    OUTPUT: Markdown table with search categories and metrics per PMax campaign.

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

    # Determine which PMax campaigns to query
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        pmax_campaigns = [{"id": campaign_id, "name": campaign}]
    else:
        list_q = (
            "SELECT campaign.id, campaign.name "
            "FROM campaign "
            "WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX' "
            "AND campaign.status = 'ENABLED'"
        )
        camp_rows = run_query(customer_id, list_q)
        pmax_campaigns = [
            {"id": str(r.get("campaign.id", "")), "name": r.get("campaign.name", "")}
            for r in camp_rows
        ]

    total_campaigns = len(pmax_campaigns)
    if total_campaigns > 50:
        logger.warning("Unusual: %d PMax campaigns for customer %s", total_campaigns, customer_id)

    if not pmax_campaigns:
        return "No enabled Performance Max campaigns found."

    # Query each PMax campaign's search term insights
    by_category = defaultdict(lambda: {
        "campaign.name": "", "category_label": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    campaigns_queried = 0
    for camp in pmax_campaigns:
        q = (
            "SELECT "
            "campaign_search_term_insight.category_label, "
            "metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            f"FROM campaign_search_term_insight "
            f"WHERE campaign_search_term_insight.campaign_id = '{camp['id']}' "
            f"AND {date_cond}"
        )
        try:
            rows = run_query(customer_id, q)
            campaigns_queried += 1
        except ValueError as e:
            logger.warning("pmax_search_categories: campaign %s (%s) failed: %s",
                           camp["name"], camp["id"], e)
            continue

        for row in rows:
            label = row.get("campaign_search_term_insight.category_label", "")
            key = (camp["id"], label)
            a = by_category[key]
            a["campaign.name"] = camp["name"]
            a["category_label"] = label
            a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
            a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for a in by_category.values():
        compute_derived_metrics(a)
        results.append(a)

    rows_out, total, truncated, filter_desc, summary = process_rows(
        results, sort_by=sort_by, limit=limit,
    )

    columns = [
        ("campaign.name", "Campaign"),
        ("category_label", "Search Category"),
        ("_spend", "Spend \u20ac"),
        ("metrics.clicks", "Clicks"),
        ("metrics.impressions", "Impr"),
        ("_ctr", "CTR%"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA \u20ac"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="PMax Search Category Insights",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{campaigns_queried} PMax campaigns queried \u00b7 {total} categories",
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
