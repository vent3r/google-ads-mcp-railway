"""T9: Audience segment performance at campaign and ad group level."""

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
def audience_performance(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    level: str = "campaign",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze audience segment performance at campaign or ad group level.

    USE THIS TOOL WHEN:
    - User asks about audience performance, remarketing lists, audience segments
    - "performance audience", "quali audience funzionano", "remarketing"

    DO NOT USE WHEN:
    - PMax audience signals (config only) -> use pmax_signals
    - Demographics (age/gender) -> use demographics

    OUTPUT: Markdown table with audience segments and core metrics.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        level: "campaign" or "adgroup" (default campaign).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    date_cond = DateHelper.date_condition(date_from, date_to)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    if level.lower() == "adgroup":
        q = (
            "SELECT "
            "campaign.name, ad_group.name, "
            "ad_group_criterion.user_list.user_list, "
            "metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            f"FROM ad_group_audience_view "
            f"WHERE {date_cond}{campaign_clause}"
        )
    else:
        q = (
            "SELECT "
            "campaign.name, "
            "campaign_criterion.user_list.user_list, "
            "metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            f"FROM campaign_audience_view "
            f"WHERE {date_cond}{campaign_clause}"
        )

    rows = run_query(customer_id, q)

    # Aggregate
    by_audience = defaultdict(lambda: {
        "campaign.name": "", "ad_group.name": "", "audience": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        camp_name = row.get("campaign.name", "")
        ag_name = row.get("ad_group.name", "") if level.lower() == "adgroup" else ""
        audience_rn = (
            row.get("ad_group_criterion.user_list.user_list", "")
            or row.get("campaign_criterion.user_list.user_list", "")
            or ""
        )
        # Clean up audience resource name
        audience = str(audience_rn).split("/")[-1] if "/" in str(audience_rn) else str(audience_rn)

        key = (camp_name, ag_name, audience)
        a = by_audience[key]
        a["campaign.name"] = camp_name
        a["ad_group.name"] = ag_name
        a["audience"] = audience
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for a in by_audience.values():
        compute_derived_metrics(a)
        results.append(a)

    rows_out, total, truncated, filter_desc, summary = process_rows(
        results, sort_by=sort_by, limit=limit,
    )

    if level.lower() == "adgroup":
        columns = [
            ("campaign.name", "Campaign"),
            ("ad_group.name", "Ad Group"),
            ("audience", "Audience"),
            ("_spend", "Spend \u20ac"),
            ("metrics.clicks", "Clicks"),
            ("metrics.impressions", "Impr"),
            ("_ctr", "CTR%"),
            ("metrics.conversions", "Conv"),
            ("_cpa", "CPA \u20ac"),
            ("_roas", "ROAS"),
        ]
    else:
        columns = [
            ("campaign.name", "Campaign"),
            ("audience", "Audience"),
            ("_spend", "Spend \u20ac"),
            ("metrics.clicks", "Clicks"),
            ("metrics.impressions", "Impr"),
            ("_ctr", "CTR%"),
            ("metrics.conversions", "Conv"),
            ("_cpa", "CPA \u20ac"),
            ("_roas", "ROAS"),
        ]

    header = build_header(
        title=f"Audience Performance ({level.title()} Level)",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} audience segments",
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
