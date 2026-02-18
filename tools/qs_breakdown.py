"""R13: Quality score breakdown — distribution and low-QS keyword analysis."""

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
from tools.options import format_output, build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def qs_breakdown(
    client: str,
    campaign: str = "",
    date_from: str = "",
    date_to: str = "",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze quality score distribution and identify low-QS keywords.

    Shows QS distribution (1-3/4-6/7-10) and highlights keywords with
    low QS and high spend.

    USE THIS TOOL WHEN:
    - User asks about quality scores
    - "quality score", "QS", "punteggio qualit\u00e0"
    - Investigating high CPC or low ad rank

    DO NOT USE WHEN:
    - General keyword performance → use keyword_analysis
    - Keyword ideas → use keyword_ideas

    OUTPUT: QS distribution summary + table of keywords with QS components.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (optional).
        date_from: Start date YYYY-MM-DD (optional, defaults to last 30 days).
        date_to: End date YYYY-MM-DD (optional).
        sort_by: spend, quality_score, clicks (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    if not date_from or not date_to:
        date_from, date_to = DateHelper.days_ago(30)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        "SELECT ad_group_criterion.keyword.text, "
        "ad_group_criterion.keyword.match_type, "
        "ad_group_criterion.quality_info.quality_score, "
        "ad_group_criterion.quality_info.creative_quality_score, "
        "ad_group_criterion.quality_info.post_click_quality_score, "
        "ad_group_criterion.quality_info.search_predicted_ctr, "
        "campaign.name, ad_group.name, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros "
        "FROM keyword_view "
        f"WHERE ad_group_criterion.status = 'ENABLED' "
        f"AND ad_group_criterion.negative = FALSE"
        f"{campaign_clause} "
        f"AND {DateHelper.date_condition(date_from, date_to)}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by keyword + campaign + adgroup
    by_kw = defaultdict(lambda: {
        "keyword": "", "match_type": "", "qs": 0,
        "expected_ctr": "", "ad_relevance": "", "landing_page": "",
        "campaign.name": "", "ad_group.name": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0,
    })

    for row in rows:
        kw = row.get("ad_group_criterion.keyword.text", "")
        camp = row.get("campaign.name", "")
        ag = row.get("ad_group.name", "")
        key = (kw, camp, ag)
        a = by_kw[key]
        a["keyword"] = kw
        a["match_type"] = row.get("ad_group_criterion.keyword.match_type", "")
        a["campaign.name"] = camp
        a["ad_group.name"] = ag

        qs = row.get("ad_group_criterion.quality_info.quality_score")
        if qs and int(qs) > 0:
            a["qs"] = int(qs)
        ctr = row.get("ad_group_criterion.quality_info.search_predicted_ctr", "")
        if ctr:
            a["expected_ctr"] = str(ctr).replace("_", " ").title()
        cr = row.get("ad_group_criterion.quality_info.creative_quality_score", "")
        if cr:
            a["ad_relevance"] = str(cr).replace("_", " ").title()
        lp = row.get("ad_group_criterion.quality_info.post_click_quality_score", "")
        if lp:
            a["landing_page"] = str(lp).replace("_", " ").title()

        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)

    results = list(by_kw.values())
    for r in results:
        r["_spend"] = round(r["metrics.cost_micros"] / 1_000_000, 2)

    # QS distribution
    qs_low = sum(1 for r in results if 0 < r["qs"] <= 3)
    qs_mid = sum(1 for r in results if 4 <= r["qs"] <= 6)
    qs_high = sum(1 for r in results if r["qs"] >= 7)
    qs_none = sum(1 for r in results if r["qs"] == 0)

    sort_key = {"spend": "_spend", "quality_score": "qs", "clicks": "metrics.clicks"}.get(sort_by, "_spend")
    reverse = sort_by != "quality_score"
    results.sort(key=lambda r: r.get(sort_key, 0), reverse=reverse)
    total = len(results)
    if limit and limit < total:
        results = results[:limit]

    columns = [
        ("keyword", "Keyword"),
        ("match_type", "Match"),
        ("qs", "QS"),
        ("expected_ctr", "Exp CTR"),
        ("ad_relevance", "Ad Rel"),
        ("landing_page", "Landing"),
        ("campaign.name", "Campaign"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend \u20ac"),
    ]

    header = build_header(
        title="Quality Score Breakdown",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"QS Distribution: Low(1-3): {qs_low} | Mid(4-6): {qs_mid} | High(7-10): {qs_high} | N/A: {qs_none}",
    )
    footer = f"\n**Showing {len(results)} of {total} keywords.**" if total > len(results) else ""

    return format_output(results, columns, header=header, footer=footer, output_mode="full")
