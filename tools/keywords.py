"""Tool 4: keyword_analysis — Keyword performance with quality score.

Enhancements vs original:
- quality_info.quality_score, creative_quality_score, post_click_quality_score,
  search_predicted_ctr
- Cleaner aggregation and output
"""

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
    """Analyze keyword performance with quality score data.

    Aggregates per-day rows, adds quality score breakdown (QS, creative,
    landing page, expected CTR), filters and sorts.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        min_clicks: Minimum clicks (default 0).
        max_cpa: Max CPA filter — 0 = no limit (default 0).
        min_conversions: Min conversions (default 0).
        match_type: EXACT, PHRASE, BROAD, or ALL (default ALL).
        sort_by: spend, clicks, conversions, cpa, ctr, or quality_score (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)

    conditions = [DateHelper.date_condition(date_from, date_to)]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")
    if match_type.upper() != "ALL":
        conditions.append(
            f"ad_group_criterion.keyword.match_type = '{match_type.upper()}'"
        )

    where = " AND ".join(conditions)

    query = (
        "SELECT "
        "campaign.name, ad_group.name, "
        "ad_group_criterion.keyword.text, "
        "ad_group_criterion.keyword.match_type, "
        "ad_group_criterion.status, "
        "ad_group_criterion.quality_info.quality_score, "
        "ad_group_criterion.quality_info.creative_quality_score, "
        "ad_group_criterion.quality_info.post_click_quality_score, "
        "ad_group_criterion.quality_info.search_predicted_ctr, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM keyword_view WHERE {where}"
    )

    rows = run_query(customer_id, query)
    total_api = len(rows)

    # Aggregate by keyword + campaign + adgroup + match_type
    agg = defaultdict(lambda: {
        "campaign.name": "", "ad_group.name": "",
        "kw_text": "", "kw_match": "", "status": "",
        "qs": None, "qs_creative": "", "qs_landing": "", "qs_ctr": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        kw = row.get("ad_group_criterion.keyword.text", "")
        camp = row.get("campaign.name", "")
        ag = row.get("ad_group.name", "")
        mt = row.get("ad_group_criterion.keyword.match_type", "")
        key = (kw, camp, ag, mt)

        a = agg[key]
        a["campaign.name"] = camp
        a["ad_group.name"] = ag
        a["kw_text"] = kw
        a["kw_match"] = mt
        a["status"] = row.get("ad_group_criterion.status", "")

        # Quality score: take latest non-null value (doesn't change daily)
        qs = row.get("ad_group_criterion.quality_info.quality_score")
        if qs and qs != 0:
            a["qs"] = int(qs)
        qsc = row.get("ad_group_criterion.quality_info.creative_quality_score", "")
        if qsc:
            a["qs_creative"] = qsc
        qsl = row.get("ad_group_criterion.quality_info.post_click_quality_score", "")
        if qsl:
            a["qs_landing"] = qsl
        qsctr = row.get("ad_group_criterion.quality_info.search_predicted_ctr", "")
        if qsctr:
            a["qs_ctr"] = qsctr

        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    # Process
    processed = []
    for row in agg.values():
        compute_derived_metrics(row)
        clicks = int(row.get("metrics.clicks", 0))
        conv = float(row.get("metrics.conversions", 0))
        cpa = row["_cpa"]

        if clicks < min_clicks:
            continue
        if conv < min_conversions:
            continue
        if max_cpa > 0 and cpa > max_cpa and conv > 0:
            continue
        processed.append(row)

    logger.info("keyword_analysis: %d API rows -> %d keywords", total_api, len(processed))

    # Sort
    sort_keys = {
        "spend": "_spend", "clicks": "metrics.clicks",
        "conversions": "metrics.conversions", "cpa": "_cpa",
        "ctr": "_ctr", "quality_score": "qs",
    }
    sk = sort_keys.get(sort_by.lower(), "_spend")
    processed.sort(
        key=lambda r: float(r.get(sk, 0) or 0),
        reverse=(sort_by.lower() != "cpa"),
    )

    total = len(processed)
    processed = processed[:limit]

    output = []
    for row in processed:
        output.append({
            "keyword": row["kw_text"],
            "match": row["kw_match"],
            "campaign": row["campaign.name"],
            "clicks": ResultFormatter.fmt_int(row["metrics.clicks"]),
            "spend": ResultFormatter.fmt_currency(row["_spend"]),
            "conv": f"{float(row['metrics.conversions']):,.1f}",
            "cpa": ResultFormatter.fmt_currency(row["_cpa"]),
            "qs": str(row["qs"]) if row["qs"] else "-",
            "qs_ctr": str(row["qs_ctr"]).replace("_", " ").title() if row["qs_ctr"] else "-",
            "qs_landing": str(row["qs_landing"]).replace("_", " ").title() if row["qs_landing"] else "-",
        })

    columns = [
        ("keyword", "Keyword"), ("match", "Match"), ("campaign", "Campaign"),
        ("clicks", "Clicks"), ("spend", "Spend"), ("conv", "Conv"),
        ("cpa", "CPA"), ("qs", "QS"), ("qs_ctr", "Exp CTR"),
        ("qs_landing", "Landing"),
    ]

    header = (
        f"**Keyword Analysis** — {date_from} to {date_to}\n"
        f"{total:,} keywords ({total_api:,} API rows). Sorted by {sort_by}.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
