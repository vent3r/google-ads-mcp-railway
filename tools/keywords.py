"""Tool 4: keyword_analysis — Keyword performance with quality score.

GAQL → aggregate by keyword+campaign+adgroup+match → compute_derived_metrics →
process_rows (filter/sort/limit) → format_output with QS columns and benchmarks.
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
from tools.options import (
    Benchmarks,
    COLUMNS,
    OutputFormat,
    build_footer,
    build_header,
    format_output,
    process_rows,
)

logger = logging.getLogger(__name__)


@mcp.tool()
def keyword_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    match_type: str = "ALL",
    contains: str = "",
    excludes: str = "",
    status: str = "",
    min_clicks: int = 0,
    min_spend: float = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    min_ctr: float = 0,
    max_cpc: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze keyword performance with quality score data.

    Aggregates per-day rows, adds quality score breakdown (QS, creative,
    landing page, expected CTR), applies universal filters and sorts.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        match_type: EXACT, PHRASE, BROAD, or ALL (default ALL).
        contains: Comma-separated — keep keywords whose text contains ANY of these.
        excludes: Comma-separated — remove keywords whose text contains ANY of these.
        status: Filter by status: ENABLED, PAUSED, or empty for all.
        min_clicks: Minimum clicks (default 0).
        min_spend: Minimum spend € (default 0).
        min_conversions: Minimum conversions (default 0).
        max_cpa: Maximum CPA € — 0 = no limit (default 0).
        min_roas: Minimum ROAS — 0 = no limit (default 0).
        min_ctr: Minimum CTR % (default 0).
        max_cpc: Maximum CPC € — 0 = no limit (default 0).
        zero_conversions: If true, only show keywords with 0 conversions (default false).
        sort_by: spend, clicks, conversions, cpa, roas, ctr, quality_score (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

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

    # Aggregate by keyword + campaign + adgroup + match_type
    agg = defaultdict(lambda: {
        "campaign.name": "", "ad_group.name": "",
        "kw_text": "", "kw_match_type": "",
        "ad_group_criterion.status": "",
        "qs": 0, "qs_creative": "", "qs_landing": "", "qs_ctr": "",
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
        a["kw_match_type"] = mt
        a["ad_group_criterion.status"] = row.get("ad_group_criterion.status", "")

        # Quality score: take latest non-null value
        qs = row.get("ad_group_criterion.quality_info.quality_score")
        if qs and qs != 0:
            a["qs"] = int(qs)
        qsc = row.get("ad_group_criterion.quality_info.creative_quality_score", "")
        if qsc:
            a["qs_creative"] = str(qsc).replace("_", " ").title()
        qsl = row.get("ad_group_criterion.quality_info.post_click_quality_score", "")
        if qsl:
            a["qs_landing"] = str(qsl).replace("_", " ").title()
        qsctr = row.get("ad_group_criterion.quality_info.search_predicted_ctr", "")
        if qsctr:
            a["qs_ctr"] = str(qsctr).replace("_", " ").title()

        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    # Compute derived metrics
    aggregated = list(agg.values())
    for row in aggregated:
        compute_derived_metrics(row)

    # Apply options pipeline
    filtered, total, truncated, filter_desc = process_rows(
        aggregated,
        text_field="kw_text",
        contains=contains,
        excludes=excludes,
        status=status,
        min_clicks=min_clicks,
        min_spend=min_spend,
        min_conversions=min_conversions,
        max_cpa=max_cpa,
        min_roas=min_roas,
        min_ctr=min_ctr,
        max_cpc=max_cpc,
        zero_conversions=zero_conversions,
        sort_by=sort_by,
        ascending=(sort_by.lower() == "cpa"),
        limit=limit,
    )

    # Benchmarks
    alerts = Benchmarks.summarize_flags(filtered, name_field="kw_text")

    # Summary
    summary = OutputFormat.summary_row(filtered) if filtered else None

    # Columns: KEYWORD preset + QS breakdown
    columns = COLUMNS.KEYWORD + [
        ("qs_ctr", "Exp CTR"),
        ("qs_landing", "Landing"),
        ("qs_creative", "Ad Rel"),
    ]

    # Build output
    header = build_header(
        title="Keyword Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        filter_desc=filter_desc,
    )
    footer = build_footer(total, len(filtered), truncated, summary)

    result = format_output(filtered, columns, header=header, footer=footer)

    if alerts:
        result += f"\n\n{alerts}"

    return result
