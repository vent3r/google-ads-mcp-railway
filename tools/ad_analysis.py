"""R4: Ad performance analysis with ad strength and RSA metrics."""

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
def ad_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    adgroup: str = "",
    sort_by: str = "spend",
    limit: int = 50,
    output_mode: str = "summary",
) -> str:
    """Analyze ad performance with ad strength and creative metrics.

    USE THIS TOOL WHEN:
    - User asks about ad performance, ad strength, RSA quality
    - "performance annunci", "ad strength", "quali annunci funzionano"
    - Investigating underperforming creatives

    DO NOT USE WHEN:
    - Campaign-level overview → use campaign_analysis
    - Keyword performance → use keyword_analysis

    OUTPUT: Markdown table with ads sorted by spend.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        adgroup: Ad group name or ID (optional).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows (default 50).
        output_mode: "summary" or "full". Default summary.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    conditions = [
        DateHelper.date_condition(date_from, date_to),
        "ad_group_ad.status != 'REMOVED'",
    ]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    q = (
        "SELECT ad_group_ad.ad.id, ad_group_ad.ad.type, "
        "ad_group_ad.ad_strength, ad_group_ad.status, "
        "ad_group_ad.ad.final_urls, "
        "campaign.name, ad_group.name, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM ad_group_ad WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by ad ID (rows may be per-day)
    by_ad = defaultdict(lambda: {
        "ad_id": "", "ad_type": "", "ad_strength": "", "ad_status": "",
        "campaign.name": "", "ad_group.name": "", "final_url": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        ad_id = str(row.get("ad_group_ad.ad.id", ""))
        a = by_ad[ad_id]
        a["ad_id"] = ad_id
        a["ad_type"] = str(row.get("ad_group_ad.ad.type", "")).replace("_", " ").title()
        strength = row.get("ad_group_ad.ad_strength", "")
        if strength:
            a["ad_strength"] = str(strength).replace("_", " ").title()
        a["ad_status"] = str(row.get("ad_group_ad.status", ""))
        a["campaign.name"] = row.get("campaign.name", "")
        a["ad_group.name"] = row.get("ad_group.name", "")
        urls = row.get("ad_group_ad.ad.final_urls", [])
        if urls:
            url = urls[0] if isinstance(urls, list) else str(urls)
            a["final_url"] = url[:60] + "..." if len(str(url)) > 60 else str(url)
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for a in by_ad.values():
        compute_derived_metrics(a)
        results.append(a)

    sort_key = {"spend": "_spend", "clicks": "metrics.clicks", "conversions": "metrics.conversions",
                "cpa": "_cpa", "roas": "_roas"}.get(sort_by, "_spend")
    results.sort(key=lambda r: r.get(sort_key, 0), reverse=(sort_by != "cpa"))
    total = len(results)
    if limit and limit < total:
        results = results[:limit]

    columns = [
        ("ad_id", "Ad ID"),
        ("ad_type", "Type"),
        ("ad_strength", "Strength"),
        ("campaign.name", "Campaign"),
        ("ad_group.name", "Ad Group"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend €"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA €"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="Ad Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} ads",
    )
    footer = f"\n**Showing {len(results)} of {total} ads.**" if total > len(results) else ""

    return format_output(results, columns, header=header, footer=footer, output_mode=output_mode)
