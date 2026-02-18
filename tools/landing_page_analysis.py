"""R8: Landing page performance analysis."""

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
def landing_page_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze landing page performance.

    USE THIS TOOL WHEN:
    - User asks about landing page performance
    - "performance pagine di destinazione", "landing page", "URL performance"
    - Identifying low-converting landing pages

    DO NOT USE WHEN:
    - Ad creative analysis → use ad_analysis
    - Campaign performance → use campaign_analysis

    OUTPUT: Table with landing pages sorted by spend.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        "SELECT landing_page_view.unexpanded_final_url, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM landing_page_view "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}{campaign_clause}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by URL
    by_url = defaultdict(lambda: {
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        url = row.get("landing_page_view.unexpanded_final_url", "")
        a = by_url[url]
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    results = []
    for url, a in by_url.items():
        a["url"] = url[:60] + "..." if len(url) > 60 else url
        a["full_url"] = url
        compute_derived_metrics(a)
        results.append(a)

    sort_key = {"spend": "_spend", "clicks": "metrics.clicks", "conversions": "metrics.conversions",
                "cpa": "_cpa", "roas": "_roas"}.get(sort_by, "_spend")
    results.sort(key=lambda r: r.get(sort_key, 0), reverse=(sort_by != "cpa"))
    total = len(results)
    if limit and limit < total:
        results = results[:limit]

    columns = [
        ("url", "Landing Page"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend €"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA €"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="Landing Page Analysis",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} pages",
    )
    footer = f"\n**Showing {len(results)} of {total} pages.**" if total > len(results) else ""

    return format_output(results, columns, header=header, footer=footer, output_mode="full")
