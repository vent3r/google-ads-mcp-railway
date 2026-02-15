"""Tool 3: adgroup_analysis — Ad group performance analysis."""

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    compute_derived_metrics,
    run_query,
)


@mcp.tool()
def adgroup_analysis(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    min_clicks: int = 0,
    sort_by: str = "spend",
    limit: int = 30,
) -> str:
    """Analyze ad group performance, optionally filtered by campaign.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional — omit for all campaigns).
        min_clicks: Minimum clicks to include an ad group (default 0).
        sort_by: Sort metric — spend, clicks, conversions, or cpa (default spend).
        limit: Maximum rows to return (default 30).
    """
    customer_id = ClientResolver.resolve(client)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    query = (
        "SELECT "
        "campaign.name, ad_group.name, ad_group.id, ad_group.status, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, "
        "metrics.ctr, metrics.average_cpc "
        "FROM ad_group "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{campaign_clause}"
    )

    rows = run_query(customer_id, query)

    # Compute derived metrics and filter
    processed = []
    for row in rows:
        compute_derived_metrics(row)
        clicks = int(row.get("metrics.clicks", 0) or 0)
        if clicks >= min_clicks:
            processed.append(row)

    # Sort
    sort_keys = {
        "spend": "_spend",
        "clicks": "metrics.clicks",
        "conversions": "metrics.conversions",
        "cpa": "_cpa",
    }
    sort_key = sort_keys.get(sort_by.lower(), "_spend")
    processed.sort(key=lambda r: float(r.get(sort_key, 0) or 0), reverse=True)

    # Truncate
    total = len(processed)
    processed = processed[:limit]

    # Format output
    output = []
    for row in processed:
        output.append({
            "campaign": row.get("campaign.name", ""),
            "adgroup": row.get("ad_group.name", ""),
            "status": row.get("ad_group.status", ""),
            "impressions": f"{int(row.get('metrics.impressions', 0) or 0):,}",
            "clicks": f"{int(row.get('metrics.clicks', 0) or 0):,}",
            "spend": ResultFormatter.format_currency(row["_spend"]),
            "conversions": f"{float(row.get('metrics.conversions', 0) or 0):,.1f}",
            "cpa": ResultFormatter.format_currency(row["_cpa"]),
            "roas": f"{row['_roas']:.2f}",
        })

    columns = [
        ("campaign", "Campaign"),
        ("adgroup", "Ad Group"),
        ("status", "Status"),
        ("impressions", "Impr"),
        ("clicks", "Clicks"),
        ("spend", "Spend"),
        ("conversions", "Conv"),
        ("cpa", "CPA"),
        ("roas", "ROAS"),
    ]

    header = (
        f"**Ad Group Analysis** — {date_from} to {date_to}\n"
        f"Sorted by {sort_by} (min clicks: {min_clicks})\n\n"
    )

    table = ResultFormatter.markdown_table(output, columns, max_rows=limit)
    if total > limit:
        table += f"\n\n*Showing {limit} of {total:,} ad groups matching filters.*"

    return header + table
