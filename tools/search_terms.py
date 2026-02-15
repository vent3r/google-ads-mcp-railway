"""Tool 5: search_term_analysis — Search term analysis with server-side processing."""

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
) -> str:
    """Analyze search terms with server-side processing for large volumes.

    Fetches up to 10,000 search terms, computes metrics server-side, and
    returns filtered/sorted results without saturating the LLM context.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional).
        min_clicks: Minimum clicks to include (default 1).
        max_cpa: Maximum CPA to include — 0 means no limit (default 0).
        zero_conversions: If true, show ONLY search terms with 0 conversions (default false).
        sort_by: Sort metric — spend, clicks, impressions, cpa, or ctr (default spend).
        limit: Maximum rows to return (default 50).
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
        "metrics.conversions, metrics.conversions_value, metrics.ctr "
        f"FROM search_term_view WHERE {where} "
        "LIMIT 10000"
    )

    rows = run_query(customer_id, query)
    total_raw = len(rows)

    # Compute derived metrics
    for row in rows:
        compute_derived_metrics(row)

    # Apply filters
    filtered = []
    for row in rows:
        clicks = int(row.get("metrics.clicks", 0) or 0)
        conversions = float(row.get("metrics.conversions", 0) or 0)
        cpa = row["_cpa"]

        if clicks < min_clicks:
            continue
        if zero_conversions and conversions > 0:
            continue
        if not zero_conversions and max_cpa > 0 and cpa > max_cpa and conversions > 0:
            continue

        filtered.append(row)

    total_filtered = len(filtered)

    # Sort
    sort_keys = {
        "spend": "_spend",
        "clicks": "metrics.clicks",
        "impressions": "metrics.impressions",
        "cpa": "_cpa",
        "ctr": "metrics.ctr",
    }
    sort_key = sort_keys.get(sort_by.lower(), "_spend")
    filtered.sort(key=lambda r: float(r.get(sort_key, 0) or 0), reverse=True)

    display = filtered[:limit]

    # Format
    output = []
    for row in display:
        output.append({
            "search_term": row.get("search_term_view.search_term", ""),
            "campaign": row.get("campaign.name", ""),
            "adgroup": row.get("ad_group.name", ""),
            "status": row.get("search_term_view.status", ""),
            "clicks": f"{int(row.get('metrics.clicks', 0) or 0):,}",
            "spend": ResultFormatter.format_currency(row["_spend"]),
            "conversions": f"{float(row.get('metrics.conversions', 0) or 0):,.1f}",
            "cpa": ResultFormatter.format_currency(row["_cpa"]),
        })

    columns = [
        ("search_term", "Search Term"),
        ("campaign", "Campaign"),
        ("adgroup", "Ad Group"),
        ("clicks", "Clicks"),
        ("spend", "Spend"),
        ("conversions", "Conv"),
        ("cpa", "CPA"),
    ]

    filters_desc = [f"clicks >= {min_clicks}"]
    if zero_conversions:
        filters_desc.append("zero conversions only")
    if max_cpa > 0:
        filters_desc.append(f"CPA <= {max_cpa}")

    header = (
        f"**Search Term Analysis** — {date_from} to {date_to}\n"
        f"Found {total_raw:,} search terms. "
        f"Showing top {min(limit, total_filtered)} by {sort_by} "
        f"(filtered: {', '.join(filters_desc)}). "
        f"{total_filtered:,} match filters.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
