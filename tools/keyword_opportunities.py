"""R2: Identify high-converting search terms that aren't yet keywords."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    DateHelper,
    compute_derived_metrics,
    run_query,
)
from tools.options import format_output, build_header, build_footer

logger = logging.getLogger(__name__)


@mcp.tool()
def keyword_opportunities(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    min_conversions: int = 1,
    min_clicks: int = 5,
    limit: int = 50,
    output_mode: str = "summary",
) -> str:
    """Identify high-converting search terms that aren't yet keywords.

    USE THIS TOOL WHEN:
    - Find search terms to convert into new keywords
    - "opportunità di keyword", "search terms da aggiungere"

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        min_conversions: Minimum conversions (default 1).
        min_clicks: Minimum clicks (default 5).
        limit: Max rows (default 50).
        output_mode: "summary" or "full". Default summary.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        from tools.name_resolver import resolve_campaign
        _, campaign_id = resolve_campaign(client, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        f"SELECT search_term_view.search_term, "
        f"metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value "
        f"FROM search_term_view "
        f"WHERE {DateHelper.date_condition(date_from, date_to)} {campaign_clause}"
    )
    search_terms = run_query(customer_id, q)

    q_kw = "SELECT ad_group_criterion.keyword.text FROM ad_group_criterion WHERE ad_group_criterion.negative = FALSE"
    keyword_rows = run_query(customer_id, q_kw)
    existing_keywords = {row.get("ad_group_criterion.keyword.text", "").lower() for row in keyword_rows}

    results = []
    for row in search_terms:
        term = row.get("search_term_view.search_term", "")
        conversions = float(row.get("metrics.conversions", 0) or 0)
        clicks = int(row.get("metrics.clicks", 0) or 0)

        if conversions < min_conversions or clicks < min_clicks:
            continue
        if term.lower() in existing_keywords:
            continue

        row["conversions_value"] = float(row.get("metrics.conversions_value", 0) or 0)
        compute_derived_metrics(row)
        results.append(row)

    results.sort(key=lambda r: r.get("metrics.conversions", 0), reverse=True)
    total = len(results)
    if limit and limit < total:
        results = results[:limit]

    columns = [
        ("search_term_view.search_term", "Search Term"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend €"),
        ("metrics.conversions", "Conv"),
        ("_roas", "ROAS"),
    ]

    header = build_header(
        title="Keyword Opportunities",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
    )
    footer = f"\n**Total**: {total} opportunities"

    return format_output(results, columns, header=header, footer=footer, output_mode=output_mode)
