"""R1: Suggest negative keywords based on wasteful search terms."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    compute_derived_metrics,
    run_query,
)
from tools.options import format_output, build_header, build_footer

logger = logging.getLogger(__name__)


@mcp.tool()
def suggest_negatives(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    min_spend: float = 5.0,
    min_clicks: int = 3,
    sort_by: str = "spend",
    limit: int = 50,
    output_mode: str = "summary",
) -> str:
    """Analyze search terms and suggest negative keywords to add.

    Finds search terms with spend > threshold and 0 conversions.
    Suggests match type: EXACT for single words, PHRASE for multi-word.

    USE THIS TOOL WHEN:
    - User wants to find wasteful search terms to block
    - "suggerisci negative", "quali search term bloccare"
    - Before using add_negatives to actually block them

    DO NOT USE WHEN:
    - General search term analysis → use search_term_analysis
    - Already know which keywords to add → use add_negatives directly

    OUTPUT: Table with wasteful search terms, spend, suggested match type, estimated savings.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional — all campaigns if empty).
        min_spend: Minimum spend in € to consider (default 5.0).
        min_clicks: Minimum clicks (default 3).
        sort_by: spend, clicks, ctr (default spend).
        limit: Max rows (default 50).
        output_mode: "summary" or "full". Default summary.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        f"SELECT search_term_view.search_term, "
        f"campaign.name, campaign.id, "
        f"metrics.impressions, metrics.clicks, metrics.cost_micros, "
        f"metrics.conversions, metrics.conversions_value "
        f"FROM search_term_view "
        f"WHERE {DateHelper.date_condition(date_from, date_to)}"
        f"{campaign_clause}"
    )
    rows = run_query(customer_id, q)

    by_term = {}
    for row in rows:
        term = row.get("search_term_view.search_term", "")
        if term not in by_term:
            by_term[term] = {
                "search_term": term,
                "metrics.impressions": 0,
                "metrics.clicks": 0,
                "metrics.cost_micros": 0,
                "metrics.conversions": 0.0,
                "metrics.conversions_value": 0.0,
                "campaigns": set(),
            }
        t = by_term[term]
        t["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        t["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        t["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        t["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        t["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)
        t["campaigns"].add(row.get("campaign.name", ""))

    results = []
    for t in by_term.values():
        compute_derived_metrics(t)
        if t["metrics.conversions"] > 0:
            continue
        if t["_spend"] < min_spend:
            continue
        if t["metrics.clicks"] < min_clicks:
            continue
        word_count = len(t["search_term"].split())
        t["suggested_match"] = "EXACT" if word_count == 1 else "PHRASE"
        t["savings_eur"] = t["_spend"]
        t["campaign_count"] = len(t["campaigns"])
        t.pop("campaigns")
        results.append(t)

    columns = [
        ("search_term", "Search Term"),
        ("_spend", "Spend €"),
        ("metrics.clicks", "Clicks"),
        ("_ctr", "CTR %"),
        ("suggested_match", "Match Type"),
        ("savings_eur", "Est. Savings €"),
        ("campaign_count", "Campaigns"),
    ]

    sort_key = {"spend": "_spend", "clicks": "metrics.clicks", "ctr": "_ctr"}.get(sort_by, "_spend")
    results.sort(key=lambda r: r.get(sort_key, 0), reverse=True)
    total = len(results)
    if limit and limit < total:
        results = results[:limit]

    total_savings = sum(r["savings_eur"] for r in results)

    header = build_header(
        title="Suggested Negatives",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        filter_desc=f"0 conversions, min spend €{min_spend}",
    )
    footer = f"\n**Total**: {total} wasteful terms | Est. savings: €{total_savings:,.2f}"

    return format_output(
        results,
        columns,
        header=header,
        footer=footer,
        output_mode=output_mode,
        total_filtered=total,
    )
