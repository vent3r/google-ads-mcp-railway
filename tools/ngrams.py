"""Tool 6: search_term_ngrams — N-gram aggregation of search terms."""

from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    run_query,
)


def _extract_ngrams(text: str, n: int) -> list[str]:
    """Extract n-grams from a text string."""
    words = text.lower().split()
    if len(words) < n:
        return [" ".join(words)] if words else []
    return [" ".join(words[i : i + n]) for i in range(len(words) - n + 1)]


@mcp.tool()
def search_term_ngrams(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    ngram_size: int = 1,
    min_clicks: int = 10,
    max_cpa: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 30,
) -> str:
    """Aggregate search terms into n-grams to find patterns.

    Fetches all search terms, extracts n-grams (unigrams, bigrams, or trigrams),
    aggregates metrics across all search terms containing each n-gram, and
    returns the top results. Useful for finding high-spend or wasteful word patterns.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID to filter (optional).
        ngram_size: Size of n-grams — 1, 2, or 3 (default 1).
        min_clicks: Minimum aggregated clicks to include (default 10).
        max_cpa: Maximum aggregated CPA — 0 means no limit (default 0).
        zero_conversions: If true, show only n-grams with 0 conversions (default false).
        sort_by: Sort metric — spend, clicks, conversions, or cpa (default spend).
        limit: Maximum rows to return (default 30).
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
        "search_term_view.search_term, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM search_term_view WHERE {where} "
        "LIMIT 10000"
    )

    rows = run_query(customer_id, query)
    total_terms = len(rows)

    # Aggregate n-grams
    ngram_data: dict[str, dict] = defaultdict(
        lambda: {
            "clicks": 0,
            "impressions": 0,
            "cost_micros": 0,
            "conversions": 0.0,
            "conversions_value": 0.0,
            "frequency": 0,
            "terms": set(),
        }
    )

    for row in rows:
        term = row.get("search_term_view.search_term", "")
        ngrams = _extract_ngrams(term, ngram_size)

        clicks = int(row.get("metrics.clicks", 0) or 0)
        impressions = int(row.get("metrics.impressions", 0) or 0)
        cost_micros = float(row.get("metrics.cost_micros", 0) or 0)
        conversions = float(row.get("metrics.conversions", 0) or 0)
        conv_value = float(row.get("metrics.conversions_value", 0) or 0)

        for ng in ngrams:
            d = ngram_data[ng]
            d["clicks"] += clicks
            d["impressions"] += impressions
            d["cost_micros"] += cost_micros
            d["conversions"] += conversions
            d["conversions_value"] += conv_value
            d["terms"].add(term)
            d["frequency"] = len(d["terms"])

    total_ngrams = len(ngram_data)

    # Compute derived metrics and filter
    processed = []
    for ngram, data in ngram_data.items():
        spend = data["cost_micros"] / 1_000_000
        conversions = data["conversions"]
        cpa = spend / conversions if conversions > 0 else 0.0

        if data["clicks"] < min_clicks:
            continue
        if zero_conversions and conversions > 0:
            continue
        if not zero_conversions and max_cpa > 0 and cpa > max_cpa and conversions > 0:
            continue

        processed.append({
            "ngram": ngram,
            "frequency": data["frequency"],
            "clicks": data["clicks"],
            "impressions": data["impressions"],
            "spend": round(spend, 2),
            "conversions": round(conversions, 1),
            "cpa": round(cpa, 2),
        })

    # Sort
    sort_key = sort_by.lower() if sort_by.lower() in ("spend", "clicks", "conversions", "cpa") else "spend"
    processed.sort(key=lambda r: r.get(sort_key, 0), reverse=True)

    total_matching = len(processed)
    processed = processed[:limit]

    # Format for display
    output = []
    for row in processed:
        output.append({
            "ngram": row["ngram"],
            "frequency": str(row["frequency"]),
            "clicks": f"{row['clicks']:,}",
            "spend": ResultFormatter.format_currency(row["spend"]),
            "conversions": f"{row['conversions']:,.1f}",
            "cpa": ResultFormatter.format_currency(row["cpa"]),
        })

    columns = [
        ("ngram", "N-gram"),
        ("frequency", "Freq"),
        ("clicks", "Clicks"),
        ("spend", "Spend"),
        ("conversions", "Conv"),
        ("cpa", "CPA"),
    ]

    ngram_label = {1: "unigrams", 2: "bigrams", 3: "trigrams"}.get(ngram_size, f"{ngram_size}-grams")

    header = (
        f"**Search Term N-gram Analysis** — {date_from} to {date_to}\n"
        f"Analyzed {total_terms:,} search terms. "
        f"Extracted {total_ngrams:,} unique {ngram_label}. "
        f"{total_matching:,} match filters.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
