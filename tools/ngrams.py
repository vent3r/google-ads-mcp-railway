"""Tool 6: search_term_ngrams — N-gram aggregation of search terms."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    ResultFormatter,
    run_query,
)

logger = logging.getLogger(__name__)


def _extract_ngrams(text: str, n: int) -> list[str]:
    words = text.lower().split()
    if len(words) < n:
        return [" ".join(words)] if words else []
    return [" ".join(words[i:i + n]) for i in range(len(words) - n + 1)]


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
    """Aggregate search terms into n-grams to find spending patterns.

    Step 1: aggregate per-day rows by search term.
    Step 2: extract n-grams and aggregate metrics across terms.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        ngram_size: 1, 2, or 3 (default 1).
        min_clicks: Min aggregated clicks (default 10).
        max_cpa: Max CPA — 0 = no limit (default 0).
        zero_conversions: Only show n-grams with 0 conversions (default false).
        sort_by: spend, clicks, conversions, or cpa (default spend).
        limit: Max rows (default 30).
    """
    customer_id = ClientResolver.resolve(client)

    conditions = [
        DateHelper.date_condition(date_from, date_to),
        "metrics.impressions > 0",
    ]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    query = (
        "SELECT "
        "search_term_view.search_term, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value "
        f"FROM search_term_view WHERE {' AND '.join(conditions)}"
    )

    rows = run_query(customer_id, query)
    total_api = len(rows)

    # Step 1: aggregate by search term
    term_agg: dict[str, dict] = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "cost_micros": 0,
        "conversions": 0.0, "conversions_value": 0.0,
    })

    for row in rows:
        term = row.get("search_term_view.search_term", "")
        if not term:
            continue
        a = term_agg[term]
        a["impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        a["conversions"] += float(row.get("metrics.conversions", 0) or 0)
        a["conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

    total_terms = len(term_agg)

    # Step 2: extract n-grams
    ngram_data: dict[str, dict] = defaultdict(lambda: {
        "clicks": 0, "impressions": 0, "cost_micros": 0,
        "conversions": 0.0, "conversions_value": 0.0, "terms": set(),
    })

    for term, m in term_agg.items():
        for ng in _extract_ngrams(term, ngram_size):
            d = ngram_data[ng]
            d["clicks"] += m["clicks"]
            d["impressions"] += m["impressions"]
            d["cost_micros"] += m["cost_micros"]
            d["conversions"] += m["conversions"]
            d["conversions_value"] += m["conversions_value"]
            d["terms"].add(term)

    # Filter
    processed = []
    for ngram, data in ngram_data.items():
        spend = data["cost_micros"] / 1_000_000
        conv = data["conversions"]
        cpa = spend / conv if conv > 0 else 0.0

        if data["clicks"] < min_clicks:
            continue
        if zero_conversions and conv > 0:
            continue
        if not zero_conversions and max_cpa > 0 and cpa > max_cpa and conv > 0:
            continue

        processed.append({
            "ngram": ngram,
            "freq": len(data["terms"]),
            "clicks": data["clicks"],
            "spend": round(spend, 2),
            "conv": round(conv, 1),
            "cpa": round(cpa, 2),
            "roas": round(data["conversions_value"] / spend, 2) if spend > 0 else 0.0,
        })

    sk = sort_by.lower() if sort_by.lower() in ("spend", "clicks", "conversions", "cpa") else "spend"
    if sk == "conversions":
        sk = "conv"
    processed.sort(key=lambda r: r.get(sk, 0), reverse=True)

    total_match = len(processed)
    processed = processed[:limit]

    output = []
    for row in processed:
        output.append({
            "ngram": row["ngram"],
            "freq": str(row["freq"]),
            "clicks": f"{row['clicks']:,}",
            "spend": ResultFormatter.fmt_currency(row["spend"]),
            "conv": f"{row['conv']:,.1f}",
            "cpa": ResultFormatter.fmt_currency(row["cpa"]),
            "roas": f"{row['roas']:.2f}",
        })

    columns = [
        ("ngram", "N-gram"), ("freq", "Freq"), ("clicks", "Clicks"),
        ("spend", "Spend"), ("conv", "Conv"), ("cpa", "CPA"), ("roas", "ROAS"),
    ]

    label = {1: "unigrams", 2: "bigrams", 3: "trigrams"}.get(ngram_size, f"{ngram_size}-grams")

    header = (
        f"**N-gram Analysis ({label})** — {date_from} to {date_to}\n"
        f"{total_terms:,} terms ({total_api:,} API rows). "
        f"{total_match:,} {label} match filters.\n\n"
    )

    return header + ResultFormatter.markdown_table(output, columns, max_rows=limit)
