"""Tool 6: search_term_ngrams — N-gram aggregation with options.py pipeline.

Step 1: aggregate per-day rows by search term.
Step 2: extract n-grams and aggregate metrics across terms.
Step 3: process_rows for filtering/sorting/limiting.
"""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    run_query,
)
from tools.options import (
    COLUMNS,
    OutputFormat,
    build_footer,
    build_header,
    format_output,
    process_rows,
    text_match,
)

logger = logging.getLogger(__name__)


def _extract_ngrams(text: str, n: int) -> list:
    """Extract n-grams from text."""
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
    contains: str = "",
    excludes: str = "",
    min_clicks: int = 10,
    min_spend: float = 0,
    min_conversions: float = 0,
    max_cpa: float = 0,
    min_roas: float = 0,
    zero_conversions: bool = False,
    sort_by: str = "spend",
    limit: int = 50,
    output_mode: str = "summary",
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
        contains: Comma-separated — keep n-grams containing ANY of these words.
        excludes: Comma-separated — remove n-grams containing ANY of these words.
        min_clicks: Min aggregated clicks (default 10).
        min_spend: Minimum spend € (default 0).
        min_conversions: Minimum conversions (default 0).
        max_cpa: Max CPA € — 0 = no limit (default 0).
        min_roas: Minimum ROAS — 0 = no limit (default 0).
        zero_conversions: Only show n-grams with 0 conversions (default false).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows (default 50).
        output_mode: "summary" (top 10 + totals) or "full" (all rows). Default summary.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

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

    # Step 1: aggregate by search term
    term_agg = defaultdict(lambda: {
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

    # Step 2: extract n-grams and aggregate
    ngram_data = defaultdict(lambda: {
        "metrics.clicks": 0, "metrics.impressions": 0, "metrics.cost_micros": 0,
        "metrics.conversions": 0.0, "metrics.conversions_value": 0.0,
        "terms": set(),
    })

    for term, m in term_agg.items():
        for ng in _extract_ngrams(term, ngram_size):
            d = ngram_data[ng]
            d["metrics.clicks"] += m["clicks"]
            d["metrics.impressions"] += m["impressions"]
            d["metrics.cost_micros"] += m["cost_micros"]
            d["metrics.conversions"] += m["conversions"]
            d["metrics.conversions_value"] += m["conversions_value"]
            d["terms"].add(term)

    # Finalize: compute derived metrics, set ngram field
    aggregated = []
    for ngram, data in ngram_data.items():
        terms_set = data.pop("terms")
        data["ngram"] = ngram
        data["term_count"] = len(terms_set)

        # Compute derived metrics (needs _spend, _cpa, etc.)
        cost_micros = float(data["metrics.cost_micros"])
        spend = cost_micros / 1_000_000
        conv = float(data["metrics.conversions"])
        conv_val = float(data["metrics.conversions_value"])
        clicks = int(data["metrics.clicks"])
        impr = int(data["metrics.impressions"])

        data["_spend"] = round(spend, 2)
        data["_cpa"] = round(spend / conv, 2) if conv > 0 else 0.0
        data["_roas"] = round(conv_val / spend, 2) if spend > 0 else 0.0
        data["_ctr"] = round(clicks / impr * 100, 2) if impr > 0 else 0.0
        data["_cpc"] = round(spend / clicks, 2) if clicks > 0 else 0.0

        aggregated.append(data)

    # Apply options pipeline
    filtered, total, truncated, filter_desc, all_summary = process_rows(
        aggregated,
        text_field="ngram",
        contains=contains,
        excludes=excludes,
        min_clicks=min_clicks,
        min_spend=min_spend,
        min_conversions=min_conversions,
        max_cpa=max_cpa,
        min_roas=min_roas,
        zero_conversions=zero_conversions,
        sort_by=sort_by,
        limit=limit,
    )

    # Columns
    columns = COLUMNS.NGRAM

    label = {1: "unigrams", 2: "bigrams", 3: "trigrams"}.get(
        ngram_size, f"{ngram_size}-grams"
    )

    # Build output
    header = build_header(
        title=f"N-gram Analysis ({label})",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        filter_desc=filter_desc,
        extra=f"{total_terms:,} search terms",
    )
    footer = build_footer(total, len(filtered), truncated, all_summary)

    return format_output(filtered, columns, header=header, footer=footer,
                         output_mode=output_mode, pre_summary=all_summary,
                         total_filtered=total)
