"""R12: Find duplicate keywords across ad groups and campaigns."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    compute_derived_metrics,
    run_query,
)
from tools.options import format_output, build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def duplicate_keywords(
    client: str,
    limit: int = 50,
) -> str:
    """Find duplicate keywords appearing in multiple ad groups.

    USE THIS TOOL WHEN:
    - User asks about keyword conflicts or duplicates
    - "keyword duplicati", "parole chiave duplicate", "conflitti keyword"
    - Account cleanup audit

    DO NOT USE WHEN:
    - Keyword performance → use keyword_analysis
    - Negative keywords → use suggest_negatives

    OUTPUT: Table showing duplicated keywords with their locations and spend.

    Args:
        client: Account name or customer ID.
        limit: Max rows (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    q = (
        "SELECT ad_group_criterion.keyword.text, "
        "ad_group_criterion.keyword.match_type, "
        "campaign.name, ad_group.name, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros "
        "FROM ad_group_criterion "
        "WHERE ad_group_criterion.negative = FALSE "
        "AND ad_group_criterion.status = 'ENABLED'"
    )
    rows = run_query(customer_id, q)

    # Group by (keyword_text, match_type)
    by_kw = defaultdict(lambda: {
        "locations": [],
        "metrics.impressions": 0,
        "metrics.clicks": 0,
        "metrics.cost_micros": 0,
    })

    for row in rows:
        kw = row.get("ad_group_criterion.keyword.text", "")
        mt = row.get("ad_group_criterion.keyword.match_type", "")
        camp = row.get("campaign.name", "")
        ag = row.get("ad_group.name", "")
        key = (kw, mt)

        a = by_kw[key]
        location = f"{camp} > {ag}"
        if location not in a["locations"]:
            a["locations"].append(location)
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)

    # Filter: only where count > 1
    duplicates = []
    for (kw, mt), data in by_kw.items():
        if len(data["locations"]) > 1:
            spend = data["metrics.cost_micros"] / 1_000_000
            duplicates.append({
                "keyword": kw,
                "match_type": mt,
                "count": len(data["locations"]),
                "locations": "; ".join(data["locations"][:3]) + (
                    f" (+{len(data['locations'])-3} more)" if len(data["locations"]) > 3 else ""
                ),
                "_spend": round(spend, 2),
                "metrics.clicks": data["metrics.clicks"],
            })

    if not duplicates:
        return f"No duplicate keywords found in {client_name}."

    duplicates.sort(key=lambda r: r["_spend"], reverse=True)
    total = len(duplicates)
    if limit and limit < total:
        duplicates = duplicates[:limit]

    columns = [
        ("keyword", "Keyword"),
        ("match_type", "Match"),
        ("count", "Copies"),
        ("locations", "Locations"),
        ("metrics.clicks", "Clicks"),
        ("_spend", "Spend \u20ac"),
    ]

    header = build_header(
        title="Duplicate Keywords",
        client_name=client_name,
        extra=f"{total} duplicated keywords",
    )

    return format_output(duplicates, columns, header=header, output_mode="full")
