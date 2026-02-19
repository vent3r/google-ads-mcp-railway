"""T10: Demographic performance — age, gender, income, parental status."""

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
from tools.options import build_header, format_output

logger = logging.getLogger(__name__)

_DIMENSION_CONFIG = {
    "age": {
        "resource": "age_range_view",
        "field": "ad_group_criterion.age_range.type",
        "title": "Age Range",
    },
    "gender": {
        "resource": "gender_view",
        "field": "ad_group_criterion.gender.type",
        "title": "Gender",
    },
    "income": {
        "resource": "income_range_view",
        "field": "ad_group_criterion.income_range.type",
        "title": "Income Range",
    },
    "parental_status": {
        "resource": "parental_status_view",
        "field": "ad_group_criterion.parental_status.type",
        "title": "Parental Status",
    },
}


@mcp.tool()
def demographics(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    dimension: str = "all",
) -> str:
    """Analyze performance by demographic dimensions (age, gender, income, parental status).

    USE THIS TOOL WHEN:
    - User asks about demographics, age/gender performance
    - "performance per eta", "genere", "reddito", "stato genitoriale"
    - "demographics", "fascia d'eta"

    DO NOT USE WHEN:
    - Audience segments/remarketing -> use audience_performance
    - Device breakdown -> use device_breakdown

    OUTPUT: Multi-section markdown with one table per demographic dimension.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        dimension: "age", "gender", "income", "parental_status", or "all" (default all).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    date_cond = DateHelper.date_condition(date_from, date_to)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    dim_key = dimension.lower().strip()
    if dim_key == "all":
        dimensions = list(_DIMENSION_CONFIG.keys())
    elif dim_key in _DIMENSION_CONFIG:
        dimensions = [dim_key]
    else:
        return f"Invalid dimension: {dimension}. Choose from: age, gender, income, parental_status, all"

    main_header = build_header(
        title="Demographic Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
    )
    parts = [f"**{main_header}**"]

    columns = [
        ("segment", "Segment"),
        ("_spend", "Spend \u20ac"),
        ("metrics.clicks", "Clicks"),
        ("metrics.impressions", "Impr"),
        ("_ctr", "CTR%"),
        ("metrics.conversions", "Conv"),
        ("_cpa", "CPA \u20ac"),
        ("metrics.conversions_value", "Value \u20ac"),
        ("_roas", "ROAS"),
    ]

    for dim in dimensions:
        config = _DIMENSION_CONFIG[dim]
        q = (
            f"SELECT "
            f"{config['field']}, campaign.name, "
            f"metrics.impressions, metrics.clicks, metrics.cost_micros, "
            f"metrics.conversions, metrics.conversions_value "
            f"FROM {config['resource']} "
            f"WHERE {date_cond}{campaign_clause}"
        )

        try:
            rows = run_query(customer_id, q)
        except ValueError as e:
            logger.warning("demographics: %s query failed: %s", dim, e)
            parts.append(f"\n## {config['title']}\n\nNo data available.")
            continue

        by_segment = defaultdict(lambda: {
            "segment": "",
            "metrics.impressions": 0, "metrics.clicks": 0,
            "metrics.cost_micros": 0, "metrics.conversions": 0.0,
            "metrics.conversions_value": 0.0,
        })

        for row in rows:
            seg_val = str(row.get(config["field"], "")).replace("_", " ").title()
            a = by_segment[seg_val]
            a["segment"] = seg_val
            a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
            a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

        results = []
        for a in by_segment.values():
            compute_derived_metrics(a)
            results.append(a)

        results.sort(key=lambda r: r.get("_spend", 0), reverse=True)

        parts.append(f"\n## {config['title']}")
        parts.append(format_output(results, columns, output_mode="full"))

    return "\n".join(parts)
