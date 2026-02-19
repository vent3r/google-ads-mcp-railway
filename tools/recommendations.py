"""T13: Google Ads recommendations from the API."""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    run_query,
)
from tools.options import build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def recommendations(
    client: str,
    campaign: str = "",
    recommendation_type: str = "",
) -> str:
    """Surface Google's optimization recommendations from the API.

    No date segments — returns current active (non-dismissed) recommendations.
    Shows recommendation type, target campaign/ad group, and estimated impact.

    USE THIS TOOL WHEN:
    - User asks about Google recommendations, optimization suggestions from Google
    - "raccomandazioni Google", "suggerimenti ottimizzazione", "recommendations"
    - Reviewing what Google suggests for the account

    DO NOT USE WHEN:
    - Custom optimization analysis -> use optimization_suggestions
    - Budget pacing -> use budget_pacing

    OUTPUT: Categorized markdown with recommendations grouped by type.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (optional).
        recommendation_type: Filter by type: KEYWORD, BID, BUDGET, AD, or empty for all.
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    conditions = ["recommendation.dismissed = FALSE"]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(
            f"recommendation.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'"
        )

    q = (
        "SELECT "
        "recommendation.type, "
        "recommendation.campaign, "
        "recommendation.ad_group, "
        "recommendation.impact.base_metrics.impressions, "
        "recommendation.impact.base_metrics.clicks, "
        "recommendation.impact.base_metrics.cost_micros, "
        "recommendation.impact.potential_metrics.impressions, "
        "recommendation.impact.potential_metrics.clicks, "
        "recommendation.impact.potential_metrics.cost_micros "
        f"FROM recommendation WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    if not rows:
        return "No active recommendations found."

    # Filter by type if specified
    if recommendation_type:
        rtype_upper = recommendation_type.upper().strip()
        rows = [r for r in rows if rtype_upper in str(r.get("recommendation.type", "")).upper()]

    if not rows:
        return f"No recommendations of type '{recommendation_type}' found."

    # Group by recommendation type
    by_type = {}
    for row in rows:
        rtype = str(row.get("recommendation.type", "UNKNOWN")).replace("_", " ").title()
        if rtype not in by_type:
            by_type[rtype] = []

        # Extract campaign/adgroup from resource names
        camp_rn = str(row.get("recommendation.campaign", ""))
        camp_id = camp_rn.split("/")[-1] if "/" in camp_rn else camp_rn
        ag_rn = str(row.get("recommendation.ad_group", ""))
        ag_id = ag_rn.split("/")[-1] if "/" in ag_rn else ""

        # Compute estimated impact
        base_impr = int(row.get("recommendation.impact.base_metrics.impressions", 0) or 0)
        pot_impr = int(row.get("recommendation.impact.potential_metrics.impressions", 0) or 0)
        base_clicks = int(row.get("recommendation.impact.base_metrics.clicks", 0) or 0)
        pot_clicks = int(row.get("recommendation.impact.potential_metrics.clicks", 0) or 0)
        base_cost = float(row.get("recommendation.impact.base_metrics.cost_micros", 0) or 0) / 1_000_000
        pot_cost = float(row.get("recommendation.impact.potential_metrics.cost_micros", 0) or 0) / 1_000_000

        impact_parts = []
        if pot_impr > base_impr:
            impact_parts.append(f"+{pot_impr - base_impr:,} impr")
        if pot_clicks > base_clicks:
            impact_parts.append(f"+{pot_clicks - base_clicks:,} clicks")
        if pot_cost != base_cost:
            delta = pot_cost - base_cost
            impact_parts.append(f"{'+' if delta > 0 else ''}\u20ac{delta:,.2f} cost")

        impact = " \u00b7 ".join(impact_parts) if impact_parts else "N/A"

        target = f"Campaign {camp_id}"
        if ag_id:
            target += f" > AdGroup {ag_id}"

        by_type[rtype].append({
            "target": target,
            "impact": impact,
        })

    # Build output
    header = build_header(
        title="Google Ads Recommendations",
        client_name=client_name,
        extra=f"{len(rows)} active recommendations",
    )
    parts = [f"**{header}**"]

    for rtype in sorted(by_type.keys()):
        recs = by_type[rtype]
        parts.append(f"\n## {rtype} ({len(recs)})")
        for rec in recs[:20]:  # Cap display per type
            parts.append(f"- {rec['target']} \u2014 Est. impact: {rec['impact']}")
        if len(recs) > 20:
            parts.append(f"*... and {len(recs) - 20} more*")

    return "\n".join(parts)
