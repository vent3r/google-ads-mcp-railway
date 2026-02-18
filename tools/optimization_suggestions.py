"""R11: Proactive optimization suggestions based on account data."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    DateHelper,
    compute_derived_metrics,
    run_query,
)

logger = logging.getLogger(__name__)


@mcp.tool()
def optimization_suggestions(
    client: str,
    campaign: str = "",
) -> str:
    """Get proactive optimization suggestions based on account data.

    Analyzes multiple signals: budget lost IS, low quality scores,
    wasteful search terms, and ad group ad count.

    USE THIS TOOL WHEN:
    - User asks for optimization ideas or account health check
    - "suggerimenti", "cosa posso migliorare", "optimization"
    - Proactive account review

    DO NOT USE WHEN:
    - Specific metric analysis → use dedicated tools
    - Making changes → use write tools

    OUTPUT: Categorized list of actionable suggestions.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (optional — all campaigns if empty).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    date_from, date_to = DateHelper.days_ago(30)
    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    suggestions = []

    # 1. Budget lost IS > 20%
    try:
        q = (
            "SELECT campaign.name, metrics.search_budget_lost_impression_share "
            f"FROM campaign WHERE campaign.status = 'ENABLED'"
            f"{campaign_clause} "
            f"AND {DateHelper.date_condition(date_from, date_to)}"
        )
        rows = run_query(customer_id, q)
        budget_lost = {}
        for row in rows:
            cname = row.get("campaign.name", "")
            val = float(row.get("metrics.search_budget_lost_impression_share", 0) or 0)
            if cname:
                budget_lost[cname] = budget_lost.get(cname, [])
                budget_lost[cname].append(val)
        for cname, vals in budget_lost.items():
            avg = sum(vals) / len(vals) if vals else 0
            if avg > 0.20:
                suggestions.append({
                    "category": "BUDGET",
                    "priority": "HIGH",
                    "suggestion": f"Increase budget for '{cname}' \u2014 losing {avg*100:.0f}% impression share due to budget.",
                })
    except Exception:
        pass

    # 2. Low quality score keywords
    try:
        q = (
            "SELECT ad_group_criterion.keyword.text, "
            "ad_group_criterion.quality_info.quality_score, "
            "campaign.name, metrics.clicks, metrics.cost_micros "
            "FROM keyword_view "
            f"WHERE {DateHelper.date_condition(date_from, date_to)}"
            f"{campaign_clause}"
        )
        rows = run_query(customer_id, q)
        for row in rows:
            qs = int(row.get("ad_group_criterion.quality_info.quality_score", 0) or 0)
            clicks = int(row.get("metrics.clicks", 0) or 0)
            kw = row.get("ad_group_criterion.keyword.text", "")
            if 0 < qs < 5 and clicks > 10:
                suggestions.append({
                    "category": "QUALITY_SCORE",
                    "priority": "MEDIUM",
                    "suggestion": f"Improve QS for '{kw}' (QS={qs}, {clicks} clicks) \u2014 review ad relevance and landing page.",
                })
    except Exception:
        pass

    # 3. Wasteful search terms (spend > \u20ac10, 0 conversions)
    try:
        q = (
            "SELECT search_term_view.search_term, "
            "metrics.cost_micros, metrics.conversions "
            f"FROM search_term_view "
            f"WHERE {DateHelper.date_condition(date_from, date_to)}"
            f"{campaign_clause}"
        )
        rows = run_query(customer_id, q)
        term_spend = {}
        term_conv = {}
        for row in rows:
            term = row.get("search_term_view.search_term", "")
            spend = float(row.get("metrics.cost_micros", 0) or 0) / 1_000_000
            conv = float(row.get("metrics.conversions", 0) or 0)
            term_spend[term] = term_spend.get(term, 0) + spend
            term_conv[term] = term_conv.get(term, 0) + conv
        wasteful = [(t, s) for t, s in term_spend.items() if s > 10 and term_conv.get(t, 0) == 0]
        wasteful.sort(key=lambda x: x[1], reverse=True)
        for term, spend in wasteful[:5]:
            suggestions.append({
                "category": "NEGATIVES",
                "priority": "HIGH",
                "suggestion": f"Add negative: '{term}' \u2014 \u20ac{spend:,.2f} spent with 0 conversions.",
            })
    except Exception:
        pass

    # 4. Ad groups with < 2 enabled ads
    try:
        q = (
            "SELECT ad_group.name, campaign.name, ad_group_ad.status "
            f"FROM ad_group_ad WHERE ad_group_ad.status = 'ENABLED'"
            f"{campaign_clause}"
        )
        rows = run_query(customer_id, q)
        ag_ad_count = {}
        ag_campaign = {}
        for row in rows:
            ag = row.get("ad_group.name", "")
            camp = row.get("campaign.name", "")
            ag_ad_count[ag] = ag_ad_count.get(ag, 0) + 1
            ag_campaign[ag] = camp
        for ag, count in ag_ad_count.items():
            if count < 2:
                suggestions.append({
                    "category": "ADS",
                    "priority": "LOW",
                    "suggestion": f"Add more ads to '{ag}' ({ag_campaign.get(ag, '')}) \u2014 only {count} active ad(s).",
                })
    except Exception:
        pass

    if not suggestions:
        return f"# Optimization Suggestions for {client_name}\n\nNo actionable suggestions found. Account looks healthy!"

    # Sort by priority
    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    suggestions.sort(key=lambda s: priority_order.get(s["priority"], 3))

    lines = [f"# Optimization Suggestions for {client_name}"]
    lines.append(f"*Based on last 30 days ({date_from} \u2192 {date_to})*\n")

    current_cat = None
    for s in suggestions:
        if s["category"] != current_cat:
            current_cat = s["category"]
            lines.append(f"\n## {current_cat}")
        icon = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}.get(s["priority"], "\u26aa")
        lines.append(f"- {icon} [{s['priority']}] {s['suggestion']}")

    lines.append(f"\n**Total**: {len(suggestions)} suggestions")
    return "\n".join(lines)
