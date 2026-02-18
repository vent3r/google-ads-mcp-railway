"""R10: Deep dive into a single campaign — multi-section snapshot."""

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
from tools.validation import micros_to_euros

logger = logging.getLogger(__name__)


@mcp.tool()
def campaign_overview(
    client: str,
    campaign: str,
    date_from: str = "",
    date_to: str = "",
) -> str:
    """Get a complete campaign snapshot: settings, performance, ad groups, keywords.

    USE THIS TOOL WHEN:
    - User asks for a deep dive on a specific campaign
    - "panoramica campagna", "dettagli campagna", "campaign overview"
    - Before making changes to understand current state

    DO NOT USE WHEN:
    - Multiple campaigns comparison → use campaign_analysis
    - Keyword details → use keyword_analysis

    OUTPUT: Multi-section markdown with campaign settings and metrics.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (REQUIRED).
        date_from: Start date YYYY-MM-DD (optional, defaults to last 30 days).
        date_to: End date YYYY-MM-DD (optional, defaults to yesterday).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    campaign_id = CampaignResolver.resolve(customer_id, campaign)

    if not date_from or not date_to:
        date_from, date_to = DateHelper.days_ago(30)

    # 1. Campaign info
    q_info = (
        "SELECT campaign.id, campaign.name, campaign.status, "
        "campaign.advertising_channel_type, campaign.bidding_strategy_type, "
        "campaign_budget.amount_micros "
        f"FROM campaign WHERE campaign.id = {campaign_id} LIMIT 1"
    )
    info_rows = run_query(customer_id, q_info)
    if not info_rows:
        return f"Campaign {campaign_id} not found."
    info = info_rows[0]

    # 2. Performance last period
    q_perf = (
        "SELECT metrics.impressions, metrics.clicks, metrics.cost_micros, "
        "metrics.conversions, metrics.conversions_value, "
        "metrics.search_impression_share "
        f"FROM campaign WHERE campaign.id = {campaign_id} "
        f"AND {DateHelper.date_condition(date_from, date_to)}"
    )
    perf_rows = run_query(customer_id, q_perf)
    perf = {"metrics.impressions": 0, "metrics.clicks": 0, "metrics.cost_micros": 0,
            "metrics.conversions": 0.0, "metrics.conversions_value": 0.0}
    for row in perf_rows:
        perf["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        perf["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        perf["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
        perf["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
        perf["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)
    compute_derived_metrics(perf)

    # 3. Ad groups
    q_ag = (
        "SELECT ad_group.name, ad_group.status, ad_group.id "
        f"FROM ad_group WHERE campaign.id = {campaign_id} "
        "AND ad_group.status != 'REMOVED' ORDER BY ad_group.name"
    )
    ag_rows = run_query(customer_id, q_ag)

    # 4. Keyword count
    q_kw = (
        "SELECT ad_group_criterion.criterion_id "
        f"FROM ad_group_criterion WHERE campaign.id = {campaign_id} "
        "AND ad_group_criterion.status != 'REMOVED' "
        "AND ad_group_criterion.negative = FALSE"
    )
    kw_rows = run_query(customer_id, q_kw)

    # Build output
    budget_eur = micros_to_euros(int(info.get("campaign_budget.amount_micros", 0) or 0))
    campaign_name = info.get("campaign.name", "")
    sections = []

    sections.append(f"# Campaign Overview: {campaign_name}")
    sections.append(f"**Client**: {client_name}\n")

    sections.append("## Settings")
    sections.append(f"- **Status**: {info.get('campaign.status', '')}")
    sections.append(f"- **Type**: {info.get('campaign.advertising_channel_type', '')}")
    sections.append(f"- **Bidding**: {info.get('campaign.bidding_strategy_type', '')}")
    sections.append(f"- **Daily Budget**: \u20ac{budget_eur:,.2f}")

    sections.append(f"\n## Performance ({date_from} \u2192 {date_to})")
    sections.append(f"- **Impressions**: {perf['metrics.impressions']:,}")
    sections.append(f"- **Clicks**: {perf['metrics.clicks']:,}")
    sections.append(f"- **Spend**: \u20ac{perf['_spend']:,.2f}")
    sections.append(f"- **CTR**: {perf['_ctr']:.2f}%")
    sections.append(f"- **CPC**: \u20ac{perf['_cpc']:.2f}")
    sections.append(f"- **Conversions**: {perf['metrics.conversions']:,.1f}")
    sections.append(f"- **CPA**: \u20ac{perf['_cpa']:,.2f}")
    sections.append(f"- **ROAS**: {perf['_roas']:.2f}")

    sections.append(f"\n## Ad Groups ({len(ag_rows)})")
    if ag_rows:
        # Deduplicate by ad_group.id
        seen = set()
        unique_ag = []
        for r in ag_rows:
            ag_id = r.get("ad_group.id", "")
            if ag_id not in seen:
                seen.add(ag_id)
                unique_ag.append(r)
        sections.append("| Ad Group | Status |")
        sections.append("|----------|--------|")
        for r in unique_ag:
            sections.append(f"| {r.get('ad_group.name', '')} | {r.get('ad_group.status', '')} |")

    sections.append(f"\n## Keywords: {len(kw_rows)} active")

    return "\n".join(sections)
