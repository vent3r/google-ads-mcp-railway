"""T5: Performance Max audience signals configuration."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    run_query,
)
from tools.options import build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def pmax_signals(
    client: str,
    campaign: str = "",
) -> str:
    """Show audience signals configured on Performance Max asset groups.

    Configuration-only view — no metrics, no date range. Shows which audiences,
    custom segments, and signals are configured per asset group.

    USE THIS TOOL WHEN:
    - User asks about PMax audience signals, targeting configuration
    - "segnali audience PMax", "audience signals", "targeting PMax"

    DO NOT USE WHEN:
    - Audience performance metrics -> use audience_performance
    - PMax asset group metrics -> use pmax_asset_groups

    OUTPUT: Multi-section markdown with signals per asset group.

    Args:
        client: Account name or customer ID.
        campaign: PMax campaign name or ID (optional, all PMax if empty).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    conditions = [
        "campaign.advertising_channel_type = 'PERFORMANCE_MAX'",
    ]
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        conditions.append(f"campaign.id = {campaign_id}")

    q = (
        "SELECT "
        "campaign.name, "
        "asset_group.name, "
        "asset_group_signal.audience.audience "
        f"FROM asset_group_signal WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    if not rows:
        return "No audience signals found for Performance Max campaigns."

    # Group by campaign > asset group
    by_camp_ag = defaultdict(lambda: defaultdict(list))
    for row in rows:
        camp = row.get("campaign.name", "Unknown")
        ag = row.get("asset_group.name", "Unknown")
        audience = row.get("asset_group_signal.audience.audience", "")
        if audience:
            by_camp_ag[camp][ag].append(str(audience))

    # Build multi-section output
    header = build_header(
        title="PMax Audience Signals",
        client_name=client_name,
    )
    parts = [f"**{header}**"]

    total_signals = 0
    for camp_name in sorted(by_camp_ag.keys()):
        parts.append(f"\n## {camp_name}")
        for ag_name in sorted(by_camp_ag[camp_name].keys()):
            signals = by_camp_ag[camp_name][ag_name]
            total_signals += len(signals)
            parts.append(f"\n**{ag_name}** ({len(signals)} signals)")
            for s in signals:
                # Clean up resource name for readability
                display = s.split("/")[-1] if "/" in s else s
                parts.append(f"- {display}")

    parts.append(f"\n*{total_signals} total signals across {len(rows)} entries.*")
    return "\n".join(parts)
