"""T6: Performance Max top asset combinations."""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    AssetResolver,
    CampaignResolver,
    ClientResolver,
    run_query,
)
from tools.options import build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def pmax_top_combinations(
    client: str,
    campaign: str = "",
) -> str:
    """Show best-performing asset combinations in Performance Max campaigns.

    Uses the asset_group_top_combination_view to show which combinations of
    headlines, descriptions, and images perform best together. Resolves asset
    resource names to readable text via cached asset lookup.

    USE THIS TOOL WHEN:
    - User asks about best asset combinations in PMax
    - "migliori combinazioni PMax", "top combinations", "quali creativita insieme"

    DO NOT USE WHEN:
    - Individual asset performance labels -> use pmax_assets
    - Asset group level metrics -> use pmax_asset_groups

    OUTPUT: Multi-section markdown with top combinations per asset group.

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
        "asset_group_top_combination_view.asset_group_top_combinations "
        f"FROM asset_group_top_combination_view WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    if not rows:
        return "No top combination data found for Performance Max campaigns."

    # Load asset lookup (cached 1h)
    asset_lookup = AssetResolver.resolve(customer_id)

    def _resolve_asset(resource_name: str) -> str:
        """Resolve an asset resource name to readable text."""
        info = asset_lookup.get(resource_name, {})
        text = info.get("text", "")
        if text:
            return text[:80] + "..." if len(text) > 80 else text
        return resource_name.split("/")[-1] if "/" in resource_name else resource_name

    # Build multi-section output
    header = build_header(
        title="PMax Top Asset Combinations",
        client_name=client_name,
    )
    parts = [f"**{header}**"]

    combo_count = 0
    for row in rows:
        camp_name = row.get("campaign.name", "Unknown")
        ag_name = row.get("asset_group.name", "Unknown")
        combos = row.get("asset_group_top_combination_view.asset_group_top_combinations", [])

        if not combos:
            continue

        parts.append(f"\n## {camp_name} > {ag_name}")

        if isinstance(combos, list):
            for i, combo in enumerate(combos, 1):
                combo_count += 1
                parts.append(f"\n**Combination {i}:**")
                # combo may be a list of asset dicts or a nested structure
                assets = combo if isinstance(combo, list) else [combo]
                for asset_entry in assets:
                    if isinstance(asset_entry, dict):
                        # Try to get asset resource name from the entry
                        asset_rn = (
                            asset_entry.get("asset", "")
                            or asset_entry.get("asset_resource_name", "")
                            or ""
                        )
                        field_type = asset_entry.get("field_type", "")
                        if asset_rn:
                            resolved = _resolve_asset(str(asset_rn))
                            type_label = str(field_type).replace("_", " ").title() if field_type else "Asset"
                            parts.append(f"- **{type_label}**: {resolved}")
                        else:
                            parts.append(f"- {asset_entry}")
                    elif isinstance(asset_entry, str):
                        resolved = _resolve_asset(asset_entry)
                        parts.append(f"- {resolved}")
                    else:
                        parts.append(f"- {asset_entry}")
        else:
            parts.append(f"- {combos}")

    parts.append(f"\n*{combo_count} top combinations found.*")
    return "\n".join(parts)
