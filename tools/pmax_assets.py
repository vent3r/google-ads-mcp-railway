"""T2: Performance Max individual asset performance with performance labels."""

import logging

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    run_query,
)
from tools.options import build_header, format_output

logger = logging.getLogger(__name__)


@mcp.tool()
def pmax_assets(
    client: str,
    campaign: str = "",
    asset_type: str = "",
) -> str:
    """Show individual asset performance labels in Performance Max campaigns.

    Shows headlines, descriptions, images, videos with their BEST/GOOD/LOW/LEARNING
    performance labels. This resource does NOT have date-range metrics.

    USE THIS TOOL WHEN:
    - User asks about PMax asset performance, which headlines/images work best
    - "performance label degli asset", "quali headline funzionano in PMax"
    - "asset PMax", "creativita PMax"

    DO NOT USE WHEN:
    - Asset group level metrics -> use pmax_asset_groups
    - Top asset combinations -> use pmax_top_combinations

    OUTPUT: Markdown table grouped by asset group with type, content, and performance label.

    Args:
        client: Account name or customer ID.
        campaign: PMax campaign name or ID (optional, all PMax if empty).
        asset_type: Filter by type: HEADLINE, DESCRIPTION, LONG_HEADLINE,
            MARKETING_IMAGE, YOUTUBE_VIDEO, BUSINESS_NAME, LOGO, CALL_TO_ACTION_SELECTION
            (optional, all types if empty).
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
        "asset_group_asset.field_type, "
        "asset_group_asset.performance_label, "
        "asset_group_asset.status, "
        "asset.name, "
        "asset.text_asset.text, "
        "asset.image_asset.full_size.url, "
        "asset.youtube_video_asset.youtube_video_id "
        f"FROM asset_group_asset WHERE {' AND '.join(conditions)}"
    )
    rows = run_query(customer_id, q)

    results = []
    for row in rows:
        field_type = str(row.get("asset_group_asset.field_type", ""))

        # Filter by asset_type if specified
        if asset_type and asset_type.upper() not in field_type.upper():
            continue

        # Unify asset content from whichever sub-field is populated
        content = (
            row.get("asset.text_asset.text", "")
            or row.get("asset.image_asset.full_size.url", "")
            or row.get("asset.youtube_video_asset.youtube_video_id", "")
            or row.get("asset.name", "")
            or ""
        )
        if len(str(content)) > 80:
            content = str(content)[:77] + "..."

        perf_label = str(row.get("asset_group_asset.performance_label", "")).replace("_", " ").title()
        status = str(row.get("asset_group_asset.status", ""))

        results.append({
            "campaign.name": row.get("campaign.name", ""),
            "asset_group.name": row.get("asset_group.name", ""),
            "field_type": field_type.replace("_", " ").title(),
            "content": str(content),
            "performance_label": perf_label,
            "status": status,
        })

    # Sort: BEST first, then GOOD, LOW, LEARNING, others
    label_order = {"Best": 0, "Good": 1, "Low": 2, "Learning": 3}
    results.sort(key=lambda r: (
        r["campaign.name"],
        r["asset_group.name"],
        label_order.get(r["performance_label"], 9),
    ))

    columns = [
        ("campaign.name", "Campaign"),
        ("asset_group.name", "Asset Group"),
        ("field_type", "Type"),
        ("content", "Content"),
        ("performance_label", "Performance"),
        ("status", "Status"),
    ]

    header = build_header(
        title="PMax Asset Performance Labels",
        client_name=client_name,
        extra=f"{len(results)} assets" + (f" | type: {asset_type}" if asset_type else ""),
    )

    return format_output(results, columns, header=header, output_mode="full")
