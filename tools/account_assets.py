"""T15: Campaign asset/extension performance (sitelinks, callouts, etc.)."""

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
from tools.options import build_header, format_output, process_rows

logger = logging.getLogger(__name__)


@mcp.tool()
def account_assets(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    asset_type: str = "",
) -> str:
    """Show campaign-level asset (extension) performance — sitelinks, callouts, etc.

    USE THIS TOOL WHEN:
    - User asks about extensions, sitelinks, callouts, structured snippets
    - "performance estensioni", "sitelink performance", "callout"
    - "asset campagna", "campaign extensions"

    DO NOT USE WHEN:
    - PMax asset performance labels -> use pmax_assets
    - PMax top combinations -> use pmax_top_combinations

    OUTPUT: Markdown table with asset type, content, performance label, and metrics.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        asset_type: Filter: SITELINK, CALLOUT, STRUCTURED_SNIPPET, CALL, IMAGE (optional).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    date_cond = DateHelper.date_condition(date_from, date_to)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    q = (
        "SELECT "
        "campaign.name, "
        "campaign_asset.field_type, "
        "campaign_asset.performance_label, "
        "campaign_asset.status, "
        "asset.name, "
        "asset.type, "
        "asset.text_asset.text, "
        "asset.sitelink_asset.link_text, "
        "asset.sitelink_asset.final_urls, "
        "asset.callout_asset.callout_text, "
        "metrics.impressions, metrics.clicks, metrics.cost_micros "
        f"FROM campaign_asset "
        f"WHERE {date_cond}{campaign_clause}"
    )
    rows = run_query(customer_id, q)

    # Aggregate by campaign + asset content (rows split by date)
    by_asset = defaultdict(lambda: {
        "campaign.name": "", "field_type": "", "content": "",
        "performance_label": "", "status": "",
        "metrics.impressions": 0, "metrics.clicks": 0,
        "metrics.cost_micros": 0, "metrics.conversions": 0.0,
        "metrics.conversions_value": 0.0,
    })

    for row in rows:
        field_type = str(row.get("campaign_asset.field_type", ""))

        # Filter by asset_type if specified
        if asset_type and asset_type.upper() not in field_type.upper():
            continue

        # Extract content based on asset type
        content = (
            row.get("asset.sitelink_asset.link_text", "")
            or row.get("asset.callout_asset.callout_text", "")
            or row.get("asset.text_asset.text", "")
            or row.get("asset.name", "")
            or ""
        )
        if len(str(content)) > 60:
            content = str(content)[:57] + "..."

        camp_name = row.get("campaign.name", "")
        key = (camp_name, field_type, str(content))
        a = by_asset[key]
        a["campaign.name"] = camp_name
        a["field_type"] = field_type.replace("_", " ").title()
        a["content"] = str(content)
        perf = row.get("campaign_asset.performance_label", "")
        if perf:
            a["performance_label"] = str(perf).replace("_", " ").title()
        a["status"] = str(row.get("campaign_asset.status", ""))
        a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
        a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
        a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)

    results = []
    for a in by_asset.values():
        compute_derived_metrics(a)
        results.append(a)

    rows_out, total, truncated, filter_desc, summary = process_rows(
        results, sort_by="impressions", limit=50,
    )

    columns = [
        ("campaign.name", "Campaign"),
        ("field_type", "Type"),
        ("content", "Content"),
        ("performance_label", "Performance"),
        ("metrics.impressions", "Impr"),
        ("metrics.clicks", "Clicks"),
        ("_ctr", "CTR%"),
    ]

    header = build_header(
        title="Campaign Asset Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
        extra=f"{total} assets" + (f" | type: {asset_type}" if asset_type else ""),
    )

    return format_output(
        rows_out, columns, header=header, output_mode="summary",
        pre_summary=summary, total_filtered=total,
    )
