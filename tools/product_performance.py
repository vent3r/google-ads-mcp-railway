"""T7: Product performance — unified Shopping products + PMax product groups."""

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
def product_performance(
    client: str,
    date_from: str,
    date_to: str,
    campaign: str = "",
    campaign_type: str = "",
    brand: str = "",
    sort_by: str = "spend",
    limit: int = 50,
) -> str:
    """Analyze product-level performance across Shopping and Performance Max campaigns.

    Shows TWO separate sections:
    1. Shopping Products — individual product performance from shopping_performance_view
    2. PMax Product Groups — asset group level metrics from asset_group_product_group_view

    USE THIS TOOL WHEN:
    - User asks about product performance, shopping products, which products sell
    - "performance prodotti", "quali prodotti vendono", "shopping performance"
    - "prodotti PMax", "product groups"

    DO NOT USE WHEN:
    - Product partition tree structure -> use listing_groups
    - Campaign-level overview -> use campaign_analysis

    OUTPUT: Two markdown tables under separate headers.

    Args:
        client: Account name or customer ID.
        date_from: Start date YYYY-MM-DD.
        date_to: End date YYYY-MM-DD.
        campaign: Campaign name or ID (optional).
        campaign_type: SHOPPING, PERFORMANCE_MAX, or empty for both.
        brand: Filter Shopping products by brand (optional).
        sort_by: spend, clicks, conversions, cpa, roas (default spend).
        limit: Max rows per section (default 50).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    date_cond = DateHelper.date_condition(date_from, date_to)

    campaign_clause = ""
    if campaign:
        campaign_id = CampaignResolver.resolve(customer_id, campaign)
        campaign_clause = f" AND campaign.id = {campaign_id}"

    parts = []
    header = build_header(
        title="Product Performance",
        client_name=client_name,
        date_from=date_from,
        date_to=date_to,
    )
    parts.append(f"**{header}**")

    ctype = campaign_type.upper().strip() if campaign_type else ""

    # --- Section 1: Shopping Products ---
    if ctype in ("", "SHOPPING"):
        shop_q = (
            "SELECT "
            "segments.product_item_id, segments.product_title, "
            "segments.product_brand, segments.product_type_l1, "
            "campaign.name, "
            "metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            f"FROM shopping_performance_view "
            f"WHERE {date_cond} "
            f"AND campaign.advertising_channel_type = 'SHOPPING'"
            f"{campaign_clause}"
        )
        shop_rows = run_query(customer_id, shop_q)

        by_product = defaultdict(lambda: {
            "campaign.name": "", "product_id": "", "product_title": "",
            "product_brand": "", "product_type": "",
            "metrics.impressions": 0, "metrics.clicks": 0,
            "metrics.cost_micros": 0, "metrics.conversions": 0.0,
            "metrics.conversions_value": 0.0,
        })

        for row in shop_rows:
            pid = row.get("segments.product_item_id", "")
            b = row.get("segments.product_brand", "")
            if brand and brand.lower() not in str(b).lower():
                continue
            a = by_product[pid]
            a["campaign.name"] = row.get("campaign.name", "")
            a["product_id"] = pid
            a["product_title"] = row.get("segments.product_title", "")
            a["product_brand"] = b
            a["product_type"] = row.get("segments.product_type_l1", "")
            a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
            a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

        shop_results = []
        for a in by_product.values():
            compute_derived_metrics(a)
            shop_results.append(a)

        shop_out, shop_total, _, _, shop_summary = process_rows(
            shop_results, sort_by=sort_by, limit=limit,
        )

        shop_columns = [
            ("campaign.name", "Campaign"),
            ("product_id", "Product ID"),
            ("product_title", "Title"),
            ("product_brand", "Brand"),
            ("product_type", "Type"),
            ("_spend", "Spend \u20ac"),
            ("metrics.clicks", "Clicks"),
            ("metrics.conversions", "Conv"),
            ("_cpa", "CPA \u20ac"),
            ("_roas", "ROAS"),
        ]

        parts.append("\n## Shopping Products")
        parts.append(format_output(
            shop_out, shop_columns, output_mode="summary",
            pre_summary=shop_summary, total_filtered=shop_total,
        ))

    # --- Section 2: PMax Product Groups ---
    if ctype in ("", "PERFORMANCE_MAX"):
        pmax_q = (
            "SELECT "
            "asset_group.name, campaign.name, "
            "metrics.impressions, metrics.clicks, metrics.cost_micros, "
            "metrics.conversions, metrics.conversions_value "
            f"FROM asset_group_product_group_view "
            f"WHERE {date_cond}"
            f"{campaign_clause}"
        )
        pmax_rows = run_query(customer_id, pmax_q)

        by_ag = defaultdict(lambda: {
            "campaign.name": "", "asset_group.name": "",
            "metrics.impressions": 0, "metrics.clicks": 0,
            "metrics.cost_micros": 0, "metrics.conversions": 0.0,
            "metrics.conversions_value": 0.0,
        })

        for row in pmax_rows:
            ag_name = row.get("asset_group.name", "")
            camp_name = row.get("campaign.name", "")
            key = (camp_name, ag_name)
            a = by_ag[key]
            a["campaign.name"] = camp_name
            a["asset_group.name"] = ag_name
            a["metrics.impressions"] += int(row.get("metrics.impressions", 0) or 0)
            a["metrics.clicks"] += int(row.get("metrics.clicks", 0) or 0)
            a["metrics.cost_micros"] += float(row.get("metrics.cost_micros", 0) or 0)
            a["metrics.conversions"] += float(row.get("metrics.conversions", 0) or 0)
            a["metrics.conversions_value"] += float(row.get("metrics.conversions_value", 0) or 0)

        pmax_results = []
        for a in by_ag.values():
            compute_derived_metrics(a)
            pmax_results.append(a)

        pmax_out, pmax_total, _, _, pmax_summary = process_rows(
            pmax_results, sort_by=sort_by, limit=limit,
        )

        pmax_columns = [
            ("campaign.name", "Campaign"),
            ("asset_group.name", "Asset Group"),
            ("_spend", "Spend \u20ac"),
            ("metrics.clicks", "Clicks"),
            ("metrics.conversions", "Conv"),
            ("_cpa", "CPA \u20ac"),
            ("_roas", "ROAS"),
        ]

        parts.append("\n## PMax Product Groups")
        parts.append(format_output(
            pmax_out, pmax_columns, output_mode="summary",
            pre_summary=pmax_summary, total_filtered=pmax_total,
        ))

    return "\n".join(parts)
