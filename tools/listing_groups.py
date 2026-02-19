"""T8: Product partition tree for Shopping and PMax campaigns."""

import logging
import re

from ads_mcp.coordinator import mcp
from tools.helpers import (
    CampaignResolver,
    ClientResolver,
    run_query,
)
from tools.options import build_header

logger = logging.getLogger(__name__)


def _extract_criterion_id(resource_name: str) -> str:
    """Extract criterion ID from Shopping ad group criterion resource name.

    Format: customers/{cid}/adGroupCriteria/{ag_id}~{criterion_id}
    """
    match = re.search(r"~(\d+)$", resource_name)
    return match.group(1) if match else resource_name


def _extract_filter_id(resource_name: str) -> str:
    """Extract filter ID from PMax listing group filter resource name.

    Format: customers/{cid}/assetGroupListingGroupFilters/{filter_id}
    """
    match = re.search(r"/assetGroupListingGroupFilters/(\d+)$", resource_name)
    return match.group(1) if match else resource_name


def _get_case_value(row: dict, prefix: str) -> str:
    """Extract the case value (brand, type, item_id) from a listing group row."""
    brand = row.get(f"{prefix}.case_value.product_brand.value", "")
    ptype = row.get(f"{prefix}.case_value.product_type.value", "")
    item_id = row.get(f"{prefix}.case_value.product_item_id.value", "")

    if brand:
        return f"Brand: {brand}"
    if ptype:
        return f"Type: {ptype}"
    if item_id:
        return f"Item: {item_id}"
    return "All Products"


def _build_tree_output(nodes: dict, children: dict, root_id: str, indent: int = 0) -> list:
    """DFS render of the product partition tree."""
    lines = []
    node = nodes.get(root_id, {})
    prefix = "  " * indent

    label = node.get("label", "Unknown")
    status = node.get("status", "")
    bid = node.get("bid", "")
    lg_type = node.get("lg_type", "")

    parts = [f"{prefix}- {label}"]
    if status:
        parts.append(f"[{status}]")
    if bid:
        parts.append(f"Bid: \u20ac{bid}")
    if lg_type and lg_type.upper() == "SUBDIVISION":
        parts.append("(subdivision)")

    lines.append(" ".join(parts))

    for child_id in sorted(children.get(root_id, [])):
        lines.extend(_build_tree_output(nodes, children, child_id, indent + 1))

    return lines


@mcp.tool()
def listing_groups(
    client: str,
    campaign: str,
) -> str:
    """Show the product partition (listing group) tree for a Shopping or PMax campaign.

    Renders the hierarchical product partition structure as an indented tree,
    showing brand/type/item splits, status, and bid amounts.

    USE THIS TOOL WHEN:
    - User asks about product partitions, listing groups, product tree
    - "struttura prodotti", "listing groups", "partizioni prodotto"
    - Checking how products are subdivided in Shopping or PMax

    DO NOT USE WHEN:
    - Product performance metrics -> use product_performance
    - Asset group overview -> use pmax_asset_groups

    OUTPUT: Indented tree view of product partitions.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID (REQUIRED — tree view needs a specific campaign).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)
    campaign_id = CampaignResolver.resolve(customer_id, campaign)

    # Auto-detect campaign type
    type_q = (
        f"SELECT campaign.advertising_channel_type, campaign.name "
        f"FROM campaign WHERE campaign.id = {campaign_id}"
    )
    type_rows = run_query(customer_id, type_q)
    if not type_rows:
        return f"Campaign {campaign} not found."

    camp_type = str(type_rows[0].get("campaign.advertising_channel_type", ""))
    camp_name = type_rows[0].get("campaign.name", campaign)

    header = build_header(
        title="Product Partition Tree",
        client_name=client_name,
        extra=f"Campaign: {camp_name} ({camp_type})",
    )

    if "PERFORMANCE_MAX" in camp_type:
        return _render_pmax_tree(customer_id, campaign_id, header)
    elif "SHOPPING" in camp_type:
        return _render_shopping_tree(customer_id, campaign_id, header)
    else:
        return f"{header}\n\nCampaign type {camp_type} does not have listing groups."


def _render_shopping_tree(customer_id: str, campaign_id: str, header: str) -> str:
    """Render listing group tree for standard Shopping campaign."""
    q = (
        "SELECT "
        "ad_group.name, "
        "ad_group_criterion.listing_group.type, "
        "ad_group_criterion.listing_group.case_value.product_brand.value, "
        "ad_group_criterion.listing_group.case_value.product_type.value, "
        "ad_group_criterion.listing_group.case_value.product_item_id.value, "
        "ad_group_criterion.listing_group.parent_ad_group_criterion, "
        "ad_group_criterion.criterion_id, "
        "ad_group_criterion.resource_name, "
        "ad_group_criterion.status, "
        "ad_group_criterion.cpc_bid_micros "
        f"FROM ad_group_criterion "
        f"WHERE campaign.id = {campaign_id} "
        f"AND ad_group_criterion.type = 'LISTING_GROUP' "
        f"AND ad_group_criterion.status != 'REMOVED'"
    )
    rows = run_query(customer_id, q)

    if not rows:
        return f"**{header}**\n\nNo listing groups found."

    # Group by ad group
    by_adgroup = {}
    for row in rows:
        ag_name = row.get("ad_group.name", "Default")
        if ag_name not in by_adgroup:
            by_adgroup[ag_name] = []
        by_adgroup[ag_name].append(row)

    parts = [f"**{header}**"]

    for ag_name, ag_rows in sorted(by_adgroup.items()):
        parts.append(f"\n### Ad Group: {ag_name}")

        nodes = {}
        children = {}
        roots = []

        for row in ag_rows:
            crit_id = str(row.get("ad_group_criterion.criterion_id", ""))
            parent_rn = row.get("ad_group_criterion.listing_group.parent_ad_group_criterion", "")
            parent_id = _extract_criterion_id(parent_rn) if parent_rn else ""

            label = _get_case_value(row, "ad_group_criterion.listing_group")
            status = str(row.get("ad_group_criterion.status", ""))
            bid_micros = float(row.get("ad_group_criterion.cpc_bid_micros", 0) or 0)
            bid = f"{bid_micros / 1_000_000:.2f}" if bid_micros > 0 else ""
            lg_type = str(row.get("ad_group_criterion.listing_group.type", ""))

            nodes[crit_id] = {
                "label": label, "status": status,
                "bid": bid, "lg_type": lg_type,
            }

            if parent_id and parent_id != crit_id:
                children.setdefault(parent_id, []).append(crit_id)
            else:
                roots.append(crit_id)

        for root_id in roots:
            tree_lines = _build_tree_output(nodes, children, root_id)
            parts.extend(tree_lines)

    return "\n".join(parts)


def _render_pmax_tree(customer_id: str, campaign_id: str, header: str) -> str:
    """Render listing group filter tree for PMax campaign."""
    q = (
        "SELECT "
        "asset_group.name, "
        "asset_group_listing_group_filter.type, "
        "asset_group_listing_group_filter.case_value.product_brand.value, "
        "asset_group_listing_group_filter.case_value.product_type.value, "
        "asset_group_listing_group_filter.parent_listing_group_filter, "
        "asset_group_listing_group_filter.id, "
        "asset_group_listing_group_filter.resource_name "
        f"FROM asset_group_listing_group_filter "
        f"WHERE campaign.id = {campaign_id}"
    )
    rows = run_query(customer_id, q)

    if not rows:
        return f"**{header}**\n\nNo listing group filters found."

    # Group by asset group
    by_ag = {}
    for row in rows:
        ag_name = row.get("asset_group.name", "Default")
        if ag_name not in by_ag:
            by_ag[ag_name] = []
        by_ag[ag_name].append(row)

    parts = [f"**{header}**"]

    for ag_name, ag_rows in sorted(by_ag.items()):
        parts.append(f"\n### Asset Group: {ag_name}")

        nodes = {}
        children = {}
        roots = []

        for row in ag_rows:
            filter_id = str(row.get("asset_group_listing_group_filter.id", ""))
            if not filter_id:
                rn = row.get("asset_group_listing_group_filter.resource_name", "")
                filter_id = _extract_filter_id(rn) if rn else ""

            parent_rn = row.get("asset_group_listing_group_filter.parent_listing_group_filter", "")
            parent_id = _extract_filter_id(parent_rn) if parent_rn else ""

            label = _get_case_value(row, "asset_group_listing_group_filter")
            lg_type = str(row.get("asset_group_listing_group_filter.type", ""))

            nodes[filter_id] = {
                "label": label, "status": "",
                "bid": "", "lg_type": lg_type,
            }

            if parent_id and parent_id != filter_id:
                children.setdefault(parent_id, []).append(filter_id)
            else:
                roots.append(filter_id)

        for root_id in roots:
            tree_lines = _build_tree_output(nodes, children, root_id)
            parts.extend(tree_lines)

    return "\n".join(parts)
