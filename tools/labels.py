"""T16: Labels and their associations with campaigns and ad groups."""

import logging
from collections import defaultdict

from ads_mcp.coordinator import mcp
from tools.helpers import (
    ClientResolver,
    run_query,
)
from tools.options import build_header

logger = logging.getLogger(__name__)


@mcp.tool()
def labels(
    client: str,
    level: str = "all",
) -> str:
    """Show account labels and their associations with campaigns and ad groups.

    No date range — shows current label configuration.

    USE THIS TOOL WHEN:
    - User asks about labels, how campaigns/ad groups are labeled
    - "etichette", "labels", "quali label ci sono"

    OUTPUT: Label inventory + associations table.

    Args:
        client: Account name or customer ID.
        level: "all", "campaign", or "adgroup" (default all).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    # Query 1: All labels
    q_labels = (
        "SELECT label.name, label.id, label.status "
        "FROM label WHERE label.status = 'ENABLED'"
    )
    label_rows = run_query(customer_id, q_labels)

    if not label_rows:
        return "No labels found in this account."

    lvl = level.lower().strip()

    # Query 2: Campaign labels (if level is all or campaign)
    camp_labels = defaultdict(list)
    if lvl in ("all", "campaign"):
        q_camp = "SELECT campaign.name, label.name FROM campaign_label"
        try:
            camp_rows = run_query(customer_id, q_camp)
            for row in camp_rows:
                label_name = row.get("label.name", "")
                camp_name = row.get("campaign.name", "")
                if label_name and camp_name:
                    camp_labels[label_name].append(camp_name)
        except ValueError as e:
            logger.warning("labels: campaign_label query failed: %s", e)

    # Query 3: Ad group labels (if level is all or adgroup)
    ag_labels = defaultdict(list)
    if lvl in ("all", "adgroup"):
        q_ag = "SELECT campaign.name, ad_group.name, label.name FROM ad_group_label"
        try:
            ag_rows = run_query(customer_id, q_ag)
            for row in ag_rows:
                label_name = row.get("label.name", "")
                camp_name = row.get("campaign.name", "")
                ag_name = row.get("ad_group.name", "")
                if label_name and ag_name:
                    ag_labels[label_name].append(f"{camp_name} > {ag_name}")
        except ValueError as e:
            logger.warning("labels: ad_group_label query failed: %s", e)

    # Build output
    header = build_header(
        title="Account Labels",
        client_name=client_name,
        extra=f"{len(label_rows)} labels",
    )
    parts = [f"**{header}**"]

    # Overview table
    parts.append("\n## Label Inventory")
    parts.append("| Label | Campaigns | Ad Groups |")
    parts.append("| --- | --- | --- |")
    for row in label_rows:
        name = row.get("label.name", "")
        n_camps = len(camp_labels.get(name, []))
        n_ags = len(ag_labels.get(name, []))
        parts.append(f"| {name} | {n_camps} | {n_ags} |")

    # Detail per label
    for row in label_rows:
        name = row.get("label.name", "")
        camps = camp_labels.get(name, [])
        ags = ag_labels.get(name, [])

        if not camps and not ags:
            continue

        parts.append(f"\n## {name}")
        if camps:
            parts.append(f"**Campaigns ({len(camps)}):**")
            for c in sorted(camps):
                parts.append(f"- {c}")
        if ags:
            parts.append(f"**Ad Groups ({len(ags)}):**")
            for a in sorted(ags)[:50]:
                parts.append(f"- {a}")
            if len(ags) > 50:
                parts.append(f"*... and {len(ags) - 50} more*")

    return "\n".join(parts)
