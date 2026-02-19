"""T14: Shared negative keyword lists and their campaign associations."""

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
def shared_negatives(
    client: str,
    shared_set_name: str = "",
) -> str:
    """Show shared negative keyword lists, their keywords, and campaign associations.

    No date range — shows current configuration.

    USE THIS TOOL WHEN:
    - User asks about shared negative keyword lists
    - "liste negative condivise", "shared negatives", "negative keyword lists"
    - Reviewing which campaigns share which negative lists

    DO NOT USE WHEN:
    - Suggesting new negatives -> use suggest_negatives
    - Adding negatives -> use add_negatives

    OUTPUT: Multi-section markdown with list overview, keywords per list, and campaign associations.

    Args:
        client: Account name or customer ID.
        shared_set_name: Filter to a specific shared set name (optional).
    """
    customer_id = ClientResolver.resolve(client)
    client_name = ClientResolver.resolve_name(customer_id)

    # Query 1: Shared sets overview
    q_sets = (
        "SELECT shared_set.name, shared_set.id, shared_set.member_count "
        "FROM shared_set "
        "WHERE shared_set.type = 'NEGATIVE_KEYWORDS' "
        "AND shared_set.status = 'ENABLED'"
    )
    set_rows = run_query(customer_id, q_sets)

    if not set_rows:
        return "No shared negative keyword lists found."

    # Query 2: Keywords in lists
    q_criteria = (
        "SELECT shared_criterion.keyword.text, shared_criterion.keyword.match_type, "
        "shared_set.name "
        "FROM shared_criterion "
        "WHERE shared_set.type = 'NEGATIVE_KEYWORDS'"
    )
    crit_rows = run_query(customer_id, q_criteria)

    # Query 3: Campaign associations
    q_assoc = (
        "SELECT campaign.name, shared_set.name "
        "FROM campaign_shared_set "
        "WHERE shared_set.type = 'NEGATIVE_KEYWORDS'"
    )
    assoc_rows = run_query(customer_id, q_assoc)

    # Organize keywords by set
    keywords_by_set = defaultdict(list)
    for row in crit_rows:
        set_name = row.get("shared_set.name", "")
        kw_text = row.get("shared_criterion.keyword.text", "")
        match_type = str(row.get("shared_criterion.keyword.match_type", "")).replace("_", " ").title()
        if kw_text:
            keywords_by_set[set_name].append(f"{kw_text} [{match_type}]")

    # Organize campaigns by set
    campaigns_by_set = defaultdict(list)
    for row in assoc_rows:
        set_name = row.get("shared_set.name", "")
        camp_name = row.get("campaign.name", "")
        if camp_name:
            campaigns_by_set[set_name].append(camp_name)

    # Build output
    header = build_header(
        title="Shared Negative Keyword Lists",
        client_name=client_name,
        extra=f"{len(set_rows)} lists",
    )
    parts = [f"**{header}**"]

    # Overview table
    parts.append("\n## Overview")
    parts.append("| List Name | Keywords | Campaigns |")
    parts.append("| --- | --- | --- |")
    for row in set_rows:
        name = row.get("shared_set.name", "")
        member_count = row.get("shared_set.member_count", 0)
        camp_count = len(campaigns_by_set.get(name, []))
        parts.append(f"| {name} | {member_count} | {camp_count} |")

    # Detail per list (filter if shared_set_name specified)
    target_sets = set_rows
    if shared_set_name:
        target_sets = [r for r in set_rows if shared_set_name.lower() in str(r.get("shared_set.name", "")).lower()]

    for row in target_sets:
        name = row.get("shared_set.name", "")

        # Keywords
        kws = keywords_by_set.get(name, [])
        parts.append(f"\n## {name} ({len(kws)} keywords)")
        if kws:
            for kw in sorted(kws)[:100]:
                parts.append(f"- {kw}")
            if len(kws) > 100:
                parts.append(f"*... and {len(kws) - 100} more keywords*")
        else:
            parts.append("*No keywords loaded (member_count may differ from API visibility)*")

        # Campaign associations
        camps = campaigns_by_set.get(name, [])
        if camps:
            parts.append(f"\n**Applied to {len(camps)} campaigns:**")
            for c in sorted(camps):
                parts.append(f"- {c}")

    return "\n".join(parts)
