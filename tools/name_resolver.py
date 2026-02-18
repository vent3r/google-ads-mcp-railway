"""Resolve campaign/adgroup/keyword names to numeric IDs.

Extends the existing ClientResolver pattern to support write tools that need to
accept human-readable names from the LLM.
"""

import logging
from tools.helpers import ClientResolver, run_query

logger = logging.getLogger(__name__)


def resolve_campaign(client: str, campaign: str) -> tuple:
    """Resolve campaign name or ID to (customer_id, campaign_id).

    Args:
        client: Account name or customer ID
        campaign: Campaign name or numeric ID

    Returns:
        (customer_id: str, campaign_id: str)

    Raises:
        ValueError: if not found or ambiguous
    """
    customer_id = ClientResolver.resolve(client)

    # If already numeric ID, verify existence
    if campaign.isdigit():
        q = (
            f"SELECT campaign.id, campaign.name "
            f"FROM campaign "
            f"WHERE campaign.id = {campaign} "
            f"LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            raise ValueError(f"Campaign ID {campaign} not found in account {client}.")
        return customer_id, campaign

    # Search by name (case-insensitive via GAQL)
    q = (
        f"SELECT campaign.id, campaign.name "
        f"FROM campaign "
        f"WHERE campaign.name = '{campaign}' "
        f"AND campaign.status != 'REMOVED'"
    )
    rows = run_query(customer_id, q)

    if len(rows) == 0:
        raise ValueError(f"Campaign '{campaign}' not found in account {client}.")
    if len(rows) > 1:
        names = [f"- {r.get('campaign.name')} (ID: {r.get('campaign.id')})" for r in rows]
        raise ValueError(
            f"Multiple campaigns match '{campaign}':\n"
            + "\n".join(names)
            + "\nSpecify the campaign ID to disambiguate."
        )
    return customer_id, str(rows[0].get("campaign.id"))


def resolve_adgroup(customer_id: str, campaign_id: str, adgroup: str) -> str:
    """Resolve ad group name or ID to adgroup_id.

    Args:
        customer_id: Already resolved customer ID
        campaign_id: Already resolved campaign ID
        adgroup: Ad group name or numeric ID

    Returns:
        adgroup_id: str

    Raises:
        ValueError: if not found or ambiguous
    """
    if adgroup.isdigit():
        q = (
            f"SELECT ad_group.id FROM ad_group "
            f"WHERE ad_group.id = {adgroup} "
            f"AND campaign.id = {campaign_id} "
            f"LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            raise ValueError(f"Ad group ID {adgroup} not found in campaign {campaign_id}.")
        return adgroup

    q = (
        f"SELECT ad_group.id, ad_group.name FROM ad_group "
        f"WHERE ad_group.name = '{adgroup}' "
        f"AND campaign.id = {campaign_id} "
        f"AND ad_group.status != 'REMOVED'"
    )
    rows = run_query(customer_id, q)

    if len(rows) == 0:
        raise ValueError(f"Ad group '{adgroup}' not found in campaign {campaign_id}.")
    if len(rows) > 1:
        names = [f"- {r.get('ad_group.name')} (ID: {r.get('ad_group.id')})" for r in rows]
        raise ValueError(
            f"Multiple ad groups match '{adgroup}':\n"
            + "\n".join(names)
            + "\nSpecify the ad group ID to disambiguate."
        )
    return str(rows[0].get("ad_group.id"))


def resolve_keyword(customer_id: str, adgroup_id: str, keyword: str) -> str:
    """Resolve keyword text or ID to criterion_id.

    Args:
        customer_id: Already resolved customer ID
        adgroup_id: Already resolved ad group ID
        keyword: Keyword text or numeric criterion ID

    Returns:
        criterion_id: str

    Raises:
        ValueError: if not found or ambiguous
    """
    if keyword.isdigit():
        q = (
            f"SELECT ad_group_criterion.criterion_id "
            f"FROM ad_group_criterion "
            f"WHERE ad_group_criterion.criterion_id = {keyword} "
            f"AND ad_group.id = {adgroup_id} "
            f"LIMIT 1"
        )
        rows = run_query(customer_id, q)
        if not rows:
            raise ValueError(f"Keyword ID {keyword} not found in ad group {adgroup_id}.")
        return keyword

    q = (
        f"SELECT ad_group_criterion.criterion_id, "
        f"ad_group_criterion.keyword.text, "
        f"ad_group_criterion.keyword.match_type "
        f"FROM ad_group_criterion "
        f"WHERE ad_group_criterion.keyword.text = '{keyword}' "
        f"AND ad_group.id = {adgroup_id} "
        f"AND ad_group_criterion.status != 'REMOVED' "
        f"AND ad_group_criterion.negative = FALSE"
    )
    rows = run_query(customer_id, q)

    if len(rows) == 0:
        raise ValueError(f"Keyword '{keyword}' not found in ad group {adgroup_id}.")
    if len(rows) > 1:
        items = [
            f"- '{r.get('ad_group_criterion.keyword.text')}' "
            f"[{r.get('ad_group_criterion.keyword.match_type')}] "
            f"(ID: {r.get('ad_group_criterion.criterion_id')})"
            for r in rows
        ]
        raise ValueError(
            f"Multiple keywords match '{keyword}':\n"
            + "\n".join(items)
            + "\nSpecify the criterion ID to disambiguate."
        )
    return str(rows[0].get("ad_group_criterion.criterion_id"))
