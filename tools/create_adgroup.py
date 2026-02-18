"""W9: Create an ad group."""

import logging
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_bid_amount, euros_to_micros
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


def _get_ads_client():
    from ads_mcp.coordinator import get_google_ads_client
    return get_google_ads_client()


@mcp.tool()
def create_adgroup(
    client: str,
    campaign: str,
    name: str,
    cpc_bid_eur: float = 0.0,
    mode: str = "preview",
) -> str:
    """Create an ad group in a campaign.

    USE THIS TOOL WHEN:
    - User wants to create a new ad group
    - "crea un nuovo ad group", "nuovo gruppo di annunci"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        name: Ad group name (unique within campaign).
        cpc_bid_eur: Default CPC bid EUR (optional, 0.01-100.00). If 0, inherits from campaign.
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if cpc_bid_eur > 0 and not validate_bid_amount(cpc_bid_eur):
        return format_error_for_llm(handle_validation_error(f"Bid out of range", "cpc_bid_eur"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    preview = MutationPreview(
        tool_name="create_adgroup",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Create ad group: {name}",
        changes=[
            {"field": "Name", "old": "—", "new": name},
            {"field": "Default CPC", "old": "—", "new": f"€{cpc_bid_eur:.3f}" if cpc_bid_eur > 0 else "—"},
            {"field": "Status", "old": "—", "new": "ENABLED"},
        ],
        warnings=["Ad group will be ENABLED but campaign may be PAUSED"],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        ads_client = _get_ads_client()
        svc = ads_client.get_service("AdGroupService")
        op = ads_client.get_type("AdGroupOperation")
        ad_group = op.create
        ad_group.name = name
        ad_group.campaign = svc.campaign_path(customer_id, campaign_id)
        ad_group.status = ads_client.enums.AdGroupStatusEnum.AdGroupStatus.ENABLED
        if cpc_bid_eur > 0:
            ad_group.cpc_bid_micros = euros_to_micros(cpc_bid_eur)

        response = svc.mutate_ad_groups(customer_id=customer_id, operations=[op])

        result = MutationResult(
            success=True,
            resource_id=response.results[0].resource_name.split("/")[-1],
            message=f"Ad group '{name}' created (ENABLED).",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="create_adgroup",
                action="create",
                parameters={"campaign": campaign, "name": name, "cpc_bid_eur": cpc_bid_eur},
                old_values={},
                new_values={"adgroup_id": result.resource_id},
                success=True,
            )
        return format_result_for_llm(result)

    except GoogleAdsException as ex:
        error = handle_google_ads_error(ex)
        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="create_adgroup",
                action="create",
                parameters={"campaign": campaign, "name": name},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
