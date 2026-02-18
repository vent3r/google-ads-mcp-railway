"""W15: Set audience targeting with bid adjustments."""

import logging
import ads_mcp.utils as utils
from ads_mcp.coordinator import mcp
from tools.helpers import ClientResolver
from tools.validation import validate_mode, validate_numeric_range
from tools.error_handler import handle_google_ads_error, handle_validation_error, format_error_for_llm
from tools.mutation import MutationPreview, MutationResult, format_preview_for_llm, format_result_for_llm
from tools.audit import get_audit_logger
from tools.name_resolver import resolve_campaign
from google.ads.googleads.errors import GoogleAdsException

logger = logging.getLogger(__name__)


@mcp.tool()
def set_audience_targeting(
    client: str,
    campaign: str,
    audience_ids: str,
    bid_modifier: float = 0.0,
    mode: str = "preview",
) -> str:
    """Set audience targeting for a campaign with optional bid adjustments.

    USE THIS TOOL WHEN:
    - User wants to target specific audiences
    - "targeting per audience", "audience list"

    ALWAYS call with mode="preview" first.

    Args:
        client: Account name or customer ID.
        campaign: Campaign name or ID.
        audience_ids: Comma-separated audience list IDs.
        bid_modifier: Bid adjustment (-0.90 to 10.0). 0 = no modifier (default).
        mode: "preview" or "execute". Default preview.
    """
    if not validate_mode(mode):
        return format_error_for_llm(handle_validation_error("mode must be 'preview' or 'execute'", "mode"))
    if not validate_numeric_range(bid_modifier, -0.90, 10.0):
        return format_error_for_llm(handle_validation_error("bid_modifier out of range", "bid_modifier"))

    audience_list = [aid.strip() for aid in audience_ids.split(",") if aid.strip()]
    if not audience_list:
        return format_error_for_llm(handle_validation_error("No audience IDs provided", "audience_ids"))

    for aid in audience_list:
        if not aid.isdigit():
            return format_error_for_llm(handle_validation_error(f"Invalid audience ID: {aid}", "audience_ids"))

    try:
        customer_id, campaign_id = resolve_campaign(client, campaign)
        client_name = ClientResolver.resolve_name(customer_id)
    except ValueError as e:
        return format_error_for_llm(handle_validation_error(str(e)))

    pct = bid_modifier * 100 if bid_modifier != 0 else 0
    preview = MutationPreview(
        tool_name="set_audience_targeting",
        client_name=client_name,
        customer_id=customer_id,
        action=f"Target {len(audience_list)} audience(s)",
        changes=[
            {"field": "Audiences", "old": "—", "new": f"{len(audience_list)} audiences"},
            {"field": "Bid Modifier", "old": "—", "new": f"{pct:+.0f}%" if bid_modifier != 0 else "None"},
        ],
    )

    if mode == "preview":
        return format_preview_for_llm(preview)

    try:
        svc = utils.get_googleads_service("CampaignCriterionService")
        operations = []

        for aud_id in audience_list:
            op = utils.get_googleads_type("CampaignCriterionOperation")
            op.create.campaign = svc.campaign_path(customer_id, campaign_id)
            op.create.user_list.user_list = f"customers/{customer_id}/userLists/{aud_id}"
            if bid_modifier != 0:
                op.create.bid_modifier = bid_modifier
            operations.append(op)

        response = svc.mutate_campaign_criteria(customer_id=customer_id, operations=operations)

        result = MutationResult(
            success=True,
            message=f"Added targeting for {len(audience_list)} audience(s).",
        )

        audit = get_audit_logger()
        if audit:
            audit.log_mutation(
                customer_id=customer_id,
                client_name=client_name,
                tool_name="set_audience_targeting",
                action="set_targeting",
                parameters={"audience_ids": audience_list, "bid_modifier": bid_modifier},
                old_values={},
                new_values={"audiences_added": audience_list},
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
                tool_name="set_audience_targeting",
                action="set_targeting",
                parameters={},
                old_values={},
                new_values={},
                success=False,
                error_message=error.message,
            )
        return format_error_for_llm(error)
